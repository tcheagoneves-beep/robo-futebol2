# --- 8. DASHBOARD ---
if ROBO_LIGADO:
    carregar_tudo()

    hoje_real = get_time_br().strftime('%Y-%m-%d')
    st.session_state['historico_sinais'] = [
        s for s in st.session_state['historico_sinais'] 
        if s['Data'] == hoje_real
    ]

    api_error = False
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        resp = requests.get(url, headers={"x-apisports-key": API_KEY}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
        update_api_usage(resp.headers) 
        res = resp.json()
        if "errors" in res and res['errors']: api_error = True; jogos_live = []
        else: jogos_live = res.get('response', [])
    except: jogos_live = []; api_error = True

    if not api_error: 
        check_green_red_hibrido(jogos_live, TG_TOKEN, TG_CHAT, API_KEY)
        verificar_alerta_matinal(TG_TOKEN, TG_CHAT, API_KEY) 

    radar = []
    # --- PREPARAÇÃO DOS IDS PARA CLASSIFICAÇÃO ---
    ids_black = st.session_state['df_black']['id'].values
    ids_safe = st.session_state['df_safe']['id'].values
    
    df_vip_temp = st.session_state.get('df_vip', pd.DataFrame())
    ids_obs = df_vip_temp['id'].values if not df_vip_temp.empty else []
    
    candidatos_multipla = []; ids_no_radar = [] 

    for j in jogos_live:
        lid = str(j['league']['id']); fid = j['fixture']['id']
        
        if lid in ids_black: continue
        
        # --- LÓGICA VISUAL DA LIGA ---
        nome_liga_show = j['league']['name']
        if lid in ids_safe:
            nome_liga_show += " 🛡️"  # Ícone para Segura/Validada
        elif lid in ids_obs:
            nome_liga_show += " ⚠️"  # Ícone para Observação/Strike
            
        ids_no_radar.append(fid)
        tempo = j['fixture']['status']['elapsed'] or 0
        status_short = j['fixture']['status']['short']
        home = j['teams']['home']['name']; away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"
        gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
        stats = []; lista_sinais = []; status_vis = "👁️"
        
        rank_h, rank_a = None, None; tem_tabela = False
        if j['league']['id'] in LIGAS_TABELA:
            season = j['league']['season']
            ranking = buscar_ranking(API_KEY, j['league']['id'], season)
            rank_h = ranking.get(home); rank_a = ranking.get(away)
            if rank_h and rank_a: tem_tabela = True

        tempo_espera = 180 
        if 69 <= tempo <= 76: tempo_espera = 60 
        elif tempo <= 15: tempo_espera = 90 
        ultimo_check = st.session_state['controle_stats'].get(fid, datetime.min)
        agora_dt = datetime.now()
        pode_buscar_api = (agora_dt - ultimo_check).total_seconds() > tempo_espera
        chave_memoria_stats = f"stats_cache_{fid}"

        if deve_buscar_stats(tempo, gh, ga, status_short):
            if pode_buscar_api:
                try:
                    resp_stats = requests.get("https://v3.football.api-sports.io/fixtures/statistics", headers={"x-apisports-key": API_KEY}, params={"fixture": fid}, timeout=5)
                    update_api_usage(resp_stats.headers)
                    stats_req = resp_stats.json().get('response', [])
                    if stats_req:
                        stats = stats_req
                        st.session_state['controle_stats'][fid] = agora_dt
                        st.session_state[chave_memoria_stats] = stats
                    else: stats = []
                except: stats = []
            else: stats = st.session_state.get(chave_memoria_stats, [])
        else: stats = [] 

        if stats:
            lista_sinais = processar(j, stats, tempo, placar, rank_h, rank_a)
            salvar_safe_league(lid, j['league']['country'], j['league']['name'], True, tem_tabela)
            if status_short == 'HT' and gh == 0 and ga == 0:
                try:
                    sh_h = next((x['value'] for x in stats[0]['statistics'] if x['type']=='Total Shots'), 0) or 0
                    sh_a = next((x['value'] for x in stats[1]['statistics'] if x['type']=='Total Shots'), 0) or 0
                    sog_h = next((x['value'] for x in stats[0]['statistics'] if x['type']=='Shots on Goal'), 0) or 0
                    sog_a = next((x['value'] for x in stats[1]['statistics'] if x['type']=='Shots on Goal'), 0) or 0
                    if (sh_h + sh_a) > 12 and (sog_h + sog_a) > 6:
                        candidatos_multipla.append({'fid': fid, 'jogo': f"{home} x {away}", 'stats': f"{sh_h+sh_a} Chutes", 'indica': "Over 0.5 FT"})
                except: pass
        else: status_vis = "💤"

        if not lista_sinais and not stats and tempo >= 45 and status_short != 'HT': gerenciar_strikes(lid, j['league']['country'], j['league']['name'])
        
        if lista_sinais:
            status_vis = f"✅ {len(lista_sinais)} Sinais"
            for sinal in lista_sinais:
                id_unico = f"{fid}_{sinal['tag']}"
                if id_unico not in st.session_state['alertas_enviados']:
                    item = {"FID": fid, "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Placar_Sinal": placar, "Estrategia": sinal['tag'], "Resultado": "Pendente"}
                    if adicionar_historico(item):
                        prob_msg = buscar_inteligencia(sinal['tag'], j['league']['name'], f"{home} x {away}")
                        
                        msg = f"<b>🚨 SINAL ENCONTRADO 🚨</b>\n\n🏆 <b>{j['league']['name']}</b>\n⚽ {home} 🆚 {away}\n⏰ <b>{tempo}' minutos</b> (Placar: {placar})\n\n🔥 <b>{sinal['tag'].upper()}</b>\n⚠️ <b>AÇÃO:</b> {sinal['ordem']}\n\n📊 <i>Dados: {sinal['stats']}</i>{prob_msg}"
                        enviar_telegram(TG_TOKEN, TG_CHAT, msg)
                        st.session_state['alertas_enviados'].add(id_unico)
                        st.toast(f"Sinal: {sinal['tag']}")
        
        # AQUI USAMOS O NOME COM O ICONE
        radar.append({"Liga": nome_liga_show, "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})

    if candidatos_multipla:
        novos = [c for c in candidatos_multipla if c['fid'] not in st.session_state['multiplas_enviadas']]
        if novos:
            msg = "<b>🚀 OPORTUNIDADE DE MÚLTIPLA (HT) 🚀</b>\n"
            for c in novos: msg += f"\n⚽ {c['jogo']} ({c['stats']})"; st.session_state['multiplas_enviadas'].add(c['fid'])
            enviar_telegram(TG_TOKEN, TG_CHAT, msg)

    agenda = []
    if not api_error:
        hoje_br = get_time_br().strftime('%Y-%m-%d')
        prox = buscar_agenda_cached(API_KEY, hoje_br)
        agora = get_time_br()
        for p in prox:
            try:
                pid = p['fixture']['id']
                if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] in ['NS', 'TBD'] and pid not in ids_no_radar:
                    if datetime.fromisoformat(p['fixture']['date']) > agora:
                        agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
            except: pass

    # RELATORIO AUTOMÁTICO 23:59
    hora_atual = get_time_br().hour
    if hora_atual >= 23:
        arquivo_hoje = get_time_br().strftime('%Y-%m-%d')
        chave_relatorio = f"relatorio_enviado_{arquivo_hoje}"
        if chave_relatorio not in st.session_state:
            enviar_relatorio_bi(TG_TOKEN, TG_CHAT)
            st.session_state[chave_relatorio] = True

    # DISPLAY
    dashboard_placeholder = st.empty()
    with dashboard_placeholder.container():
        if api_error: st.markdown('<div class="status-error">🚨 API LIMITADA - AGUARDE</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        hist_hoje = pd.DataFrame(st.session_state['historico_sinais'])
        t_hj, g_hj, r_hj, w_hj = calcular_stats(hist_hoje)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t_hj}</div><div class="metric-sub">{g_hj} Green | {r_hj} Red</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(radar)}</div><div class="metric-sub">Monitorando</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Ligas Seguras</div><div class="metric-value">{len(st.session_state["df_safe"])}</div><div class="metric-sub">Validadas</div></div>', unsafe_allow_html=True)
        
        st.write("")
        abas = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", f"📜 Histórico ({len(hist_hoje)})", "📈 BI & Analytics", f"🚫 Blacklist ({len(st.session_state['df_black'])})", f"🛡️ Seguras ({len(st.session_state['df_safe'])})", f"⚠️ Obs ({len(st.session_state.get('df_vip', []))})"])
        
        with abas[0]: 
            if radar: st.dataframe(pd.DataFrame(radar).astype(str), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        with abas[1]: 
            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos futuros hoje.")
        with abas[2]: 
            if not hist_hoje.empty: st.dataframe(hist_hoje.astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Vazio.")
        
        with abas[3]: 
            st.markdown("### 📊 Inteligência de Mercado")
            df_bi = st.session_state.get('historico_full', pd.DataFrame())
            if df_bi.empty: st.warning("Sem dados históricos na nuvem.")
            else:
                dias = st.selectbox("📅 Período", ["Tudo", "Hoje", "7 Dias", "30 Dias"])
                df_bi['Data'] = pd.to_datetime(df_bi['Data'], errors='coerce')
                hoje_bi = pd.to_datetime(get_time_br().date())
                if dias == "Hoje": df_show = df_bi[df_bi['Data'] == hoje_bi]
                elif dias == "7 Dias": df_show = df_bi[df_bi['Data'] >= (hoje_bi - timedelta(days=7))]
                elif dias == "30 Dias": df_show = df_bi[df_bi['Data'] >= (hoje_bi - timedelta(days=30))]
                else: df_show = df_bi
                
                if not df_show.empty:
                    greens_bi = df_show['Resultado'].str.contains('GREEN').sum()
                    reds_bi = df_show['Resultado'].str.contains('RED').sum()
                    tot_bi = greens_bi + reds_bi
                    wr_bi = (greens_bi / tot_bi * 100) if tot_bi > 0 else 0
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Sinais", tot_bi); m2.metric("Greens", greens_bi); m3.metric("Reds", reds_bi); m4.metric("Assertividade", f"{wr_bi:.1f}%")
                    st.divider()
                    
                    stats_strat = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                    if not stats_strat.empty:
                        counts = stats_strat.groupby(['Estrategia', 'Resultado']).size().reset_index(name='Qtd')
                        fig = px.bar(counts, x='Estrategia', y='Qtd', color='Resultado', 
                                     color_discrete_map={'✅ GREEN': '#00FF00', '❌ RED': '#FF0000'},
                                     title="Performance por Estratégia", text='Qtd')
                        fig.update_layout(xaxis_title=None, yaxis_title=None, template="plotly_dark")
                        st.plotly_chart(fig, use_container_width=True)

                    cb1, cb2 = st.columns(2)
                    with cb1:
                        st.caption("🏆 Melhores Ligas")
                        stats = df_show.groupby('Liga')['Resultado'].apply(lambda x: x.str.contains('GREEN').sum() / len(x) * 100).reset_index(name='Winrate')
                        counts = df_show['Liga'].value_counts().reset_index(name='Qtd')
                        final = stats.merge(counts, left_on='Liga', right_on='Liga')
                        st.dataframe(final[final['Qtd'] >= 2].sort_values('Winrate', ascending=False).head(5), hide_index=True, use_container_width=True)
                    with cb2:
                        st.caption("⚡ Top Estratégias")
                        stats_s = df_show.groupby('Estrategia')['Resultado'].apply(lambda x: x.str.contains('GREEN').sum() / len(x) * 100).reset_index(name='Winrate')
                        st.dataframe(stats_s.sort_values('Winrate', ascending=False), hide_index=True, use_container_width=True)
                    
                    st.divider()
                    st.markdown("### 👑 Reis do Green (Times que mais lucram)")
                    df_g = df_show[df_show['Resultado'].str.contains('GREEN')]
                    lista_times = []
                    for jogo in df_g['Jogo']:
                        try:
                            partes = jogo.split(' x ')
                            t1 = partes[0].split('(')[0].strip()
                            t2 = partes[1].split('(')[0].strip()
                            lista_times.append(t1)
                            lista_times.append(t2)
                        except: pass
                    if lista_times:
                        df_reis = pd.DataFrame(lista_times, columns=['Time'])
                        top_reis = df_reis['Time'].value_counts().reset_index()
                        top_reis.columns = ['Time', 'Qtd Green']
                        st.dataframe(top_reis.head(10), use_container_width=True, hide_index=True)
                    else:
                        st.caption("Sem dados de times suficientes.")
        
        with abas[4]: st.dataframe(st.session_state['df_black'], use_container_width=True, hide_index=True)
        with abas[5]: st.dataframe(st.session_state['df_safe'], use_container_width=True, hide_index=True)
        with abas[6]: st.dataframe(st.session_state.get('df_vip', pd.DataFrame()), use_container_width=True, hide_index=True)

    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
        time.sleep(1)
    st.rerun()

else:
    st.title("❄️ Neves Analytics")
    st.info("💡 Robô em espera. Configure na lateral.")
