# ==============================================================================
# 5. SIDEBAR E LOOP PRINCIPAL (EXECUÇÃO)
# ==============================================================================
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=True):
        st.session_state['API_KEY'] = st.text_input("Chave API:", value=st.session_state['API_KEY'], type="password")
        st.session_state['TG_TOKEN'] = st.text_input("Token Telegram:", value=st.session_state['TG_TOKEN'], type="password")
        st.session_state['TG_CHAT'] = st.text_input("Chat IDs:", value=st.session_state['TG_CHAT'])
        INTERVALO = st.slider("Ciclo (s):", 60, 300, 60)
        if st.button("🧹 Limpar Cache"): 
            st.cache_data.clear(); carregar_tudo(force=True); st.session_state['last_db_update'] = 0; st.toast("Cache Limpo!")
        st.write("---")
        if st.button("🧠 Pedir Análise do BI"):
            if IA_ATIVADA:
                with st.spinner("🤖 O Consultor Neves está analisando seus dados..."):
                    analise = analisar_bi_com_ia()
                    st.markdown("### 📝 Relatório do Consultor")
                    st.info(analise)
            else: st.error("IA Desconectada.")
        if st.button("🧪 Criar Nova Estratégia (Big Data)"):
            if IA_ATIVADA:
                with st.spinner("🤖 Analisando padrões globais no Big Data..."):
                    sugestao = criar_estrategia_nova_ia()
                    st.markdown("### 💡 Sugestão da IA")
                    st.success(sugestao)
            else: st.error("IA Desconectada.")
        
        if st.button("🔧 Otimizar Estratégias (IA)"):
            if IA_ATIVADA and db_firestore:
                with st.spinner("🕵️ Cruzando Greens/Reds com Big Data..."):
                    relatorio = otimizar_estrategias_existentes_ia()
                    st.markdown("### 🛠️ Plano de Melhoria")
                    if "Erro" in relatorio or "Atenção" in relatorio: st.warning(relatorio)
                    else: st.success("Análise Concluída!"); st.write(relatorio)
            else: st.error("Requer IA Ativa e Conexão Firebase.")
        
        if st.button("🔄 Forçar Backfill (Salvar Jogos Perdidos)"):
            with st.spinner("Buscando na API todos os jogos finalizados hoje..."):
                hoje_real = get_time_br().strftime('%Y-%m-%d')
                todos_jogos_hoje = buscar_agenda_cached(st.session_state['API_KEY'], hoje_real)
                ft_pendentes = [j for j in todos_jogos_hoje if j['fixture']['status']['short'] in ['FT', 'AET', 'PEN'] and str(j['fixture']['id']) not in st.session_state['jogos_salvos_bigdata']]
                if ft_pendentes:
                    st.info(f"Processando {len(ft_pendentes)} jogos...")
                    stats_recuperadas = atualizar_stats_em_paralelo(ft_pendentes, st.session_state['API_KEY'])
                    count_salvos = 0
                    for fid, stats in stats_recuperadas.items():
                        j_obj = next((x for x in ft_pendentes if str(x['fixture']['id']) == str(fid)), None)
                        if j_obj: salvar_bigdata(j_obj, stats); count_salvos += 1
                    st.success(f"✅ Recuperados e Salvos: {count_salvos} jogos!")
                else: st.warning("Nenhum jogo finalizado pendente.")

        if st.button("📊 Enviar Relatório BI"): enviar_relatorio_bi(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT']); st.toast("Relatório Enviado!")
        if st.button("💰 Enviar Relatório Financeiro"):
            if 'last_fin_stats' in st.session_state:
                s = st.session_state['last_fin_stats']
                enviar_relatorio_financeiro(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], s['cenario'], s['lucro'], s['roi'], s['entradas'])
                st.toast("Relatório Enviado!")
            else: st.error("Abra a aba Financeiro primeiro.")

    with st.expander("💰 Gestão de Banca", expanded=False):
        stake_padrao = st.number_input("Valor da Aposta (R$)", value=st.session_state.get('stake_padrao', 10.0), step=5.0)
        banca_inicial = st.number_input("Banca Inicial (R$)", value=st.session_state.get('banca_inicial', 100.0), step=50.0)
        st.session_state['stake_padrao'] = stake_padrao
        st.session_state['banca_inicial'] = banca_inicial
        
    with st.expander("📶 Consumo API", expanded=False):
        verificar_reset_diario()
        u = st.session_state['api_usage']; perc = min(u['used'] / u['limit'], 1.0) if u['limit'] > 0 else 0
        st.progress(perc); st.caption(f"Utilizado: **{u['used']}** / {u['limit']}")
    
    with st.expander("🤖 Consumo IA (Gemini)", expanded=False):
        u_ia = st.session_state['gemini_usage']; u_ia['limit'] = 10000 
        perc_ia = min(u_ia['used'] / u_ia['limit'], 1.0)
        st.progress(perc_ia); st.caption(f"Requições Hoje: **{u_ia['used']}** / {u_ia['limit']}")
        if st.button("🔓 Destravar IA Agora"):
            st.session_state['ia_bloqueada_ate'] = None; st.toast("✅ IA Destravada!")

    st.write("---")
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    
    if IA_ATIVADA:
        if st.session_state['ia_bloqueada_ate']:
            ag = datetime.now()
            if ag < st.session_state['ia_bloqueada_ate']:
                m_rest = int((st.session_state['ia_bloqueada_ate'] - ag).total_seconds()/60)
                st.markdown(f'<div class="status-warning">⚠️ IA PAUSADA (Proteção) - Volta em {m_rest} min</div>', unsafe_allow_html=True)
            else: st.markdown('<div class="status-active">🤖 IA GEMINI ATIVA</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🤖 IA GEMINI ATIVA</div>', unsafe_allow_html=True)
    else: st.markdown('<div class="status-error">❌ IA DESCONECTADA</div>', unsafe_allow_html=True)

    if db_firestore: st.markdown('<div class="status-active">🔥 FIREBASE CONECTADO</div>', unsafe_allow_html=True)
    else: st.markdown('<div class="status-warning">⚠️ FIREBASE OFFLINE</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### ⚠️ Zona de Perigo")
    if st.button("☢️ ZERAR ROBÔ", type="primary", use_container_width=True): st.session_state['confirmar_reset'] = True
    if st.session_state.get('confirmar_reset'):
        st.error("Tem certeza? Isso apaga TODO o histórico.")
        c1, c2 = st.columns(2)
        if c1.button("✅ SIM"): 
            st.cache_data.clear()
            st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
            salvar_aba("Historico", st.session_state['historico_full'])
            st.session_state['confirmar_reset'] = False; st.rerun()
        if c2.button("❌ NÃO"): st.session_state['confirmar_reset'] = False; st.rerun()

if st.session_state.ROBO_LIGADO:
    with placeholder_root.container():
        carregar_tudo()
        s_padrao = st.session_state.get('stake_padrao', 10.0)
        b_inicial = st.session_state.get('banca_inicial', 100.0)
        safe_token = st.session_state.get('TG_TOKEN', '')
        safe_chat = st.session_state.get('TG_CHAT', '')
        safe_api = st.session_state.get('API_KEY', '')

        verificar_automacao_bi(safe_token, safe_chat, s_padrao)
        verificar_alerta_matinal(safe_token, safe_chat, safe_api)
        
        ids_black = [normalizar_id(x) for x in st.session_state['df_black']['id'].values]
        df_obs = st.session_state.get('df_vip', pd.DataFrame()); count_obs = len(df_obs)
        df_safe_show = st.session_state.get('df_safe', pd.DataFrame()); count_safe = len(df_safe_show)
        ids_safe = [normalizar_id(x) for x in df_safe_show['id'].values]
        hoje_real = get_time_br().strftime('%Y-%m-%d')
        if 'historico_full' in st.session_state and not st.session_state['historico_full'].empty:
             df_full = st.session_state['historico_full']
             st.session_state['historico_sinais'] = df_full[df_full['Data'] == hoje_real].to_dict('records')[::-1]

        api_error = False; jogos_live = []
        try:
            url = "https://v3.football.api-sports.io/fixtures"
            resp = requests.get(url, headers={"x-apisports-key": safe_api}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
            update_api_usage(resp.headers); res = resp.json()
            raw_live = res.get('response', []) if not res.get('errors') else []
            dict_clean = {j['fixture']['id']: j for j in raw_live}
            jogos_live = list(dict_clean.values())
            api_error = bool(res.get('errors'))
            if api_error and "errors" in res: st.error(f"Detalhe do Erro: {res['errors']}")
        except Exception as e: jogos_live = []; api_error = True; st.error(f"Erro de Conexão: {e}")

        if not api_error: 
            check_green_red_hibrido(jogos_live, safe_token, safe_chat, safe_api)
            conferir_resultados_sniper(jogos_live, safe_api) 
            verificar_var_rollback(jogos_live, safe_token, safe_chat)
        
        radar = []; agenda = []; candidatos_multipla = []; ids_no_radar = []
        if not api_error:
            # 1. BIG DATA (RANDOMIZADO)
            prox = buscar_agenda_cached(safe_api, hoje_real); agora = get_time_br()
            ft_para_salvar = []
            for p in prox:
                try:
                    if p['fixture']['status']['short'] in ['FT', 'AET', 'PEN'] and str(p['fixture']['id']) not in st.session_state['jogos_salvos_bigdata']:
                        ft_para_salvar.append(p)
                except: pass
            if ft_para_salvar:
                lote = random.sample(ft_para_salvar, min(len(ft_para_salvar), 3)) 
                stats_ft = atualizar_stats_em_paralelo(lote, safe_api)
                for fid, s in stats_ft.items():
                    j_obj = next((x for x in lote if x['fixture']['id'] == fid), None)
                    if j_obj: salvar_bigdata(j_obj, s)

            # 2. PROCESSAMENTO DE SINAIS
            STATUS_BOLA_ROLANDO = ['1H', '2H', 'HT', 'ET', 'P', 'BT']
            
            for j in jogos_live:
                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                if lid in ids_black: continue
                status_short = j['fixture']['status']['short']
                elapsed = j['fixture']['status']['elapsed']
                if status_short not in STATUS_BOLA_ROLANDO: continue
                if (elapsed is None or elapsed == 0) and status_short != 'HT': continue

                nome_liga_show = j['league']['name']
                if lid in ids_safe: nome_liga_show += " 🛡️"
                elif lid in df_obs['id'].values: nome_liga_show += " ⚠️"
                else: nome_liga_show += " ❓" 
                ids_no_radar.append(fid)
                tempo = j['fixture']['status']['elapsed'] or 0; st_short = j['fixture']['status']['short']
                home = j['teams']['home']['name']; away = j['teams']['away']['name']
                placar = f"{j['goals']['home']}x{j['goals']['away']}"; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
                if st_short == 'FT': continue 
                
                stats = []
                ult_chk = st.session_state['controle_stats'].get(fid, datetime.min)
                t_esp = 60 if (69<=tempo<=76) else (90 if tempo<=15 else 180)
                if deve_buscar_stats(tempo, gh, ga, st_short):
                    if (datetime.now() - ult_chk).total_seconds() > t_esp:
                         fid_res, s_res, h_res = fetch_stats_single(fid, safe_api)
                         if s_res:
                             st.session_state['controle_stats'][fid] = datetime.now()
                             st.session_state[f"st_{fid}"] = s_res
                             update_api_usage(h_res)
                stats = st.session_state.get(f"st_{fid}", [])
                
                rank_h = None; rank_a = None
                if j['league']['id'] in LIGAS_TABELA:
                    rk = buscar_ranking(safe_api, j['league']['id'], j['league']['season'])
                    rank_h = rk.get(home); rank_a = rk.get(away)
                lista_sinais = []
                if stats:
                    lista_sinais = processar(j, stats, tempo, placar, rank_h, rank_a)
                    salvar_safe_league_basic(lid, j['league']['country'], j['league']['name'], tem_tabela=(rank_h is not None))
                    resetar_erros(lid)
                    # Lógica de Múltipla HT
                    if st_short == 'HT' and gh == 0 and ga == 0:
                        try:
                            s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
                            v1 = next((x['value'] for x in s1 if x['type']=='Total Shots'), 0) or 0
                            v2 = next((x['value'] for x in s2 if x['type']=='Total Shots'), 0) or 0
                            sg1 = next((x['value'] for x in s1 if x['type']=='Shots on Goal'), 0) or 0
                            sg2 = next((x['value'] for x in s2 if x['type']=='Shots on Goal'), 0) or 0
                            if (v1+v2) > 12 and (sg1+sg2) > 6: candidatos_multipla.append({'fid': fid, 'jogo': f"{home} x {away}", 'stats': f"{v1+v2} Chutes", 'indica': "Over 0.5 FT"})
                        except: pass
                else: gerenciar_erros(lid, j['league']['country'], j['league']['name'], fid)

                # --- LÓGICA DE STATUS E RADAR ---
                status_vis = "🟢 Monitorando"

                if lista_sinais:
                    status_vis = f"✅ {len(lista_sinais)} Sinais"
                    medias_gols = buscar_media_gols_ultimos_jogos(safe_api, j['teams']['home']['id'], j['teams']['away']['id'])
                    
                    for s in lista_sinais:
                        rh = s.get('rh', 0); ra = s.get('ra', 0)
                        txt_pressao = gerar_barra_pressao(rh, ra) 
                        
                        # --- CHAVE UNIVERSAL ---
                        uid_normal = gerar_chave_universal(fid, s['tag'], "SINAL")
                        uid_super = f"SUPER_{uid_normal}"
                        
                        if uid_normal in st.session_state['alertas_enviados']: continue 
                        st.session_state['alertas_enviados'].add(uid_normal)

                        odd_atual_str = get_live_odds(fid, safe_api, s['tag'], gh+ga)
                        try: odd_val = float(odd_atual_str)
                        except: odd_val = 0.0
                        
                        destaque_odd = ""
                        if odd_val >= 1.80:
                            destaque_odd = "\n💎 <b>SUPER ODD DETECTADA! (EV+)</b>"
                            st.session_state['alertas_enviados'].add(uid_super)
                        
                        opiniao_txt = ""; opiniao_db = "Neutro"
                        if IA_ATIVADA:
                            try:
                                time.sleep(0.3)
                                dados_ia = {'jogo': f"{home} x {away}", 'placar': placar, 'tempo': f"{tempo}'"}
                                opiniao_txt = consultar_ia_gemini(dados_ia, s['tag'], stats)
                                if "Aprovado" in opiniao_txt: opiniao_db = "Aprovado"
                                elif "Arriscado" in opiniao_txt: opiniao_db = "Arriscado"
                            except: pass
                        
                        item = {"FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Placar_Sinal": placar, "Estrategia": s['tag'], "Resultado": "Pendente", "HomeID": str(j['teams']['home']['id']) if lid in ids_safe else "", "AwayID": str(j['teams']['away']['id']) if lid in ids_safe else "", "Odd": odd_atual_str, "Odd_Atualizada": "", "Opiniao_IA": opiniao_db}
                        
                        if adicionar_historico(item):
                            prob = buscar_inteligencia(s['tag'], j['league']['name'], f"{home} x {away}")
                            msg = f"<b>🚨 SINAL ENCONTRADO 🚨</b>\n\n🏆 <b>{j['league']['name']}</b>\n⚽ {home} 🆚 {away}\n⏰ <b>{tempo}' minutos</b> (Placar: {placar})\n\n🔥 {s['tag'].upper()}\n⚠️ <b>AÇÃO:</b> {s['ordem']}{destaque_odd}\n\n💰 <b>Odd: @{odd_atual_str}</b>{txt_pressao}\n📊 <i>Dados: {s['stats']}</i>\n⚽ <b>Médias (10j):</b> Casa {medias_gols['home']} | Fora {medias_gols['away']}{prob}{opiniao_txt}"
                            enviar_telegram(safe_token, safe_chat, msg)
                            st.toast(f"Sinal Enviado: {s['tag']}")
                        elif uid_super not in st.session_state['alertas_enviados'] and odd_val >= 1.80:
                             st.session_state['alertas_enviados'].add(uid_super)
                             msg_super = (f"💎 <b>OPORTUNIDADE DE VALOR!</b>\n\n⚽ {home} 🆚 {away}\n📈 <b>A Odd subiu!</b> Entrada valorizada.\n🔥 <b>Estratégia:</b> {s['tag']}\n💰 <b>Nova Odd: @{odd_atual_str}</b>\n<i>O jogo mantém o padrão da estratégia.</i>{txt_pressao}")
                             enviar_telegram(safe_token, safe_chat, msg_super)
                
                radar.append({"Liga": nome_liga_show, "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})
            
            if candidatos_multipla:
                novos = [c for c in candidatos_multipla if c['fid'] not in st.session_state['multiplas_enviadas']]
                if novos:
                    msg = "<b>🚀 OPORTUNIDADE DE MÚLTIPLA (HT) 🚀</b>\n" + "".join([f"\n⚽ {c['jogo']} ({c['stats']})\n⚠️ AÇÃO: {c['indica']}" for c in novos])
                    for c in novos: st.session_state['multiplas_enviadas'].add(c['fid'])
                    enviar_telegram(safe_token, safe_chat, msg)
            
            for p in prox:
                try:
                    if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] in ['NS', 'TBD'] and p['fixture']['id'] not in ids_no_radar:
                        if datetime.fromisoformat(p['fixture']['date']) > agora:
                            l_id = normalizar_id(p['league']['id']); l_nm = p['league']['name']
                            if l_id in ids_safe: l_nm += " 🛡️"
                            elif l_id in df_obs['id'].values: l_nm += " ⚠️"
                            agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": l_nm, "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
                except: pass
        
        if st.session_state.get('precisa_salvar'):
            if 'historico_full' in st.session_state and not st.session_state['historico_full'].empty:
                st.caption("⏳ Sincronizando dados pendentes...")
                salvar_aba("Historico", st.session_state['historico_full'])
        
        if api_error: st.markdown('<div class="status-error">🚨 API LIMITADA - AGUARDE</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        hist_hj = pd.DataFrame(st.session_state['historico_sinais'])
        t, g, r, w = calcular_stats(hist_hj)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t}</div><div class="metric-sub">{g} Green | {r} Red</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(radar)}</div><div class="metric-sub">Monitorando</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Ligas Seguras</div><div class="metric-value">{count_safe}</div><div class="metric-sub">Validadas</div></div>', unsafe_allow_html=True)
        
        st.write("")
        abas = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", f"💰 Financeiro", f"📜 Histórico ({len(hist_hj)})", "📈 BI & Analytics", f"🚫 Blacklist ({len(st.session_state['df_black'])})", f"🛡️ Seguras ({count_safe})", f"⚠️ Obs ({count_obs})", "💾 Big Data (Firebase)"])
        
        with abas[0]: 
            if radar: st.dataframe(pd.DataFrame(radar)[['Liga', 'Jogo', 'Tempo', 'Status']].astype(str), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        
        with abas[1]: 
            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos futuros hoje.")
        
        with abas[2]:
            st.markdown("### 💰 Evolução Financeira")
            c_fin1, c_fin2 = st.columns(2)
            stake_padrao = c_fin1.number_input("Valor da Aposta (Stake):", value=st.session_state.get('stake_padrao', 10.0), step=5.0)
            banca_inicial = c_fin2.number_input("Banca Inicial:", value=st.session_state.get('banca_inicial', 100.0), step=50.0)
            st.session_state['stake_padrao'] = stake_padrao; st.session_state['banca_inicial'] = banca_inicial
            modo_simulacao = st.radio("Cenário de Entrada:", ["Todos os sinais", "Apenas 1 sinal por jogo", "Até 2 sinais por jogo"], horizontal=True)
            filtrar_ia = st.checkbox("🤖 Somente Sinais APROVADOS pela IA")
            df_fin = st.session_state.get('historico_full', pd.DataFrame())
            if not df_fin.empty:
                df_fin = df_fin.copy()
                df_fin['Odd_Calc'] = df_fin.apply(lambda row: obter_odd_final_para_calculo(row['Odd'], row['Estrategia']), axis=1)
                df_fin = df_fin[df_fin['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
                df_fin = df_fin.sort_values(by=['FID', 'Hora'], ascending=[True, True])
                if filtrar_ia and 'Opiniao_IA' in df_fin.columns: df_fin = df_fin[df_fin['Opiniao_IA'] == 'Aprovado']
                if modo_simulacao == "Apenas 1 sinal por jogo": df_fin = df_fin.groupby('FID').head(1)
                elif modo_simulacao == "Até 2 sinais por jogo": df_fin = df_fin.groupby('FID').head(2)
                if not df_fin.empty:
                    lucros = []; saldo_atual = banca_inicial; historico_saldo = [banca_inicial]; qtd_greens = 0; qtd_reds = 0
                    for idx, row in df_fin.iterrows():
                        res = row['Resultado']; odd = row['Odd_Calc']
                        if 'GREEN' in res: lucro = (stake_padrao * odd) - stake_padrao; qtd_greens += 1
                        else: lucro = -stake_padrao; qtd_reds += 1
                        saldo_atual += lucro; lucros.append(lucro); historico_saldo.append(saldo_atual)
                    df_fin['Lucro'] = lucros; total_lucro = sum(lucros)
                    roi = (total_lucro / (len(df_fin) * stake_padrao)) * 100
                    st.session_state['last_fin_stats'] = {'cenario': modo_simulacao, 'lucro': total_lucro, 'roi': roi, 'entradas': len(df_fin)}
                    m1, m2, m3, m4 = st.columns(4)
                    cor_delta = "normal" if total_lucro >= 0 else "inverse"
                    m1.metric("Banca Atual", f"R$ {saldo_atual:.2f}")
                    m2.metric("Lucro Líquido", f"R$ {total_lucro:.2f}", delta=f"{roi:.1f}%", delta_color=cor_delta)
                    m3.metric("Entradas", len(df_fin))
                    m4.metric("Winrate", f"{(qtd_greens/len(df_fin)*100):.1f}%")
                    fig_fin = px.line(y=historico_saldo, x=range(len(historico_saldo)), title="Crescimento da Banca (Realista)")
                    fig_fin.update_layout(xaxis_title="Entradas", yaxis_title="Saldo (R$)", template="plotly_dark")
                    fig_fin.add_hline(y=banca_inicial, line_dash="dot", annotation_text="Início", line_color="gray")
                    st.plotly_chart(fig_fin, use_container_width=True)
                else: st.info("Aguardando fechamento de sinais.")
            else: st.info("Sem dados históricos.")

        with abas[3]: 
            if not hist_hj.empty: 
                df_show = hist_hj.copy()
                if 'Jogo' in df_show.columns and 'Placar_Sinal' in df_show.columns: df_show['Jogo'] = df_show['Jogo'] + " (" + df_show['Placar_Sinal'].astype(str) + ")"
                colunas_esconder = ['FID', 'HomeID', 'AwayID', 'Data_Str', 'Data_DT', 'Odd_Atualizada', 'Placar_Sinal']
                cols_view = [c for c in df_show.columns if c not in colunas_esconder]
                st.dataframe(df_show[cols_view], use_container_width=True, hide_index=True)
            else: st.caption("Vazio.")

        with abas[4]: 
            st.markdown("### 📊 Inteligência de Mercado")
            df_bi = st.session_state.get('historico_full', pd.DataFrame())
            if df_bi.empty: st.warning("Sem dados históricos.")
            else:
                try:
                    df_bi = df_bi.copy()
                    df_bi['Data_Str'] = df_bi['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
                    df_bi['Data_DT'] = pd.to_datetime(df_bi['Data_Str'], errors='coerce')
                    df_bi = df_bi.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
                    hoje = pd.to_datetime(get_time_br().date())
                    d_hoje = df_bi[df_bi['Data_DT'] == hoje]; d_7d = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=7))]; d_30d = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=30))]; d_total = df_bi
                    def fmt_placar(d):
                        if d.empty: return "0G - 0R (0%)"
                        g = d['Resultado'].str.contains('GREEN', na=False).sum(); r = d['Resultado'].str.contains('RED', na=False).sum(); t = g + r; wr = (g/t*100) if t > 0 else 0
                        return f"{g}G - {r}R ({wr:.0f}%)"
                    def fmt_ia_stats(periodo_df, label_periodo):
                        if 'Opiniao_IA' not in periodo_df.columns: return ""
                        d_fin = periodo_df[periodo_df['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        stats_aprov = fmt_placar(d_fin[d_fin['Opiniao_IA'] == 'Aprovado'])
                        stats_risk = fmt_placar(d_fin[d_fin['Opiniao_IA'] == 'Arriscado'])
                        return f"🤖 IA ({label_periodo}):\n👍 Aprovados: {stats_aprov}\n⚠️ Arriscados: {stats_risk}"
                    
                    msg_ia_hoje = fmt_ia_stats(d_hoje, "Hoje"); msg_ia_7d = fmt_ia_stats(d_7d, "7 Dias"); msg_ia_30d = fmt_ia_stats(d_30d, "30 Dias"); msg_ia_total = fmt_ia_stats(d_total, "Geral")
                    if 'bi_filter' not in st.session_state: st.session_state['bi_filter'] = "Tudo"
                    filtro = st.selectbox("📅 Período", ["Tudo", "Hoje", "7 Dias", "30 Dias"], key="bi_select")
                    if filtro == "Hoje": df_show = d_hoje
                    elif filtro == "7 Dias": df_show = d_7d
                    elif filtro == "30 Dias": df_show = d_30d
                    else: df_show = df_bi 
                    
                    if not df_show.empty:
                        gr = df_show['Resultado'].str.contains('GREEN').sum(); rd = df_show['Resultado'].str.contains('RED').sum(); tt = len(df_show); ww = (gr/tt*100) if tt>0 else 0
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Sinais", tt); m2.metric("Greens", gr); m3.metric("Reds", rd); m4.metric("Assertividade", f"{ww:.1f}%")
                        st.divider()
                        st.markdown("### 🏆 Melhores e Piores Ligas (Com Drill-Down)")
                        df_finished = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        if not df_finished.empty:
                            stats_ligas = df_finished.groupby('Liga')['Resultado'].apply(lambda x: pd.Series({'Winrate': (x.str.contains('GREEN').sum() / len(x) * 100), 'Total': len(x), 'Reds': x.str.contains('RED').sum(), 'Greens': x.str.contains('GREEN').sum()})).unstack()
                            stats_ligas = stats_ligas[stats_ligas['Total'] >= 2]
                            col_top, col_worst = st.columns(2)
                            with col_top:
                                st.caption("🌟 Top Ligas (Mais Lucrativas)")
                                top_ligas = stats_ligas.sort_values(by=['Winrate', 'Total'], ascending=[False, False]).head(10)
                                st.dataframe(top_ligas[['Winrate', 'Total', 'Greens']].style.format({'Winrate': '{:.2f}%', 'Total': '{:.0f}', 'Greens': '{:.0f}'}), use_container_width=True)
                            with col_worst:
                                st.caption("💀 Ligas Críticas")
                                worst_ligas = stats_ligas.sort_values(by=['Reds'], ascending=False).head(10)
                                dados_drill = []
                                for liga, row in worst_ligas.iterrows():
                                    if row['Reds'] > 0:
                                        erros_liga = df_finished[(df_finished['Liga'] == liga) & (df_finished['Resultado'].str.contains('RED'))]
                                        pior_strat = erros_liga['Estrategia'].value_counts().head(1)
                                        nome_strat = pior_strat.index[0] if not pior_strat.empty else "-"
                                        dados_drill.append({"Liga": liga, "Total Reds": int(row['Reds']), "Pior Estratégia": nome_strat})
                                if dados_drill: st.dataframe(pd.DataFrame(dados_drill), use_container_width=True, hide_index=True)
                                else: st.success("Nenhuma liga com Reds significativos.")
                        st.divider()
                        st.markdown("### 🧠 Auditoria da IA (Aprovações vs Resultado)")
                        if 'Opiniao_IA' in df_show.columns:
                            df_audit = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                            if not df_audit.empty:
                                pivot = pd.crosstab(df_audit['Opiniao_IA'], df_audit['Resultado'], margins=True)
                                if '✅ GREEN' in pivot.columns and 'All' in pivot.columns: pivot['Winrate %'] = (pivot['✅ GREEN'] / pivot['All'] * 100)
                                format_dict = {'Winrate %': '{:.2f}%'}
                                for col in pivot.columns:
                                    if col != 'Winrate %': format_dict[col] = '{:.0f}'
                                st.dataframe(pivot.style.format(format_dict, na_rep="-").highlight_max(axis=0, color='#1F4025'), use_container_width=True)
                        st.markdown("### 📈 Performance por Estratégia")
                        st_s = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        if not st_s.empty:
                            resumo_strat = st_s.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)
                            if '✅ GREEN' in resumo_strat.columns and '❌ RED' in resumo_strat.columns:
                                resumo_strat['Winrate'] = (resumo_strat['✅ GREEN'] / (resumo_strat['✅ GREEN'] + resumo_strat['❌ RED']) * 100)
                                format_strat = {'Winrate': '{:.2f}%'}
                                for c in resumo_strat.columns:
                                    if c != 'Winrate': format_strat[c] = '{:.0f}'
                                st.dataframe(resumo_strat.sort_values('Winrate', ascending=False).style.format(format_strat), use_container_width=True)
                            cts = st_s.groupby(['Estrategia', 'Resultado']).size().reset_index(name='Qtd')
                            fig = px.bar(cts, x='Estrategia', y='Qtd', color='Resultado', color_discrete_map={'✅ GREEN': '#00FF00', '❌ RED': '#FF0000'}, title="Volume de Sinais", text='Qtd')
                            fig.update_layout(template="plotly_dark"); st.plotly_chart(fig, use_container_width=True)
                except Exception as e: st.error(f"Erro BI: {e}")

        with abas[5]: st.dataframe(st.session_state['df_black'][['País', 'Liga', 'Motivo']], use_container_width=True, hide_index=True)
        
        with abas[6]: 
            df_safe_show = st.session_state.get('df_safe', pd.DataFrame()).copy()
            if not df_safe_show.empty:
                def calc_risco(x):
                    try: v = int(float(str(x)))
                    except: v = 0
                    return "🟢 100% Estável" if v == 0 else f"⚠️ Atenção ({v}/10)"
                df_safe_show['Status Risco'] = df_safe_show['Strikes'].apply(calc_risco)
                st.dataframe(df_safe_show[['País', 'Liga', 'Motivo', 'Status Risco']], use_container_width=True, hide_index=True)
            else: st.info("Nenhuma liga segura ainda.")

        with abas[7]: 
            df_vip_show = st.session_state.get('df_vip', pd.DataFrame()).copy()
            if not df_vip_show.empty: df_vip_show['Strikes'] = df_vip_show['Strikes'].apply(formatar_inteiro_visual)
            st.dataframe(df_vip_show[['País', 'Liga', 'Data_Erro', 'Strikes']], use_container_width=True, hide_index=True)

        with abas[8]:
            st.markdown(f"### 💾 Banco de Dados de Partidas (Firebase)")
            st.caption("A IA usa esses dados para criar novas estratégias. Os dados são salvos na nuvem.")
            if db_firestore:
                col_fb1, col_fb2 = st.columns([1, 3])
                if col_fb1.button("🔄 Carregar/Atualizar Tabela"):
                    try:
                        with st.spinner("Baixando dados do Firebase..."):
                            docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(50).stream()
                            data = [d.to_dict() for d in docs]
                            st.session_state['cache_firebase_view'] = data 
                            st.toast(f"Dados atualizados! ({len(data)} jogos)")
                    except Exception as e: st.error(f"Erro ao ler Firebase: {e}")
                if 'cache_firebase_view' in st.session_state and st.session_state['cache_firebase_view']:
                    st.success(f"📂 Visualizando {len(st.session_state['cache_firebase_view'])} registros (Cache Local)")
                    st.dataframe(pd.DataFrame(st.session_state['cache_firebase_view']), use_container_width=True)
                else: st.info("ℹ️ Clique no botão acima para visualizar os dados salvos (Isso consome leituras da cota).")
            else: st.warning("⚠️ Firebase não conectado.")

        for i in range(INTERVALO, 0, -1):
            st.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()
else:
    with placeholder_root.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera. Configure na lateral.")
def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    df_raw = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

@st.cache_data(ttl=86400)
def buscar_ranking(api_key, league_id, season):
    try:
        url = "https://v3.football.api-sports.io/standings"
        params = {"league": league_id, "season": season}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        ranking = {}
        if res.get('response'):
            for team in res['response'][0]['league']['standings'][0]: ranking[team['team']['name']] = team['rank']
        return ranking
    except: return {}

@st.cache_data(ttl=120) 
def buscar_agenda_cached(api_key, date_str):
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        return requests.get(url, headers={"x-apisports-key": api_key}, params={"date": date_str, "timezone": "America/Sao_Paulo"}).json().get('response', [])
    except: return []

def get_live_odds(fixture_id, api_key, strategy_name, total_gols_atual=0):
    try:
        url = "https://v3.football.api-sports.io/odds/live"
        params = {"fixture": fixture_id}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        target_markets = []
        target_line = 0.0
        
        if "Relâmpago" in strategy_name and total_gols_atual == 0:
            target_markets = ["1st half", "first half"]; target_line = 0.5
        elif "Golden" in strategy_name and total_gols_atual == 1:
            target_markets = ["match goals", "goals over/under"]; target_line = 1.5
        else:
            ht_strategies = ["Relâmpago", "Massacre", "Choque", "Briga", "Morno"]
            is_ht = any(x in strategy_name for x in ht_strategies)
            target_markets = ["1st half", "first half"] if is_ht else ["match goals", "goals over/under"]
            target_line = total_gols_atual + 0.5
        
        best_odd = "0.00"
        if res.get('response'):
            markets = res['response'][0]['odds']
            for m in markets:
                m_name = m['name'].lower()
                if any(tm in m_name for tm in target_markets) and "over" in m_name:
                    for v in m['values']:
                        try:
                            line_raw = str(v['value']).lower().replace("over", "").strip()
                            line_val = float(''.join(c for c in line_raw if c.isdigit() or c == '.'))
                            if abs(line_val - target_line) < 0.1:
                                raw_odd = float(v['odd'])
                                if raw_odd > 50: raw_odd = raw_odd / 1000
                                return "{:.2f}".format(raw_odd)
                        except: pass
        return "1.20" 
    except: return "1.20"

def buscar_inteligencia(estrategia, liga, jogo):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "\n🔮 <b>Prob: Sem Histórico</b>"
    try:
        times = jogo.split(' x ')
        time_casa = times[0].split('(')[0].strip()
        time_visitante = times[1].split('(')[0].strip()
    except: return "\n🔮 <b>Prob: Erro Nome</b>"
    
    numerador = 0; denominador = 0; fontes = []
    f_casa = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_casa, na=False))]
    f_vis = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_visitante, na=False))]
    
    if len(f_casa) >= 3 or len(f_vis) >= 3:
        wr_c = (f_casa['Resultado'].str.contains('GREEN').sum()/len(f_casa)*100) if len(f_casa)>=3 else 0
        wr_v = (f_vis['Resultado'].str.contains('GREEN').sum()/len(f_vis)*100) if len(f_vis)>=3 else 0
        div = 2 if (len(f_casa)>=3 and len(f_vis)>=3) else 1
        numerador += ((wr_c + wr_v)/div) * 5; denominador += 5; fontes.append("Time")

    f_liga = df[(df['Estrategia'] == estrategia) & (df['Liga'] == liga)]
    if len(f_liga) >= 3:
        wr_l = (f_liga['Resultado'].str.contains('GREEN').sum()/len(f_liga)*100)
        numerador += wr_l * 3; denominador += 3; fontes.append("Liga")
    
    f_geral = df[df['Estrategia'] == estrategia]
    if len(f_geral) >= 1:
        wr_g = (f_geral['Resultado'].str.contains('GREEN').sum()/len(f_geral)*100)
        numerador += wr_g * 1; denominador += 1
        
    if denominador == 0: return "\n🔮 <b>Prob: Calculando...</b>"
    prob_final = numerador / denominador
    str_fontes = "+".join(fontes) if fontes else "Geral"
    return f"\n{'🔥' if prob_final >= 80 else '🔮' if prob_final > 40 else '⚠️'} <b>Prob: {prob_final:.0f}% ({str_fontes})</b>"

def calcular_odd_media_historica(estrategia):
    df = st.session_state.get('historico_full', pd.DataFrame())
    PADRAO_CONSERVADOR = 1.20 
    TETO_MAXIMO_FALLBACK = 1.50 
    
    if df.empty: return PADRAO_CONSERVADOR
    try:
        df_strat = df[df['Estrategia'] == estrategia].copy()
        df_strat['Odd_Num'] = pd.to_numeric(df_strat['Odd'], errors='coerce')
        df_validas = df_strat[(df_strat['Odd_Num'] > 1.15) & (df_strat['Odd_Num'] < 50.0)]
        if df_validas.empty: return PADRAO_CONSERVADOR
        media = df_validas['Odd_Num'].mean()
        media_segura = min(media, TETO_MAXIMO_FALLBACK)
        if media_segura < 1.15: return PADRAO_CONSERVADOR
        return float(f"{media_segura:.2f}")
    except: return PADRAO_CONSERVADOR

def obter_odd_final_para_calculo(odd_registro, estrategia):
    try:
        valor = float(odd_registro)
        if valor <= 1.15: return calcular_odd_media_historica(estrategia)
        return valor
    except: return calcular_odd_media_historica(estrategia)

# --- IA ATUALIZADA (VERSÃO CAÇADOR DE GOLS) ---
def consultar_ia_gemini(dados_jogo, estrategia, stats_raw):
    if not IA_ATIVADA: return ""
    
    dados_ricos = extrair_dados_completos(stats_raw)
    nome_strat = estrategia.upper()
    
    # Histórico (Contexto de Winrate)
    df = st.session_state.get('historico_full', pd.DataFrame())
    resumo_historico = ""
    if not df.empty:
        df_strat = df[df['Estrategia'] == estrategia]
        if len(df_strat) > 0:
            greens = len(df_strat[df_strat['Resultado'].str.contains('GREEN', na=False)])
            winrate = (greens / len(df_strat) * 100)
            resumo_historico = f"Winrate histórico no bot: {winrate:.0f}%."

    # --- REGRA GLOBAL DE FORMATO ---
    regra_texto = "Responda SEM formatação (sem negrito, sem asteriscos). Seja direto."

    # --- PERFIL 1: ESTRATÉGIAS DE UNDER (JOGO MORNO) ---
    # Aqui a lógica é INVERSA: Queremos jogo ruim.
    if "MORNO" in nome_strat or "UNDER" in nome_strat:
        prompt = f"""
        Atue como Especialista em UNDER GOLS.
        Cenário: {dados_jogo['jogo']} | Stats: {dados_ricos}
        
        Sua Missão: Validar se o jogo vai continuar 0x0 ou com poucos gols.
        Critério: Se tiver muitos chutes (mesmo pra fora) ou jogo aberto, REJEITE (Arriscado).
        APROVE apenas se: Ataques inofensivos, bola presa no meio, times fracos.
        
        {regra_texto}
        Responda: [Aprovado/Arriscado] - [Motivo curto]
        """

    # --- PERFIL 2: ESTRATÉGIAS DE OVER (TODAS AS OUTRAS) ---
    # Golden, Blitz, Relâmpago, Massacre, Choque, etc.
    # MENTALIDADE: Não importa quem ganha. Importa se a bola vai entrar.
    else:
        # Contexto específico de tempo para ajudar a IA
        contexto_tempo = ""
        if "GOLDEN" in nome_strat or "JANELA" in nome_strat:
            contexto_tempo = "FIM DE JOGO (80min+). O time que perde precisa se expor. Cuidado apenas com 'Pressão Falsa' (chutes de longe)."
        elif any(x in nome_strat for x in ["RELÂMPAGO", "CHOQUE", "BRIGA"]):
            contexto_tempo = "INÍCIO DE JOGO. Buscamos intensidade imediata e defesas desatentas."
        else:
            contexto_tempo = "MEIO DO JOGO. Buscamos jogo aberto, trocação ou pressão forte."

        prompt = f"""
        Atue como um PROFISSIONAL DE OVER GOLS (Caçador de Gols).
        Estratégia: {estrategia} ({contexto_tempo})
        Dados: {dados_jogo['jogo']} | Placar: {dados_jogo['placar']}
        Stats: {dados_ricos} | {resumo_historico}
        
        REGRA DE OURO (VISÃO DE APOSTADOR):
        1. NÃO ANALISE QUEM VAI GANHAR. Analise se vai sair GOL.
        2. Defesa Ruim = APROVADO. Se o favorito ataca mal e toma contra-ataque, isso é ÓTIMO para gols.
        3. Jogo "Lá e Cá" (Trocação) = APROVADO.
        4. Favorito Perdendo = APROVADO (Vai se lançar ao ataque e deixar espaço).
        
        QUANDO REJEITAR (Arriscado):
        - Apenas se o jogo estiver TRAVADO, LENTO ou com times medrosos que não chutam.
        - No Fim de Jogo (Golden), rejeite se for só "chuveirinho" sem direção (Chutes fora >>> Chutes no gol).
        
        {regra_texto}
        Responda: [Aprovado/Arriscado] - [Motivo focado em chance de gol]
        """

    # --- EXECUÇÃO ---
    try:
        response = model_ia.generate_content(prompt, request_options={"timeout": 12})
        st.session_state['gemini_usage']['used'] += 1
        
        # Limpeza total da resposta
        texto_raw = response.text.strip()
        texto_limpo = texto_raw.replace("**", "").replace("*", "").replace("Motivo:", "").replace("Here's", "")
        
        # Veredito Inteligente
        veredicto = "Arriscado"
        if "Aprovado" in texto_limpo or "aprovado" in texto_limpo: veredicto = "Aprovado"
        
        # Extração do motivo
        motivo = texto_limpo
        for divisor in ["-", ":", "\n"]:
            if divisor in texto_limpo:
                partes = texto_limpo.split(divisor, 1)
                if len(partes) > 1: motivo = partes[1].strip(); break
        
        motivo = motivo.replace("Aprovado", "").replace("Arriscado", "").strip()
        emoji = "✅" if veredicto == "Aprovado" else "⚠️"
        
        return f"\n🤖 <b>IA:</b> {emoji} <b>{veredicto}</b> - {motivo[:130]}" 
        
    except Exception as e:
        if "429" in str(e): return "\n🤖 <b>IA:</b> (Ocupada)"
        return ""

def analisar_bi_com_ia():
    if not IA_ATIVADA: return "IA Desconectada."
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados."
    try:
        hoje_str = get_time_br().strftime('%Y-%m-%d')
        df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
        df_hoje = df[df['Data_Str'] == hoje_str]
        if df_hoje.empty: return "Sem sinais hoje."
        df_f = df_hoje[df_hoje['Resultado'].isin(['✅ GREEN', '❌ RED'])]
        total = len(df_f); greens = len(df_f[df_f['Resultado'].str.contains('GREEN')])
        resumo = df_f.groupby('Estrategia')['Resultado'].apply(lambda x: f"{(x.str.contains('GREEN').sum()/len(x)*100):.1f}%").to_dict()
        prompt = f"""
        Analise o dia ({hoje_str}):
        Total: {total}, Greens: {greens}
        Estratégias: {json.dumps(resumo, ensure_ascii=False)}
        3 dicas curtas para amanhã.
        """
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro BI: {e}"

def analisar_financeiro_com_ia(stake, banca):
    if not IA_ATIVADA: return "IA Desconectada."
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados."
    try:
        hoje_str = get_time_br().strftime('%Y-%m-%d')
        df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
        df_hoje = df[df['Data_Str'] == hoje_str].copy()
        if df_hoje.empty: return "Sem operações hoje."
        lucro_total = 0.0; investido = 0.0; qtd=0; odds_greens = []
        for _, row in df_hoje.iterrows():
            res = str(row['Resultado'])
            odd_final = obter_odd_final_para_calculo(row['Odd'], row['Estrategia'])
            if 'GREEN' in res:
                lucro_total += (stake * odd_final) - stake; investido += stake
                if odd_final > 1: odds_greens.append(odd_final)
            elif 'RED' in res:
                lucro_total -= stake; investido += stake
        roi = (lucro_total / investido * 100) if investido > 0 else 0
        odd_media = (sum(odds_greens)/len(odds_greens)) if odds_greens else 0
        prompt_fin = f"""
        Gestor Financeiro. Dia:
        - Banca Ini: R$ {banca:.2f} | Fim: R$ {banca+lucro_total:.2f}
        - Stake: R$ {stake:.2f} | Entradas: {qtd}
        - Lucro: R$ {lucro_total:.2f} | ROI: {roi:.2f}% | Odd Média: {odd_media:.2f}
        Feedback curto sobre saúde financeira.
        """
        response = model_ia.generate_content(prompt_fin)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro Fin: {e}"

def criar_estrategia_nova_ia():
    if not IA_ATIVADA: return "IA Desconectada."
    if db_firestore:
        try:
            docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(100).stream()
            data = [d.to_dict() for d in docs]
            if len(data) < 5: return "Coletando dados no Firebase... Aguarde mais jogos."
            amostra = json.dumps(data) 
        except: return "Erro ao ler Firebase."
    else: return "Firebase Offline."

    try:
        prompt_criacao = f"""
        Cientista de Dados. Analise CSV (JSON): {amostra}
        MISSÃO: Padrão ESTATÍSTICO GLOBAL lucrativo (Cantos, Cartões).
        Saída: Nome, Regra e Lógica.
        """
        response = model_ia.generate_content(prompt_criacao)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro na criação: {e}"

def otimizar_estrategias_existentes_ia():
    if not IA_ATIVADA: return "⚠️ IA Desconectada."
    if not db_firestore: return "⚠️ Firebase Offline (Necessário para cruzar dados)."
    df_hist = st.session_state.get('historico_full', pd.DataFrame())
    if df_hist.empty: return "Sem histórico suficiente para análise."
    df_closed = df_hist[df_hist['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
    if df_closed.empty: return "Nenhum sinal finalizado para avaliar."
    try:
        docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(200).stream()
        big_data_dict = {d.to_dict()['fid']: d.to_dict() for d in docs}
    except Exception as e: return f"Erro ao ler Firebase: {e}"
    analise_pacote = {}
    estrategias_unicas = df_closed['Estrategia'].unique()
    for strat in estrategias_unicas:
        if strat not in MAPA_LOGICA_ESTRATEGIAS: continue
        d_strat = df_closed[df_closed['Estrategia'] == strat]
        total = len(d_strat)
        if total < 5: continue 
        greens = d_strat[d_strat['Resultado'].str.contains('GREEN')]
        reds = d_strat[d_strat['Resultado'].str.contains('RED')]
        winrate = (len(greens) / total) * 100
        stats_reds = []
        for fid in reds['FID'].values:
            fid_str = str(fid)
            if fid_str in big_data_dict:
                stats_reds.append(big_data_dict[fid_str].get('estatisticas', {}))
        if stats_reds:
            try:
                avg_chutes = sum([x.get('chutes_total', 0) for x in stats_reds]) / len(stats_reds)
                avg_posse = sum([int(x.get('posse_casa', '0').replace('%','')) for x in stats_reds]) / len(stats_reds)
            except: avg_chutes = 0; avg_posse = 0
            analise_pacote[strat] = {
                "Winrate Atual": f"{winrate:.1f}%",
                "Regra Atual": MAPA_LOGICA_ESTRATEGIAS[strat],
                "Total Entradas": total,
                "Qtd Reds": len(reds),
                "Perfil dos Jogos que deram RED (Médias)": {
                    "Chutes Totais no jogo": f"{avg_chutes:.1f}",
                    "Posse Casa": f"{avg_posse:.1f}%"
                }
            }
    if not analise_pacote: return "Dados insuficientes (cruzamento Sheets x Firebase) para gerar insights."
    prompt_otimizacao = f"""
    Atue como um Especialista em Data Science focado em Apostas Esportivas.
    SEU OBJETIVO: Analisar os dados dos erros e sugerir UMA melhoria na lógica.
    DADOS: {json.dumps(analise_pacote, indent=2, ensure_ascii=False)}
    SAÍDA ESPERADA: Nome, Diagnóstico e AÇÃO SUGERIDA (Mudança de Parâmetro).
    """
    try:
        response = model_ia.generate_content(prompt_otimizacao)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro na IA: {e}"

def _worker_telegram(token, chat_id, msg):
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=5)
    except: pass

def _worker_telegram_photo(token, chat_id, photo_buffer, caption):
    try:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        files = {'photo': photo_buffer}
        data = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'HTML'}
        res = requests.post(url, files=files, data=data, timeout=15)
        if res.status_code != 200: st.error(f"Erro Telegram Foto ({res.status_code}): {res.text}")
    except Exception as e: st.error(f"Erro envio foto: {e}")

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        t = threading.Thread(target=_worker_telegram, args=(token, cid, msg))
        t.daemon = True; t.start()

def enviar_analise_estrategia(token, chat_ids):
    sugestao = criar_estrategia_nova_ia()
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    msg = f"🧪 <b>LABORATÓRIO DE ESTRATÉGIAS (IA)</b>\n\n{sugestao}"
    for cid in ids: enviar_telegram(token, cid, msg)

def enviar_relatorio_financeiro(token, chat_ids, cenario, lucro, roi, entradas):
    msg = f"💰 <b>RELATÓRIO FINANCEIRO</b>\n\n📊 <b>Cenário:</b> {cenario}\n💵 <b>Lucro Líquido:</b> R$ {lucro:.2f}\n📈 <b>ROI:</b> {roi:.1f}%\n🎟️ <b>Entradas:</b> {entradas}\n\n<i>Cálculo baseado na gestão configurada.</i>"
    enviar_telegram(token, chat_ids, msg)

def enviar_relatorio_bi(token, chat_ids):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    try:
        df = df.copy()
        df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
        df['Data_DT'] = pd.to_datetime(df['Data_Str'], errors='coerce')
        df = df.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
        hoje = pd.to_datetime(get_time_br().date())
        d_hoje = df[df['Data_DT'] == hoje]
        d_7d = df[df['Data_DT'] >= (hoje - timedelta(days=7))]
        d_30d = df[df['Data_DT'] >= (hoje - timedelta(days=30))]
        d_total = df
        
        def fmt_placar(d):
            if d.empty: return "0G - 0R (0%)"
            g = d['Resultado'].str.contains('GREEN', na=False).sum()
            r = d['Resultado'].str.contains('RED', na=False).sum()
            t = g + r
            wr = (g/t*100) if t > 0 else 0
            return f"{g}G - {r}R ({wr:.0f}%)"

        insight_text = analisar_bi_com_ia()
        msg_texto = f"""📈 <b>RELATÓRIO BI AVANÇADO</b>\n📆 <b>HOJE:</b> {fmt_placar(d_hoje)}\n🗓 <b>SEMANA:</b> {fmt_placar(d_7d)}\n📅 <b>MÊS (30d):</b> {fmt_placar(d_30d)}\n♾ <b>TOTAL:</b> {fmt_placar(d_total)}\n\n🧠 <b>INSIGHT IA:</b>\n{insight_text}"""
        enviar_telegram(token, chat_ids, msg_texto)
    except Exception as e: st.error(f"Erro ao gerar BI: {e}")

def verificar_automacao_bi(token, chat_ids, stake_padrao):
    agora = get_time_br()
    hoje_str = agora.strftime('%Y-%m-%d')
    if st.session_state['last_check_date'] != hoje_str:
        st.session_state['bi_enviado'] = False
        st.session_state['ia_enviada'] = False
        st.session_state['financeiro_enviado'] = False
        st.session_state['bigdata_enviado'] = False
        st.session_state['matinal_enviado'] = False
        st.session_state['last_check_date'] = hoje_str
    if agora.hour == 23 and agora.minute >= 30 and not st.session_state['bi_enviado']:
        enviar_relatorio_bi(token, chat_ids)
        st.session_state['bi_enviado'] = True
    if agora.hour == 23 and agora.minute >= 40 and not st.session_state['financeiro_enviado']:
        analise_fin = analisar_financeiro_com_ia(stake_padrao, st.session_state.get('banca_inicial', 100))
        msg_fin = f"💰 <b>CONSULTORIA FINANCEIRA</b>\n\n{analise_fin}"
        enviar_telegram(token, chat_ids, msg_fin)
        st.session_state['financeiro_enviado'] = True
    if agora.hour == 23 and agora.minute >= 55 and not st.session_state['bigdata_enviado']:
        enviar_analise_estrategia(token, chat_ids)
        st.session_state['bigdata_enviado'] = True

def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br()
    if 8 <= agora.hour < 11 and not st.session_state['matinal_enviado']:
        insights = gerar_insights_matinais_ia(api_key)
        if insights and "Sem insights" not in insights:
            ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
            msg_final = f"🌅 <b>INSIGHTS MATINAIS (IA + API)</b>\n\n{insights}"
            for cid in ids: enviar_telegram(token, cid, msg_final)
            st.session_state['matinal_enviado'] = True

def gerar_insights_matinais_ia(api_key):
    if not IA_ATIVADA: return "IA Offline."
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        LIGAS_TOP = [39, 140, 78, 135, 61, 71, 72, 2, 3]
        jogos_top = [j for j in jogos if j['league']['id'] in LIGAS_TOP][:6] 
        if not jogos_top: return "Nenhum jogo 'Top Tier' para análise matinal hoje."
        relatorio_final = ""
        for j in jogos_top:
            fid = j['fixture']['id']
            time_casa = j['teams']['home']['name']
            time_fora = j['teams']['away']['name']
            ja_enviado = False
            for s in st.session_state['historico_sinais']:
                if str(s['FID']) == str(fid) and "Sniper" in s['Estrategia']:
                    ja_enviado = True; break
            if ja_enviado: continue
            url_odds = "https://v3.football.api-sports.io/odds"
            res_odds = requests.get(url_odds, headers={"x-apisports-key": api_key}, params={"fixture": fid, "bookmaker": "6"}).json()
            odd_home = "N/A"; odd_away = "N/A"; str_odds = "Odds Indisponíveis"
            if res_odds.get('response'):
                try:
                    vals = res_odds['response'][0]['bookmakers'][0]['bets'][0]['values']
                    for v in vals:
                        if v['value'] == 'Home': odd_home = v['odd']
                        if v['value'] == 'Away': odd_away = v['odd']
                    str_odds = f"Odds: Casa @{odd_home} | Visitante @{odd_away}"
                except: pass
            url_pred = "https://v3.football.api-sports.io/predictions"
            res_pred = requests.get(url_pred, headers={"x-apisports-key": api_key}, params={"fixture": fid}).json()
            if res_pred.get('response'):
                pred = res_pred['response'][0]['predictions']
                comp = res_pred['response'][0]['comparison']
                info_jogo = (f"JOGO: {time_casa} (Casa) vs {time_fora} (Fora)\nMERCADO: {str_odds}\nDADOS API: Advice: {pred['advice']} | Chance Vitoria: {pred['percent']}\nCOMPARATIVO FORÇA (0-100%): \n- Ataque: {comp['att']['home']} vs {comp['att']['away']}\n- Defesa: {comp['def']['home']} vs {comp['def']['away']}\n- Forma: {comp['form']['home']} vs {comp['form']['away']}")
                prompt_matinal = f"Atue como Tipster Profissional. Analise:\n{info_jogo}\nREGRAS: 1. Odds < 1.30 ignore vitoria seca. 2. Defesa fraca = OVER. 3. Criativo mas seguro. Formato: BET: [MERCADO] | MOTIVO: [Motivo]"
                resp_ia = model_ia.generate_content(prompt_matinal)
                st.session_state['gemini_usage']['used'] += 1
                texto_ia = resp_ia.text.strip()
                if "BET:" in texto_ia.upper():
                    try:
                        partes = texto_ia.split('|')
                        aposta_raw = partes[0].replace("BET:", "").strip()
                        motivo_raw = partes[1].replace("MOTIVO:", "").strip() if len(partes) > 1 else "Análise favorável."
                    except: aposta_raw = texto_ia.replace("BET:", "").strip(); motivo_raw = "Oportunidade."
                    relatorio_final += f"🎯 <b>SNIPER: {time_casa} x {time_fora}</b>\n👉 <b>{aposta_raw}</b>\n💡 <i>{motivo_raw}</i>\n📊 {str_odds}\n\n"
                    odd_reg = "1.70" 
                    if "CASA" in aposta_raw.upper() and odd_home != "N/A": odd_reg = odd_home
                    elif "FORA" in aposta_raw.upper() and odd_away != "N/A": odd_reg = odd_away
                    item_bi = {"FID": str(fid), "Data": hoje, "Hora": "08:00", "Liga": j['league']['name'], "Jogo": f"{time_casa} x {time_fora}", "Placar_Sinal": aposta_raw, "Estrategia": "Sniper Matinal", "Resultado": "Pendente", "HomeID": str(j['teams']['home']['id']), "AwayID": str(j['teams']['away']['id']), "Odd": str(odd_reg), "Odd_Atualizada": "", "Opiniao_IA": "Sniper"}
                    adicionar_historico(item_bi)
                    time.sleep(1.5)
        return relatorio_final if relatorio_final else "Sem oportunidades de alto valor encontradas hoje."
    except Exception as e: return f"Erro Matinal: {e}"
# ==============================================================================
# BLOCO DE LÓGICA CORE (GREEN/RED, SNIPER, PROCESSAMENTO)
# ==============================================================================

# --- FUNÇÃO PROCESSAR RESULTADO (BLINDADA CONTRA DUPLICIDADE) ---
def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
    st_short = jogo_api['fixture']['status']['short']
    fid = sinal['FID']; strat = sinal['Estrategia']
    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: ph, pa = 0, 0
    
    # Usa a função universal para garantir que a chave seja IDÊNTICA à criada no envio
    key_green = gerar_chave_universal(fid, strat, "GREEN")
    key_red = gerar_chave_universal(fid, strat, "RED")
    
    if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()

    if (gh + ga) > (ph + pa):
        if "Morno" in strat or "Under" in strat:
            if (gh+ga) >= 2:
                sinal['Resultado'] = '❌ RED'
                if key_red not in st.session_state['alertas_enviados']:
                    enviar_telegram(token, chats, f"❌ <b>RED | OVER 1.5 BATIDO</b>\n⚽ {sinal['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {strat}")
                    st.session_state['alertas_enviados'].add(key_red)
                    st.session_state['precisa_salvar'] = True
                return True
        else:
            sinal['Resultado'] = '✅ GREEN'
            if key_green not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📈 Placar: <b>{gh}x{ga}</b>\n🎯 {strat}")
                st.session_state['alertas_enviados'].add(key_green)
                st.session_state['precisa_salvar'] = True
            return True

    STRATS_HT_ONLY = ["Gol Relâmpago", "Massacre", "Choque", "Briga"]
    eh_ht_strat = any(x in strat for x in STRATS_HT_ONLY)
    if eh_ht_strat and st_short in ['HT', '2H', 'FT', 'AET', 'PEN', 'ABD']:
        sinal['Resultado'] = '❌ RED'
        if key_red not in st.session_state['alertas_enviados']:
            enviar_telegram(token, chats, f"❌ <b>RED | INTERVALO (HT)</b>\n⚽ {sinal['Jogo']}\n📉 Placar HT: {gh}x{ga}\n🎯 {strat} (Não bateu no 1º Tempo)")
            st.session_state['alertas_enviados'].add(key_red)
            st.session_state['precisa_salvar'] = True
        return True
    
    if st_short in ['FT', 'AET', 'PEN', 'ABD']:
        if ("Morno" in strat or "Under" in strat) and (gh+ga) <= 1:
             sinal['Resultado'] = '✅ GREEN'
             if key_green not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"✅ <b>GREEN | UNDER BATIDO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}")
                st.session_state['alertas_enviados'].add(key_green)
                st.session_state['precisa_salvar'] = True
             return True
        sinal['Resultado'] = '❌ RED'
        if key_red not in st.session_state['alertas_enviados']:
            enviar_telegram(token, chats, f"❌ <b>RED | ENCERRADO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {strat}")
            st.session_state['alertas_enviados'].add(key_red)
            st.session_state['precisa_salvar'] = True
        return True
    return False

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    hist = st.session_state['historico_sinais']
    pendentes = [s for s in hist if s['Resultado'] == 'Pendente']
    if not pendentes: return
    hoje_str = get_time_br().strftime('%Y-%m-%d')
    updates_buffer = []
    mapa_live = {j['fixture']['id']: j for j in jogos_live}
    
    for s in pendentes:
        if s.get('Data') != hoje_str: continue
        fid = int(float(str(s['FID']).strip()))
        strat = s['Estrategia']
        
        # --- VERIFICAÇÃO RÁPIDA (Memória) ---
        key_green = gerar_chave_universal(fid, strat, "GREEN")
        key_red = gerar_chave_universal(fid, strat, "RED")
        
        if key_green in st.session_state['alertas_enviados']:
            s['Resultado'] = '✅ GREEN'; updates_buffer.append(s); continue
        if key_red in st.session_state['alertas_enviados']:
            s['Resultado'] = '❌ RED'; updates_buffer.append(s); continue

        # Busca jogo
        jogo_encontrado = mapa_live.get(fid)
        if not jogo_encontrado:
             try:
                 # Retry na API se sumiu do live
                 res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                 if res['response']: jogo_encontrado = res['response'][0]
             except: pass
        
        if jogo_encontrado:
            if processar_resultado(s, jogo_encontrado, token, chats): updates_buffer.append(s)
            
    if updates_buffer: atualizar_historico_ram(updates_buffer)

def conferir_resultados_sniper(jogos_live, api_key):
    hist = st.session_state.get('historico_sinais', [])
    snipers = [s for s in hist if "Sniper" in s['Estrategia'] and s['Resultado'] == "Pendente"]
    if not snipers: return
    updates = []
    ids_live = {str(j['fixture']['id']): j for j in jogos_live} 
    for s in snipers:
        fid = str(s['FID']); jogo = ids_live.get(fid)
        if not jogo:
            try:
                res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                if res.get('response'): jogo = res['response'][0]; update_api_usage(res.get('headers', {}))
            except: pass
        if not jogo: continue
        status = jogo['fixture']['status']['short']
        if status in ['FT', 'AET', 'PEN', 'INT']:
            gh = jogo['goals']['home'] or 0; ga = jogo['goals']['away'] or 0; tg = gh + ga
            target = s['Placar_Sinal'].upper().strip()
            res_final = None
            if "OVER 2.5" in target: res_final = '✅ GREEN' if tg > 2.5 else '❌ RED'
            elif "UNDER 2.5" in target: res_final = '✅ GREEN' if tg < 2.5 else '❌ RED'
            elif "AMBAS" in target: res_final = '✅ GREEN' if (gh > 0 and ga > 0) else '❌ RED'
            elif "CASA" in target: res_final = '✅ GREEN' if gh > ga else '❌ RED'
            elif "FORA" in target: res_final = '✅ GREEN' if ga > gh else '❌ RED'
            if res_final:
                s['Resultado'] = res_final; updates.append(s)
                enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], f"{res_final} <b>RESULTADO SNIPER</b>\n⚽ {s['Jogo']}\n🎯 {target}\n📉 Placar Final: {gh}x{ga}")
                st.session_state['precisa_salvar'] = True
    if updates: atualizar_historico_ram(updates)

def verificar_var_rollback(jogos_live, token, chats):
    hist = st.session_state['historico_sinais']
    greens = [s for s in hist if 'GREEN' in str(s['Resultado'])]
    if not greens: return
    updates = []
    for s in greens:
        if "Morno" in s['Estrategia']: continue
        fid = int(clean_fid(s.get('FID', 0)))
        jogo_api = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        if jogo_api:
            gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
            try:
                ph, pa = map(int, s['Placar_Sinal'].split('x'))
                if (gh + ga) <= (ph + pa):
                    s['Resultado'] = 'Pendente'
                    st.session_state['precisa_salvar'] = True
                    updates.append(s)
                    enviar_telegram(token, chats, f"⚠️ <b>VAR ACIONADO | GOL ANULADO</b>\n⚽ {s['Jogo']}\n📉 Placar voltou: <b>{gh}x{ga}</b>")
            except: pass
    if updates: atualizar_historico_ram(updates)

def momentum(fid, sog_h, sog_a):
    mem = st.session_state['memoria_pressao'].get(fid, {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
    if 'sog_h' not in mem: mem = {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []}
    now = datetime.now()
    if sog_h > mem['sog_h']: mem['h_t'].extend([now]*(sog_h-mem['sog_h']))
    if sog_a > mem['sog_a']: mem['a_t'].extend([now]*(sog_a-mem['sog_a']))
    mem['h_t'] = [t for t in mem['h_t'] if now - t <= timedelta(minutes=7)]
    mem['a_t'] = [t for t in mem['a_t'] if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a
    st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_t']), len(mem['a_t'])

def deve_buscar_stats(tempo, gh, ga, status):
    if 5 <= tempo <= 15: return True
    if 70 <= tempo <= 85 and abs(gh - ga) <= 1: return True
    if status == 'HT': return True
    return False

def fetch_stats_single(fid, api_key):
    try:
        url = "https://v3.football.api-sports.io/fixtures/statistics"
        r = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fid}, timeout=3)
        return fid, r.json().get('response', []), r.headers
    except: return fid, [], None

def atualizar_stats_em_paralelo(jogos_alvo, api_key):
    resultados = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_stats_single, j['fixture']['id'], api_key): j for j in jogos_alvo}
        time.sleep(0.2)
        for future in as_completed(futures):
            fid, stats, headers = future.result()
            if stats:
                resultados[fid] = stats
                update_api_usage(headers)
    return resultados

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        rh, ra = momentum(j['fixture']['id'], sog_h, sog_a)
        SINAIS = []
        if tempo <= 30 and (j['goals']['home'] + j['goals']['away']) >= 2: 
            SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": "🔥 Over Gols (Tendência de Goleada)", "stats": f"{sh_h+sh_a} Chutes", "rh": rh, "ra": ra})
        if (j['goals']['home'] + j['goals']['away']) == 0:
            if (tempo <= 2 and (sog_h + sog_a) >= 1) or (tempo <= 10 and (sh_h + sh_a) >= 2):
                SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": "Over 0.5 HT (Entrar para sair gol no 1º tempo)", "stats": f"{sh_h+sh_a} Chutes", "rh": rh, "ra": ra})
        if 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(j['goals']['home']-j['goals']['away']) <= 1: 
            SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": "Over Gols (Gol no final - Limite)", "stats": f"{sh_h+sh_a} Chutes", "rh": rh, "ra": ra})
        if tempo <= 60:
            if j['goals']['home'] <= j['goals']['away'] and (rh >= 2 or sh_h >= 8): SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": "Over Gols (Gol maduro na partida)", "stats": f"Pressão: {rh}", "rh": rh, "ra": ra})
            if j['goals']['away'] <= j['goals']['home'] and (ra >= 2 or sh_a >= 8): SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": "Over Gols (Gol maduro na partida)", "stats": f"Pressão: {ra}", "rh": rh, "ra": ra})
        if rank_home and rank_away:
            is_top_home = rank_home <= 4; is_top_away = rank_away <= 4; is_bot_home = rank_home >= 11; is_bot_away = rank_away >= 11; is_mid_home = rank_home >= 5; is_mid_away = rank_away >= 5
            if (is_top_home and is_bot_away) or (is_top_away and is_bot_home):
                if tempo <= 5 and (sh_h + sh_a) >= 1: SINAIS.append({"tag": "🔥 Massacre", "ordem": "Over 0.5 HT (Favorito deve abrir placar)", "stats": f"Rank: {rank_home}x{rank_away}", "rh": rh, "ra": ra})
            if 5 <= tempo <= 15:
                if is_top_home and (rh >= 2 or sh_h >= 3): SINAIS.append({"tag": "🦁 Favorito", "ordem": "Over Gols (Partida)", "stats": f"Pressão: {rh}", "rh": rh, "ra": ra})
                if is_top_away and (ra >= 2 or sh_a >= 3): SINAIS.append({"tag": "🦁 Favorito", "ordem": "Over Gols (Partida)", "stats": f"Pressão: {ra}", "rh": rh, "ra": ra})
            if is_top_home and is_top_away and tempo <= 7:
                if (sh_h + sh_a) >= 2 and (sog_h + sog_a) >= 1: SINAIS.append({"tag": "⚔️ Choque Líderes", "ordem": "Over 0.5 HT (Jogo intenso)", "stats": f"{sh_h+sh_a} Chutes", "rh": rh, "ra": ra})
            if is_mid_home and is_mid_away:
                if tempo <= 7 and 2 <= (sh_h + sh_a) <= 3: SINAIS.append({"tag": "🥊 Briga de Rua", "ordem": "Over 0.5 HT (Trocação franca)", "stats": f"{sh_h+sh_a} Chutes", "rh": rh, "ra": ra})
                is_bot_home_morno = rank_home >= 10; is_bot_away_morno = rank_away >= 10
                if is_bot_home_morno and is_bot_away_morno:
                    if 15 <= tempo <= 16 and (sh_h + sh_a) == 0: SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": "Under 1.5 HT (Apostar que NÃO saem 2 gols no 1º tempo)", "stats": "0 Chutes (Times Z-4)", "rh": rh, "ra": ra})
        if 75 <= tempo <= 85 and abs(j['goals']['home'] - j['goals']['away']) <= 1:
            if (sh_h + sh_a) >= 16 and (sog_h + sog_a) >= 8: SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": "Gol no Final (Over Limit) (Aposta seca que sai mais um gol)", "stats": "🔥 Pressão Máxima", "rh": rh, "ra": ra})
        return SINAIS
    except: return []

# ==============================================================================
# 5. SIDEBAR E LOOP PRINCIPAL (EXECUÇÃO)
# ==============================================================================
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=True):
        st.session_state['API_KEY'] = st.text_input("Chave API:", value=st.session_state['API_KEY'], type="password")
        st.session_state['TG_TOKEN'] = st.text_input("Token Telegram:", value=st.session_state['TG_TOKEN'], type="password")
        st.session_state['TG_CHAT'] = st.text_input("Chat IDs:", value=st.session_state['TG_CHAT'])
        INTERVALO = st.slider("Ciclo (s):", 60, 300, 60)
        if st.button("🧹 Limpar Cache"): 
            st.cache_data.clear(); carregar_tudo(force=True); st.session_state['last_db_update'] = 0; st.toast("Cache Limpo!")
        st.write("---")
        if st.button("🧠 Pedir Análise do BI"):
            if IA_ATIVADA:
                with st.spinner("🤖 O Consultor Neves está analisando seus dados..."):
                    analise = analisar_bi_com_ia()
                    st.markdown("### 📝 Relatório do Consultor")
                    st.info(analise)
            else: st.error("IA Desconectada.")
        if st.button("🧪 Criar Nova Estratégia (Big Data)"):
            if IA_ATIVADA:
                with st.spinner("🤖 Analisando padrões globais no Big Data..."):
                    sugestao = criar_estrategia_nova_ia()
                    st.markdown("### 💡 Sugestão da IA")
                    st.success(sugestao)
            else: st.error("IA Desconectada.")
        
        if st.button("🔧 Otimizar Estratégias (IA)"):
            if IA_ATIVADA and db_firestore:
                with st.spinner("🕵️ Cruzando Greens/Reds com Big Data..."):
                    relatorio = otimizar_estrategias_existentes_ia()
                    st.markdown("### 🛠️ Plano de Melhoria")
                    if "Erro" in relatorio or "Atenção" in relatorio: st.warning(relatorio)
                    else: st.success("Análise Concluída!"); st.write(relatorio)
            else: st.error("Requer IA Ativa e Conexão Firebase.")
        
        if st.button("🔄 Forçar Backfill (Salvar Jogos Perdidos)"):
            with st.spinner("Buscando na API todos os jogos finalizados hoje..."):
                hoje_real = get_time_br().strftime('%Y-%m-%d')
                todos_jogos_hoje = buscar_agenda_cached(st.session_state['API_KEY'], hoje_real)
                ft_pendentes = [j for j in todos_jogos_hoje if j['fixture']['status']['short'] in ['FT', 'AET', 'PEN'] and str(j['fixture']['id']) not in st.session_state['jogos_salvos_bigdata']]
                if ft_pendentes:
                    st.info(f"Processando {len(ft_pendentes)} jogos...")
                    stats_recuperadas = atualizar_stats_em_paralelo(ft_pendentes, st.session_state['API_KEY'])
                    count_salvos = 0
                    for fid, stats in stats_recuperadas.items():
                        j_obj = next((x for x in ft_pendentes if str(x['fixture']['id']) == str(fid)), None)
                        if j_obj: salvar_bigdata(j_obj, stats); count_salvos += 1
                    st.success(f"✅ Recuperados e Salvos: {count_salvos} jogos!")
                else: st.warning("Nenhum jogo finalizado pendente.")

        if st.button("📊 Enviar Relatório BI"): enviar_relatorio_bi(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT']); st.toast("Relatório Enviado!")
        if st.button("💰 Enviar Relatório Financeiro"):
            if 'last_fin_stats' in st.session_state:
                s = st.session_state['last_fin_stats']
                enviar_relatorio_financeiro(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], s['cenario'], s['lucro'], s['roi'], s['entradas'])
                st.toast("Relatório Financeiro Enviado!")
            else: st.error("Abra a aba Financeiro primeiro.")

    with st.expander("💰 Gestão de Banca", expanded=False):
        stake_padrao = st.number_input("Valor da Aposta (R$)", value=st.session_state.get('stake_padrao', 10.0), step=5.0)
        banca_inicial = st.number_input("Banca Inicial (R$)", value=st.session_state.get('banca_inicial', 100.0), step=50.0)
        st.session_state['stake_padrao'] = stake_padrao
        st.session_state['banca_inicial'] = banca_inicial
        
    with st.expander("📶 Consumo API", expanded=False):
        verificar_reset_diario()
        u = st.session_state['api_usage']; perc = min(u['used'] / u['limit'], 1.0) if u['limit'] > 0 else 0
        st.progress(perc); st.caption(f"Utilizado: **{u['used']}** / {u['limit']}")
    
    with st.expander("🤖 Consumo IA (Gemini)", expanded=False):
        u_ia = st.session_state['gemini_usage']; u_ia['limit'] = 10000 
        perc_ia = min(u_ia['used'] / u_ia['limit'], 1.0)
        st.progress(perc_ia); st.caption(f"Requições Hoje: **{u_ia['used']}** / {u_ia['limit']}")
        if st.button("🔓 Destravar IA Agora"):
            st.session_state['ia_bloqueada_ate'] = None; st.toast("✅ IA Destravada!")

    st.write("---")
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    
    if IA_ATIVADA:
        if st.session_state['ia_bloqueada_ate']:
            ag = datetime.now()
            if ag < st.session_state['ia_bloqueada_ate']:
                m_rest = int((st.session_state['ia_bloqueada_ate'] - ag).total_seconds()/60)
                st.markdown(f'<div class="status-warning">⚠️ IA PAUSADA (Proteção) - Volta em {m_rest} min</div>', unsafe_allow_html=True)
            else: st.markdown('<div class="status-active">🤖 IA GEMINI ATIVA</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🤖 IA GEMINI ATIVA</div>', unsafe_allow_html=True)
    else: st.markdown('<div class="status-error">❌ IA DESCONECTADA</div>', unsafe_allow_html=True)

    if db_firestore: st.markdown('<div class="status-active">🔥 FIREBASE CONECTADO</div>', unsafe_allow_html=True)
    else: st.markdown('<div class="status-warning">⚠️ FIREBASE OFFLINE</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### ⚠️ Zona de Perigo")
    if st.button("☢️ ZERAR ROBÔ", type="primary", use_container_width=True): st.session_state['confirmar_reset'] = True
    if st.session_state.get('confirmar_reset'):
        st.error("Tem certeza? Isso apaga TODO o histórico.")
        c1, c2 = st.columns(2)
        if c1.button("✅ SIM"): 
            st.cache_data.clear()
            st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
            salvar_aba("Historico", st.session_state['historico_full'])
            st.session_state['confirmar_reset'] = False; st.rerun()
        if c2.button("❌ NÃO"): st.session_state['confirmar_reset'] = False; st.rerun()

if st.session_state.ROBO_LIGADO:
    with placeholder_root.container():
        carregar_tudo()
        s_padrao = st.session_state.get('stake_padrao', 10.0)
        b_inicial = st.session_state.get('banca_inicial', 100.0)
        safe_token = st.session_state.get('TG_TOKEN', '')
        safe_chat = st.session_state.get('TG_CHAT', '')
        safe_api = st.session_state.get('API_KEY', '')

        verificar_automacao_bi(safe_token, safe_chat, s_padrao)
        verificar_alerta_matinal(safe_token, safe_chat, safe_api)
        
        ids_black = [normalizar_id(x) for x in st.session_state['df_black']['id'].values]
        df_obs = st.session_state.get('df_vip', pd.DataFrame()); count_obs = len(df_obs)
        df_safe_show = st.session_state.get('df_safe', pd.DataFrame()); count_safe = len(df_safe_show)
        ids_safe = [normalizar_id(x) for x in df_safe_show['id'].values]
        hoje_real = get_time_br().strftime('%Y-%m-%d')
        if 'historico_full' in st.session_state and not st.session_state['historico_full'].empty:
             df_full = st.session_state['historico_full']
             st.session_state['historico_sinais'] = df_full[df_full['Data'] == hoje_real].to_dict('records')[::-1]

        api_error = False; jogos_live = []
        try:
            url = "https://v3.football.api-sports.io/fixtures"
            resp = requests.get(url, headers={"x-apisports-key": safe_api}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
            update_api_usage(resp.headers); res = resp.json()
            raw_live = res.get('response', []) if not res.get('errors') else []
            
            # --- CORREÇÃO DE DUPLICIDADE GLOBAL NA API ---
            dict_clean = {j['fixture']['id']: j for j in raw_live}
            jogos_live = list(dict_clean.values())
            
            api_error = bool(res.get('errors'))
            if api_error and "errors" in res: st.error(f"Detalhe do Erro: {res['errors']}")
        except Exception as e: jogos_live = []; api_error = True; st.error(f"Erro de Conexão: {e}")

        if not api_error: 
            check_green_red_hibrido(jogos_live, safe_token, safe_chat, safe_api)
            conferir_resultados_sniper(jogos_live, safe_api) 
            verificar_var_rollback(jogos_live, safe_token, safe_chat)
        
        radar = []; agenda = []; candidatos_multipla = []; ids_no_radar = []
        
        if not api_error:
            # 1. BIG DATA (RANDOMIZADO)
            prox = buscar_agenda_cached(safe_api, hoje_real); agora = get_time_br()
            ft_para_salvar = []
            for p in prox:
                try:
                    if p['fixture']['status']['short'] in ['FT', 'AET', 'PEN'] and str(p['fixture']['id']) not in st.session_state['jogos_salvos_bigdata']:
                        ft_para_salvar.append(p)
                except: pass
            if ft_para_salvar:
                lote = random.sample(ft_para_salvar, min(len(ft_para_salvar), 3)) 
                stats_ft = atualizar_stats_em_paralelo(lote, safe_api)
                for fid, s in stats_ft.items():
                    j_obj = next((x for x in lote if x['fixture']['id'] == fid), None)
                    if j_obj: salvar_bigdata(j_obj, s)

            # 2. PROCESSAMENTO DE SINAIS
            STATUS_BOLA_ROLANDO = ['1H', '2H', 'HT', 'ET', 'P', 'BT']
            
            for j in jogos_live:
                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                if lid in ids_black: continue
                status_short = j['fixture']['status']['short']
                elapsed = j['fixture']['status']['elapsed']
                if status_short not in STATUS_BOLA_ROLANDO: continue
                if (elapsed is None or elapsed == 0) and status_short != 'HT': continue

                nome_liga_show = j['league']['name']
                if lid in ids_safe: nome_liga_show += " 🛡️"
                elif lid in df_obs['id'].values: nome_liga_show += " ⚠️"
                else: nome_liga_show += " ❓" 
                ids_no_radar.append(fid)
                tempo = j['fixture']['status']['elapsed'] or 0; st_short = j['fixture']['status']['short']
                home = j['teams']['home']['name']; away = j['teams']['away']['name']
                placar = f"{j['goals']['home']}x{j['goals']['away']}"; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
                if st_short == 'FT': continue 
                
                stats = []
                ult_chk = st.session_state['controle_stats'].get(fid, datetime.min)
                t_esp = 60 if (69<=tempo<=76) else (90 if tempo<=15 else 180)
                if deve_buscar_stats(tempo, gh, ga, st_short):
                    if (datetime.now() - ult_chk).total_seconds() > t_esp:
                         fid_res, s_res, h_res = fetch_stats_single(fid, safe_api)
                         if s_res:
                             st.session_state['controle_stats'][fid] = datetime.now()
                             st.session_state[f"st_{fid}"] = s_res
                             update_api_usage(h_res)
                stats = st.session_state.get(f"st_{fid}", [])
                
                rank_h = None; rank_a = None
                if j['league']['id'] in LIGAS_TABELA:
                    rk = buscar_ranking(safe_api, j['league']['id'], j['league']['season'])
                    rank_h = rk.get(home); rank_a = rk.get(away)
                lista_sinais = []
                if stats:
                    lista_sinais = processar(j, stats, tempo, placar, rank_h, rank_a)
                    salvar_safe_league_basic(lid, j['league']['country'], j['league']['name'], tem_tabela=(rank_h is not None))
                    resetar_erros(lid)
                    # Lógica de Múltipla HT
                    if st_short == 'HT' and gh == 0 and ga == 0:
                        try:
                            s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
                            v1 = next((x['value'] for x in s1 if x['type']=='Total Shots'), 0) or 0
                            v2 = next((x['value'] for x in s2 if x['type']=='Total Shots'), 0) or 0
                            sg1 = next((x['value'] for x in s1 if x['type']=='Shots on Goal'), 0) or 0
                            sg2 = next((x['value'] for x in s2 if x['type']=='Shots on Goal'), 0) or 0
                            if (v1+v2) > 12 and (sg1+sg2) > 6: candidatos_multipla.append({'fid': fid, 'jogo': f"{home} x {away}", 'stats': f"{v1+v2} Chutes", 'indica': "Over 0.5 FT"})
                        except: pass
                else: gerenciar_erros(lid, j['league']['country'], j['league']['name'], fid)

                # --- LÓGICA DE STATUS E RADAR (CORRIGIDA) ---
                status_vis = "🟢 Monitorando"

                if lista_sinais:
                    status_vis = f"✅ {len(lista_sinais)} Sinais"
                    medias_gols = buscar_media_gols_ultimos_jogos(safe_api, j['teams']['home']['id'], j['teams']['away']['id'])
                    
                    for s in lista_sinais:
                        rh = s.get('rh', 0); ra = s.get('ra', 0)
                        txt_pressao = gerar_barra_pressao(rh, ra) 
                        
                        # --- CHAVE UNIVERSAL BLINDADA ---
                        uid_normal = gerar_chave_universal(fid, s['tag'], "SINAL")
                        uid_super = f"SUPER_{uid_normal}"
                        
                        # TRAVA DE SEGURANÇA 1 (Já enviou?)
                        if uid_normal in st.session_state['alertas_enviados']: continue 
                        
                        # TRAVA DE SEGURANÇA 2 (Bloqueia Agora)
                        st.session_state['alertas_enviados'].add(uid_normal)

                        odd_atual_str = get_live_odds(fid, safe_api, s['tag'], gh+ga)
                        try: odd_val = float(odd_atual_str)
                        except: odd_val = 0.0
                        
                        destaque_odd = ""
                        if odd_val >= 1.80:
                            destaque_odd = "\n💎 <b>SUPER ODD DETECTADA! (EV+)</b>"
                            st.session_state['alertas_enviados'].add(uid_super)
                        
                        opiniao_txt = ""; opiniao_db = "Neutro"
                        if IA_ATIVADA:
                            try:
                                time.sleep(0.3)
                                dados_ia = {'jogo': f"{home} x {away}", 'placar': placar, 'tempo': f"{tempo}'"}
                                opiniao_txt = consultar_ia_gemini(dados_ia, s['tag'], stats)
                                if "Aprovado" in opiniao_txt: opiniao_db = "Aprovado"
                                elif "Arriscado" in opiniao_txt: opiniao_db = "Arriscado"
                            except: pass
                        
                        item = {"FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Placar_Sinal": placar, "Estrategia": s['tag'], "Resultado": "Pendente", "HomeID": str(j['teams']['home']['id']) if lid in ids_safe else "", "AwayID": str(j['teams']['away']['id']) if lid in ids_safe else "", "Odd": odd_atual_str, "Odd_Atualizada": "", "Opiniao_IA": opiniao_db}
                        
                        if adicionar_historico(item):
                            prob = buscar_inteligencia(s['tag'], j['league']['name'], f"{home} x {away}")
                            msg = f"<b>🚨 SINAL ENCONTRADO 🚨</b>\n\n🏆 <b>{j['league']['name']}</b>\n⚽ {home} 🆚 {away}\n⏰ <b>{tempo}' minutos</b> (Placar: {placar})\n\n🔥 {s['tag'].upper()}\n⚠️ <b>AÇÃO:</b> {s['ordem']}{destaque_odd}\n\n💰 <b>Odd: @{odd_atual_str}</b>{txt_pressao}\n📊 <i>Dados: {s['stats']}</i>\n⚽ <b>Médias (10j):</b> Casa {medias_gols['home']} | Fora {medias_gols['away']}{prob}{opiniao_txt}"
                            enviar_telegram(safe_token, safe_chat, msg)
                            st.toast(f"Sinal Enviado: {s['tag']}")
                        elif uid_super not in st.session_state['alertas_enviados'] and odd_val >= 1.80:
                             st.session_state['alertas_enviados'].add(uid_super)
                             msg_super = (f"💎 <b>OPORTUNIDADE DE VALOR!</b>\n\n⚽ {home} 🆚 {away}\n📈 <b>A Odd subiu!</b> Entrada valorizada.\n🔥 <b>Estratégia:</b> {s['tag']}\n💰 <b>Nova Odd: @{odd_atual_str}</b>\n<i>O jogo mantém o padrão da estratégia.</i>{txt_pressao}")
                             enviar_telegram(safe_token, safe_chat, msg_super)
                
                # RADAR E STATUS (FORA DO IF SINAIS, MAS DENTRO DO LOOP DE JOGOS)
                radar.append({"Liga": nome_liga_show, "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})
            
            # --- MÚLTIPLAS E AGENDA (FORA DO LOOP DE JOGOS PARA OTIMIZAR) ---
            if candidatos_multipla:
                novos = [c for c in candidatos_multipla if c['fid'] not in st.session_state['multiplas_enviadas']]
                if novos:
                    msg = "<b>🚀 OPORTUNIDADE DE MÚLTIPLA (HT) 🚀</b>\n" + "".join([f"\n⚽ {c['jogo']} ({c['stats']})\n⚠️ AÇÃO: {c['indica']}" for c in novos])
                    for c in novos: st.session_state['multiplas_enviadas'].add(c['fid'])
                    enviar_telegram(safe_token, safe_chat, msg)
            
            for p in prox:
                try:
                    if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] in ['NS', 'TBD'] and p['fixture']['id'] not in ids_no_radar:
                        if datetime.fromisoformat(p['fixture']['date']) > agora:
                            l_id = normalizar_id(p['league']['id']); l_nm = p['league']['name']
                            if l_id in ids_safe: l_nm += " 🛡️"
                            elif l_id in df_obs['id'].values: l_nm += " ⚠️"
                            agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": l_nm, "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
                except: pass
        
        if st.session_state.get('precisa_salvar'):
            if 'historico_full' in st.session_state and not st.session_state['historico_full'].empty:
                st.caption("⏳ Sincronizando dados pendentes...")
                salvar_aba("Historico", st.session_state['historico_full'])
        
        if api_error: st.markdown('<div class="status-error">🚨 API LIMITADA - AGUARDE</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        hist_hj = pd.DataFrame(st.session_state['historico_sinais'])
        t, g, r, w = calcular_stats(hist_hj)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t}</div><div class="metric-sub">{g} Green | {r} Red</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(radar)}</div><div class="metric-sub">Monitorando</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Ligas Seguras</div><div class="metric-value">{count_safe}</div><div class="metric-sub">Validadas</div></div>', unsafe_allow_html=True)
        
        st.write("")
        abas = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", f"💰 Financeiro", f"📜 Histórico ({len(hist_hj)})", "📈 BI & Analytics", f"🚫 Blacklist ({len(st.session_state['df_black'])})", f"🛡️ Seguras ({count_safe})", f"⚠️ Obs ({count_obs})", "💾 Big Data (Firebase)"])
        
        with abas[0]: 
            if radar: st.dataframe(pd.DataFrame(radar)[['Liga', 'Jogo', 'Tempo', 'Status']].astype(str), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        
        with abas[1]: 
            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos futuros hoje.")
        
        with abas[2]:
            st.markdown("### 💰 Evolução Financeira")
            c_fin1, c_fin2 = st.columns(2)
            stake_padrao = c_fin1.number_input("Valor da Aposta (Stake):", value=st.session_state.get('stake_padrao', 10.0), step=5.0)
            banca_inicial = c_fin2.number_input("Banca Inicial:", value=st.session_state.get('banca_inicial', 100.0), step=50.0)
            st.session_state['stake_padrao'] = stake_padrao; st.session_state['banca_inicial'] = banca_inicial
            modo_simulacao = st.radio("Cenário de Entrada:", ["Todos os sinais", "Apenas 1 sinal por jogo", "Até 2 sinais por jogo"], horizontal=True)
            filtrar_ia = st.checkbox("🤖 Somente Sinais APROVADOS pela IA")
            df_fin = st.session_state.get('historico_full', pd.DataFrame())
            if not df_fin.empty:
                df_fin = df_fin.copy()
                df_fin['Odd_Calc'] = df_fin.apply(lambda row: obter_odd_final_para_calculo(row['Odd'], row['Estrategia']), axis=1)
                df_fin = df_fin[df_fin['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
                df_fin = df_fin.sort_values(by=['FID', 'Hora'], ascending=[True, True])
                if filtrar_ia and 'Opiniao_IA' in df_fin.columns: df_fin = df_fin[df_fin['Opiniao_IA'] == 'Aprovado']
                if modo_simulacao == "Apenas 1 sinal por jogo": df_fin = df_fin.groupby('FID').head(1)
                elif modo_simulacao == "Até 2 sinais por jogo": df_fin = df_fin.groupby('FID').head(2)
                if not df_fin.empty:
                    lucros = []; saldo_atual = banca_inicial; historico_saldo = [banca_inicial]; qtd_greens = 0; qtd_reds = 0
                    for idx, row in df_fin.iterrows():
                        res = row['Resultado']; odd = row['Odd_Calc']
                        if 'GREEN' in res: lucro = (stake_padrao * odd) - stake_padrao; qtd_greens += 1
                        else: lucro = -stake_padrao; qtd_reds += 1
                        saldo_atual += lucro; lucros.append(lucro); historico_saldo.append(saldo_atual)
                    df_fin['Lucro'] = lucros; total_lucro = sum(lucros)
                    roi = (total_lucro / (len(df_fin) * stake_padrao)) * 100
                    st.session_state['last_fin_stats'] = {'cenario': modo_simulacao, 'lucro': total_lucro, 'roi': roi, 'entradas': len(df_fin)}
                    m1, m2, m3, m4 = st.columns(4)
                    cor_delta = "normal" if total_lucro >= 0 else "inverse"
                    m1.metric("Banca Atual", f"R$ {saldo_atual:.2f}")
                    m2.metric("Lucro Líquido", f"R$ {total_lucro:.2f}", delta=f"{roi:.1f}%", delta_color=cor_delta)
                    m3.metric("Entradas", len(df_fin))
                    m4.metric("Winrate", f"{(qtd_greens/len(df_fin)*100):.1f}%")
                    fig_fin = px.line(y=historico_saldo, x=range(len(historico_saldo)), title="Crescimento da Banca (Realista)")
                    fig_fin.update_layout(xaxis_title="Entradas", yaxis_title="Saldo (R$)", template="plotly_dark")
                    fig_fin.add_hline(y=banca_inicial, line_dash="dot", annotation_text="Início", line_color="gray")
                    st.plotly_chart(fig_fin, use_container_width=True)
                else: st.info("Aguardando fechamento de sinais.")
            else: st.info("Sem dados históricos.")

        with abas[3]: 
            if not hist_hj.empty: 
                df_show = hist_hj.copy()
                if 'Jogo' in df_show.columns and 'Placar_Sinal' in df_show.columns: df_show['Jogo'] = df_show['Jogo'] + " (" + df_show['Placar_Sinal'].astype(str) + ")"
                colunas_esconder = ['FID', 'HomeID', 'AwayID', 'Data_Str', 'Data_DT', 'Odd_Atualizada', 'Placar_Sinal']
                cols_view = [c for c in df_show.columns if c not in colunas_esconder]
                st.dataframe(df_show[cols_view], use_container_width=True, hide_index=True)
            else: st.caption("Vazio.")

        with abas[4]: 
            st.markdown("### 📊 Inteligência de Mercado")
            df_bi = st.session_state.get('historico_full', pd.DataFrame())
            if df_bi.empty: st.warning("Sem dados históricos.")
            else:
                try:
                    df_bi = df_bi.copy()
                    df_bi['Data_Str'] = df_bi['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
                    df_bi['Data_DT'] = pd.to_datetime(df_bi['Data_Str'], errors='coerce')
                    df_bi = df_bi.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
                    hoje = pd.to_datetime(get_time_br().date())
                    d_hoje = df_bi[df_bi['Data_DT'] == hoje]; d_7d = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=7))]; d_30d = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=30))]; d_total = df_bi
                    def fmt_placar(d):
                        if d.empty: return "0G - 0R (0%)"
                        g = d['Resultado'].str.contains('GREEN', na=False).sum(); r = d['Resultado'].str.contains('RED', na=False).sum(); t = g + r; wr = (g/t*100) if t > 0 else 0
                        return f"{g}G - {r}R ({wr:.0f}%)"
                    def fmt_ia_stats(periodo_df, label_periodo):
                        if 'Opiniao_IA' not in periodo_df.columns: return ""
                        d_fin = periodo_df[periodo_df['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        stats_aprov = fmt_placar(d_fin[d_fin['Opiniao_IA'] == 'Aprovado'])
                        stats_risk = fmt_placar(d_fin[d_fin['Opiniao_IA'] == 'Arriscado'])
                        return f"🤖 IA ({label_periodo}):\n👍 Aprovados: {stats_aprov}\n⚠️ Arriscados: {stats_risk}"
                    
                    msg_ia_hoje = fmt_ia_stats(d_hoje, "Hoje"); msg_ia_7d = fmt_ia_stats(d_7d, "7 Dias"); msg_ia_30d = fmt_ia_stats(d_30d, "30 Dias"); msg_ia_total = fmt_ia_stats(d_total, "Geral")
                    if 'bi_filter' not in st.session_state: st.session_state['bi_filter'] = "Tudo"
                    filtro = st.selectbox("📅 Período", ["Tudo", "Hoje", "7 Dias", "30 Dias"], key="bi_select")
                    if filtro == "Hoje": df_show = d_hoje
                    elif filtro == "7 Dias": df_show = d_7d
                    elif filtro == "30 Dias": df_show = d_30d
                    else: df_show = df_bi 
                    
                    if not df_show.empty:
                        gr = df_show['Resultado'].str.contains('GREEN').sum(); rd = df_show['Resultado'].str.contains('RED').sum(); tt = len(df_show); ww = (gr/tt*100) if tt>0 else 0
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Sinais", tt); m2.metric("Greens", gr); m3.metric("Reds", rd); m4.metric("Assertividade", f"{ww:.1f}%")
                        st.divider()
                        st.markdown("### 🏆 Melhores e Piores Ligas (Com Drill-Down)")
                        df_finished = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        if not df_finished.empty:
                            stats_ligas = df_finished.groupby('Liga')['Resultado'].apply(lambda x: pd.Series({'Winrate': (x.str.contains('GREEN').sum() / len(x) * 100), 'Total': len(x), 'Reds': x.str.contains('RED').sum(), 'Greens': x.str.contains('GREEN').sum()})).unstack()
                            stats_ligas = stats_ligas[stats_ligas['Total'] >= 2]
                            col_top, col_worst = st.columns(2)
                            with col_top:
                                st.caption("🌟 Top Ligas (Mais Lucrativas)")
                                top_ligas = stats_ligas.sort_values(by=['Winrate', 'Total'], ascending=[False, False]).head(10)
                                st.dataframe(top_ligas[['Winrate', 'Total', 'Greens']].style.format({'Winrate': '{:.2f}%', 'Total': '{:.0f}', 'Greens': '{:.0f}'}), use_container_width=True)
                            with col_worst:
                                st.caption("💀 Ligas Críticas")
                                worst_ligas = stats_ligas.sort_values(by=['Reds'], ascending=False).head(10)
                                dados_drill = []
                                for liga, row in worst_ligas.iterrows():
                                    if row['Reds'] > 0:
                                        erros_liga = df_finished[(df_finished['Liga'] == liga) & (df_finished['Resultado'].str.contains('RED'))]
                                        pior_strat = erros_liga['Estrategia'].value_counts().head(1)
                                        nome_strat = pior_strat.index[0] if not pior_strat.empty else "-"
                                        dados_drill.append({"Liga": liga, "Total Reds": int(row['Reds']), "Pior Estratégia": nome_strat})
                                if dados_drill: st.dataframe(pd.DataFrame(dados_drill), use_container_width=True, hide_index=True)
                                else: st.success("Nenhuma liga com Reds significativos.")
                        st.divider()
                        st.markdown("### 🧠 Auditoria da IA (Aprovações vs Resultado)")
                        if 'Opiniao_IA' in df_show.columns:
                            df_audit = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                            if not df_audit.empty:
                                pivot = pd.crosstab(df_audit['Opiniao_IA'], df_audit['Resultado'], margins=True)
                                if '✅ GREEN' in pivot.columns and 'All' in pivot.columns: pivot['Winrate %'] = (pivot['✅ GREEN'] / pivot['All'] * 100)
                                format_dict = {'Winrate %': '{:.2f}%'}
                                for col in pivot.columns:
                                    if col != 'Winrate %': format_dict[col] = '{:.0f}'
                                st.dataframe(pivot.style.format(format_dict, na_rep="-").highlight_max(axis=0, color='#1F4025'), use_container_width=True)
                        st.markdown("### 📈 Performance por Estratégia")
                        st_s = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        if not st_s.empty:
                            resumo_strat = st_s.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)
                            if '✅ GREEN' in resumo_strat.columns and '❌ RED' in resumo_strat.columns:
                                resumo_strat['Winrate'] = (resumo_strat['✅ GREEN'] / (resumo_strat['✅ GREEN'] + resumo_strat['❌ RED']) * 100)
                                format_strat = {'Winrate': '{:.2f}%'}
                                for c in resumo_strat.columns:
                                    if c != 'Winrate': format_strat[c] = '{:.0f}'
                                st.dataframe(resumo_strat.sort_values('Winrate', ascending=False).style.format(format_strat), use_container_width=True)
                            cts = st_s.groupby(['Estrategia', 'Resultado']).size().reset_index(name='Qtd')
                            fig = px.bar(cts, x='Estrategia', y='Qtd', color='Resultado', color_discrete_map={'✅ GREEN': '#00FF00', '❌ RED': '#FF0000'}, title="Volume de Sinais", text='Qtd')
                            fig.update_layout(template="plotly_dark"); st.plotly_chart(fig, use_container_width=True)
                except Exception as e: st.error(f"Erro BI: {e}")

        with abas[5]: st.dataframe(st.session_state['df_black'][['País', 'Liga', 'Motivo']], use_container_width=True, hide_index=True)
        
        with abas[6]: 
            df_safe_show = st.session_state.get('df_safe', pd.DataFrame()).copy()
            if not df_safe_show.empty:
                def calc_risco(x):
                    try: v = int(float(str(x)))
                    except: v = 0
                    return "🟢 100% Estável" if v == 0 else f"⚠️ Atenção ({v}/10)"
                df_safe_show['Status Risco'] = df_safe_show['Strikes'].apply(calc_risco)
                st.dataframe(df_safe_show[['País', 'Liga', 'Motivo', 'Status Risco']], use_container_width=True, hide_index=True)
            else: st.info("Nenhuma liga segura ainda.")

        with abas[7]: 
            df_vip_show = st.session_state.get('df_vip', pd.DataFrame()).copy()
            if not df_vip_show.empty: df_vip_show['Strikes'] = df_vip_show['Strikes'].apply(formatar_inteiro_visual)
            st.dataframe(df_vip_show[['País', 'Liga', 'Data_Erro', 'Strikes']], use_container_width=True, hide_index=True)

        with abas[8]:
            st.markdown(f"### 💾 Banco de Dados de Partidas (Firebase)")
            st.caption("A IA usa esses dados para criar novas estratégias. Os dados são salvos na nuvem.")
            if db_firestore:
                col_fb1, col_fb2 = st.columns([1, 3])
                if col_fb1.button("🔄 Carregar/Atualizar Tabela"):
                    try:
                        with st.spinner("Baixando dados do Firebase..."):
                            docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(50).stream()
                            data = [d.to_dict() for d in docs]
                            st.session_state['cache_firebase_view'] = data 
                            st.toast(f"Dados atualizados! ({len(data)} jogos)")
                    except Exception as e: st.error(f"Erro ao ler Firebase: {e}")
                if 'cache_firebase_view' in st.session_state and st.session_state['cache_firebase_view']:
                    st.success(f"📂 Visualizando {len(st.session_state['cache_firebase_view'])} registros (Cache Local)")
                    st.dataframe(pd.DataFrame(st.session_state['cache_firebase_view']), use_container_width=True)
                else: st.info("ℹ️ Clique no botão acima para visualizar os dados salvos (Isso consome leituras da cota).")
            else: st.warning("⚠️ Firebase não conectado.")

        for i in range(INTERVALO, 0, -1):
            st.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()
else:
    with placeholder_root.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera. Configure na lateral.")
