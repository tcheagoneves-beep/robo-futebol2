# --- LÓGICA DE STATUS E RADAR (Correção: Define padrão antes do IF) ---
                status_vis = "🟢 Monitorando"

                if lista_sinais:
                    status_vis = f"✅ {len(lista_sinais)} Sinais"
                    
                    # Busca médias apenas se tiver sinal (Economia de API)
                    medias_gols = buscar_media_gols_ultimos_jogos(safe_api, j['teams']['home']['id'], j['teams']['away']['id'])
                    
                    for s in lista_sinais:
                        rh = s.get('rh', 0); ra = s.get('ra', 0)
                        txt_pressao = gerar_barra_pressao(rh, ra) 
                        
                        # --- CHAVE ÚNICA BLINDADA ---
                        tag_limpa = str(s['tag']).strip()
                        fid_str = str(fid).strip()
                        uid_normal = f"{fid_str}_{tag_limpa}"
                        uid_super = f"SUPER_{fid_str}_{tag_limpa}"
                        
                        # Trava 1: Já enviou?
                        if uid_normal in st.session_state['alertas_enviados']:
                            continue 
                        
                        # Trava 2: Vacina imediata
                        st.session_state['alertas_enviados'].add(uid_normal)

                        # Odds
                        odd_atual_str = get_live_odds(fid, safe_api, s['tag'], gh+ga)
                        try: odd_val = float(odd_atual_str)
                        except: odd_val = 0.0
                        
                        destaque_odd = ""
                        if odd_val >= 1.80:
                            destaque_odd = "\n💎 <b>SUPER ODD DETECTADA! (EV+)</b>"
                            st.session_state['alertas_enviados'].add(uid_super)
                        
                        # IA (Caçador de Gols)
                        opiniao_txt = ""
                        opiniao_db = "Neutro"
                        if IA_ATIVADA:
                            try:
                                time.sleep(0.3)
                                dados_ia = {'jogo': f"{home} x {away}", 'placar': placar, 'tempo': f"{tempo}'"}
                                opiniao_txt = consultar_ia_gemini(dados_ia, s['tag'], stats)
                                if "Aprovado" in opiniao_txt: opiniao_db = "Aprovado"
                                elif "Arriscado" in opiniao_txt: opiniao_db = "Arriscado"
                            except: pass
                        
                        # Salva e Envia
                        item = {
                            "FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), 
                            "Hora": get_time_br().strftime('%H:%M'), 
                            "Liga": j['league']['name'], "Jogo": f"{home} x {away}", 
                            "Placar_Sinal": placar, "Estrategia": s['tag'], 
                            "Resultado": "Pendente", 
                            "HomeID": str(j['teams']['home']['id']) if lid in ids_safe else "", 
                            "AwayID": str(j['teams']['away']['id']) if lid in ids_safe else "", 
                            "Odd": odd_atual_str, "Odd_Atualizada": "", "Opiniao_IA": opiniao_db
                        }
                        
                        if adicionar_historico(item):
                            prob = buscar_inteligencia(s['tag'], j['league']['name'], f"{home} x {away}")
                            msg = f"<b>🚨 SINAL ENCONTRADO 🚨</b>\n\n🏆 <b>{j['league']['name']}</b>\n⚽ {home} 🆚 {away}\n⏰ <b>{tempo}' minutos</b> (Placar: {placar})\n\n🔥 {s['tag'].upper()}\n⚠️ <b>AÇÃO:</b> {s['ordem']}{destaque_odd}\n\n💰 <b>Odd: @{odd_atual_str}</b>{txt_pressao}\n📊 <i>Dados: {s['stats']}</i>\n⚽ <b>Médias (10j):</b> Casa {medias_gols['home']} | Fora {medias_gols['away']}{prob}{opiniao_txt}"
                            enviar_telegram(safe_token, safe_chat, msg)
                            st.toast(f"Sinal Enviado: {s['tag']}")
                        
                        elif uid_super not in st.session_state['alertas_enviados'] and odd_val >= 1.80:
                             st.session_state['alertas_enviados'].add(uid_super)
                             msg_super = (f"💎 <b>OPORTUNIDADE DE VALOR!</b>\n\n⚽ {home} 🆚 {away}\n📈 <b>A Odd subiu!</b> Entrada valorizada.\n🔥 <b>Estratégia:</b> {s['tag']}\n💰 <b>Nova Odd: @{odd_atual_str}</b>\n<i>O jogo mantém o padrão da estratégia.</i>{txt_pressao}")
                             enviar_telegram(safe_token, safe_chat, msg_super)
                
                # --- CORREÇÃO DO RADAR: Fica FORA do if lista_sinais ---
                # Agora ele adiciona o jogo mesmo se status for "Monitorando"
                radar.append({"Liga": nome_liga_show, "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})
            
            # --- MÚLTIPLAS E AGENDA (CORREÇÃO DE INDENTAÇÃO) ---
            # Estes blocos devem estar alinhados com o 'for j in jogos_live' (nível 12), e não dentro dele (nível 16).
            
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
