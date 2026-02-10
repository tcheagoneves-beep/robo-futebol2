"""
╔══════════════════════════════════════════════════════════════════╗
║  CORREÇÃO: FUNÇÃO processar() REBALANCEADA                      ║
║                                                                  ║
║  INSTRUÇÕES:                                                     ║
║  1. No seu código, procure "def processar("                      ║
║  2. Selecione TUDO desde "def processar(" até o próximo          ║
║     "# --- FIM PARTE" ou próxima função                          ║
║  3. Substitua pela função abaixo                                 ║
║                                                                  ║
║  O QUE FOI CORRIGIDO:                                            ║
║  • Vovô: MAIS RESTRITIVA (era a que disparava 90%)              ║
║  • Golden Bet: sog 5→3, bloqueados 5→3, chutes 18→14            ║
║  • Janela de Ouro: janela 70-75→65-78, chutes_gol 5→3           ║
║  • Porteira Aberta: janela 30→35, + caminho alternativo          ║
║  • Gol Relâmpago: tempo 12→15, chutes 4→3                       ║
║  • Blitz: sh 10→7, rh 3→2, removido filtro post_h               ║
║  • Tiroteio Elite: janela 15-25→12-30, chutes 8→6, sog 4→3     ║
║  • Lay Goleada: chutes 16→12, diff 3→2                          ║
║  • Sniper Final: rh 5→3, chutes_gol 6→4, fora 6→8              ║
║  • Jogo Morno: domínio 60→65, sog 3→4                           ║
║  • RECUPERADAS: Massacre, Choque Líderes, Briga de Rua,         ║
║    Contra-Ataque Letal (estavam AUSENTES no código!)             ║
╚══════════════════════════════════════════════════════════════════╝
"""


def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0
        
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        ck_h = get_v(stats_h, 'Corner Kicks'); ck_a = get_v(stats_a, 'Corner Kicks')
        blk_h = get_v(stats_h, 'Blocked Shots'); blk_a = get_v(stats_a, 'Blocked Shots')

        total_chutes = sh_h + sh_a
        total_chutes_gol = sog_h + sog_a
        total_bloqueados = blk_h + blk_a
        chutes_fora_h = max(0, sh_h - sog_h - blk_h)
        chutes_fora_a = max(0, sh_a - sog_a - blk_a)
        total_fora = chutes_fora_h + chutes_fora_a
        
        posse_h = 50
        try: posse_h = int(str(next((x['value'] for x in stats_h if x['type']=='Ball Possession'), "50%")).replace('%', ''))
        except: pass
        posse_a = 100 - posse_h
        
        rh, ra = momentum(j['fixture']['id'], sog_h, sog_a)
        gh = j['goals']['home']; ga = j['goals']['away']; total_gols = gh + ga

        def gerar_ordem_gol(gols_atuais, tipo="Over"):
            linha = gols_atuais + 0.5
            if tipo == "Over": return f"👉 <b>FAZER:</b> Entrar em GOLS (Over)\n✅ Aposta: <b>Mais de {linha} Gols</b>"
            elif tipo == "HT": return f"👉 <b>FAZER:</b> Entrar em GOLS 1º TEMPO\n✅ Aposta: <b>Mais de 0.5 Gols HT</b>"
            elif tipo == "Limite": return f"👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de {gols_atuais + 1.0} Gols</b> (Asiático)"
            return "Apostar em Gols."

        SINAIS = []
        golden_bet_ativada = False

        # =================================================================
        # 1. GOLDEN BET
        #    ANTES: sog>=5, bloqueados>=5, chutes>=18, tempo 65-75
        #    AGORA: sog>=3, bloqueados>=3, chutes>=14, tempo 60-78
        # =================================================================
        if 60 <= tempo <= 78:
            pressao_real_h = (rh >= 2 and sog_h >= 3)
            pressao_real_a = (ra >= 2 and sog_a >= 3)
            if (pressao_real_h or pressao_real_a) and (total_gols >= 1 or total_chutes >= 14):
                if total_bloqueados >= 3:
                    SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"🛡️ {total_bloqueados} Bloqueios (Pressão Real)", "rh": rh, "ra": ra, "favorito": "GOLS"})
                    golden_bet_ativada = True

        # =================================================================
        # 2. JANELA DE OURO
        #    ANTES: 70-75 min, chutes_gol>=5
        #    AGORA: 65-78 min, chutes_gol>=3
        # =================================================================
        if not golden_bet_ativada and (65 <= tempo <= 78) and abs(gh - ga) <= 1:
            if total_chutes_gol >= 3:
                SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"🔥 {total_chutes_gol} Chutes no Gol", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # =================================================================
        # 3. JOGO MORNO
        #    ANTES: domínio se posse>60 OU sog>3, chutes<=10, sog<=2
        #    AGORA: domínio se posse>65 OU sog>4, chutes<=12, sog<=3
        # =================================================================
        dominio_claro = (posse_h > 65 or posse_a > 65) or (sog_h > 4 or sog_a > 4)
        if 50 <= tempo <= 78 and total_chutes <= 12 and (sog_h + sog_a) <= 3 and gh == ga and not dominio_claro:
            SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": f"👉 <b>FAZER:</b> Under Gols\n✅ Aposta: <b>Menos de {total_gols + 0.5} Gols</b>", "stats": "Jogo Travado", "rh": rh, "ra": ra, "favorito": "UNDER"})

        # =================================================================
        # 4. VOVÔ — MAIS RESTRITIVA (disparava 90% dos sinais)
        #    ANTES: 75-85, ra<1, posse>=45, chutes<18 (muito fácil!)
        #    AGORA: 78-88, ra<1, posse>=50, chutes 5-16, sog>=2 do vencedor
        # =================================================================
        if 78 <= tempo <= 88 and total_chutes >= 5 and total_chutes <= 16:
            diff = gh - ga
            if diff == 1 and ra < 1 and posse_h >= 50 and sog_h >= 2:
                SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito (Segurar)\n✅ Aposta: <b>Vitória Seca</b>", "stats": "Controle Total", "rh": rh, "ra": ra, "favorito": "FAVORITO"})
            elif diff == -1 and rh < 1 and posse_a >= 50 and sog_a >= 2:
                SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito (Segurar)\n✅ Aposta: <b>Vitória Seca</b>", "stats": "Controle Total", "rh": rh, "ra": ra, "favorito": "FAVORITO"})

        # =================================================================
        # 5. PORTEIRA ABERTA
        #    ANTES: tempo<=30, gols>=2, sog_h>=1 AND sog_a>=1
        #    AGORA: tempo<=35, + caminho alternativo (1 gol + volume)
        # =================================================================
        if tempo <= 35:
            if total_gols >= 2 and (sog_h >= 1 and sog_a >= 1):
                SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": gerar_ordem_gol(total_gols), "stats": "Jogo Aberto", "rh": rh, "ra": ra, "favorito": "GOLS"})
            elif total_gols >= 1 and total_chutes >= 6 and total_chutes_gol >= 3:
                SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": gerar_ordem_gol(total_gols), "stats": "Jogo Ofensivo", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # =================================================================
        # 6. GOL RELÂMPAGO
        #    ANTES: tempo<=12, chutes>=4
        #    AGORA: tempo<=15, chutes>=3
        # =================================================================
        if total_gols == 0 and (tempo <= 15 and total_chutes >= 3):
            SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": gerar_ordem_gol(0, "HT"), "stats": "Início Intenso", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # =================================================================
        # 7. BLITZ
        #    ANTES: sh>=10 OU rh>=3, post_h==0
        #    AGORA: sh>=7 OU rh>=2, sog>=2, sem filtro post
        # =================================================================
        if tempo <= 65:
            blitz_casa = (gh <= ga) and (rh >= 2 or sh_h >= 7) and sog_h >= 2
            blitz_fora = (ga <= gh) and (ra >= 2 or sh_a >= 7) and sog_a >= 2
            if blitz_casa:
                SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": gerar_ordem_gol(total_gols), "stats": "Pressão Limpa", "rh": rh, "ra": ra, "favorito": "GOLS"})
            elif blitz_fora:
                SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": gerar_ordem_gol(total_gols), "stats": "Pressão Limpa", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # =================================================================
        # 8. TIROTEIO ELITE
        #    ANTES: 15-25 min, chutes>=8, sog>=4
        #    AGORA: 12-30 min, chutes>=6, sog>=3
        # =================================================================
        if 12 <= tempo <= 30 and total_chutes >= 6 and (sog_h + sog_a) >= 3:
            SINAIS.append({"tag": "🏹 Tiroteio Elite", "ordem": gerar_ordem_gol(total_gols), "stats": "Muitos Chutes", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # =================================================================
        # 9. LAY GOLEADA
        #    ANTES: 60-88, diff>=3, chutes>=16, sog>=2
        #    AGORA: 55-88, diff>=2, chutes>=12, sog>=1
        # =================================================================
        if 55 <= tempo <= 88 and abs(gh - ga) >= 2 and (total_chutes >= 12):
            time_perdendo_chuta = (gh < ga and sog_h >= 1) or (ga < gh and sog_a >= 1)
            if time_perdendo_chuta:
                SINAIS.append({"tag": "🔫 Lay Goleada", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": "Goleada Viva", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # =================================================================
        # 10. SNIPER FINAL
        #     ANTES: tempo>=80, rh>=5 OU chutes_gol>=6 OU ra>=5, fora<=6
        #     AGORA: tempo>=78, rh>=3 OU chutes_gol>=4 OU ra>=3, fora<=8
        # =================================================================
        if tempo >= 78 and abs(gh - ga) <= 1:
            if total_fora <= 8 and ((rh >= 3) or (total_chutes_gol >= 4) or (ra >= 3)):
                SINAIS.append({"tag": "💎 Sniper Final", "ordem": "👉 <b>FAZER:</b> Over Gol Limite\n✅ Busque o Gol no Final", "stats": "Pontaria Ajustada", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # =================================================================
        # 11. MASSACRE — RECUPERADA (estava AUSENTE no seu código!)
        #     Time dominando absurdamente no 1º tempo
        # =================================================================
        if 20 <= tempo <= 45 and total_gols >= 1:
            if (sh_h >= 8 and sog_h >= 4 and posse_h >= 60) or (sh_a >= 8 and sog_a >= 4 and posse_a >= 60):
                SINAIS.append({"tag": "🔥 Massacre", "ordem": gerar_ordem_gol(total_gols, "HT"), "stats": "Domínio Absoluto", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # =================================================================
        # 12. CHOQUE DE LÍDERES — RECUPERADA (estava AUSENTE!)
        #     Jogo equilibrado com muito volume dos dois lados
        # =================================================================
        if 15 <= tempo <= 45 and abs(posse_h - posse_a) <= 10:
            if sog_h >= 2 and sog_a >= 2 and total_chutes >= 8:
                SINAIS.append({"tag": "⚔️ Choque Líderes", "ordem": gerar_ordem_gol(total_gols, "HT"), "stats": "Equilíbrio Ofensivo", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # =================================================================
        # 13. BRIGA DE RUA — RECUPERADA (estava AUSENTE!)
        #     Jogo agressivo com muitas faltas e pressão
        # =================================================================
        if 20 <= tempo <= 45:
            faltas_h = get_v(stats_h, 'Fouls'); faltas_a = get_v(stats_a, 'Fouls')
            total_faltas = faltas_h + faltas_a
            if total_faltas >= 12 and total_chutes >= 6 and (sog_h + sog_a) >= 2:
                SINAIS.append({"tag": "🥊 Briga de Rua", "ordem": gerar_ordem_gol(total_gols, "HT"), "stats": f"🔥 {total_faltas} Faltas", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # =================================================================
        # 14. CONTRA-ATAQUE LETAL — RECUPERADA (estava AUSENTE!)
        #     Time perdendo mas com chutes perigosos (Back Zebra)
        # =================================================================
        if 30 <= tempo <= 70:
            if gh < ga and sog_h >= 3 and sh_h >= 5:
                SINAIS.append({"tag": "⚡ Contra-Ataque Letal", "ordem": "👉 <b>FAZER:</b> Back Empate ou Zebra\n✅ Aposta: <b>Mandante (Recuperação)</b>", "stats": "Pressão do Perdedor", "rh": rh, "ra": ra, "favorito": "ZEBRA"})
            elif ga < gh and sog_a >= 3 and sh_a >= 5:
                SINAIS.append({"tag": "⚡ Contra-Ataque Letal", "ordem": "👉 <b>FAZER:</b> Back Empate ou Zebra\n✅ Aposta: <b>Visitante (Recuperação)</b>", "stats": "Pressão do Perdedor", "rh": rh, "ra": ra, "favorito": "ZEBRA"})

        return SINAIS
    except: return []
