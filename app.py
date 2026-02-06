def consultar_ia_gemini(dados_jogo, estrategia, stats_raw, rh, ra, extra_context="", time_favoravel=""):
    if not IA_ATIVADA: return "", "N/A"
    try:
        # --- 1. Extração de Dados Brutos ---
        s1 = stats_raw[0]['statistics']; s2 = stats_raw[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        
        # Casa
        chutes_h = gv(s1, 'Total Shots'); gol_h = gv(s1, 'Shots on Goal')
        cantos_h = gv(s1, 'Corner Kicks'); atq_perigo_h = gv(s1, 'Dangerous Attacks')
        faltas_h = gv(s1, 'Fouls'); cards_h = gv(s1, 'Yellow Cards') + gv(s1, 'Red Cards')
        
        # Fora
        chutes_a = gv(s2, 'Total Shots'); gol_a = gv(s2, 'Shots on Goal')
        cantos_a = gv(s2, 'Corner Kicks'); atq_perigo_a = gv(s2, 'Dangerous Attacks')
        faltas_a = gv(s2, 'Fouls'); cards_a = gv(s2, 'Yellow Cards') + gv(s2, 'Red Cards')
        
        # Totais
        chutes_totais = chutes_h + chutes_a
        atq_perigo_total = atq_perigo_h + atq_perigo_a
        total_faltas = faltas_h + faltas_a
        total_chutes_gol = gol_h + gol_a
        
        tempo_str = str(dados_jogo.get('tempo', '0')).replace("'", "")
        tempo = int(tempo_str) if tempo_str.isdigit() else 1

        # --- CORREÇÃO DE DADOS (FALLBACK DE INTENSIDADE) ---
        # Se a API não entregar ataques perigosos, usamos os chutes para estimar a pressão
        usou_estimativa = False
        if atq_perigo_total == 0 and chutes_totais > 0:
            # Estimativa: 1 chute equivale a aprox 5 a 7 ataques perigosos em termos de métrica
            atq_perigo_total = int(chutes_totais * 6)
            usou_estimativa = True

        # --- 2. ENGENHARIA DE DADOS (KPIs) ---
        intensidade_jogo = atq_perigo_total / tempo if tempo > 0 else 0
        
        # Recalcula o status visual baseado na nova intensidade corrigida
        status_intensidade = "😐 MÉDIA"
        if intensidade_jogo > 1.0: status_intensidade = "🔥 ALTA"
        elif intensidade_jogo < 0.6: status_intensidade = "❄️ BAIXA"

        soma_atq = atq_perigo_h + atq_perigo_a
        dominancia_h = (atq_perigo_h / soma_atq * 100) if soma_atq > 0 else 50
        
        quem_manda = "EQUILIBRADO"
        if dominancia_h > 60: quem_manda = f"DOMÍNIO CASA ({dominancia_h:.0f}%)"
        elif dominancia_h < 40: quem_manda = f"DOMÍNIO VISITANTE ({100-dominancia_h:.0f}%)"

        # Define se a estratégia sugerida é de Under ou Over
        tipo_sugestao = "UNDER" if any(x in estrategia for x in ["Under", "Morno", "Arame", "Segurar"]) else "OVER"
        
        # Momento (Pressão nos últimos minutos)
        pressao_txt = "Neutro"
        if rh >= 3: pressao_txt = "CASA AMASSANDO"
        elif ra >= 3: pressao_txt = "VISITANTE AMASSANDO"
        
        # Aviso para a IA se usamos estimativa
        aviso_ia = ""
        if usou_estimativa:
            aviso_ia = "(NOTA TÉCNICA: Dados de Ataques Perigosos ausentes na API. Intensidade foi calculada baseada no volume de CHUTES. Confie nos Chutes.)"

        # --- 4. O PROMPT (A NOVA INTELIGÊNCIA) ---
        prompt = f"""
        ATUE COMO UM CIENTISTA DE DADOS DE FUTEBOL E TRADER ESPORTIVO.
        Analise a entrada: '{estrategia}' (Tipo: {tipo_sugestao}).
        {aviso_ia}

        VOCÊ DEVE CRUZAR O "MOMENTO" (O que está acontecendo agora) COM A "VERDADE" (Histórico de 50 jogos).
        
        🏟️ DADOS DO AO VIVO ({tempo} min | Placar: {dados_jogo['placar']}):
        - Intensidade Calculada: {intensidade_jogo:.2f}/min ({status_intensidade}).
        - Chutes Totais: {chutes_totais} | No Gol: {total_chutes_gol}
        - Cenário: {quem_manda} | {pressao_txt}
        
        📚 CONTEXTO HISTÓRICO (A VERDADE):
        {extra_context}
        
        -----------------------------------------------------------
        🧠 INTELIGÊNCIA DE DECISÃO:
        
        1. **ESTRATÉGIA GOL RELÂMPAGO/BLITZ:**
           - Se a estratégia é OVER e tem chutes ({chutes_totais}), **IGNORE** se a intensidade parecer baixa. Foque nos Chutes. aprove como **PADRÃO** ou **DIAMANTE**.
        
        2. **ARAME LISO (FALSA PRESSÃO)?**
           - Se tem muitos chutes mas poucos no gol, E o histórico mostra poucos gols -> **APROVAR UNDER**.

        3. **GIGANTE ACORDOU?**
           - Se a estratégia for "OVER" e o time começou a chutar no gol agora -> **APROVAR**.

        CLASSIFIQUE:
        💎 DIAMANTE: Leitura perfeita (Histórico + Momento batem).
        ✅ PADRÃO: Dados favoráveis.
        ⚠️ ARRISCADO: Contradição nos dados.
        ⛔ VETADO: Risco alto (Ex: Sugerir Under em jogo de time goleador).

        JSON: {{ "classe": "...", "probabilidade": "0-100", "motivo_tecnico": "..." }}
        """
        
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        st.session_state['gemini_usage']['used'] += 1
        
        txt_limpo = response.text.replace("```json", "").replace("```", "").strip()
        r_json = json.loads(txt_limpo)
        
        classe = r_json.get('classe', 'PADRAO').upper()
        prob_val = int(r_json.get('probabilidade', 70))
        motivo = r_json.get('motivo_tecnico', 'Análise baseada em KPIs.')
        
        emoji = "✅"
        if "DIAMANTE" in classe or (prob_val >= 85): emoji = "💎"; classe = "DIAMANTE"
        elif "ARRISCADO" in classe: emoji = "⚠️"
        elif "VETADO" in classe or prob_val < 60: emoji = "⛔"; classe = "VETADO"

        prob_str = f"{prob_val}%"
        
        # HTML para o Telegram
        html_analise = f"\n🤖 <b>IA LIVE (Híbrida):</b>\n{emoji} <b>{classe} ({prob_str})</b>\n"
        
        # Agora mostramos o dado correto de intensidade visualmente
        icone_int = "🔥" if status_intensidade == "🔥 ALTA" else "❄️"
        html_analise += f"📊 <i>Intensidade: {intensidade_jogo:.1f} {icone_int}</i>\n"
        html_analise += f"📝 <i>{motivo}</i>"
        
        return html_analise, prob_str

    except Exception as e: return "", "N/A"
