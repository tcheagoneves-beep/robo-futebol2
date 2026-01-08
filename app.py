import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Robô Gols - Estratégia VIP", layout="wide", page_icon="⚽")

# --- BARRA LATERAL ---
with st.sidebar:
    st.header("⚙️ Configurações")
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("Sua API Key:", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.info("Estratégia carregada: 'Over Gols' e 'Pressão do Favorito'.")

# --- LISTA DE LIGAS (SEU FILTRO VIP) ---
# Mapeamos os IDs das ligas que você citou como boas
LIGAS_VIP = {
    # EUROPA PRINCIPAIS
    39: "Inglaterra - Premier League",
    40: "Inglaterra - Championship", # 2ª Divisão
    78: "Alemanha - Bundesliga 1",
    79: "Alemanha - Bundesliga 2",
    140: "Espanha - La Liga",
    141: "Espanha - La Liga 2",
    94: "Portugal - Primeira Liga",
    88: "Holanda - Eredivisie", # Com cuidado
    179: "Escócia - Premiership", # Olho em Rangers/Celtic
    103: "Noruega - Eliteserien",
    
    # MUNDO / ASIÁTICOS / EMERGENTES
    307: "Arábia Saudita - Pro League",
    201: "Emirados Árabes - Pro League",
    203: "Turquia - Süper Lig",
    169: "China - Super League",
    98: "Japão - J1 League",
    292: "Coreia do Sul - K League 1",
    
    # BRASIL
    71: "Brasil - Série A",
    72: "Brasil - Série B",
    
    # COPAS
    2: "Champions League",
    3: "Europa League",
    13: "Libertadores",
    11: "Sul-Americana"
}

# --- FUNÇÕES DE CONEXÃO ---

def buscar_jogos_do_dia(api_key):
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"date": datetime.today().strftime('%Y-%m-%d')}
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try:
        return requests.get(url, headers=headers, params=querystring).json().get('response', [])
    except: return []

def buscar_classificacao(api_key, league_id, season):
    url = "https://api-football-v1.p.rapidapi.com/v3/standings"
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try:
        data = requests.get(url, headers=headers, params={"league": league_id, "season": season}).json()
        if data.get('response'): return data['response'][0]['league']['standings'][0]
        return []
    except: return []

def buscar_stats_live(api_key, fixture_id):
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try:
        return requests.get(url, headers=headers, params={"fixture": fixture_id}).json().get('response', [])
    except: return []

# --- CÉREBRO 1: PRÉ-JOGO (A SUA CONTA DE MÉDIA DE GOLS) ---
def analisar_matematica_gols(time_stats, jogos_jogados):
    # Sua regra: (Gols Feitos + Gols Sofridos) / Jogos
    # Se der alto (> 2.5 ou 3.0), é jogo pra Over.
    if jogos_jogados < 3: return 0 # Amostra muito pequena
    
    gols_total_evento = time_stats['goals']['for'] + time_stats['goals']['against']
    media = gols_total_evento / jogos_jogados
    return media

# --- CÉREBRO 2: AO VIVO (PRESSÃO E REGRAS DE TEMPO) ---
def analisar_pressao_live(tempo, placar, stats_casa, stats_fora):
    
    # Extrair valores seguros
    def get_v(s, k): 
        v = s.get(k, 0)
        return int(v.replace('%','')) if isinstance(v, str) else (v or 0)

    chutes_gol_casa = get_v(stats_casa, 'Shots on Goal')
    chutes_gol_fora = get_v(stats_fora, 'Shots on Goal')
    chutes_total_casa = get_v(stats_casa, 'Total Shots')
    chutes_total_fora = get_v(stats_fora, 'Total Shots')
    atk_p_casa = get_v(stats_casa, 'Dangerous Attacks')
    atk_p_fora = get_v(stats_fora, 'Dangerous Attacks')

    msg = "Aguardar"
    cor = "grey"
    
    soma_chutes_gol = chutes_gol_casa + chutes_gol_fora

    # --- REGRA 1: INÍCIO DO JOGO (OS 6 MINUTOS) ---
    # "Nos primeiros 5-6 min já tem que ter chute em gol."
    if 4 <= tempo <= 12:
        if soma_chutes_gol >= 1 or (chutes_total_casa + chutes_total_fora) >= 3:
            msg = "⚡ INÍCIO ELETRIZANTE: Já tem chute! (Over HT Potencial)"
            cor = "green"
        else:
            msg = "⚠️ INÍCIO LENTO: Sem chutes ainda. Cuidado."
            cor = "orange"

    # --- REGRA 2: A REVOLTA DO FAVORITO (BASEADO NO SEU PRINT AL-AIN) ---
    # Se o jogo já rodou (ex: 20min+) e tem MUITO chute, o gol vai sair.
    elif tempo > 15:
        # Pressão Absurda (Tipo Al Ain com 16 finalizações)
        soma_total_chutes = chutes_total_casa + chutes_total_fora
        
        if soma_total_chutes >= 12: # Jogo muito aberto
            if atk_p_casa > (atk_p_fora * 2) or atk_p_fora > (atk_p_casa * 2):
                msg = "🔥 PRESSÃO ESMAGADORA: Um time está amassando! (Gol Maduro)"
                cor = "red"
            else:
                msg = "💰 JOGO ABERTO: Lá e cá (Muitos chutes)"
                cor = "green"

    return msg, cor, (chutes_total_casa + chutes_total_fora)

# --- INTERFACE ---
st.title("🤖 Robô Trader: Mapeamento de Gols")

tab1, tab2 = st.tabs(["1. Filtro Matemático (Pré)", "2. Radar Ao Vivo"])

# --- ABA 1: PRÉ-JOGO ---
with tab1:
    if st.button("Buscar Melhores Jogos (Filtro Ligas VIP)"):
        if not API_KEY: st.error("Coloque a API Key!")
        else:
            with st.spinner("Analisando médias de gols nas ligas selecionadas..."):
                todos = buscar_jogos_do_dia(API_KEY)
                alvos = []
                
                # Barra de progresso para não parecer travado
                bar = st.progress(0)
                processados = 0
                
                # Filtrar só as ligas VIP
                jogos_vip = [j for j in todos if j['league']['id'] in LIGAS_VIP]
                
                if not jogos_vip:
                    st.warning("Nenhum jogo das Ligas VIP hoje. Tentando buscar jogos gerais...")
                    jogos_vip = todos[:20] # Fallback

                for jogo in jogos_vip:
                    if jogo['fixture']['status']['short'] not in ['FT', 'AET', 'PEN']:
                        lid = jogo['league']['id']
                        season = jogo['league']['season']
                        
                        # Busca Tabela para fazer a conta
                        tabela = buscar_classificacao(API_KEY, lid, season)
                        
                        if tabela:
                            idc = jogo['teams']['home']['id']
                            idf = jogo['teams']['away']['id']
                            
                            # Acha os times na tabela
                            time_c = next((t for t in tabela if t['team']['id']==idc), None)
                            time_f = next((t for t in tabela if t['team']['id']==idf), None)
                            
                            if time_c and time_f:
                                # APLICA SUA MATEMÁTICA
                                media_c = analisar_matematica_gols(time_c['all'], time_c['all']['played'])
                                media_f = analisar_matematica_gols(time_f['all'], time_f['all']['played'])
                                
                                media_jogo = (media_c + media_f) / 2
                                
                                # Filtro: Só mostra se a média combinada for > 2.5 (Jogos de gols)
                                if media_jogo > 2.5:
                                    alvos.append({
                                        'id': jogo['fixture']['id'],
                                        'liga': LIGAS_VIP.get(lid, jogo['league']['name']),
                                        'jogo': f"{jogo['teams']['home']['name']} x {jogo['teams']['away']['name']}",
                                        'hora': jogo['fixture']['date'][11:16],
                                        'media_gols': f"{media_jogo:.2f}",
                                        'status': 'Pendente'
                                    })
                    
                    processados += 1
                    bar.progress(min(processados / len(jogos_vip), 1.0))

                st.session_state['lista_gols'] = alvos
                st.success(f"{len(alvos)} Jogos com Alta Média de Gols Encontrados!")
                
    if 'lista_gols' in st.session_state and st.session_state['lista_gols']:
        df = pd.DataFrame(st.session_state['lista_gols'])
        st.dataframe(df[['hora', 'liga', 'jogo', 'media_gols']], use_container_width=True)

# --- ABA 2: AO VIVO ---
with tab2:
    if st.button("📡 Analisar Jogos Selecionados Agora"):
        if 'lista_gols' not in st.session_state: st.warning("Faça o filtro na Aba 1 primeiro.")
        else:
            for alvo in st.session_state['lista_gols']:
                stats_raw = buscar_stats_live(API_KEY, alvo['id'])
                
                # Simular tempo/placar se não tiver na chamada stats (Limitação API Free)
                # Na prática real, faríamos uma chamada '/fixtures' para pegar o tempo exato
                # Aqui vamos tentar inferir ou mostrar os dados crus
                
                if stats_raw:
                    st.markdown(f"### {alvo['jogo']}")
                    st.caption(f"Média Pré-Live: {alvo['media_gols']} gols/jogo")
                    
                    s_casa = {i['type']: i['value'] for i in stats_raw[0]['statistics']}
                    s_fora = {i['type']: i['value'] for i in stats_raw[1]['statistics']}
                    
                    # Vamos assumir um tempo fictício para teste ou tentar pegar se disponível
                    # Como não temos o tempo real na rota /statistics, o usuário olha o tempo na Bet365
                    # e o Robô diz se vale a pena baseado nos chutes.
                    
                    # CÁLCULO DE PRESSÃO
                    chutes_c = s_casa.get('Total Shots', 0) or 0
                    chutes_f = s_fora.get('Total Shots', 0) or 0
                    chutes_gol_c = s_casa.get('Shots on Goal', 0) or 0
                    chutes_gol_f = s_fora.get('Shots on Goal', 0) or 0
                    
                    total_chutes = chutes_c + chutes_f
                    
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Chutes Totais", total_chutes)
                    c2.metric("No Gol (Casa)", chutes_gol_c)
                    c3.metric("No Gol (Fora)", chutes_gol_f)
                    
                    # LÓGICA DO AL-AIN (FAVORITO AMASSANDO)
                    if total_chutes >= 10:
                        st.error("🔥 JOGO PEGANDO FOGO! Muitos chutes. Alta chance de gol.")
                    elif total_chutes >= 2 and total_chutes < 5:
                        st.warning("⚠️ Jogo Travado. Poucos chutes.")
                    else:
                        st.info("Jogo morno ou no início.")
                        
                    st.divider()
                else:
                    # Se não tem stats, o jogo pode não ter começado
                    pass
            st.write("Fim da análise ao vivo.")
