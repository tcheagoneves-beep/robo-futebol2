import streamlit as st
import pandas as pd
import requests
import random
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Robô Gols - Demo", layout="wide", page_icon="⚽")

# --- BARRA LATERAL ---
with st.sidebar:
    st.header("⚙️ Configurações")
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("Sua API Key (RapidAPI):", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.markdown("---")
    MODO_DEMO = st.checkbox("🛠️ Forçar Modo Simulação", value=False, help="Marca isso se sua API Key ainda estiver 'Pending'. Cria jogos fictícios para teste.")
    
    st.info("Estratégia: Over Gols & Pressão HT/FT")

# LIGAS VIP
LIGAS_VIP = [39, 40, 78, 79, 140, 141, 94, 88, 179, 103, 307, 201, 203, 169, 98, 292, 71, 72, 2, 3, 13, 11]

# --- FUNÇÕES ---

def buscar_jogos_do_dia(api_key):
    # SE TIVER NO MODO DEMO, GERA DADOS FALSOS
    if MODO_DEMO:
        return gerar_jogos_falsos()

    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"date": datetime.today().strftime('%Y-%m-%d')}
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try:
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code == 200:
            return response.json().get('response', [])
        else:
            return [] # Retorna vazio se der erro (ex: Pending Approval)
    except: return []

def buscar_stats_live(api_key, fixture_id):
    if MODO_DEMO:
        return gerar_stats_falsos()

    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try:
        return requests.get(url, headers=headers, params={"fixture": fixture_id}).json().get('response', [])
    except: return []

# --- GERADORES DE DADOS FALSOS (PARA VOCÊ VER O LAYOUT) ---
def gerar_jogos_falsos():
    return [
        {
            "fixture": {"id": 111, "date": "2026-01-08T15:00:00", "status": {"short": "1H"}},
            "league": {"id": 39, "name": "Premier League", "season": 2025},
            "teams": {"home": {"id": 1, "name": "Arsenal"}, "away": {"id": 2, "name": "Liverpool"}},
            "demo_analise": "Média de Gols: 3.20 (JOGO TOP)"
        },
        {
            "fixture": {"id": 222, "date": "2026-01-08T16:00:00", "status": {"short": "NS"}},
            "league": {"id": 201, "name": "UAE Pro League", "season": 2025},
            "teams": {"home": {"id": 3, "name": "Al Ain"}, "away": {"id": 4, "name": "Al Dhafra"}},
            "demo_analise": "Média de Gols: 2.80 (Favorito em Casa)"
        }
    ]

def gerar_stats_falsos():
    # Simula o jogo do Arsenal com pressão absurda (Over)
    return [
        {"team": {"name": "Casa"}, "statistics": [{"type": "Total Shots", "value": 15}, {"type": "Shots on Goal", "value": 8}, {"type": "Dangerous Attacks", "value": 45}, {"type": "Ball Possession", "value": "60%"}]},
        {"team": {"name": "Fora"}, "statistics": [{"type": "Total Shots", "value": 5}, {"type": "Shots on Goal", "value": 2}, {"type": "Dangerous Attacks", "value": 10}, {"type": "Ball Possession", "value": "40%"}]}
    ]

# --- INTERFACE ---
st.title("🤖 Robô Trader: Scanner de Gols")

if MODO_DEMO:
    st.warning("⚠️ MODO SIMULAÇÃO ATIVO: Estes dados são fictícios para testar o painel enquanto sua API libera.")

tab1, tab2 = st.tabs(["1. Filtro do Dia", "2. Radar Ao Vivo"])

# --- ABA 1 ---
with tab1:
    if st.button("Buscar Jogos"):
        with st.spinner("Processando..."):
            todos = buscar_jogos_do_dia(API_KEY)
            
            if not todos and not MODO_DEMO:
                st.error("A API retornou 0 jogos. Provavelmente sua chave ainda está 'Pending'.")
                st.info("💡 DICA: Ative o 'Forçar Modo Simulação' na barra lateral para testar o painel agora!")
            else:
                # Se for demo, cria lista direto
                alvos = []
                for jogo in todos:
                    # Se for real, faz filtro. Se for demo, pega o dado pronto
                    analise = jogo.get('demo_analise', "Análise Pendente (API Real)")
                    
                    alvos.append({
                        'id': jogo['fixture']['id'],
                        'liga': jogo['league']['name'],
                        'jogo': f"{jogo['teams']['home']['name']} x {jogo['teams']['away']['name']}",
                        'hora': jogo['fixture']['date'][11:16],
                        'analise': analise
                    })
                
                st.session_state['lista_gols'] = alvos
                st.success(f"{len(alvos)} Jogos Encontrados!")
                st.dataframe(pd.DataFrame(alvos))

# --- ABA 2 ---
with tab2:
    if st.button("📡 Analisar Ao Vivo (Chutes & Pressão)"):
        if 'lista_gols' not in st.session_state or not st.session_state['lista_gols']:
            st.warning("Rode a Aba 1 primeiro!")
        else:
            for alvo in st.session_state['lista_gols']:
                st.markdown(f"#### {alvo['jogo']} ({alvo['hora']})")
                
                stats_raw = buscar_stats_live(API_KEY, alvo['id'])
                
                if stats_raw:
                    s_casa = {i['type']: i['value'] for i in stats_raw[0]['statistics']}
                    s_fora = {i['type']: i['value'] for i in stats_raw[1]['statistics']}
                    
                    def v(d, k): 
                        val = d.get(k, 0)
                        if val is None: return 0
                        return int(str(val).replace('%',''))
                    
                    chutes = v(s_casa, 'Total Shots') + v(s_fora, 'Total Shots')
                    no_gol = v(s_casa, 'Shots on Goal') + v(s_fora, 'Shots on Goal')
                    
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Chutes Totais", chutes)
                    c2.metric("Chutes no Gol", no_gol)
                    c3.metric("Ataques Perigosos", v(s_casa, 'Dangerous Attacks') + v(s_fora, 'Dangerous Attacks'))
                    
                    if chutes >= 12 or no_gol >= 6:
                        st.error("🔥 JOGO PEGANDO FOGO! (Pressão Alta confirmada)")
                    else:
                        st.info("Jogo Morno.")
                
                st.divider()
