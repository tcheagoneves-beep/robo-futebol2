import streamlit as st
import pandas as pd
import requests
import time
import os
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Neves Analytics", layout="centered", page_icon="❄️")

st.markdown("""
<style>
    .stApp {background-color: #121212; color: white;}
    .card {background-color: #1E1E1E; padding: 20px; border-radius: 15px; border: 1px solid #333; margin-bottom: 20px;}
    .titulo-time {font-size: 20px; font-weight: bold; color: #ffffff;}
    .placar {font-size: 35px; font-weight: 800; color: #FFD700; text-align: center;}
    .tempo {font-size: 14px; color: #FF4B4B; font-weight: bold; text-align: center;}
    .status-online {color: #00FF00; font-weight: bold; animation: pulse 2s infinite; padding: 10px; border: 1px solid #00FF00; text-align: center; margin-bottom: 20px; border-radius: 15px;}
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.7); } 70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); } 100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); } }
    .stats-row { display: flex; justify-content: space-around; text-align: center; margin-top: 15px; padding-top: 15px; border-top: 1px solid #333; }
    .metric-val {font-size: 18px; font-weight: bold;}
    .metric-label {font-size: 10px; color: #888;}
</style>
""", unsafe_allow_html=True)

# --- 2. AUXILIARES E TRADUÇÃO ---
def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

def traduzir_instrucao(sinal, favorito=""):
    if "MÚLTIPLA" in sinal: return "Bilhete de 2 ou mais jogos."
    if "HT" in sinal: return "Mais um gol no 1º tempo."
    return "Entrada no mercado de gols."

# --- 3. BANCO DE DADOS ---
DB_FILE = 'neves_dados.txt'

def carregar_db():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
    try: return pd.read_csv(DB_FILE)
    except: return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])

def salvar_sinal_db(fixture_id, jogo, sinal, gols_inicial):
    df = carregar_db()
    if not ((df['id'] == fixture_id) & (df['sinal'] == sinal)).any():
        data_br = agora_brasil()
        novo = pd.DataFrame([{'id': fixture_id, 'data': data_br.strftime('%Y-%m-%d'), 'hora': data_br.strftime('%H:%M'), 'jogo': jogo, 'sinal': sinal, 'gols_inicial': gols_inicial, 'status': 'Pendente'}])
        df = pd.concat([df, novo], ignore_index=True)
        df.to_csv(DB_FILE, index=False)

# --- 4. SIDEBAR ---
with st.sidebar:
    st.header("❄️ Neves Analytics")
    API_KEY = st.text_input("Chave API-SPORTS:", type="password")
    tg_token = st.text_input("Telegram Token:", type="password")
    tg_chat_ids = st.text_input("Chat IDs:")
    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)
    INTERVALO = st.slider("Ciclo (seg):", 60, 300, 60)
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)

# --- 5. LÓGICA DE API ---
if 'ligas_sem_stats' not in st.session_state or isinstance(st.session_state['ligas_sem_stats'], set):
    st.session_state['ligas_sem_stats'] = {}

@st.cache_data(ttl=3600)
def buscar_proximos(key):
    if not key: return []
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"date": agora_brasil().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}
    try:
        res = requests.get(url, headers={"x-apisports-key": key}, params=params).json()
        return [j for j in res.get('response', []) if j['fixture']['status']['short'] == 'NS']
    except: return []

def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        if "statistics" in endpoint:
            return [{"statistics": [{"type": "Total Shots", "value": 10}, {"type": "Shots on Goal", "value": 4}, {"type": "Dangerous Attacks", "value": 30}]}, 
                    {"statistics": [{"type": "Total Shots", "value": 5}, {"type": "Shots on Goal", "value": 2}, {"type": "Dangerous Attacks", "value": 15}]}]
        return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 35}}, "league": {"id": 99, "name": "Liga Demo", "country": "Brasil"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}]
    
    url = f"https://v3.football.api-sports.io/{endpoint}"
    try:
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

# --- 6. CÉREBRO ---
def analisar(tempo, stats_list, sc, sf):
    def v(s_idx, k):
        try:
            items = stats_list[s_idx]['statistics']
            val = next((i['value'] for i in items if i['type'] == k), 0)
            return int(str(val).replace('%','')) if val else 0
        except: return 0
    
    c_tot = v(0, 'Total Shots'); f_tot = v(1, 'Total Shots')
    c_gol = v(0, 'Shots on Goal'); f_gol = v(1, 'Shots on Goal')
    
    sinal = None; mot = ""
    if tempo <= 30 and (sc + sf) >= 2:
        sinal = "CANDIDATO P/ MÚLTIPLA"; mot = "Ritmo intenso."
    elif 5 <= tempo <= 25 and (c_tot + f_tot) >= 5:
        sinal = "GOL HT"; mot = "Pressão inicial."
    
    return sinal, mot, (c_tot + f_tot), (c_gol + f_gol)

# --- 7. EXECUÇÃO ---
st.title("❄️ Neves Analytics")

if ROBO_LIGADO:
    st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
    
    jogos_live = buscar_dados("fixtures", {"live": "all"})
    radar = []
    
    for j in jogos_live:
        f_id = j['fixture']['id']
        l_id = str(j['league']['id'])
        tempo = j['fixture']['status'].get('elapsed', 0)
        sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
        
        info = {"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {sc}x{sf} {j['teams']['away']['name']}", "Tempo": f"{tempo}'", "Status": "👁️"}
        
        if l_id in st.session_state['ligas_sem_stats']:
            info["Status"] = "🚫 Bloqueada"
        else:
            stats = buscar_dados("statistics", {"fixture": f_id})
            if not stats:
                if (sc + sf) > 0:
                    st.session_state['ligas_sem_stats'][l_id] = {"País": j['league']['country'], "Liga": j['league']['name'], "Motivo": "Gols s/ Stats"}
                    info["Status"] = "🚫 Bloqueada"
                else: info["Status"] = "⏳ Aguardando"
            else:
                sinal, mot, ch, gol = analisar(tempo, stats, sc, sf)
                if sinal:
                    info["Status"] = f"✅ {sinal}"
                    salvar_sinal_db(f_id, info["Jogo"], sinal, sc+sf)
        radar.append(info)

    tab1, tab2, tab3, tab4 = st.tabs(["📡 Ao Vivo", "📅 Próximos", "📊 Histórico", "🚫 Bloqueadas"])
    
    with tab1:
        if radar: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
        else: st.info("Sem jogos ao vivo.")

    with tab2:
        prox = buscar_proximos(API_KEY)
        if prox:
            df_p = pd.DataFrame([{"Hora": j['fixture']['date'][11:16], "Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} vs {j['teams']['away']['name']}"} for j in prox])
            st.dataframe(df_p.sort_values("Hora"), use_container_width=True, hide_index=True)
        else: st.caption("Nenhum jogo futuro hoje.")

    with tab3:
        df_h = carregar_db()
        if not df_h.empty: st.dataframe(df_h.sort_values("hora", ascending=False), use_container_width=True, hide_index=True)
        else: st.caption("Sem sinais ainda.")

    with tab4:
        if st.session_state['ligas_sem_stats']: st.table(list(st.session_state['ligas_sem_stats'].values()))
        else: st.caption("Nenhuma liga na blacklist.")

    time.sleep(INTERVALO)
    st.rerun()
else:
    st.info("Ligue o robô na barra lateral para começar.")
