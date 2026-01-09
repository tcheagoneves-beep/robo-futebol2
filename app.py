import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Neves Analytics", layout="centered", page_icon="❄️")

st.markdown("""
<style>
    .stApp {background-color: #121212; color: white;}
    .status-online {color: #00FF00; font-weight: bold; animation: pulse 2s infinite; padding: 10px; border: 1px solid #00FF00; text-align: center; margin-bottom: 10px; border-radius: 15px;}
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.4); } 70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); } 100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); } }
    .timer-text { font-size: 11px; color: #555; text-align: right; margin-top: -10px; }
    /* Esconder o menu padrão do Streamlit para um look mais clean */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# --- 2. AUXILIARES ---
def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

# --- 3. BANCO DE DADOS ---
DB_FILE = 'neves_dados.txt'

def carregar_db():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
    try: return pd.read_csv(DB_FILE)
    except: return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])

def salvar_sinal_db(fixture_id, jogo, sinal, gols_inicial):
    df = carregar_db()
    if not ((df['id'].astype(str) == str(fixture_id)) & (df['sinal'] == sinal)).any():
        data_br = agora_brasil()
        novo = pd.DataFrame([{'id': fixture_id, 'data': data_br.strftime('%Y-%m-%d'), 'hora': data_br.strftime('%H:%M'), 'jogo': jogo, 'sinal': sinal, 'gols_inicial': gols_inicial, 'status': 'Pendente'}])
        df = pd.concat([df, novo], ignore_index=True)
        df.to_csv(DB_FILE, index=False)

# --- 4. SIDEBAR (RETRAÍDA) ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        INTERVALO = st.slider("Ciclo (seg):", 60, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        if st.button("🗑️ Resetar Banco"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 5. LÓGICA DE API ---
if 'ligas_sem_stats' not in st.session_state:
    st.session_state['ligas_sem_stats'] = {}

@st.cache_data(ttl=3600)
def buscar_proximos(key):
    if not key and not MODO_DEMO: return []
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"date": agora_brasil().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}
    try:
        res = requests.get(url, headers={"x-apisports-key": key}, params=params, timeout=10).json()
        return [j for j in res.get('response', []) if j['fixture']['status']['short'] == 'NS']
    except: return []

def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        if "statistics" in endpoint: return [{"statistics": [{"type": "Total Shots", "value": 5}]}, {"statistics": [{"type": "Total Shots", "value": 2}]}]
        return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 10}}, "league": {"id": 1, "name": "Liga Teste", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}]
    if not API_KEY: return []
    url = f"https://v3.football.api-sports.io/{endpoint}"
    try:
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

# --- 6. EXECUÇÃO PRINCIPAL ---
st.title("❄️ Neves Analytics")

# Criamos um container principal que será limpo a cada ciclo
main_container = st.empty()

if ROBO_LIGADO:
    with main_container.container():
        st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        # Área do Timer
        t_col1, t_col2 = st.columns([4, 1])
        with t_col2:
            timer_placeholder = st.empty()
        bar_placeholder = st.empty()
        
        # Busca de Dados
        jogos_live = buscar_dados("fixtures", {"live": "all"})
        radar = []
        
        for j in jogos_live:
            f_id, l_id = j['fixture']['id'], str(j['league']['id'])
            tempo = j['fixture']['status'].get('elapsed', 0)
            sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
            info = {"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {sc}x{sf} {j['teams']['away']['name']}", "Tempo": f"{tempo}'", "Status": "👁️"}
            
            if l_id in st.session_state['ligas_sem_stats']:
                info["Status"] = "🚫"
            elif (5 <= tempo <= 30) or (sc+sf > 0):
                stats = buscar_dados("statistics", {"fixture": f_id})
                if not stats and (sc+sf > 0):
                    st.session_state['ligas_sem_stats'][l_id] = {"País": j['league']['country'], "Liga": j['league']['name']}
                    info["Status"] = "🚫"
            radar.append(info)

        prox_lista = buscar_proximos(API_KEY)
        hist_df = carregar_db()
        black_count = len(st.session_state['ligas_sem_stats'])

        # --- ABAS ---
        t1, t2, t3, t4 = st.tabs([
            f"📡 Ao Vivo ({len(radar)})", 
            f"📅 Próximos ({len(prox_lista)})", 
            f"📊 Histórico ({len(hist_df)})", 
            f"🚫 Blacklist ({black_count})"
        ])
        
        with t1:
            if radar: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos ao vivo...")

        with t2:
            if prox_lista:
                df_p = pd.DataFrame([{"Hora": j['fixture']['date'][11:16], "Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} vs {j['teams']['away']['name']}"} for j in prox_lista])
                st.dataframe(df_p.sort_values("Hora"), use_container_width=True, hide_index=True)
            else: st.caption("Nenhum jogo futuro para hoje.")

        with t3:
            if not hist_df.empty: st.dataframe(hist_df.sort_values(by=['data', 'hora'], ascending=False), use_container_width=True, hide_index=True)
            else: st.caption("Nenhum sinal no histórico.")

        with t4:
            if st.session_state['ligas_sem_stats']: st.table(list(st.session_state['ligas_sem_stats'].values()))
            else: st.caption("Nenhuma liga na lista negra.")

        # Ciclo de espera com Timer
        for i in range(INTERVALO, 0, -1):
            timer_placeholder.markdown(f'<div class="timer-text">{i}s</div>', unsafe_allow_html=True)
            bar_placeholder.progress(i / INTERVALO)
            time.sleep(1)
        
        st.rerun()

else:
    with main_container.container():
        st.info("💡 Robô em espera. Ligue o monitoramento na barra lateral.")
        df_h = carregar_db()
        if not df_h.empty:
            st.subheader("📊 Últimos Sinais")
            st.dataframe(df_h.sort_values(by=['data', 'hora'], ascending=False).head(10), use_container_width=True, hide_index=True)
