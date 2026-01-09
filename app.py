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
    .status-online {color: #00FF00; font-weight: bold; animation: pulse 2s infinite; padding: 10px; border: 1px solid #00FF00; text-align: center; border-radius: 15px;}
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.4); } 70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); } 100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); } }
    .timer-label { font-size: 11px; color: #888; text-align: right; }
    div[data-testid="stExpander"] { border: none !important; box-shadow: none !important; }
</style>
""", unsafe_allow_html=True)

# --- 2. BANCO DE DADOS E FUNÇÕES ---
DB_FILE = 'neves_dados.txt'

def carregar_db():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
    try: return pd.read_csv(DB_FILE)
    except: return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])

def enviar_teste_telegram(token, chat_ids):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid.strip(), "text": "✅ Teste OK!"})
            except: pass

# --- 3. SIDEBAR (CONFIGURAÇÕES) ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        if st.button("🔔 Testar Telegram"):
            enviar_teste_telegram(tg_token, tg_chat_ids)
            st.toast("Teste enviado!")
        INTERVALO = st.slider("Ciclo (seg):", 60, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        if st.button("🗑️ Resetar Banco"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            st.rerun()
    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 4. LÓGICA DE API (ECONOMIA ATIVA) ---
@st.cache_data(ttl=3600)
def buscar_proximos(key):
    if not key and not MODO_DEMO: return []
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"date": (datetime.utcnow() - timedelta(hours=3)).strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}
    try:
        res = requests.get(url, headers={"x-apisports-key": key}, params=params, timeout=10).json()
        return [j for j in res.get('response', []) if j['fixture']['status']['short'] == 'NS']
    except: return []

def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        if "statistics" in endpoint: return [{"statistics": [{"type": "Total Shots", "value": 5}]}, {"statistics": [{"type": "Total Shots", "value": 2}]}]
        return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 15}}, "league": {"id": 1, "name": "Liga Teste", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}]
    if not API_KEY: return []
    url = f"https://v3.football.api-sports.io/{endpoint}"
    try:
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

# --- 5. EXECUÇÃO PRINCIPAL ---
st.title("❄️ Neves Analytics")

# Função isolada para rodar o robô e evitar o espelhamento
@st.fragment(run_every=INTERVALO)
def monitor_loop():
    if ROBO_LIGADO:
        st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        # Coluna de Timer sutil
        c1, c2 = st.columns([5, 1])
        with c2: st.caption(f"Ciclo: {INTERVALO}s")
        st.progress(1.0) # Barra cheia para indicar refresh

        # Busca de Dados
        jogos_live = buscar_dados("fixtures", {"live": "all"})
        radar = []
        for j in jogos_live:
            tempo = j['fixture']['status'].get('elapsed', 0)
            sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
            radar.append({"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {sc}x{sf} {j['teams']['away']['name']}", "Tempo": f"{tempo}'", "Status": "👁️"})

        prox_lista = buscar_proximos(API_KEY)
        hist_df = carregar_db()

        # Abas
        t1, t2, t3, t4 = st.tabs([f"📡 Ao Vivo ({len(radar)})", f"📅 Próximos ({len(prox_lista)})", f"📊 Histórico ({len(hist_df)})", "🚫 Blacklist"])
        
        with t1: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True) if radar else st.info("Buscando...")
        with t2:
            if prox_lista:
                df_p = pd.DataFrame([{"Hora": j['fixture']['date'][11:16], "Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} vs {j['teams']['away']['name']}"} for j in prox_lista])
                st.dataframe(df_p.sort_values("Hora"), use_container_width=True, hide_index=True)
        with t3: st.dataframe(hist_df.sort_values(by=['hora'], ascending=False), use_container_width=True, hide_index=True) if not hist_df.empty else st.caption("Vazio.")
        with t4: st.caption("Ligas bloqueadas aparecerão aqui.")

    else:
        st.info("💡 Ligue o robô na barra lateral.")
        df_h = carregar_db()
        if not df_h.empty:
            st.subheader("📊 Resumo")
            st.dataframe(df_h.head(10), use_container_width=True, hide_index=True)

# Chama a função que se auto-atualiza sem dar rerun na página toda
monitor_loop()
