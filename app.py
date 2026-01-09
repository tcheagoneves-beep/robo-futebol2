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
    .status-online {color: #00FF00; font-weight: bold; animation: pulse 2s infinite; padding: 10px; border: 1px solid #00FF00; text-align: center; border-radius: 15px; margin-bottom: 20px;}
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.4); } 70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); } 100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); } }
    .timer-label { font-size: 11px; color: #888; text-align: right; }
</style>
""", unsafe_allow_html=True)

# --- 2. FUNÇÕES DE APOIO ---
def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

def enviar_teste_telegram(token, chat_ids):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try:
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                requests.post(url, data={"chat_id": cid.strip(), "text": "✅ Neves Analytics: Conexão OK!"}, timeout=5)
            except: pass

# --- 3. BANCO DE DADOS ---
DB_FILE = 'neves_dados.txt'

def carregar_db():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
    try: return pd.read_csv(DB_FILE)
    except: return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])

# --- 4. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    
    with st.expander("⚙️ Painel de Configuração", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        
        if st.button("🔔 Testar Telegram"):
            enviar_teste_telegram(tg_token, tg_chat_ids)
            st.toast("Teste enviado!")
            
        INTERVALO = st.slider("Ciclo de Atualização (seg):", 60, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        
        if st.button("🗑️ Resetar Banco"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 5. LOGICA DE API (COM CACHE) ---
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
        return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 20}}, "league": {"id": 1, "name": "Liga Teste", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}]
    if not API_KEY: return []
    url = f"https://v3.football.api-sports.io/{endpoint}"
    try:
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

# --- 6. CONTAINER PRINCIPAL (ESTRATÉGIA ANTI-ESPELHO) ---
# Criamos um único lugar onde tudo vai acontecer.
placeholder_principal = st.container()

with placeholder_principal:
    st.title("❄️ Neves Analytics")
    
    if ROBO_LIGADO:
        st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        # Área do Timer
        c_timer, c_prog = st.columns([1, 5])
        timer_text = c_timer.empty()
        bar_prog = c_prog.progress(0)

        # Dados
        jogos_live = buscar_dados("fixtures", {"live": "all"})
        radar = []
        for j in jogos_live:
            tempo = j['fixture']['status'].get('elapsed', 0)
            sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
            radar.append({"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {sc}x{sf} {j['teams']['away']['name']}", "Tempo": f"{tempo}'", "Status": "👁️"})

        prox_lista = buscar_proximos(API_KEY)
        hist_df = carregar_db()

        # Abas
        t1, t2, t3, t4 = st.tabs([
            f"📡 Ao Vivo ({len(radar)})", 
            f"📅 Próximos ({len(prox_lista)})", 
            f"📊 Histórico ({len(hist_df)})", 
            f"🚫 Blacklist ({len(st.session_state['ligas_sem_stats'])})"
        ])
        
        with t1:
            if radar: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
            else: st.info("Sem jogos ao vivo.")

        with t2:
            if prox_lista:
                df_p = pd.DataFrame([{"Hora": j['fixture']['date'][11:16], "Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} vs {j['teams']['away']['name']}"} for j in prox_lista])
                st.dataframe(df_p.sort_values("Hora"), use_container_width=True, hide_index=True)

        with t3:
            if not hist_df.empty: st.dataframe(hist_df.sort_values(by=['hora'], ascending=False), use_container_width=True, hide_index=True)
            else: st.caption("Sem histórico.")

        with t4:
            if st.session_state['ligas_sem_stats']: st.table(list(st.session_state['ligas_sem_stats'].values()))
            else: st.caption("Limpo.")

        # Lógica de contagem regressiva
        for i in range(INTERVALO, 0, -1):
            timer_text.markdown(f"**{i}s**")
            bar_prog.progress(i / INTERVALO)
            time.sleep(1)
        
        st.rerun()

    else:
        st.info("💡 Robô em espera. Configure e ligue na barra lateral.")
        df_h = carregar_db()
        if not df_h.empty:
            st.subheader("📊 Resumo do Histórico")
            st.dataframe(df_h.sort_values(by=['data', 'hora'], ascending=False).head(10), use_container_width=True, hide_index=True)
