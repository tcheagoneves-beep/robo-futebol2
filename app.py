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
    .update-info { font-size: 12px; color: #888; text-align: center; margin-bottom: 10px; }
</style>
""", unsafe_allow_html=True)

# --- 2. GESTÃO DE ARQUIVOS ---
DB_FILE = 'neves_dados.txt'
BLACK_FILE = 'neves_blacklist.txt'

def carregar_db():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
    try: return pd.read_csv(DB_FILE)
    except: return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])

def carregar_blacklist():
    if not os.path.exists(BLACK_FILE):
        return pd.DataFrame(columns=['id', 'País', 'Liga'])
    try: return pd.read_csv(BLACK_FILE)
    except: return pd.DataFrame(columns=['id', 'País', 'Liga'])

def salvar_na_blacklist(id_liga, pais, nome_liga):
    df = carregar_blacklist()
    if str(id_liga) not in df['id'].astype(str).values:
        novo = pd.DataFrame([{'id': str(id_liga), 'País': pais, 'Liga': nome_liga}])
        df = pd.concat([df, novo], ignore_index=True)
        df.to_csv(BLACK_FILE, index=False)

# --- 3. FUNÇÕES DE APOIO ---
def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

def enviar_teste_telegram(token, chat_ids):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try:
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                requests.post(url, data={"chat_id": cid.strip(), "text": "✅ Neves Analytics: Conexão OK!"}, timeout=5)
            except: pass

# --- 4. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        if st.button("🔔 Testar Telegram"):
            enviar_teste_telegram(tg_token, tg_chat_ids)
            st.toast("Teste enviado!")
        INTERVALO = st.number_input("Ciclo (segundos):", min_value=30, max_value=600, value=60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        if st.button("🗑️ Resetar Tudo"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
            st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 5. LÓGICA DE API ---
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

# --- 6. EXECUÇÃO ---
st.title("❄️ Neves Analytics")

if ROBO_LIGADO:
    st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
    
    # Info de tempo simples e estável
    agora = agora_brasil()
    proxima = agora + timedelta(seconds=INTERVALO)
    st.markdown(f'<div class="update-info">Última: {agora.strftime("%H:%M:%S")} | Próxima: {proxima.strftime("%H:%M:%S")}</div>', unsafe_allow_html=True)

    # Dados
    black_df = carregar_blacklist()
    black_ids = black_df['id'].astype(str).values
    jogos_live = buscar_dados("fixtures", {"live": "all"})
    radar = []
    
    for j in jogos_live:
        f_id, l_id = j['fixture']['id'], str(j['league']['id'])
        sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
        info = {"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {sc}x{sf} {j['teams']['away']['name']}", "Tempo": f"{j['fixture']['status'].get('elapsed', 0)}'", "Status": "👁️"}
        
        if l_id in black_ids:
            info["Status"] = "🚫"
        else:
            stats = buscar_dados("statistics", {"fixture": f_id})
            if not stats and (sc+sf > 0):
                salvar_na_blacklist(l_id, j['league']['country'], j['league']['name'])
                info["Status"] = "🚫"
        radar.append(info)

    prox_lista = buscar_proximos(API_KEY)
    hist_df = carregar_db()

    # ABAS
    t1, t2, t3, t4 = st.tabs([f"📡 Ao Vivo ({len(radar)})", f"📅 Próximos ({len(prox_lista)})", f"📊 Histórico ({len(hist_df)})", f"🚫 Blacklist ({len(black_df)})"])
    
    with t1: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True) if radar else st.info("Sem jogos.")
    with t2:
        if prox_lista:
            df_p = pd.DataFrame([{"Hora": j['fixture']['date'][11:16], "Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} vs {j['teams']['away']['name']}"} for j in prox_lista])
            st.dataframe(df_p.sort_values("Hora"), use_container_width=True, hide_index=True)
    with t3: st.dataframe(hist_df.sort_values(by=['hora'], ascending=False), use_container_width=True, hide_index=True) if not hist_df.empty else st.caption("Vazio.")
    with t4: st.table(black_df[['País', 'Liga']]) if not black_df.empty else st.caption("Limpo.")

    # Pausa e Recarrega
    time.sleep(INTERVALO)
    st.rerun()

else:
    st.info("💡 Robô desligado. Configure na barra lateral.")
    st.subheader("🚫 Blacklist Atual")
    st.table(carregar_blacklist()[['País', 'Liga']])
