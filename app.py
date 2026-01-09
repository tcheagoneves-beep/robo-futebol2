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
    .status-online {color: #00FF00; font-weight: bold; animation: pulse 2s infinite; padding: 10px; border: 1px solid #00FF00; text-align: center; margin-bottom: 20px; border-radius: 15px;}
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.4); } 70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); } 100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); } }
    .timer-text { font-size: 11px; color: #555; text-align: right; }
</style>
""", unsafe_allow_html=True)

# --- 2. AUXILIARES E ARQUIVOS ---
DB_FILE = 'neves_dados.txt'
BLACK_FILE = 'neves_blacklist.txt'

def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

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
        pd.concat([df, novo], ignore_index=True).to_csv(BLACK_FILE, index=False)

def enviar_teste_telegram(token, chat_ids):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                              data={"chat_id": cid.strip(), "text": "✅ Neves Analytics: Teste de Conexão OK!"}, timeout=10)
            except: pass

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        if st.button("🔔 Testar Envio Telegram"):
            enviar_teste_telegram(tg_token, tg_chat_ids)
            st.toast("Tentativa de envio realizada!")
        INTERVALO = st.slider("Ciclo (seg):", 60, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        if st.button("🗑️ Resetar Tudo"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
            st.rerun()
    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 4. FUNÇÕES DE API ---
@st.cache_data(ttl=3600)
def buscar_proximos(key):
    if not key and not MODO_DEMO: return []
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"date": agora_brasil().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}
    try:
        res = requests.get(url, headers={"x-apisports-key": key}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        if "statistics" in endpoint: return [{"statistics": [{"type": "Total Shots", "value": 5}]}]
        return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 10}}, "league": {"id": 999, "name": "Liga Teste", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}]
    if not API_KEY: return []
    try:
        res = requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

# --- 5. CORPO PRINCIPAL (ANTI-ESPELHAMENTO) ---
# Criamos um placeholder vazio que ocupará a tela inteira
main_placeholder = st.empty()

if ROBO_LIGADO:
    # Tudo o que o robô faz agora acontece dentro do .container() do placeholder
    with main_placeholder.container():
        st.title("❄️ Neves Analytics")
        st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        timer_space = st.empty()
        bar_space = st.empty()

        df_black = carregar_blacklist()
        ids_bloqueados = df_black['id'].astype(str).values
        
        # 1. Dados Ao Vivo (Filtrados)
        jogos_live = buscar_dados("fixtures", {"live": "all"})
        radar = []
        for j in jogos_live:
            l_id = str(j['league']['id'])
            if l_id not in ids_bloqueados:
                f_id = j['fixture']['id']
                sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
                if sc + sf > 0:
                    stats = buscar_dados("statistics", {"fixture": f_id})
                    if not stats:
                        salvar_na_blacklist(l_id, j['league']['country'], j['league']['name'])
                        continue 
                radar.append({"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {sc}x{sf} {j['teams']['away']['name']}", "Tempo": f"{j['fixture']['status'].get('elapsed', 0)}'"})

        # 2. Dados Próximos (Filtrados)
        prox_raw = buscar_proximos(API_KEY)
        prox_filtrado = [{"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"} 
                         for p in prox_raw if str(p['league']['id']) not in ids_bloqueados and p['fixture']['status']['short'] == 'NS']

        # 3. Exibição
        t1, t2, t3, t4 = st.tabs([f"📡 Ao Vivo ({len(radar)})", f"📅 Próximos ({len(prox_filtrado)})", "📊 Histórico", f"🚫 Blacklist ({len(df_black)})"])
        
        with t1:
            st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True) if radar else st.info("Sem jogos válidos.")
        with t2:
            st.dataframe(pd.DataFrame(prox_filtrado).sort_values("Hora"), use_container_width=True, hide_index=True) if prox_filtrado else st.caption("Ligas bloqueadas ocultas.")
        with t3:
            st.dataframe(carregar_db().sort_values(by=['data', 'hora'], ascending=False), use_container_width=True, hide_index=True)
        with t4:
            st.table(df_black[['País', 'Liga']]) if not df_black.empty else st.caption("Vazio.")

    # 4. Contagem regressiva fora do container de dados para evitar flickering
    for i in range(INTERVALO, 0, -1):
        timer_space.markdown(f'<div class="timer-text">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
        bar_space.progress(i / INTERVALO)
        time.sleep(1)
    
    st.rerun()

else:
    with main_placeholder.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera. Configure e ligue na lateral.")
        st.write(f"Ligas na Blacklist: {len(carregar_blacklist())}")
