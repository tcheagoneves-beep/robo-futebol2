import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Neves Analytics", layout="wide", page_icon="❄️")

st.markdown("""
<style>
    .stApp {background-color: #121212; color: white;}
    .status-online {color: #00FF00; font-weight: bold; animation: pulse 2s infinite; padding: 10px; border: 1px solid #00FF00; text-align: center; border-radius: 15px; margin-bottom: 20px;}
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.4); } 70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); } 100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); } }
    .update-info { font-size: 14px; color: #FFD700; text-align: center; font-weight: bold;}
</style>
""", unsafe_allow_html=True)

# --- 2. BANCO DE DADOS E ARQUIVOS ---
DB_FILE = 'neves_dados.txt'
BLACK_FILE = 'neves_blacklist.txt'

def carregar_db():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
    return pd.read_csv(DB_FILE)

def carregar_blacklist():
    if not os.path.exists(BLACK_FILE):
        return pd.DataFrame(columns=['id', 'País', 'Liga'])
    return pd.read_csv(BLACK_FILE)

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
                              data={"chat_id": cid.strip(), "text": "❄️ Neves Analytics: Teste de Envio OK!"}, timeout=5)
            except: pass

# --- 3. SIDEBAR (RETRAÍDA) ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ CONFIGURAÇÕES AVANÇADAS", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs (vincule com vírgula):")
        
        if st.button("🔔 TESTAR ENVIO TELEGRAM"):
            enviar_teste_telegram(tg_token, tg_chat_ids)
            st.success("Sinal de teste enviado!")
            
        INTERVALO = st.number_input("Ciclo (segundos):", min_value=30, value=60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação")
        
        if st.button("🗑️ RESETAR TUDO"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
            st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR MONITORAMENTO", value=False)

# --- 4. LÓGICA DE API ---
@st.cache_data(ttl=3600)
def buscar_proximos(key):
    if not key and not MODO_DEMO: return []
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"date": (datetime.utcnow() - timedelta(hours=3)).strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}
    try:
        res = requests.get(url, headers={"x-apisports-key": key}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 25}}, "league": {"id": 1, "name": "Liga Teste", "country": "BR"}, "goals": {"home": 1, "away": 0}, "teams": {"home": {"name": "Time Casa"}, "away": {"name": "Time Fora"}}}]
    if not API_KEY: return []
    try:
        res = requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

# --- 5. EXECUÇÃO ---
# O segredo para não espelhar é colocar tudo dentro de UM placeholder que se limpa
tela = st.empty()

if ROBO_LIGADO:
    while True:
        with tela.container():
            st.markdown('<div class="status-online">🟢 ROBÔ OPERANDO AO VIVO</div>', unsafe_allow_html=True)
            
            agora = (datetime.utcnow() - timedelta(hours=3)).strftime("%H:%M:%S")
            st.markdown(f'<p class="update-info">🕒 Última Checagem: {agora}</p>', unsafe_allow_html=True)

            # Processamento
            black_df = carregar_blacklist()
            black_ids = black_df['id'].astype(str).values
            jogos_live = buscar_dados("fixtures", {"live": "all"})
            
            radar = []
            for j in jogos_live:
                l_id = str(j['league']['id'])
                info = {"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} vs {j['teams']['away']['name']}", "Placar": f"{j['goals']['home']}x{j['goals']['away']}", "Tempo": f"{j['fixture']['status']['elapsed']}'"}
                info["Status"] = "🚫" if l_id in black_ids else "👁️ Analisando"
                radar.append(info)

            prox_raw = buscar_proximos(API_KEY)
            prox_lista = [{"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"} for p in prox_raw if p['fixture']['status']['short'] == 'NS']
            
            hist_df = carregar_db()

            # Exibição
            tab1, tab2, tab3, tab4 = st.tabs([f"📡 AO VIVO ({len(radar)})", f"📅 PRÓXIMOS ({len(prox_lista)})", f"📊 HISTÓRICO", f"🚫 BLACKLIST ({len(black_df)})"])
            
            with tab1: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
            with tab2: st.dataframe(pd.DataFrame(prox_lista).sort_values("Hora"), use_container_width=True, hide_index=True) if prox_lista else st.write("Sem jogos NS.")
            with tab3: st.dataframe(hist_df.sort_values(by=['hora'], ascending=False), use_container_width=True, hide_index=True) if not hist_df.empty else st.write("Histórico vazio.")
            with tab4: st.table(black_df[['País', 'Liga']]) if not black_df.empty else st.write("Nenhuma liga bloqueada.")

        time.sleep(INTERVALO)
        # O rerun() aqui dentro do loop com container limpa a tela de verdade
        st.rerun()
else:
    with tela.container():
        st.info("💡 Neves Analytics pronto. Configure a API e ligue o robô na lateral.")
        st.subheader("📊 Resumo do Banco de Dados")
        st.write(f"Sinais no Histórico: {len(carregar_db())}")
        st.write(f"Ligas na Blacklist: {len(carregar_blacklist())}")
