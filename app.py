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

def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    # Geramos uma chave baseada no estado para evitar duplicidade na sidebar
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password", key="api_key_input")
        tg_token = st.text_input("Telegram Token:", type="password", key="tg_token_input")
        tg_chat_ids = st.text_input("Chat IDs:", key="tg_chat_ids_input")
        
        INTERVALO = st.number_input("Ciclo (segundos):", min_value=30, max_value=600, value=60, key="intervalo_input")
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False, key="demo_input")
        
        if st.button("🗑️ Resetar Tudo", key="reset_button"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
            st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False, key="ligar_robo_key")

# --- 4. LOGICA DE API ---
def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 20}}, "league": {"id": 1, "name": "Liga Teste", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}]
    if not API_KEY: return []
    url = f"https://v3.football.api-sports.io/{endpoint}"
    try:
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

# --- 5. EXECUÇÃO ---
st.title("❄️ Neves Analytics")

if ROBO_LIGADO:
    # Criamos um container único que será limpo a cada rerun
    with st.container():
        st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        agora = agora_brasil()
        stamp = agora.strftime("%H:%M:%S")
        st.markdown(f'<div class="update-info">Última Atualização: {stamp}</div>', unsafe_allow_html=True)

        # Lógica de processamento
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

        hist_df = carregar_db()

        # ABAS COM KEYS ÚNICAS (isso mata o erro de Duplicate ID)
        t1, t2, t3 = st.tabs([f"📡 Ao Vivo ({len(radar)})", f"📊 Histórico ({len(hist_df)})", f"🚫 Blacklist ({len(black_df)})"])
        
        with t1:
            if radar: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True, key=f"df_live_{stamp}")
            else: st.info("Buscando jogos...")

        with t2:
            if not hist_df.empty: st.dataframe(hist_df.sort_values(by=['hora'], ascending=False), use_container_width=True, hide_index=True, key=f"df_hist_{stamp}")
            else: st.caption("Vazio.")

        with t3:
            if not black_df.empty: st.table(black_df[['País', 'Liga']])
            else: st.caption("Limpo.")

    # Pausa e recarrega
    time.sleep(INTERVALO)
    st.rerun()

else:
    st.info("💡 Robô desligado. Configure na barra lateral.")
    st.subheader("🚫 Blacklist Salva")
    st.table(carregar_blacklist()[['País', 'Liga']])
