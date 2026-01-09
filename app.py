import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL (FIXA) ---
st.set_page_config(page_title="Neves Analytics", layout="centered", page_icon="❄️")

st.markdown("""
<style>
    .stApp {background-color: #121212; color: white;}
    .status-online {color: #00FF00; font-weight: bold; animation: pulse 2s infinite; padding: 10px; border: 1px solid #00FF00; text-align: center; border-radius: 15px; margin-bottom: 20px;}
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.4); } 70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); } 100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); } }
    .timer-text { font-size: 11px; color: #555; text-align: right; }
</style>
""", unsafe_allow_html=True)

# --- 2. GESTÃO DE ARQUIVOS (PERMANENTE) ---
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

# --- 3. SIDEBAR (CONFIGURAÇÕES) ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        
        if st.button("🔔 Testar Envio Telegram"):
            if tg_token and tg_chat_ids:
                for cid in tg_chat_ids.split(','):
                    requests.post(f"https://api.telegram.org/bot{tg_token}/sendMessage", 
                                  data={"chat_id": cid.strip(), "text": "✅ Teste Neves Analytics OK!"})
                st.toast("Enviado!")

        INTERVALO = st.slider("Ciclo (seg):", 60, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação")
        
        if st.button("🗑️ Resetar Tudo"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
            st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 4. FUNÇÕES DE DADOS ---
def buscar_dados(endpoint, key, params=None):
    if MODO_DEMO:
        return [{"fixture": {"id": 1, "date": "2024-01-01T20:00:00", "status": {"short": "1H", "elapsed": 10}}, "league": {"id": 999, "name": "Liga Teste", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}]
    if not key: return []
    try:
        url = f"https://v3.football.api-sports.io/{endpoint}"
        res = requests.get(url, headers={"x-apisports-key": key}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

# --- 5. CORPO PRINCIPAL ---
st.title("❄️ Neves Analytics")

if ROBO_LIGADO:
    st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
    
    # Placeholders para o timer
    timer_txt = st.empty()
    timer_bar = st.empty()

    # 1. Carregar Filtros
    black_df = carregar_blacklist()
    black_ids = black_df['id'].astype(str).values

    # 2. Processar AO VIVO
    jogos_live = buscar_dados("fixtures", API_KEY, {"live": "all"})
    radar = []
    for j in jogos_live:
        l_id = str(j['league']['id'])
        if l_id not in black_ids:
            # Se tem gol, checa se tem estatística
            sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
            if sc + sf > 0:
                stats = buscar_dados("statistics", API_KEY, {"fixture": j['fixture']['id']})
                if not stats:
                    salvar_na_blacklist(l_id, j['league']['country'], j['league']['name'])
                    continue
            radar.append({"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {sc}x{sf} {j['teams']['away']['name']}", "Tempo": f"{j['fixture']['status']['elapsed']}'"})

    # 3. Processar PRÓXIMOS
    data_hoje = (datetime.utcnow() - timedelta(hours=3)).strftime('%Y-%m-%d')
    prox_raw = buscar_dados("fixtures", API_KEY, {"date": data_hoje, "timezone": "America/Sao_Paulo"})
    prox_lista = [{"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"} 
                  for p in prox_raw if str(p['league']['id']) not in black_ids and p['fixture']['status']['short'] == 'NS']

    # 4. EXIBIÇÃO EM ABAS
    tab1, tab2, tab3, tab4 = st.tabs([f"📡 Ao Vivo ({len(radar)})", f"📅 Próximos ({len(prox_lista)})", "📊 Histórico", f"🚫 Blacklist ({len(black_df)})"])
    
    with tab1: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True) if radar else st.info("Sem jogos válidos.")
    with tab2: st.dataframe(pd.DataFrame(prox_lista).sort_values("Hora"), use_container_width=True, hide_index=True) if prox_lista else st.caption("Sem jogos NS.")
    with tab3: st.dataframe(carregar_db(), use_container_width=True, hide_index=True)
    with tab4: st.table(black_df[['País', 'Liga']]) if not black_df.empty else st.caption("Vazio.")

    # 5. TIMER E REFRESH (A mágica que evita o espelhamento)
    for i in range(INTERVALO, 0, -1):
        timer_txt.markdown(f'<div class="timer-text">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
        timer_bar.progress(i / INTERVALO)
        time.sleep(1)
    
    st.rerun() # Reinicia o script do zero, limpando a memória visual

else:
    st.info("💡 Robô desligado. Ligue na lateral para começar.")
    st.write(f"Ligas na Blacklist: {len(carregar_blacklist())}")
