import streamlit as st
import pandas as pd
import requests
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
    try: return pd.read_csv(BLACK_FILE).drop_duplicates()
    except: return pd.DataFrame(columns=['id', 'País', 'Liga'])

def salvar_na_blacklist(id_liga, pais, nome_liga):
    df = carregar_blacklist()
    if str(id_liga) not in df['id'].astype(str).values:
        novo = pd.DataFrame([{'id': str(id_liga), 'País': pais, 'Liga': nome_liga}])
        df = pd.concat([df, novo], ignore_index=True)
        df.to_csv(BLACK_FILE, index=False)

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        
        if st.button("🔔 Testar Telegram"):
            if tg_token and tg_chat_ids:
                for cid in tg_chat_ids.split(','):
                    requests.post(f"https://api.telegram.org/bot{tg_token}/sendMessage", 
                                  data={"chat_id": cid.strip(), "text": "✅ Neves Analytics: OK!"})
                st.toast("Teste enviado!")
        
        INTERVALO = st.number_input("Ciclo (segundos):", min_value=30, value=60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        
        if st.button("🗑️ Resetar Tudo"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
            st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 4. LOGICA DE DADOS ---
def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        return [{"fixture": {"id": 1, "date": "2024-01-01T20:00:00", "status": {"short": "1H", "elapsed": 10}}, "league": {"id": 999, "name": "Liga Teste", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}]
    if not API_KEY: return []
    try:
        url = f"https://v3.football.api-sports.io/{endpoint}"
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

# --- 5. EXECUÇÃO ---
st.title("❄️ Neves Analytics")

if ROBO_LIGADO:
    st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
    
    # Horário da atualização
    agora = (datetime.utcnow() - timedelta(hours=3))
    st.markdown(f'<div class="update-info">Última atualização: {agora.strftime("%H:%M:%S")}</div>', unsafe_allow_html=True)

    # 1. Filtros
    black_df = carregar_blacklist()
    black_ids = black_df['id'].astype(str).values

    # 2. Ao Vivo
    jogos_live = buscar_dados("fixtures", {"live": "all"})
    radar = []
    for j in jogos_live:
        l_id = str(j['league']['id'])
        if l_id not in black_ids:
            sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
            if sc + sf > 0:
                stats = buscar_dados("statistics", {"fixture": j['fixture']['id']})
                if not stats:
                    salvar_na_blacklist(l_id, j['league']['country'], j['league']['name'])
                    continue
            radar.append({"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {sc}x{sf} {j['teams']['away']['name']}", "Tempo": f"{j['fixture']['status']['elapsed']}'"})

    # 3. Próximos
    prox_raw = buscar_dados("fixtures", {"date": agora.strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"})
    prox_lista = [{"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"} 
                  for p in prox_raw if str(p['league']['id']) not in black_ids and p['fixture']['status']['short'] == 'NS']

    # 4. Abas
    t1, t2, t3, t4 = st.tabs([f"📡 Ao Vivo ({len(radar)})", f"📅 Próximos ({len(prox_lista)})", "📊 Histórico", f"🚫 Blacklist ({len(black_df)})"])
    
    with t1: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True) if radar else st.info("Sem jogos.")
    with t2: st.dataframe(pd.DataFrame(prox_lista).sort_values("Hora"), use_container_width=True, hide_index=True) if prox_lista else st.caption("Vazio.")
    with t3: st.dataframe(carregar_db(), use_container_width=True, hide_index=True)
    with t4: st.table(black_df[['País', 'Liga']]) if not black_df.empty else st.caption("Vazio.")

    # --- O SEGREDO DO REFRESH SEM ERRO ---
    # Em vez de um loop de segundos que trava o script, usamos o tempo de espera e recarregamos.
    import time
    time.sleep(INTERVALO)
    st.rerun()

else:
    st.info("💡 Robô desligado na barra lateral.")
    st.write(f"Ligas na Blacklist: {len(carregar_blacklist())}")
