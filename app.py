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
    .timer-text { font-size: 12px; color: #888; text-align: right; margin-bottom: 5px; }
</style>
""", unsafe_allow_html=True)

# --- 2. AUXILIARES ---
def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

# --- 3. BANCO DE DADOS (NOVA ESTRUTURA GARANTIDA) ---
DB_FILE = 'neves_dados.txt'

def carregar_db():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
    try:
        df = pd.read_csv(DB_FILE)
        return df
    except:
        return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])

def salvar_sinal_db(fixture_id, jogo, sinal, gols_inicial):
    df = carregar_db()
    # Chave única para evitar duplicados: ID do jogo + Nome do Sinal
    if not ((df['id'].astype(str) == str(fixture_id)) & (df['sinal'] == sinal)).any():
        data_br = agora_brasil()
        novo = pd.DataFrame([{
            'id': fixture_id, 'data': data_br.strftime('%Y-%m-%d'), 'hora': data_br.strftime('%H:%M'),
            'jogo': jogo, 'sinal': sinal, 'gols_inicial': gols_inicial, 'status': 'Pendente'
        }])
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
    INTERVALO = st.slider("Ciclo de Atualização (seg):", 60, 300, 60)
    
    if st.button("🗑️ Resetar Banco (Zerar Tudo)"):
        if os.path.exists(DB_FILE): os.remove(DB_FILE)
        st.rerun()
    
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)

# --- 5. LÓGICA DE API (COM ECONOMIA DE CONSUMO) ---
if 'ligas_sem_stats' not in st.session_state:
    st.session_state['ligas_sem_stats'] = {}

@st.cache_data(ttl=3600) # Economia: Só busca lista de jogos 1x por hora
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
        if "statistics" in endpoint:
            return [{"statistics": [{"type": "Total Shots", "value": 8}]}, {"statistics": [{"type": "Total Shots", "value": 3}]}]
        return [{"fixture": {"id": 99, "status": {"short": "1H", "elapsed": 20}}, "league": {"id": 1, "name": "Liga Teste", "country": "Brasil"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}]
    
    if not API_KEY: return []
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
    
    chutes = v(0, 'Total Shots') + v(1, 'Total Shots')
    sinal = None; mot = ""
    
    if tempo <= 30 and (sc + sf) >= 2:
        sinal = "CANDIDATO P/ MÚLTIPLA"; mot = "Jogo muito movimentado."
    elif 5 <= tempo <= 25 and chutes >= 5:
        sinal = "GOL HT"; mot = f"Frequência alta: {chutes} chutes."
    
    return sinal, mot, chutes

# --- 7. EXECUÇÃO ---
st.title("❄️ Neves Analytics")

if ROBO_LIGADO:
    st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
    
    # Timer Visual
    placeholder_timer = st.empty()
    
    jogos_live = buscar_dados("fixtures", {"live": "all"})
    radar = []
    
    for j in jogos_live:
        f_id = j['fixture']['id']
        l_id = str(j['league']['id'])
        tempo = j['fixture']['status'].get('elapsed', 0)
        sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
        
        info = {"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {sc}x{sf} {j['teams']['away']['name']}", "Tempo": f"{tempo}'", "Status": "👁️"}
        
        if l_id in st.session_state['ligas_sem_stats']:
            info["Status"] = "🚫 Sem Stats"
        else:
            # Trava de Economia: Só busca stats em momentos de interesse ou se sair gol
            zona_interesse = (5 <= tempo <= 30) or (sc+sf > 0)
            if zona_interesse:
                stats = buscar_dados("statistics", {"fixture": f_id})
                if not stats:
                    if (sc + sf) > 0:
                        st.session_state['ligas_sem_stats'][l_id] = {"País": j['league']['country'], "Liga": j['league']['name']}
                        info["Status"] = "🚫 Bloqueada"
                else:
                    sinal, mot, ch = analisar(tempo, stats, sc, sf)
                    if sinal:
                        info["Status"] = f"✅ {sinal}"
                        salvar_sinal_db(f_id, info["Jogo"], sinal, sc+sf)
            else:
                info["Status"] = "⏳ Monitorando"
        
        radar.append(info)

    tab1, tab2, tab3, tab4 = st.tabs(["📡 Ao Vivo", "📅 Próximos", "📊 Histórico", "🚫 Blacklist"])
    
    with tab1:
        if radar: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
        else: st.info("Buscando jogos...")

    with tab2:
        prox = buscar_proximos(API_KEY)
        if prox:
            df_p = pd.DataFrame([{"Hora": j['fixture']['date'][11:16], "Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} vs {j['teams']['away']['name']}"} for j in prox])
            st.dataframe(df_p.sort_values("Hora"), use_container_width=True, hide_index=True)

    with tab3:
        df_h = carregar_db()
        if not df_h.empty: st.dataframe(df_h.sort_values(by=['data', 'hora'], ascending=False), use_container_width=True, hide_index=True)
        else: st.caption("Histórico zerado. Aguardando sinais...")

    with tab4:
        if st.session_state['ligas_sem_stats']: st.table(list(st.session_state['ligas_sem_stats'].values()))

    # Lógica do Timer de Contagem Regressiva
    for i in range(INTERVALO, 0, -1):
        placeholder_timer.markdown(f'<div class="timer-text">Próxima atualização em: {i}s</div>', unsafe_allow_html=True)
        time.sleep(1)
    
    st.rerun()
else:
    st.info("Robô pausado. O histórico abaixo será atualizado assim que você ligar.")
    st.dataframe(carregar_db().sort_values(by=['data', 'hora'], ascending=False).head(5), use_container_width=True, hide_index=True)
