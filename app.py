import streamlit as st
import pandas as pd
import requests
import time
import os
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Neves Analytics", layout="centered", page_icon="❄️")

st.markdown("""
<style>
    .stApp {background-color: #121212; color: white;}
    .status-online {color: #00FF00; font-weight: bold; text-align: center; padding: 10px; border: 1px solid #00FF00; border-radius: 10px; margin-bottom: 20px;}
    .status-off {color: #FF4B4B; text-align: center; padding: 10px;}
</style>
""", unsafe_allow_html=True)

# --- 2. AUXILIARES ---
def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

def traduzir_instrucao(sinal, favorito=""):
    if "MÚLTIPLA" in sinal: return "Excelente para bilhetes de 2+ jogos."
    if "HT" in sinal: return "Entrar para sair mais um gol no 1º tempo."
    return "Analisar entrada no mercado de gols."

# --- 3. BANCO DE DADOS ---
DB_FILE = 'neves_dados.txt'

def carregar_db():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
    try:
        return pd.read_csv(DB_FILE)
    except:
        return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])

def salvar_sinal_db(fixture_id, jogo, sinal, gols_inicial):
    df = carregar_db()
    if not ((df['id'] == fixture_id) & (df['sinal'] == sinal)).any():
        data_br = agora_brasil()
        novo = pd.DataFrame([{
            'id': fixture_id, 'data': data_br.strftime('%Y-%m-%d'), 'hora': data_br.strftime('%H:%M'),
            'jogo': jogo, 'sinal': sinal, 'gols_inicial': gols_inicial, 'status': 'Pendente'
        }])
        df = pd.concat([df, novo], ignore_index=True)
        df.to_csv(DB_FILE, index=False)

# --- 4. SIDEBAR ---
with st.sidebar:
    st.header("❄️ Painel de Controle")
    API_KEY = st.text_input("Chave API-SPORTS:", type="password")
    tg_token = st.text_input("Telegram Token:", type="password")
    tg_chat_ids = st.text_input("Chat IDs:")
    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)
    INTERVALO = st.slider("Ciclo (seg):", 60, 300, 60)
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)

# --- 5. LÓGICA DE API ---
if 'ligas_sem_stats' not in st.session_state or not isinstance(st.session_state['ligas_sem_stats'], dict):
    st.session_state['ligas_sem_stats'] = {}

def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        if "statistics" in endpoint:
            return [{"statistics": [{"type": "Total Shots", "value": 12}, {"type": "Shots on Goal", "value": 5}, {"type": "Dangerous Attacks", "value": 45}]}, 
                    {"statistics": [{"type": "Total Shots", "value": 4}, {"type": "Shots on Goal", "value": 1}, {"type": "Dangerous Attacks", "value": 15}]}]
        return [{"fixture": {"id": 123, "status": {"short": "1H", "elapsed": 32}}, "league": {"id": 99, "name": "Liga Demo", "country": "Brasil"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}]
    
    url = f"https://v3.football.api-sports.io/{endpoint}"
    headers = {"x-apisports-key": API_KEY}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10).json()
        return res.get('response', [])
    except:
        return []

# --- 6. CÉREBRO ---
def analisar(tempo, s_casa, s_fora, sc, sf):
    def v(d, k): 
        val = next((item['value'] for item in d if item['type'] == k), 0)
        return int(str(val).replace('%','')) if val else 0
    
    c_tot = v(s_casa, 'Total Shots'); f_tot = v(s_fora, 'Total Shots')
    c_gol = v(s_casa, 'Shots on Goal'); f_gol = v(s_fora, 'Shots on Goal')
    
    sinal = None; motivo = ""
    if tempo <= 30 and (sc + sf) >= 2:
        sinal = "CANDIDATO P/ MÚLTIPLA"; motivo = "Ritmo frenético de gols."
    elif 5 <= tempo <= 25 and (c_tot + f_tot) >= 5:
        sinal = "GOL HT"; motivo = "Alta frequência de chutes no início."
    
    return sinal, motivo, (c_tot + f_tot), (c_gol + f_gol)

# --- 7. EXECUÇÃO ---
if ROBO_LIGADO:
    st.markdown('<div class="status-online">🟢 ROBÔ ATIVO E MONITORANDO</div>', unsafe_allow_html=True)
    
    jogos = buscar_dados("fixtures", {"live": "all"})
    radar = []
    
    if jogos:
        for j in jogos:
            f_id = j['fixture']['id']
            l_id = str(j['league']['id'])
            tempo = j['fixture']['status'].get('elapsed', 0)
            sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
            
            item = {"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {sc}x{sf} {j['teams']['away']['name']}", "Tempo": f"{tempo}'", "Status": "👁️"}
            
            if l_id in st.session_state['ligas_sem_stats']:
                item["Status"] = "🚫 Bloqueada"
            else:
                stats = buscar_dados("statistics", {"fixture": f_id})
                if not stats:
                    if (sc + sf) > 0:
                        st.session_state['ligas_sem_stats'][l_id] = {"País": j['league']['country'], "Liga": j['league']['name'], "Motivo": "Gols s/ Stats"}
                        item["Status"] = "🚫 Bloqueada"
                    else:
                        item["Status"] = "⏳ Aguardando"
                else:
                    sinal, mot, ch, gol = analisar(tempo, stats[0]['statistics'], stats[1]['statistics'], sc, sf)
                    if sinal:
                        item["Status"] = f"✅ {sinal}"
                        salvar_sinal_db(f_id, item["Jogo"], sinal, sc+sf)
            
            radar.append(item)

    # --- ABAS DE EXIBIÇÃO ---
    tab1, tab2, tab3 = st.tabs(["📡 Radar ao Vivo", "📊 Histórico", "🚫 Blacklist"])
    
    with tab1:
        if radar:
            st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
        else:
            st.info("Aguardando início de partidas...")

    with tab2:
        df_h = carregar_db()
        if not df_h.empty:
            st.dataframe(df_h.sort_values(by='hora', ascending=False), use_container_width=True, hide_index=True)
        else:
            st.caption("Nenhum sinal registrado ainda.")

    with tab3:
        if st.session_state['ligas_sem_stats']:
            df_b = pd.DataFrame(list(st.session_state['ligas_sem_stats'].values()))
            st.table(df_b)
        else:
            st.caption("Nenhuma liga bloqueada.")

    time.sleep(INTERVALO)
    st.rerun()

else:
    st.markdown('<div class="status-off">🔴 O robô está pausado. Ligue na barra lateral.</div>', unsafe_allow_html=True)
