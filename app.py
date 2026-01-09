import streamlit as st
import pandas as pd
import requests
import time
import os
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Neves Analytics",
    layout="centered",
    page_icon="❄️"
)

st.markdown("""
<style>
    .stApp {background-color: #121212; color: white;}
    .card {background-color: #1E1E1E; padding: 20px; border-radius: 15px; border: 1px solid #333; margin-bottom: 20px;}
    .titulo-time {font-size: 20px; font-weight: bold; color: #ffffff;}
    .odd-label {font-size: 12px; color: #aaa; background-color: #333; padding: 2px 6px; border-radius: 4px;}
    .placar {font-size: 35px; font-weight: 800; color: #FFD700; text-align: center;}
    .tempo {font-size: 14px; color: #FF4B4B; font-weight: bold; text-align: center;}
    .sinal-box {background-color: #00C853; color: white; padding: 10px; border-radius: 8px; text-align: center; font-weight: bold; margin-top: 15px;}
    .multipla-box {background-color: #9C27B0; color: white; padding: 10px; border-radius: 8px; text-align: center; font-weight: bold; margin-top: 15px;}
    .alerta-over-box {background-color: #FF9800; color: white; padding: 10px; border-radius: 8px; text-align: center; font-weight: bold; margin-top: 15px;}
    .status-online {color: #00FF00; font-weight: bold; animation: pulse 2s infinite; padding: 5px 10px; border: 1px solid #00FF00; text-align: center; margin-bottom: 20px; border-radius: 15px;}
    .status-sleep {color: #448AFF; font-weight: bold; padding: 5px 10px; border: 1px solid #448AFF; text-align: center; margin-bottom: 20px; border-radius: 15px;}
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.7); } 70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); } 100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); } }
    .metric-val {font-size: 22px; font-weight: bold;}
    .metric-label {font-size: 10px; color: #888; text-transform: uppercase;}
    .stats-row { display: flex; justify-content: space-around; text-align: center; margin-top: 15px; padding-top: 15px; border-top: 1px solid #333; }
</style>
""", unsafe_allow_html=True)

# --- 2. AJUSTE DE FUSO HORÁRIO (BRASIL) ---
def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

def traduzir_instrucao(sinal, favorito=""):
    if "MÚLTIPLA" in sinal: return "Excelente para incluir em bilhetes de 2 ou mais jogos."
    if "PRÓXIMO GOL" in sinal: return f"Entrar no mercado de 'Próximo Gol' para o {favorito}."
    if "OVER GOLS" in sinal: return "Entrar no mercado de Over (mais de) gols na partida."
    if "HT" in sinal: return "Entrar para sair mais um gol ainda no primeiro tempo."
    return "Analisar entrada no mercado de gols ou vencedor."

# --- 3. FUNÇÕES DE BANCO DE DADOS E NOTIFICAÇÃO ---
DB_FILE = 'neves_dados.txt'

def enviar_msg_telegram(token, chat_ids, mensagem):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid.strip(), "text": mensagem, "parse_mode": "Markdown"})
            except: pass

def carregar_db():
    if not os.path.exists(DB_FILE):
        df = pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
        df.to_csv(DB_FILE, index=False)
        return df
    try: return pd.read_csv(DB_FILE)
    except: return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])

def salvar_sinal_db(fixture_id, jogo, sinal, gols_inicial):
    df = carregar_db()
    if not ((df['id'] == fixture_id) & (df['status'] == 'Pendente')).any():
        data_br = agora_brasil()
        novo_registro = {
            'id': fixture_id, 'data': data_br.strftime('%Y-%m-%d'), 'hora': data_br.strftime('%H:%M'),
            'jogo': jogo, 'sinal': sinal, 'gols_inicial': gols_inicial, 'status': 'Pendente'
        }
        df = pd.concat([df, pd.DataFrame([novo_registro])], ignore_index=True)
        df.to_csv(DB_FILE, index=False)

def atualizar_status_db(lista_jogos_api, tg_token=None, tg_chat_ids=None):
    df = carregar_db()
    if df.empty: return df
    modificado = False
    pendentes = df[df['status'] == 'Pendente']
    for index, row in pendentes.iterrows():
        jogo_dados = next((j for j in lista_jogos_api if j['fixture']['id'] == row['id']), None)
        if jogo_dados:
            gols_agora = (jogo_dados['goals']['home'] or 0) + (jogo_dados['goals']['away'] or 0)
            status_match = jogo_dados['fixture']['status']['short']
            if gols_agora > row['gols_inicial']:
                df.at[index, 'status'] = 'Green'
                modificado = True
                if tg_token and tg_chat_ids:
                    msg = f"✅ **GREEN! PAGOU!**\n\n⚽ **{row['jogo']}**\nSinal: {row['sinal']}"
                    enviar_msg_telegram(tg_token, tg_chat_ids, msg)
            elif status_match in ['FT', 'AET', 'PEN']:
                df.at[index, 'status'] = 'Red'
                modificado = True
    if modificado: df.to_csv(DB_FILE, index=False)
    return df

# --- 4. SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Configurações")
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("Chave API-SPORTS:", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    st.markdown("---")
    tg_token = st.text_input("Telegram Bot Token:", type="password")
    tg_chat_ids = st.text_input("Telegram Chat IDs:")
    st.markdown("---")
    ROBO_LIGADO = st.checkbox("LIGAR ROBÔ", value=False)
    INTERVALO = st.slider("Ciclo (seg):", 60, 300, 60)
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)

# --- 5. FUNÇÕES DE API ---
if 'ligas_sem_stats' not in st.session_state or isinstance(st.session_state['ligas_sem_stats'], set):
    st.session_state['ligas_sem_stats'] = {}

def buscar_jogos_live(api_key):
    if MODO_DEMO: return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 35}}, "league": {"id": 999, "name": "Simulada", "country": "Brasil"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Real"}, "away": {"name": "Almeria"}}}]
    url = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": api_key}
    try: return requests.get(url, headers=headers, params={"live": "all"}).json().get('response', [])
    except: return []

def buscar_stats(api_key, fixture_id):
    if MODO_DEMO: return [{"statistics": [{"type": "Total Shots", "value": 10}, {"type": "Shots on Goal", "value": 4}, {"type": "Dangerous Attacks", "value": 40}]}, {"statistics": [{"type": "Total Shots", "value": 5}, {"type": "Shots on Goal", "value": 2}, {"type": "Dangerous Attacks", "value": 20}]}]
    url = "https://v3.football.api-sports.io/fixtures/statistics"
    headers = {"x-apisports-key": api_key}
    try: return requests.get(url, headers=headers, params={"fixture": fixture_id}).json().get('response', [])
    except: return []

# --- 6. CÉREBRO ANALÍTICO ---
def analisar_partida(tempo, s_casa, s_fora, t_casa, t_fora, sc, sf):
    def v(d, k): val = d.get(k, 0); return int(str(val).replace('%','')) if val else 0
    gol_c = v(s_casa, 'Shots on Goal'); gol_f = v(s_fora, 'Shots on Goal')
    chutes_c = v(s_casa, 'Total Shots'); chutes_f = v(s_fora, 'Total Shots')
    atq_c = v(s_casa, 'Dangerous Attacks'); atq_f = v(s_fora, 'Dangerous Attacks')
    total_chutes = chutes_c + chutes_f
    sinal = None; motivo = ""; tipo = "normal"
    
    if tempo <= 30 and (sc + sf) >= 2:
        sinal = "CANDIDATO P/ MÚLTIPLA"; motivo = "Jogo muito aberto cedo."; tipo="multipla"
    elif 5 <= tempo <= 20 and (chutes_c + chutes_f) >= 4:
        sinal = "GOL HT (PRESSÃO INICIAL)"; motivo = "Muitas finalizações no início."; tipo="over"
    
    return sinal, motivo, total_chutes, (gol_c + gol_f), (atq_c + atq_f), tipo

# --- 7. EXECUÇÃO ---
st.title("❄️ Neves Analytics")

if ROBO_LIGADO:
    st.markdown('<div class="status-online">🟢 SISTEMA ONLINE</div>', unsafe_allow_html=True)
    jogos_live = buscar_jogos_live(API_KEY)
    atualizar_status_db(jogos_live, tg_token, tg_chat_ids)
    
    radar = []
    for jogo in jogos_live:
        f_id = jogo['fixture']['id']
        tempo = jogo['fixture']['status'].get('elapsed', 0)
        l_id = jogo['league']['id']
        l_name = jogo['league']['name']
        l_country = jogo['league']['country']
        sc = jogo['goals']['home'] or 0
        sf = jogo['goals']['away'] or 0
        
        info = {"Liga": l_name, "Jogo": f"{jogo['teams']['home']['name']} {sc}x{sf} {jogo['teams']['away']['name']}", "Status": "👁️"}
        
        if str(l_id) in st.session_state['ligas_sem_stats']:
            info["Status"] = "🚫 Bloqueada"
            radar.append(info)
            continue
            
        stats = buscar_stats(API_KEY, f_id)
        if not stats:
            if (sc + sf) > 0:
                st.session_state['ligas_sem_stats'][str(l_id)] = {"País": l_country, "Liga": l_name, "Motivo": "Gols sem Stats"}
                info["Status"] = "🚫 Bloqueada Agora"
            else:
                info["Status"] = "⏳ Aguardando"
        else:
            s_casa = {i['type']: i['value'] for i in stats[0]['statistics']}
            s_fora = {i['type']: i['value'] for i in stats[1]['statistics']}
            sinal, motivo, chutes, no_gol, atq_p, tipo = analisar_partida(tempo, s_casa, s_fora, jogo['teams']['home']['name'], jogo['teams']['away']['name'], sc, sf)
            
            if sinal:
                st.info(f"Sinal Detectado: {sinal} em {info['Jogo']}")
                salvar_sinal_db(f_id, info['Jogo'], sinal, sc+sf)
                # Envio Telegram aqui...
        
        radar.append(info)

    t1, t2, t3 = st.tabs(["📡 Radar", "📊 Histórico", "🚫 Bloqueadas"])
    with t1: st.table(radar)
    with t2: st.dataframe(carregar_db())
    with t3: st.table(list(st.session_state['ligas_sem_stats'].values()))
    
    time.sleep(INTERVALO)
    st.rerun()
