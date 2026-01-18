import streamlit as st
import pandas as pd
import requests
import time
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import io
from datetime import datetime, timedelta
import pytz
from streamlit_gsheets import GSheetsConnection
import google.generativeai as genai
import json
import re

# ==============================================================================
# 1. CONFIGURAÇÃO INICIAL E CSS
# ==============================================================================
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")
placeholder_root = st.empty()

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .main .block-container { max-width: 100%; padding: 1rem 1rem 5rem 1rem; }
    .metric-box { background-color: #1A1C24; border: 1px solid #333; border-radius: 8px; padding: 15px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.2); }
    .metric-title {font-size: 12px; color: #aaaaaa; text-transform: uppercase; margin-bottom: 5px;}
    .metric-value {font-size: 24px; font-weight: bold; color: #00FF00;}
    .metric-sub {font-size: 12px; color: #cccccc; margin-top: 5px;}
    .status-active { background-color: #1F4025; color: #00FF00; border: 1px solid #00FF00; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 15px; font-weight: bold;}
    .status-error { background-color: #3B1010; color: #FF4B4B; border: 1px solid #FF4B4B; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 15px; font-weight: bold;}
    .status-warning { background-color: #3B3B10; color: #FFFF00; border: 1px solid #FFFF00; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 15px; font-weight: bold;}
    .stButton button { width: 100%; height: 50px !important; font-size: 16px !important; font-weight: bold !important; background-color: #262730; border: 1px solid #4e4e4e; color: white; }
    .stButton button:hover { border-color: #00FF00; color: #00FF00; }
    .footer-timer { position: fixed; left: 0; bottom: 0; width: 100%; background-color: #0E1117; color: #FFD700; text-align: center; padding: 8px; font-size: 14px; border-top: 1px solid #333; z-index: 9999; }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. INICIALIZAÇÃO E ESTADO (SESSION STATE)
# ==============================================================================
for key in ['TG_TOKEN', 'TG_CHAT', 'API_KEY', 'bi_enviado_data', 'last_check_date']:
    if key not in st.session_state: st.session_state[key] = ""

if 'ROBO_LIGADO' not in st.session_state: st.session_state.ROBO_LIGADO = False
if 'last_db_update' not in st.session_state: st.session_state['last_db_update'] = 0
if 'api_usage' not in st.session_state: st.session_state['api_usage'] = {'used': 0, 'limit': 75000}
if 'gemini_usage' not in st.session_state: st.session_state['gemini_usage'] = {'used': 0, 'limit': 10000}
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'jogos_salvos_bigdata' not in st.session_state: st.session_state['jogos_salvos_bigdata'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'controle_stats' not in st.session_state: st.session_state['controle_stats'] = {}
if 'ia_bloqueada_ate' not in st.session_state: st.session_state['ia_bloqueada_ate'] = None
if 'multiplas_enviadas' not in st.session_state: st.session_state['multiplas_enviadas'] = set()

# ==============================================================================
# 3. CONFIGURAÇÃO IA & GOOGLE SHEETS
# ==============================================================================
IA_ATIVADA = False
try:
    if "GEMINI_KEY" in st.secrets:
        genai.configure(api_key=st.secrets["GEMINI_KEY"])
        model_ia = genai.GenerativeModel('gemini-2.0-flash') 
        IA_ATIVADA = True
except: IA_ATIVADA = False

conn = st.connection("gsheets", type=GSheetsConnection)

COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID', 'Odd', 'Odd_Atualizada']
COLS_BIGDATA = ['FID', 'Data', 'Liga', 'Jogo', 'Placar_Final', 'Chutes_Total', 'Chutes_Gol', 'Escanteios', 'Posse_Casa', 'Faltas', 'Ataques_Perigosos']

# ==============================================================================
# 4. FUNÇÕES DE UTILIDADE E PERFORMANCE
# ==============================================================================
def get_time_br(): return datetime.now(pytz.timezone('America/Sao_Paulo'))

def limpar_cache_radar(ids_vivos):
    """Remove do cache jogos que não estão mais Live para evitar travamento visual."""
    chaves = list(st.session_state.keys())
    for c in chaves:
        if c.startswith("st_"):
            fid = c.replace("st_", "")
            if fid not in [str(x) for x in ids_vivos]:
                del st.session_state[c]

def gv(l, t): 
    """Busca valor nas estatísticas da API."""
    try: return next((x['value'] for x in l if x['type']==t), 0) or 0
    except: return 0

# ==============================================================================
# 5. FUNÇÕES DE BANCO DE DADOS (GOOGLE SHEETS)
# ==============================================================================
def carregar_aba(nome_aba, colunas):
    try:
        df = conn.read(worksheet=nome_aba, ttl=0)
        return df.fillna("").astype(str) if not df.empty else pd.DataFrame(columns=colunas)
    except: return pd.DataFrame(columns=colunas)

def salvar_aba(nome_aba, df):
    try: conn.update(worksheet=nome_aba, data=df); return True
    except: return False

def salvar_bigdata_append(jogo_api, stats):
    """Melhoria: Salva no Big Data usando lógica de anexar para não travar."""
    fid = str(jogo_api['fixture']['id'])
    if fid in st.session_state['jogos_salvos_bigdata']: return
    try:
        s1, s2 = stats[0]['statistics'], stats[1]['statistics']
        novo = {
            'FID': fid, 'Data': get_time_br().strftime('%Y-%m-%d'),
            'Liga': jogo_api['league']['name'], 'Jogo': f"{jogo_api['teams']['home']['name']} x {jogo_api['teams']['away']['name']}",
            'Placar_Final': f"{jogo_api['goals']['home']}x{jogo_api['goals']['away']}",
            'Chutes_Total': gv(s1, 'Total Shots') + gv(s2, 'Total Shots'),
            'Chutes_Gol': gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal'),
            'Escanteios': gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks'),
            'Posse_Casa': f"{gv(s1, 'Ball Possession')}/{gv(s2, 'Ball Possession')}",
            'Faltas': gv(s1, 'Fouls') + gv(s2, 'Fouls'),
            'Ataques_Perigosos': gv(s1, 'Dangerous Attacks') + gv(s2, 'Dangerous Attacks')
        }
        df_new = pd.DataFrame([novo])
        conn.create(worksheet="BigData", data=df_new)
        st.session_state['jogos_salvos_bigdata'].add(fid)
    except: pass

# ==============================================================================
# 6. INTELIGÊNCIA ARTIFICIAL (GEMINI)
# ==============================================================================
def consultar_ia_gemini(dados_jogo, estrategia, stats_raw):
    if not IA_ATIVADA or st.session_state['ia_bloqueada_ate']: return ""
    try:
        prompt = f"Trader Profissional. Jogo: {dados_jogo['jogo']} aos {dados_jogo['tempo']}'. Estratégia: {estrategia}. Dados: {stats_raw}. Responda apenas: APROVADO ou ARRISCADO + motivo curto."
        resp = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return f"\n🤖 <b>IA:</b> {resp.text.strip()}"
    except: return ""

def analisar_bi_com_ia():
    if not IA_ATIVADA: return "IA offline."
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados."
    prompt = f"Analise o histórico de hoje: {df.tail(10).to_json()}. Dê 3 dicas para amanhã."
    return model_ia.generate_content(prompt).text

# ==============================================================================
# 7. MOTOR DE ESTRATÉGIAS (AQUI ESTÃO SEUS GOLS!)
# ==============================================================================
def processar_estrategias(j, stats, tempo, placar, rk_h=None, rk_a=None):
    if not stats: return []
    s1, s2 = stats[0]['statistics'], stats[1]['statistics']
    sh_h, sog_h = gv(s1, 'Total Shots'), gv(s1, 'Shots on Goal')
    sh_a, sog_a = gv(s2, 'Total Shots'), gv(s2, 'Shots on Goal')
    gh, ga = j['goals']['home'] or 0, j['goals']['away'] or 0
    
    # Momentum (Pressão 7 min)
    fid = j['fixture']['id']
    rh, ra = momentum(fid, sog_h, sog_a)
    
    sinais = []
    # ⚡ GOL RELÂMPAGO
    if (gh+ga) == 0 and tempo <= 10 and (sh_h+sh_a) >= 2:
        sinais.append({"tag": "⚡ Gol Relâmpago", "ordem": "Over 0.5 HT", "stats": f"{sh_h+sh_a} Chutes"})
    
    # 🔥 MASSACRE
    if rk_h and rk_a and tempo <= 15:
        if (rk_h <= 4 and rk_a >= 12 and rh >= 2):
            sinais.append({"tag": "🔥 Massacre", "ordem": "Over 0.5 HT / Favorito", "stats": "Pressão Total"})

    # 💎 GOLDEN BET (Limite)
    if 75 <= tempo <= 85 and abs(gh-ga) <= 1 and (sh_h+sh_a) >= 16:
        sinais.append({"tag": "💎 GOLDEN BET", "ordem": "Over Gols Limite", "stats": "Pressão Final"})

    # 🟢 BLITZ
    if tempo <= 60 and ((gh <= ga and rh >= 2) or (ga <= gh and ra >= 2)):
        sinais.append({"tag": "🟢 Blitz", "ordem": "Over Gols Partida", "stats": "Ataque Constante"})

    return sinais

def momentum(fid, sog_h, sog_a):
    mem = st.session_state['memoria_pressao'].get(fid, {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
    now = datetime.now()
    if sog_h > mem['sog_h']: mem['h_t'].append(now)
    if sog_a > mem['sog_a']: mem['a_t'].append(now)
    mem['h_t'] = [t for t in mem['h_t'] if now - t <= timedelta(minutes=7)]
    mem['a_t'] = [t for t in mem['a_t'] if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a
    st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_t']), len(mem['a_t'])

# ==============================================================================
# 8. TELEGRAM E NOTIFICAÇÕES
# ==============================================================================
def enviar_telegram(msg):
    token, chat = st.session_state['TG_TOKEN'], st.session_state['TG_CHAT']
    if not token or not chat: return
    for cid in chat.split(','):
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid.strip(), "text": msg, "parse_mode": "HTML"})

# ==============================================================================
# 9. LOOP PRINCIPAL (EXECUÇÃO)
# ==============================================================================
if st.session_state.ROBO_LIGADO:
    with placeholder_root.container():
        # Sincronização de Banco (a cada 10 min)
        if time.time() - st.session_state['last_db_update'] > 600:
            st.session_state['historico_full'] = carregar_aba("Historico", COLS_HIST)
            st.session_state['last_db_update'] = time.time()

        # Chamada API Live
        api_key = st.session_state['API_KEY']
        try:
            # Melhoria: live=all e filtragem manual para garantir que são jogos de HOJE
            res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"live": "all", "timezone": "America/Sao_Paulo"}).json()
            jogos_api = res.get('response', [])
            
            # FILTRO CRÍTICO: Remove jogos encerrados ou de datas passadas que o cache da API mantém
            jogos_live = [j for j in jogos_api if j['fixture']['status']['short'] not in ['FT', 'AET', 'PEN', 'PST']]
            ids_vivos = [j['fixture']['id'] for j in jogos_live]
            limpar_cache_radar(ids_vivos)
        except: jogos_live = []

        radar_data = []
        for j in jogos_live:
            fid = j['fixture']['id']
            tempo = j['fixture']['status']['elapsed'] or 0
            home, away = j['teams']['home']['name'], j['teams']['away']['name']
            placar = f"{j['goals']['home']}x{j['goals']['away']}"
            
            # Busca Stats se necessário
            if f"st_{fid}" not in st.session_state or tempo % 5 == 0:
                s_res = requests.get("https://v3.football.api-sports.io/fixtures/statistics", headers={"x-apisports-key": api_key}, params={"fixture": fid}).json()
                if s_res.get('response'): st.session_state[f"st_{fid}"] = s_res['response']
            
            stats = st.session_state.get(f"st_{fid}")
            sinais = processar_estrategias(j, stats, tempo, placar)
            
            for s in sinais:
                uid = f"{fid}_{s['tag']}"
                if uid not in st.session_state['alertas_enviados']:
                    analise_ia = consultar_ia_gemini({'jogo': f"{home} x {away}", 'tempo': tempo}, s['tag'], stats)
                    msg = f"<b>🚨 SINAL: {s['tag']}</b>\n⚽ {home} x {away}\n⏰ {tempo}' ({placar})\n🎯 {s['ordem']}{analise_ia}"
                    enviar_telegram(msg)
                    st.session_state['alertas_enviados'].add(uid)
                    # Adiciona ao Histórico RAM
                    # (Lógica de salvar no Sheets ocorre no final do ciclo)

            radar_data.append({"Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Tempo": f"{tempo}'", "Placar": placar})

        # UI
        st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        col1.metric("Radar Live", len(radar_data))
        col2.metric("Sinais Enviados", len(st.session_state['alertas_enviados']))
        
        st.tabs(["📡 Radar", "💰 Financeiro", "📜 Histórico", "📊 BI", "💾 Big Data"])
        with st.expander("Visualizar Radar"):
            st.table(radar_data)

        # Timer
        for i in range(60, 0, -1):
            st.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()
else:
    st.title("❄️ Neves Analytics - Sniper Gol")
    st.info("Configure as chaves e clique em 'Ligar Robô' na barra lateral.")
