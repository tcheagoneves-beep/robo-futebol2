import streamlit as st
import pandas as pd
import requests
import time
import os
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib
matplotlib.use('Agg')
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
import firebase_admin
from firebase_admin import credentials, firestore

# ==============================================================================
# 1. CONFIGURAÇÃO INICIAL E CSS
# ==============================================================================
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")
placeholder_root = st.empty()

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .main .block-container { max-width: 100%; padding: 1rem 1rem 80px 1rem; }
    .metric-box { background-color: #1A1C24; border: 1px solid #333; border-radius: 8px; padding: 10px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.2); margin-bottom: 10px; }
    .metric-title {font-size: 10px; color: #aaaaaa; text-transform: uppercase; margin-bottom: 2px;}
    .metric-value {font-size: 20px; font-weight: bold; color: #00FF00;}
    .metric-sub {font-size: 10px; color: #cccccc;}
    .status-active { background-color: #1F4025; color: #00FF00; border: 1px solid #00FF00; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 5px; font-weight: bold; font-size: 14px;}
    .status-error { background-color: #3B1010; color: #FF4B4B; border: 1px solid #FF4B4B; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 5px; font-weight: bold; font-size: 14px;}
    .status-warning { background-color: #3B3B10; color: #FFFF00; border: 1px solid #FFFF00; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 5px; font-weight: bold; font-size: 14px;}
    .stButton button { width: 100%; height: 55px !important; font-size: 18px !important; font-weight: bold !important; background-color: #262730; border: 1px solid #4e4e4e; color: white; border-radius: 8px; }
    .stButton button:hover { border-color: #00FF00; color: #00FF00; }
    .footer-timer { position: fixed; left: 0; bottom: 0; width: 100%; background-color: #0E1117; color: #FFD700; text-align: center; padding: 10px; font-size: 12px; border-top: 1px solid #333; z-index: 99999; box-shadow: 0 -2px 10px rgba(0,0,0,0.5); }
    .stDataFrame { font-size: 12px; }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. INICIALIZAÇÃO DE VARIÁVEIS E CONSTANTES
# ==============================================================================
ODD_MINIMA_LIVE = 1.60
ODD_CRITICA_LIVE = 1.30

if 'TG_TOKEN' not in st.session_state: st.session_state['TG_TOKEN'] = ""
if 'TG_CHAT' not in st.session_state: st.session_state['TG_CHAT'] = ""
if 'API_KEY' not in st.session_state: st.session_state['API_KEY'] = ""
if 'ROBO_LIGADO' not in st.session_state: st.session_state.ROBO_LIGADO = False
if 'last_db_update' not in st.session_state: st.session_state['last_db_update'] = 0
if 'last_static_update' not in st.session_state: st.session_state['last_static_update'] = 0
if 'stake_padrao' not in st.session_state: st.session_state['stake_padrao'] = 10.0
if 'banca_inicial' not in st.session_state: st.session_state['banca_inicial'] = 100.0
if 'api_usage' not in st.session_state: st.session_state['api_usage'] = {'used': 0, 'limit': 75000}
if 'data_api_usage' not in st.session_state: st.session_state['data_api_usage'] = datetime.now(pytz.utc).date()
if 'gemini_usage' not in st.session_state: st.session_state['gemini_usage'] = {'used': 0, 'limit': 10000}
if 'alvos_do_dia' not in st.session_state: st.session_state['alvos_do_dia'] = {}
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'var_avisado_cache' not in st.session_state: st.session_state['var_avisado_cache'] = set()
if 'multiplas_enviadas' not in st.session_state: st.session_state['multiplas_enviadas'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'controle_stats' not in st.session_state: st.session_state['controle_stats'] = {}
if 'jogos_salvos_bigdata' not in st.session_state: st.session_state['jogos_salvos_bigdata'] = set()
if 'jogos_salvos_bigdata_carregados' not in st.session_state: st.session_state['jogos_salvos_bigdata_carregados'] = False
if 'ia_bloqueada_ate' not in st.session_state: st.session_state['ia_bloqueada_ate'] = None
if 'last_check_date' not in st.session_state: st.session_state['last_check_date'] = ""
if 'bi_enviado' not in st.session_state: st.session_state['bi_enviado'] = False
if 'ia_enviada' not in st.session_state: st.session_state['ia_enviada'] = False
if 'financeiro_enviado' not in st.session_state: st.session_state['financeiro_enviado'] = False
if 'bigdata_enviado' not in st.session_state: st.session_state['bigdata_enviado'] = False
if 'matinal_enviado' not in st.session_state: st.session_state['matinal_enviado'] = False
if 'precisa_salvar' not in st.session_state: st.session_state['precisa_salvar'] = False
if 'BLOQUEAR_SALVAMENTO' not in st.session_state: st.session_state['BLOQUEAR_SALVAMENTO'] = False
if 'total_bigdata_count' not in st.session_state: st.session_state['total_bigdata_count'] = 0
if 'multipla_matinal_enviada' not in st.session_state: st.session_state['multipla_matinal_enviada'] = False
if 'multiplas_live_cache' not in st.session_state: st.session_state['multiplas_live_cache'] = {}
if 'multiplas_pendentes' not in st.session_state: st.session_state['multiplas_pendentes'] = []
if 'alternativos_enviado' not in st.session_state: st.session_state['alternativos_enviado'] = False
if 'alavancagem_enviada' not in st.session_state: st.session_state['alavancagem_enviada'] = False
if 'drop_enviado_12' not in st.session_state: st.session_state['drop_enviado_12'] = False
if 'drop_enviado_16' not in st.session_state: st.session_state['drop_enviado_16'] = False
# [FUSÃO] Toggle para IA Profunda (Tier 2 - H2H, Momentum, etc)
if 'ia_profunda_ativada' not in st.session_state: st.session_state['ia_profunda_ativada'] = False

db_firestore = None
if "FIREBASE_CONFIG" in st.secrets:
    try:
        if not firebase_admin._apps:
            fb_creds = json.loads(st.secrets["FIREBASE_CONFIG"])
            cred = credentials.Certificate(fb_creds)
            firebase_admin.initialize_app(cred)
        db_firestore = firestore.client()
    except Exception as e: st.error(f"Erro Firebase: {e}")

IA_ATIVADA = False
try:
    if "GEMINI_KEY" in st.secrets:
        genai.configure(api_key=st.secrets["GEMINI_KEY"])
        model_ia = genai.GenerativeModel('gemini-2.0-flash')
        IA_ATIVADA = True
except: IA_ATIVADA = False

conn = st.connection("gsheets", type=GSheetsConnection)

COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID', 'Odd', 'Odd_Atualizada', 'Opiniao_IA', 'Probabilidade', 'Stake_Recomendado_Pct', 'Stake_Recomendado_RS', 'Modo_Gestao']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo', 'Strikes', 'Jogos_Erro']
COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes', 'Jogos_Erro']
COLS_BLACK = ['id', 'País', 'Liga', 'Motivo']
LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]
DB_CACHE_TIME = 60
STATIC_CACHE_TIME = 600

MAPA_ODDS_TEORICAS = {
    "🟣 Porteira Aberta": {"min": 1.50, "max": 1.80},
    "⚡ Gol Relâmpago": {"min": 1.30, "max": 1.45},
    "💰 Janela de Ouro": {"min": 1.70, "max": 2.10},
    "🟢 Blitz Casa": {"min": 1.50, "max": 1.70},
    "🟢 Blitz Visitante": {"min": 1.50, "max": 1.70},
    "🔥 Massacre": {"min": 1.25, "max": 1.40},
    "⚔️ Choque Líderes": {"min": 1.40, "max": 1.60},
    "🥊 Briga de Rua": {"min": 1.40, "max": 1.60},
    "❄️ Jogo Morno": {"min": 1.20, "max": 1.35},
    "💎 GOLDEN BET": {"min": 1.80, "max": 2.40},
    "🏹 Tiroteio Elite": {"min": 1.40, "max": 1.60},
    "⚡ Contra-Ataque Letal": {"min": 1.60, "max": 2.20},
    "💎 Sniper Final": {"min": 1.80, "max": 2.50},
    "🦁 Back Favorito (Nettuno)": {"min": 1.40, "max": 1.60},
    "🔫 Lay Goleada": {"min": 1.60, "max": 2.20},
    "👴 Estratégia do Vovô": {"min": 1.05, "max": 1.25},
    "🟨 Sniper de Cartões": {"min": 1.50, "max": 1.90},
    "🧤 Muralha (Defesas)": {"min": 1.60, "max": 2.10},
    "🧊 Arame Liso": {"min": 1.20, "max": 1.50}
}

ODD_MINIMA_POR_ESTRATEGIA = {
    "estratégia do vovô": 1.20, "vovô": 1.20,
    "jogo morno": 1.35, "morno": 1.35,
    "porteira aberta": 1.50, "porteira": 1.50,
    "golden bet": 1.80, "golden": 1.80,
    "gol relâmpago": 1.60, "relâmpago": 1.60,
    "blitz": 1.60, "massacre": 1.70,
    "alavancagem": 3.50, "sniper": 1.80,
    "arame liso": 1.35, "under": 1.40,
}

# ==============================================================================
# 3. FUNÇÕES AUXILIARES
# ==============================================================================
def get_time_br(): return datetime.now(pytz.timezone('America/Sao_Paulo'))
def clean_fid(x):
    try: return str(int(float(x)))
    except: return '0'
def normalizar_id(val):
    try:
        s_val = str(val).strip()
        if not s_val or s_val.lower() == 'nan': return ""
        return str(int(float(s_val)))
    except: return str(val).strip()
def formatar_inteiro_visual(val):
    try:
        if str(val) == 'nan' or str(val) == '': return "0"
        return str(int(float(str(val))))
    except: return str(val)

def gerar_chave_universal(fid, estrategia, tipo_sinal="SINAL"):
    try: fid_clean = str(int(float(str(fid).strip())))
    except: fid_clean = str(fid).strip()
    strat_clean = str(estrategia).strip().upper().replace(" ", "_")
    chave = f"{fid_clean}_{strat_clean}"
    if tipo_sinal == "SINAL": return chave
    elif tipo_sinal == "GREEN": return f"RES_GREEN_{chave}"
    elif tipo_sinal == "RED": return f"RES_RED_{chave}"
    return chave

# ==============================================================================
# [FUSÃO] CLASSIFICAÇÃO OVER/UNDER/RESULTADO + KELLY + ODD HELPERS
# ==============================================================================
def classificar_tipo_estrategia(estrategia: str) -> str:
    estrategia_lower = str(estrategia or '').lower()
    resultado_keywords = ['estratégia do vovô','estrategia do vovo','vovô','vovo','contra-ataque','contra ataque','back','vitória','vitoria','empate']
    under_keywords = ['jogo morno','arame liso','under','sem gols','morno','arame']
    over_keywords = ['porteira aberta','golden bet','gol relâmpago','gol relampago','blitz casa','blitz visitante','blitz','massacre','choque','briga de rua','janela de ouro','janela ouro','tiroteio elite','sniper final','sniper matinal','lay goleada','over','btts','ambas marcam','gol']
    for kw in resultado_keywords:
        if kw in estrategia_lower: return 'RESULTADO'
    for kw in under_keywords:
        if kw in estrategia_lower: return 'UNDER'
    for kw in over_keywords:
        if kw in estrategia_lower: return 'OVER'
    return 'NEUTRO'

def obter_descricao_aposta(estrategia: str) -> dict:
    tipo = classificar_tipo_estrategia(estrategia)
    e = str(estrategia or '').lower()
    if 'golden bet' in e: return {'tipo':'OVER','aposta':'Mais de 0.5 Gols (Asiático)','ordem':'👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols','ganha_se':'Sai GOL','perde_se':'Não sai GOL'}
    if 'janela' in e and 'ouro' in e: return {'tipo':'OVER','aposta':'Mais de 0.5 Gols (Asiático)','ordem':'👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols','ganha_se':'Sai GOL','perde_se':'Não sai GOL'}
    if 'gol relâmpago' in e or 'gol relampago' in e: return {'tipo':'OVER','aposta':'Mais de 0.5 Gols 1T','ordem':'👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT','ganha_se':'Sai GOL no 1T','perde_se':'Não sai GOL no 1T'}
    if 'blitz' in e: return {'tipo':'OVER','aposta':'Mais de 0.5 Gols','ordem':'👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols','ganha_se':'Sai GOL','perde_se':'Não sai GOL'}
    if 'porteira' in e: return {'tipo':'OVER','aposta':'Mais de 0.5 Gols','ordem':'👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols','ganha_se':'Sai GOL','perde_se':'Não sai GOL'}
    if 'tiroteio' in e: return {'tipo':'OVER','aposta':'Mais de 0.5 Gols','ordem':'👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols','ganha_se':'Sai GOL','perde_se':'Não sai GOL'}
    if 'sniper' in e: return {'tipo':'OVER','aposta':'Mais de 0.5 Gols','ordem':'👉 FAZER: Over Gol Limite (Acréscimos)\n✅ Aposta: Mais de 0.5 Gols','ganha_se':'Sai GOL','perde_se':'Não sai GOL'}
    if 'lay goleada' in e: return {'tipo':'OVER','aposta':'Mais de 0.5 Gols','ordem':'👉 FAZER: Entrar em GOL LIMITE (gol da honra)\n✅ Aposta: Mais de 0.5 Gols','ganha_se':'Sai GOL','perde_se':'Não sai GOL'}
    if 'massacre' in e: return {'tipo':'OVER','aposta':'Mais de 0.5 Gols 1T','ordem':'👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT','ganha_se':'Sai GOL no 1T','perde_se':'Não sai GOL no 1T'}
    if 'choque' in e: return {'tipo':'OVER','aposta':'Mais de 0.5 Gols 1T','ordem':'👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT','ganha_se':'Sai GOL no 1T','perde_se':'Não sai GOL no 1T'}
    if 'briga de rua' in e: return {'tipo':'OVER','aposta':'Mais de 0.5 Gols 1T','ordem':'👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT','ganha_se':'Sai GOL no 1T','perde_se':'Não sai GOL no 1T'}
    if 'contra-ataque' in e or 'contra ataque' in e: return {'tipo':'RESULTADO','aposta':'Back Empate ou Zebra','ordem':'👉 FAZER: Back no time PERDENDO\n⚡ Aposta: Recuperação ou Empate','ganha_se':'Time EMPATA ou VIRA','perde_se':'Favorito MANTÉM'}
    if 'jogo morno' in e or 'morno' in e: return {'tipo':'UNDER','aposta':'Menos de 0.5 Gols','ordem':'👉 FAZER: Entrar em UNDER GOLS\n❄️ Aposta: NÃO SAI mais gol','ganha_se':'NÃO sai GOL','perde_se':'Sai GOL'}
    if 'arame liso' in e or 'arame' in e: return {'tipo':'UNDER','aposta':'Menos de 0.5 Gols','ordem':'👉 FAZER: Entrar em UNDER GOLS\n🧊 Aposta: Falsa pressão, NÃO SAI gol','ganha_se':'NÃO sai GOL','perde_se':'Sai GOL'}
    if 'vovô' in e or 'vovo' in e: return {'tipo':'RESULTADO','aposta':'Vitória do time ganhando','ordem':'👉 FAZER: Back no time GANHANDO\n👴 Aposta: Manter vitória','ganha_se':'Time MANTÉM vantagem','perde_se':'Time EMPATA ou PERDE'}
    if tipo == 'OVER': return {'tipo':'OVER','aposta':'Mais de 0.5 Gols','ordem':'👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols','ganha_se':'Sai GOL','perde_se':'Não sai GOL'}
    if tipo == 'UNDER': return {'tipo':'UNDER','aposta':'Menos de 0.5 Gols','ordem':'👉 FAZER: Entrar em UNDER GOLS\n❄️ Aposta: NÃO SAI mais gol','ganha_se':'NÃO sai GOL','perde_se':'Sai GOL'}
    return {'tipo':'RESULTADO','aposta':'Resultado Final','ordem':'👉 FAZER: Apostar no resultado indicado','ganha_se':'Resultado se confirma','perde_se':'Resultado não se confirma'}

def calcular_gols_atuais(placar_str: str) -> int:
    try:
        gh, ga = map(int, str(placar_str).lower().replace(' ', '').split('x'))
        return int(gh) + int(ga)
    except: return 0

def rastrear_movimento_odd(fid, estrategia, odd_atual, janela_min=5):
    try:
        fid = str(fid); odd_atual = float(odd_atual)
    except: return 'ESTÁVEL', 0.0
    if 'odd_history' not in st.session_state: st.session_state['odd_history'] = {}
    hist = st.session_state['odd_history'].get(fid, [])
    agora = get_time_br()
    hist.append({'t': agora, 'odd': odd_atual, 'estrategia': str(estrategia)})
    limite = agora - timedelta(minutes=int(janela_min))
    hist = [x for x in hist if x.get('t') and x['t'] >= limite]
    st.session_state['odd_history'][fid] = hist
    if len(hist) < 2: return 'ESTÁVEL', 0.0
    odd_ini = hist[0]['odd']
    if odd_ini <= 0: return 'ESTÁVEL', 0.0
    variacao = ((odd_atual - odd_ini) / odd_ini) * 100.0
    if variacao <= -7: return 'CAINDO FORTE', variacao
    if variacao <= -3: return 'CAINDO', variacao
    if variacao >= 7: return 'SUBINDO FORTE', variacao
    if variacao >= 3: return 'SUBINDO', variacao
    return 'ESTÁVEL', variacao

def calcular_kelly_criterion(probabilidade, odd, modo='fracionario'):
    try:
        prob_decimal = float(probabilidade) / 100.0
        odd = float(odd)
        if odd <= 1.01: return 0.0
        kelly = (prob_decimal * odd - 1) / (odd - 1)
        if kelly <= 0: kelly = 0
        elif kelly > 0.25: kelly = 0.25
        if modo == 'conservador':
            if float(probabilidade) >= 85: return 2.0
            if float(probabilidade) >= 70: return 1.5
            return 1.0
        if modo == 'fracionario': kelly *= 0.5
        k = round(kelly * 100.0, 1)
        if 0 < k < 0.5: k = 0.5
        return k
    except: return 1.5

def calcular_stake_recomendado(banca_atual, probabilidade, odd, modo='fracionario'):
    try:
        banca_atual = float(banca_atual)
        pct = calcular_kelly_criterion(probabilidade, odd, modo)
        valor = round((banca_atual * pct) / 100.0, 2)
        if 0 < valor < 2.0: valor = 2.0
        return {'porcentagem': pct, 'valor': valor, 'modo': modo}
    except: return {'porcentagem': 1.5, 'valor': round(float(banca_atual or 0) * 0.015, 2), 'modo': 'erro'}

def obter_odd_minima(estrategia):
    try:
        estrategia_lower = str(estrategia or '').lower()
        for chave, odd_min in ODD_MINIMA_POR_ESTRATEGIA.items():
            if chave in estrategia_lower: return float(odd_min)
        return 1.50
    except: return 1.50

def buscar_odd_pre_match(api_key, fid):
    try:
        url = "https://v3.football.api-sports.io/odds"
        params = {"fixture": fid, "bookmaker": "8"}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        if not r.get('response'): return 0.0, "Sem Bet365"
        bookmakers = r['response'][0]['bookmakers']
        if not bookmakers: return 0.0, "Sem Bet365"
        bet365 = bookmakers[0]
        if bet365:
            mercado_gols = next((m for m in bet365['bets'] if m['id'] == 5), None)
            if mercado_gols:
                odd_obj = next((v for v in mercado_gols['values'] if v['value'] == "Over 2.5"), None)
                if not odd_obj: odd_obj = next((v for v in mercado_gols['values'] if v['value'] == "Over 1.5"), None)
                if odd_obj: return float(odd_obj['odd']), f"{odd_obj['value']} (Bet365)"
        return 0.0, "N/A"
    except: return 0.0, "N/A"

def update_api_usage(headers):
    if not headers: return
    try:
        limit = int(headers.get('x-ratelimit-requests-limit', 75000))
        remaining = int(headers.get('x-ratelimit-requests-remaining', 0))
        st.session_state['api_usage'] = {'used': limit - remaining, 'limit': limit}
    except: pass

def verificar_reset_diario():
    hoje_utc = datetime.now(pytz.utc).date()
    if st.session_state['data_api_usage'] != hoje_utc:
        st.session_state['api_usage']['used'] = 0; st.session_state['data_api_usage'] = hoje_utc
        st.session_state['gemini_usage']['used'] = 0; st.session_state['alvos_do_dia'] = {}
        st.session_state['matinal_enviado'] = False; st.session_state['multipla_matinal_enviada'] = False
        st.session_state['alternativos_enviado'] = False; st.session_state['alavancagem_enviada'] = False
        st.session_state['drop_enviado_12'] = False; st.session_state['drop_enviado_16'] = False
        return True
    return False

def testar_conexao_telegram(token):
    if not token: return False, "Token Vazio"
    try:
        res = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=5)
        if res.status_code == 200: return True, res.json()['result']['first_name']
        return False, f"Erro {res.status_code}"
    except: return False, "Sem Conexão"

def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    df_raw = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

# ==============================================================================
# [MÓDULO TRADING] DROP ODDS PRÉ-LIVE
# ==============================================================================
def buscar_odds_comparativas(api_key, fixture_id):
    url = "https://v3.football.api-sports.io/odds"
    try:
        params_b365 = {"fixture": fixture_id, "bookmaker": "8"}
        params_pin = {"fixture": fixture_id, "bookmaker": "4"}
        r365 = requests.get(url, headers={"x-apisports-key": api_key}, params=params_b365).json()
        rpin = requests.get(url, headers={"x-apisports-key": api_key}, params=params_pin).json()
        if r365.get('response'):
            mkts = r365['response'][0]['bookmakers'][0]['bets']
            vencedor = next((m for m in mkts if m['id'] == 1), None)
            if vencedor:
                v_casa = float(next((v['odd'] for v in vencedor['values'] if v['value'] == 'Home'), 0))
                v_fora = float(next((v['odd'] for v in vencedor['values'] if v['value'] == 'Away'), 0))
                if rpin.get('response'):
                    mkts_pin = rpin['response'][0]['bookmakers'][0]['bets']
                    vencedor_pin = next((m for m in mkts_pin if m['id'] == 1), None)
                    if vencedor_pin:
                        p_casa = float(next((v['odd'] for v in vencedor_pin['values'] if v['value'] == 'Home'), 0))
                        p_fora = float(next((v['odd'] for v in vencedor_pin['values'] if v['value'] == 'Away'), 0))
                        margem = 1.10
                        if v_casa > (p_casa * margem): return v_casa, p_casa, "Casa"
                        elif v_fora > (p_fora * margem): return v_fora, p_fora, "Visitante"
        return 0, 0, None
    except: return 0, 0, None

def scanner_drop_odds_pre_live(api_key):
    agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    hoje_str = agora.strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        res = requests.get(url, headers={"x-apisports-key": api_key}, params={"date": hoje_str, "timezone": "America/Sao_Paulo"}).json()
        jogos = res.get('response', [])
        LIGAS_PERMITIDAS = [39, 140, 78, 135, 61, 2, 3]
        oportunidades = []
        for j in jogos:
            lid = j['league']['id']; fid = j['fixture']['id']
            if lid not in LIGAS_PERMITIDAS: continue
            try: hora_jogo = datetime.fromisoformat(j['fixture']['date'].replace('Z', '+00:00'))
            except: continue
            diff = (hora_jogo - datetime.now(pytz.utc)).total_seconds() / 3600
            if j['fixture']['status']['short'] != 'NS' or not (3 <= diff <= 8): continue
            odd_b365, odd_pin, lado = buscar_odds_comparativas(api_key, fid)
            if odd_b365 > 0 and lado:
                diferenca = ((odd_b365 - odd_pin) / odd_pin) * 100
                oportunidades.append({"fid": fid, "jogo": f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", "liga": j['league']['name'], "hora": j['fixture']['date'][11:16], "lado": lado, "odd_b365": odd_b365, "odd_pinnacle": odd_pin, "valor": diferenca})
        return oportunidades
    except: return []

# ==============================================================================
# GERENCIAMENTO DE PLANILHAS E DADOS
# ==============================================================================
def carregar_aba(nome_aba, colunas_esperadas):
    chave_memoria = ""
    if nome_aba == "Historico": chave_memoria = 'historico_full'
    elif nome_aba == "Seguras": chave_memoria = 'df_safe'
    elif nome_aba == "Obs": chave_memoria = 'df_vip'
    elif nome_aba == "Blacklist": chave_memoria = 'df_black'
    try:
        df = conn.read(worksheet=nome_aba, ttl=10)
        if df.empty and chave_memoria in st.session_state:
            df_ram = st.session_state[chave_memoria]
            if not df_ram.empty: return df_ram
        if not df.empty:
            for col in colunas_esperadas:
                if col not in df.columns:
                    if col == 'Probabilidade': df[col] = "0"
                    else: df[col] = "1.20" if col == 'Odd' else ""
            return df.fillna("").astype(str)
        return pd.DataFrame(columns=colunas_esperadas)
    except:
        if chave_memoria and chave_memoria in st.session_state:
            df_ram = st.session_state[chave_memoria]
            if not df_ram.empty: return df_ram
        st.session_state['BLOQUEAR_SALVAMENTO'] = True
        return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df_para_salvar):
    if nome_aba in ["Historico", "Seguras", "Obs"] and df_para_salvar.empty: return False
    if st.session_state.get('BLOQUEAR_SALVAMENTO', False):
        st.session_state['precisa_salvar'] = True; return False
    try:
        conn.update(worksheet=nome_aba, data=df_para_salvar)
        if nome_aba == "Historico": st.session_state['precisa_salvar'] = False
        return True
    except:
        st.session_state['precisa_salvar'] = True; return False

def salvar_blacklist(id_liga, pais, nome_liga, motivo_ban):
    df = st.session_state['df_black']; id_norm = normalizar_id(id_liga)
    if id_norm in df['id'].values:
        idx = df[df['id'] == id_norm].index[0]; df.at[idx, 'Motivo'] = str(motivo_ban)
    else:
        novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': str(motivo_ban)}])
        df = pd.concat([df, novo], ignore_index=True)
    st.session_state['df_black'] = df; salvar_aba("Blacklist", df); sanitizar_conflitos()

def sanitizar_conflitos():
    df_black = st.session_state.get('df_black', pd.DataFrame())
    df_vip = st.session_state.get('df_vip', pd.DataFrame())
    df_safe = st.session_state.get('df_safe', pd.DataFrame())
    if df_black.empty or df_vip.empty or df_safe.empty: return
    alterou_black, alterou_vip, alterou_safe = False, False, False
    for idx, row in df_black.iterrows():
        id_b = normalizar_id(row['id']); motivo_atual = str(row['Motivo'])
        df_vip['id_norm'] = df_vip['id'].apply(normalizar_id); mask_vip = df_vip['id_norm'] == id_b
        if mask_vip.any():
            strikes = formatar_inteiro_visual(df_vip.loc[mask_vip, 'Strikes'].values[0])
            novo_motivo = f"Banida ({strikes} Jogos Sem Dados)"
            if motivo_atual != novo_motivo: df_black.at[idx, 'Motivo'] = novo_motivo; alterou_black = True
            df_vip = df_vip[~mask_vip]; alterou_vip = True
        df_safe['id_norm'] = df_safe['id'].apply(normalizar_id); mask_safe = df_safe['id_norm'] == id_b
        if mask_safe.any(): df_safe = df_safe[~mask_safe]; alterou_safe = True
    if 'id_norm' in df_vip.columns: df_vip = df_vip.drop(columns=['id_norm'])
    if 'id_norm' in df_safe.columns: df_safe = df_safe.drop(columns=['id_norm'])
    if alterou_black: st.session_state['df_black'] = df_black; salvar_aba("Blacklist", df_black)
    if alterou_vip: st.session_state['df_vip'] = df_vip; salvar_aba("Obs", df_vip)
    if alterou_safe: st.session_state['df_safe'] = df_safe; salvar_aba("Seguras", df_safe)

def salvar_safe_league_basic(id_liga, pais, nome_liga, tem_tabela=False):
    id_norm = normalizar_id(id_liga); df = st.session_state['df_safe']
    txt_motivo = "Validada (Chutes + Tabela)" if tem_tabela else "Validada (Chutes)"
    if id_norm not in df['id'].values:
        novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': txt_motivo, 'Strikes': '0', 'Jogos_Erro': ''}])
        final = pd.concat([df, novo], ignore_index=True)
        if salvar_aba("Seguras", final): st.session_state['df_safe'] = final; sanitizar_conflitos()
    else:
        idx = df[df['id'] == id_norm].index[0]
        if df.at[idx, 'Motivo'] != txt_motivo: df.at[idx, 'Motivo'] = txt_motivo; salvar_aba("Seguras", df); st.session_state['df_safe'] = df

def resetar_erros(id_liga):
    id_norm = normalizar_id(id_liga); df_safe = st.session_state.get('df_safe', pd.DataFrame())
    if not df_safe.empty and id_norm in df_safe['id'].values:
        idx = df_safe[df_safe['id'] == id_norm].index[0]
        if str(df_safe.at[idx, 'Strikes']) != '0':
            df_safe.at[idx, 'Strikes'] = '0'; df_safe.at[idx, 'Jogos_Erro'] = ''
            if salvar_aba("Seguras", df_safe): st.session_state['df_safe'] = df_safe

def gerenciar_erros(id_liga, pais, nome_liga, fid_jogo):
    id_norm = normalizar_id(id_liga); fid_str = str(fid_jogo)
    df_safe = st.session_state.get('df_safe', pd.DataFrame())
    if not df_safe.empty and id_norm in df_safe['id'].values:
        idx = df_safe[df_safe['id'] == id_norm].index[0]
        jogos_erro = str(df_safe.at[idx, 'Jogos_Erro']).split(',') if str(df_safe.at[idx, 'Jogos_Erro']).strip() else []
        if fid_str in jogos_erro: return
        jogos_erro.append(fid_str); strikes = len(jogos_erro)
        if strikes >= 10:
            df_safe = df_safe.drop(idx); salvar_aba("Seguras", df_safe); st.session_state['df_safe'] = df_safe
            df_vip = st.session_state.get('df_vip', pd.DataFrame())
            novo_obs = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Data_Erro': get_time_br().strftime('%Y-%m-%d'), 'Strikes': '1', 'Jogos_Erro': fid_str}])
            final_vip = pd.concat([df_vip, novo_obs], ignore_index=True); salvar_aba("Obs", final_vip); st.session_state['df_vip'] = final_vip
        else:
            df_safe.at[idx, 'Strikes'] = str(strikes); df_safe.at[idx, 'Jogos_Erro'] = ",".join(jogos_erro)
            salvar_aba("Seguras", df_safe); st.session_state['df_safe'] = df_safe
        return
    df_vip = st.session_state.get('df_vip', pd.DataFrame()); jogos_erro = []
    if not df_vip.empty and id_norm in df_vip['id'].values:
        row = df_vip[df_vip['id'] == id_norm].iloc[0]
        val_jogos = str(row.get('Jogos_Erro', '')).strip()
        if val_jogos: jogos_erro = val_jogos.split(',')
    if fid_str in jogos_erro: return
    jogos_erro.append(fid_str); strikes = len(jogos_erro)
    if strikes >= 10: salvar_blacklist(id_liga, pais, nome_liga, f"Banida ({formatar_inteiro_visual(strikes)} Jogos Sem Dados)")
    else:
        if id_norm in df_vip['id'].values:
            idx = df_vip[df_vip['id'] == id_norm].index[0]
            df_vip.at[idx, 'Strikes'] = str(strikes); df_vip.at[idx, 'Jogos_Erro'] = ",".join(jogos_erro)
            df_vip.at[idx, 'Data_Erro'] = get_time_br().strftime('%Y-%m-%d'); salvar_aba("Obs", df_vip); st.session_state['df_vip'] = df_vip
        else:
            novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Data_Erro': get_time_br().strftime('%Y-%m-%d'), 'Strikes': '1', 'Jogos_Erro': fid_str}])
            final = pd.concat([df_vip, novo], ignore_index=True); salvar_aba("Obs", final); st.session_state['df_vip'] = final

def carregar_tudo(force=False):
    now = time.time()
    if force or (now - st.session_state['last_static_update']) > STATIC_CACHE_TIME or 'df_black' not in st.session_state:
        st.session_state['df_black'] = carregar_aba("Blacklist", COLS_BLACK)
        st.session_state['df_safe'] = carregar_aba("Seguras", COLS_SAFE)
        st.session_state['df_vip'] = carregar_aba("Obs", COLS_OBS)
        if not st.session_state['df_black'].empty: st.session_state['df_black']['id'] = st.session_state['df_black']['id'].apply(normalizar_id)
        if not st.session_state['df_safe'].empty: st.session_state['df_safe']['id'] = st.session_state['df_safe']['id'].apply(normalizar_id)
        if not st.session_state['df_vip'].empty: st.session_state['df_vip']['id'] = st.session_state['df_vip']['id'].apply(normalizar_id)
        sanitizar_conflitos(); st.session_state['last_static_update'] = now
    if 'historico_full' not in st.session_state or force:
        df = carregar_aba("Historico", COLS_HIST)
        if not df.empty and 'Probabilidade' in df.columns:
            def normalizar_legado_prob(val):
                s_val = str(val).strip().replace(',', '.')
                if '%' in s_val: return s_val
                if s_val == 'nan' or s_val == '': return '0%'
                try:
                    float_val = float(s_val)
                    if float_val <= 1.0: float_val *= 100
                    return f"{int(float_val)}%"
                except: return s_val
            df['Probabilidade'] = df['Probabilidade'].apply(normalizar_legado_prob)
        if df.empty and 'historico_full' in st.session_state and not st.session_state['historico_full'].empty:
            df = st.session_state['historico_full']
        if not df.empty and 'Data' in df.columns:
            df['FID'] = df['FID'].apply(clean_fid)
            try:
                df['Data_Temp'] = pd.to_datetime(df['Data'], errors='coerce')
                df['Data'] = df['Data_Temp'].dt.strftime('%Y-%m-%d').fillna(df['Data'])
                df = df.drop(columns=['Data_Temp'])
            except: pass
            st.session_state['historico_full'] = df
            hoje = get_time_br().strftime('%Y-%m-%d')
            st.session_state['historico_sinais'] = df[df['Data'] == hoje].to_dict('records')[::-1]
            if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
            for item in st.session_state['historico_sinais']:
                st.session_state['alertas_enviados'].add(gerar_chave_universal(item['FID'], item['Estrategia'], "SINAL"))
                if 'GREEN' in str(item['Resultado']): st.session_state['alertas_enviados'].add(gerar_chave_universal(item['FID'], item['Estrategia'], "GREEN"))
                if 'RED' in str(item['Resultado']): st.session_state['alertas_enviados'].add(gerar_chave_universal(item['FID'], item['Estrategia'], "RED"))
        else:
            if 'historico_full' not in st.session_state:
                st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST); st.session_state['historico_sinais'] = []
    if not st.session_state.get('jogos_salvos_bigdata_carregados', False) or force:
        st.session_state['jogos_salvos_bigdata_carregados'] = True
    st.session_state['last_db_update'] = now

def adicionar_historico(item):
    if 'historico_full' not in st.session_state: st.session_state['historico_full'] = carregar_aba("Historico", COLS_HIST)
    df_memoria = st.session_state['historico_full']
    try:
        banca_atual_local = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000.0)))
        modo_local = st.session_state.get('modo_gestao_banca', 'fracionario')
        prob_str = str(item.get('Probabilidade', '70%')).replace('%','').strip()
        prob_val = float(prob_str) if prob_str and prob_str.replace('.','',1).isdigit() else 70.0
        odd_str = str(item.get('Odd', '1.50')).replace(',', '.').strip()
        odd_val_item = float(odd_str) if odd_str and odd_str.replace('.','',1).isdigit() else 1.50
        stake_calc = calcular_stake_recomendado(banca_atual_local, prob_val, odd_val_item, modo_local)
        item['Stake_Recomendado_Pct'] = f"{stake_calc['porcentagem']}%"
        item['Stake_Recomendado_RS'] = f"R$ {stake_calc['valor']:.2f}"
        item['Modo_Gestao'] = str(modo_local)
    except:
        item['Stake_Recomendado_Pct'] = item.get('Stake_Recomendado_Pct', '')
        item['Stake_Recomendado_RS'] = item.get('Stake_Recomendado_RS', '')
        item['Modo_Gestao'] = item.get('Modo_Gestao', '')
    df_novo = pd.DataFrame([item])
    df_final = pd.concat([df_novo, df_memoria], ignore_index=True)
    st.session_state['historico_full'] = df_final
    st.session_state['historico_sinais'].insert(0, item)
    st.session_state['precisa_salvar'] = True
    return True

def atualizar_historico_ram(lista_atualizada_hoje):
    if 'historico_full' not in st.session_state: return
    df_memoria = st.session_state['historico_full']
    df_hoje_updates = pd.DataFrame(lista_atualizada_hoje)
    if df_hoje_updates.empty or df_memoria.empty: return
    mapa_atualizacao = {}
    for _, row in df_hoje_updates.iterrows(): mapa_atualizacao[f"{row['FID']}_{row['Estrategia']}"] = row
    def atualizar_linha(row):
        chave = f"{row['FID']}_{row['Estrategia']}"
        if chave in mapa_atualizacao:
            nova_linha = mapa_atualizacao[chave]
            if str(row['Resultado']) != str(nova_linha['Resultado']): st.session_state['precisa_salvar'] = True
            return nova_linha
        return row
    st.session_state['historico_full'] = df_memoria.apply(atualizar_linha, axis=1)

# ==============================================================================
# BIG DATA E FUNÇÕES DE DADOS
# ==============================================================================
def consultar_bigdata_cenario_completo(home_id, away_id):
    if not db_firestore: return "Big Data Offline"
    try:
        docs_h = db_firestore.collection("BigData_Futebol").where("home_id", "==", str(home_id)).order_by("data_hora", direction=firestore.Query.DESCENDING).limit(50).stream()
        docs_a = db_firestore.collection("BigData_Futebol").where("away_id", "==", str(away_id)).order_by("data_hora", direction=firestore.Query.DESCENDING).limit(50).stream()
        h_placares = []; h_cantos = []
        for d in docs_h:
            dd = d.to_dict(); st_d = dd.get('estatisticas', {})
            h_placares.append(dd.get('placar_final', '?'))
            try: h_cantos.append(int(float(st_d.get('escanteios_casa', 0))))
            except: h_cantos.append(0)
        a_placares = []; a_cantos = []
        for d in docs_a:
            dd = d.to_dict(); st_d = dd.get('estatisticas', {})
            a_placares.append(dd.get('placar_final', '?'))
            try: a_cantos.append(int(float(st_d.get('escanteios_fora', 0))))
            except: a_cantos.append(0)
        if not h_placares and not a_placares: return "Sem dados suficientes."
        txt_h = f"MANDANTE (Últimos {len(h_placares)}j): Placares {h_placares} | Cantos {h_cantos}"
        txt_a = f"VISITANTE (Últimos {len(a_placares)}j): Placares {a_placares} | Cantos {a_cantos}"
        return f"{txt_h} || {txt_a}"
    except Exception as e: return f"Erro BD: {str(e)}"

def salvar_bigdata(jogo_api, stats):
    if not db_firestore: return
    try:
        fid = str(jogo_api['fixture']['id'])
        if fid in st.session_state['jogos_salvos_bigdata']: return
        s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        def sanitize(val): return str(val) if val is not None else "0"
        rate_h = 0; rate_a = 0
        item_bigdata = {
            'fid': fid, 'data_hora': get_time_br().strftime('%Y-%m-%d %H:%M'),
            'liga': sanitize(jogo_api['league']['name']), 'home_id': str(jogo_api['teams']['home']['id']), 'away_id': str(jogo_api['teams']['away']['id']),
            'jogo': f"{sanitize(jogo_api['teams']['home']['name'])} x {sanitize(jogo_api['teams']['away']['name'])}",
            'placar_final': f"{jogo_api['goals']['home']}x{jogo_api['goals']['away']}",
            'rating_home': str(rate_h), 'rating_away': str(rate_a),
            'estatisticas': {
                'chutes_total': gv(s1, 'Total Shots') + gv(s2, 'Total Shots'),
                'chutes_gol': gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal'),
                'chutes_area': gv(s1, 'Shots insidebox') + gv(s2, 'Shots insidebox'),
                'escanteios_total': gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks'),
                'escanteios_casa': gv(s1, 'Corner Kicks'), 'escanteios_fora': gv(s2, 'Corner Kicks'),
                'faltas_total': gv(s1, 'Fouls') + gv(s2, 'Fouls'),
                'cartoes_amarelos': gv(s1, 'Yellow Cards') + gv(s2, 'Yellow Cards'),
                'cartoes_vermelhos': gv(s1, 'Red Cards') + gv(s2, 'Red Cards'),
                'posse_casa': str(gv(s1, 'Ball Possession')),
                'ataques_perigosos': gv(s1, 'Dangerous Attacks') + gv(s2, 'Dangerous Attacks'),
            }
        }
        db_firestore.collection("BigData_Futebol").document(fid).set(item_bigdata)
        st.session_state['jogos_salvos_bigdata'].add(fid)
    except: pass

def extrair_dados_completos(stats_api):
    if not stats_api: return "Dados indisponíveis."
    try:
        s1 = stats_api[0]['statistics']; s2 = stats_api[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        return f"📊 STATS: Posse {gv(s1,'Ball Possession')}x{gv(s2,'Ball Possession')} | Chutes {gv(s1,'Total Shots')}x{gv(s2,'Total Shots')} | Cantos {gv(s1,'Corner Kicks')}x{gv(s2,'Corner Kicks')}"
    except: return "Erro stats."

@st.cache_data(ttl=3600)
def buscar_media_gols_ultimos_jogos(api_key, home_id, away_id):
    try:
        def get_avg_goals(team_id, location_filter):
            url = "https://v3.football.api-sports.io/fixtures"
            res = requests.get(url, headers={"x-apisports-key": api_key}, params={"team": team_id, "last": "20", "status": "FT"}).json()
            jogos = res.get('response', []); gols_marcados = 0; jogos_contados = 0
            for j in jogos:
                is_home_match = (j['teams']['home']['id'] == team_id)
                if location_filter == 'home' and is_home_match: gols_marcados += (j['goals']['home'] or 0); jogos_contados += 1
                elif location_filter == 'away' and not is_home_match: gols_marcados += (j['goals']['away'] or 0); jogos_contados += 1
                if jogos_contados >= 10: break
            return "{:.2f}".format(gols_marcados / jogos_contados) if jogos_contados > 0 else "0.00"
        return {'home': get_avg_goals(home_id, 'home'), 'away': get_avg_goals(away_id, 'away')}
    except: return {'home': '?', 'away': '?'}

@st.cache_data(ttl=86400)
def analisar_tendencia_50_jogos(api_key, home_id, away_id):
    try:
        def get_stats_50(team_id):
            url = "https://v3.football.api-sports.io/fixtures"
            res = requests.get(url, headers={"x-apisports-key": api_key}, params={"team": team_id, "last": "50", "status": "FT"}).json()
            jogos = res.get('response', [])
            if not jogos: return {"qtd": 0, "win": 0, "draw": 0, "loss": 0, "over25": 0, "btts": 0}
            stats = {"qtd": len(jogos), "win": 0, "draw": 0, "loss": 0, "over25": 0, "btts": 0}
            for j in jogos:
                gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
                if (gh + ga) > 2: stats["over25"] += 1
                if gh > 0 and ga > 0: stats["btts"] += 1
                is_home = (j['teams']['home']['id'] == team_id)
                if is_home:
                    if gh > ga: stats["win"] += 1
                    elif gh == ga: stats["draw"] += 1
                    else: stats["loss"] += 1
                else:
                    if ga > gh: stats["win"] += 1
                    elif ga == gh: stats["draw"] += 1
                    else: stats["loss"] += 1
            total = stats["qtd"]
            return {"win": int((stats["win"]/total)*100), "draw": int((stats["draw"]/total)*100), "loss": int((stats["loss"]/total)*100), "over25": int((stats["over25"]/total)*100), "btts": int((stats["btts"]/total)*100), "qtd": total}
        return {"home": get_stats_50(home_id), "away": get_stats_50(away_id)}
    except: return None

@st.cache_data(ttl=120)
def analisar_tendencia_macro_micro(api_key, home_id, away_id):
    try:
        def get_team_stats_unified(team_id):
            url = "https://v3.football.api-sports.io/fixtures"
            res = requests.get(url, headers={"x-apisports-key": api_key}, params={"team": team_id, "last": "10", "status": "FT"}).json()
            jogos = res.get('response', [])
            if not jogos: return "Sem dados.", 0, 0, 0
            over_gols_count = 0; sequencia = []; total_gols_marcados = 0
            for j in jogos[:5]:
                goals_home = j['goals']['home']; goals_away = j['goals']['away']
                if j['teams']['home']['id'] == team_id:
                    gols_time = goals_home; gols_adv = goals_away
                else:
                    gols_time = goals_away; gols_adv = goals_home
                if gols_time > gols_adv: sequencia.append('V')
                elif gols_time == gols_adv: sequencia.append('E')
                else: sequencia.append('D')
                total_gols_marcados += gols_time
                if (goals_home + goals_away) >= 2: over_gols_count += 1
            seq_str = ' '.join(sequencia)
            media_gols = total_gols_marcados / 5 if len(jogos) >= 5 else 0
            resumo_txt = f"{seq_str} | Média {media_gols:.1f} gols/jogo"
            pct_over_recent = int((over_gols_count / min(len(jogos), 5)) * 100)
            total_amarelos = 0; total_vermelhos = 0
            # Cartões simplificados (sem chamada extra de API por jogo)
            media_cards = 0; total_vermelhos = 0
            return resumo_txt, pct_over_recent, media_cards, total_vermelhos
        h_txt, h_pct, h_med_cards, h_reds = get_team_stats_unified(home_id)
        a_txt, a_pct, a_med_cards, a_reds = get_team_stats_unified(away_id)
        return {
            "home": {"resumo": h_txt, "micro": h_pct, "avg_cards": h_med_cards, "reds": h_reds},
            "away": {"resumo": a_txt, "micro": a_pct, "avg_cards": a_med_cards, "reds": a_reds}
        }
    except: return None

@st.cache_data(ttl=120)
def buscar_agenda_cached(api_key, date_str):
    try:
        return requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"date": date_str, "timezone": "America/Sao_Paulo"}).json().get('response', [])
    except: return []

def carregar_contexto_global_firebase():
    if not db_firestore: return "Firebase Offline."
    try:
        docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(20000).stream()
        stats_gerais = {"total": 0, "over05": 0, "gols_total": 0}
        for d in docs:
            dd = d.to_dict(); stats_gerais["total"] += 1; placar = dd.get('placar_final', '0x0')
            try:
                gh, ga = map(int, placar.split('x'))
                if (gh + ga) > 0: stats_gerais["over05"] += 1
                stats_gerais["gols_total"] += (gh + ga)
            except: pass
        if stats_gerais["total"] == 0: return "Sem dados no Firebase."
        return f"BIG DATA ({stats_gerais['total']} jogos): Média {stats_gerais['gols_total']/stats_gerais['total']:.2f} gols | Over 0.5: {(stats_gerais['over05']/stats_gerais['total'])*100:.1f}%."
    except Exception as e: return f"Erro Firebase: {e}"

def filtrar_multiplas_nao_correlacionadas(itens, janela_min=90):
    try:
        meta_global = st.session_state.get('multipla_meta', {}) or {}
        selecionados = []
        for it in (itens or []):
            fid = str(it.get('fid', '')); lid = it.get('league_id'); ko = it.get('kickoff')
            if (lid is None or ko is None) and fid in meta_global:
                mg = meta_global.get(fid, {}); lid = lid if lid else mg.get('league_id'); ko = ko if ko else mg.get('kickoff')
            correlacionado = False
            for s in selecionados:
                fid_s = str(s.get('fid', '')); lid_s = s.get('league_id'); ko_s = s.get('kickoff')
                if (lid_s is None or ko_s is None) and fid_s in meta_global:
                    ms = meta_global.get(fid_s, {}); lid_s = lid_s if lid_s else ms.get('league_id'); ko_s = ko_s if ko_s else ms.get('kickoff')
                if lid and lid_s and str(lid) == str(lid_s): correlacionado = True; break
                try:
                    if ko and ko_s and abs((ko - ko_s).total_seconds()) <= janela_min * 60: correlacionado = True; break
                except: pass
            if not correlacionado: selecionados.append(it)
        return selecionados
    except: return itens

# ==============================================================================
# MATINAL + MÚLTIPLAS + ALTERNATIVAS (HORÁRIO CORRIGIDO ATÉ 12H)
# ==============================================================================
def gerar_multipla_matinal_ia(api_key):
    if not IA_ATIVADA: return None, []
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"date": hoje, "timezone": "America/Sao_Paulo"}).json()
        jogos = [j for j in res.get('response', []) if j['fixture']['status']['short'] == 'NS']
        if len(jogos) < 2: return None, []
        lista_jogos_txt = ""; mapa_jogos = {}; meta_local = {}; count_validos = 0
        random.shuffle(jogos)
        for j in jogos:
            if count_validos >= 30: break
            fid = j['fixture']['id']
            odd_val, odd_nome = buscar_odd_pre_match(api_key, fid)
            if odd_val == 0: continue
            home = j['teams']['home']['name']; away = j['teams']['away']['name']
            stats = analisar_tendencia_macro_micro(api_key, j['teams']['home']['id'], j['teams']['away']['id'])
            if stats and stats['home']['micro'] > 0:
                if stats['home']['micro'] < 40 and stats['away']['micro'] < 40: continue
                h_mic = stats['home']['micro']; a_mic = stats['away']['micro']
                mapa_jogos[fid] = f"{home} x {away}"
                try:
                    kickoff = datetime.fromisoformat(j['fixture']['date'].replace('Z', '+00:00'))
                    meta_local[str(fid)] = {'league_id': j['league'].get('id'), 'kickoff': kickoff}
                except: pass
                lista_jogos_txt += f"\n- ID {fid}: {home} x {away} ({j['league']['name']})\n  Odd: {odd_val} ({odd_nome})\n  Casa: {h_mic}% Over | Fora: {a_mic}% Over\n"
                count_validos += 1
        if not lista_jogos_txt: return None, []
        prompt = f"""GESTOR DE RISCO. Crie DUPLA ou TRIPLA segura (@1.80-@2.50 combinadas).
DADOS: {lista_jogos_txt}
CRITÉRIOS: Ambos >= 60% recente. Ligas diferentes. Mercados: Over 0.5 HT ou Over 1.5 FT.
SAÍDA JSON: {{ "jogos": [ {{"fid": 123, "jogo": "A x B", "motivo": "...", "recente": 60}} ], "probabilidade_combinada": "90" }}"""
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        st.session_state['gemini_usage']['used'] += 1
        st.session_state['multipla_meta'] = meta_local
        return json.loads(response.text), mapa_jogos
    except: return None, []

def gerar_insights_matinais_ia(api_key):
    if not IA_ATIVADA: return "IA Offline.", {}
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"date": hoje, "timezone": "America/Sao_Paulo"}).json()
        jogos_candidatos = [j for j in res.get('response', []) if j['fixture']['status']['short'] == 'NS']
        if not jogos_candidatos: return "Sem jogos para analisar hoje.", {}
        lista_para_ia = ""; mapa_jogos = {}; count = 0
        random.shuffle(jogos_candidatos)
        for j in jogos_candidatos:
            if count >= 80: break
            fid = j['fixture']['id']; home_id = j['teams']['home']['id']; away_id = j['teams']['away']['id']
            home = j['teams']['home']['name']; away = j['teams']['away']['name']
            mapa_jogos[f"{home} x {away}"] = {'fid': str(fid), 'home_id': home_id, 'away_id': away_id}
            odd_val, odd_nome = buscar_odd_pre_match(api_key, fid)
            if odd_val == 0: continue
            macro = analisar_tendencia_50_jogos(api_key, home_id, away_id)
            micro = analisar_tendencia_macro_micro(api_key, home_id, away_id)
            if macro and micro:
                h_50 = macro['home']; a_50 = macro['away']
                lista_para_ia += f"""
---
⚽ Jogo: {home} x {away} ({j['league']['name']})
💰 Ref (Over 2.5): @{odd_val:.2f}
📅 LONGO PRAZO (50J): Casa {h_50['win']}%V | {h_50['over25']}% Over 2.5 | Fora {a_50['win']}%V | {a_50['over25']}% Over 2.5
🔥 FASE ATUAL (10J): Casa {micro['home']['resumo']} | Fora {micro['away']['resumo']}
"""
                count += 1
        if not lista_para_ia: return "Nenhum jogo com dados suficientes hoje.", {}
        prompt = f"""CIENTISTA DE DADOS E TRADER ESPORTIVO (PERFIL SNIPER).
Dados de 50 JOGOS + 10 JOGOS. Cruze para encontrar valor.
DADOS: {lista_para_ia}
FILTRO DE ELITE: Falso Favorito (win<40% em 50j), Vitória Magra (1x0), Arame Liso (empates), Instabilidade (V-D-V-D).
MISSÃO: Preencher 6 zonas. Mínimo 5 jogos diferentes.
SAÍDA: 🔥 ZONA DE GOLS (OVER) | ❄️ ZONA DE TRINCHEIRA (UNDER) | 🏆 ZONA DE MATCH ODDS | 🟨 ZONA DE CARTÕES | 🏴 ZONA DE ESCANTEIOS | 🧤 ZONA DE DEFESAS DO GOLEIRO
Para cada: ⚽ Jogo: [Casa] x [Fora] | 🏆 Liga: [nome] | 🎯 Palpite: [indicação] | 💰 Ref: @[odd] | 📝 Motivo: [dados 50j]"""
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(temperature=0.0))
        st.session_state['gemini_usage']['used'] += 1
        texto_ia = response.text
        texto_ia = re.sub(r'\*\*([^\*]+)\*\*', r'<b>\1</b>', texto_ia)
        texto_ia = re.sub(r'^\s*\*\s+', '├─ ', texto_ia, flags=re.MULTILINE)
        return texto_ia, mapa_jogos
    except Exception as e: return f"Erro na análise: {str(e)}", {}

def gerar_analise_mercados_alternativos_ia(api_key):
    if not IA_ATIVADA: return []
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"date": hoje, "timezone": "America/Sao_Paulo"}).json()
        jogos_candidatos = [j for j in res.get('response', []) if j['fixture']['status']['short'] == 'NS']
        if not jogos_candidatos: return []
        random.shuffle(jogos_candidatos); dados_analise = ""; count_validos = 0
        for j in jogos_candidatos:
            if count_validos >= 30: break
            home = j['teams']['home']['name']; away = j['teams']['away']['name']; fid = j['fixture']['id']
            stats = analisar_tendencia_macro_micro(api_key, j['teams']['home']['id'], j['teams']['away']['id'])
            media_cartoes_total = (stats['home']['avg_cards'] + stats['away']['avg_cards']) if stats else 0
            if media_cartoes_total > 0:
                dados_analise += f"\n- Jogo: {home} x {away} ({j['league']['name']})\n  FID: {fid} | Média Cartões: {media_cartoes_total:.1f}\n"
                count_validos += 1
        if not dados_analise: return []
        prompt = f"""ESPECIALISTA EM MERCADOS ALTERNATIVOS. Encontre 3 oportunidades de CARTÕES (Over e Under).
DADOS: {dados_analise}
SAÍDA JSON: {{ "sinais": [ {{ "fid": "...", "tipo": "CARTAO", "titulo": "🟨 AÇOUGUEIRO ou 🕊️ JOGO LIMPO", "jogo": "A x B", "destaque": "Motivo.", "indicacao": "Mais/Menos de X.5 Cartões" }} ] }}"""
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        st.session_state['gemini_usage']['used'] += 1
        return json.loads(response.text).get('sinais', [])
    except: return []

def gerar_bet_builder_alavancagem(api_key):
    if not IA_ATIVADA: return []
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"date": hoje, "timezone": "America/Sao_Paulo"}).json()
        LIGAS_ALAVANCAGEM = [39, 140, 78, 135, 61, 71, 72, 2, 3]
        candidatos = [j for j in res.get('response', []) if j['league']['id'] in LIGAS_ALAVANCAGEM and j['fixture']['status']['short'] == 'NS']
        if not candidatos: return []
        lista_provaveis = []
        for j in candidatos[:10]:
            try:
                dados_bd = consultar_bigdata_cenario_completo(j['teams']['home']['id'], j['teams']['away']['id'])
                lista_provaveis.append({"jogo": j, "bigdata": dados_bd, "referee": j['fixture'].get('referee', 'Desc.')})
            except: pass
        resultados_finais = []
        for pick in lista_provaveis[:3]:
            j = pick['jogo']; home = j['teams']['home']['name']; away = j['teams']['away']['name']
            prompt = f"""ANALISTA ALAVANCAGEM. BET BUILDER para {home} x {away} ({j['league']['name']}).
BIG DATA: {pick['bigdata']} | JUIZ: {pick['referee']}
Odd combinada >= @3.50. SAÍDA JSON: {{ "titulo": "🚀 ALAVANCAGEM", "selecoes": ["..."], "analise_ia": "...", "confianca": "Alta" }}"""
            try:
                response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
                st.session_state['gemini_usage']['used'] += 1
                r_json = json.loads(response.text); r_json['fid'] = j['fixture']['id']; r_json['jogo'] = f"{home} x {away}"
                resultados_finais.append(r_json)
            except: pass
        return resultados_finais
    except: return []

# ==============================================================================
# TELEGRAM - ENVIO DE MENSAGENS (FORMATO COMPACTO FUSÃO)
# ==============================================================================
def enviar_telegram(token, chat_id, msg, reply_markup=None, parse_mode='HTML'):
    if not token or not chat_id: return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": msg, "parse_mode": parse_mode, "disable_web_page_preview": True}
        if reply_markup: payload["reply_markup"] = json.dumps(reply_markup)
        res = requests.post(url, json=payload, timeout=10)
        return res.status_code == 200
    except: return False

def enviar_telegram_long(token, chat_id, msg, parse_mode='HTML'):
    if not token or not chat_id: return False
    if len(msg) <= 4000: return enviar_telegram(token, chat_id, msg, parse_mode=parse_mode)
    partes = [msg[i:i+4000] for i in range(0, len(msg), 4000)]
    for parte in partes: enviar_telegram(token, chat_id, parte, parse_mode=parse_mode)
    return True

def formatar_mensagem_compacta(estrategia, jogo_nome, liga, minuto, placar, odd_val, opiniao_ia, probabilidade, stats_resumo="", odd_info=None, stake_info=None, exit_tip=None):
    """[FUSÃO] Formato compacto ~13 linhas com TODA info essencial"""
    tipo_info = obter_descricao_aposta(estrategia)
    tipo_emoji = "⚽" if tipo_info['tipo'] == 'OVER' else ("❄️" if tipo_info['tipo'] == 'UNDER' else "🏆")
    
    # Semáforo de odd (INFORMATIVO, nunca bloqueia)
    odd_min = obter_odd_minima(estrategia)
    try: odd_float = float(str(odd_val).replace(',', '.'))
    except: odd_float = 0
    if odd_float >= odd_min * 1.15: semaforo = "✅"
    elif odd_float >= odd_min: semaforo = "⏳"
    else: semaforo = "⛔"
    
    # Movimento de odd
    odd_mov_txt = ""
    if odd_info:
        mov, var = odd_info
        if mov != 'ESTÁVEL':
            emoji_mov = "📈" if 'SUBINDO' in mov else "📉"
            odd_mov_txt = f"\n{emoji_mov} Odd {mov} ({var:+.1f}%)"
    
    # Stake
    stake_txt = ""
    if stake_info:
        stake_txt = f"\n💰 Stake: R$ {stake_info['valor']:.2f} ({stake_info['porcentagem']}%) | Kelly {stake_info['modo'].title()}"
    
    # IA
    ia_txt = ""
    if opiniao_ia and str(opiniao_ia).strip():
        prob_str = str(probabilidade).replace('%','').strip()
        try: prob_num = int(float(prob_str))
        except: prob_num = 0
        if prob_num >= 75: ia_emoji = "✅"
        elif prob_num >= 60: ia_emoji = "⚠️"
        else: ia_emoji = "🔻"
        ia_txt = f"\n🤖 IA: {ia_emoji} {opiniao_ia} ({prob_num}%)"
    
    # Exit tip
    exit_txt = ""
    if exit_tip: exit_txt = f"\n🚪 Saída: {exit_tip}"
    
    msg = f"""{semaforo} SINAL {estrategia} | 🟢 Strat
🏆 {liga}
⚽ {jogo_nome}
⏰ {minuto}' min | 🥅 Placar: {placar}

{tipo_emoji} {tipo_info['tipo']}: {tipo_info['ganha_se']}
🔥 ODD DE VALOR: @{odd_val}
{tipo_info['ordem']}
✅ Aposta: {tipo_info['aposta']}{ia_txt}{odd_mov_txt}{stake_txt}{exit_txt}"""
    
    if stats_resumo: msg += f"\n📊 {stats_resumo}"
    return msg.strip()

def formatar_resultado_compacto(estrategia, jogo_nome, resultado, placar_atual, odd_original):
    tipo_info = obter_descricao_aposta(estrategia)
    if 'GREEN' in resultado:
        try:
            odd_f = float(str(odd_original).replace(',', '.'))
            lucro_pct = (odd_f - 1) * 100
            return f"✅ GREEN {estrategia}\n⚽ {jogo_nome} | {placar_atual}\n💰 Odd @{odd_original} (+{lucro_pct:.0f}%)\n✅ {tipo_info['ganha_se']}"
        except:
            return f"✅ GREEN {estrategia}\n⚽ {jogo_nome} | {placar_atual}\n💰 Odd @{odd_original}"
    else:
        return f"❌ RED {estrategia}\n⚽ {jogo_nome} | {placar_atual}\n💸 {tipo_info['perde_se']}"

# ==============================================================================
# CONSULTA IA GEMINI (FUSÃO: SEM VETO - IA SÓ INFORMA)
# ==============================================================================
def consultar_ia_gemini(estrategia, minuto, placar, stats_txt, odd_val, jogo_nome, liga_nome, tendencia_50=None, tendencia_micro=None, bigdata_txt=""):
    """[FUSÃO] IA retorna opinião + probabilidade mas NUNCA veta/bloqueia envio"""
    if not IA_ATIVADA:
        return "IA Offline", "70%"
    
    if st.session_state.get('ia_bloqueada_ate'):
        if get_time_br() < st.session_state['ia_bloqueada_ate']:
            return "IA em Cooldown", "70%"
        else:
            st.session_state['ia_bloqueada_ate'] = None
    
    try:
        tipo_aposta = classificar_tipo_estrategia(estrategia)
        descr = obter_descricao_aposta(estrategia)
        
        contexto_extra = ""
        if tendencia_50:
            h50 = tendencia_50.get('home', {}); a50 = tendencia_50.get('away', {})
            contexto_extra += f"\n50 JOGOS: Casa {h50.get('win',0)}%V {h50.get('over25',0)}%Over2.5 | Fora {a50.get('win',0)}%V {a50.get('over25',0)}%Over2.5"
        if tendencia_micro:
            hm = tendencia_micro.get('home', {}); am = tendencia_micro.get('away', {})
            contexto_extra += f"\nFASE ATUAL: Casa {hm.get('resumo','?')} | Fora {am.get('resumo','?')}"
        if bigdata_txt and bigdata_txt != "Big Data Offline":
            contexto_extra += f"\nBIG DATA: {bigdata_txt[:500]}"
        
        prompt = f"""ANALISTA DE APOSTAS. Avalie este sinal {tipo_aposta}.

DADOS:
- Estratégia: {estrategia} ({descr['tipo']}: {descr['ganha_se']})
- Jogo: {jogo_nome} ({liga_nome})
- Minuto: {minuto}' | Placar: {placar} | Odd: @{odd_val}
- Stats: {stats_txt}
{contexto_extra}

INSTRUÇÕES:
1. Analise se os dados SUSTENTAM a estratégia
2. Dê probabilidade de 0 a 100
3. NÃO VETE - apenas informe sua opinião
4. Dê uma dica de saída (cashout, segurar, etc)

RESPONDA EXATAMENTE NESTE FORMATO JSON:
{{"opiniao": "Aprovado" ou "Atenção" ou "Risco Alto", "probabilidade": "75", "motivo": "frase curta", "saida": "dica de saída curta"}}"""

        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(
            response_mime_type="application/json", temperature=0.1))
        st.session_state['gemini_usage']['used'] += 1
        
        r = json.loads(response.text)
        opiniao = str(r.get('opiniao', 'Aprovado'))
        prob = str(r.get('probabilidade', '70'))
        saida = str(r.get('saida', ''))
        
        # [FUSÃO] IA NUNCA VETA - transforma qualquer "Vetado" em "Risco Alto"
        if 'vetado' in opiniao.lower() or 'vetar' in opiniao.lower():
            opiniao = "Risco Alto"
        
        # Guarda tip de saída para usar na mensagem
        if saida:
            if 'ia_saida_tips' not in st.session_state: st.session_state['ia_saida_tips'] = {}
            st.session_state['ia_saida_tips'][f"{jogo_nome}_{estrategia}"] = saida
        
        return opiniao, f"{prob}%"
    except Exception as e:
        st.session_state['ia_bloqueada_ate'] = get_time_br() + timedelta(seconds=30)
        return "Erro IA", "70%"

# ==============================================================================
# ESTRATÉGIAS DE DETECÇÃO (ORIGINAL - TODAS FUNCIONAM)
# ==============================================================================
def detectar_estrategias(jogo_api, stats_raw, tabela_liga=None):
    """Detecta TODAS as estratégias aplicáveis - lógica do ORIGINAL preservada"""
    estrategias = []
    if not stats_raw or len(stats_raw) < 2: return estrategias
    
    try:
        s1 = stats_raw[0]['statistics']; s2 = stats_raw[1]['statistics']
        def gv(l, t):
            val = next((x['value'] for x in l if x['type'] == t), 0)
            if val is None: return 0
            if isinstance(val, str): val = val.replace('%', '')
            try: return int(float(val))
            except: return 0
        
        fixture = jogo_api['fixture']; goals = jogo_api['goals']
        minuto = fixture['status'].get('elapsed', 0) or 0
        gh = goals.get('home', 0) or 0; ga = goals.get('away', 0) or 0
        total_gols = gh + ga; placar = f"{gh}x{ga}"
        status_short = fixture['status']['short']
        
        is_1t = status_short in ['1H'] or (minuto <= 45 and status_short not in ['HT', '2H'])
        is_2t = status_short in ['2H'] or minuto > 45
        is_ht = status_short == 'HT'
        
        # Stats
        chutes_total = gv(s1, 'Total Shots') + gv(s2, 'Total Shots')
        chutes_gol = gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal')
        chutes_area = gv(s1, 'Shots insidebox') + gv(s2, 'Shots insidebox')
        cantos = gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks')
        ataques_perig = gv(s1, 'Dangerous Attacks') + gv(s2, 'Dangerous Attacks')
        posse_h = gv(s1, 'Ball Possession'); posse_a = gv(s2, 'Ball Possession')
        faltas = gv(s1, 'Fouls') + gv(s2, 'Fouls')
        amarelos = gv(s1, 'Yellow Cards') + gv(s2, 'Yellow Cards')
        passes_h = gv(s1, 'Total passes'); passes_a = gv(s2, 'Total passes')
        chutes_h = gv(s1, 'Total Shots'); chutes_a = gv(s2, 'Total Shots')
        sog_h = gv(s1, 'Shots on Goal'); sog_a = gv(s2, 'Shots on Goal')
        
        # Home/Away IDs para tabela
        home_id = jogo_api['teams']['home']['id']; away_id = jogo_api['teams']['away']['id']
        home_name = jogo_api['teams']['home']['name']; away_name = jogo_api['teams']['away']['name']
        
        # Helper: posição na tabela
        def get_posicao(team_id, tabela):
            if not tabela: return 99
            for standing in tabela:
                if isinstance(standing, list):
                    for row in standing:
                        if row.get('team', {}).get('id') == team_id: return row.get('rank', 99)
                elif isinstance(standing, dict):
                    if standing.get('team', {}).get('id') == team_id: return standing.get('rank', 99)
            return 99
        
        pos_home = get_posicao(home_id, tabela_liga)
        pos_away = get_posicao(away_id, tabela_liga)
        
        stats_txt = f"Chutes {chutes_total}({chutes_gol} gol) | Cantos {cantos} | Atq.Perig {ataques_perig} | Posse {posse_h}x{posse_a}"
        
        # ==========================================
        # PORTEIRA ABERTA
        # ==========================================
        if is_1t and 8 <= minuto <= 30 and total_gols == 0:
            if chutes_total >= 8 and chutes_gol >= 3 and ataques_perig >= 40:
                estrategias.append({"nome": "🟣 Porteira Aberta", "stats": stats_txt, "motivo": f"Pressão intensa: {chutes_total} chutes, {chutes_gol} no gol, {ataques_perig} atq perig"})
        
        # ==========================================
        # GOL RELÂMPAGO
        # ==========================================
        if is_1t and 5 <= minuto <= 20 and total_gols == 0:
            if chutes_total >= 6 and chutes_gol >= 2 and cantos >= 2:
                estrategias.append({"nome": "⚡ Gol Relâmpago", "stats": stats_txt, "motivo": f"Início explosivo: {chutes_total} chutes em {minuto}'"})
        
        # ==========================================
        # JANELA DE OURO
        # ==========================================
        if is_1t and 25 <= minuto <= 40 and total_gols == 0:
            if chutes_total >= 10 and chutes_gol >= 4 and ataques_perig >= 50:
                estrategias.append({"nome": "💰 Janela de Ouro", "stats": stats_txt, "motivo": f"Janela pré-gol: {chutes_total} chutes, {ataques_perig} atq perigosos"})
        
        # ==========================================
        # BLITZ CASA / VISITANTE
        # ==========================================
        if is_1t and 10 <= minuto <= 35 and total_gols == 0:
            if chutes_h >= 5 and sog_h >= 2 and posse_h >= 60:
                cantos_h = gv(s1, 'Corner Kicks')
                if cantos_h >= 2 or ataques_perig >= 40:
                    estrategias.append({"nome": "🟢 Blitz Casa", "stats": stats_txt, "motivo": f"Domínio casa: {chutes_h} chutes, {posse_h}% posse"})
            if chutes_a >= 5 and sog_a >= 2 and posse_a >= 60:
                cantos_a = gv(s2, 'Corner Kicks')
                if cantos_a >= 2 or ataques_perig >= 40:
                    estrategias.append({"nome": "🟢 Blitz Visitante", "stats": stats_txt, "motivo": f"Domínio visitante: {chutes_a} chutes, {posse_a}% posse"})
        
        # ==========================================
        # MASSACRE
        # ==========================================
        if is_1t and 10 <= minuto <= 30 and total_gols >= 1:
            if chutes_total >= 10 and chutes_gol >= 5:
                estrategias.append({"nome": "🔥 Massacre", "stats": stats_txt, "motivo": f"Jogo aberto: {total_gols} gols, {chutes_gol} no gol"})
        
        # ==========================================
        # CHOQUE DE LÍDERES
        # ==========================================
        if is_1t and 15 <= minuto <= 35 and total_gols == 0:
            if pos_home <= 5 and pos_away <= 5:
                if chutes_total >= 7 and ataques_perig >= 45:
                    estrategias.append({"nome": "⚔️ Choque Líderes", "stats": stats_txt, "motivo": f"Top5 vs Top5: #{pos_home} x #{pos_away}"})
        
        # ==========================================
        # BRIGA DE RUA
        # ==========================================
        if is_1t and 15 <= minuto <= 35 and total_gols == 0:
            if faltas >= 10 and chutes_total >= 8 and ataques_perig >= 40:
                estrategias.append({"nome": "🥊 Briga de Rua", "stats": stats_txt, "motivo": f"Jogo quente: {faltas} faltas, {chutes_total} chutes"})
        
        # ==========================================
        # GOLDEN BET (acumulativo - precisa de MUITA pressão)
        # ==========================================
        if 25 <= minuto <= 55 and total_gols == 0:
            if chutes_total >= 14 and chutes_gol >= 5 and cantos >= 5 and ataques_perig >= 60:
                estrategias.append({"nome": "💎 GOLDEN BET", "stats": stats_txt, "motivo": f"Pressão extrema 0x0: {chutes_total} chutes, {cantos} cantos, {ataques_perig} atq"})
        
        # ==========================================
        # TIROTEIO ELITE
        # ==========================================
        if 15 <= minuto <= 45 and total_gols >= 1:
            if chutes_total >= 12 and chutes_gol >= 6 and ataques_perig >= 55:
                estrategias.append({"nome": "🏹 Tiroteio Elite", "stats": stats_txt, "motivo": f"Tiroteio: {chutes_gol} no gol de {chutes_total} chutes"})
        
        # ==========================================
        # CONTRA-ATAQUE LETAL
        # ==========================================
        if is_2t and 55 <= minuto <= 75:
            time_perdendo = None
            if gh > ga and (gh - ga) == 1: time_perdendo = away_name
            elif ga > gh and (ga - gh) == 1: time_perdendo = home_name
            if time_perdendo:
                if chutes_total >= 10 and ataques_perig >= 50:
                    estrategias.append({"nome": "⚡ Contra-Ataque Letal", "stats": stats_txt, "motivo": f"{time_perdendo} precisa empatar. Jogo aberto."})
        
        # ==========================================
        # SNIPER FINAL
        # ==========================================
        if is_2t and minuto >= 70 and total_gols == 0:
            if chutes_total >= 16 and chutes_gol >= 6 and ataques_perig >= 60:
                estrategias.append({"nome": "💎 Sniper Final", "stats": stats_txt, "motivo": f"0x0 aos {minuto}' com {chutes_total} chutes. Gol iminente."})
        
        # ==========================================
        # BACK FAVORITO (NETTUNO)
        # ==========================================
        if is_2t and 50 <= minuto <= 70:
            if (gh - ga) >= 2:
                if pos_home <= 6:
                    estrategias.append({"nome": "🦁 Back Favorito (Nettuno)", "stats": stats_txt, "motivo": f"Casa #{pos_home} vencendo {placar}. Manter."})
            elif (ga - gh) >= 2:
                if pos_away <= 6:
                    estrategias.append({"nome": "🦁 Back Favorito (Nettuno)", "stats": stats_txt, "motivo": f"Visitante #{pos_away} vencendo {placar}. Manter."})
        
        # ==========================================
        # LAY GOLEADA
        # ==========================================
        if is_2t and minuto >= 60:
            if abs(gh - ga) >= 3:
                if chutes_total <= 8 and ataques_perig <= 30:
                    estrategias.append({"nome": "🔫 Lay Goleada", "stats": stats_txt, "motivo": f"Placar {placar}: gol da honra provável com pouca ação"})
        
        # ==========================================
        # ESTRATÉGIA DO VOVÔ
        # ==========================================
        if is_2t and minuto >= 75:
            if abs(gh - ga) >= 1:
                if chutes_total >= 5:
                    quem_ganha = home_name if gh > ga else away_name
                    estrategias.append({"nome": "👴 Estratégia do Vovô", "stats": stats_txt, "motivo": f"{quem_ganha} ganhando {placar} aos {minuto}'. Seguro."})
        
        # ==========================================
        # JOGO MORNO (UNDER)
        # ==========================================
        if is_2t and minuto >= 60 and total_gols == 0:
            if chutes_total <= 12 and chutes_gol <= 4 and ataques_perig <= 40:
                estrategias.append({"nome": "❄️ Jogo Morno", "stats": stats_txt, "motivo": f"0x0 aos {minuto}' com baixa ação: {chutes_total} chutes, {chutes_gol} no gol"})
        
        # ==========================================
        # ARAME LISO (UNDER)
        # ==========================================
        if is_2t and 55 <= minuto <= 75 and total_gols <= 1:
            if chutes_gol <= 3 and ataques_perig <= 35:
                estrategias.append({"nome": "🧊 Arame Liso", "stats": stats_txt, "motivo": f"Jogo travado {placar} aos {minuto}'. Pressão baixa."})
        
        # ==========================================
        # SNIPER DE CARTÕES
        # ==========================================
        if 30 <= minuto <= 70:
            if amarelos >= 3 and faltas >= 15:
                estrategias.append({"nome": "🟨 Sniper de Cartões", "stats": stats_txt, "motivo": f"Jogo quente: {amarelos} amarelos, {faltas} faltas"})
        
        # ==========================================
        # MURALHA (DEFESAS)
        # ==========================================
        if 25 <= minuto <= 60:
            saves_h = gv(s1, 'Goalkeeper Saves'); saves_a = gv(s2, 'Goalkeeper Saves')
            if (saves_h + saves_a) >= 6 and chutes_gol >= 8:
                estrategias.append({"nome": "🧤 Muralha (Defesas)", "stats": stats_txt + f" | Defesas {saves_h}x{saves_a}", "motivo": f"Goleiros em dia: {saves_h+saves_a} defesas de {chutes_gol} no gol"})
        
    except Exception as e:
        pass
    
    return estrategias

# ==============================================================================
# [FUSÃO FIX CRÍTICO] DOWNLOAD DE STATS - SEMPRE ATUALIZA A CADA CICLO
# ==============================================================================
def baixar_stats_jogo(api_key, fixture_id):
    """Baixa stats FRESCAS a cada ciclo - NÃO usa cache permanente"""
    try:
        url = "https://v3.football.api-sports.io/fixtures/statistics"
        res = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fixture_id})
        update_api_usage(res.headers)
        data = res.json()
        stats = data.get('response', [])
        if stats and len(stats) >= 2:
            return stats
        return None
    except:
        return None

def baixar_odds_live(api_key, fixture_id):
    try:
        url = "https://v3.football.api-sports.io/odds/live"
        res = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fixture_id})
        update_api_usage(res.headers)
        data = res.json()
        odds_response = data.get('response', [])
        if not odds_response: return None
        return odds_response[0] if odds_response else None
    except: return None

def extrair_odd_over05(odds_data):
    """Extrai odd de Over 0.5 Gols dos dados live"""
    if not odds_data: return 0
    try:
        bookmakers = odds_data.get('odds', [])
        for bk in bookmakers:
            if bk.get('id') == 8:  # Bet365
                for bet in bk.get('values', []):
                    if 'Over' in str(bet.get('value', '')) and '0.5' in str(bet.get('handicap', '')):
                        return float(bet.get('odd', 0))
                # Fallback: primeiro Over
                for bet in bk.get('values', []):
                    if 'Over' in str(bet.get('value', '')):
                        return float(bet.get('odd', 0))
        # Fallback: qualquer bookmaker
        for bk in bookmakers:
            for bet in bk.get('values', []):
                if 'Over' in str(bet.get('value', '')) and '0.5' in str(bet.get('handicap', '')):
                    return float(bet.get('odd', 0))
        return 0
    except: return 0

def buscar_tabela_liga_cached(api_key, league_id, season=None):
    cache_key = f"tabela_{league_id}"
    if cache_key in st.session_state:
        cached = st.session_state[cache_key]
        if time.time() - cached.get('ts', 0) < 3600:
            return cached.get('data')
    try:
        if season is None: season = get_time_br().year
        url = "https://v3.football.api-sports.io/standings"
        res = requests.get(url, headers={"x-apisports-key": api_key}, params={"league": league_id, "season": season})
        update_api_usage(res.headers)
        data = res.json()
        standings = data.get('response', [])
        if standings:
            tabela = standings[0].get('league', {}).get('standings', [])
            st.session_state[cache_key] = {'data': tabela, 'ts': time.time()}
            return tabela
        return None
    except: return None

# ==============================================================================
# CHECK GREEN/RED (FUSÃO: USA classificar_tipo_estrategia)
# ==============================================================================
def check_green_red_hibrido(sinais_hoje, jogos_live_map):
    """[FUSÃO] Apuração corrigida usando tipo de estratégia"""
    alterou = False
    for sinal in sinais_hoje:
        if 'GREEN' in str(sinal.get('Resultado', '')) or 'RED' in str(sinal.get('Resultado', '')):
            continue  # Já apurado
        
        fid = clean_fid(sinal.get('FID', '0'))
        if fid == '0': continue
        
        jogo_live = jogos_live_map.get(fid)
        if not jogo_live: continue
        
        status_short = jogo_live['fixture']['status']['short']
        if status_short not in ['FT', 'AET', 'PEN', '2H', 'HT']:
            continue
        
        gh_agora = jogo_live['goals'].get('home', 0) or 0
        ga_agora = jogo_live['goals'].get('away', 0) or 0
        total_gols_agora = gh_agora + ga_agora
        placar_agora = f"{gh_agora}x{ga_agora}"
        
        # Placar no momento do sinal
        placar_sinal = str(sinal.get('Placar_Sinal', '0x0'))
        try:
            gs_h, gs_a = map(int, placar_sinal.lower().replace(' ', '').split('x'))
            gols_no_sinal = gs_h + gs_a
        except:
            gols_no_sinal = 0
        
        estrategia = str(sinal.get('Estrategia', ''))
        tipo = classificar_tipo_estrategia(estrategia)
        
        # Só apura em FT, AET, PEN (jogo acabou) ou 2H com critério suficiente
        jogo_acabou = status_short in ['FT', 'AET', 'PEN']
        
        resultado = None
        
        if tipo == 'OVER':
            # GREEN se saiu mais gols depois do sinal
            if total_gols_agora > gols_no_sinal:
                resultado = 'GREEN'
            elif jogo_acabou:
                resultado = 'RED'
        
        elif tipo == 'UNDER':
            # GREEN se NÃO saiu gol depois do sinal
            if jogo_acabou:
                if total_gols_agora == gols_no_sinal:
                    resultado = 'GREEN'
                else:
                    resultado = 'RED'
        
        elif tipo == 'RESULTADO':
            if 'vovô' in estrategia.lower() or 'vovo' in estrategia.lower():
                # GREEN se o time que estava ganhando MANTEVE a vantagem
                if jogo_acabou:
                    if gs_h > gs_a:  # Casa ganhava
                        resultado = 'GREEN' if gh_agora > ga_agora else 'RED'
                    elif gs_a > gs_h:  # Fora ganhava
                        resultado = 'GREEN' if ga_agora > gh_agora else 'RED'
                    else:
                        resultado = 'RED'
            elif 'contra-ataque' in estrategia.lower() or 'contra ataque' in estrategia.lower():
                # GREEN se o time que perdia empatou ou virou
                if jogo_acabou:
                    if gs_h > gs_a:  # Casa ganhava
                        resultado = 'GREEN' if ga_agora >= gh_agora else 'RED'
                    elif gs_a > gs_h:  # Fora ganhava
                        resultado = 'GREEN' if gh_agora >= ga_agora else 'RED'
                    else:
                        resultado = 'RED'
            elif 'back favorito' in estrategia.lower() or 'nettuno' in estrategia.lower():
                if jogo_acabou:
                    if gs_h > gs_a:
                        resultado = 'GREEN' if gh_agora > ga_agora else 'RED'
                    elif gs_a > gs_h:
                        resultado = 'GREEN' if ga_agora > gh_agora else 'RED'
                    else:
                        resultado = 'RED'
            else:
                # Genérico: GREEN se saiu gol
                if total_gols_agora > gols_no_sinal:
                    resultado = 'GREEN'
                elif jogo_acabou:
                    resultado = 'RED'
        
        else:  # NEUTRO
            if total_gols_agora > gols_no_sinal:
                resultado = 'GREEN'
            elif jogo_acabou:
                resultado = 'RED'
        
        if resultado:
            sinal['Resultado'] = resultado
            sinal['Odd_Atualizada'] = str(sinal.get('Odd', ''))
            alterou = True
            
            # [FUSÃO] Atualizar banca automática
            try:
                banca = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000)))
                stake_rs_str = str(sinal.get('Stake_Recomendado_RS', '0')).replace('R$', '').replace(' ', '').replace(',', '.')
                stake_valor = float(stake_rs_str) if stake_rs_str else 0
                odd_str = str(sinal.get('Odd', '1.50')).replace(',', '.')
                odd_f = float(odd_str) if odd_str else 1.50
                if resultado == 'GREEN':
                    lucro = stake_valor * (odd_f - 1)
                    st.session_state['banca_atual'] = banca + lucro
                else:
                    st.session_state['banca_atual'] = banca - stake_valor
            except:
                pass
            
            # Enviar resultado no Telegram
            token = st.session_state.get('TG_TOKEN', '')
            chat_id = st.session_state.get('TG_CHAT', '')
            chave_res = gerar_chave_universal(fid, estrategia, resultado)
            if chave_res not in st.session_state['alertas_enviados']:
                jogo_nome = str(sinal.get('Jogo', ''))
                odd_original = str(sinal.get('Odd', ''))
                msg_resultado = formatar_resultado_compacto(estrategia, jogo_nome, resultado, placar_agora, odd_original)
                enviar_telegram(token, chat_id, msg_resultado)
                st.session_state['alertas_enviados'].add(chave_res)
    
    return sinais_hoje, alterou

# ==============================================================================
# SNIPERS PARSER (VERSÃO MELHORADA DO NOVO CÓDIGO)
# ==============================================================================
def salvar_snipers_do_texto_v2(texto_ia, mapa_jogos):
    """[FUSÃO] Extrai jogos por zona do texto do matinal"""
    if not texto_ia or not mapa_jogos: return 0
    alvos = {}
    zonas = {
        "ZONA DE GOLS": "GOLS", "ZONA DE TRINCHEIRA": "TRINCHEIRA",
        "ZONA DE MATCH ODDS": "MATCH ODDS", "ZONA DE CARTÕES": "CARTÕES",
        "ZONA DE ESCANTEIOS": "ESCANTEIOS", "ZONA DE DEFESAS": "DEFESAS"
    }
    for jogo_nome, dados in mapa_jogos.items():
        if jogo_nome.lower() in texto_ia.lower() or any(p in texto_ia for p in jogo_nome.split(' x ')):
            fid = dados.get('fid', '')
            zona_detectada = "GERAL"
            for zona_key, zona_nome in zonas.items():
                idx_zona = texto_ia.lower().find(zona_key.lower())
                if idx_zona >= 0:
                    idx_jogo = texto_ia.lower().find(jogo_nome.split(' x ')[0].lower().strip(), idx_zona)
                    if idx_jogo >= 0 and (idx_jogo - idx_zona) < 2000:
                        zona_detectada = zona_nome
            alvos[fid] = {"jogo": jogo_nome, "zona": zona_detectada, "home_id": dados.get('home_id'), "away_id": dados.get('away_id')}
    st.session_state['alvos_do_dia'] = alvos
    return len(alvos)

# ==============================================================================
# PROCESSAMENTO PRINCIPAL - LOOP DE SINAIS LIVE
# ==============================================================================
def processar_jogos_live(api_key, token_tg, chat_tg):
    """[FUSÃO] Loop principal - Stats FRESCAS a cada ciclo + TODOS sinais enviados"""
    agora_br = get_time_br()
    
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        res = requests.get(url, headers={"x-apisports-key": api_key}, params={"live": "all"})
        update_api_usage(res.headers)
        jogos_live = res.json().get('response', [])
    except:
        return 0, 0
    
    if not jogos_live: return 0, 0
    
    # Filtrar blacklist
    df_black = st.session_state.get('df_black', pd.DataFrame())
    ids_black = set(df_black['id'].values) if not df_black.empty else set()
    
    jogos_filtrados = []
    jogos_live_map = {}
    for j in jogos_live:
        lid = str(j['league']['id'])
        if lid in ids_black: continue
        status = j['fixture']['status']['short']
        if status in ['NS', 'FT', 'AET', 'PEN', 'PST', 'CANC', 'ABD', 'AWD', 'WO']:
            jogos_live_map[str(j['fixture']['id'])] = j
            continue
        jogos_filtrados.append(j)
        jogos_live_map[str(j['fixture']['id'])] = j
    
    # Check green/red dos sinais de hoje
    sinais_hoje = st.session_state.get('historico_sinais', [])
    if sinais_hoje:
        sinais_hoje, alterou_gr = check_green_red_hibrido(sinais_hoje, jogos_live_map)
        if alterou_gr:
            st.session_state['historico_sinais'] = sinais_hoje
            atualizar_historico_ram(sinais_hoje)
    
    sinais_enviados = 0
    jogos_processados = 0
    
    # [FUSÃO FIX CRÍTICO] Baixar stats FRESCAS para cada jogo a cada ciclo
    for jogo in jogos_filtrados:
        fid = str(jogo['fixture']['id'])
        fixture = jogo['fixture']
        status = fixture['status']['short']
        minuto = fixture['status'].get('elapsed', 0) or 0
        
        if status not in ['1H', '2H', 'HT']: continue
        if minuto < 5: continue
        
        jogos_processados += 1
        
        # ============================================
        # [FIX] SEMPRE baixa stats frescas - SEM CACHE PERMANENTE
        # ============================================
        stats = baixar_stats_jogo(api_key, fid)
        if not stats:
            # Registra erro mas continua
            gerenciar_erros(jogo['league']['id'], jogo['league'].get('country', ''), jogo['league']['name'], fid)
            continue
        
        # Liga válida - reset erros
        resetar_erros(jogo['league']['id'])
        salvar_safe_league_basic(jogo['league']['id'], jogo['league'].get('country', ''), jogo['league']['name'])
        
        # Guarda stats em session_state (mas será SOBRESCRITA no próximo ciclo)
        st.session_state[f"st_{fid}"] = stats
        
        # Tabela da liga (essa sim pode ter cache longo)
        league_id = jogo['league']['id']
        tabela = None
        if league_id in LIGAS_TABELA:
            tabela = buscar_tabela_liga_cached(api_key, league_id)
        
        # Detectar estratégias
        estrategias_detectadas = detectar_estrategias(jogo, stats, tabela)
        
        if not estrategias_detectadas: continue
        
        # Dados do jogo
        home = jogo['teams']['home']['name']; away = jogo['teams']['away']['name']
        jogo_nome = f"{home} x {away}"
        gh = jogo['goals'].get('home', 0) or 0; ga = jogo['goals'].get('away', 0) or 0
        placar = f"{gh}x{ga}"
        liga_nome = jogo['league']['name']
        home_id = jogo['teams']['home']['id']; away_id = jogo['teams']['away']['id']
        
        # Buscar odd live
        odds_data = baixar_odds_live(api_key, fid)
        odd_live = extrair_odd_over05(odds_data)
        if odd_live == 0: odd_live = 1.50  # Fallback
        
        for estrategia_info in estrategias_detectadas:
            nome_strat = estrategia_info['nome']
            stats_txt = estrategia_info['stats']
            
            chave = gerar_chave_universal(fid, nome_strat, "SINAL")
            if chave in st.session_state['alertas_enviados']: continue
            
            # [FUSÃO] Consultar IA (SEM VETO - apenas informativa)
            opiniao_ia = "Sem IA"
            probabilidade = "70%"
            exit_tip = None
            
            if IA_ATIVADA:
                # Buscar dados extras se disponíveis
                tendencia_50 = None; tendencia_micro = None; bigdata_txt = ""
                try:
                    tendencia_micro = analisar_tendencia_macro_micro(api_key, home_id, away_id)
                except: pass
                try:
                    bigdata_txt = consultar_bigdata_cenario_completo(home_id, away_id)
                except: pass
                
                opiniao_ia, probabilidade = consultar_ia_gemini(
                    nome_strat, minuto, placar, stats_txt, odd_live,
                    jogo_nome, liga_nome, tendencia_50, tendencia_micro, bigdata_txt
                )
                
                # Recuperar tip de saída
                saida_key = f"{jogo_nome}_{nome_strat}"
                exit_tip = st.session_state.get('ia_saida_tips', {}).get(saida_key)
            
            # [FUSÃO] Rastrear movimento de odd
            odd_info = rastrear_movimento_odd(fid, nome_strat, odd_live)
            
            # [FUSÃO] Calcular stake Kelly
            prob_str = str(probabilidade).replace('%', '').strip()
            try: prob_val = float(prob_str)
            except: prob_val = 70.0
            banca_atual = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000)))
            modo_gestao = st.session_state.get('modo_gestao_banca', 'fracionario')
            stake_info = calcular_stake_recomendado(banca_atual, prob_val, odd_live, modo_gestao)
            
            # [FUSÃO] Formatar mensagem compacta
            msg = formatar_mensagem_compacta(
                nome_strat, jogo_nome, liga_nome, minuto, placar, 
                f"{odd_live:.2f}", opiniao_ia, probabilidade, 
                stats_txt, odd_info, stake_info, exit_tip
            )
            
            # [FUSÃO] ENVIAR SEMPRE - IA nunca bloqueia
            enviou = enviar_telegram(token_tg, chat_tg, msg)
            
            if enviou:
                st.session_state['alertas_enviados'].add(chave)
                sinais_enviados += 1
                
                # Salvar no histórico
                item_hist = {
                    'FID': fid, 'Data': agora_br.strftime('%Y-%m-%d'),
                    'Hora': agora_br.strftime('%H:%M'), 'Liga': liga_nome,
                    'Jogo': jogo_nome, 'Placar_Sinal': placar,
                    'Estrategia': nome_strat, 'Resultado': '',
                    'HomeID': str(home_id), 'AwayID': str(away_id),
                    'Odd': f"{odd_live:.2f}", 'Odd_Atualizada': '',
                    'Opiniao_IA': str(opiniao_ia), 'Probabilidade': probabilidade,
                }
                adicionar_historico(item_hist)
        
        # Big Data - salvar stats para jogos encerrados (FT)
        if status in ['FT', 'AET', 'PEN']:
            try: salvar_bigdata(jogo, stats)
            except: pass
    
    # Salvar se necessário
    if st.session_state.get('precisa_salvar', False):
        df_hist = st.session_state.get('historico_full', pd.DataFrame())
        if not df_hist.empty:
            salvar_aba("Historico", df_hist)
    
    return jogos_processados, sinais_enviados

# ==============================================================================
# MATINAL DISPATCHER (FUSÃO: HORÁRIO ESTENDIDO ATÉ 12H)
# ==============================================================================
def executar_matinal_completo(api_key, token_tg, chat_tg):
    """[FUSÃO] Envia TODOS os relatórios matinais. Roda até 12h BRT."""
    agora = get_time_br()
    hora = agora.hour
    
    # [FUSÃO] Horário estendido: roda das 6h até 12h (antes era até 10h)
    if hora < 6 or hora >= 12:
        return
    
    # ========== 1. SNIPER MATINAL (Análise completa IA) ==========
    if not st.session_state.get('matinal_enviado', False):
        try:
            msg_header = f"🌅 <b>RELATÓRIO MATINAL</b> — {agora.strftime('%d/%m/%Y')}\n{'='*35}"
            enviar_telegram(token_tg, chat_tg, msg_header)
            
            texto_ia, mapa_jogos = gerar_insights_matinais_ia(api_key)
            if texto_ia and "Erro" not in texto_ia and "Offline" not in texto_ia:
                qtd_alvos = salvar_snipers_do_texto_v2(texto_ia, mapa_jogos)
                
                msg_sniper = f"🎯 <b>RADAR SNIPER — {qtd_alvos} Alvos Identificados</b>\n\n{texto_ia}"
                enviar_telegram_long(token_tg, chat_tg, msg_sniper)
                
                st.session_state['matinal_enviado'] = True
            else:
                # Tenta novamente no próximo ciclo
                pass
        except Exception as e:
            pass
    
    # ========== 2. MÚLTIPLA MATINAL ==========
    if not st.session_state.get('multipla_matinal_enviada', False) and st.session_state.get('matinal_enviado', False):
        try:
            resultado_multipla, mapa = gerar_multipla_matinal_ia(api_key)
            if resultado_multipla and resultado_multipla.get('jogos'):
                jogos_multipla = resultado_multipla['jogos']
                
                # [FUSÃO] Aplicar anti-correlação
                jogos_filtrados = filtrar_multiplas_nao_correlacionadas(jogos_multipla)
                
                if len(jogos_filtrados) >= 2:
                    msg_mult = "🎰 <b>MÚLTIPLA MATINAL</b>\n\n"
                    for idx, jg in enumerate(jogos_filtrados, 1):
                        jogo_nome = mapa.get(jg.get('fid'), jg.get('jogo', '?'))
                        motivo = jg.get('motivo', '')
                        msg_mult += f"{idx}. ⚽ {jogo_nome}\n   📝 {motivo}\n\n"
                    
                    prob_comb = resultado_multipla.get('probabilidade_combinada', '?')
                    msg_mult += f"📊 Probabilidade combinada: {prob_comb}%\n"
                    msg_mult += f"⚠️ Gestão: Stake reduzido (1-2% da banca)"
                    
                    enviar_telegram(token_tg, chat_tg, msg_mult)
                    st.session_state['multipla_matinal_enviada'] = True
        except: pass
    
    # ========== 3. MERCADOS ALTERNATIVOS ==========
    if not st.session_state.get('alternativos_enviado', False) and st.session_state.get('matinal_enviado', False):
        try:
            sinais_alt = gerar_analise_mercados_alternativos_ia(api_key)
            if sinais_alt:
                msg_alt = "🎯 <b>MERCADOS ALTERNATIVOS</b>\n\n"
                for s in sinais_alt:
                    msg_alt += f"{s.get('titulo', '🟨')} {s.get('jogo', '?')}\n"
                    msg_alt += f"📝 {s.get('destaque', '')}\n"
                    msg_alt += f"👉 {s.get('indicacao', '')}\n\n"
                enviar_telegram(token_tg, chat_tg, msg_alt)
                st.session_state['alternativos_enviado'] = True
        except: pass
    
    # ========== 4. ALAVANCAGEM (BET BUILDER) ==========
    if not st.session_state.get('alavancagem_enviada', False) and st.session_state.get('matinal_enviado', False):
        try:
            picks = gerar_bet_builder_alavancagem(api_key)
            if picks:
                msg_alav = "🚀 <b>ALAVANCAGEM (BET BUILDER)</b>\n\n"
                for p in picks:
                    msg_alav += f"⚽ {p.get('jogo', '?')}\n"
                    msg_alav += f"🎯 {p.get('titulo', '')}\n"
                    selecoes = p.get('selecoes', [])
                    for sel in selecoes:
                        msg_alav += f"  ✅ {sel}\n"
                    msg_alav += f"📊 Confiança: {p.get('confianca', '?')}\n\n"
                enviar_telegram(token_tg, chat_tg, msg_alav)
                st.session_state['alavancagem_enviada'] = True
        except: pass
    
    # ========== 5. DROP ODDS (às 10h e depois) ==========
    if hora >= 10 and not st.session_state.get('drop_enviado_12', False):
        try:
            drops = scanner_drop_odds_pre_live(api_key)
            if drops:
                msg_drop = "📉 <b>DROP ODDS — Oportunidades Pré-Live</b>\n\n"
                for d in drops[:5]:
                    msg_drop += f"⚽ {d['jogo']} ({d['liga']})\n"
                    msg_drop += f"🕐 {d['hora']} | Lado: {d['lado']}\n"
                    msg_drop += f"💰 Bet365: @{d['odd_b365']:.2f} vs Pinnacle: @{d['odd_pinnacle']:.2f}\n"
                    msg_drop += f"📊 Valor: +{d['valor']:.1f}%\n\n"
                enviar_telegram(token_tg, chat_tg, msg_drop)
                st.session_state['drop_enviado_12'] = True
        except: pass

# ==============================================================================
# RELATÓRIOS BI, FINANCEIRO, IA
# ==============================================================================
def gerar_relatorio_bi(token_tg, chat_tg):
    try:
        df = st.session_state.get('historico_full', pd.DataFrame())
        if df.empty: return
        hoje = get_time_br().strftime('%Y-%m-%d')
        df_hoje = df[df['Data'] == hoje]
        total, greens, reds, wr = calcular_stats(df_hoje)
        
        banca_atual = st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000))
        banca_ini = st.session_state.get('banca_inicial', 1000)
        try: lucro = float(banca_atual) - float(banca_ini)
        except: lucro = 0
        
        msg = f"""📊 <b>RELATÓRIO BI — {get_time_br().strftime('%d/%m %H:%M')}</b>

📈 Sinais Hoje: {total}
✅ Greens: {greens} | ❌ Reds: {reds}
🎯 Win Rate: {wr:.1f}%
💰 Banca: R$ {float(banca_atual):.2f} ({'+' if lucro >= 0 else ''}{lucro:.2f})"""
        
        # Top estratégias
        if not df_hoje.empty:
            strat_stats = df_hoje.groupby('Estrategia').apply(
                lambda x: pd.Series({
                    'total': len(x),
                    'greens': len(x[x['Resultado'].str.contains('GREEN', na=False)]),
                    'wr': len(x[x['Resultado'].str.contains('GREEN', na=False)]) / max(len(x[x['Resultado'].str.contains('GREEN|RED', na=False)]), 1) * 100
                })
            ).sort_values('wr', ascending=False)
            
            if len(strat_stats) > 0:
                msg += "\n\n🏆 <b>Top Estratégias:</b>"
                for nome, row in strat_stats.head(5).iterrows():
                    msg += f"\n  {nome}: {int(row['greens'])}/{int(row['total'])} ({row['wr']:.0f}%)"
        
        enviar_telegram(token_tg, chat_tg, msg)
        st.session_state['bi_enviado'] = True
    except: pass

def gerar_relatorio_financeiro(token_tg, chat_tg):
    try:
        banca_atual = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000)))
        banca_ini = float(st.session_state.get('banca_inicial', 1000))
        lucro = banca_atual - banca_ini
        roi = (lucro / banca_ini) * 100 if banca_ini > 0 else 0
        modo = st.session_state.get('modo_gestao_banca', 'fracionario')
        
        msg = f"""💰 <b>FINANCEIRO — {get_time_br().strftime('%d/%m %H:%M')}</b>

🏦 Banca Inicial: R$ {banca_ini:.2f}
💰 Banca Atual: R$ {banca_atual:.2f}
📊 Lucro/Prejuízo: R$ {lucro:+.2f}
📈 ROI: {roi:+.1f}%
⚙️ Modo: Kelly {modo.title()}"""
        
        enviar_telegram(token_tg, chat_tg, msg)
        st.session_state['financeiro_enviado'] = True
    except: pass

# ==============================================================================
# STREAMLIT UI - SIDEBAR
# ==============================================================================
with st.sidebar:
    st.markdown("### ❄️ Neves Analytics PRO")
    st.markdown("##### v3.0 FUSÃO — Todas Estratégias Ativas")
    st.markdown("---")
    
    # API Key
    api_key_input = st.text_input("🔑 API-Football Key", value=st.session_state.get('API_KEY', ''), type="password")
    if api_key_input: st.session_state['API_KEY'] = api_key_input
    
    # Telegram
    st.markdown("##### 📱 Telegram")
    tg_token = st.text_input("Token Bot", value=st.session_state.get('TG_TOKEN', ''), type="password")
    tg_chat = st.text_input("Chat ID", value=st.session_state.get('TG_CHAT', ''))
    if tg_token: st.session_state['TG_TOKEN'] = tg_token
    if tg_chat: st.session_state['TG_CHAT'] = tg_chat
    
    col_tg1, col_tg2 = st.columns(2)
    with col_tg1:
        if st.button("🔌 Testar TG"):
            ok, info = testar_conexao_telegram(tg_token)
            if ok: st.success(f"✅ {info}")
            else: st.error(f"❌ {info}")
    with col_tg2:
        if st.button("📨 Enviar Teste"):
            if enviar_telegram(tg_token, tg_chat, "🧪 Teste Neves Analytics PRO v3.0 FUSÃO"):
                st.success("✅ Enviado!")
            else: st.error("❌ Falhou")
    
    st.markdown("---")
    
    # Gestão de Banca
    st.markdown("##### 💰 Gestão de Banca")
    banca_ini = st.number_input("Banca Inicial (R$)", min_value=10.0, value=float(st.session_state.get('banca_inicial', 1000.0)), step=50.0)
    st.session_state['banca_inicial'] = banca_ini
    if 'banca_atual' not in st.session_state:
        st.session_state['banca_atual'] = banca_ini
    
    banca_atual_display = float(st.session_state.get('banca_atual', banca_ini))
    lucro_display = banca_atual_display - banca_ini
    cor_lucro = "#00FF00" if lucro_display >= 0 else "#FF4B4B"
    st.markdown(f"""
    <div class="metric-box">
        <div class="metric-title">BANCA ATUAL</div>
        <div class="metric-value">R$ {banca_atual_display:.2f}</div>
        <div class="metric-sub" style="color:{cor_lucro}">{'+'if lucro_display>=0 else ''}{lucro_display:.2f} ({(lucro_display/banca_ini*100) if banca_ini>0 else 0:+.1f}%)</div>
    </div>
    """, unsafe_allow_html=True)
    
    modo_gestao = st.selectbox("Modo Kelly", ["fracionario", "conservador", "completo"], index=0)
    st.session_state['modo_gestao_banca'] = modo_gestao
    
    # Simulador Kelly rápido
    with st.expander("🧮 Simulador Kelly"):
        sim_prob = st.slider("Probabilidade (%)", 50, 95, 75)
        sim_odd = st.number_input("Odd", min_value=1.01, value=1.65, step=0.05)
        sim_result = calcular_stake_recomendado(banca_atual_display, sim_prob, sim_odd, modo_gestao)
        st.info(f"💰 Stake: R$ {sim_result['valor']:.2f} ({sim_result['porcentagem']}% da banca)")
    
    if st.button("🔄 Resetar Banca"):
        st.session_state['banca_atual'] = banca_ini
        st.success("Banca resetada!")
    
    st.markdown("---")
    
    # [FUSÃO] Toggle IA Profunda (Tier 2)
    st.markdown("##### 🧠 IA Profunda")
    ia_profunda = st.checkbox("Ativar Tier 2 (H2H, Momentum — consome mais API)", value=st.session_state.get('ia_profunda_ativada', False))
    st.session_state['ia_profunda_ativada'] = ia_profunda
    if ia_profunda:
        st.warning("⚠️ Tier 2 ativo: +2 chamadas API por sinal")
    
    st.markdown("---")
    
    # Controle API
    st.markdown("##### 📊 Consumo API")
    api_used = st.session_state['api_usage']['used']
    api_limit = st.session_state['api_usage']['limit']
    pct_api = (api_used / api_limit * 100) if api_limit > 0 else 0
    cor_api = "#00FF00" if pct_api < 70 else ("#FFFF00" if pct_api < 90 else "#FF4B4B")
    st.markdown(f"""
    <div class="metric-box">
        <div class="metric-title">API-Football</div>
        <div class="metric-value" style="color:{cor_api}">{api_used:,}/{api_limit:,}</div>
        <div class="metric-sub">{pct_api:.1f}% usado</div>
    </div>
    """, unsafe_allow_html=True)
    
    gemini_used = st.session_state['gemini_usage']['used']
    st.markdown(f"""
    <div class="metric-box">
        <div class="metric-title">Gemini IA</div>
        <div class="metric-value">{gemini_used}</div>
        <div class="metric-sub">chamadas hoje</div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Ligas
    with st.expander("🏴 Gerenciar Ligas"):
        st.markdown("**Blacklist:**")
        df_bl = st.session_state.get('df_black', pd.DataFrame())
        if not df_bl.empty:
            st.dataframe(df_bl[['Liga', 'Motivo']].head(20), use_container_width=True, height=200)
        
        st.markdown("**Observação:**")
        df_obs = st.session_state.get('df_vip', pd.DataFrame())
        if not df_obs.empty:
            st.dataframe(df_obs[['Liga', 'Strikes']].head(20), use_container_width=True, height=200)
        
        col_bl1, col_bl2 = st.columns(2)
        with col_bl1:
            ban_id = st.text_input("ID Liga para banir")
        with col_bl2:
            if st.button("🚫 Banir"):
                if ban_id:
                    salvar_blacklist(ban_id, "Manual", f"Liga {ban_id}", "Ban manual")
                    st.success(f"Liga {ban_id} banida!")

# ==============================================================================
# STREAMLIT UI - MAIN PAGE
# ==============================================================================
st.markdown("## ❄️ Neves Analytics PRO v3.0 — FUSÃO")

# Verificar reset diário
verificar_reset_diario()

# Carregar dados
carregar_tudo()

# Status do sistema
col_s1, col_s2, col_s3, col_s4 = st.columns(4)
with col_s1:
    api_ok = bool(st.session_state.get('API_KEY'))
    st.markdown(f'<div class="{"status-active" if api_ok else "status-error"}">{"🟢 API OK" if api_ok else "🔴 Sem API"}</div>', unsafe_allow_html=True)
with col_s2:
    tg_ok = bool(st.session_state.get('TG_TOKEN')) and bool(st.session_state.get('TG_CHAT'))
    st.markdown(f'<div class="{"status-active" if tg_ok else "status-error"}">{"🟢 Telegram OK" if tg_ok else "🔴 Sem Telegram"}</div>', unsafe_allow_html=True)
with col_s3:
    ia_status = "🟢 IA Ativa" if IA_ATIVADA else "🟡 IA Off"
    st.markdown(f'<div class="{"status-active" if IA_ATIVADA else "status-warning"}">{ia_status}</div>', unsafe_allow_html=True)
with col_s4:
    fb_ok = db_firestore is not None
    st.markdown(f'<div class="{"status-active" if fb_ok else "status-warning"}">{"🟢 Firebase" if fb_ok else "🟡 Firebase Off"}</div>', unsafe_allow_html=True)

st.markdown("---")

# Stats do dia
df_hist = st.session_state.get('historico_full', pd.DataFrame())
hoje_str = get_time_br().strftime('%Y-%m-%d')
df_hoje = df_hist[df_hist['Data'] == hoje_str] if not df_hist.empty and 'Data' in df_hist.columns else pd.DataFrame()
total_sinais, greens_hoje, reds_hoje, wr_hoje = calcular_stats(df_hoje)

col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
with col_m1:
    st.markdown(f'<div class="metric-box"><div class="metric-title">SINAIS HOJE</div><div class="metric-value">{total_sinais}</div></div>', unsafe_allow_html=True)
with col_m2:
    st.markdown(f'<div class="metric-box"><div class="metric-title">GREENS</div><div class="metric-value" style="color:#00FF00">{greens_hoje}</div></div>', unsafe_allow_html=True)
with col_m3:
    st.markdown(f'<div class="metric-box"><div class="metric-title">REDS</div><div class="metric-value" style="color:#FF4B4B">{reds_hoje}</div></div>', unsafe_allow_html=True)
with col_m4:
    cor_wr = "#00FF00" if wr_hoje >= 60 else ("#FFFF00" if wr_hoje >= 50 else "#FF4B4B")
    st.markdown(f'<div class="metric-box"><div class="metric-title">WIN RATE</div><div class="metric-value" style="color:{cor_wr}">{wr_hoje:.1f}%</div></div>', unsafe_allow_html=True)
with col_m5:
    alvos_count = len(st.session_state.get('alvos_do_dia', {}))
    st.markdown(f'<div class="metric-box"><div class="metric-title">ALVOS SNIPER</div><div class="metric-value">{alvos_count}</div></div>', unsafe_allow_html=True)

st.markdown("---")

# Botões de controle
col_b1, col_b2, col_b3 = st.columns(3)
with col_b1:
    robo_label = "🔴 PARAR ROBÔ" if st.session_state.get('ROBO_LIGADO', False) else "🟢 LIGAR ROBÔ"
    if st.button(robo_label, use_container_width=True):
        st.session_state.ROBO_LIGADO = not st.session_state.ROBO_LIGADO
        st.rerun()
with col_b2:
    if st.button("📊 Enviar BI", use_container_width=True):
        gerar_relatorio_bi(st.session_state.get('TG_TOKEN', ''), st.session_state.get('TG_CHAT', ''))
        st.success("BI enviado!")
with col_b3:
    if st.button("💰 Enviar Financeiro", use_container_width=True):
        gerar_relatorio_financeiro(st.session_state.get('TG_TOKEN', ''), st.session_state.get('TG_CHAT', ''))
        st.success("Financeiro enviado!")

# Sinais de hoje
st.markdown("### 📋 Sinais de Hoje")
sinais_display = st.session_state.get('historico_sinais', [])
if sinais_display:
    for sinal in sinais_display[:30]:
        resultado = str(sinal.get('Resultado', ''))
        if 'GREEN' in resultado: cor = "#1F4025"; icon = "✅"
        elif 'RED' in resultado: cor = "#3B1010"; icon = "❌"
        else: cor = "#1A1C24"; icon = "⏳"
        
        tipo = classificar_tipo_estrategia(sinal.get('Estrategia', ''))
        tipo_badge = f"{'🔴' if tipo == 'OVER' else '🔵' if tipo == 'UNDER' else '🟡'} {tipo}"
        
        st.markdown(f"""
        <div style="background-color:{cor}; padding:8px; border-radius:6px; margin-bottom:4px; border-left: 3px solid {'#00FF00' if 'GREEN' in resultado else '#FF4B4B' if 'RED' in resultado else '#666'}">
            {icon} <b>{sinal.get('Estrategia','')}</b> | {sinal.get('Jogo','')} | {sinal.get('Placar_Sinal','')} | @{sinal.get('Odd','')} | {tipo_badge} | {sinal.get('Opiniao_IA','')} {sinal.get('Probabilidade','')}
        </div>
        """, unsafe_allow_html=True)
else:
    st.info("Nenhum sinal hoje ainda. Ligue o robô para começar!")

# ==============================================================================
# TABS: HISTÓRICO, DASHBOARD, CHAT IA
# ==============================================================================
tab_hist, tab_dash, tab_chat = st.tabs(["📊 Histórico", "📈 Dashboard", "💬 Chat IA"])

with tab_hist:
    if not df_hist.empty:
        # Filtros
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            datas_disp = sorted(df_hist['Data'].unique(), reverse=True)[:30] if 'Data' in df_hist.columns else []
            data_sel = st.selectbox("Data", ["Todas"] + list(datas_disp))
        with col_f2:
            strats_disp = sorted(df_hist['Estrategia'].unique()) if 'Estrategia' in df_hist.columns else []
            strat_sel = st.selectbox("Estratégia", ["Todas"] + list(strats_disp))
        
        df_filtrado = df_hist.copy()
        if data_sel != "Todas": df_filtrado = df_filtrado[df_filtrado['Data'] == data_sel]
        if strat_sel != "Todas": df_filtrado = df_filtrado[df_filtrado['Estrategia'] == strat_sel]
        
        total_f, greens_f, reds_f, wr_f = calcular_stats(df_filtrado)
        st.markdown(f"**{total_f} sinais** | ✅ {greens_f} Greens | ❌ {reds_f} Reds | 🎯 {wr_f:.1f}% Win Rate")
        
        cols_show = ['Data', 'Hora', 'Liga', 'Jogo', 'Estrategia', 'Placar_Sinal', 'Odd', 'Resultado', 'Opiniao_IA', 'Probabilidade']
        cols_exist = [c for c in cols_show if c in df_filtrado.columns]
        st.dataframe(df_filtrado[cols_exist].head(100), use_container_width=True, height=400)
    else:
        st.info("Sem dados históricos ainda.")

with tab_dash:
    if not df_hist.empty and len(df_hist) > 5:
        try:
            # Win rate por estratégia
            df_apurado = df_hist[df_hist['Resultado'].str.contains('GREEN|RED', na=False)]
            if not df_apurado.empty:
                strat_wr = df_apurado.groupby('Estrategia').apply(
                    lambda x: pd.Series({
                        'Total': len(x),
                        'Greens': len(x[x['Resultado'].str.contains('GREEN', na=False)]),
                        'WinRate': len(x[x['Resultado'].str.contains('GREEN', na=False)]) / len(x) * 100
                    })
                ).reset_index().sort_values('WinRate', ascending=False)
                
                fig = px.bar(strat_wr, x='Estrategia', y='WinRate', color='WinRate',
                            color_continuous_scale=['#FF4B4B', '#FFFF00', '#00FF00'],
                            title="Win Rate por Estratégia", range_color=[40, 90])
                fig.update_layout(template="plotly_dark", height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            # Timeline de resultados
            if 'Data' in df_hist.columns:
                df_daily = df_hist.groupby('Data').apply(
                    lambda x: pd.Series({
                        'Sinais': len(x),
                        'Greens': len(x[x['Resultado'].str.contains('GREEN', na=False)]),
                        'Reds': len(x[x['Resultado'].str.contains('RED', na=False)]),
                    })
                ).reset_index().tail(30)
                
                if not df_daily.empty:
                    fig2 = go.Figure()
                    fig2.add_trace(go.Bar(x=df_daily['Data'], y=df_daily['Greens'], name='Greens', marker_color='#00FF00'))
                    fig2.add_trace(go.Bar(x=df_daily['Data'], y=df_daily['Reds'], name='Reds', marker_color='#FF4B4B'))
                    fig2.update_layout(barmode='group', template="plotly_dark", title="Resultados por Dia", height=350)
                    st.plotly_chart(fig2, use_container_width=True)
        except: st.warning("Dados insuficientes para dashboard.")
    else:
        st.info("Acumule mais sinais para ver o dashboard.")

with tab_chat:
    st.markdown("### 💬 Chat com IA Gemini")
    if IA_ATIVADA:
        if 'chat_messages' not in st.session_state: st.session_state['chat_messages'] = []
        
        for msg_chat in st.session_state['chat_messages']:
            role = msg_chat['role']
            with st.chat_message(role):
                st.markdown(msg_chat['content'])
        
        user_input = st.chat_input("Pergunte sobre algum jogo, estratégia ou mercado...")
        if user_input:
            st.session_state['chat_messages'].append({"role": "user", "content": user_input})
            with st.chat_message("user"):
                st.markdown(user_input)
            
            with st.chat_message("assistant"):
                with st.spinner("🤖 Analisando..."):
                    try:
                        contexto_bd = carregar_contexto_global_firebase()
                        sinais_txt = ""
                        for s in st.session_state.get('historico_sinais', [])[:10]:
                            sinais_txt += f"\n- {s.get('Estrategia')}: {s.get('Jogo')} {s.get('Placar_Sinal')} {s.get('Resultado')}"
                        
                        prompt_chat = f"""Você é o assistente IA do Neves Analytics PRO. 
Contexto do Big Data: {contexto_bd}
Sinais de hoje: {sinais_txt}
Pergunta do trader: {user_input}

Responda de forma objetiva e técnica."""
                        
                        response = model_ia.generate_content(prompt_chat)
                        st.session_state['gemini_usage']['used'] += 1
                        resposta = response.text
                        st.markdown(resposta)
                        st.session_state['chat_messages'].append({"role": "assistant", "content": resposta})
                    except Exception as e:
                        st.error(f"Erro IA: {e}")
    else:
        st.warning("IA não configurada. Adicione GEMINI_KEY nos secrets.")

# ==============================================================================
# LOOP PRINCIPAL DO ROBÔ (AUTO-REFRESH)
# ==============================================================================
st.markdown("---")

if st.session_state.get('ROBO_LIGADO', False):
    api_key = st.session_state.get('API_KEY', '')
    token_tg = st.session_state.get('TG_TOKEN', '')
    chat_tg = st.session_state.get('TG_CHAT', '')
    
    if not api_key:
        st.error("⚠️ Configure a API Key para ligar o robô!")
        st.session_state.ROBO_LIGADO = False
    else:
        agora_br = get_time_br()
        hora_br = agora_br.hour
        
        status_container = st.empty()
        status_container.markdown(f"""
        <div class="status-active">
            🤖 ROBÔ ATIVO — Ciclo: {agora_br.strftime('%H:%M:%S')} | Próximo em 60s
        </div>
        """, unsafe_allow_html=True)
        
        # [FUSÃO] Matinal estendido até 12h
        executar_matinal_completo(api_key, token_tg, chat_tg)
        
        # Processar jogos live
        jogos_proc, sinais_env = processar_jogos_live(api_key, token_tg, chat_tg)
        
        # Relatórios automáticos
        if hora_br >= 14 and not st.session_state.get('bi_enviado', False):
            gerar_relatorio_bi(token_tg, chat_tg)
        if hora_br >= 20 and not st.session_state.get('financeiro_enviado', False):
            gerar_relatorio_financeiro(token_tg, chat_tg)
        
        # Drop odds às 16h
        if hora_br >= 16 and not st.session_state.get('drop_enviado_16', False):
            try:
                drops = scanner_drop_odds_pre_live(api_key)
                if drops:
                    msg_drop = "📉 <b>DROP ODDS 16h</b>\n\n"
                    for d in drops[:3]:
                        msg_drop += f"⚽ {d['jogo']} | @{d['odd_b365']:.2f} (+{d['valor']:.1f}%)\n"
                    enviar_telegram(token_tg, chat_tg, msg_drop)
                st.session_state['drop_enviado_16'] = True
            except: pass
        
        # Footer
        st.markdown(f"""
        <div class="footer-timer">
            ❄️ Neves Analytics PRO v3.0 FUSÃO | 🤖 Ativo | ⚽ {jogos_proc} jogos | 📨 {sinais_env} sinais | 🕐 {agora_br.strftime('%H:%M:%S')}
        </div>
        """, unsafe_allow_html=True)
        
        # Auto-refresh a cada 60 segundos
        time.sleep(60)
        st.rerun()
else:
    st.markdown("""
    <div class="status-warning">
        ⏸️ Robô pausado — Clique em LIGAR ROBÔ para iniciar
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown(f"""
    <div class="footer-timer">
        ❄️ Neves Analytics PRO v3.0 FUSÃO | ⏸️ Pausado | 🕐 {get_time_br().strftime('%H:%M:%S')}
    </div>
    """, unsafe_allow_html=True)
