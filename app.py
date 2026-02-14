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
if 'jogo_limpo_enviado' not in st.session_state: st.session_state['jogo_limpo_enviado'] = False
if 'resumo_meio_dia_enviado' not in st.session_state: st.session_state['resumo_meio_dia_enviado'] = False
if 'alerta_quente_enviado' not in st.session_state: st.session_state['alerta_quente_enviado'] = {}
if 'h2h_cache' not in st.session_state: st.session_state['h2h_cache'] = {}
if 'odd_history' not in st.session_state: st.session_state['odd_history'] = {}

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

# ==============================================================================
# 2b. FUNÇÕES AUXILIARES BÁSICAS
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
# 3. [FUSÃO] CLASSIFICAÇÃO OVER/UNDER/RESULTADO + KELLY + ODD MOVEMENT
# ==============================================================================

def classificar_tipo_estrategia(estrategia: str) -> str:
    estrategia_lower = str(estrategia or '').lower()
    over_keywords = ['porteira aberta','golden bet','gol relâmpago','gol relampago','blitz casa','blitz visitante','blitz','massacre','choque','briga de rua','escanteios','escanteio','corner','janela de ouro','janela ouro','tiroteio elite','sniper final','sniper matinal','lay goleada','over','btts','ambas marcam','gol']
    under_keywords = ['jogo morno','arame liso','under','sem gols','morno','arame']
    resultado_keywords = ['estratégia do vovô','estrategia do vovo','vovô','vovo','contra-ataque','contra ataque','back','segurar','manter','vitória','vitoria','empate']
    for keyword in resultado_keywords:
        if keyword in estrategia_lower: return 'RESULTADO'
    for keyword in under_keywords:
        if keyword in estrategia_lower: return 'UNDER'
    for keyword in over_keywords:
        if keyword in estrategia_lower: return 'OVER'
    return 'NEUTRO'


def obter_descricao_aposta(estrategia: str) -> dict:
    tipo = classificar_tipo_estrategia(estrategia)
    estrategia_lower = str(estrategia or '').lower()
    if 'golden bet' in estrategia_lower:
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols (Asiático)', 'ordem': '👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de 0.5 Gols</b>', 'ganha_se': 'Sai GOL', 'perde_se': 'Não sai GOL'}
    elif 'janela' in estrategia_lower and 'ouro' in estrategia_lower:
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols (Asiático)', 'ordem': '👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de 0.5 Gols</b>', 'ganha_se': 'Sai GOL', 'perde_se': 'Não sai GOL'}
    elif 'gol relâmpago' in estrategia_lower or 'gol relampago' in estrategia_lower:
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols no 1º Tempo', 'ordem': '👉 <b>FAZER:</b> Entrar em GOLS 1º TEMPO\n✅ Aposta: <b>Mais de 0.5 Gols HT</b>', 'ganha_se': 'Sai GOL no 1º Tempo', 'perde_se': 'Não sai GOL no 1º Tempo'}
    elif 'blitz' in estrategia_lower:
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols (Asiático)', 'ordem': '👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de 0.5 Gols</b>', 'ganha_se': 'Sai GOL', 'perde_se': 'Não sai GOL'}
    elif 'porteira' in estrategia_lower:
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols (Asiático)', 'ordem': '👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de 0.5 Gols</b>', 'ganha_se': 'Sai GOL', 'perde_se': 'Não sai GOL'}
    elif 'tiroteio' in estrategia_lower:
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols (Asiático)', 'ordem': '👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de 0.5 Gols</b>', 'ganha_se': 'Sai GOL', 'perde_se': 'Não sai GOL'}
    elif 'sniper' in estrategia_lower:
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols (Asiático)', 'ordem': '👉 <b>FAZER:</b> Entrar em GOL LIMITE nos ACRÉSCIMOS\n✅ Aposta: <b>Mais de 0.5 Gols</b>', 'ganha_se': 'Sai GOL nos acréscimos', 'perde_se': 'Não sai GOL'}
    elif 'lay goleada' in estrategia_lower:
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols (Asiático)', 'ordem': '👉 <b>FAZER:</b> Entrar em GOL LIMITE (gol da honra)\n✅ Aposta: <b>Mais de 0.5 Gols</b>', 'ganha_se': 'Sai GOL', 'perde_se': 'Não sai GOL'}
    elif 'massacre' in estrategia_lower:
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols no 1º Tempo', 'ordem': '👉 <b>FAZER:</b> Entrar em GOLS 1º TEMPO\n✅ Aposta: <b>Mais de 0.5 Gols HT</b>', 'ganha_se': 'Sai GOL no 1º Tempo', 'perde_se': 'Não sai GOL no 1º Tempo'}
    elif 'choque' in estrategia_lower:
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols no 1º Tempo', 'ordem': '👉 <b>FAZER:</b> Entrar em GOLS 1º TEMPO\n✅ Aposta: <b>Mais de 0.5 Gols HT</b>', 'ganha_se': 'Sai GOL no 1º Tempo', 'perde_se': 'Não sai GOL no 1º Tempo'}
    elif 'briga de rua' in estrategia_lower:
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols no 1º Tempo', 'ordem': '👉 <b>FAZER:</b> Entrar em GOLS 1º TEMPO\n✅ Aposta: <b>Mais de 0.5 Gols HT</b>', 'ganha_se': 'Sai GOL no 1º Tempo', 'perde_se': 'Não sai GOL no 1º Tempo'}
    elif 'contra-ataque' in estrategia_lower or 'contra ataque' in estrategia_lower:
        return {'tipo': 'RESULTADO', 'aposta': 'Back Empate ou Zebra', 'ordem': '👉 <b>FAZER:</b> Back no time que está PERDENDO\n⚡ Aposta: <b>Recuperação ou Empate</b>', 'ganha_se': 'Time EMPATA ou VIRA', 'perde_se': 'Time que está ganhando MANTÉM'}
    elif 'jogo morno' in estrategia_lower or 'morno' in estrategia_lower:
        return {'tipo': 'UNDER', 'aposta': 'Menos de 0.5 Gols (Under)', 'ordem': '👉 <b>FAZER:</b> Entrar em UNDER GOLS\n❄️ Aposta: <b>NÃO SAI mais gol</b>', 'ganha_se': 'NÃO sai GOL', 'perde_se': 'Sai GOL'}
    elif 'arame liso' in estrategia_lower or 'arame' in estrategia_lower:
        return {'tipo': 'UNDER', 'aposta': 'Menos de 0.5 Gols (Under)', 'ordem': '👉 <b>FAZER:</b> Entrar em UNDER GOLS\n🧊 Aposta: <b>Falsa pressão, NÃO SAI gol</b>', 'ganha_se': 'NÃO sai GOL (falsa pressão confirmada)', 'perde_se': 'Sai GOL'}
    elif 'vovô' in estrategia_lower or 'vovo' in estrategia_lower:
        return {'tipo': 'RESULTADO', 'aposta': 'Vitória do time que está ganhando', 'ordem': '👉 <b>FAZER:</b> Back no time que está GANHANDO\n👴 Aposta: <b>Time manterá a vitória</b>', 'ganha_se': 'Time MANTÉM ou AUMENTA vantagem', 'perde_se': 'Time EMPATA ou PERDE'}
    elif tipo == 'OVER':
        return {'tipo': 'OVER', 'aposta': 'Mais de 0.5 Gols', 'ordem': '👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de 0.5 Gols</b>', 'ganha_se': 'Sai GOL', 'perde_se': 'Não sai GOL'}
    elif tipo == 'UNDER':
        return {'tipo': 'UNDER', 'aposta': 'Menos de 0.5 Gols', 'ordem': '👉 <b>FAZER:</b> Entrar em UNDER GOLS\n❄️ Aposta: <b>NÃO SAI mais gol</b>', 'ganha_se': 'NÃO sai GOL', 'perde_se': 'Sai GOL'}
    else:
        return {'tipo': 'RESULTADO', 'aposta': 'Resultado Final', 'ordem': '👉 <b>FAZER:</b> Apostar no resultado indicado', 'ganha_se': 'Resultado se confirma', 'perde_se': 'Resultado não se confirma'}


def calcular_gols_atuais(placar_str: str) -> int:
    try:
        gh, ga = map(int, str(placar_str).lower().replace(' ', '').split('x'))
        return int(gh) + int(ga)
    except: return 0


def calcular_threshold_dinamico(estrategia: str, odd_atual: float) -> int:
    estr = str(estrategia or '')
    tipo = classificar_tipo_estrategia(estr)
    thr = 65 if tipo == 'UNDER' else (75 if ('golden' in estr.lower() or 'diamante' in estr.lower()) else 50)
    try:
        odd = float(odd_atual)
        if odd >= 2.00: thr -= 5
        elif odd <= 1.30: thr += 10
    except: pass
    return int(max(50, min(thr, 80)))


def rastrear_movimento_odd(fid, estrategia, odd_atual, janela_min=5):
    try:
        fid = str(fid); odd_atual = float(odd_atual)
    except: return 'DESCONHECIDO', 0.0
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


ODD_MINIMA_POR_ESTRATEGIA = {
    "estratégia do vovô": 1.20, "vovô": 1.20, "jogo morno": 1.35, "morno": 1.35,
    "porteira aberta": 1.50, "porteira": 1.50, "golden bet": 1.80, "golden": 1.80,
    "gol relâmpago": 1.60, "relâmpago": 1.60, "blitz": 1.60, "massacre": 1.70,
    "alavancagem": 3.50, "sniper": 1.80, "arame liso": 1.35, "under": 1.40,
}

def obter_odd_minima(estrategia):
    try:
        estrategia_lower = str(estrategia or '').lower()
        for chave, odd_min in ODD_MINIMA_POR_ESTRATEGIA.items():
            if chave in estrategia_lower: return float(odd_min)
        return 1.50
    except: return 1.50


# ==============================================================================
# 4. FUNÇÕES DE API, ODD E DADOS
# ==============================================================================

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
                if not odd_obj:
                    odd_obj = next((v for v in mercado_gols['values'] if v['value'] == "Over 1.5"), None)
                if odd_obj:
                    return float(odd_obj['odd']), f"{odd_obj['value']} (Bet365)"
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
        st.session_state['jogo_limpo_enviado'] = False
        st.session_state['resumo_meio_dia_enviado'] = False
        st.session_state['bigdata_enviado'] = False
        st.session_state['alerta_quente_enviado'] = {}
        st.session_state['multiplas_pendentes'] = []
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
            dt_jogo = j['fixture']['date']
            try: hora_jogo = datetime.fromisoformat(dt_jogo.replace('Z', '+00:00'))
            except: hora_jogo = datetime.strptime(dt_jogo[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.utc)
            agora_utc = datetime.now(pytz.utc)
            diff = (hora_jogo - agora_utc).total_seconds() / 3600
            if j['fixture']['status']['short'] != 'NS': continue
            if not (3 <= diff <= 8): continue
            odd_b365, odd_pin, lado = buscar_odds_comparativas(api_key, fid)
            if odd_b365 > 0 and lado:
                diferenca = ((odd_b365 - odd_pin) / odd_pin) * 100
                oportunidades.append({"fid": fid, "jogo": f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", "liga": j['league']['name'], "hora": j['fixture']['date'][11:16], "lado": lado, "odd_b365": odd_b365, "odd_pinnacle": odd_pin, "valor": diferenca})
        return oportunidades
    except: return []


# ==============================================================================
# 5. GERENCIAMENTO DE PLANILHAS E DADOS
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
            if not df_ram.empty: st.toast(f"⚠️ Erro leitura {nome_aba}. Usando Cache.", icon="🛡️"); return df_ram
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
    if st.session_state.get('BLOQUEAR_SALVAMENTO', False): st.session_state['precisa_salvar'] = True; return False
    try:
        conn.update(worksheet=nome_aba, data=df_para_salvar)
        if nome_aba == "Historico": st.session_state['precisa_salvar'] = False
        return True
    except: st.session_state['precisa_salvar'] = True; return False

def salvar_blacklist(id_liga, pais, nome_liga, motivo_ban):
    df = st.session_state['df_black']; id_norm = normalizar_id(id_liga)
    if id_norm in df['id'].values:
        idx = df[df['id'] == id_norm].index[0]; df.at[idx, 'Motivo'] = str(motivo_ban)
    else:
        novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': str(motivo_ban)}])
        df = pd.concat([df, novo], ignore_index=True)
    st.session_state['df_black'] = df; salvar_aba("Blacklist", df); sanitizar_conflitos()

def sanitizar_conflitos():
    df_black = st.session_state.get('df_black', pd.DataFrame()); df_vip = st.session_state.get('df_vip', pd.DataFrame()); df_safe = st.session_state.get('df_safe', pd.DataFrame())
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
    df_vip = st.session_state.get('df_vip', pd.DataFrame()); strikes = 0; jogos_erro = []
    if not df_vip.empty and id_norm in df_vip['id'].values:
        row = df_vip[df_vip['id'] == id_norm].iloc[0]
        val_jogos = str(row.get('Jogos_Erro', '')).strip()
        if val_jogos: jogos_erro = val_jogos.split(',')
    if fid_str in jogos_erro: return
    jogos_erro.append(fid_str); strikes = len(jogos_erro)
    if strikes >= 10:
        salvar_blacklist(id_liga, pais, nome_liga, f"Banida ({formatar_inteiro_visual(strikes)} Jogos Sem Dados)")
    else:
        if id_norm in df_vip['id'].values:
            idx = df_vip[df_vip['id'] == id_norm].index[0]
            df_vip.at[idx, 'Strikes'] = str(strikes); df_vip.at[idx, 'Jogos_Erro'] = ",".join(jogos_erro)
            df_vip.at[idx, 'Data_Erro'] = get_time_br().strftime('%Y-%m-%d')
            salvar_aba("Obs", df_vip); st.session_state['df_vip'] = df_vip
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
                df['Data'] = df['Data_Temp'].dt.strftime('%Y-%m-%d').fillna(df['Data']); df = df.drop(columns=['Data_Temp'])
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
    if 'jogos_salvos_bigdata_carregados' not in st.session_state or not st.session_state['jogos_salvos_bigdata_carregados'] or force:
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
    for _, row in df_hoje_updates.iterrows():
        chave = f"{row['FID']}_{row['Estrategia']}"; mapa_atualizacao[chave] = row
    def atualizar_linha(row):
        chave = f"{row['FID']}_{row['Estrategia']}"
        if chave in mapa_atualizacao:
            nova_linha = mapa_atualizacao[chave]
            if str(row['Resultado']) != str(nova_linha['Resultado']): st.session_state['precisa_salvar'] = True
            return nova_linha
        return row
    st.session_state['historico_full'] = df_memoria.apply(atualizar_linha, axis=1)


# ==============================================================================
# 6. [FUSÃO] TELEGRAM — MENSAGENS COMPACTAS
# ==============================================================================

def enviar_telegram(token, chat_id, texto, parse_mode="HTML"):
    if not token or not chat_id: return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {"chat_id": chat_id, "text": texto, "parse_mode": parse_mode}
        r = requests.post(url, data=data, timeout=15)
        return r.status_code == 200
    except: return False

def enviar_foto_telegram(token, chat_id, img_bytes, caption=""):
    if not token or not chat_id: return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        files = {'photo': ('chart.png', img_bytes, 'image/png')}
        data = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'HTML'}
        r = requests.post(url, files=files, data=data, timeout=15)
        return r.status_code == 200
    except: return False

def semaforo_odd(odd_atual, estrategia):
    """Retorna emoji de semáforo baseado na odd mínima da estratégia. NUNCA bloqueia."""
    try:
        odd_min = obter_odd_minima(estrategia)
        odd = float(odd_atual)
        if odd < odd_min * 0.85: return "⛔", f"ODD BAIXA (@{odd:.2f})"
        if odd < odd_min: return "⏳", f"ODD LIMÍTROFE (@{odd:.2f})"
        return "✅", f"ODD BOA (@{odd:.2f})"
    except: return "✅", f"@{odd_atual}"

def emoji_tipo(estrategia):
    tipo = classificar_tipo_estrategia(estrategia)
    if tipo == 'OVER': return '⚽'
    if tipo == 'UNDER': return '❄️'
    if tipo == 'RESULTADO': return '👴'
    return '📊'

def emoji_movimento_odd(movimento):
    if 'SUBINDO' in str(movimento): return '📈'
    if 'CAINDO' in str(movimento): return '📉'
    return '➡️'


def formatar_sinal_live_compacto(estrategia, liga, time_home, time_away, minuto, placar,
                                  odd_atual, confianca, dados_ia=None, dados_stats=None):
    """Formata sinal live COMPACTO (~18 linhas) com toda informação essencial."""
    tipo = classificar_tipo_estrategia(estrategia)
    descricao = obter_descricao_aposta(estrategia)
    sema_emoji, sema_txt = semaforo_odd(odd_atual, estrategia)

    # Header
    if tipo == 'OVER':
        tipo_label = "⚽ OVER: Sai gol"
        icone = "⚽"
    elif tipo == 'UNDER':
        tipo_label = "❄️ UNDER: NÃO sai gol"
        icone = "❄️"
    elif tipo == 'RESULTADO':
        tipo_label = "👴 RESULTADO: Manter vitória"
        icone = "👴"
    else:
        tipo_label = "📊 ANÁLISE"
        icone = "📊"

    msg = f"{sema_emoji} <b>SINAL {estrategia}</b> | 🟢 Strat: {confianca}%\n"
    msg += f"🏆 {liga} 🛡️\n"
    msg += f"⚽ {time_home} 🆚 {time_away}\n"
    msg += f"⏰ {minuto}' min | 🥅 Placar: {placar}\n\n"

    msg += f"{tipo_label}\n"
    msg += f"🔥 {sema_txt}\n"
    msg += f"────────────────\n"
    msg += f"{descricao['ordem']}\n\n"

    # Stats básicos
    if dados_stats:
        msg += f"📊 {dados_stats.get('analise_curta', 'Analisando...')}\n"
        if dados_stats.get('rating_home') and dados_stats.get('rating_away'):
            msg += f"⭐ Rating: Casa {dados_stats['rating_home']} | Fora {dados_stats['rating_away']}\n"
        if dados_stats.get('pct_over_home') and dados_stats.get('pct_over_away'):
            msg += f"📈 Gols (Recente): Casa {dados_stats['pct_over_home']}% | Fora {dados_stats['pct_over_away']}% Over\n"

    # IA compacta (3-4 linhas no máximo)
    if dados_ia:
        aprovado = dados_ia.get('aprovado', True)
        prob_ia = dados_ia.get('probabilidade', confianca)
        emoji_ia = "✅ APROVADO" if aprovado else "⚠️ ARRISCADO"
        intensidade = dados_ia.get('intensidade', '')
        int_txt = f" | Intensidade {intensidade} 🔥" if intensidade else ""

        msg += f"\n🤖 IA: {emoji_ia} ({prob_ia}%){int_txt}\n"

        historico_txt = dados_ia.get('historico_pessoal', '')
        consenso = dados_ia.get('consenso', '?/?')
        if historico_txt:
            msg += f"├─ 🗳️ Consenso: {consenso} | 📚 {historico_txt}\n"
        else:
            msg += f"├─ 🗳️ Consenso: {consenso}\n"

        stake_info = dados_ia.get('stake_info', '')
        if stake_info:
            msg += f"├─ 💰 {stake_info}\n"

        saida = dados_ia.get('saida', 'Segurar até o final')
        msg += f"└─ 🚪 Saída: 🔒 {saida}"
    else:
        msg += f"\n🤖 IA: ✅ Confiança {confianca}%"

    return msg


def formatar_green_compacto(estrategia, time_home, time_away, placar, lucro=None, banca=None):
    msg = f"✅ <b>GREEN CONFIRMADO!</b>\n"
    msg += f"⚽ {time_home} x {time_away}\n"
    msg += f"📈 Placar: {placar}\n"
    msg += f"🎯 {estrategia}"
    if lucro is not None and banca is not None:
        msg += f"\n💰 Lucro: +R$ {lucro:.2f} | Banca: R$ {banca:.2f}"
    return msg


def formatar_red_compacto(estrategia, time_home, time_away, placar, perda=None, banca=None):
    msg = f"❌ <b>RED CONFIRMADO</b>\n"
    msg += f"⚽ {time_home} x {time_away}\n"
    msg += f"📉 Placar: {placar}\n"
    msg += f"🎯 {estrategia}"
    if perda is not None and banca is not None:
        msg += f"\n💰 Perda: -R$ {perda:.2f} | Banca: R$ {banca:.2f}"
    return msg


def formatar_var_alerta(time_home, time_away, placar):
    msg = f"⚠️ <b>VAR ACIONADO | GOL ANULADO</b>\n"
    msg += f"⚽ {time_home} x {time_away}\n"
    msg += f"📉 Placar voltou: {placar}"
    return msg


def formatar_super_odd(time_home, time_away, estrategia, odd_nova):
    msg = f"💎 <b>OPORTUNIDADE DE VALOR!</b>\n\n"
    msg += f"⚽ {time_home} 🆚 {time_away}\n"
    msg += f"📈 A Odd subiu! Entrada valorizada.\n"
    msg += f"🔥 Estratégia: {estrategia}\n"
    msg += f"💰 Nova Odd: @{odd_nova:.2f}"
    return msg


# ==============================================================================
# 7. [FUSÃO] IA GEMINI — INFORMATIVA, NUNCA BLOQUEIA
# ==============================================================================

def consultar_ia_gemini(estrategia, liga, time_home, time_away, minuto, placar,
                         odd_atual, dados_extras=None, historico_pessoal=None):
    """
    Consulta IA Gemini para análise. NUNCA retorna veto.
    Retorna dict com análise compacta para incluir na mensagem.
    """
    if not IA_ATIVADA:
        return {'aprovado': True, 'probabilidade': 70, 'consenso': '?/?',
                'historico_pessoal': '', 'stake_info': '', 'saida': 'Segurar até o final',
                'intensidade': '', 'resumo': 'IA não disponível'}

    # Rate limit check
    if st.session_state.get('ia_bloqueada_ate'):
        agora = get_time_br()
        if agora < st.session_state['ia_bloqueada_ate']:
            return {'aprovado': True, 'probabilidade': 70, 'consenso': '?/?',
                    'historico_pessoal': '', 'stake_info': '', 'saida': 'Segurar até o final',
                    'intensidade': '', 'resumo': 'IA em cooldown'}
        else:
            st.session_state['ia_bloqueada_ate'] = None

    tipo = classificar_tipo_estrategia(estrategia)

    # Montar histórico pessoal resumido
    hist_txt = ""
    if historico_pessoal:
        greens = sum(1 for h in historico_pessoal if 'GREEN' in str(h.get('Resultado', '')))
        reds = sum(1 for h in historico_pessoal if 'RED' in str(h.get('Resultado', '')))
        total = greens + reds
        if total > 0:
            wr = int((greens / total) * 100)
            hist_txt = f"{wr}% nesta estratégia ({greens}G/{reds}R)"

    # Dados de intensidade
    intensidade = ""
    if dados_extras:
        chutes = dados_extras.get('chutes_total', 0)
        chutes_gol = dados_extras.get('chutes_gol', 0)
        try:
            min_val = int(str(minuto).replace("'", "").strip())
            if min_val > 0:
                intensidade_val = round(chutes / max(min_val / 15, 1), 1)
                intensidade = str(intensidade_val)
        except: pass

    # Prompt compacto para IA
    prompt = f"""Você é um analista de apostas esportivas. Analise este cenário e responda APENAS em JSON:

Estratégia: {estrategia} (Tipo: {tipo})
Liga: {liga}
Jogo: {time_home} x {time_away}
Minuto: {minuto}' | Placar: {placar}
Odd Atual: {odd_atual}
{"Histórico Pessoal: " + hist_txt if hist_txt else ""}
{"Chutes Total: " + str(dados_extras.get('chutes_total', '?')) + " | No Gol: " + str(dados_extras.get('chutes_gol', '?')) if dados_extras else ""}

Responda APENAS com este JSON (sem markdown):
{{"probabilidade": <50-95>, "consenso_3_experts": "<X/3>", "saida": "<segurar/cashout_parcial/cashout_total>", "resumo": "<1 frase curta>"}}

REGRAS:
- probabilidade: sua estimativa de chance de sucesso (50-95)
- consenso: quantos de 3 analistas (conservador, moderado, agressivo) aprovam
- saida: melhor plano de saída
- resumo: 1 frase curta justificando"""

    try:
        st.session_state['gemini_usage']['used'] += 1
        response = model_ia.generate_content(prompt)
        texto_resp = response.text.strip()

        # Limpar resposta
        texto_resp = texto_resp.replace('```json', '').replace('```', '').strip()

        try:
            dados = json.loads(texto_resp)
        except:
            # Tentar extrair JSON com regex
            match = re.search(r'\{[^}]+\}', texto_resp)
            if match:
                dados = json.loads(match.group())
            else:
                raise ValueError("JSON não encontrado")

        prob = min(95, max(50, int(dados.get('probabilidade', 70))))
        consenso = str(dados.get('consenso_3_experts', '2/3'))
        saida = str(dados.get('saida', 'segurar'))
        resumo = str(dados.get('resumo', ''))

        # Calcular stake
        banca_atual = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000.0)))
        modo = st.session_state.get('modo_gestao_banca', 'fracionario')
        stake_calc = calcular_stake_recomendado(banca_atual, prob, float(odd_atual), modo)
        stake_info = f"Stake: R$ {stake_calc['valor']:.2f} ({stake_calc['porcentagem']}%) Kelly"

        # Mapear saída
        saida_map = {
            'segurar': 'Segurar até o final',
            'cashout_parcial': 'Cashout parcial se green parcial',
            'cashout_total': 'Cashout total assim que puder'
        }
        saida_final = saida_map.get(saida, 'Segurar até o final')

        return {
            'aprovado': prob >= 55,
            'probabilidade': prob,
            'consenso': f"{'✅' if '3/3' in consenso else '⚖️' if '2/3' in consenso else '⚠️'} {consenso} {'Favorável' if '3' in consenso.split('/')[0] else 'Dividido'}",
            'historico_pessoal': hist_txt,
            'stake_info': stake_info,
            'saida': saida_final,
            'intensidade': intensidade,
            'resumo': resumo
        }

    except Exception as e:
        if '429' in str(e) or 'quota' in str(e).lower():
            st.session_state['ia_bloqueada_ate'] = get_time_br() + timedelta(minutes=5)

        # Fallback: retorna análise básica sem IA
        banca_atual = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000.0)))
        modo = st.session_state.get('modo_gestao_banca', 'fracionario')
        stake_calc = calcular_stake_recomendado(banca_atual, 70, float(odd_atual or 1.5), modo)

        return {
            'aprovado': True,
            'probabilidade': 70,
            'consenso': '?/? (sem IA)',
            'historico_pessoal': hist_txt,
            'stake_info': f"Stake: R$ {stake_calc['valor']:.2f} ({stake_calc['porcentagem']}%) Kelly",
            'saida': 'Segurar até o final',
            'intensidade': intensidade,
            'resumo': 'IA indisponível, usando dados estatísticos'
        }


# ==============================================================================
# 8. [FUSÃO] GESTÃO DE BANCA AUTOMÁTICA
# ==============================================================================

def inicializar_banca():
    if 'banca_atual' not in st.session_state:
        st.session_state['banca_atual'] = float(st.session_state.get('banca_inicial', 1000.0))
    if 'modo_gestao_banca' not in st.session_state:
        st.session_state['modo_gestao_banca'] = 'fracionario'
    if 'historico_banca' not in st.session_state:
        st.session_state['historico_banca'] = []

def atualizar_banca_green(odd_entrada, stake_valor):
    """Atualiza banca após GREEN."""
    try:
        lucro = round(float(stake_valor) * (float(odd_entrada) - 1), 2)
        st.session_state['banca_atual'] = round(float(st.session_state['banca_atual']) + lucro, 2)
        st.session_state['historico_banca'].append({
            'tipo': 'GREEN', 'valor': lucro, 'banca': st.session_state['banca_atual'],
            'data': get_time_br().strftime('%Y-%m-%d %H:%M')
        })
        return lucro
    except: return 0.0

def atualizar_banca_red(stake_valor):
    """Atualiza banca após RED."""
    try:
        perda = round(float(stake_valor), 2)
        st.session_state['banca_atual'] = round(float(st.session_state['banca_atual']) - perda, 2)
        st.session_state['historico_banca'].append({
            'tipo': 'RED', 'valor': -perda, 'banca': st.session_state['banca_atual'],
            'data': get_time_br().strftime('%Y-%m-%d %H:%M')
        })
        return perda
    except: return 0.0


# ==============================================================================
# 9. [FUSÃO] ANTI-CORRELAÇÃO PARA MÚLTIPLAS
# ==============================================================================

def filtrar_multiplas_nao_correlacionadas(lista_jogos, max_mesma_liga=1, min_intervalo_min=30):
    """Filtra jogos para múltiplas evitando correlação (mesma liga, horários próximos)."""
    if not lista_jogos: return []
    aprovados = []
    ligas_usadas = {}
    horarios_usados = []

    for jogo in lista_jogos:
        liga_id = jogo.get('league_id', 0)
        horario = jogo.get('horario_dt', None)

        # Check limite por liga
        if liga_id in ligas_usadas and ligas_usadas[liga_id] >= max_mesma_liga:
            continue

        # Check intervalo mínimo
        muito_proximo = False
        if horario and horarios_usados:
            for h_usado in horarios_usados:
                try:
                    diff = abs((horario - h_usado).total_seconds()) / 60
                    if diff < min_intervalo_min:
                        muito_proximo = True; break
                except: pass
        if muito_proximo: continue

        aprovados.append(jogo)
        ligas_usadas[liga_id] = ligas_usadas.get(liga_id, 0) + 1
        if horario: horarios_usados.append(horario)

    return aprovados


# ==============================================================================
# 10. BUSCAR DADOS LIVE E ESTATÍSTICAS
# ==============================================================================

def buscar_jogos_ao_vivo(api_key):
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"live": "all"}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        data = r.json()
        return data.get('response', [])
    except: return []

def buscar_estatisticas(api_key, fixture_id):
    try:
        url = "https://v3.football.api-sports.io/fixtures/statistics"
        params = {"fixture": fixture_id}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        data = r.json()
        return data.get('response', [])
    except: return []

def buscar_eventos(api_key, fixture_id):
    try:
        url = "https://v3.football.api-sports.io/fixtures/events"
        params = {"fixture": fixture_id}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        return r.json().get('response', [])
    except: return []

def buscar_odds_live(api_key, fixture_id):
    try:
        url = "https://v3.football.api-sports.io/odds/live"
        params = {"fixture": fixture_id}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        data = r.json()
        if data.get('response'):
            return data['response'][0] if data['response'] else {}
        return {}
    except: return {}

def extrair_stat(stats_list, stat_name, team_index=0):
    try:
        if not stats_list or len(stats_list) <= team_index: return 0
        team_stats = stats_list[team_index].get('statistics', [])
        for s in team_stats:
            if s['type'] == stat_name:
                val = s['value']
                if val is None: return 0
                if isinstance(val, str) and '%' in val: return float(val.replace('%', ''))
                return float(val)
        return 0
    except: return 0

def extrair_odd_live(odds_data, mercado_id=1, selecao='Over 0.5'):
    try:
        if not odds_data: return 0.0
        odds_list = odds_data.get('odds', [])
        for mercado in odds_list:
            if mercado.get('id') == mercado_id:
                for val in mercado.get('values', []):
                    if str(selecao).lower() in str(val.get('value', '')).lower():
                        return float(val.get('odd', 0))
        return 0.0
    except: return 0.0

def buscar_form_recente(api_key, team_id, ultimos=10):
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"team": team_id, "last": ultimos, "status": "FT"}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        jogos = r.json().get('response', [])
        total_gols, overs, unders, vitorias = 0, 0, 0, 0
        for j in jogos:
            gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
            total_gols += (gh + ga)
            if (gh + ga) > 1.5: overs += 1
            else: unders += 1
            hid = j['teams']['home']['id']
            if hid == int(team_id):
                if gh > ga: vitorias += 1
            else:
                if ga > gh: vitorias += 1
        n = len(jogos) if jogos else 1
        return {
            'media_gols': round(total_gols / n, 1),
            'pct_over': int((overs / n) * 100),
            'pct_under': int((unders / n) * 100),
            'pct_vitorias': int((vitorias / n) * 100),
            'total_jogos': n
        }
    except: return {'media_gols': 0, 'pct_over': 0, 'pct_under': 0, 'pct_vitorias': 0, 'total_jogos': 0}

def buscar_tabela_liga(api_key, league_id, season=None):
    try:
        if not season:
            agora = get_time_br()
            season = agora.year if agora.month >= 7 else agora.year - 1
        url = "https://v3.football.api-sports.io/standings"
        params = {"league": league_id, "season": season}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        data = r.json()
        if data.get('response'):
            standings = data['response'][0]['league']['standings']
            if standings:
                return standings[0]
        return []
    except: return []

def obter_posicao_tabela(tabela, team_id):
    if not tabela: return None
    for entry in tabela:
        if str(entry.get('team', {}).get('id', '')) == str(team_id):
            return entry
    return None


# ==============================================================================
# 11. [FUSÃO] ESTRATÉGIAS LIVE — DETECÇÃO (TODAS ENVIAM, NENHUMA VETADA)
# ==============================================================================

def analisar_estrategias_live(api_key, jogo, stats, odds_live, form_home=None, form_away=None):
    """
    Analisa um jogo ao vivo e retorna TODAS as estratégias aplicáveis.
    NUNCA veta. IA é informativa apenas.
    """
    estrategias_detectadas = []

    fid = jogo['fixture']['id']
    status = jogo['fixture']['status']['short']
    elapsed = jogo['fixture']['status'].get('elapsed', 0) or 0
    home_name = jogo['teams']['home']['name']
    away_name = jogo['teams']['away']['name']
    home_id = jogo['teams']['home']['id']
    away_id = jogo['teams']['away']['id']
    goals_home = jogo['goals']['home'] or 0
    goals_away = jogo['goals']['away'] or 0
    placar = f"{goals_home}x{goals_away}"
    total_gols = goals_home + goals_away
    liga_nome = jogo['league']['name']
    liga_id = jogo['league']['id']

    # Verificar se liga está na blacklist
    df_black = st.session_state.get('df_black', pd.DataFrame())
    if not df_black.empty and str(liga_id) in df_black['id'].values:
        return []

    # Extrair stats
    chutes_home = extrair_stat(stats, 'Total Shots', 0)
    chutes_away = extrair_stat(stats, 'Total Shots', 1)
    chutes_gol_home = extrair_stat(stats, 'Shots on Goal', 0)
    chutes_gol_away = extrair_stat(stats, 'Shots on Goal', 1)
    posse_home = extrair_stat(stats, 'Ball Possession', 0)
    posse_away = extrair_stat(stats, 'Ball Possession', 1)
    corners_home = extrair_stat(stats, 'Corner Kicks', 0)
    corners_away = extrair_stat(stats, 'Corner Kicks', 1)
    ataques_home = extrair_stat(stats, 'Dangerous Attacks', 0)
    ataques_away = extrair_stat(stats, 'Dangerous Attacks', 1)
    bloqueios_home = extrair_stat(stats, 'Blocked Shots', 0)
    bloqueios_away = extrair_stat(stats, 'Blocked Shots', 1)
    faltas_home = extrair_stat(stats, 'Fouls', 0)
    faltas_away = extrair_stat(stats, 'Fouls', 1)
    cartoes_home = extrair_stat(stats, 'Yellow Cards', 0)
    cartoes_away = extrair_stat(stats, 'Yellow Cards', 1)
    defesas_home = extrair_stat(stats, 'Goalkeeper Saves', 0)
    defesas_away = extrair_stat(stats, 'Goalkeeper Saves', 1)

    chutes_total = chutes_home + chutes_away
    chutes_gol_total = chutes_gol_home + chutes_gol_away
    ataques_total = ataques_home + ataques_away

    # Form data
    pct_over_home = form_home.get('pct_over', 50) if form_home else 50
    pct_over_away = form_away.get('pct_over', 50) if form_away else 50
    rating_home = form_home.get('media_gols', 0) if form_home else 0
    rating_away = form_away.get('media_gols', 0) if form_away else 0

    # Dados stats para mensagem
    dados_stats_base = {
        'rating_home': f"{rating_home:.1f}" if rating_home else "",
        'rating_away': f"{rating_away:.1f}" if rating_away else "",
        'pct_over_home': pct_over_home,
        'pct_over_away': pct_over_away,
        'chutes_total': chutes_total,
        'chutes_gol': chutes_gol_total
    }

    # Buscar odd live de referência
    odd_next_goal = extrair_odd_live(odds_live, 1, 'Over')
    if odd_next_goal <= 0: odd_next_goal = 1.50

    # Objeto base para sinal
    def criar_sinal(nome_estrategia, confianca, analise_curta, odd_ref=None):
        return {
            'estrategia': nome_estrategia,
            'fid': fid,
            'liga': liga_nome,
            'liga_id': liga_id,
            'home': home_name,
            'away': away_name,
            'home_id': home_id,
            'away_id': away_id,
            'minuto': elapsed,
            'placar': placar,
            'goals_home': goals_home,
            'goals_away': goals_away,
            'total_gols': total_gols,
            'odd': odd_ref or odd_next_goal,
            'confianca': confianca,
            'dados_stats': {**dados_stats_base, 'analise_curta': analise_curta}
        }

    # ════════════════════════════════════════════
    # ESTRATÉGIAS OVER (sai gol)
    # ════════════════════════════════════════════

    # ⚡ Gol Relâmpago (0-20 min, 0x0, muita pressão)
    if status in ['1H'] and 5 <= elapsed <= 20 and total_gols == 0:
        if chutes_total >= 6 and chutes_gol_total >= 2 and ataques_total >= 15:
            confianca = min(95, 60 + chutes_gol_total * 5 + (ataques_total - 15))
            estrategias_detectadas.append(criar_sinal(
                "⚡ Gol Relâmpago", confianca,
                f"Ataque: Início Intenso | {chutes_total} chutes ({chutes_gol_total} no gol)"
            ))

    # 🟣 Porteira Aberta (1T, pelo menos 1 gol, pressão)
    if status in ['1H'] and 15 <= elapsed <= 40 and total_gols >= 1:
        if chutes_total >= 8 and chutes_gol_total >= 3:
            confianca = min(95, 55 + total_gols * 10 + chutes_gol_total * 3)
            estrategias_detectadas.append(criar_sinal(
                "🟣 Porteira Aberta", confianca,
                f"Jogo aberto: {total_gols} gol(s) | {chutes_gol_total} chutes no gol"
            ))

    # 💰 Janela de Ouro (HT, 0x0, stats agressivos)
    if status in ['HT'] and total_gols == 0:
        if chutes_total >= 10 and chutes_gol_total >= 3:
            confianca = min(95, 60 + chutes_gol_total * 4 + (chutes_total - 10) * 2)
            estrategias_detectadas.append(criar_sinal(
                "💰 Janela de Ouro", confianca,
                f"HT 0x0 mas {chutes_total} chutes ({chutes_gol_total} no gol) — pressão reprimida"
            ))

    # 🟢 Blitz Casa (45-70, casa domina)
    if status in ['2H'] and 45 <= elapsed <= 70 and total_gols == 0:
        if chutes_home >= 7 and posse_home >= 60 and ataques_home >= 20:
            confianca = min(95, 55 + (chutes_home - 7) * 3 + (posse_home - 60))
            estrategias_detectadas.append(criar_sinal(
                "🟢 Blitz Casa", confianca,
                f"Casa domina: {posse_home}% posse | {chutes_home} chutes"
            ))

    # 🟢 Blitz Visitante (45-70, visitante domina)
    if status in ['2H'] and 45 <= elapsed <= 70 and total_gols == 0:
        if chutes_away >= 7 and posse_away >= 55 and ataques_away >= 18:
            confianca = min(95, 55 + (chutes_away - 7) * 3 + (posse_away - 55))
            estrategias_detectadas.append(criar_sinal(
                "🟢 Blitz Visitante", confianca,
                f"Visitante domina: {posse_away}% posse | {chutes_away} chutes"
            ))

    # 🔥 Massacre (grande diferença, domínio absoluto)
    if status in ['1H'] and 10 <= elapsed <= 35:
        if chutes_home >= 10 and chutes_away <= 2 and ataques_home >= 25:
            confianca = min(95, 70 + (chutes_home - 10) * 2)
            estrategias_detectadas.append(criar_sinal(
                "🔥 Massacre", confianca,
                f"Domínio total Casa: {chutes_home}x{chutes_away} chutes"
            ))
        elif chutes_away >= 10 and chutes_home <= 2 and ataques_away >= 25:
            confianca = min(95, 70 + (chutes_away - 10) * 2)
            estrategias_detectadas.append(criar_sinal(
                "🔥 Massacre", confianca,
                f"Domínio total Fora: {chutes_away}x{chutes_home} chutes"
            ))

    # ⚔️ Choque de Líderes (ambos atacando, jogo aberto)
    if 15 <= elapsed <= 40 and total_gols <= 1:
        if chutes_home >= 5 and chutes_away >= 5 and ataques_total >= 30:
            confianca = min(95, 55 + (ataques_total - 30) + chutes_gol_total * 3)
            estrategias_detectadas.append(criar_sinal(
                "⚔️ Choque Líderes", confianca,
                f"Jogo aberto: {chutes_home}x{chutes_away} chutes | {ataques_total} ataques"
            ))

    # 🥊 Briga de Rua (jogo violento, muitas faltas)
    if 15 <= elapsed <= 45 and total_gols <= 1:
        if faltas_home + faltas_away >= 15 and chutes_total >= 8:
            confianca = min(90, 55 + (faltas_home + faltas_away - 15) * 2 + chutes_gol_total * 3)
            estrategias_detectadas.append(criar_sinal(
                "🥊 Briga de Rua", confianca,
                f"Jogo quente: {faltas_home + faltas_away} faltas | {chutes_total} chutes"
            ))

    # 🏹 Tiroteio Elite (muitos chutes no gol)
    if 20 <= elapsed <= 60 and total_gols <= 1:
        if chutes_gol_total >= 6 and chutes_total >= 12:
            confianca = min(95, 60 + chutes_gol_total * 4)
            estrategias_detectadas.append(criar_sinal(
                "🏹 Tiroteio Elite", confianca,
                f"Tiroteio: {chutes_gol_total} chutes NO GOL de {chutes_total}"
            ))

    # 💎 GOLDEN BET (jogo avançado, muita pressão, sem gols)
    if status in ['2H'] and 55 <= elapsed <= 75 and total_gols == 0:
        if chutes_total >= 14 and chutes_gol_total >= 5:
            confianca = min(95, 65 + chutes_gol_total * 3 + (chutes_total - 14) * 2)
            estrategias_detectadas.append(criar_sinal(
                "💎 GOLDEN BET", confianca,
                f"Pressão absurda: {chutes_total} chutes ({chutes_gol_total} no gol) em {elapsed}'"
            ))

    # 💎 Sniper Final (últimos minutos, pressão)
    if status in ['2H'] and elapsed >= 75 and total_gols <= 1:
        if chutes_gol_total >= 4 and ataques_total >= 20:
            confianca = min(90, 55 + (elapsed - 75) * 2 + chutes_gol_total * 3)
            estrategias_detectadas.append(criar_sinal(
                "💎 Sniper Final", confianca,
                f"Pressão final: {chutes_gol_total} no gol | {elapsed}'"
            ))

    # ⚡ Contra-Ataque Letal (time perdendo ataca muito)
    if 30 <= elapsed <= 80 and goals_home != goals_away:
        time_perdendo = 'home' if goals_home < goals_away else 'away'
        ch_perdendo = chutes_home if time_perdendo == 'home' else chutes_away
        at_perdendo = ataques_home if time_perdendo == 'home' else ataques_away
        if ch_perdendo >= 6 and at_perdendo >= 15:
            confianca = min(90, 55 + (ch_perdendo - 6) * 3 + (at_perdendo - 15))
            nome_perdendo = home_name if time_perdendo == 'home' else away_name
            estrategias_detectadas.append(criar_sinal(
                "⚡ Contra-Ataque Letal", confianca,
                f"{nome_perdendo} pressiona: {ch_perdendo} chutes"
            ))

    # 🔫 Lay Goleada (time levando goleada, honra)
    if elapsed >= 50 and abs(goals_home - goals_away) >= 3:
        time_perdendo_id = 'home' if goals_home < goals_away else 'away'
        ch_perdendo = chutes_home if time_perdendo_id == 'home' else chutes_away
        if ch_perdendo >= 3:
            confianca = min(85, 55 + ch_perdendo * 3)
            estrategias_detectadas.append(criar_sinal(
                "🔫 Lay Goleada", confianca,
                f"Goleada: {placar} | Time perdendo ainda chuta ({ch_perdendo})"
            ))

    # 🦁 Back Favorito Nettuno (favorito domina 2T)
    if status in ['2H'] and 50 <= elapsed <= 70:
        if goals_home > goals_away and chutes_home >= 8 and posse_home >= 58:
            confianca = min(90, 60 + (posse_home - 58) + chutes_home * 2)
            estrategias_detectadas.append(criar_sinal(
                "🦁 Back Favorito (Nettuno)", confianca,
                f"Favorito controla: {posse_home}% posse | {goals_home}x{goals_away}"
            ))
        elif goals_away > goals_home and chutes_away >= 8 and posse_away >= 55:
            confianca = min(90, 60 + (posse_away - 55) + chutes_away * 2)
            estrategias_detectadas.append(criar_sinal(
                "🦁 Back Favorito (Nettuno)", confianca,
                f"Visitante controla: {posse_away}% posse | {goals_away}x{goals_home}"
            ))

    # ════════════════════════════════════════════
    # ESTRATÉGIAS UNDER (não sai gol)
    # ════════════════════════════════════════════

    # ❄️ Jogo Morno (sem gols, jogo parado)
    if elapsed >= 55 and total_gols == 0:
        if chutes_gol_total <= 3 and ataques_total <= 20:
            confianca = min(95, 60 + (elapsed - 55) * 2 + (3 - chutes_gol_total) * 5)
            estrategias_detectadas.append(criar_sinal(
                "❄️ Jogo Morno", confianca,
                f"Jogo Travado | {chutes_total} Chutes ({chutes_gol_total} no gol)"
            ))

    # 🧊 Arame Liso (parece ter pressão mas não converte)
    if elapsed >= 50 and total_gols == 0:
        if chutes_total >= 10 and chutes_gol_total <= 2 and bloqueios_home + bloqueios_away >= 5:
            confianca = min(90, 55 + (bloqueios_home + bloqueios_away - 5) * 3 + (elapsed - 50))
            estrategias_detectadas.append(criar_sinal(
                "🧊 Arame Liso", confianca,
                f"Falsa pressão: {chutes_total} chutes mas só {chutes_gol_total} no gol | 🛡️ {bloqueios_home + bloqueios_away} Bloqueios"
            ))

    # ════════════════════════════════════════════
    # ESTRATÉGIA RESULTADO (manter)
    # ════════════════════════════════════════════

    # 👴 Estratégia do Vovô (time ganha final de jogo)
    if elapsed >= 78 and goals_home != goals_away:
        time_ganhando = 'home' if goals_home > goals_away else 'away'
        posse_ganhando = posse_home if time_ganhando == 'home' else posse_away
        diff = abs(goals_home - goals_away)
        if diff >= 1:
            confianca = min(95, 65 + diff * 8 + (elapsed - 78) * 2)
            estrategias_detectadas.append(criar_sinal(
                "👴 Estratégia do Vovô", confianca,
                f"Controle Total | Posse {posse_ganhando}%"
            ))

    return estrategias_detectadas


# ==============================================================================
# 12. [FUSÃO] CHECK GREEN/RED — CORRIGIDO POR CLASSIFICAÇÃO
# ==============================================================================

def check_green_red_hibrido(sinal, jogo_atual):
    """
    Verifica se sinal deu GREEN ou RED usando classificação correta.
    OVER → GREEN se saiu gol após sinal
    UNDER → GREEN se NÃO saiu gol
    RESULTADO → GREEN se resultado manteve
    """
    estrategia = str(sinal.get('Estrategia', ''))
    tipo = classificar_tipo_estrategia(estrategia)

    status = jogo_atual['fixture']['status']['short']
    elapsed = jogo_atual['fixture']['status'].get('elapsed', 0) or 0
    goals_home = jogo_atual['goals']['home'] or 0
    goals_away = jogo_atual['goals']['away'] or 0
    placar_atual = f"{goals_home}x{goals_away}"
    total_gols_atual = goals_home + goals_away

    # Placar no momento do sinal
    try:
        placar_sinal = str(sinal.get('Placar_Sinal', '0x0'))
        gh_sinal, ga_sinal = map(int, placar_sinal.lower().replace(' ', '').split('x'))
        total_gols_sinal = gh_sinal + ga_sinal
    except:
        gh_sinal, ga_sinal = 0, 0
        total_gols_sinal = 0

    # Jogo ainda em andamento - aguardar
    if status not in ['FT', 'AET', 'PEN', 'INT', 'ABD', 'AWD', 'WO']:
        # Verificar green parcial (antes do fim)
        if tipo == 'OVER':
            if total_gols_atual > total_gols_sinal:
                return 'GREEN', placar_atual, f"✅ GOL! {placar_atual}"
        elif tipo == 'UNDER':
            # Under só confirma no final, mas se saiu gol já é RED
            if total_gols_atual > total_gols_sinal:
                return 'RED', placar_atual, f"❌ Saiu gol: {placar_atual}"
        elif tipo == 'RESULTADO':
            # Vovô: se time que ganhava agora empata ou perde = RED imediato
            if gh_sinal > ga_sinal:  # Casa ganhava
                if goals_home <= goals_away:
                    return 'RED', placar_atual, f"❌ Virada/Empate: {placar_atual}"
            elif ga_sinal > gh_sinal:  # Fora ganhava
                if goals_away <= goals_home:
                    return 'RED', placar_atual, f"❌ Virada/Empate: {placar_atual}"
        return 'PENDENTE', placar_atual, "⏳ Aguardando..."

    # ════════════════════════════════════════════
    # JOGO FINALIZADO — Resultado Definitivo
    # ════════════════════════════════════════════

    if tipo == 'OVER':
        if total_gols_atual > total_gols_sinal:
            return 'GREEN', placar_atual, f"✅ GREEN! Saiu gol: {placar_atual}"
        else:
            return 'RED', placar_atual, f"❌ RED. Sem gols: {placar_atual}"

    elif tipo == 'UNDER':
        if total_gols_atual == total_gols_sinal:
            return 'GREEN', placar_atual, f"✅ GREEN! Sem gols: {placar_atual}"
        else:
            return 'RED', placar_atual, f"❌ RED. Saiu gol: {placar_atual}"

    elif tipo == 'RESULTADO':
        # Vovô: verificar se time que ganhava manteve
        if gh_sinal > ga_sinal:  # Casa ganhava
            if goals_home > goals_away:
                return 'GREEN', placar_atual, f"✅ GREEN! Vitória mantida: {placar_atual}"
            else:
                return 'RED', placar_atual, f"❌ RED. Não manteve: {placar_atual}"
        elif ga_sinal > gh_sinal:  # Fora ganhava
            if goals_away > goals_home:
                return 'GREEN', placar_atual, f"✅ GREEN! Vitória mantida: {placar_atual}"
            else:
                return 'RED', placar_atual, f"❌ RED. Não manteve: {placar_atual}"
        else:
            # Empate no sinal - caso raro
            return 'RED', placar_atual, f"❌ RED. Sem mudança: {placar_atual}"

    # Fallback genérico
    if total_gols_atual > total_gols_sinal:
        return 'GREEN', placar_atual, f"✅ GREEN: {placar_atual}"
    return 'RED', placar_atual, f"❌ RED: {placar_atual}"


def processar_green_red(api_key, token, chat_id):
    """Verifica todos os sinais pendentes e envia GREEN/RED."""
    sinais_hoje = st.session_state.get('historico_sinais', [])
    if not sinais_hoje: return

    jogos_live = buscar_jogos_ao_vivo(api_key)
    mapa_live = {}
    for j in jogos_live:
        mapa_live[str(j['fixture']['id'])] = j

    # Buscar jogos finalizados do dia também
    hoje_str = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje_str, "timezone": "America/Sao_Paulo"}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        for j in r.json().get('response', []):
            fid_str = str(j['fixture']['id'])
            if fid_str not in mapa_live:
                mapa_live[fid_str] = j
    except: pass

    alterou = False
    for sinal in sinais_hoje:
        resultado_atual = str(sinal.get('Resultado', ''))
        if 'GREEN' in resultado_atual or 'RED' in resultado_atual:
            continue  # Já resolvido

        fid = clean_fid(sinal.get('FID', '0'))
        if fid not in mapa_live:
            continue  # Jogo não encontrado

        jogo = mapa_live[fid]
        resultado, placar, motivo = check_green_red_hibrido(sinal, jogo)

        if resultado == 'PENDENTE':
            continue

        estrategia = sinal.get('Estrategia', '')
        home = jogo['teams']['home']['name']
        away = jogo['teams']['away']['name']

        # Atualizar resultado
        sinal['Resultado'] = resultado
        sinal['Odd_Atualizada'] = placar
        alterou = True

        # Chave anti-duplicata
        chave_resultado = gerar_chave_universal(fid, estrategia, resultado)
        if chave_resultado in st.session_state['alertas_enviados']:
            continue
        st.session_state['alertas_enviados'].add(chave_resultado)

        # Calcular stake e atualizar banca
        try:
            odd_entrada = float(str(sinal.get('Odd', '1.50')).replace(',', '.'))
            banca_atual = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000.0)))
            modo = st.session_state.get('modo_gestao_banca', 'fracionario')
            prob_str = str(sinal.get('Probabilidade', '70')).replace('%', '')
            prob_val = float(prob_str) if prob_str.replace('.', '', 1).isdigit() else 70.0
            stake_calc = calcular_stake_recomendado(banca_atual, prob_val, odd_entrada, modo)
            stake_valor = stake_calc['valor']
        except:
            stake_valor = float(st.session_state.get('stake_padrao', 10.0))
            odd_entrada = 1.50

        if resultado == 'GREEN':
            lucro = atualizar_banca_green(odd_entrada, stake_valor)
            msg = formatar_green_compacto(estrategia, home, away, placar, lucro, st.session_state['banca_atual'])
            enviar_telegram(token, chat_id, msg)
        elif resultado == 'RED':
            perda = atualizar_banca_red(stake_valor)
            msg = formatar_red_compacto(estrategia, home, away, placar, perda, st.session_state['banca_atual'])
            enviar_telegram(token, chat_id, msg)

    if alterou:
        atualizar_historico_ram(sinais_hoje)
        st.session_state['precisa_salvar'] = True


# ==============================================================================
# 13. [FUSÃO] DETECÇÃO DE VAR
# ==============================================================================

def verificar_var(api_key, token, chat_id):
    """Detecta gols anulados por VAR."""
    jogos_live = buscar_jogos_ao_vivo(api_key)
    for jogo in jogos_live:
        fid = str(jogo['fixture']['id'])
        goals_home = jogo['goals']['home'] or 0
        goals_away = jogo['goals']['away'] or 0
        placar = f"{goals_home}x{goals_away}"

        # Verificar se temos sinal neste jogo
        sinais_hoje = st.session_state.get('historico_sinais', [])
        tem_sinal = any(clean_fid(s.get('FID', '0')) == fid for s in sinais_hoje)
        if not tem_sinal:
            continue

        # Buscar eventos do jogo
        try:
            eventos = buscar_eventos(api_key, fid)
            for ev in eventos:
                if ev.get('detail') and 'Goal Disallowed' in str(ev.get('detail', '')):
                    chave_var = f"VAR_{fid}_{ev.get('time', {}).get('elapsed', 0)}"
                    if chave_var not in st.session_state['var_avisado_cache']:
                        st.session_state['var_avisado_cache'].add(chave_var)
                        home = jogo['teams']['home']['name']
                        away = jogo['teams']['away']['name']
                        msg = formatar_var_alerta(home, away, placar)
                        enviar_telegram(token, chat_id, msg)
        except: pass


# ==============================================================================
# 14. [FUSÃO] ANÁLISE MATINAL — SNIPER + MÚLTIPLA + ALTERNATIVOS + ALAVANCAGEM
# ==============================================================================

def executar_matinal(api_key, token, chat_id):
    """Executa análise matinal completa e envia todos os relatórios."""
    if st.session_state.get('matinal_enviado', False): return
    agora = get_time_br()
    if not (7 <= agora.hour < 12): return

    hoje_str = agora.strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje_str, "timezone": "America/Sao_Paulo"}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        jogos = r.json().get('response', [])
    except: return

    if not jogos: return

    # Filtrar ligas não-blacklist
    df_black = st.session_state.get('df_black', pd.DataFrame())
    blacklist_ids = set(df_black['id'].values) if not df_black.empty else set()

    jogos_validos = []
    for j in jogos:
        lid = str(j['league']['id'])
        if lid in blacklist_ids: continue
        if j['fixture']['status']['short'] != 'NS': continue
        jogos_validos.append(j)

    if not jogos_validos: return

    # ════════════════════════════════════════════
    # 1. SNIPER MATINAL
    # ════════════════════════════════════════════
    msg_sniper = "🌅 <b>SNIPER MATINAL (IA + DADOS)</b>\n\n"
    zonas = {'over': [], 'under': [], 'match': [], 'cartoes': [], 'escanteios': [], 'defesas': []}
    jogos_analisados = 0
    MAX_MATINAL = 15

    for j in jogos_validos[:MAX_MATINAL]:
        fid = j['fixture']['id']
        home = j['teams']['home']['name']
        away = j['teams']['away']['name']
        liga = j['league']['name']
        home_id = j['teams']['home']['id']
        away_id = j['teams']['away']['id']

        # Buscar form
        form_h = buscar_form_recente(api_key, home_id, 10)
        form_a = buscar_form_recente(api_key, away_id, 10)

        # Buscar odd pre-match (opcional, não bloqueia se 0)
        odd_val, odd_desc = buscar_odd_pre_match(api_key, fid)

        # Classificar por zona
        media_gols_combinada = (form_h.get('media_gols', 0) + form_a.get('media_gols', 0)) / 2
        pct_over_combinado = (form_h.get('pct_over', 50) + form_a.get('pct_over', 50)) / 2
        pct_under_combinado = (form_h.get('pct_under', 50) + form_a.get('pct_under', 50)) / 2

        entrada_base = {
            'fid': fid, 'home': home, 'away': away, 'liga': liga,
            'home_id': home_id, 'away_id': away_id,
            'odd': odd_val, 'odd_desc': odd_desc,
            'form_h': form_h, 'form_a': form_a,
            'media_gols': media_gols_combinada,
            'pct_over': pct_over_combinado,
            'hora': j['fixture']['date'][11:16],
            'league_id': j['league']['id'],
            'horario_dt': None
        }

        try:
            entrada_base['horario_dt'] = datetime.fromisoformat(j['fixture']['date'].replace('Z', '+00:00'))
        except: pass

        # ZONA OVER
        if pct_over_combinado >= 60 and media_gols_combinada >= 2.0:
            if media_gols_combinada >= 2.5:
                entrada_base['palpite'] = 'MAIS de 2.5 Gols'
            else:
                entrada_base['palpite'] = 'Ambas Marcam (Sim)'
            entrada_base['motivo'] = f"{int(pct_over_combinado)}% Over nos últimos jogos (ambos). Média {media_gols_combinada:.1f} gols."
            zonas['over'].append(entrada_base.copy())

        # ZONA UNDER
        if pct_under_combinado >= 55 and media_gols_combinada <= 2.0:
            entrada_base['palpite'] = 'MENOS de 2.5 Gols'
            entrada_base['motivo'] = f"Under {int(pct_under_combinado)}% (últimos jogos). Padrão defensivo."
            zonas['under'].append(entrada_base.copy())

        # ZONA MATCH ODDS
        if form_h.get('pct_vitorias', 0) >= 70:
            entrada_base['palpite'] = f'Vitória do {home}'
            entrada_base['motivo'] = f"{form_h['pct_vitorias']}% vitórias em casa."
            zonas['match'].append(entrada_base.copy())
        elif form_a.get('pct_vitorias', 0) >= 70:
            entrada_base['palpite'] = f'Vitória do {away}'
            entrada_base['motivo'] = f"{form_a['pct_vitorias']}% vitórias fora."
            zonas['match'].append(entrada_base.copy())

        jogos_analisados += 1

    # Montar mensagem por zona
    if zonas['over']:
        msg_sniper += "🔥 <b>ZONA DE GOLS (OVER)</b>\n\n"
        for z in zonas['over'][:5]:
            odd_txt = f"💰 Ref: @{z['odd']:.2f} ({z['odd_desc']})\n" if z['odd'] > 0 else ""
            msg_sniper += f"⚽ Jogo: {z['home']} x {z['away']}\n🏆 Liga: {z['liga']}\n🎯 Palpite: {z['palpite']}\n{odd_txt}📝 Motivo: {z['motivo']}\n\n"
        msg_sniper += "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    if zonas['under']:
        msg_sniper += "❄️ <b>ZONA DE TRINCHEIRA (UNDER)</b>\n\n"
        for z in zonas['under'][:3]:
            odd_txt = f"💰 Ref: @{z['odd']:.2f}\n" if z['odd'] > 0 else ""
            msg_sniper += f"⚽ Jogo: {z['home']} x {z['away']}\n🏆 Liga: {z['liga']}\n🎯 Palpite: {z['palpite']}\n{odd_txt}📝 Motivo: {z['motivo']}\n\n"
        msg_sniper += "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    if zonas['match']:
        msg_sniper += "🏆 <b>ZONA DE MATCH ODDS</b>\n\n"
        for z in zonas['match'][:3]:
            odd_txt = f"💰 Ref: @{z['odd']:.2f}\n" if z['odd'] > 0 else ""
            msg_sniper += f"⚽ Jogo: {z['home']} x {z['away']}\n🏆 Liga: {z['liga']}\n🎯 Palpite: {z['palpite']}\n{odd_txt}📝 Motivo: {z['motivo']}\n\n"
        msg_sniper += "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    # IA Compacta por jogo top
    top_jogos = (zonas['over'] + zonas['match'])[:3]
    if top_jogos and IA_ATIVADA:
        msg_sniper += "🧠 <b>IA COMPACTA (Destaques)</b>\n\n"
        for tj in top_jogos:
            try:
                hist_pessoal = [s for s in st.session_state.get('historico_sinais', [])
                               if s.get('HomeID') == str(tj.get('home_id', ''))]
                dados_ia = consultar_ia_gemini(
                    tj['palpite'], tj['liga'], tj['home'], tj['away'], 0,
                    "0x0", tj.get('odd', 1.50), None, hist_pessoal
                )
                msg_sniper += f"⚽ {tj['home']} x {tj['away']}\n"
                msg_sniper += f"├─ 📊 Over {int(tj['pct_over'])}% | Média {tj['media_gols']:.1f} gols\n"
                msg_sniper += f"├─ 🗳️ {dados_ia.get('consenso', '?/?')}\n"
                msg_sniper += f"├─ 💰 {dados_ia.get('stake_info', '')}\n"
                msg_sniper += f"└─ 🔮 Decisão: {'⭐ ENTRAR AGORA' if dados_ia.get('probabilidade', 0) >= 70 else '⏳ Avaliar'} ({dados_ia.get('probabilidade', '?')} pts)\n\n"
            except: pass

    if jogos_analisados > 0:
        enviar_telegram(token, chat_id, msg_sniper)

    # ════════════════════════════════════════════
    # 2. MÚLTIPLA DE SEGURANÇA
    # ════════════════════════════════════════════
    if not st.session_state.get('multipla_matinal_enviada', False):
        candidatos_multipla = [z for z in zonas['over'] if z.get('pct_over', 0) >= 70]
        candidatos_filtrados = filtrar_multiplas_nao_correlacionadas(candidatos_multipla, max_mesma_liga=1, min_intervalo_min=30)

        if len(candidatos_filtrados) >= 2:
            selecionados = candidatos_filtrados[:3]
            msg_mult = "🚀 <b>MÚLTIPLA DE SEGURANÇA (IA)</b>\n\n"
            prob_combinada = 1.0
            for i, s in enumerate(selecionados, 1):
                prob_individual = min(0.92, s.get('pct_over', 70) / 100.0)
                prob_combinada *= prob_individual
                msg_mult += f"{i}️⃣ Jogo: {s['home']} x {s['away']}\n"
                msg_mult += f"🎯 Seleção: Over 0.5 Gols\n"
                msg_mult += f"📝 Motivo: {s['motivo'][:80]}\n\n"

            msg_mult += f"⚠️ Conclusão: Probabilidade combinada de {int(prob_combinada * 100)}%.\n\n"

            banca = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000.0)))
            modo = st.session_state.get('modo_gestao_banca', 'fracionario')
            stake = calcular_stake_recomendado(banca, int(prob_combinada * 100), 2.0, modo)
            msg_mult += f"🧠 IA Compacta:\n├─ 💰 Stake: R$ {stake['valor']:.2f} ({stake['porcentagem']}%) Kelly\n"
            msg_mult += f"└─ 🔮 Decisão: ⭐ ENTRAR AGORA"

            enviar_telegram(token, chat_id, msg_mult)
            st.session_state['multipla_matinal_enviada'] = True

    # ════════════════════════════════════════════
    # 3. MERCADOS ALTERNATIVOS (Cartões + Muralha)
    # ════════════════════════════════════════════
    if not st.session_state.get('alternativos_enviado', False):
        # Simplificado: usa dados de forma para identificar jogos quentes
        jogos_quentes = [z for z in zonas['over'] if z.get('media_gols', 0) >= 2.5]
        if jogos_quentes:
            jq = jogos_quentes[0]
            msg_alt = f"🟨 <b>AÇOUGUEIRO</b>\n\n"
            msg_alt += f"⚽ {jq['home']} x {jq['away']}\n\n"
            msg_alt += f"🔎 Análise:\nJogo com potencial de muitas faltas. Média alta de gols indica jogo aberto e disputado.\n\n"
            msg_alt += f"🎯 INDICAÇÃO: Mais de 3.5 Cartões na Partida\n\n"
            msg_alt += f"🧠 IA: Baseado em padrão ofensivo | 💰 Stake baixa"
            enviar_telegram(token, chat_id, msg_alt)
            st.session_state['alternativos_enviado'] = True

    # ════════════════════════════════════════════
    # 4. ALAVANCAGEM (BET BUILDER)
    # ════════════════════════════════════════════
    if not st.session_state.get('alavancagem_enviada', False):
        favoritos_fortes = [z for z in zonas['match'] if z.get('form_h', {}).get('pct_vitorias', 0) >= 75]
        if favoritos_fortes:
            fav = favoritos_fortes[0]
            msg_alav = f"💎 🚀 <b>ALAVANCAGEM</b> {fav['home']} vs {fav['away']}\n\n"
            msg_alav += f"🛠️ CRIAR APOSTA (Combinação):\n"
            msg_alav += f"✅ {fav['home']} vence\n"
            msg_alav += f"✅ Mais de 1.5 Gols {fav['home']}\n"
            msg_alav += f"✅ Mais de 3.5 Cartões\n\n"
            msg_alav += f"🧠 Motivo IA: {fav['motivo'][:100]}\n"
            msg_alav += f"⚠️ Gestão: Stake Baixa (gordura). Alvo: Odd @3.50+"
            enviar_telegram(token, chat_id, msg_alav)
            st.session_state['alavancagem_enviada'] = True

    # ════════════════════════════════════════════
    # 5. DROP ODDS CASHOUT
    # ════════════════════════════════════════════
    if not st.session_state.get('drop_enviado_12', False):
        oportunidades = scanner_drop_odds_pre_live(api_key)
        if oportunidades:
            op = oportunidades[0]
            msg_drop = f"💰 <b>ESTRATÉGIA CASHOUT (DROP ODDS)</b>\n\n"
            msg_drop += f"⚽ {op['jogo']}\n🏆 {op['liga']} | ⏰ {op['hora']}\n\n"
            msg_drop += f"📉 DESAJUSTE:\n• Bet365: @{op['odd_b365']:.2f}\n"
            msg_drop += f"• Pinnacle: @{op['odd_pinnacle']:.2f}\n"
            msg_drop += f"• Drop: {op['valor']:.1f}%\n\n"
            msg_drop += f"⚙️ AÇÃO:\n1️⃣ Compre vitória {op['lado']}\n"
            msg_drop += f"2️⃣ SAÍDA: Cashout ao igualar Pinnacle."
            enviar_telegram(token, chat_id, msg_drop)
            st.session_state['drop_enviado_12'] = True

    # ════════════════════════════════════════════
    # 6. ZONAS EXTRAS (Cartões, Escanteios, Defesas)
    # ════════════════════════════════════════════
    try:
        executar_matinal_zonas_extras(api_key, token, chat_id, jogos_validos)
    except: pass

    st.session_state['matinal_enviado'] = True


# ==============================================================================
# 15. [FUSÃO] RELATÓRIOS NOTURNOS — BI + FINANCEIRO + AJUSTE TÉCNICO
# ==============================================================================

def executar_relatorios_noite(token, chat_id):
    """Envia relatórios de fechamento do dia."""
    agora = get_time_br()
    if not (22 <= agora.hour <= 23): return

    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    hoje = agora.strftime('%Y-%m-%d')
    df_hoje = df[df['Data'] == hoje] if 'Data' in df.columns else pd.DataFrame()

    # ════════════════════════════════════════════
    # 1. RELATÓRIO BI
    # ════════════════════════════════════════════
    if not st.session_state.get('bi_enviado', False) and not df.empty:
        total, greens, reds, winrate = calcular_stats(df)

        # Golden Combos (WR >= 80% com pelo menos 5 apostas)
        golden = []
        red_zones = []
        if 'Estrategia' in df.columns and 'Liga' in df.columns:
            for (est, liga), grp in df.groupby(['Estrategia', 'Liga']):
                g = len(grp[grp['Resultado'].str.contains('GREEN', na=False)])
                r = len(grp[grp['Resultado'].str.contains('RED', na=False)])
                t = g + r
                if t >= 5:
                    wr = int((g / t) * 100)
                    if wr >= 80: golden.append({'combo': f"{est} + {liga}", 'wr': wr, 'g': g, 'r': r})
                    elif wr < 40: red_zones.append({'combo': f"{est} + {liga}", 'wr': wr, 'g': g, 'r': r})

        msg_bi = f"📊 <b>RELATÓRIO BI</b>\n\n"
        msg_bi += f"🧠 DIAGNÓSTICO:\nWinrate global {winrate:.1f}% ({total} apostas).\n\n"

        if golden:
            msg_bi += "💎 GOLDEN COMBOS (WR ≥ 80%):\n"
            for gc in golden[:3]:
                msg_bi += f"├─ {gc['combo']} → {gc['wr']}% ({gc['g']}G/{gc['r']}R)\n"
            msg_bi += "\n"

        if red_zones:
            msg_bi += "🚨 RED ZONES (WR < 40%):\n"
            for rz in red_zones[:3]:
                msg_bi += f"├─ {rz['combo']} → {rz['wr']}% ({rz['g']}G/{rz['r']}R) ⛔ PARAR\n"
            msg_bi += "\n"

        if red_zones:
            msg_bi += f"🎯 AÇÃO IMEDIATA:\nPare de operar: {red_zones[0]['combo']}."
        if golden:
            msg_bi += f"\nConcentre em: {golden[0]['combo']}."

        enviar_telegram(token, chat_id, msg_bi)
        st.session_state['bi_enviado'] = True

    # ════════════════════════════════════════════
    # 2. RELATÓRIO FINANCEIRO
    # ════════════════════════════════════════════
    if not st.session_state.get('financeiro_enviado', False) and not df_hoje.empty:
        total_h, greens_h, reds_h, winrate_h = calcular_stats(df_hoje)

        banca = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000.0)))
        banca_ini = float(st.session_state.get('banca_inicial', 1000.0))
        lucro_total = banca - banca_ini
        roi = (lucro_total / banca_ini * 100) if banca_ini > 0 else 0

        msg_fin = f"💰 <b>FECHAMENTO DO DIA</b>\n\n"
        msg_fin += f"📅 Data: {hoje}\n\n"
        msg_fin += f"├─ Entradas: {total_h} apostas\n"
        msg_fin += f"├─ Greens: {greens_h} | Reds: {reds_h}\n"
        msg_fin += f"├─ Winrate: {winrate_h:.1f}%\n"
        msg_fin += f"├─ Lucro Acum.: R$ {lucro_total:+.2f}\n"
        msg_fin += f"├─ ROI: {roi:+.1f}%\n"
        msg_fin += f"└─ Banca: R$ {banca:.2f}\n\n"

        # Barra visual
        pct_banca = min(100, max(0, (banca / (banca_ini * 2)) * 100))
        barras = int(pct_banca / 7)
        msg_fin += f"📈 Evolução: {'█' * barras}{'░' * (15 - barras)} {roi:+.1f}%"

        enviar_telegram(token, chat_id, msg_fin)
        st.session_state['financeiro_enviado'] = True

    # ════════════════════════════════════════════
    # 3. AJUSTE TÉCNICO
    # ════════════════════════════════════════════
    if not st.session_state.get('ia_enviada', False) and not df_hoje.empty:
        reds_hoje = df_hoje[df_hoje['Resultado'].str.contains('RED', na=False)]
        if len(reds_hoje) >= 2:
            msg_aj = f"🔧 <b>AJUSTE TÉCNICO</b>\n\n"
            msg_aj += f"🔍 Análise dos REDs de hoje ({len(reds_hoje)}):\n"

            # Analisar padrões nos reds
            if 'Liga' in reds_hoje.columns:
                ligas_red = reds_hoje['Liga'].value_counts()
                if len(ligas_red) > 0:
                    liga_pior = ligas_red.index[0]
                    msg_aj += f"├─ {ligas_red.iloc[0]}/{len(reds_hoje)} REDs da {liga_pior}\n"

            if 'Odd' in reds_hoje.columns:
                try:
                    odds_red = reds_hoje['Odd'].apply(lambda x: float(str(x).replace(',', '.')) if str(x).replace(',','').replace('.','').isdigit() else 1.5)
                    baixas = sum(1 for o in odds_red if o < 1.40)
                    if baixas > 0:
                        msg_aj += f"├─ {baixas}/{len(reds_hoje)} REDs com odd < 1.40\n"
                except: pass

            msg_aj += f"\n🛠️ Recomendação:\nAnalisar padrões de RED para otimizar filtros."
            enviar_telegram(token, chat_id, msg_aj)

        st.session_state['ia_enviada'] = True


# ==============================================================================
# 16. [FUSÃO] LOOP PRINCIPAL DE MONITORAMENTO
# ==============================================================================

def executar_ciclo_completo(api_key, token, chat_id):
    """Ciclo principal: matinal → live → green/red → var → noturno."""
    verificar_reset_diario()
    inicializar_banca()
    carregar_estado_firestore()

    # Limpar caches antigos a cada ciclo
    limpar_caches_antigos()

    # Verificar limite API
    if not verificar_limite_api():
        st.toast("⛔ Limite da API atingido! Aguardando reset.", icon="⛔")
        return

    agora = get_time_br()
    hora = agora.hour

    # ═══ MATINAL (7-12h) ═══
    if 7 <= hora < 12:
        executar_matinal(api_key, token, chat_id)
        formatar_matinal_jogo_limpo(api_key, token, chat_id)

    # ═══ RESUMO MEIO-DIA (12-13h) ═══
    if 12 <= hora < 13:
        enviar_resumo_meio_dia(token, chat_id)

    # ═══ SECOND DROP ODDS (16h) ═══
    if 16 <= hora < 17:
        executar_drop_odds_segundo(api_key, token, chat_id)

    # ═══ LIVE (10-23h) ═══
    if 10 <= hora <= 23:
        jogos_live = buscar_jogos_ao_vivo(api_key)
        if jogos_live:
            # Cache de form para não repetir chamadas
            form_cache = {}

            for jogo in jogos_live:
                fid = jogo['fixture']['id']
                status = jogo['fixture']['status']['short']
                if status not in ['1H', '2H', 'HT']: continue

                liga_id = str(jogo['league']['id'])
                df_black = st.session_state.get('df_black', pd.DataFrame())
                if not df_black.empty and liga_id in df_black['id'].values: continue

                # Stats
                stats = buscar_estatisticas(api_key, fid)
                if not stats: continue

                # Odds live
                odds_live = buscar_odds_live(api_key, fid)

                # Form (com cache)
                home_id = jogo['teams']['home']['id']
                away_id = jogo['teams']['away']['id']

                if home_id not in form_cache:
                    form_cache[home_id] = buscar_form_recente(api_key, home_id, 10)
                if away_id not in form_cache:
                    form_cache[away_id] = buscar_form_recente(api_key, away_id, 10)

                form_h = form_cache[home_id]
                form_a = form_cache[away_id]

                # Detectar estratégias
                sinais = analisar_estrategias_live(api_key, jogo, stats, odds_live, form_h, form_a)

                # Detectar estratégias extras (Cartões + Muralha)
                sinais_extras = analisar_estrategias_extras_live(jogo, stats)
                sinais.extend(sinais_extras)

                for sinal in sinais:
                    chave = gerar_chave_universal(sinal['fid'], sinal['estrategia'])
                    if chave in st.session_state['alertas_enviados']:
                        continue

                    # Buscar histórico pessoal para esta estratégia
                    hist_pessoal = []
                    df_hist = st.session_state.get('historico_full', pd.DataFrame())
                    if not df_hist.empty:
                        mask = df_hist['Estrategia'].str.contains(
                            sinal['estrategia'].split(' ')[-1] if ' ' in sinal['estrategia'] else sinal['estrategia'],
                            na=False, case=False
                        )
                        hist_pessoal = df_hist[mask].to_dict('records')[:20]

                    # Consultar IA (INFORMATIVA, nunca bloqueia)
                    dados_ia = consultar_ia_gemini(
                        sinal['estrategia'], sinal['liga'],
                        sinal['home'], sinal['away'],
                        sinal['minuto'], sinal['placar'],
                        sinal['odd'],
                        {'chutes_total': sinal['dados_stats'].get('chutes_total', 0),
                         'chutes_gol': sinal['dados_stats'].get('chutes_gol', 0)},
                        hist_pessoal
                    )

                    # Rastrear movimento de odd
                    mov, var_pct = rastrear_movimento_odd(sinal['fid'], sinal['estrategia'], sinal['odd'])

                    # ═══ FORMATAR E ENVIAR (SEMPRE, SEM VETO) ═══
                    msg = formatar_sinal_live_compacto(
                        sinal['estrategia'], sinal['liga'],
                        sinal['home'], sinal['away'],
                        sinal['minuto'], sinal['placar'],
                        sinal['odd'], sinal['confianca'],
                        dados_ia, sinal['dados_stats']
                    )

                    sucesso = enviar_telegram(token, chat_id, msg)

                    if sucesso:
                        st.session_state['alertas_enviados'].add(chave)

                        # Salvar no histórico
                        prob_final = dados_ia.get('probabilidade', sinal['confianca'])
                        item_hist = {
                            'FID': str(fid),
                            'Data': agora.strftime('%Y-%m-%d'),
                            'Hora': agora.strftime('%H:%M'),
                            'Liga': sinal['liga'],
                            'Jogo': f"{sinal['home']} x {sinal['away']}",
                            'Placar_Sinal': sinal['placar'],
                            'Estrategia': sinal['estrategia'],
                            'Resultado': 'PENDENTE',
                            'HomeID': str(sinal['home_id']),
                            'AwayID': str(sinal['away_id']),
                            'Odd': str(sinal['odd']),
                            'Odd_Atualizada': '',
                            'Opiniao_IA': dados_ia.get('resumo', ''),
                            'Probabilidade': f"{prob_final}%",
                            'Stake_Recomendado_Pct': '',
                            'Stake_Recomendado_RS': '',
                            'Modo_Gestao': ''
                        }
                        adicionar_historico(item_hist)

                        # Super Odd (se odd subiu)
                        if mov in ['SUBINDO', 'SUBINDO FORTE'] and var_pct >= 5:
                            msg_super = formatar_super_odd(sinal['home'], sinal['away'], sinal['estrategia'], sinal['odd'])
                            enviar_telegram(token, chat_id, msg_super)

            # Verificar GREEN/RED
            processar_green_red(api_key, token, chat_id)

            # Verificar VAR
            verificar_var(api_key, token, chat_id)

            # Processar múltiplas live pendentes
            processar_multiplas_live(api_key, token, chat_id)

            # Salvar BigData de jogos finalizados
            for jogo in jogos_live:
                if jogo['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
                    try:
                        stats_bd = buscar_estatisticas(api_key, jogo['fixture']['id'])
                        salvar_bigdata_jogo(api_key, jogo, stats_bd)
                    except: pass

            # Múltipla Live (quebra-empate)
            jogos_empatados_pressao = []
            for jogo in jogos_live:
                if jogo['fixture']['status']['short'] == 'HT':
                    gh = jogo['goals']['home'] or 0
                    ga = jogo['goals']['away'] or 0
                    if gh == ga:
                        stats_j = buscar_estatisticas(api_key, jogo['fixture']['id'])
                        chutes_t = extrair_stat(stats_j, 'Total Shots', 0) + extrair_stat(stats_j, 'Total Shots', 1)
                        if chutes_t >= 14:
                            jogos_empatados_pressao.append({
                                'fid': jogo['fixture']['id'],
                                'home': jogo['teams']['home']['name'],
                                'away': jogo['teams']['away']['name'],
                                'placar': f"{gh}x{ga}",
                                'chutes': chutes_t
                            })

            if len(jogos_empatados_pressao) >= 2:
                chave_mp = f"MULT_LIVE_{'_'.join(str(j['fid']) for j in jogos_empatados_pressao[:2])}"
                if chave_mp not in st.session_state.get('multiplas_enviadas', set()):
                    msg_mp = "🚀 <b>ALERTA: DUPLA QUEBRA-EMPATE</b>\nJogos empatados com alta pressão.\n\n"
                    for j in jogos_empatados_pressao[:2]:
                        msg_mp += f"⚽ {j['home']} x {j['away']} ({j['placar']})\n⏰ HT | 🔥 {j['chutes']} Chutes\n\n"
                    msg_mp += "🎯 Indicação: Múltipla Over +0.5 Gols na partida"
                    enviar_telegram(token, chat_id, msg_mp)
                    st.session_state['multiplas_enviadas'].add(chave_mp)
                    # Registrar para tracking
                    registrar_multipla_live(
                        [j['fid'] for j in jogos_empatados_pressao[:2]],
                        "Dupla Quebra-Empate",
                        "QUEBRA_EMPATE"
                    )

    # ═══ NOTURNO (22-23h) ═══
    if 22 <= hora <= 23:
        executar_relatorios_noite(token, chat_id)
        enviar_graficos_bi(token, chat_id)
        enviar_relatorio_bigdata(token, chat_id)

    # ═══ ALERTAS PERIÓDICOS ═══
    enviar_alerta_horario_quente(token, chat_id)

    # ═══ SALVAR PERIODICAMENTE ═══
    if st.session_state.get('precisa_salvar', False):
        df = st.session_state.get('historico_full', pd.DataFrame())
        if not df.empty:
            if salvar_aba("Historico", df):
                st.session_state['precisa_salvar'] = False

    # ═══ SALVAR ESTADO NO FIRESTORE ═══
    salvar_estado_firestore()


# ==============================================================================
# 17. [FUSÃO] BIGDATA — SALVAR JOGO COMPLETO
# ==============================================================================

def salvar_bigdata_jogo(api_key, jogo, stats):
    """Salva dados completos do jogo no Firestore para análise futura."""
    if not db_firestore: return
    fid = str(jogo['fixture']['id'])
    if fid in st.session_state.get('jogos_salvos_bigdata', set()): return

    try:
        home = jogo['teams']['home']['name']
        away = jogo['teams']['away']['name']
        goals_h = jogo['goals']['home'] or 0
        goals_a = jogo['goals']['away'] or 0

        doc_data = {
            'fid': fid,
            'data': get_time_br().strftime('%Y-%m-%d'),
            'hora': get_time_br().strftime('%H:%M'),
            'liga': jogo['league']['name'],
            'liga_id': jogo['league']['id'],
            'home': home,
            'away': away,
            'home_id': jogo['teams']['home']['id'],
            'away_id': jogo['teams']['away']['id'],
            'goals_home': goals_h,
            'goals_away': goals_a,
            'status': jogo['fixture']['status']['short'],
            'chutes_home': extrair_stat(stats, 'Total Shots', 0),
            'chutes_away': extrair_stat(stats, 'Total Shots', 1),
            'chutes_gol_home': extrair_stat(stats, 'Shots on Goal', 0),
            'chutes_gol_away': extrair_stat(stats, 'Shots on Goal', 1),
            'posse_home': extrair_stat(stats, 'Ball Possession', 0),
            'posse_away': extrair_stat(stats, 'Ball Possession', 1),
            'corners_home': extrair_stat(stats, 'Corner Kicks', 0),
            'corners_away': extrair_stat(stats, 'Corner Kicks', 1),
            'ataques_home': extrair_stat(stats, 'Dangerous Attacks', 0),
            'ataques_away': extrair_stat(stats, 'Dangerous Attacks', 1),
            'faltas_home': extrair_stat(stats, 'Fouls', 0),
            'faltas_away': extrair_stat(stats, 'Fouls', 1),
        }

        db_firestore.collection('bigdata_jogos').document(fid).set(doc_data)
        st.session_state['jogos_salvos_bigdata'].add(fid)
        st.session_state['total_bigdata_count'] = len(st.session_state['jogos_salvos_bigdata'])
    except: pass


# ==============================================================================
# 18. STREAMLIT UI — INTERFACE PRINCIPAL
# ==============================================================================

def main():
    with st.sidebar:
        st.title("❄️ Neves Analytics PRO")
        st.caption("🔀 FUSÃO v3.0 — Compacto + IA")

        # Configuração API
        with st.expander("⚙️ Configuração", expanded=not st.session_state.get('API_KEY')):
            api_key = st.text_input("🔑 API-Football Key", value=st.session_state.get('API_KEY', ''), type="password")
            if api_key: st.session_state['API_KEY'] = api_key

            tg_token = st.text_input("🤖 Token Telegram", value=st.session_state.get('TG_TOKEN', ''), type="password")
            if tg_token: st.session_state['TG_TOKEN'] = tg_token

            tg_chat = st.text_input("💬 Chat ID Telegram", value=st.session_state.get('TG_CHAT', ''))
            if tg_chat: st.session_state['TG_CHAT'] = tg_chat

            col_t1, col_t2 = st.columns(2)
            with col_t1:
                if st.button("🔌 Testar TG"):
                    ok, info = testar_conexao_telegram(tg_token)
                    if ok: st.success(f"✅ {info}")
                    else: st.error(f"❌ {info}")
            with col_t2:
                if st.button("📡 Testar API"):
                    if api_key:
                        try:
                            r = requests.get("https://v3.football.api-sports.io/status",
                                           headers={"x-apisports-key": api_key}, timeout=5)
                            if r.status_code == 200:
                                data = r.json()
                                used = data['response']['requests']['current']
                                limit = data['response']['requests']['limit_day']
                                st.success(f"✅ {used}/{limit} requisições")
                            else: st.error("❌ Erro API")
                        except: st.error("❌ Sem conexão")

        # Gestão de Banca
        with st.expander("💰 Gestão de Banca"):
            inicializar_banca()
            banca_ini = st.number_input("Banca Inicial (R$)", min_value=10.0,
                                        value=float(st.session_state.get('banca_inicial', 1000.0)), step=50.0)
            st.session_state['banca_inicial'] = banca_ini

            if 'banca_atual' not in st.session_state or st.session_state['banca_atual'] == 0:
                st.session_state['banca_atual'] = banca_ini

            modo = st.selectbox("Modo Kelly", ['fracionario', 'conservador', 'full'],
                              index=['fracionario', 'conservador', 'full'].index(
                                  st.session_state.get('modo_gestao_banca', 'fracionario')))
            st.session_state['modo_gestao_banca'] = modo

            banca_atual = float(st.session_state.get('banca_atual', banca_ini))
            lucro = banca_atual - banca_ini
            cor = "#00FF00" if lucro >= 0 else "#FF4B4B"
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-title">BANCA ATUAL</div>
                <div class="metric-value" style="color:{cor}">R$ {banca_atual:.2f}</div>
                <div class="metric-sub">Lucro: R$ {lucro:+.2f} | Kelly: {modo}</div>
            </div>
            """, unsafe_allow_html=True)

            if st.button("🔄 Resetar Banca"):
                st.session_state['banca_atual'] = banca_ini
                st.session_state['historico_banca'] = []
                st.rerun()

        # Status
        with st.expander("📊 Status"):
            api_used = st.session_state['api_usage']['used']
            api_limit = st.session_state['api_usage']['limit']
            pct_api = (api_used / api_limit * 100) if api_limit > 0 else 0
            st.progress(min(1.0, pct_api / 100), text=f"API: {api_used}/{api_limit} ({pct_api:.1f}%)")

            gem_used = st.session_state['gemini_usage']['used']
            st.caption(f"🤖 Gemini: {gem_used} chamadas | IA: {'✅ Ativa' if IA_ATIVADA else '❌ Off'}")

            total_sinais = len(st.session_state.get('alertas_enviados', set()))
            st.caption(f"📡 Sinais hoje: {total_sinais}")

            bigdata_count = st.session_state.get('total_bigdata_count', 0)
            st.caption(f"💾 BigData: {bigdata_count} jogos")

        # Controles
        st.markdown("---")
        col_b1, col_b2 = st.columns(2)
        with col_b1:
            if st.button("▶️ LIGAR ROBÔ" if not st.session_state.ROBO_LIGADO else "⏹️ DESLIGAR", use_container_width=True):
                st.session_state.ROBO_LIGADO = not st.session_state.ROBO_LIGADO
                st.rerun()
        with col_b2:
            if st.button("🔄 Recarregar", use_container_width=True):
                carregar_tudo(force=True)
                st.rerun()

    # ═══════════════════════════════════════════
    # ÁREA PRINCIPAL
    # ═══════════════════════════════════════════

    # Status do robô
    if st.session_state.ROBO_LIGADO:
        st.markdown('<div class="status-active">🟢 ROBÔ ATIVO — Monitorando em tempo real</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="status-warning">🟡 ROBÔ PARADO — Clique em LIGAR no menu lateral</div>', unsafe_allow_html=True)

    # Métricas
    col1, col2, col3, col4, col5 = st.columns(5)

    df_hist = st.session_state.get('historico_full', pd.DataFrame())
    hoje = get_time_br().strftime('%Y-%m-%d')
    df_hoje = df_hist[df_hist['Data'] == hoje] if not df_hist.empty and 'Data' in df_hist.columns else pd.DataFrame()
    total_h, greens_h, reds_h, winrate_h = calcular_stats(df_hoje) if not df_hoje.empty else (0, 0, 0, 0)
    total_g, greens_g, reds_g, winrate_g = calcular_stats(df_hist) if not df_hist.empty else (0, 0, 0, 0)

    with col1:
        st.markdown(f"""<div class="metric-box"><div class="metric-title">HOJE</div>
        <div class="metric-value">{greens_h}G / {reds_h}R</div>
        <div class="metric-sub">{total_h} sinais</div></div>""", unsafe_allow_html=True)
    with col2:
        cor_wr = "#00FF00" if winrate_h >= 65 else ("#FFFF00" if winrate_h >= 50 else "#FF4B4B")
        st.markdown(f"""<div class="metric-box"><div class="metric-title">WINRATE HOJE</div>
        <div class="metric-value" style="color:{cor_wr}">{winrate_h:.1f}%</div>
        <div class="metric-sub">Meta: 65%+</div></div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""<div class="metric-box"><div class="metric-title">GLOBAL</div>
        <div class="metric-value">{winrate_g:.1f}%</div>
        <div class="metric-sub">{total_g} apostas total</div></div>""", unsafe_allow_html=True)
    with col4:
        banca_val = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000.0)))
        st.markdown(f"""<div class="metric-box"><div class="metric-title">BANCA</div>
        <div class="metric-value">R$ {banca_val:.0f}</div>
        <div class="metric-sub">Kelly {st.session_state.get('modo_gestao_banca', 'frac')}</div></div>""", unsafe_allow_html=True)
    with col5:
        agora = get_time_br()
        st.markdown(f"""<div class="metric-box"><div class="metric-title">HORÁRIO</div>
        <div class="metric-value">{agora.strftime('%H:%M')}</div>
        <div class="metric-sub">{agora.strftime('%d/%m/%Y')}</div></div>""", unsafe_allow_html=True)

    # Tabs principais
    tab_live, tab_hist, tab_ligas, tab_banca = st.tabs(["⚡ Live", "📊 Histórico", "🏆 Ligas", "💰 Banca"])

    with tab_live:
        st.subheader("📡 Sinais de Hoje")
        sinais_hoje = st.session_state.get('historico_sinais', [])
        if sinais_hoje:
            for s in sinais_hoje[:20]:
                resultado = str(s.get('Resultado', 'PENDENTE'))
                if 'GREEN' in resultado: emoji_r = "✅"
                elif 'RED' in resultado: emoji_r = "❌"
                else: emoji_r = "⏳"

                est = s.get('Estrategia', '')
                tipo = classificar_tipo_estrategia(est)
                tipo_emoji = '⚽' if tipo == 'OVER' else ('❄️' if tipo == 'UNDER' else ('👴' if tipo == 'RESULTADO' else '📊'))

                odd_str = s.get('Odd', '')
                prob_str = s.get('Probabilidade', '')
                stake_str = s.get('Stake_Recomendado_RS', '')

                st.markdown(f"""
                {emoji_r} **{est}** {tipo_emoji}
                ⚽ {s.get('Jogo', '')} | 🏆 {s.get('Liga', '')}
                ⏰ {s.get('Hora', '')} | 🥅 {s.get('Placar_Sinal', '')} | 💰 @{odd_str} | {prob_str} | {stake_str}
                ---
                """)
        else:
            st.info("Nenhum sinal enviado hoje. Aguardando jogos...")

    with tab_hist:
        st.subheader("📊 Histórico Completo")
        if not df_hist.empty:
            # Filtros
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                estrategias = ['Todas'] + sorted(df_hist['Estrategia'].unique().tolist()) if 'Estrategia' in df_hist.columns else ['Todas']
                filtro_est = st.selectbox("Estratégia", estrategias)
            with col_f2:
                ligas = ['Todas'] + sorted(df_hist['Liga'].unique().tolist()) if 'Liga' in df_hist.columns else ['Todas']
                filtro_liga = st.selectbox("Liga", ligas)

            df_filtrado = df_hist.copy()
            if filtro_est != 'Todas': df_filtrado = df_filtrado[df_filtrado['Estrategia'] == filtro_est]
            if filtro_liga != 'Todas': df_filtrado = df_filtrado[df_filtrado['Liga'] == filtro_liga]

            total_f, greens_f, reds_f, winrate_f = calcular_stats(df_filtrado)
            st.metric("Winrate Filtrado", f"{winrate_f:.1f}%", f"{greens_f}G / {reds_f}R de {total_f}")

            st.dataframe(df_filtrado.head(100), use_container_width=True)
        else:
            st.info("Sem histórico disponível.")

    with tab_ligas:
        st.subheader("🏆 Gestão de Ligas")
        col_l1, col_l2, col_l3 = st.columns(3)

        with col_l1:
            st.markdown("**✅ Ligas Seguras**")
            df_safe = st.session_state.get('df_safe', pd.DataFrame())
            if not df_safe.empty: st.dataframe(df_safe, use_container_width=True)
            else: st.caption("Nenhuma liga segura cadastrada.")

        with col_l2:
            st.markdown("**⚠️ Em Observação**")
            df_vip = st.session_state.get('df_vip', pd.DataFrame())
            if not df_vip.empty: st.dataframe(df_vip, use_container_width=True)
            else: st.caption("Nenhuma liga em observação.")

        with col_l3:
            st.markdown("**🚫 Blacklist**")
            df_black = st.session_state.get('df_black', pd.DataFrame())
            if not df_black.empty: st.dataframe(df_black, use_container_width=True)
            else: st.caption("Nenhuma liga banida.")

    with tab_banca:
        st.subheader("💰 Evolução da Banca")
        hist_banca = st.session_state.get('historico_banca', [])
        if hist_banca:
            df_banca = pd.DataFrame(hist_banca)
            fig = px.line(df_banca, y='banca', title='Evolução da Banca',
                         labels={'banca': 'R$', 'index': 'Operações'})
            fig.update_layout(template='plotly_dark', height=400)
            st.plotly_chart(fig, use_container_width=True)

            col_b1, col_b2, col_b3 = st.columns(3)
            with col_b1:
                greens_banca = sum(1 for h in hist_banca if h['tipo'] == 'GREEN')
                st.metric("Greens", greens_banca)
            with col_b2:
                reds_banca = sum(1 for h in hist_banca if h['tipo'] == 'RED')
                st.metric("Reds", reds_banca)
            with col_b3:
                lucro_total = sum(h['valor'] for h in hist_banca)
                st.metric("Lucro Total", f"R$ {lucro_total:.2f}")
        else:
            st.info("Sem movimentações de banca registradas.")

    # ═══ FOOTER COM TIMER ═══
    if st.session_state.ROBO_LIGADO:
        agora = get_time_br()
        st.markdown(f"""
        <div class="footer-timer">
            ❄️ Neves Analytics PRO | FUSÃO v3.0 | {agora.strftime('%H:%M:%S')} |
            📡 {len(st.session_state.get('alertas_enviados', set()))} sinais |
            💰 R$ {float(st.session_state.get('banca_atual', 0)):.0f} |
            🤖 IA: {'ON' if IA_ATIVADA else 'OFF'}
        </div>
        """, unsafe_allow_html=True)

    # ═══ EXECUÇÃO AUTOMÁTICA ═══
    if st.session_state.ROBO_LIGADO:
        api_key = st.session_state.get('API_KEY', '')
        token = st.session_state.get('TG_TOKEN', '')
        chat_id = st.session_state.get('TG_CHAT', '')

        if api_key and token and chat_id:
            # Carregar dados
            carregar_tudo()

            # Executar ciclo
            try:
                executar_ciclo_completo(api_key, token, chat_id)
            except Exception as e:
                st.toast(f"⚠️ Erro no ciclo: {str(e)[:100]}", icon="🚨")

            # Auto-refresh a cada 90 segundos
            time.sleep(2)
            st.rerun()
        else:
            st.warning("⚠️ Configure API Key, Token e Chat ID no menu lateral.")


# ==============================================================================
# 19. EXECUÇÃO
# ==============================================================================

if __name__ == "__main__":
    main()


# ==============================================================================
# 20. [COMPLEMENTO] SEASON DETECTION + H2H CACHE
# ==============================================================================

def detectar_season(liga_id):
    """Detecta a temporada correta baseada na liga."""
    agora = get_time_br()
    LIGAS_ANO_CIVIL = [71, 72, 128, 130, 253, 262, 307, 218]  # BR, AR, JP, etc
    if int(liga_id) in LIGAS_ANO_CIVIL:
        return agora.year
    return agora.year if agora.month >= 7 else agora.year - 1


def buscar_h2h_cache(api_key, home_id, away_id, ultimos=10):
    """Busca confronto direto com cache em session_state."""
    cache_key = f"h2h_{home_id}_{away_id}"
    if cache_key in st.session_state.get('h2h_cache', {}):
        cached = st.session_state['h2h_cache'][cache_key]
        if (get_time_br() - cached['timestamp']).total_seconds() < 3600:
            return cached['data']

    try:
        url = "https://v3.football.api-sports.io/fixtures/headtohead"
        params = {"h2h": f"{home_id}-{away_id}", "last": ultimos}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        jogos = r.json().get('response', [])

        total = len(jogos)
        if total == 0:
            return {'total': 0, 'over25': 0, 'btts': 0, 'media_gols': 0, 'pct_over25': 0, 'pct_btts': 0}

        over25, btts_count, total_gols = 0, 0, 0
        vitorias_home, vitorias_away, empates = 0, 0, 0

        for j in jogos:
            gh = j['goals']['home'] or 0
            ga = j['goals']['away'] or 0
            total_gols += gh + ga
            if gh + ga > 2: over25 += 1
            if gh > 0 and ga > 0: btts_count += 1
            hid = j['teams']['home']['id']
            if gh > ga:
                if hid == int(home_id): vitorias_home += 1
                else: vitorias_away += 1
            elif ga > gh:
                if hid == int(home_id): vitorias_away += 1
                else: vitorias_home += 1
            else: empates += 1

        resultado = {
            'total': total,
            'over25': over25,
            'btts': btts_count,
            'media_gols': round(total_gols / total, 1),
            'pct_over25': int((over25 / total) * 100),
            'pct_btts': int((btts_count / total) * 100),
            'vitorias_home': vitorias_home,
            'vitorias_away': vitorias_away,
            'empates': empates
        }

        if 'h2h_cache' not in st.session_state:
            st.session_state['h2h_cache'] = {}
        st.session_state['h2h_cache'][cache_key] = {'data': resultado, 'timestamp': get_time_br()}
        return resultado
    except:
        return {'total': 0, 'over25': 0, 'btts': 0, 'media_gols': 0, 'pct_over25': 0, 'pct_btts': 0}


def buscar_stats_cartoes_recentes(api_key, team_id, ultimos=10):
    """Busca média de cartões nos últimos jogos do time."""
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"team": team_id, "last": ultimos, "status": "FT"}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        jogos = r.json().get('response', [])
        total_cartoes = 0
        total_escanteios = 0
        for j in jogos:
            fid_j = j['fixture']['id']
            try:
                stats_j = buscar_estatisticas(api_key, fid_j)
                hid = j['teams']['home']['id']
                idx = 0 if hid == int(team_id) else 1
                cartoes_amarelos = extrair_stat(stats_j, 'Yellow Cards', idx)
                cartoes_vermelhos = extrair_stat(stats_j, 'Red Cards', idx)
                corners = extrair_stat(stats_j, 'Corner Kicks', idx)
                total_cartoes += cartoes_amarelos + cartoes_vermelhos
                total_escanteios += corners
            except: pass
        n = len(jogos) if jogos else 1
        return {
            'media_cartoes': round(total_cartoes / n, 1),
            'media_escanteios': round(total_escanteios / n, 1),
            'total_jogos': n
        }
    except:
        return {'media_cartoes': 0, 'media_escanteios': 0, 'total_jogos': 0}


def buscar_stats_goleiro(api_key, team_id, ultimos=10):
    """Busca média de defesas do goleiro nos últimos jogos."""
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"team": team_id, "last": ultimos, "status": "FT"}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        jogos = r.json().get('response', [])
        total_defesas = 0
        total_chutes_sofridos = 0
        for j in jogos:
            fid_j = j['fixture']['id']
            try:
                stats_j = buscar_estatisticas(api_key, fid_j)
                hid = j['teams']['home']['id']
                idx = 0 if hid == int(team_id) else 1
                idx_opp = 1 - idx
                defesas = extrair_stat(stats_j, 'Goalkeeper Saves', idx)
                chutes_opp = extrair_stat(stats_j, 'Shots on Goal', idx_opp)
                total_defesas += defesas
                total_chutes_sofridos += chutes_opp
            except: pass
        n = len(jogos) if jogos else 1
        return {
            'media_defesas': round(total_defesas / n, 1),
            'media_chutes_sofridos': round(total_chutes_sofridos / n, 1),
            'total_jogos': n
        }
    except:
        return {'media_defesas': 0, 'media_chutes_sofridos': 0, 'total_jogos': 0}


# ==============================================================================
# 21. [COMPLEMENTO] MATINAL COMPLETO — ZONAS EXTRAS + TABELA
# ==============================================================================

def executar_matinal_zonas_extras(api_key, token, chat_id, jogos_validos):
    """Envia as zonas extras da matinal: Cartões, Escanteios, Defesas do Goleiro."""

    msg_extras = ""

    # Cache para não repetir chamadas
    stats_cartoes_cache = {}
    stats_goleiro_cache = {}

    # ════════════════════════════════════════════
    # ZONA DE CARTÕES (🟨)
    # ════════════════════════════════════════════
    jogos_cartoes = []
    for j in jogos_validos[:10]:
        home_id = j['teams']['home']['id']
        away_id = j['teams']['away']['id']

        if home_id not in stats_cartoes_cache:
            stats_cartoes_cache[home_id] = buscar_stats_cartoes_recentes(api_key, home_id, 8)
        if away_id not in stats_cartoes_cache:
            stats_cartoes_cache[away_id] = buscar_stats_cartoes_recentes(api_key, away_id, 8)

        media_h = stats_cartoes_cache[home_id]['media_cartoes']
        media_a = stats_cartoes_cache[away_id]['media_cartoes']
        media_total = media_h + media_a

        if media_total >= 4.5:
            jogos_cartoes.append({
                'home': j['teams']['home']['name'],
                'away': j['teams']['away']['name'],
                'liga': j['league']['name'],
                'media_cartoes': media_total,
                'media_h': media_h,
                'media_a': media_a
            })

    if jogos_cartoes:
        jogos_cartoes.sort(key=lambda x: x['media_cartoes'], reverse=True)
        msg_extras += "🟨 <b>ZONA DE CARTÕES</b>\n\n"
        for jc in jogos_cartoes[:3]:
            msg_extras += f"⚽ Jogo: {jc['home']} x {jc['away']}\n"
            msg_extras += f"🏆 Liga: {jc['liga']}\n"
            msg_extras += f"🎯 Palpite: Mais de 4.5 Cartões\n"
            msg_extras += f"📝 Motivo: Média somada {jc['media_cartoes']:.1f} cartões/jogo ({jc['media_h']:.1f} + {jc['media_a']:.1f})\n\n"
        msg_extras += "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    # ════════════════════════════════════════════
    # ZONA DE ESCANTEIOS (🏴)
    # ════════════════════════════════════════════
    jogos_escanteios = []
    for j in jogos_validos[:10]:
        home_id = j['teams']['home']['id']
        away_id = j['teams']['away']['id']

        if home_id not in stats_cartoes_cache:
            stats_cartoes_cache[home_id] = buscar_stats_cartoes_recentes(api_key, home_id, 8)
        if away_id not in stats_cartoes_cache:
            stats_cartoes_cache[away_id] = buscar_stats_cartoes_recentes(api_key, away_id, 8)

        esc_h = stats_cartoes_cache[home_id]['media_escanteios']
        esc_a = stats_cartoes_cache[away_id]['media_escanteios']
        esc_total = esc_h + esc_a

        if esc_total >= 9.0:
            jogos_escanteios.append({
                'home': j['teams']['home']['name'],
                'away': j['teams']['away']['name'],
                'liga': j['league']['name'],
                'media_esc': esc_total,
                'esc_h': esc_h,
                'esc_a': esc_a
            })

    if jogos_escanteios:
        jogos_escanteios.sort(key=lambda x: x['media_esc'], reverse=True)
        msg_extras += "🏴 <b>ZONA DE ESCANTEIOS</b>\n\n"
        for je in jogos_escanteios[:3]:
            msg_extras += f"⚽ Jogo: {je['home']} x {je['away']}\n"
            msg_extras += f"🏆 Liga: {je['liga']}\n"
            msg_extras += f"🎯 Palpite: Mais de 9.5 Escanteios\n"
            msg_extras += f"📝 Motivo: Média combinada {je['media_esc']:.1f} cantos ({je['esc_h']:.1f} casa + {je['esc_a']:.1f} fora)\n\n"
        msg_extras += "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    # ════════════════════════════════════════════
    # ZONA DE DEFESAS DO GOLEIRO (🧤)
    # ════════════════════════════════════════════
    jogos_defesas = []
    for j in jogos_validos[:10]:
        home_id = j['teams']['home']['id']
        away_id = j['teams']['away']['id']

        if home_id not in stats_goleiro_cache:
            stats_goleiro_cache[home_id] = buscar_stats_goleiro(api_key, home_id, 8)
        if away_id not in stats_goleiro_cache:
            stats_goleiro_cache[away_id] = buscar_stats_goleiro(api_key, away_id, 8)

        # Goleiro do away sofre muitos chutes do home
        chutes_home_media = stats_goleiro_cache[away_id]['media_chutes_sofridos']
        defesas_away = stats_goleiro_cache[away_id]['media_defesas']

        # Goleiro do home sofre muitos chutes do away
        chutes_away_media = stats_goleiro_cache[home_id]['media_chutes_sofridos']
        defesas_home = stats_goleiro_cache[home_id]['media_defesas']

        # Cenário: home domina → goleiro do away faz muitas defesas
        if chutes_home_media >= 5.0 and defesas_away >= 3.5:
            jogos_defesas.append({
                'home': j['teams']['home']['name'],
                'away': j['teams']['away']['name'],
                'liga': j['league']['name'],
                'goleiro_time': j['teams']['away']['name'],
                'media_defesas': defesas_away,
                'media_chutes_sofridos': chutes_home_media,
                'cenario': 'massacre'
            })
        # Cenário: away domina
        elif chutes_away_media >= 5.0 and defesas_home >= 3.5:
            jogos_defesas.append({
                'home': j['teams']['home']['name'],
                'away': j['teams']['away']['name'],
                'liga': j['league']['name'],
                'goleiro_time': j['teams']['home']['name'],
                'media_defesas': defesas_home,
                'media_chutes_sofridos': chutes_away_media,
                'cenario': 'pressão fora'
            })

    if jogos_defesas:
        jogos_defesas.sort(key=lambda x: x['media_defesas'], reverse=True)
        msg_extras += "🧤 <b>ZONA DE DEFESAS DO GOLEIRO</b>\n\n"
        for jd in jogos_defesas[:3]:
            msg_extras += f"⚽ Jogo: {jd['home']} x {jd['away']}\n"
            msg_extras += f"🏆 Liga: {jd['liga']}\n"
            msg_extras += f"🎯 Palpite: Goleiro {jd['goleiro_time']}: Mais de 3.5 Defesas\n"
            msg_extras += f"📝 Motivo: Média {jd['media_defesas']:.1f} defesas/jogo. Adversário chuta {jd['media_chutes_sofridos']:.1f} no gol.\n"
            msg_extras += f"⚠️ Regra: Aposte no 'Goleiro do Time', não no nome do jogador.\n\n"
        msg_extras += "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    if msg_extras:
        enviar_telegram(token, chat_id, msg_extras)

    return stats_cartoes_cache, stats_goleiro_cache


def adicionar_contexto_tabela(api_key, jogo, msg_base):
    """Adiciona posição na tabela como contexto."""
    try:
        liga_id = jogo['league']['id']
        if int(liga_id) not in LIGAS_TABELA:
            return msg_base

        season = detectar_season(liga_id)
        tabela = buscar_tabela_liga(api_key, liga_id, season)
        if not tabela:
            return msg_base

        home_id = jogo['teams']['home']['id']
        away_id = jogo['teams']['away']['id']

        pos_home = obter_posicao_tabela(tabela, home_id)
        pos_away = obter_posicao_tabela(tabela, away_id)

        if pos_home and pos_away:
            rank_h = pos_home.get('rank', '?')
            rank_a = pos_away.get('rank', '?')
            pts_h = pos_home.get('points', 0)
            pts_a = pos_away.get('points', 0)
            msg_base += f"📊 Tabela: {rank_h}º ({pts_h}pts) vs {rank_a}º ({pts_a}pts)\n"

        return msg_base
    except:
        return msg_base


# ==============================================================================
# 22. [COMPLEMENTO] ESTRATÉGIAS LIVE EXTRAS — CARTÕES + MURALHA
# ==============================================================================

def analisar_estrategias_extras_live(jogo, stats):
    """Detecta estratégias de Cartões e Defesas do Goleiro ao vivo."""
    extras = []

    fid = jogo['fixture']['id']
    elapsed = jogo['fixture']['status'].get('elapsed', 0) or 0
    home_name = jogo['teams']['home']['name']
    away_name = jogo['teams']['away']['name']
    home_id = jogo['teams']['home']['id']
    away_id = jogo['teams']['away']['id']
    goals_home = jogo['goals']['home'] or 0
    goals_away = jogo['goals']['away'] or 0
    placar = f"{goals_home}x{goals_away}"
    liga_nome = jogo['league']['name']
    liga_id = jogo['league']['id']

    faltas_home = extrair_stat(stats, 'Fouls', 0)
    faltas_away = extrair_stat(stats, 'Fouls', 1)
    cartoes_home = extrair_stat(stats, 'Yellow Cards', 0)
    cartoes_away = extrair_stat(stats, 'Yellow Cards', 1)
    defesas_home = extrair_stat(stats, 'Goalkeeper Saves', 0)
    defesas_away = extrair_stat(stats, 'Goalkeeper Saves', 1)
    chutes_gol_home = extrair_stat(stats, 'Shots on Goal', 0)
    chutes_gol_away = extrair_stat(stats, 'Shots on Goal', 1)

    faltas_total = faltas_home + faltas_away
    cartoes_total = cartoes_home + cartoes_away

    # 🟨 Sniper de Cartões (jogo quente, muitas faltas)
    if 20 <= elapsed <= 60:
        if faltas_total >= 12 and cartoes_total >= 2:
            # Projetar cartões para 90 min
            projecao = round((cartoes_total / max(elapsed, 1)) * 90, 1)
            if projecao >= 5.0:
                confianca = min(90, 55 + cartoes_total * 5 + (faltas_total - 12) * 2)
                extras.append({
                    'estrategia': '🟨 Sniper de Cartões',
                    'fid': fid, 'liga': liga_nome, 'liga_id': liga_id,
                    'home': home_name, 'away': away_name,
                    'home_id': home_id, 'away_id': away_id,
                    'minuto': elapsed, 'placar': placar,
                    'goals_home': goals_home, 'goals_away': goals_away,
                    'total_gols': goals_home + goals_away,
                    'odd': 1.65,
                    'confianca': confianca,
                    'dados_stats': {
                        'analise_curta': f"Jogo quente: {cartoes_total} cartões | {faltas_total} faltas | Projeção: {projecao:.0f} cartões",
                        'rating_home': '', 'rating_away': '',
                        'pct_over_home': 0, 'pct_over_away': 0,
                        'chutes_total': 0, 'chutes_gol': 0
                    }
                })

    # 🧤 Muralha (goleiro fazendo muitas defesas)
    if 30 <= elapsed <= 70:
        # Goleiro do away
        if defesas_away >= 4 and chutes_gol_home >= 5:
            projecao_defesas = round((defesas_away / max(elapsed, 1)) * 90, 1)
            confianca = min(90, 55 + defesas_away * 5)
            extras.append({
                'estrategia': '🧤 Muralha (Defesas)',
                'fid': fid, 'liga': liga_nome, 'liga_id': liga_id,
                'home': home_name, 'away': away_name,
                'home_id': home_id, 'away_id': away_id,
                'minuto': elapsed, 'placar': placar,
                'goals_home': goals_home, 'goals_away': goals_away,
                'total_gols': goals_home + goals_away,
                'odd': 1.70,
                'confianca': confianca,
                'dados_stats': {
                    'analise_curta': f"Goleiro {away_name}: {defesas_away} defesas | {chutes_gol_home} chutes no gol sofridos | Projeção: {projecao_defesas:.0f}",
                    'rating_home': '', 'rating_away': '',
                    'pct_over_home': 0, 'pct_over_away': 0,
                    'chutes_total': 0, 'chutes_gol': 0
                }
            })
        # Goleiro do home
        if defesas_home >= 4 and chutes_gol_away >= 5:
            projecao_defesas = round((defesas_home / max(elapsed, 1)) * 90, 1)
            confianca = min(90, 55 + defesas_home * 5)
            extras.append({
                'estrategia': '🧤 Muralha (Defesas)',
                'fid': fid, 'liga': liga_nome, 'liga_id': liga_id,
                'home': home_name, 'away': away_name,
                'home_id': home_id, 'away_id': away_id,
                'minuto': elapsed, 'placar': placar,
                'goals_home': goals_home, 'goals_away': goals_away,
                'total_gols': goals_home + goals_away,
                'odd': 1.70,
                'confianca': confianca,
                'dados_stats': {
                    'analise_curta': f"Goleiro {home_name}: {defesas_home} defesas | {chutes_gol_away} chutes no gol sofridos | Projeção: {projecao_defesas:.0f}",
                    'rating_home': '', 'rating_away': '',
                    'pct_over_home': 0, 'pct_over_away': 0,
                    'chutes_total': 0, 'chutes_gol': 0
                }
            })

    return extras


# ==============================================================================
# 23. [COMPLEMENTO] MÚLTIPLA LIVE — TRACKING + GREEN/RED
# ==============================================================================

def registrar_multipla_live(fids, descricao, tipo_multipla="QUEBRA_EMPATE"):
    """Registra uma múltipla live para tracking de resultado."""
    chave = f"MULT_{tipo_multipla}_{'_'.join(str(f) for f in fids)}"
    if chave in st.session_state.get('multiplas_enviadas', set()):
        return False

    multipla = {
        'chave': chave,
        'fids': fids,
        'descricao': descricao,
        'tipo': tipo_multipla,
        'status': 'PENDENTE',
        'resultados': {},
        'timestamp': get_time_br()
    }

    if 'multiplas_pendentes' not in st.session_state:
        st.session_state['multiplas_pendentes'] = []
    st.session_state['multiplas_pendentes'].append(multipla)
    st.session_state['multiplas_enviadas'].add(chave)
    return True


def processar_multiplas_live(api_key, token, chat_id):
    """Verifica resultado de múltiplas live pendentes."""
    pendentes = st.session_state.get('multiplas_pendentes', [])
    if not pendentes: return

    jogos_live = buscar_jogos_ao_vivo(api_key)
    mapa_live = {str(j['fixture']['id']): j for j in jogos_live}

    # Buscar finalizados também
    hoje_str = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje_str, "timezone": "America/Sao_Paulo"}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        for j in r.json().get('response', []):
            fid_str = str(j['fixture']['id'])
            if fid_str not in mapa_live:
                mapa_live[fid_str] = j
    except: pass

    for multipla in pendentes:
        if multipla['status'] != 'PENDENTE':
            continue

        todos_resolvidos = True
        algum_red = False
        todos_green = True
        placares = []

        for fid in multipla['fids']:
            fid_str = str(fid)
            if fid_str not in mapa_live:
                todos_resolvidos = False
                continue

            jogo = mapa_live[fid_str]
            status = jogo['fixture']['status']['short']
            gh = jogo['goals']['home'] or 0
            ga = jogo['goals']['away'] or 0
            total = gh + ga

            if status in ['FT', 'AET', 'PEN']:
                if total > 0:
                    multipla['resultados'][fid_str] = 'GREEN'
                    placares.append(f"{gh}x{ga}")
                else:
                    multipla['resultados'][fid_str] = 'RED'
                    algum_red = True
                    todos_green = False
                    placares.append(f"{gh}x{ga}")
            elif status in ['1H', '2H', 'HT']:
                if total > 0:
                    multipla['resultados'][fid_str] = 'GREEN'
                    placares.append(f"{gh}x{ga}")
                else:
                    todos_resolvidos = False
                    todos_green = False
            else:
                todos_resolvidos = False

        # Enviar resultado se resolvido
        chave_res = f"RES_{multipla['chave']}"
        if chave_res in st.session_state['alertas_enviados']:
            continue

        if algum_red:
            # RED imediato se algum jogo terminou 0x0
            msg = f"❌ <b>RED MÚLTIPLA FINALIZADA</b>\n"
            msg += f"Uma das seleções não bateu.\n"
            msg += f"📉 Placares: {' / '.join(placares)}"
            enviar_telegram(token, chat_id, msg)
            multipla['status'] = 'RED'
            st.session_state['alertas_enviados'].add(chave_res)

        elif todos_resolvidos and todos_green:
            msg = f"✅ <b>GREEN MÚLTIPLA CONFIRMADO!</b>\n"
            msg += f"Todas as seleções bateram!\n"
            msg += f"📈 Placares: {' / '.join(placares)}"
            enviar_telegram(token, chat_id, msg)
            multipla['status'] = 'GREEN'
            st.session_state['alertas_enviados'].add(chave_res)


# ==============================================================================
# 24. [COMPLEMENTO] GRÁFICOS BI — ENVIO COMO IMAGEM
# ==============================================================================

def gerar_grafico_evolucao_banca():
    """Gera gráfico de evolução da banca como bytes PNG."""
    hist_banca = st.session_state.get('historico_banca', [])
    if not hist_banca or len(hist_banca) < 2:
        return None

    try:
        valores = [h['banca'] for h in hist_banca]
        tipos = [h['tipo'] for h in hist_banca]
        cores = ['#00FF00' if t == 'GREEN' else '#FF4B4B' for t in tipos]

        fig, ax = plt.subplots(figsize=(10, 4))
        fig.patch.set_facecolor('#0E1117')
        ax.set_facecolor('#1A1C24')

        ax.plot(range(len(valores)), valores, color='#00BFFF', linewidth=2, zorder=3)
        ax.fill_between(range(len(valores)), valores, alpha=0.15, color='#00BFFF')

        for i, (v, c) in enumerate(zip(valores, cores)):
            ax.scatter(i, v, color=c, s=30, zorder=5)

        banca_ini = float(st.session_state.get('banca_inicial', 1000.0))
        ax.axhline(y=banca_ini, color='#FFD700', linestyle='--', alpha=0.5, label=f'Inicial: R$ {banca_ini:.0f}')

        ax.set_title('📈 Evolução da Banca', color='white', fontsize=14, fontweight='bold')
        ax.set_ylabel('R$', color='white')
        ax.tick_params(colors='white')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#333')
        ax.spines['bottom'].set_color('#333')
        ax.legend(facecolor='#1A1C24', edgecolor='#333', labelcolor='white')

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=100, bbox_inches='tight', facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()
    except:
        return None


def gerar_grafico_winrate_estrategias():
    """Gera gráfico de barras de winrate por estratégia."""
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty or 'Estrategia' not in df.columns:
        return None

    try:
        dados = []
        for est, grp in df.groupby('Estrategia'):
            g = len(grp[grp['Resultado'].str.contains('GREEN', na=False)])
            r = len(grp[grp['Resultado'].str.contains('RED', na=False)])
            t = g + r
            if t >= 3:
                wr = int((g / t) * 100)
                dados.append({'estrategia': str(est)[:20], 'winrate': wr, 'total': t})

        if not dados:
            return None

        dados.sort(key=lambda x: x['winrate'], reverse=True)
        dados = dados[:10]

        nomes = [d['estrategia'] for d in dados]
        winrates = [d['winrate'] for d in dados]
        cores = ['#00FF00' if wr >= 65 else ('#FFFF00' if wr >= 50 else '#FF4B4B') for wr in winrates]

        fig, ax = plt.subplots(figsize=(10, 5))
        fig.patch.set_facecolor('#0E1117')
        ax.set_facecolor('#1A1C24')

        bars = ax.barh(nomes[::-1], winrates[::-1], color=cores[::-1], edgecolor='#333')
        ax.axvline(x=65, color='#00FF00', linestyle='--', alpha=0.3, label='Meta 65%')
        ax.axvline(x=50, color='#FF4B4B', linestyle='--', alpha=0.3, label='Break-even 50%')

        for bar, wr in zip(bars, winrates[::-1]):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                   f'{wr}%', va='center', color='white', fontsize=10)

        ax.set_title('📊 Winrate por Estratégia', color='white', fontsize=14, fontweight='bold')
        ax.set_xlabel('Winrate %', color='white')
        ax.tick_params(colors='white')
        ax.set_xlim(0, 105)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#333')
        ax.spines['bottom'].set_color('#333')
        ax.legend(facecolor='#1A1C24', edgecolor='#333', labelcolor='white')

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=100, bbox_inches='tight', facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()
    except:
        return None


def enviar_graficos_bi(token, chat_id):
    """Envia gráficos do BI como imagens no Telegram."""
    grafico_banca = gerar_grafico_evolucao_banca()
    if grafico_banca:
        enviar_foto_telegram(token, chat_id, grafico_banca, "📈 Evolução da Banca — Neves Analytics")

    grafico_wr = gerar_grafico_winrate_estrategias()
    if grafico_wr:
        enviar_foto_telegram(token, chat_id, grafico_wr, "📊 Winrate por Estratégia — Neves Analytics")


# ==============================================================================
# 25. [COMPLEMENTO] RELATÓRIO BIGDATA NOTURNO
# ==============================================================================

def enviar_relatorio_bigdata(token, chat_id):
    """Envia relatório de dados BigData coletados no dia."""
    if st.session_state.get('bigdata_enviado', False): return
    if not db_firestore: return

    agora = get_time_br()
    if not (22 <= agora.hour <= 23): return

    total = st.session_state.get('total_bigdata_count', 0)
    if total == 0: return

    # Buscar resumo do Firestore
    try:
        hoje = agora.strftime('%Y-%m-%d')
        docs = db_firestore.collection('bigdata_jogos').where('data', '==', hoje).stream()

        total_jogos = 0
        total_gols = 0
        ligas_set = set()
        jogos_over25 = 0
        jogos_under25 = 0

        for doc in docs:
            d = doc.to_dict()
            total_jogos += 1
            gh = d.get('goals_home', 0)
            ga = d.get('goals_away', 0)
            total_gols += gh + ga
            ligas_set.add(d.get('liga', 'N/A'))
            if gh + ga > 2: jogos_over25 += 1
            else: jogos_under25 += 1

        if total_jogos == 0: return

        media_gols = round(total_gols / total_jogos, 1)
        pct_over = int((jogos_over25 / total_jogos) * 100)

        msg = f"💾 <b>RELATÓRIO BIGDATA</b>\n\n"
        msg += f"📅 Data: {hoje}\n"
        msg += f"⚽ Jogos coletados: {total_jogos}\n"
        msg += f"🏆 Ligas: {len(ligas_set)}\n"
        msg += f"⚽ Gols totais: {total_gols}\n"
        msg += f"📊 Média gols/jogo: {media_gols}\n"
        msg += f"📈 Over 2.5: {pct_over}% ({jogos_over25}/{total_jogos})\n"
        msg += f"📉 Under 2.5: {100 - pct_over}% ({jogos_under25}/{total_jogos})\n\n"
        msg += f"💡 Dados armazenados para machine learning futuro."

        enviar_telegram(token, chat_id, msg)
        st.session_state['bigdata_enviado'] = True
    except: pass


# ==============================================================================
# 26. [COMPLEMENTO] ROBUSTEZ — RETRY, RATE LIMIT, EDGE CASES
# ==============================================================================

def api_request_with_retry(url, headers, params, max_retries=2, timeout=15):
    """Faz request HTTP com retry automático em caso de falha."""
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            update_api_usage(r.headers)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            else:
                return {'response': []}
        except requests.exceptions.Timeout:
            if attempt < max_retries:
                time.sleep(2)
                continue
            return {'response': []}
        except:
            return {'response': []}
    return {'response': []}


def verificar_limite_api():
    """Verifica se estamos perto do limite da API."""
    used = st.session_state['api_usage']['used']
    limit = st.session_state['api_usage']['limit']
    if limit <= 0: return True
    pct = (used / limit) * 100
    if pct >= 95:
        return False  # Bloquear chamadas
    if pct >= 85:
        st.toast("⚠️ API próxima do limite diário (85%+)", icon="⚠️")
    return True


def limpar_caches_antigos():
    """Limpa caches antigos para liberar memória."""
    agora = get_time_br()

    # Limpar odd_history com mais de 30 minutos
    if 'odd_history' in st.session_state:
        limite = agora - timedelta(minutes=30)
        for fid in list(st.session_state['odd_history'].keys()):
            hist = st.session_state['odd_history'][fid]
            st.session_state['odd_history'][fid] = [x for x in hist if x.get('t') and x['t'] >= limite]
            if not st.session_state['odd_history'][fid]:
                del st.session_state['odd_history'][fid]

    # Limpar h2h_cache com mais de 2 horas
    if 'h2h_cache' in st.session_state:
        limite = agora - timedelta(hours=2)
        for key in list(st.session_state['h2h_cache'].keys()):
            if st.session_state['h2h_cache'][key]['timestamp'] < limite:
                del st.session_state['h2h_cache'][key]

    # Limpar var_avisado_cache (manter último 100)
    if len(st.session_state.get('var_avisado_cache', set())) > 100:
        st.session_state['var_avisado_cache'] = set(list(st.session_state['var_avisado_cache'])[-50:])


def salvar_estado_firestore():
    """Salva estado crítico no Firestore para persistência."""
    if not db_firestore: return
    try:
        estado = {
            'banca_atual': float(st.session_state.get('banca_atual', 0)),
            'banca_inicial': float(st.session_state.get('banca_inicial', 1000)),
            'modo_gestao': st.session_state.get('modo_gestao_banca', 'fracionario'),
            'total_sinais_hoje': len(st.session_state.get('alertas_enviados', set())),
            'timestamp': get_time_br().isoformat(),
            'data': get_time_br().strftime('%Y-%m-%d')
        }
        db_firestore.collection('estado_robo').document('atual').set(estado)
    except: pass


def carregar_estado_firestore():
    """Carrega estado salvo do Firestore."""
    if not db_firestore: return
    try:
        doc = db_firestore.collection('estado_robo').document('atual').get()
        if doc.exists:
            estado = doc.to_dict()
            hoje = get_time_br().strftime('%Y-%m-%d')
            if estado.get('data') == hoje:
                if 'banca_atual' not in st.session_state or st.session_state['banca_atual'] == float(st.session_state.get('banca_inicial', 1000)):
                    st.session_state['banca_atual'] = float(estado.get('banca_atual', st.session_state.get('banca_inicial', 1000)))
    except: pass


# ==============================================================================
# 27. [COMPLEMENTO] FORMATAÇÃO MATINAL AVANÇADA — H2H + TABELA
# ==============================================================================

def formatar_matinal_jogo_completo(api_key, jogo, form_h, form_a, zona_tipo, palpite, motivo):
    """Formata um jogo da matinal com contexto completo (H2H + Tabela)."""
    home = jogo['teams']['home']['name']
    away = jogo['teams']['away']['name']
    liga = jogo['league']['name']
    home_id = jogo['teams']['home']['id']
    away_id = jogo['teams']['away']['id']
    hora = jogo['fixture']['date'][11:16]

    msg = f"⚽ Jogo: {home} x {away}\n"
    msg += f"🏆 Liga: {liga} | ⏰ {hora}\n"
    msg += f"🎯 Palpite: {palpite}\n"

    # Odd pre-match
    odd_val, odd_desc = buscar_odd_pre_match(api_key, jogo['fixture']['id'])
    if odd_val > 0:
        msg += f"💰 Ref: @{odd_val:.2f} ({odd_desc})\n"

    # Motivo
    msg += f"📝 Motivo: {motivo}\n"

    # Tabela (se disponível)
    try:
        liga_id = jogo['league']['id']
        if int(liga_id) in LIGAS_TABELA:
            season = detectar_season(liga_id)
            tabela = buscar_tabela_liga(api_key, liga_id, season)
            if tabela:
                pos_h = obter_posicao_tabela(tabela, home_id)
                pos_a = obter_posicao_tabela(tabela, away_id)
                if pos_h and pos_a:
                    msg += f"📊 Tabela: {pos_h.get('rank', '?')}º ({pos_h.get('points', 0)}pts) vs {pos_a.get('rank', '?')}º ({pos_a.get('points', 0)}pts)\n"
    except: pass

    # H2H resumido
    try:
        h2h = buscar_h2h_cache(api_key, home_id, away_id, 8)
        if h2h and h2h.get('total', 0) >= 3:
            msg += f"🤝 H2H ({h2h['total']}j): Over 2.5 {h2h['pct_over25']}% | BTTS {h2h['pct_btts']}% | Média {h2h['media_gols']} gols\n"
    except: pass

    msg += "\n"
    return msg


# ==============================================================================
# 28. [COMPLEMENTO] MATINAL APRIMORADO — MENSAGEM JOGOS LIMPOS + H2H
# ==============================================================================

def formatar_matinal_jogo_limpo(api_key, token, chat_id):
    """Envia relatório matinal de Jogo Limpo (poucos cartões)."""
    if st.session_state.get('jogo_limpo_enviado', False): return
    if 'jogo_limpo_enviado' not in st.session_state:
        st.session_state['jogo_limpo_enviado'] = False

    agora = get_time_br()
    if not (7 <= agora.hour < 12): return

    hoje_str = agora.strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje_str, "timezone": "America/Sao_Paulo"}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=15)
        update_api_usage(r.headers)
        jogos = r.json().get('response', [])
    except: return

    df_black = st.session_state.get('df_black', pd.DataFrame())
    blacklist_ids = set(df_black['id'].values) if not df_black.empty else set()

    jogos_limpos = []
    for j in jogos[:8]:
        lid = str(j['league']['id'])
        if lid in blacklist_ids: continue
        if j['fixture']['status']['short'] != 'NS': continue

        home_id = j['teams']['home']['id']
        away_id = j['teams']['away']['id']

        stats_h = buscar_stats_cartoes_recentes(api_key, home_id, 8)
        stats_a = buscar_stats_cartoes_recentes(api_key, away_id, 8)

        media_total = stats_h['media_cartoes'] + stats_a['media_cartoes']
        if media_total <= 3.0:
            jogos_limpos.append({
                'home': j['teams']['home']['name'],
                'away': j['teams']['away']['name'],
                'liga': j['league']['name'],
                'media_cartoes': media_total,
                'media_h': stats_h['media_cartoes'],
                'media_a': stats_a['media_cartoes']
            })

    if jogos_limpos:
        msg = "🟩 <b>JOGO LIMPO (Poucos Cartões)</b>\n\n"
        for jl in jogos_limpos[:3]:
            msg += f"⚽ {jl['home']} x {jl['away']}\n"
            msg += f"🏆 {jl['liga']}\n"
            msg += f"🎯 Palpite: Menos de 3.5 Cartões\n"
            msg += f"📝 Motivo: Média {jl['media_cartoes']:.1f} cartões/jogo ({jl['media_h']:.1f} + {jl['media_a']:.1f})\n\n"
        enviar_telegram(token, chat_id, msg)
        st.session_state['jogo_limpo_enviado'] = True


# ==============================================================================
# 29. [COMPLEMENTO] SECOND DROP ODDS (16h)
# ==============================================================================

def executar_drop_odds_segundo(api_key, token, chat_id):
    """Segunda varredura de Drop Odds às 16h."""
    if st.session_state.get('drop_enviado_16', False): return
    agora = get_time_br()
    if not (16 <= agora.hour < 17): return

    oportunidades = scanner_drop_odds_pre_live(api_key)
    if oportunidades:
        op = oportunidades[0]
        msg = f"💰 <b>ESTRATÉGIA CASHOUT (DROP ODDS) — 2ª Varredura</b>\n\n"
        msg += f"⚽ {op['jogo']}\n🏆 {op['liga']} | ⏰ {op['hora']}\n\n"
        msg += f"📉 DESAJUSTE:\n• Bet365: @{op['odd_b365']:.2f}\n"
        msg += f"• Pinnacle: @{op['odd_pinnacle']:.2f}\n"
        msg += f"• Drop: {op['valor']:.1f}%\n\n"
        msg += f"⚙️ AÇÃO:\n1️⃣ Compre vitória {op['lado']}\n"
        msg += f"2️⃣ SAÍDA: Cashout ao igualar Pinnacle."
        enviar_telegram(token, chat_id, msg)
        st.session_state['drop_enviado_16'] = True


# ==============================================================================
# 30. [COMPLEMENTO] ALERTA HORÁRIO QUENTE
# ==============================================================================

def enviar_alerta_horario_quente(token, chat_id):
    """Alerta quando há muitos jogos começando nas próximas 2 horas."""
    if 'alerta_quente_enviado' not in st.session_state:
        st.session_state['alerta_quente_enviado'] = {}

    agora = get_time_br()
    chave = agora.strftime('%Y-%m-%d_%H')
    if chave in st.session_state['alerta_quente_enviado']:
        return

    # Contar jogos próximos
    sinais_pendentes = sum(1 for s in st.session_state.get('historico_sinais', [])
                          if 'PENDENTE' in str(s.get('Resultado', '')))

    if sinais_pendentes >= 5:
        msg = f"🔥 <b>HORÁRIO QUENTE!</b>\n\n"
        msg += f"📡 {sinais_pendentes} sinais ativos monitorados.\n"
        msg += f"⏰ Acompanhe de perto os próximos minutos.\n"
        msg += f"💡 Dica: Não adicione novas entradas se já tem 5+ sinais abertos."
        enviar_telegram(token, chat_id, msg)
        st.session_state['alerta_quente_enviado'][chave] = True


# ==============================================================================
# 31. [COMPLEMENTO] RESUMO MEIO-DIA
# ==============================================================================

def enviar_resumo_meio_dia(token, chat_id):
    """Envia resumo parcial ao meio-dia."""
    if st.session_state.get('resumo_meio_dia_enviado', False): return
    if 'resumo_meio_dia_enviado' not in st.session_state:
        st.session_state['resumo_meio_dia_enviado'] = False

    agora = get_time_br()
    if not (12 <= agora.hour < 13): return

    sinais = st.session_state.get('historico_sinais', [])
    if not sinais: return

    greens = sum(1 for s in sinais if 'GREEN' in str(s.get('Resultado', '')))
    reds = sum(1 for s in sinais if 'RED' in str(s.get('Resultado', '')))
    pendentes = sum(1 for s in sinais if 'PENDENTE' in str(s.get('Resultado', '')))
    total = greens + reds

    if total == 0 and pendentes == 0: return

    banca = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000)))
    wr = int((greens / total * 100)) if total > 0 else 0

    msg = f"📊 <b>RESUMO PARCIAL (Meio-Dia)</b>\n\n"
    msg += f"✅ Greens: {greens} | ❌ Reds: {reds} | ⏳ Pendentes: {pendentes}\n"
    if total > 0:
        msg += f"📈 Winrate: {wr}%\n"
    msg += f"💰 Banca: R$ {banca:.2f}\n\n"

    if wr >= 70 and total >= 3:
        msg += "🔥 Excelente manhã! Continue assim."
    elif wr < 50 and total >= 3:
        msg += "⚠️ Manhã difícil. Considere reduzir stakes."
    else:
        msg += "📊 Desempenho normal. Continue monitorando."

    enviar_telegram(token, chat_id, msg)
    st.session_state['resumo_meio_dia_enviado'] = True
