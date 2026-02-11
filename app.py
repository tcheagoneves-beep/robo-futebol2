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
# --- Constantes de Valor para o Semáforo ---
ODD_MINIMA_LIVE = 1.60  # Meta de valor
ODD_CRITICA_LIVE = 1.30 # Abaixo disso é perigo
# -------------------------------------------

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

# --- VARIÁVEIS PARA MÚLTIPLAS, TRADING E ALAVANCAGEM ---
if 'multipla_matinal_enviada' not in st.session_state: st.session_state['multipla_matinal_enviada'] = False
if 'multiplas_live_cache' not in st.session_state: st.session_state['multiplas_live_cache'] = {}
if 'multiplas_pendentes' not in st.session_state: st.session_state['multiplas_pendentes'] = []
if 'alternativos_enviado' not in st.session_state: st.session_state['alternativos_enviado'] = False
if 'alavancagem_enviada' not in st.session_state: st.session_state['alavancagem_enviada'] = False 
if 'drop_enviado_12' not in st.session_state: st.session_state['drop_enviado_12'] = False
if 'drop_enviado_16' not in st.session_state: st.session_state['drop_enviado_16'] = False

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

# Mapa para referência e simulação de odds
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
# 2. FUNÇÕES AUXILIARES, DADOS E API
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
# [MELHORIA V3] CLASSIFICAÇÃO (OVER/UNDER) + GOLS + ODD MOVEMENT + KELLY
# ==============================================================================
def classificar_tipo_estrategia(estrategia: str) -> str:
    '''
    [PATCH V5.2] Classifica CORRETAMENTE se a estratégia é OVER, UNDER ou RESULTADO.
    
    OVER: Aposta que SAI GOL (GREEN quando sai gol)
    UNDER: Aposta que NÃO SAI GOL (GREEN quando não sai gol)
    RESULTADO: Aposta em vitória/empate (GREEN quando time ganha/mantém)
    '''
    estrategia_lower = str(estrategia or '').lower()
    
    # ─────────────────────────────────────────────────────────────────────
    # ESTRATÉGIAS DE OVER (Aposta que SAI GOL)
    # ─────────────────────────────────────────────────────────────────────
    over_keywords = [
        'porteira aberta',      # 🟣 Porteira Aberta
        'golden bet',           # 💎 Golden Bet
        'gol relâmpago',        # ⚡ Gol Relâmpago
        'gol relampago',        # (sem acento)
        'blitz casa',           # 🟢 Blitz Casa
        'blitz visitante',      # 🟢 Blitz Visitante
        'blitz',                # (genérico)
        'massacre',             # 🔥 Massacre
        'choque',               # ⚔️ Choque Líderes
        'briga de rua',         # 🥊 Briga de Rua
        'escanteios',           # 🏴 Escanteios
        'escanteio',            # (singular)
        'corner',               # Corner (inglês)
        'janela de ouro',       # 💰 Janela de Ouro
        'janela ouro',          # (sem "de")
        'tiroteio elite',       # 🏹 Tiroteio Elite
        'sniper final',         # 💎 Sniper Final
        'sniper matinal',       # Sniper Matinal
        'lay goleada',          # 🔫 Lay Goleada
        'gigante dormindo',     # Gigante Dormindo (se existir)
        'reação do gigante',    # Reação do Gigante (se existir)
        'over',                 # Genérico OVER
        'btts',                 # Both Teams to Score
        'ambas marcam',         # Ambas Marcam
        'gol',                  # (genérico se contém "gol")
    ]
    
    # ─────────────────────────────────────────────────────────────────────
    # ESTRATÉGIAS DE UNDER (Aposta que NÃO SAI GOL)
    # ─────────────────────────────────────────────────────────────────────
    under_keywords = [
        'jogo morno',           # ❄️ Jogo Morno
        'arame liso',           # 🧊 Arame Liso
        'under',                # Genérico UNDER
        'sem gols',             # Sem Gols
        'morno',                # (simplificado)
        'arame',                # (simplificado)
    ]
    
    # ─────────────────────────────────────────────────────────────────────
    # ESTRATÉGIAS DE RESULTADO (Aposta em vitória/empate)
    # ─────────────────────────────────────────────────────────────────────
    resultado_keywords = [
        'estratégia do vovô',   # 👴 Estratégia do Vovô
        'estrategia do vovo',   # (sem acento)
        'vovô',                 # (simplificado)
        'vovo',                 # (sem acento)
        'contra-ataque',        # ⚡ Contra-Ataque Letal
        'contra ataque',        # (sem hífen)
        'back',                 # Back (apostar no favorito)
        'segurar',              # Segurar resultado
        'manter',               # Manter resultado
        'vitória',              # Vitória
        'vitoria',              # (sem acento)
        'empate',               # Empate
    ]
    
    # ─────────────────────────────────────────────────────────────────────
    # ORDEM DE CHECAGEM (IMPORTANTE: Mais específico primeiro)
    # ─────────────────────────────────────────────────────────────────────
    
    # 1. Checa RESULTADO primeiro (para evitar conflito com "estratégia do vovô")
    for keyword in resultado_keywords:
        if keyword in estrategia_lower:
            return 'RESULTADO'
    
    # 2. Checa UNDER
    for keyword in under_keywords:
        if keyword in estrategia_lower:
            return 'UNDER'
    
    # 3. Checa OVER
    for keyword in over_keywords:
        if keyword in estrategia_lower:
            return 'OVER'
    
    # 4. Se não matchou nada, assume NEUTRO (não deveria acontecer)
    return 'NEUTRO'


def obter_descricao_aposta(estrategia: str) -> dict:
    '''
    [NOVO V5.2] Retorna descrição da aposta para cada estratégia.
    
    Retorna dict com:
        'tipo': OVER/UNDER/RESULTADO
        'aposta': Descrição da aposta
        'ordem': Texto para Telegram (o que fazer)
        'ganha_se': Quando GREEN
        'perde_se': Quando RED
    '''
    tipo = classificar_tipo_estrategia(estrategia)
    estrategia_lower = str(estrategia or '').lower()
    
    # ─────────────────────────────────────────────────────────────────────
    # MAPEAMENTO ESTRATÉGIA → DESCRIÇÃO
    # ─────────────────────────────────────────────────────────────────────
    
    if 'golden bet' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }
    
    elif 'janela' in estrategia_lower and 'ouro' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }
    
    elif 'gol relâmpago' in estrategia_lower or 'gol relampago' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols no 1º Tempo',
            'ordem': '👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT',
            'ganha_se': 'Sai GOL no 1º Tempo',
            'perde_se': 'Não sai GOL no 1º Tempo'
        }
    
    elif 'blitz' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }
    
    elif 'porteira' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }
    
    elif 'tiroteio' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }
    
    elif 'sniper' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE nos ACRÉSCIMOS\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL nos acréscimos',
            'perde_se': 'Não sai GOL'
        }
    
    elif 'lay goleada' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE (gol da honra)\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }
    
    elif 'massacre' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols no 1º Tempo',
            'ordem': '👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT',
            'ganha_se': 'Sai GOL no 1º Tempo',
            'perde_se': 'Não sai GOL no 1º Tempo'
        }
    
    elif 'choque' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols no 1º Tempo',
            'ordem': '👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT',
            'ganha_se': 'Sai GOL no 1º Tempo',
            'perde_se': 'Não sai GOL no 1º Tempo'
        }
    
    elif 'briga de rua' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols no 1º Tempo',
            'ordem': '👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT',
            'ganha_se': 'Sai GOL no 1º Tempo',
            'perde_se': 'Não sai GOL no 1º Tempo'
        }
    
    elif 'contra-ataque' in estrategia_lower or 'contra ataque' in estrategia_lower:
        return {
            'tipo': 'RESULTADO',
            'aposta': 'Back Empate ou Zebra',
            'ordem': '👉 FAZER: Back no time que está PERDENDO\n⚡ Aposta: Recuperação ou Empate',
            'ganha_se': 'Time EMPATA ou VIRA',
            'perde_se': 'Time que está ganhando MANTÉM'
        }
    
    # ─── UNDER STRATEGIES ───
    
    elif 'jogo morno' in estrategia_lower or 'morno' in estrategia_lower:
        return {
            'tipo': 'UNDER',
            'aposta': 'Menos de 0.5 Gols (Under)',
            'ordem': '👉 FAZER: Entrar em UNDER GOLS\n❄️ Aposta: NÃO SAI mais gol',
            'ganha_se': 'NÃO sai GOL',
            'perde_se': 'Sai GOL'
        }
    
    elif 'arame liso' in estrategia_lower or 'arame' in estrategia_lower:
        return {
            'tipo': 'UNDER',
            'aposta': 'Menos de 0.5 Gols (Under)',
            'ordem': '👉 FAZER: Entrar em UNDER GOLS\n🧊 Aposta: Falsa pressão, NÃO SAI gol',
            'ganha_se': 'NÃO sai GOL (falsa pressão confirmada)',
            'perde_se': 'Sai GOL'
        }
    
    # ─── RESULTADO STRATEGIES ───
    
    elif 'vovô' in estrategia_lower or 'vovo' in estrategia_lower:
        return {
            'tipo': 'RESULTADO',
            'aposta': 'Vitória do time que está ganhando',
            'ordem': '👉 FAZER: Back no time que está GANHANDO\n👴 Aposta: Time manterá a vitória',
            'ganha_se': 'Time MANTÉM ou AUMENTA vantagem',
            'perde_se': 'Time EMPATA ou PERDE'
        }
    
    # ─── FALLBACK GENÉRICO ───
    
    elif tipo == 'OVER':
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }
    
    elif tipo == 'UNDER':
        return {
            'tipo': 'UNDER',
            'aposta': 'Menos de 0.5 Gols',
            'ordem': '👉 FAZER: Entrar em UNDER GOLS\n❄️ Aposta: NÃO SAI mais gol',
            'ganha_se': 'NÃO sai GOL',
            'perde_se': 'Sai GOL'
        }
    
    else:  # RESULTADO ou NEUTRO
        return {
            'tipo': 'RESULTADO',
            'aposta': 'Resultado Final',
            'ordem': '👉 FAZER: Apostar no resultado indicado',
            'ganha_se': 'Resultado se confirma',
            'perde_se': 'Resultado não se confirma'
        }

def calcular_gols_atuais(placar_str: str) -> int:
    '''Calcula gols atuais (ex: 2x1 -> 3).'''
    try:
        gh, ga = map(int, str(placar_str).lower().replace(' ', '').split('x'))
        return int(gh) + int(ga)
    except:
        return 0


def calcular_threshold_dinamico(estrategia: str, odd_atual: float) -> int:
    '''Threshold dinâmico por estratégia + odd (50-80).'''
    estr = str(estrategia or '')
    tipo = classificar_tipo_estrategia(estr)
    if tipo == 'UNDER':
        thr = 65
    elif 'golden' in estr.lower() or 'diamante' in estr.lower():
        thr = 75
    else:
        thr = 50

    try:
        odd = float(odd_atual)
        if odd >= 2.00:
            thr -= 5
        elif odd <= 1.30:
            thr += 10
    except:
        pass

    return int(max(50, min(thr, 80)))


def rastrear_movimento_odd(fid, estrategia, odd_atual, janela_min=5):
    '''Rastreia movimento de odd em memória (últimos X min).'''
    try:
        fid = str(fid)
        odd_atual = float(odd_atual)
    except:
        return 'DESCONHECIDO', 0.0

    if 'odd_history' not in st.session_state:
        st.session_state['odd_history'] = {}

    hist = st.session_state['odd_history'].get(fid, [])
    agora = get_time_br()
    hist.append({'t': agora, 'odd': odd_atual, 'estrategia': str(estrategia)})

    limite = agora - timedelta(minutes=int(janela_min))
    hist = [x for x in hist if x.get('t') and x['t'] >= limite]
    st.session_state['odd_history'][fid] = hist

    if len(hist) < 2:
        return 'ESTÁVEL', 0.0

    odd_ini = hist[0]['odd']
    if odd_ini <= 0:
        return 'ESTÁVEL', 0.0

    variacao = ((odd_atual - odd_ini) / odd_ini) * 100.0

    if variacao <= -7:
        return 'CAINDO FORTE', variacao
    if variacao <= -3:
        return 'CAINDO', variacao
    if variacao >= 7:
        return 'SUBINDO FORTE', variacao
    if variacao >= 3:
        return 'SUBINDO', variacao
    return 'ESTÁVEL', variacao


def calcular_kelly_criterion(probabilidade, odd, modo='fracionario'):
    '''Kelly % ideal (prob 0-100).'''
    try:
        prob_decimal = float(probabilidade) / 100.0
        odd = float(odd)
        if odd <= 1.01:
            return 0.0
        kelly = (prob_decimal * odd - 1) / (odd - 1)
        if kelly <= 0:
            kelly = 0
        elif kelly > 0.25:
            kelly = 0.25

        if modo == 'conservador':
            if float(probabilidade) >= 85:
                return 2.0
            if float(probabilidade) >= 70:
                return 1.5
            return 1.0

        if modo == 'fracionario':
            kelly *= 0.5

        k = round(kelly * 100.0, 1)
        if 0 < k < 0.5:
            k = 0.5
        return k
    except:
        return 1.5


def calcular_stake_recomendado(banca_atual, probabilidade, odd, modo='fracionario'):
    '''Stake recomendado em % e R$.'''
    try:
        banca_atual = float(banca_atual)
        pct = calcular_kelly_criterion(probabilidade, odd, modo)
        valor = round((banca_atual * pct) / 100.0, 2)
        if 0 < valor < 2.0:
            valor = 2.0
        return {'porcentagem': pct, 'valor': valor, 'modo': modo}
    except:
        return {'porcentagem': 1.5, 'valor': round(float(banca_atual or 0) * 0.015, 2), 'modo': 'erro'}


# ==============================================================================
# [CORREÇÃO CRÍTICA] ODDS MÍNIMAS POR ESTRATÉGIA
# ==============================================================================
ODD_MINIMA_POR_ESTRATEGIA = {
    "estratégia do vovô": 1.20,
    "vovô": 1.20,
    "jogo morno": 1.35,
    "morno": 1.35,
    "porteira aberta": 1.50,
    "porteira": 1.50,
    "golden bet": 1.80,
    "golden": 1.80,
    "gol relâmpago": 1.60,
    "relâmpago": 1.60,
    "blitz": 1.60,
    "massacre": 1.70,
    "alavancagem": 3.50,
    "sniper": 1.80,
    "arame liso": 1.35,
    "under": 1.40,
}

def obter_odd_minima(estrategia):
    """Retorna a odd mínima aceitável para a estratégia. Padrão 1.50 se não encontrar."""
    try:
        estrategia_lower = str(estrategia or '').lower()
        for chave, odd_min in ODD_MINIMA_POR_ESTRATEGIA.items():
            if chave in estrategia_lower:
                return float(odd_min)
        return 1.50
    except:
        return 1.50

# --- [MELHORIA] NOVA FUNÇÃO DE BUSCA DE ODD PRÉ-MATCH (ROBUSTA) ---
def buscar_odd_pre_match(api_key, fid):
    try:
        url = "https://v3.football.api-sports.io/odds"
        params = {"fixture": fid, "bookmaker": "8"} # ID 8 = Bet365
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        
        if not r.get('response'): return 0.0, "Sem Bet365"
        
        bookmakers = r['response'][0]['bookmakers']
        if not bookmakers: return 0.0, "Sem Bet365"

        bet365 = bookmakers[0]
            
        if bet365:
            # Procura Over 2.5 (ID 5 na API)
            mercado_gols = next((m for m in bet365['bets'] if m['id'] == 5), None)
            if mercado_gols:
                # Tenta pegar linha 2.5
                odd_obj = next((v for v in mercado_gols['values'] if v['value'] == "Over 2.5"), None)
                # Se não tiver 2.5, tenta 1.5 (fallback para jogos under)
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
        used = limit - remaining
        st.session_state['api_usage'] = {'used': used, 'limit': limit}
    except: pass

def verificar_reset_diario():
    hoje_utc = datetime.now(pytz.utc).date()
    if st.session_state['data_api_usage'] != hoje_utc:
        st.session_state['api_usage']['used'] = 0; st.session_state['data_api_usage'] = hoje_utc
        st.session_state['gemini_usage']['used'] = 0
        st.session_state['alvos_do_dia'] = {}
        st.session_state['matinal_enviado'] = False
        st.session_state['multipla_matinal_enviada'] = False
        st.session_state['alternativos_enviado'] = False
        st.session_state['alavancagem_enviada'] = False 
        st.session_state['drop_enviado_12'] = False
        st.session_state['drop_enviado_16'] = False
        return True
    return False

def testar_conexao_telegram(token):
    if not token: return False, "Token Vazio"
    try:
        res = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=5)
        if res.status_code == 200:
            return True, res.json()['result']['first_name']
        return False, f"Erro {res.status_code}"
    except:
        return False, "Sem Conexão"

def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    df_raw = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

# [MÓDULO] ESTRATÉGIA CASHOUT / DROP ODDS (PRÉ-LIVE)
def buscar_odds_comparativas(api_key, fixture_id):
    url = "https://v3.football.api-sports.io/odds"
    try:
        params_b365 = {"fixture": fixture_id, "bookmaker": "8"} 
        params_pin = {"fixture": fixture_id, "bookmaker": "4"}
        
        r365 = requests.get(url, headers={"x-apisports-key": api_key}, params=params_b365).json()
        rpin = requests.get(url, headers={"x-apisports-key": api_key}, params=params_pin).json()
        
        odd_365 = 0; odd_pin = 0; time_alvo = ""
        
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
        params = {"date": hoje_str, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        
        LIGAS_PERMITIDAS = [39, 140, 78, 135, 61, 2, 3] 
        oportunidades = []
        
        for j in jogos:
            lid = j['league']['id']
            fid = j['fixture']['id']
            if lid not in LIGAS_PERMITIDAS: continue
            
            dt_jogo = j['fixture']['date']
            try:
                hora_jogo = datetime.fromisoformat(dt_jogo.replace('Z', '+00:00'))
            except:
                 hora_jogo = datetime.strptime(dt_jogo[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.utc)

            agora_utc = datetime.now(pytz.utc)
            diff = (hora_jogo - agora_utc).total_seconds() / 3600
            
            if j['fixture']['status']['short'] != 'NS': continue
            if not (3 <= diff <= 8): continue 
            
            odd_b365, odd_pin, lado = buscar_odds_comparativas(api_key, fid)
            
            if odd_b365 > 0 and lado:
                diferenca = ((odd_b365 - odd_pin) / odd_pin) * 100
                oportunidades.append({
                    "fid": fid,
                    "jogo": f"{j['teams']['home']['name']} x {j['teams']['away']['name']}",
                    "liga": j['league']['name'],
                    "hora": j['fixture']['date'][11:16],
                    "lado": lado,
                    "odd_b365": odd_b365,
                    "odd_pinnacle": odd_pin,
                    "valor": diferenca
                })
        return oportunidades
    except Exception as e: return []

# --- GERENCIAMENTO DE PLANILHAS E DADOS ---

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
            if not df_ram.empty:
                st.toast(f"⚠️ Erro leitura {nome_aba}. Usando Cache.", icon="🛡️")
                return df_ram
        if not df.empty:
            for col in colunas_esperadas:
                if col not in df.columns:
                    if col == 'Probabilidade': df[col] = "0"
                    else: df[col] = "1.20" if col == 'Odd' else ""
            return df.fillna("").astype(str)
        return pd.DataFrame(columns=colunas_esperadas)
    except Exception as e:
        if chave_memoria and chave_memoria in st.session_state:
            df_ram = st.session_state[chave_memoria]
            if not df_ram.empty: return df_ram
        st.session_state['BLOQUEAR_SALVAMENTO'] = True
        return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df_para_salvar):
    if nome_aba in ["Historico", "Seguras", "Obs"] and df_para_salvar.empty: return False
    if st.session_state.get('BLOQUEAR_SALVAMENTO', False):
        st.session_state['precisa_salvar'] = True 
        return False
    try:
        conn.update(worksheet=nome_aba, data=df_para_salvar)
        if nome_aba == "Historico": st.session_state['precisa_salvar'] = False
        return True
    except: 
        st.session_state['precisa_salvar'] = True
        return False

def salvar_blacklist(id_liga, pais, nome_liga, motivo_ban):
    df = st.session_state['df_black']
    id_norm = normalizar_id(id_liga)
    if id_norm in df['id'].values:
        idx = df[df['id'] == id_norm].index[0]
        df.at[idx, 'Motivo'] = str(motivo_ban)
    else:
        novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': str(motivo_ban)}])
        df = pd.concat([df, novo], ignore_index=True)
    st.session_state['df_black'] = df
    salvar_aba("Blacklist", df)
    sanitizar_conflitos()

def sanitizar_conflitos():
    df_black = st.session_state.get('df_black', pd.DataFrame())
    df_vip = st.session_state.get('df_vip', pd.DataFrame())
    df_safe = st.session_state.get('df_safe', pd.DataFrame())
    if df_black.empty or df_vip.empty or df_safe.empty: return
    alterou_black, alterou_vip, alterou_safe = False, False, False
    for idx, row in df_black.iterrows():
        id_b = normalizar_id(row['id'])
        motivo_atual = str(row['Motivo'])
        df_vip['id_norm'] = df_vip['id'].apply(normalizar_id)
        mask_vip = df_vip['id_norm'] == id_b
        if mask_vip.any():
            strikes = formatar_inteiro_visual(df_vip.loc[mask_vip, 'Strikes'].values[0])
            novo_motivo = f"Banida ({strikes} Jogos Sem Dados)"
            if motivo_atual != novo_motivo:
                df_black.at[idx, 'Motivo'] = novo_motivo
                alterou_black = True
            df_vip = df_vip[~mask_vip]
            alterou_vip = True
        df_safe['id_norm'] = df_safe['id'].apply(normalizar_id)
        mask_safe = df_safe['id_norm'] == id_b
        if mask_safe.any():
            df_safe = df_safe[~mask_safe]
            alterou_safe = True
    if 'id_norm' in df_vip.columns: df_vip = df_vip.drop(columns=['id_norm'])
    if 'id_norm' in df_safe.columns: df_safe = df_safe.drop(columns=['id_norm'])
    if alterou_black: st.session_state['df_black'] = df_black; salvar_aba("Blacklist", df_black)
    if alterou_vip: st.session_state['df_vip'] = df_vip; salvar_aba("Obs", df_vip)
    if alterou_safe: st.session_state['df_safe'] = df_safe; salvar_aba("Seguras", df_safe)

def salvar_safe_league_basic(id_liga, pais, nome_liga, tem_tabela=False):
    id_norm = normalizar_id(id_liga)
    df = st.session_state['df_safe']
    txt_motivo = "Validada (Chutes + Tabela)" if tem_tabela else "Validada (Chutes)"
    if id_norm not in df['id'].values:
        novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': txt_motivo, 'Strikes': '0', 'Jogos_Erro': ''}])
        final = pd.concat([df, novo], ignore_index=True)
        if salvar_aba("Seguras", final): st.session_state['df_safe'] = final; sanitizar_conflitos()
    else:
        idx = df[df['id'] == id_norm].index[0]
        if df.at[idx, 'Motivo'] != txt_motivo:
            df.at[idx, 'Motivo'] = txt_motivo
            if salvar_aba("Seguras", df): st.session_state['df_safe'] = df

def resetar_erros(id_liga):
    id_norm = normalizar_id(id_liga)
    df_safe = st.session_state.get('df_safe', pd.DataFrame())
    if not df_safe.empty and id_norm in df_safe['id'].values:
        idx = df_safe[df_safe['id'] == id_norm].index[0]
        if str(df_safe.at[idx, 'Strikes']) != '0':
            df_safe.at[idx, 'Strikes'] = '0'; df_safe.at[idx, 'Jogos_Erro'] = ''
            if salvar_aba("Seguras", df_safe): st.session_state['df_safe'] = df_safe

def gerenciar_erros(id_liga, pais, nome_liga, fid_jogo):
    id_norm = normalizar_id(id_liga)
    fid_str = str(fid_jogo)
    df_safe = st.session_state.get('df_safe', pd.DataFrame())
    if not df_safe.empty and id_norm in df_safe['id'].values:
        idx = df_safe[df_safe['id'] == id_norm].index[0]
        jogos_erro = str(df_safe.at[idx, 'Jogos_Erro']).split(',') if str(df_safe.at[idx, 'Jogos_Erro']).strip() else []
        if fid_str in jogos_erro: return 
        jogos_erro.append(fid_str)
        strikes = len(jogos_erro)
        if strikes >= 10:
            df_safe = df_safe.drop(idx)
            salvar_aba("Seguras", df_safe); st.session_state['df_safe'] = df_safe
            df_vip = st.session_state.get('df_vip', pd.DataFrame())
            novo_obs = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Data_Erro': get_time_br().strftime('%Y-%m-%d'), 'Strikes': '1', 'Jogos_Erro': fid_str}])
            final_vip = pd.concat([df_vip, novo_obs], ignore_index=True)
            salvar_aba("Obs", final_vip); st.session_state['df_vip'] = final_vip
        else:
            df_safe.at[idx, 'Strikes'] = str(strikes); df_safe.at[idx, 'Jogos_Erro'] = ",".join(jogos_erro)
            salvar_aba("Seguras", df_safe); st.session_state['df_safe'] = df_safe
        return
    df_vip = st.session_state.get('df_vip', pd.DataFrame())
    strikes = 0; jogos_erro = []
    if not df_vip.empty and id_norm in df_vip['id'].values:
        row = df_vip[df_vip['id'] == id_norm].iloc[0]
        val_jogos = str(row.get('Jogos_Erro', '')).strip()
        if val_jogos: jogos_erro = val_jogos.split(',')
    if fid_str in jogos_erro: return
    jogos_erro.append(fid_str)
    strikes = len(jogos_erro)
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
            final = pd.concat([df_vip, novo], ignore_index=True)
            salvar_aba("Obs", final); st.session_state['df_vip'] = final

def carregar_tudo(force=False):
    now = time.time()
    if force or (now - st.session_state['last_static_update']) > STATIC_CACHE_TIME or 'df_black' not in st.session_state:
        st.session_state['df_black'] = carregar_aba("Blacklist", COLS_BLACK)
        st.session_state['df_safe'] = carregar_aba("Seguras", COLS_SAFE)
        st.session_state['df_vip'] = carregar_aba("Obs", COLS_OBS)
        if not st.session_state['df_black'].empty: st.session_state['df_black']['id'] = st.session_state['df_black']['id'].apply(normalizar_id)
        if not st.session_state['df_safe'].empty: st.session_state['df_safe']['id'] = st.session_state['df_safe']['id'].apply(normalizar_id)
        if not st.session_state['df_vip'].empty: st.session_state['df_vip']['id'] = st.session_state['df_vip']['id'].apply(normalizar_id)
        sanitizar_conflitos()
        st.session_state['last_static_update'] = now
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
                st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
                st.session_state['historico_sinais'] = []
    if 'jogos_salvos_bigdata_carregados' not in st.session_state or not st.session_state['jogos_salvos_bigdata_carregados'] or force:
        st.session_state['jogos_salvos_bigdata_carregados'] = True
    st.session_state['last_db_update'] = now

def adicionar_historico(item):
    if 'historico_full' not in st.session_state: st.session_state['historico_full'] = carregar_aba("Historico", COLS_HIST)
    df_memoria = st.session_state['historico_full']

    # [MELHORIA] Stake recomendado (Kelly) no histórico
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
        chave = f"{row['FID']}_{row['Estrategia']}"
        mapa_atualizacao[chave] = row
    def atualizar_linha(row):
        chave = f"{row['FID']}_{row['Estrategia']}"
        if chave in mapa_atualizacao:
            nova_linha = mapa_atualizacao[chave]
            if str(row['Resultado']) != str(nova_linha['Resultado']): st.session_state['precisa_salvar'] = True
            return nova_linha
        return row
    df_final = df_memoria.apply(atualizar_linha, axis=1)
    st.session_state['historico_full'] = df_final

# --- [RECUPERADO] FUNÇÃO ESSENCIAL DO BIG DATA ---
def consultar_bigdata_cenario_completo(home_id, away_id):
    if not db_firestore: return "Big Data Offline"
    try:
        # Aumentei o limite para 50 jogos para ter amostra estatística relevante
        docs_h = db_firestore.collection("BigData_Futebol").where("home_id", "==", str(home_id)).order_by("data_hora", direction=firestore.Query.DESCENDING).limit(50).stream()
        docs_a = db_firestore.collection("BigData_Futebol").where("away_id", "==", str(away_id)).order_by("data_hora", direction=firestore.Query.DESCENDING).limit(50).stream()
        
        def safe_get(stats_dict, key):
            try: return float(stats_dict.get(key, 0))
            except: return 0.0
            
        # Listas para a IA analisar a variância (Consistência)
        h_placares = []; h_cantos = []
        for d in docs_h:
            dd = d.to_dict(); st = dd.get('estatisticas', {})
            h_placares.append(dd.get('placar_final', '?'))
            h_cantos.append(int(safe_get(st, 'escanteios_casa')))
            
        a_placares = []; a_cantos = []
        for d in docs_a:
            dd = d.to_dict(); st = dd.get('estatisticas', {})
            a_placares.append(dd.get('placar_final', '?'))
            a_cantos.append(int(safe_get(st, 'escanteios_fora')))

        if not h_placares and not a_placares: return "Sem dados suficientes."

        # Montamos a string RAW para a IA
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
        if 'API_KEY' in st.session_state:
            try:
                url_stats = "https://v3.football.api-sports.io/fixtures/players"
                p_res = requests.get(url_stats, headers={"x-apisports-key": st.session_state['API_KEY']}, params={"fixture": fid}).json()
                if p_res.get('response'):
                    for t in p_res['response']:
                        is_h = (t['team']['id'] == jogo_api['teams']['home']['id'])
                        notas = []
                        for p in t['players']:
                            try:
                                rating = float(p['statistics'][0]['games']['rating'])
                                if rating > 0: notas.append(rating)
                            except: pass
                        if notas:
                            media = sum(notas)/len(notas)
                            if is_h: rate_h = media
                            else: rate_a = media
            except: pass
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
                'impedimentos': gv(s1, 'Offsides') + gv(s2, 'Offsides'),
                'passes_pct_casa': str(gv(s1, 'Passes %')).replace('%',''), 'passes_pct_fora': str(gv(s2, 'Passes %')).replace('%','')
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
            params = {"team": team_id, "last": "20", "status": "FT"}
            res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
            jogos = res.get('response', [])
            gols_marcados = 0; jogos_contados = 0
            for j in jogos:
                is_home_match = (j['teams']['home']['id'] == team_id)
                if location_filter == 'home' and is_home_match:
                    gols_marcados += (j['goals']['home'] or 0); jogos_contados += 1
                elif location_filter == 'away' and not is_home_match:
                    gols_marcados += (j['goals']['away'] or 0); jogos_contados += 1
                if jogos_contados >= 10: break 
            if jogos_contados == 0: return "0.00"
            return "{:.2f}".format(gols_marcados / jogos_contados)
        return {'home': get_avg_goals(home_id, 'home'), 'away': get_avg_goals(away_id, 'away')}
    except: return {'home': '?', 'away': '?'}

@st.cache_data(ttl=86400)
def analisar_tendencia_50_jogos(api_key, home_id, away_id):
    try:
        def get_stats_50(team_id):
            url = "https://v3.football.api-sports.io/fixtures"
            # BUSCA OS ÚLTIMOS 50 JOGOS REAIS
            params = {"team": team_id, "last": "50", "status": "FT"}
            res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
            jogos = res.get('response', [])
            
            if not jogos: return {"qtd": 0, "win": 0, "draw": 0, "loss": 0, "over25": 0, "btts": 0}
            
            stats = {"qtd": len(jogos), "win": 0, "draw": 0, "loss": 0, "over25": 0, "btts": 0}
            
            for j in jogos:
                gh = j['goals']['home'] or 0
                ga = j['goals']['away'] or 0
                
                # Check Over 2.5 e BTTS
                if (gh + ga) > 2: stats["over25"] += 1
                if gh > 0 and ga > 0: stats["btts"] += 1
                
                # Check Vencedor (Winrate)
                is_home = (j['teams']['home']['id'] == team_id)
                if is_home:
                    if gh > ga: stats["win"] += 1
                    elif gh == ga: stats["draw"] += 1
                    else: stats["loss"] += 1
                else: # Visitante
                    if ga > gh: stats["win"] += 1
                    elif ga == gh: stats["draw"] += 1
                    else: stats["loss"] += 1
            
            # Converte para Porcentagem Inteira
            total = stats["qtd"]
            return {
                "win": int((stats["win"] / total) * 100),
                "draw": int((stats["draw"] / total) * 100),
                "loss": int((stats["loss"] / total) * 100),
                "over25": int((stats["over25"] / total) * 100),
                "btts": int((stats["btts"] / total) * 100),
                "qtd": total
            }
            
        return {"home": get_stats_50(home_id), "away": get_stats_50(away_id)}
    except: return None

@st.cache_data(ttl=86400)
def analisar_tendencia_macro_micro(api_key, home_id, away_id):
    try:
        # Função auxiliar para buscar stats de um jogo específico (Cartões)
        def get_card_stats_single_game(fid, team_id):
            try:
                url = "https://v3.football.api-sports.io/fixtures/statistics"
                r = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fid, "team": team_id}, timeout=2)
                d = r.json()
                if d.get('response'):
                    stats = d['response'][0]['statistics']
                    # Busca segura dos valores
                    yc = next((x['value'] for x in stats if x['type'] == 'Yellow Cards'), 0) or 0
                    rc = next((x['value'] for x in stats if x['type'] == 'Red Cards'), 0) or 0
                    return int(yc), int(rc)
                return 0, 0
            except: return 0, 0

        def get_team_stats_unified(team_id):
            url = "https://v3.football.api-sports.io/fixtures"
            # Pedimos os últimos 10 jogos
            params = {"team": team_id, "last": "10", "status": "FT"} 
            res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
            jogos = res.get('response', [])
            
            if not jogos: return "Sem dados.", 0, 0, 0
            
            resumo_txt = ""
            over_gols_count = 0
            sequencia = []
            total_gols_marcados = 0
            
            # --- PARTE 1: GOLS (Últimos 5 Jogos - Formato Simples) ---
            for j in jogos[:5]: 
                adv = j['teams']['away']['name'] if j['teams']['home']['id'] == team_id else j['teams']['home']['name']
                goals_home = j['goals']['home']
                goals_away = j['goals']['away']
                
                # Determina se time jogou em casa ou fora
                if j['teams']['home']['id'] == team_id:
                    # Jogou em casa
                    gols_time = goals_home
                    gols_adv = goals_away
                else:
                    # Jogou fora
                    gols_time = goals_away
                    gols_adv = goals_home
                
                # Sequência V/E/D
                if gols_time > gols_adv:
                    sequencia.append('V')
                elif gols_time == gols_adv:
                    sequencia.append('E')
                else:
                    sequencia.append('D')
                
                total_gols_marcados += gols_time
                
                if (goals_home + goals_away) >= 2: over_gols_count += 1
            
            # Monta texto resumido
            seq_str = ' '.join(sequencia)  # Ex: "V V E D V"
            media_gols = total_gols_marcados / 5 if len(jogos) >= 5 else 0
            resumo_txt = f"{seq_str} | Média {media_gols:.1f} gols/jogo"

            pct_over_recent = int((over_gols_count / min(len(jogos), 5)) * 100)

            # --- PARTE 2: CARTÕES (Últimos 10 Jogos - Heavy Work) ---
            total_amarelos = 0
            total_vermelhos = 0
            fids_para_buscar = [j['fixture']['id'] for j in jogos] # IDs dos 10 jogos
            
            # Executa em paralelo para ser rápido (não travar o robô)
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(get_card_stats_single_game, fid, team_id): fid for fid in fids_para_buscar}
                for future in as_completed(futures):
                    y, r = future.result()
                    total_amarelos += y
                    total_vermelhos += r
            
            media_cards = total_amarelos / len(jogos)
            
            return resumo_txt, pct_over_recent, media_cards, total_vermelhos

        # Processa Casa e Fora
        h_txt, h_pct, h_med_cards, h_reds = get_team_stats_unified(home_id)
        a_txt, a_pct, a_med_cards, a_reds = get_team_stats_unified(away_id)
        
        return {
            "home": {"resumo": h_txt, "micro": h_pct, "avg_cards": h_med_cards, "reds": h_reds},
            "away": {"resumo": a_txt, "micro": a_pct, "avg_cards": a_med_cards, "reds": a_reds}
        }
    except Exception as e: 
        print(f"Erro MacroMicro: {e}")
        return None

@st.cache_data(ttl=120) 
def buscar_agenda_cached(api_key, date_str):
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        return requests.get(url, headers={"x-apisports-key": api_key}, params={"date": date_str, "timezone": "America/Sao_Paulo"}).json().get('response', [])
    except: return []

# ==============================================================================
# [NOVO] FUNÇÕES DE INTELIGÊNCIA HÍBRIDA (MÚLTIPLAS + NOVOS MERCADOS)
# ==============================================================================

def carregar_contexto_global_firebase():
    if not db_firestore: return "Firebase Offline."
    try:
        docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(20000).stream()
        stats_gerais = {"total": 0, "over05": 0, "gols_total": 0}
        for d in docs:
            dd = d.to_dict()
            stats_gerais["total"] += 1
            placar = dd.get('placar_final', '0x0')
            try:
                gh, ga = map(int, placar.split('x'))
                if (gh + ga) > 0: stats_gerais["over05"] += 1
                stats_gerais["gols_total"] += (gh + ga)
            except: pass
        if stats_gerais["total"] == 0: return "Sem dados no Firebase."
        media_gols = stats_gerais["gols_total"] / stats_gerais["total"]
        pct_over05 = (stats_gerais["over05"] / stats_gerais["total"]) * 100
        return f"BIG DATA (Base Massiva {stats_gerais['total']} jogos): Média de Gols {media_gols:.2f} | Taxa Over 0.5 Global: {pct_over05:.1f}%."
    except Exception as e: return f"Erro Firebase: {e}"


# [MELHORIA] Anti-correlação para Múltiplas: evita jogos da mesma liga e/ou com horário muito próximo
def filtrar_multiplas_nao_correlacionadas(itens, janela_min=90):
    """Remove itens correlacionados (mesma liga ou kickoff dentro da janela_min).
    Funciona com lista de dicts contendo (fid, league_id, kickoff) ou (fid) usando st.session_state['multipla_meta'].
    Mantém a ordem original (prioriza o que a IA escolheu primeiro).
    """
    try:
        meta_global = st.session_state.get('multipla_meta', {}) or {}
        selecionados = []
        for it in (itens or []):
            fid = str(it.get('fid', ''))
            lid = it.get('league_id', None)
            ko  = it.get('kickoff', None)
            if (lid is None or ko is None) and fid in meta_global:
                mg = meta_global.get(fid, {})
                lid = lid if lid is not None else mg.get('league_id')
                ko  = ko  if ko  is not None else mg.get('kickoff')
            correlacionado = False
            for s in selecionados:
                fid_s = str(s.get('fid', ''))
                lid_s = s.get('league_id', None)
                ko_s  = s.get('kickoff', None)
                if (lid_s is None or ko_s is None) and fid_s in meta_global:
                    ms = meta_global.get(fid_s, {})
                    lid_s = lid_s if lid_s is not None else ms.get('league_id')
                    ko_s  = ko_s  if ko_s  is not None else ms.get('kickoff')
                if lid is not None and lid_s is not None and str(lid) == str(lid_s):
                    correlacionado = True; break
                try:
                    if ko and ko_s and abs((ko - ko_s).total_seconds()) <= janela_min * 60:
                        correlacionado = True; break
                except:
                    pass
            if not correlacionado:
                selecionados.append(it)
        return selecionados
    except:
        return itens
def gerar_multipla_matinal_ia(api_key):
    if not IA_ATIVADA: return None, []
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        
        # Filtra jogos não iniciados
        jogos_candidatos = [j for j in jogos if j['fixture']['status']['short'] == 'NS']
        
        if len(jogos_candidatos) < 2: return None, []
        
        lista_jogos_txt = ""
        mapa_jogos = {}
        # [MELHORIA] Metadados para filtro de correlação (liga/horário)
        meta_local = {}
        
        count_validos = 0
        random.shuffle(jogos_candidatos)

        for j in jogos_candidatos:
            if count_validos >= 30: break
            
            fid = j['fixture']['id']
            
            # 1. Filtro Bet365 (Obrigatório)
            odd_val, odd_nome = buscar_odd_pre_match(api_key, fid)
            if odd_val == 0: continue 

            home = j['teams']['home']['name']
            away = j['teams']['away']['name']
            
            # 2. Usa a NOVA função de Tendência (Macro/Micro)
            stats = analisar_tendencia_macro_micro(api_key, j['teams']['home']['id'], j['teams']['away']['id'])
            
            if stats and stats['home']['micro'] > 0: # Garante que tem dados
                # Regra para Múltipla: Queremos segurança. 
                # Evita times com média recente muito baixa (<40% de over)
                if stats['home']['micro'] < 40 and stats['away']['micro'] < 40: continue
                
                # Formata os dados para a IA
                h_mic = stats['home']['micro']
                a_mic = stats['away']['micro']
                
                mapa_jogos[fid] = f"{home} x {away}"
            # [MELHORIA] Guarda liga e kickoff para evitar correlação em múltiplas
            try:
                dt_iso = j['fixture']['date']
                try:
                    kickoff = datetime.fromisoformat(dt_iso.replace('Z', '+00:00'))
                except:
                    kickoff = None
                meta_local[str(fid)] = {'league_id': j['league'].get('id'), 'kickoff': kickoff, 'name': f"{home} x {away}", 'recente': int(min(h_mic, a_mic))}
            except:
                pass
                lista_jogos_txt += f"""
                - ID {fid}: {home} x {away} ({j['league']['name']})
                  Odd: {odd_val} ({odd_nome})
                  Casa: Recente {h_mic}% Over
                  Fora: Recente {a_mic}% Over
                """
                count_validos += 1

        if not lista_jogos_txt: return None, []
        
        prompt = f"""
        ATUE COMO UM GESTOR DE RISCO (MONTAGEM DE BILHETE PRONTO).
        OBJETIVO: Criar uma DUPLA (2 jogos) ou TRIPLA (3 jogos) de Alta Segurança (Odds @1.80 a @2.50 combinadas).
        
        DADOS (JOGOS FILTRADOS NA BET365):
        {lista_jogos_txt}
        
        CRITÉRIOS OBRIGATÓRIOS:
1. AMBOS os jogos devem ter Recente >= 60% (forma atual).
2. Se algum jogo tiver < 60%, NÃO force múltipla.
3. Priorize ligas diferentes e horários afastados (anti-correlação).
4. Mercados permitidos: Over 0.5 HT ou Over 1.5 FT.
        
        SAÍDA JSON: {{ "jogos": [ {{"fid": 123, "jogo": "A x B", "motivo": "...", "recente": 60}} ], "probabilidade_combinada": "90" }}
        """
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        st.session_state['gemini_usage']['used'] += 1
        # [MELHORIA] Disponibiliza metadados para validação de correlação na etapa de envio
        st.session_state['multipla_meta'] = meta_local
        return json.loads(response.text), mapa_jogos

    except Exception as e: return None, []

# --- RECUPERADO: GERAÇÃO MATINAL DETALHADA ---

def formatar_sniper_para_telegram(texto_gemini):
    """
    Formata o texto do Gemini para ficar bonito no Telegram com HTML.
    """
    try:
        # Converte markdown para HTML
        texto = texto_gemini
        
        # Remove o preâmbulo do Gemini
        texto = re.sub(r'^Ok,.*?ativado\.\s*', '', texto, flags=re.DOTALL)
        
        # Substitui **texto** por <b>texto</b>
        texto = re.sub(r'\*\*([^\*]+)\*\*', r'<b>\1</b>', texto)
        
        # Substitui __texto__ por <i>texto</i>
        texto = re.sub(r'__([^_]+)__', r'<i>\1</i>', texto)
        
        # Melhora as linhas divisórias
        texto = re.sub(r'^\*\*.*?\*\*$', lambda m: f"━━━━━━━━━━━━━━━━━━━━━━━━━\n{m.group(0)}\n━━━━━━━━━━━━━━━━━━━━━━━━━", texto, flags=re.MULTILINE)
        
        # Corrige bullets
        texto = re.sub(r'^\s*\*\s+<b>', '• <b>', texto, flags=re.MULTILINE)
        texto = re.sub(r'^\s*\*\s+', '• ', texto, flags=re.MULTILINE)
        
        # Adiciona quebras de linha extras para melhor legibilidade
        texto = re.sub(r'(<b>[^<]+</b>)\s*\n', r'\1\n\n', texto)
        
        return texto
    except Exception as e:
        print(f"Erro ao formatar Sniper: {e}")
        return texto_gemini



def gerar_insights_matinais_ia(api_key):
    if not IA_ATIVADA: return "IA Offline.", {}
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        
        jogos_candidatos = [j for j in jogos if j['fixture']['status']['short'] == 'NS']
        
        if not jogos_candidatos: return "Sem jogos para analisar hoje.", {}
        
        lista_para_ia = ""
        mapa_jogos = {}
        count = 0
        random.shuffle(jogos_candidatos) 
        
        for j in jogos_candidatos:
            if count >= 80: break 
            
            fid = j['fixture']['id']
            home_id = j['teams']['home']['id']
            away_id = j['teams']['away']['id']
            home = j['teams']['home']['name']
            away = j['teams']['away']['name']

            # [MELHORIA V2] WINRATE PESSOAL (usuário)
            txt_pessoal = ''
            try:
                df_sheets = st.session_state.get('historico_full', pd.DataFrame())
                if df_sheets is not None and not df_sheets.empty:
                    f_h = df_sheets[df_sheets['Jogo'].str.contains(home, na=False, case=False)]
                    if len(f_h) >= 3:
                        wr_pessoal = (f_h['Resultado'].str.contains('GREEN', na=False).sum() / len(f_h)) * 100
                        txt_pessoal = f"WINRATE PESSOAL ({home}): {wr_pessoal:.0f}% ({len(f_h)} apostas)"
            except:
                txt_pessoal = ''

            liga = j['league']['name']
            
            # Armazena FID + IDs dos times para análise posterior
            mapa_jogos[f"{home} x {away}"] = {
                'fid': str(fid),
                'home_id': home_id,
                'away_id': away_id
            }

            # 1. Referência de Preço
            odd_val, odd_nome = buscar_odd_pre_match(api_key, fid)
            if odd_val == 0: continue  # [CORREÇÃO 1] Pula se não tiver na Bet365
            
            # 2. DADOS MACRO (CONSISTÊNCIA - 50 JOGOS)
            macro = analisar_tendencia_50_jogos(api_key, home_id, away_id)
            
            # 3. DADOS MICRO (MOMENTO - 10 JOGOS)
            micro = analisar_tendencia_macro_micro(api_key, home_id, away_id)
        
            if macro and micro:
                h_50 = macro['home']; a_50 = macro['away']
                media_cards_soma = micro['home']['avg_cards'] + micro['away']['avg_cards']
                
                lista_para_ia += f"""
            ---
            ⚽ Jogo: {home} x {away} ({liga})
            💰 Ref (Over 2.5): @{odd_val:.2f}
            
            📅 LONGO PRAZO (50 Jogos - A Verdade):
            - Casa: {h_50['win']}% Vitórias | {h_50['over25']}% Over 2.5
            - Fora: {a_50['win']}% Vitórias | {a_50['over25']}% Over 2.5
            
            🔥 FASE ATUAL (10 Jogos - O Momento):
            - {txt_pessoal}
            - Casa: {micro['home']['resumo']} | Média Cartões: {micro['home']['avg_cards']:.1f}
            - Fora: {micro['away']['resumo']} | Média Cartões: {micro['away']['avg_cards']:.1f}
            - Soma Cartões (Média): {media_cards_soma:.1f}
            """
                count += 1
        
        if not lista_para_ia: return "Nenhum jogo com dados suficientes hoje.", {}

        # --- O PROMPT DEFINITIVO (DADOS 50J + FILTRO DE ELITE + VISUAL LIMPO) ---
        prompt = f"""
        ATUE COMO UM CIENTISTA DE DADOS E TRADER ESPORTIVO (PERFIL SNIPER).
        
        Analise a lista de jogos. Você tem dados de **50 JOGOS** (Histórico) e **10 JOGOS** (Momento).
        Cruze essas informações para encontrar valor real.
        
        DADOS DOS JOGOS:
        {lista_para_ia}

        ---------------------------------------------------------------------
        🚫 FILTRO DE ELITE (OBRIGATÓRIO - SEJA RIGOROSO):
        1. "Falso Favorito": Se o time ganhou os últimos 2 jogos, mas nos 50 jogos tem menos de 40% de vitórias -> NÃO indique Vitória (É sorte).
        2. "Vitória Magra": Se o favorito costuma ganhar de 1x0 -> NÃO indique Over Gols. Indique Vencedor ou Under.
        3. "Arame Liso": Se os times empatam muito (0x0, 1x1) tanto no longo quanto no curto prazo -> OBRIGATÓRIO sugerir UNDER.
        4. "Instabilidade": Se o histórico mostra V-D-V-D -> Jogo imprevisível -> DESCARTE.
        ---------------------------------------------------------------------

        🧠 INTELIGÊNCIA DE SELEÇÃO:
        
        1. 🏆 **MATCH ODDS (Vencedor):**
           - Só sugira se a consistência (50j) for alta (>50% win) E o momento (10j) for bom.
           
        2. ⚡ **GOLS (OVER):**
           - Busque times com alta taxa de Over 2.5 no longo prazo (>60%) E ataques ativos agora.
           
        3. ❄️ **UNDER (TRINCHEIRA):**
           - Busque jogos onde a taxa de gols em 50 jogos é baixa (<40%) E o momento confirma placares magros.
        
        4. 🟨 **CARTÕES:**
           - Se Soma Cartões (Média) > 4.5 -> Indique "Mais de 3.5 Cartões" ou "Mais de 4.5 Cartões".
           - Se Soma Cartões (Média) < 3.0 -> Indique "Menos de 4.5 Cartões" (Jogo Limpo).
        
        5. 🏴 **ESCANTEIOS:**
           - Favorito claro (>55% win + Over alto) jogando em casa -> "Mais de 8.5 Escanteios" ou "Mais de 9.5".
           - Jogo equilibrado com alto volume de ataque -> "Mais de 7.5 Escanteios".
           - Jogo travado/defensivo -> "Menos de 9.5 Escanteios".
        
        6. 🧤 **DEFESAS DO GOLEIRO:**
           - Favorito MASSACRANDO (>65% win) contra time fraco -> "Goleiro Visitante: Mais de 3.5 Defesas".
           - Time que sofre muitos chutes mas não toma gols -> Goleiro forte, indicar Over Defesas.
        
        SUA MISSÃO: Preencher as 6 listas abaixo. Retorne NO MÍNIMO 5 jogos DIFERENTES.
        
        SAÍDA OBRIGATÓRIA (VISUAL LIMPO E DIRETO):
        
        🔥 **ZONA DE GOLS (OVER)**
        ⚽ Jogo: [Time Casa] x [Time Fora]
        🏆 Liga: [Nome da liga]
        🎯 Palpite: **MAIS de 2.5 Gols** (ou Ambas Marcam / 1.5 HT)
        💰 Ref (Over 2.5): @[valor da odd]
        📝 Motivo: [Cite dados de 50 jogos]
        
        ❄️ **ZONA DE TRINCHEIRA (UNDER)**
        ⚽ Jogo: [Time Casa] x [Time Fora]
        🏆 Liga: [Nome da liga]
        🎯 Palpite: **MENOS de 2.5 Gols** (ou Menos de 3.5)
        💰 Ref (Under 2.5): @[valor da odd]
        📝 Motivo: [Cite dados de 50 jogos]
        
        🏆 **ZONA DE MATCH ODDS**
        ⚽ Jogo: [Time Casa] x [Time Fora]
        🏆 Liga: [Nome da liga]
        🎯 Palpite: **Vitória do [Time]** (ou Empate Anula)
        💰 Ref (Casa/Fora): @[valor da odd]
        📝 Motivo: [Cite dados de 50 jogos]
        
        🟨 **ZONA DE CARTÕES**
        ⚽ Jogo: [Time Casa] x [Time Fora]
        🏆 Liga: [Nome da liga]
        🎯 Palpite: **Mais de 3.5 Cartões** (ou Menos de 4.5 / linha exata)
        📝 Motivo: [Cite média de cartões dos times]
        
        🏴 **ZONA DE ESCANTEIOS**
        ⚽ Jogo: [Time Casa] x [Time Fora]
        🏆 Liga: [Nome da liga]
        🎯 Palpite: **Mais de 8.5 Escanteios** (ou linha exata)
        📝 Motivo: [Cite volume de ataque e dados]
        
        🧤 **ZONA DE DEFESAS DO GOLEIRO**
        ⚽ Jogo: [Time Casa] x [Time Fora]
        🏆 Liga: [Nome da liga]
        🎯 Palpite: **Goleiro [Time]: Mais de 3.5 Defesas**
        📝 Motivo: [Cite dados de chutes e pressão]
        
        IMPORTANTE: 
        - Retorne NO MÍNIMO 5 jogos diferentes (pode repetir jogo em zonas diferentes se fizer sentido)
        - Use SEMPRE formato: "Time Casa x Time Fora" (SEM parênteses no nome)
        - Não coloque nome da liga entre parênteses depois do jogo
        - TODAS as 6 zonas devem ter pelo menos 1 indicação (se não houver dados, escreva "Nenhuma oportunidade encontrada nesta zona")
        """
        
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(temperature=0.0))
        st.session_state['gemini_usage']['used'] += 1
        
        texto_ia = response.text

        # [VALIDAÇÃO V2] Se IA ignorar filtro de elite, refaz 1 vez
        try:
            if ('Falso Favorito' not in texto_ia) and ('50 Jogos' not in texto_ia) and ('50 jogos' not in texto_ia):
                st.warning('⚠️ IA ignorou filtro de elite. Refazendo análise (1 tentativa)...')
                prompt2 = prompt + "\n\nIMPORTANTE: cite explicitamente os dados de 50 jogos e mencione ao menos 1 regra do FILTRO DE ELITE."
                response2 = model_ia.generate_content(prompt2, generation_config=genai.types.GenerationConfig(temperature=0.0))
                st.session_state['gemini_usage']['used'] += 1
                texto_ia = response2.text
        except:
            pass

        # ═══════════════════════════════════════════════════════════════════════
        # [NOVA IA COMPLETA] Análise individual de cada jogo recomendado
        # ═══════════════════════════════════════════════════════════════════════
        
        import re
        # Extrai jogos (formato: "⚽ Jogo: Time A x Time B")
        jogos_encontrados = re.findall(r'⚽ Jogo: ([^\n]+?)(?=\n|$)', texto_ia)
        
        # Remove parênteses e liga do nome
        jogos_limpos = []
        for jogo in jogos_encontrados:
            jogo_clean = jogo.split('(')[0].strip()
            if jogo_clean and jogo_clean not in jogos_limpos:
                jogos_limpos.append(jogo_clean)
        
        print(f"[SNIPER] Jogos encontrados: {len(jogos_limpos)}")
        
        if jogos_limpos and len(jogos_limpos) > 0:
            # Para cada jogo recomendado, faz análise completa
            texto_ia += "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            texto_ia += "🧠 <b>ANÁLISE IA COMPLETA (7 MÓDULOS)</b>\n"
            texto_ia += "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            
            jogos_analisados = 0
            for jogo_nome in jogos_limpos[:5]:  # Até 5 jogos
                try:
                    if ' x ' not in jogo_nome:
                        print(f"[SNIPER] Jogo sem 'x': {jogo_nome}")
                        continue
                    
                    # Extrai times
                    time_casa = jogo_nome.split(' x ')[0].strip()
                    time_fora = jogo_nome.split(' x ')[1].strip()
                    
                    # Remove parênteses
                    time_casa = time_casa.replace('(', '').replace(')', '').strip()
                    time_fora = time_fora.replace('(', '').replace(')', '').strip()
                    
                    # Busca no mapa
                    jogo_key = f"{time_casa} x {time_fora}"
                    print(f"[SNIPER] Buscando: {jogo_key}")
                    
                    if jogo_key in mapa_jogos:
                            jogo_data = mapa_jogos[jogo_key]
                            fid_jogo = int(jogo_data['fid'])
                            home_id_jogo = jogo_data['home_id']
                            away_id_jogo = jogo_data['away_id']
                            
                            # Busca odd
                            odd_jogo, _ = buscar_odd_pre_match(api_key, fid_jogo)
                            
                            if odd_jogo > 0:
                                # [NOVA IA HÍBRIDA] Análise PRÉ-JOGO (7 módulos)
                                analise_pre = ia_analise_completa_pre_jogo(
                                    estrategia="Sniper Matinal",
                                    liga="Matinal Mix",
                                    time_casa=time_casa,
                                    time_fora=time_fora,
                                    home_id=home_id_jogo,
                                    away_id=away_id_jogo,
                                    fid_jogo=fid_jogo,
                                    odd_atual=odd_jogo,
                                    probabilidade_ia=75,
                                    api_key=api_key
                                )
                                
                                texto_ia += f"\n\n⚽ <b>{time_casa} x {time_fora}</b>\n"
                                texto_ia += formatar_mensagem_ia_pre_jogo(analise_pre)
                                jogos_analisados += 1
                            else:
                                print(f"[SNIPER] Odd zero para {jogo_key}")
                    else:
                        print(f"[SNIPER] Jogo não encontrado: {jogo_key}")
                        print(f"[SNIPER] Jogos disponíveis: {list(mapa_jogos.keys())[:5]}")
                        
                except Exception as e:
                    print(f"[SNIPER] Erro análise individual: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            if jogos_analisados == 0:
                texto_ia += "\n⚠️ Não foi possível gerar análise IA dos jogos.\n"
                print("[SNIPER] ERRO: Nenhum jogo analisado!")
            else:
                print(f"[SNIPER] ✅ {jogos_analisados} jogos analisados")
        else:
            print(f"[SNIPER] ERRO: Nenhum jogo encontrado no texto da IA")
            print(f"[SNIPER] Regex encontrou: {jogos_encontrados}")

        # Formata texto do Gemini para Telegram (HTML)
        # Remove preâmbulo do Gemini
        texto_ia = re.sub(r'^Ok,.*?ativado\.?\s*', '', texto_ia, flags=re.DOTALL)
        
        # Converte markdown para HTML
        texto_ia = re.sub(r'\*\*([^\*]+)\*\*', r'<b>\1</b>', texto_ia)  # **texto** -> <b>texto</b>
        texto_ia = re.sub(r'__([^_]+)__', r'<i>\1</i>', texto_ia)  # __texto__ -> <i>texto</i>
        
        # Corrige bullets
        texto_ia = re.sub(r'^\s*\*\s+', '├─ ', texto_ia, flags=re.MULTILINE)
        
        # Melhora estrutura visual
        texto_ia = texto_ia.replace('Análise Detalhada:', '\n━━━━━━━━━━━━━━━━━━━━━━━━━\n<b>📊 ANÁLISE DETALHADA:</b>')
        texto_ia = texto_ia.replace('Aplicação dos Filtros', '\n<b>🎯 FILTROS DE ELITE:</b>')
        texto_ia = texto_ia.replace('Ref (', '<b>💰 Ref (')
        texto_ia = texto_ia.replace('):**', '):</b>')
        texto_ia = texto_ia.replace('Longo Prazo', '\n<b>📅 LONGO PRAZO (50 Jogos):</b>')
        texto_ia = texto_ia.replace('Fase Atual', '\n<b>🔥 FASE ATUAL (10 Jogos):</b>')
        
        return texto_ia, mapa_jogos

    except Exception as e: return f"Erro na análise: {str(e)}", {}

def gerar_analise_mercados_alternativos_ia(api_key):
    if not IA_ATIVADA: return []
    # [MELHORIA V2] Base de juízes rigorosos (Top Europa - ajuste conforme necessário)
    JUIZES_RIGOROSOS = ['Michael Oliver','Clément Turpin','Daniele Orsato','Szymon Marciniak','Felix Brych','Antonio Mateu Lahoz','Danny Makkelie','Björn Kuipers','Artur Soares Dias','Jesús Gil Manzano','Carlos del Cerro Grande','José María Sánchez Martínez','Marco Guida','Davide Massa','Slavko Vinčić','Anthony Taylor','Stéphanie Frappart','István Kovács']

    hoje = get_time_br().strftime('%Y-%m-%d')
    
    # Ligas Big Markets (Para Player Props - Chutes/Goleiro)
    LIGAS_BIG_MARKETS = [39, 140, 135, 78, 61, 2, 3, 71, 72, 9, 10, 13, 848, 143, 137] 

    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        
        jogos_candidatos = [j for j in jogos if j['fixture']['status']['short'] == 'NS']
        if not jogos_candidatos: return []
        
        random.shuffle(jogos_candidatos)
        dados_analise = ""
        count_validos = 0
        
        for j in jogos_candidatos:
            if count_validos >= 30: break 
            
            fid = j['fixture']['id']
            lid = j['league']['id']
            home = j['teams']['home']['name']
            away = j['teams']['away']['name']
            liga_nome = j['league']['name']
            juiz = j['fixture'].get('referee', 'Desconhecido')
            juiz_rigor = 'SIM' if any(jr.lower() in str(juiz).lower() for jr in JUIZES_RIGOROSOS) else 'NAO'
            
            # --- LÓGICA DE DADOS ---
            permite_player_props = "SIM" if lid in LIGAS_BIG_MARKETS else "NAO"
            
            stats = analisar_tendencia_macro_micro(api_key, j['teams']['home']['id'], j['teams']['away']['id'])
            media_cartoes_total = 0
            if stats:
                media_cartoes_total = stats['home']['avg_cards'] + stats['away']['avg_cards']
            
            cenario = "Equilibrado"
            try:
                url_odd = "https://v3.football.api-sports.io/odds"
                r_odd = requests.get(url_odd, headers={"x-apisports-key": api_key}, params={"fixture": fid, "bookmaker": "8", "bet": "1"}).json()
                if r_odd.get('response'):
                    vals = r_odd['response'][0]['bookmakers'][0]['bets'][0]['values']
                    v1 = next((float(v['odd']) for v in vals if v['value']=='Home'), 0)
                    v2 = next((float(v['odd']) for v in vals if v['value']=='Away'), 0)
                    if v1 < 1.50: cenario = "Massacre Casa"
                    elif v2 < 1.50: cenario = "Massacre Visitante"
            except: pass

            # Manda para a IA se tiver dados de cartão ou se permitir props
            if media_cartoes_total > 0 or permite_player_props == "SIM":
                dados_analise += f"""
                - Jogo: {home} x {away} ({liga_nome})
                  Juiz: {juiz} | Cenário: {cenario}
                  Permite Jogador? {permite_player_props}
                  Média Cartões (Soma dos Times): {media_cartoes_total:.1f}
                """
                count_validos += 1

        if not dados_analise: return []

        prompt = f"""
        ATUE COMO UM ESPECIALISTA EM MERCADOS ALTERNATIVOS.
        
        Analise a lista de jogos abaixo. Olhe para os dois lados da moeda: Jogo Violento (Over) e Jogo Limpo (Under).
        
        DADOS:
        {dados_analise}
        
        SUA MISSÃO (ENCONTRAR 3 OPORTUNIDADES):
        
        1. 🟨 **MERCADO DE CARTÕES (Over e Under):**
           - **MODO AÇOUGUEIRO (OVER):** Se a Média Soma for ALTA (> 4.5) e o Juiz rigoroso -> Indique "Mais de 3.5 Cartões" ou "Mais de 4.5".
           - **MODO JOGO LIMPO (UNDER):** Se a Média Soma for BAIXA (< 3.5) -> Indique "Menos de 4.5 Cartões" (Segurança) ou "Menos de 3.5".
           - **OBRIGATÓRIO:** Dê a linha exata (Ex: Mais de 3.5 / Menos de 4.5).
        
        2. 🧤 **MURALHA / 🎯 SNIPER (Jogadores):**
           - Só sugira se "Permite Jogador? SIM".
           - Se for Massacre Casa -> Goleiro Visitante Over 3.5 Defesas.
        
        SAÍDA JSON OBRIGATÓRIA:
        {{
            "sinais": [
                {{
                    "fid": "...", 
                    "tipo": "CARTAO" (ou GOLEIRO/CHUTE),
                    "titulo": "🟨 AÇOUGUEIRO" (se for Over) ou "🕊️ JOGO LIMPO" (se for Under),
                    "jogo": "Time A x Time B",
                    "destaque": "Explique (Ex: Times disciplinados, média somada de apenas 2.8 cartões).",
                    "indicacao": "Menos de 4.5 Cartões na Partida"
                }}
            ]
        }}
        """
        
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        st.session_state['gemini_usage']['used'] += 1
        return json.loads(response.text).get('sinais', [])
    except: return []

# [MÓDULO ATUALIZADO] ESTRATÉGIA ALAVANCAGEM SNIPER (TOP 3)
def gerar_bet_builder_alavancagem(api_key):
    if not IA_ATIVADA: return []
    
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        
        LIGAS_ALAVANCAGEM = [39, 140, 78, 135, 61, 71, 72, 2, 3] 
        candidatos = [j for j in jogos if j['league']['id'] in LIGAS_ALAVANCAGEM]
        
        if not candidatos: return []
        
        lista_provaveis = []
        df_historico = st.session_state.get('historico_full', pd.DataFrame())

        for j in candidatos:
            try:
                home_nm = j['teams']['home']['name']
                away_nm = j['teams']['away']['name']
                home_id = j['teams']['home']['id']
                away_id = j['teams']['away']['id']
                
                dados_bd = consultar_bigdata_cenario_completo(home_id, away_id)
                
                txt_historico_pessoal = "Sem histórico recente."
                wr_h = 0; wr_a = 0
                if not df_historico.empty:
                    f_h = df_historico[df_historico['Jogo'].str.contains(home_nm, na=False, case=False)]
                    f_a = df_historico[df_historico['Jogo'].str.contains(away_nm, na=False, case=False)]
                    if len(f_h) > 0: wr_h = len(f_h[f_h['Resultado'].str.contains('GREEN', na=False)]) / len(f_h) * 100
                    if len(f_a) > 0: wr_a = len(f_a[f_a['Resultado'].str.contains('GREEN', na=False)]) / len(f_a) * 100
                    if len(f_h) > 0 or len(f_a) > 0:
                        txt_historico_pessoal = f"Histórico: {home_nm} ({wr_h:.0f}%) | {away_nm} ({wr_a:.0f}%)."

                score = 0

                # [MELHORIA V2] Score numérico real baseado em ratings (evita substring)
                try:
                    m_rt = re.search(r'Rating\s*:?\s*(\d+\.\d+)', str(dados_bd))
                    rv = float(m_rt.group(1)) if m_rt else 0.0
                    if rv >= 7.0: score += 2
                    elif rv >= 6.7: score += 1
                except:
                    pass 
                if (len(f_h) > 2 and wr_h < 40) or (len(f_a) > 2 and wr_a < 40): score -= 5
                
                if score >= 2:
                    lista_provaveis.append({
                        "jogo": j,
                        "bigdata": dados_bd,
                        "historico": txt_historico_pessoal,
                        "referee": j['fixture'].get('referee', 'Desconhecido'),
                        "score": score
                    })
            except: pass
            
        if not lista_provaveis: return []
        
        lista_provaveis.sort(key=lambda x: x['score'], reverse=True)
        top_picks = [p for p in lista_provaveis if p.get('score',0) >= 2][:3]
        if not top_picks: return []
        
        resultados_finais = []
        for pick in top_picks:
            j = pick['jogo']
            home = j['teams']['home']['name']
            away = j['teams']['away']['name']
            fid = j['fixture']['id']
            
            prompt = f"""
            ATUE COMO ANALISTA SÊNIOR DE ALAVANCAGEM.
            MONTE UM BET BUILDER PARA ESTE JOGO ESPECÍFICO.
            
            DADOS:
            1. JOGO: {home} x {away} ({j['league']['name']})
            2. BIG DATA: {pick['bigdata']}
            3. HISTÓRICO USER: {pick['historico']}
            4. JUIZ: {pick['referee']}
            
            A REGRA OBRIGATÓRIA:
- A odd combinada deve ser >= @3.50
- Se ficar abaixo, remova uma seleção (ex: tire cartões)

SAÍDA JSON:
            {{
                "titulo": "🚀 ALAVANCAGEM {home} vs {away}",
                "selecoes": ["Vencedor...", "Gols...", "Cartões..."],
                "analise_ia": "Explicação técnica rápida.",
                "confianca": "Alta"
            }}
            """
            try:
                time.sleep(1)
                response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
                st.session_state['gemini_usage']['used'] += 1
                r_json = json.loads(response.text)
                r_json['fid'] = fid
                r_json['jogo'] = f"{home} x {away}"
                resultados_finais.append(r_json)
            except: pass
            
        return resultados_finais
        
    except Exception as e: return []

# ==============================================================================
# 4. INTELIGÊNCIA ARTIFICIAL, CÁLCULOS E ESTRATÉGIAS (O CÉREBRO)
# ==============================================================================

def buscar_rating_inteligente(api_key, team_id):
    if db_firestore:
        try:
            docs_h = db_firestore.collection("BigData_Futebol").where("home_id", "==", str(team_id)).limit(20).stream()
            docs_a = db_firestore.collection("BigData_Futebol").where("away_id", "==", str(team_id)).limit(20).stream()
            notas = []
            for d in docs_h:
                dados = d.to_dict()
                if 'rating_home' in dados and float(dados['rating_home']) > 0: notas.append(float(dados['rating_home']))
            for d in docs_a:
                dados = d.to_dict()
                if 'rating_away' in dados and float(dados['rating_away']) > 0: notas.append(float(dados['rating_away']))
            if len(notas) >= 3:
                return f"{(sum(notas)/len(notas)):.2f} (Média {len(notas)}j)"
        except: pass
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"team": team_id, "last": "1", "status": "FT"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        if not res.get('response'): return "N/A"
        last_fid = res['response'][0]['fixture']['id']
        url_stats = "https://v3.football.api-sports.io/fixtures/players"
        p_res = requests.get(url_stats, headers={"x-apisports-key": api_key}, params={"fixture": last_fid}).json()
        if not p_res.get('response'): return "N/A"
        for t in p_res['response']:
            if t['team']['id'] == team_id:
                notas = []
                for p in t['players']:
                    try:
                        rating = float(p['statistics'][0]['games']['rating'])
                        if rating > 0: notas.append(rating)
                    except: pass
                if notas: return f"{(sum(notas)/len(notas)):.2f}"
        return "N/A"
    except: return "N/A"

def estimar_odd_teorica(estrategia, tempo_jogo):
    import random
    limites = MAPA_ODDS_TEORICAS.get(estrategia, {"min": 1.40, "max": 1.60})
    odd_base_min = limites['min']
    odd_base_max = limites['max']
    fator_tempo = 0.0
    try:
        t = int(str(tempo_jogo).replace("'", ""))
        if t > 80: fator_tempo = 0.20
        elif t > 70: fator_tempo = 0.10
    except: pass
    odd_simulada = random.uniform(odd_base_min, odd_base_max) + fator_tempo
    return "{:.2f}".format(odd_simulada)

def get_live_odds(fixture_id, api_key, strategy_name, total_gols_atual=0, tempo_jogo=0):
    try:
        url = "https://v3.football.api-sports.io/odds/live"
        params = {"fixture": fixture_id}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        target_markets = []
        target_line = 0.0
        
        # Lógica para pegar odd de Under se a estratégia for de Under
        is_under = "Under" in strategy_name or "Morno" in strategy_name or "Arame" in strategy_name
        
        if "Relâmpago" in strategy_name and total_gols_atual == 0:
            target_markets = ["1st half", "first half"]; target_line = 0.5
        elif "Golden" in strategy_name and total_gols_atual == 1:
            target_markets = ["match goals", "goals over/under"]; target_line = 1.5
        else:
            ht_strategies = ["Relâmpago", "Massacre", "Choque", "Briga", "Morno"]
            is_ht = any(x in strategy_name for x in ht_strategies)
            target_markets = ["1st half", "first half"] if is_ht else ["match goals", "goals over/under"]
            target_line = total_gols_atual + 0.5
            
        if res.get('response'):
            markets = res['response'][0]['odds']
            for m in markets:
                m_name = m['name'].lower()
                # Procura Over ou Under dependendo da estratégia
                tipo_aposta = "under" if is_under else "over"
                
                if any(tm in m_name for tm in target_markets) and "goal" in m_name:
                    for v in m['values']:
                        try:
                            val_name = str(v['value']).lower()
                            if tipo_aposta in val_name: # Filtra "over" ou "under"
                                line_raw = val_name.replace(tipo_aposta, "").strip()
                                line_val = float(''.join(c for c in line_raw if c.isdigit() or c == '.'))
                                if abs(line_val - target_line) < 0.1:
                                    raw_odd = float(v['odd'])
                                    if raw_odd > 50: raw_odd = raw_odd / 1000
                                    return "{:.2f}".format(raw_odd)
                        except: pass
        return estimar_odd_teorica(strategy_name, tempo_jogo)
    except: return estimar_odd_teorica(strategy_name, tempo_jogo)

def buscar_inteligencia(estrategia, liga, jogo):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "\n🔮 <b>Prob: Sem Histórico</b>"
    try:
        times = jogo.split(' x ')
        if len(times) < 2: return "\n🔮 <b>Prob: Nomes Irregulares</b>"
        time_casa = times[0].split('(')[0].strip()
        time_visitante = times[1].split('(')[0].strip()
    except: return "\n🔮 <b>Prob: Erro Nome</b>"
    
    numerador = 0; denominador = 0; fontes = []
    
    try:
        f_casa = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_casa, na=False))]
        f_vis = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_visitante, na=False))]
        if len(f_casa) >= 3 or len(f_vis) >= 3:
            wr_c = (f_casa['Resultado'].str.contains('GREEN').sum()/len(f_casa)*100) if len(f_casa)>=3 else 0
            wr_v = (f_vis['Resultado'].str.contains('GREEN').sum()/len(f_vis)*100) if len(f_vis)>=3 else 0
            div = 2 if (len(f_casa)>=3 and len(f_vis)>=3) else 1
            numerador += ((wr_c + wr_v)/div) * 5; denominador += 5; fontes.append("Time")
    except: pass

    try:
        f_liga = df[(df['Estrategia'] == estrategia) & (df['Liga'] == liga)]
        if len(f_liga) >= 3:
            wr_l = (f_liga['Resultado'].str.contains('GREEN').sum()/len(f_liga)*100)
            numerador += wr_l * 3; denominador += 3; fontes.append("Liga")
    except: pass

    if denominador == 0: return "\n🔮 <b>Prob: Calculando...</b>"
    prob_final = numerador / denominador
    str_fontes = "+".join(fontes) if fontes else "Geral"
    return f"\n{'🔥' if prob_final >= 80 else '🔮' if prob_final > 40 else '⚠️'} <b>Prob: {prob_final:.0f}% ({str_fontes})</b>"

def obter_odd_final_para_calculo(odd_registro, estrategia):
    try:
        if pd.isna(odd_registro) or str(odd_registro).strip() == "" or str(odd_registro).lower() == "nan":
            limites = MAPA_ODDS_TEORICAS.get(estrategia, {"min": 1.40, "max": 1.60})
            return (limites['min'] + limites['max']) / 2
        valor = float(odd_registro)
        if valor <= 1.01: 
            limites = MAPA_ODDS_TEORICAS.get(estrategia, {"min": 1.40, "max": 1.60})
            return (limites['min'] + limites['max']) / 2
        return valor
    except: return 1.50

# --- [RECUPERADO] IA LIVE COMPLETA ---
def consultar_ia_gemini(dados_jogo, estrategia, stats_raw, rh, ra, extra_context="", time_favoravel=""):
    if not IA_ATIVADA: return "", "N/A"
    try:
        # --- 1. Extração de Dados Brutos ---
        s1 = stats_raw[0]['statistics']; s2 = stats_raw[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        
        # Casa
        chutes_h = gv(s1, 'Total Shots'); gol_h = gv(s1, 'Shots on Goal')
        cantos_h = gv(s1, 'Corner Kicks'); atq_perigo_h = gv(s1, 'Dangerous Attacks')
        faltas_h = gv(s1, 'Fouls'); cards_h = gv(s1, 'Yellow Cards') + gv(s1, 'Red Cards')
        
        # Fora
        chutes_a = gv(s2, 'Total Shots'); gol_a = gv(s2, 'Shots on Goal')
        cantos_a = gv(s2, 'Corner Kicks'); atq_perigo_a = gv(s2, 'Dangerous Attacks')
        faltas_a = gv(s2, 'Fouls'); cards_a = gv(s2, 'Yellow Cards') + gv(s2, 'Red Cards')
        
        # Totais
        chutes_totais = chutes_h + chutes_a
        atq_perigo_total = atq_perigo_h + atq_perigo_a
        total_faltas = faltas_h + faltas_a
        total_chutes_gol = gol_h + gol_a
        
        tempo_str = str(dados_jogo.get('tempo', '0')).replace("'", "")
        tempo = int(tempo_str) if tempo_str.isdigit() else 1

        # --- CORREÇÃO DE DADOS (FALLBACK DE INTENSIDADE) ---
        # Se a API não entregar ataques perigosos, usamos os chutes para estimar a pressão
        usou_estimativa = False
        if atq_perigo_total == 0 and chutes_totais > 0:
            # Estimativa: 1 chute equivale a aprox 5 a 7 ataques perigosos em termos de métrica
            atq_perigo_total = int(chutes_totais * 6)
            # [MELHORIA] Ajuste conservador: evita inflar 'Ataques Perigosos' estimados
            # Mantemos a linha original acima para rastreabilidade, mas recalculamos com fator mais realista (x3).
            atq_perigo_total = int(chutes_totais * 3)
            aviso_ia = "(DADOS ESTIMADOS - Ataques Perigosos ausentes. Intensidade recalculada com fator conservador x3. Confie mais nos Chutes.)"
            usou_estimativa = True


        # [MELHORIA V2] Instrução explícita para IA quando intensidade é estimada
        if usou_estimativa:
            instrucao_ia = f"""
⚠️ DADOS ESTIMADOS: Ignore a métrica de intensidade. Confie APENAS em:
- Chutes Totais: {chutes_totais}
- Chutes no Gol: {total_chutes_gol}
- Cenário: {quem_manda}
"""

        # --- 2. ENGENHARIA DE DADOS (KPIs) ---
        intensidade_jogo = atq_perigo_total / tempo if tempo > 0 else 0
        
        # Recalcula o status visual baseado na nova intensidade corrigida
        status_intensidade = "😐 MÉDIA"
        if intensidade_jogo > 1.0: status_intensidade = "🔥 ALTA"
        elif intensidade_jogo < 0.6: status_intensidade = "❄️ BAIXA"

        soma_atq = atq_perigo_h + atq_perigo_a
        dominancia_h = (atq_perigo_h / soma_atq * 100) if soma_atq > 0 else 50
        
        quem_manda = "EQUILIBRADO"
        if dominancia_h > 60: quem_manda = f"DOMÍNIO CASA ({dominancia_h:.0f}%)"
        elif dominancia_h < 40: quem_manda = f"DOMÍNIO VISITANTE ({100-dominancia_h:.0f}%)"

        # Define se a estratégia sugerida é de Under ou Over
        tipo_sugestao = "UNDER" if any(x in estrategia for x in ["Under", "Morno", "Arame", "Segurar"]) else "OVER"
        
        # Momento (Pressão nos últimos minutos)
        pressao_txt = "Neutro"
        if rh >= 3: pressao_txt = "CASA AMASSANDO"
        elif ra >= 3: pressao_txt = "VISITANTE AMASSANDO"

        # ==============================================================================
        # [MELHORIA V3] Calibração de VETOS + Odd Movement + BigData Global
        # ==============================================================================
        tipo_estrategia = classificar_tipo_estrategia(estrategia)
        gols_atuais = calcular_gols_atuais(dados_jogo.get('placar', '0x0'))
        forca_aprovacao_minima = (tipo_estrategia == 'OVER' and gols_atuais >= 2)
        threshold_forcado = 65 if forca_aprovacao_minima else None
        forca_veto = (tipo_estrategia == 'UNDER' and gols_atuais >= 2)

        tendencia_odd = 'ESTÁVEL'
        variacao_odd = 0.0
        alerta_movimento = ''
        contexto_bigdata_global = ''
        try:
            fid_local = dados_jogo.get('fid', dados_jogo.get('id', 0))
            odd_local = float(dados_jogo.get('odd_atual', 1.50))
            tendencia_odd, variacao_odd = rastrear_movimento_odd(fid_local, estrategia, odd_local)
            if tendencia_odd == 'CAINDO FORTE':
                alerta_movimento = f'\nALERTA DE ODD: Odd CAIU {abs(variacao_odd):.1f}% (Sharp Money).'
            elif tendencia_odd == 'SUBINDO FORTE':
                alerta_movimento = f'\nOPORTUNIDADE DE VALOR: Odd SUBIU {variacao_odd:.1f}% (Mercado pagando mais).'
        except:
            pass
        try:
            contexto_bigdata_global = carregar_contexto_global_firebase()
        except:
            contexto_bigdata_global = ''


        
        # Aviso para a IA se usamos estimativa
        aviso_ia = ""
        instrucao_ia = 'Use todos os dados normalmente.'
        if usou_estimativa:
            aviso_ia = "(NOTA TÉCNICA: Dados de Ataques Perigosos ausentes na API. Intensidade foi calculada baseada no volume de CHUTES. Confie nos Chutes.)"

        # --- 4. O PROMPT (A NOVA INTELIGÊNCIA) ---
        prompt = f"""
        ATUE COMO UM CIENTISTA DE DADOS DE FUTEBOL E TRADER ESPORTIVO.
        Analise a entrada: '{estrategia}' (Tipo: {tipo_sugestao}).
        {aviso_ia}
{instrucao_ia}

        VOCÊ DEVE CRUZAR O "MOMENTO" (O que está acontecendo agora) COM A "VERDADE" (Histórico de 50 jogos).
        
        🏟️ DADOS DO AO VIVO ({tempo} min | Placar: {dados_jogo['placar']}):
        - Intensidade Calculada: {intensidade_jogo:.2f}/min ({status_intensidade}).
        - Chutes Totais: {chutes_totais} | No Gol: {total_chutes_gol}
        - Cenário: {quem_manda} | {pressao_txt}
- Gols já marcados: {gols_atuais}
- Tipo de Estratégia: {tipo_estrategia}
MOVIMENTO DE ODD (últimos 5 min):
- Tendência: {tendencia_odd}
- Variação: {variacao_odd:+.1f}%
{alerta_movimento}
        
        📊 CONTEXTO GLOBAL (BIG DATA):
{contexto_bigdata_global}

📚 CONTEXTO HISTÓRICO (A VERDADE):
        {extra_context}
        
        -----------------------------------------------------------
        🧠 INTELIGÊNCIA DE DECISÃO:
0. **REGRA FUNDAMENTAL (NOVA):**
- Se tipo é OVER e jogo já tem >= 2 gols -> APROVAÇÃO obrigatória (mínimo PADRÃO).
- Se tipo é UNDER e jogo já tem >= 2 gols -> VETO obrigatório (inconsistência).
- Não ignore o placar atual.
        
        1. **ESTRATÉGIA GOL RELÂMPAGO/BLITZ:**
           - Se a estratégia é OVER e tem chutes ({chutes_totais}), **IGNORE** se a intensidade parecer baixa. Foque nos Chutes. aprove como **PADRÃO** ou **DIAMANTE**.
        
        2. **ARAME LISO (FALSA PRESSÃO)?**
           - Se tem muitos chutes mas poucos no gol, E o histórico mostra poucos gols -> **APROVAR UNDER**.

        3. **GIGANTE ACORDOU?**
           - Se a estratégia for "OVER" e o time começou a chutar no gol agora -> **APROVAR**.

        CLASSIFIQUE:
        💎 DIAMANTE: Leitura perfeita (Histórico + Momento batem).
        ✅ PADRÃO: Dados favoráveis.
        ⚠️ ARRISCADO: Contradição nos dados.
        ⛔ VETADO: Risco alto (Ex: Sugerir Under em jogo de time goleador).

        JSON: {{ "classe": "...", "probabilidade": "0-100", "motivo_tecnico": "..." }}
        """
        
                # [PATCH V5.2] Instruções específicas por tipo de estratégia (OVER/UNDER/RESULTADO)
        if tipo_estrategia == 'OVER':
            prompt += ("\n⚽ VOCÊ ESTÁ ANALISANDO UMA ESTRATÉGIA DE OVER (GOL):\n"
                      "- Aposta: VAI SAIR GOL.\n"
                      "- APROVE se pressão indica gol iminente (chutes/SOG/bloqueios) e jogo aberto.\n"
                      "- VETE se jogo travado, poucos chutes, ou histórico defensivo forte.\n"
                      "- Se o jogo já tem 2+ gols, OVER tende a ser favorável.\n"
                     )
        elif tipo_estrategia == 'UNDER':
            prompt += ("\n❄️ VOCÊ ESTÁ ANALISANDO UMA ESTRATÉGIA DE UNDER (SEM GOL):\n"
                      "- Aposta: NÃO VAI SAIR GOL.\n"
                      "- APROVE se jogo travado OU falsa pressão (muitos chutes fora, poucos SOG).\n"
                      "- VETE se muitos chutes NO GOL (SOG), pressão contínua, jogo aberto, ou ataques perigosos altos.\n"
                      "- Se o jogo já tem 2+ gols, UNDER é inconsistente (tende a veto).\n"
                     )
        elif tipo_estrategia == 'RESULTADO':
            prompt += ("\n👴 VOCÊ ESTÁ ANALISANDO UMA ESTRATÉGIA DE RESULTADO (VOVÔ):\n"
                      "- Aposta: time que está ganhando vai MANTER/AUMENTAR a vantagem.\n"
                      "- APROVE se o time que vence controla (posse, baixa pressão contra).\n"
                      "- VETE se o time perdendo está amassando (pressão/ataques).\n"
                     )
        
# [MELHORIA V2] Regra explícita: Winrate Pessoal >=80% deve pesar como DIAMANTE
        
        try:
        
            if 'Winrate Pessoal' in str(extra_context):
        
                m_wr = re.search(r'(\d+)%', str(extra_context))
        
                if m_wr and int(m_wr.group(1)) >= 80:
        
                    prompt += '\n\nREGRA OBRIGATÓRIA: Usuário tem winrate pessoal >=80% com o time citado. Se não houver contradição forte, classifique como DIAMANTE.'
        
        except:
        
            pass

        
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        st.session_state['gemini_usage']['used'] += 1
        
        txt_limpo = response.text.replace("```json", "").replace("```", "").strip()
        r_json = json.loads(txt_limpo)
        
        classe = r_json.get('classe', 'PADRAO').upper()
        prob_val = int(r_json.get('probabilidade', 70))
        motivo = r_json.get('motivo_tecnico', 'Análise baseada em KPIs.')
        # ==============================================================================
        # [MELHORIA V3] Aplicar proteções (anti-veto/anti-perda) + validação de odd
        # ==============================================================================
        try:
            odd_atual = float(dados_jogo.get('odd_atual', 1.50))
        except:
            odd_atual = 1.50
        odd_minima_necessaria = obter_odd_minima(estrategia)

        if forca_veto:
            classe = 'VETADO'
            prob_val = 0
            motivo = 'UNDER em jogo com {} gols (inconsistência lógica)'.format(gols_atuais)

        if odd_atual < odd_minima_necessaria:
            if not forca_aprovacao_minima:
                classe = 'VETADO'
                prob_val = 0
                motivo = 'Odd {:.2f} abaixo do mínimo {:.2f}'.format(odd_atual, odd_minima_necessaria)
            else:
                motivo = str(motivo) + ' | AVISO: Odd baixa ({:.2f})'.format(odd_atual)

        if forca_aprovacao_minima:
            if threshold_forcado and prob_val < threshold_forcado:
                prob_val = threshold_forcado
            if classe in ['VETADO','ARRISCADO']:
                classe = 'PADRÃO'
            motivo = str(motivo) + ' | Ajustado (OVER com {} gols)'.format(gols_atuais)

        
        # [CORREÇÃO 2] Classificação simples (sem veto por threshold)
        emoji = "✅"
        if "DIAMANTE" in classe or (prob_val >= 85): 
            emoji = "💎"
            classe = "DIAMANTE"
        elif "ARRISCADO" in classe: 
            emoji = "⚠️"
            classe = "ARRISCADO"
        else:
            emoji = "✅"
            classe = "APROVADO"

        prob_str = f"{prob_val}%"
        
        # HTML para o Telegram
        html_analise = f"\n🤖 <b>IA LIVE (Híbrida):</b>\n{emoji} <b>{classe} ({prob_str})</b>\n"
        
        # Agora mostramos o dado correto de intensidade visualmente
        icone_int = "🔥" if status_intensidade == "🔥 ALTA" else "❄️"
        html_analise += f"📊 <i>Intensidade: {intensidade_jogo:.1f} {icone_int}</i>\n"
        html_analise += f"📝 <i>{motivo}</i>"
        
        return html_analise, prob_str

    except Exception as e: return "", "N/A"

def momentum(fid, sog_h, sog_a):
    mem = st.session_state['memoria_pressao'].get(fid, {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
    if 'sog_h' not in mem: mem = {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []}
    now = datetime.now()
    if sog_h > mem['sog_h']: mem['h_t'].extend([now]*(sog_h-mem['sog_h']))
    if sog_a > mem['sog_a']: mem['a_t'].extend([now]*(sog_a-mem['sog_a']))
    mem['h_t'] = [t for t in mem['h_t'] if now - t <= timedelta(minutes=7)]
    mem['a_t'] = [t for t in mem['a_t'] if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a
    st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_t']), len(mem['a_t'])
# ═══════════════════════════════════════════════════════════════════════════
# 🧠 MÓDULOS DE IA INTELIGENTE - VERSÃO COMPLETA
# ═══════════════════════════════════════════════════════════════════════════
#
# LOCALIZAÇÃO: Adicionar após a linha ~2120 (após função consultar_ia_gemini)
# 
# INSTRUÇÕES:
# 1. Copie TODO este bloco
# 2. Cole após a função consultar_ia_gemini (linha ~2120)
# 3. Salve e deploy
#
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 1: 🧠 IA MEMORY - APRENDIZADO COM HISTÓRICO PESSOAL
# ═══════════════════════════════════════════════════════════════════════════

def ia_memory_analise(estrategia, liga, time_casa, time_fora):
    """
    Analisa histórico pessoal do usuário para esta estratégia/liga/time.
    Retorna confiança ajustada baseada em performance real.
    """
    try:
        df = st.session_state.get('historico_full', pd.DataFrame())
        if df.empty:
            return {
                'tem_historico': False,
                'winrate': 0,
                'total_apostas': 0,
                'confianca': 'neutro',
                'mensagem': 'Sem histórico pessoal ainda.'
            }
        
        # Filtro 1: Estratégia específica
        f_estrategia = df[df['Estrategia'] == estrategia]
        
        # Filtro 2: Liga específica
        f_liga = df[df['Liga'] == liga]
        
        # Filtro 3: Times específicos
        f_times = df[
            (df['Jogo'].str.contains(time_casa, na=False, case=False)) | 
            (df['Jogo'].str.contains(time_fora, na=False, case=False))
        ]
        
        # Prioridade: Estratégia + Liga > Estratégia > Liga
        if len(f_estrategia[f_estrategia['Liga'] == liga]) >= 3:
            amostra = f_estrategia[f_estrategia['Liga'] == liga]
            contexto = f"{estrategia} + {liga}"
        elif len(f_estrategia) >= 5:
            amostra = f_estrategia
            contexto = f"{estrategia} (geral)"
        elif len(f_liga) >= 5:
            amostra = f_liga
            contexto = f"{liga} (geral)"
        elif len(f_times) >= 3:
            amostra = f_times
            contexto = f"Times ({time_casa}/{time_fora})"
        else:
            return {
                'tem_historico': False,
                'winrate': 0,
                'total_apostas': 0,
                'confianca': 'neutro',
                'mensagem': f'Pouco histórico ({len(df)} apostas totais).'
            }
        
        # Calcula winrate
        finalizados = amostra[amostra['Resultado'].isin(['✅ GREEN', '❌ RED'])]
        if len(finalizados) == 0:
            return {
                'tem_historico': False,
                'winrate': 0,
                'total_apostas': len(amostra),
                'confianca': 'neutro',
                'mensagem': 'Sinais ainda pendentes.'
            }
        
        greens = len(finalizados[finalizados['Resultado'].str.contains('GREEN', na=False)])
        total = len(finalizados)
        winrate = (greens / total) * 100
        
        # Classifica confiança
        if winrate >= 75:
            confianca = 'muito_alta'
            emoji = '🔥'
        elif winrate >= 65:
            confianca = 'alta'
            emoji = '💎'
        elif winrate >= 55:
            confianca = 'media'
            emoji = '⚖️'
        elif winrate >= 45:
            confianca = 'baixa'
            emoji = '⚠️'
        else:
            confianca = 'muito_baixa'
            emoji = '🚨'
        
        mensagem = f"{emoji} {winrate:.0f}% em {contexto} ({greens}G/{total-greens}R)"
        
        return {
            'tem_historico': True,
            'winrate': winrate,
            'total_apostas': total,
            'confianca': confianca,
            'mensagem': mensagem,
            'contexto': contexto,
            'greens': greens,
            'reds': total - greens
        }
        
    except Exception as e:
        return {
            'tem_historico': False,
            'winrate': 0,
            'total_apostas': 0,
            'confianca': 'neutro',
            'mensagem': f'Erro: {str(e)}'
        }


# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 2: 💰 IA STAKE MANAGER - GESTÃO DINÂMICA (KELLY CRITERION)
# ═══════════════════════════════════════════════════════════════════════════

def ia_stake_manager(probabilidade_ia, odd_atual, winrate_historico=None):
    """
    Calcula stake ideal usando Kelly Criterion modificado.
    Considera probabilidade IA + histórico pessoal + odd.
    """
    try:
        banca_atual = float(st.session_state.get('banca_atual', 
                           st.session_state.get('banca_inicial', 1000.0)))
        
        # Ajusta probabilidade se tiver histórico
        prob_final = probabilidade_ia / 100
        if winrate_historico and winrate_historico > 0:
            # Média ponderada: 60% histórico, 40% IA
            prob_final = (winrate_historico/100 * 0.6) + (probabilidade_ia/100 * 0.4)
        
        # Kelly Criterion: f = (bp - q) / b
        # f = fração da banca
        # b = odd - 1 (decimal)
        # p = probabilidade de win
        # q = probabilidade de loss (1-p)
        
        b = odd_atual - 1
        p = prob_final
        q = 1 - p
        
        kelly = (b * p - q) / b
        
        # Limites de segurança
        kelly = max(0, kelly)  # Nunca negativo
        kelly = min(kelly, 0.15)  # Máximo 15% da banca
        
        # Fracional Kelly (mais conservador)
        kelly_fracional = kelly * 0.5  # 50% do Kelly
        
        # 3 Níveis de gestão
        conservador_pct = max(2, kelly_fracional * 100)  # Min 2%
        moderado_pct = max(5, kelly * 100 * 0.75)  # Min 5%
        agressivo_pct = max(8, kelly * 100)  # Min 8%
        
        # Limita máximos
        conservador_pct = min(conservador_pct, 5)
        moderado_pct = min(moderado_pct, 10)
        agressivo_pct = min(agressivo_pct, 15)
        
        return {
            'conservador': {
                'porcentagem': conservador_pct,
                'valor': (conservador_pct / 100) * banca_atual
            },
            'moderado': {
                'porcentagem': moderado_pct,
                'valor': (moderado_pct / 100) * banca_atual
            },
            'agressivo': {
                'porcentagem': agressivo_pct,
                'valor': (agressivo_pct / 100) * banca_atual
            },
            'kelly_puro': kelly * 100,
            'recomendado': 'moderado'  # Padrão
        }
        
    except Exception as e:
        # Fallback seguro
        banca = st.session_state.get('banca_inicial', 1000.0)
        return {
            'conservador': {'porcentagem': 2, 'valor': banca * 0.02},
            'moderado': {'porcentagem': 5, 'valor': banca * 0.05},
            'agressivo': {'porcentagem': 10, 'valor': banca * 0.10},
            'kelly_puro': 0,
            'recomendado': 'moderado'
        }


# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 3: 📈 IA ODD TRACKER - TIMING DE ENTRADA
# ═══════════════════════════════════════════════════════════════════════════

def ia_odd_tracker_analise(odd_atual, estrategia, tempo_jogo):
    """
    Analisa se a odd atual é boa ou se deve aguardar valorização.
    Usa histórico de movimentação + padrões.
    """
    try:
        # Odds mínimas aceitáveis por estratégia
        odds_minimas = {
            "Golden Bet": 1.70,
            "Sniper Final": 1.80,
            "Janela de Ouro": 1.65,
            "Blitz Casa": 1.50,
            "Blitz Visitante": 1.50,
            "Porteira Aberta": 1.55,
            "Tiroteio Elite": 1.45,
            "Gol Relâmpago": 1.35,
            "Lay Goleada": 1.60
        }
        
        odd_minima = odds_minimas.get(estrategia, 1.50)
        
        # Análise de timing
        if odd_atual >= (odd_minima * 1.15):  # 15% acima do mínimo
            status = "excelente"
            acao = "ENTRE AGORA"
            emoji = "💎"
            mensagem = f"Odd ótima! {odd_atual:.2f} está acima da média histórica."
        elif odd_atual >= odd_minima:
            status = "boa"
            acao = "PODE ENTRAR"
            emoji = "✅"
            mensagem = f"Odd aceitável (@{odd_atual:.2f})."
        elif odd_atual >= (odd_minima * 0.90):  # Até 10% abaixo
            status = "aguardar"
            acao = "AGUARDE 2-3 MIN"
            emoji = "⏰"
            mensagem = f"Odd baixa (@{odd_atual:.2f}). Pode valorizar."
        else:
            status = "ruim"
            acao = "NÃO RECOMENDADO"
            emoji = "⛔"
            mensagem = f"Odd muito baixa (@{odd_atual:.2f})."
        
        # Projeção de valorização (baseado em tempo de jogo)
        if tempo_jogo and tempo_jogo < 75 and status == "aguardar":
            projecao_odd = odd_atual + (0.10 * (75 - tempo_jogo) / 30)  # Aproximação
            tempo_espera = "2-4 minutos"
        else:
            projecao_odd = odd_atual
            tempo_espera = "0"
        
        return {
            'status': status,
            'acao': acao,
            'emoji': emoji,
            'mensagem': mensagem,
            'odd_atual': odd_atual,
            'odd_minima': odd_minima,
            'projecao': projecao_odd,
            'tempo_espera': tempo_espera
        }
        
    except:
        return {
            'status': 'desconhecido',
            'acao': 'ANALISE MANUAL',
            'emoji': '❓',
            'mensagem': 'Sem dados suficientes.',
            'odd_atual': odd_atual,
            'odd_minima': 1.50,
            'projecao': odd_atual,
            'tempo_espera': '0'
        }


# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 4: 🚪 IA EXIT STRATEGY - CASHOUT INTELIGENTE
# ═══════════════════════════════════════════════════════════════════════════

def ia_exit_strategy(tempo_atual, placar_atual, odd_entrada, estrategia):
    """
    Sugere quando fazer cashout ou segurar até o final.
    Analisa risco vs recompensa baseado no tempo restante.
    """
    try:
        tempo_restante = 90 - tempo_atual
        
        # Estratégias que pedem cashout precoce (alta volatilidade)
        estrategias_volateis = [
            "Golden Bet", "Sniper Final", "Lay Goleada", 
            "Porteira Aberta", "Blitz Casa", "Blitz Visitante"
        ]
        
        # Estratégias para segurar (baixa volatilidade)
        estrategias_seguras = [
            "Jogo Morno", "Estratégia do Vovô", "Arame Liso"
        ]
        
        # Análise de risco
        if tempo_restante <= 5:
            # Últimos 5 minutos
            recomendacao = "segurar"
            emoji = "🔒"
            mensagem = "Faltam poucos minutos. Segure!"
            cashout_pct = 0
            
        elif tempo_restante <= 15 and estrategia in estrategias_seguras:
            # Estratégias seguras: segurar
            recomendacao = "segurar"
            emoji = "🔒"
            mensagem = "Estratégia segura. Aguarde o final."
            cashout_pct = 0
            
        elif tempo_restante >= 20 and estrategia in estrategias_volateis:
            # Estratégias voláteis com muito tempo: cashout parcial
            recomendacao = "cashout_parcial"
            emoji = "🚪"
            mensagem = f"Ainda {tempo_restante} min. Jogo pode virar."
            cashout_pct = 60  # Segura 60%, deixa 40% correr
            
        elif tempo_restante <= 10:
            # Reta final: decisão por placar
            if "x" in placar_atual:
                try:
                    gh, ga = map(int, placar_atual.split('x'))
                    diferenca = abs(gh - ga)
                    
                    if diferenca >= 2:
                        # Placar tranquilo
                        recomendacao = "segurar"
                        emoji = "🔒"
                        mensagem = "Placar seguro. Mantenha!"
                        cashout_pct = 0
                    else:
                        # Placar apertado
                        recomendacao = "cashout_parcial"
                        emoji = "⚠️"
                        mensagem = "Placar apertado. Proteja lucro."
                        cashout_pct = 70
                except:
                    recomendacao = "segurar"
                    emoji = "🔒"
                    mensagem = "Aguarde o final."
                    cashout_pct = 0
            else:
                recomendacao = "segurar"
                emoji = "🔒"
                mensagem = "Aguarde o final."
                cashout_pct = 0
        else:
            # Caso padrão: segurar
            recomendacao = "segurar"
            emoji = "🔒"
            mensagem = "Mantenha a posição."
            cashout_pct = 0
        
        return {
            'recomendacao': recomendacao,
            'emoji': emoji,
            'mensagem': mensagem,
            'cashout_pct': cashout_pct,
            'tempo_restante': tempo_restante
        }
        
    except:
        return {
            'recomendacao': 'segurar',
            'emoji': '🔒',
            'mensagem': 'Sem análise. Decisão manual.',
            'cashout_pct': 0,
            'tempo_restante': 0
        }


# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 5: 🧬 IA MULTI-AGENTE - VOTAÇÃO DE ESPECIALISTAS
# ═══════════════════════════════════════════════════════════════════════════

def ia_multi_agente_votacao(
    probabilidade_ia, 
    winrate_historico, 
    stats_jogo,
    big_data_info,
    estrategia
):
    """
    3 agentes IA analisam o sinal e votam.
    Mostra consenso ou divergência para o usuário decidir.
    """
    try:
        votos = []
        
        # ─────────────────────────────────────────────────────────────────
        # AGENTE 1: 🛡️ CONSERVADOR (Foca em histórico pessoal)
        # ─────────────────────────────────────────────────────────────────
        if winrate_historico and winrate_historico >= 70:
            voto_conservador = "aprovar"
            conf_conservador = "alta"
            razao_conservador = f"Seu histórico: {winrate_historico:.0f}%"
        elif winrate_historico and winrate_historico >= 55:
            voto_conservador = "neutro"
            conf_conservador = "media"
            razao_conservador = f"Histórico OK: {winrate_historico:.0f}%"
        elif winrate_historico and winrate_historico < 55:
            voto_conservador = "rejeitar"
            conf_conservador = "baixa"
            razao_conservador = f"Histórico fraco: {winrate_historico:.0f}%"
        else:
            voto_conservador = "neutro"
            conf_conservador = "media"
            razao_conservador = "Sem histórico suficiente"
        
        votos.append({
            'agente': '🛡️ Conservador',
            'voto': voto_conservador,
            'confianca': conf_conservador,
            'razao': razao_conservador
        })
        
        # ─────────────────────────────────────────────────────────────────
        # AGENTE 2: ⚖️ MODERADO (Foca em stats do jogo atual)
        # ─────────────────────────────────────────────────────────────────
        if probabilidade_ia >= 75:
            voto_moderado = "aprovar"
            conf_moderado = "alta"
            razao_moderado = f"Stats fortes ({probabilidade_ia}%)"
        elif probabilidade_ia >= 60:
            voto_moderado = "aprovar"
            conf_moderado = "media"
            razao_moderado = f"Stats boas ({probabilidade_ia}%)"
        else:
            voto_moderado = "rejeitar"
            conf_moderado = "baixa"
            razao_moderado = f"Stats fracas ({probabilidade_ia}%)"
        
        votos.append({
            'agente': '⚖️ Moderado',
            'voto': voto_moderado,
            'confianca': conf_moderado,
            'razao': razao_moderado
        })
        
        # ─────────────────────────────────────────────────────────────────
        # AGENTE 3: 🚀 AGRESSIVO (Foca em Big Data macro)
        # ─────────────────────────────────────────────────────────────────
        # Analisa se o padrão do Big Data confirma
        if "8" in str(big_data_info) or "9" in str(big_data_info):
            # Média de gols alta no histórico
            voto_agressivo = "aprovar"
            conf_agressivo = "alta"
            razao_agressivo = "Padrão macro confirma"
        elif "MANDANTE" in str(big_data_info) and "Casa" in estrategia:
            voto_agressivo = "aprovar"
            conf_agressivo = "alta"
            razao_agressivo = "Big Data favorece casa"
        elif "VISITANTE" in str(big_data_info) and "Visitante" in estrategia:
            voto_agressivo = "aprovar"
            conf_agressivo = "alta"
            razao_agressivo = "Big Data favorece visitante"
        else:
            voto_agressivo = "neutro"
            conf_agressivo = "media"
            razao_agressivo = "Sem padrão macro claro"
        
        votos.append({
            'agente': '🚀 Agressivo',
            'voto': voto_agressivo,
            'confianca': conf_agressivo,
            'razao': razao_agressivo
        })
        
        # ─────────────────────────────────────────────────────────────────
        # ANÁLISE DE CONSENSO
        # ─────────────────────────────────────────────────────────────────
        aprovar_count = sum(1 for v in votos if v['voto'] == 'aprovar')
        rejeitar_count = sum(1 for v in votos if v['voto'] == 'rejeitar')
        neutro_count = sum(1 for v in votos if v['voto'] == 'neutro')
        
        if aprovar_count >= 2:
            consenso = "favoravel"
            emoji_consenso = "✅"
            mensagem_consenso = f"{aprovar_count}/3 Favorável"
        elif rejeitar_count >= 2:
            consenso = "desfavoravel"
            emoji_consenso = "⛔"
            mensagem_consenso = f"{rejeitar_count}/3 Contra"
        else:
            consenso = "dividido"
            emoji_consenso = "⚖️"
            mensagem_consenso = "Sem consenso (decisão sua)"
        
        return {
            'votos': votos,
            'consenso': consenso,
            'emoji': emoji_consenso,
            'mensagem': mensagem_consenso,
            'aprovar': aprovar_count,
            'rejeitar': rejeitar_count,
            'neutro': neutro_count
        }
        
    except Exception as e:
        return {
            'votos': [],
            'consenso': 'erro',
            'emoji': '❓',
            'mensagem': f'Erro: {str(e)}',
            'aprovar': 0,
            'rejeitar': 0,
            'neutro': 0
        }


# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÃO ORQUESTRADORA: 🎯 IA COMPLETA (CHAMA TODOS OS 5 MÓDULOS)
# ═══════════════════════════════════════════════════════════════════════════

def ia_analise_completa(
    estrategia, 
    liga, 
    time_casa, 
    time_fora,
    placar,
    tempo_jogo,
    odd_atual,
    probabilidade_ia,
    stats_jogo,
    big_data_info
):
    """
    Orquestra os 5 módulos de IA e retorna análise completa.
    Esta função é chamada para CADA sinal (ao vivo ou pré-jogo).
    """
    try:
        # Módulo 1: Memory
        memory = ia_memory_analise(estrategia, liga, time_casa, time_fora)
        
        # Módulo 2: Stake Manager
        winrate_hist = memory['winrate'] if memory['tem_historico'] else None
        stake = ia_stake_manager(probabilidade_ia, odd_atual, winrate_hist)
        
        # Módulo 3: Odd Tracker
        odd_tracker = ia_odd_tracker_analise(odd_atual, estrategia, tempo_jogo)
        
        # Módulo 4: Exit Strategy (só para sinais ao vivo)
        if tempo_jogo:
            exit_strategy = ia_exit_strategy(tempo_jogo, placar, odd_atual, estrategia)
        else:
            exit_strategy = None
        
        # Módulo 5: Multi-Agente
        multi_agente = ia_multi_agente_votacao(
            probabilidade_ia,
            winrate_hist,
            stats_jogo,
            big_data_info,
            estrategia
        )
        
        return {
            'memory': memory,
            'stake': stake,
            'odd_tracker': odd_tracker,
            'exit_strategy': exit_strategy,
            'multi_agente': multi_agente
        }
        
    except Exception as e:
        # Fallback seguro
        return {
            'memory': {'tem_historico': False, 'mensagem': 'Erro'},
            'stake': {
                'conservador': {'porcentagem': 2, 'valor': 20},
                'moderado': {'porcentagem': 5, 'valor': 50},
                'agressivo': {'porcentagem': 10, 'valor': 100}
            },
            'odd_tracker': {'status': 'desconhecido', 'acao': 'ANALISE MANUAL'},
            'exit_strategy': None,
            'multi_agente': {'consenso': 'erro', 'mensagem': 'Erro análise'}
        }


# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÃO AUXILIAR: FORMATAR MENSAGEM TELEGRAM COM IA COMPLETA
# ═══════════════════════════════════════════════════════════════════════════

def formatar_mensagem_ia_completa(analise_ia, tipo_sinal="AO_VIVO"):
    """
    Formata a mensagem do Telegram com TODAS as análises da IA.
    
    Args:
        analise_ia: Resultado de ia_analise_completa()
        tipo_sinal: "AO_VIVO" ou "PRE_JOGO"
    
    Returns:
        String formatada para Telegram (HTML)
    """
    try:
        msg = "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "🧠 <b>ANÁLISE IA COMPLETA</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 1. HISTÓRICO PESSOAL
        # ─────────────────────────────────────────────────────────────────
        memory = analise_ia['memory']
        msg += f"\n📚 <b>SEU HISTÓRICO:</b>\n"
        if memory['tem_historico']:
            msg += f"├─ {memory['mensagem']}\n"
            msg += f"└─ Confiança: <b>{memory['confianca'].upper().replace('_', ' ')}</b>\n"
        else:
            msg += f"└─ {memory['mensagem']}\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 2. VOTAÇÃO DOS ESPECIALISTAS
        # ─────────────────────────────────────────────────────────────────
        multi = analise_ia['multi_agente']
        msg += f"\n🗳️ <b>VOTAÇÃO ESPECIALISTAS:</b>\n"
        
        for voto in multi['votos']:
            if voto['voto'] == 'aprovar':
                emoji_voto = "✅"
            elif voto['voto'] == 'rejeitar':
                emoji_voto = "⛔"
            else:
                emoji_voto = "⚖️"
            
            msg += f"├─ {voto['agente']}: {emoji_voto}\n"
            msg += f"│  └─ {voto['razao']}\n"
        
        msg += f"└─ <b>Consenso: {multi['emoji']} {multi['mensagem']}</b>\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 3. TIMING DE ENTRADA (ODD TRACKER)
        # ─────────────────────────────────────────────────────────────────
        odd_t = analise_ia['odd_tracker']
        msg += f"\n📈 <b>TIMING DE ENTRADA:</b>\n"
        msg += f"├─ Odd Atual: @{odd_t['odd_atual']:.2f}\n"
        msg += f"├─ Status: {odd_t['emoji']} <b>{odd_t['status'].upper()}</b>\n"
        msg += f"└─ Ação: <b>{odd_t['acao']}</b>\n"
        
        if odd_t['status'] == 'aguardar':
            msg += f"   └─ Projeção: @{odd_t['projecao']:.2f} em {odd_t['tempo_espera']}\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 4. GESTÃO DE STAKE (KELLY CRITERION)
        # ─────────────────────────────────────────────────────────────────
        stake = analise_ia['stake']
        msg += f"\n💰 <b>GESTÃO DE STAKE:</b>\n"
        msg += f"├─ 🛡️ Conservador: R$ {stake['conservador']['valor']:.2f} ({stake['conservador']['porcentagem']:.1f}%)\n"
        msg += f"├─ ⚖️ Moderado: R$ {stake['moderado']['valor']:.2f} ({stake['moderado']['porcentagem']:.1f}%) ⭐\n"
        msg += f"└─ 🚀 Agressivo: R$ {stake['agressivo']['valor']:.2f} ({stake['agressivo']['porcentagem']:.1f}%)\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 5. ESTRATÉGIA DE SAÍDA (Só para sinais ao vivo)
        # ─────────────────────────────────────────────────────────────────
        if tipo_sinal == "AO_VIVO" and analise_ia['exit_strategy']:
            exit_s = analise_ia['exit_strategy']
            msg += f"\n🚪 <b>PLANO DE SAÍDA:</b>\n"
            msg += f"├─ {exit_s['emoji']} {exit_s['mensagem']}\n"
            
            if exit_s['cashout_pct'] > 0:
                msg += f"└─ <b>Cashout sugerido: {exit_s['cashout_pct']}%</b>\n"
                msg += f"   └─ (Segura {exit_s['cashout_pct']}%, deixa {100-exit_s['cashout_pct']}% correr)\n"
            else:
                msg += f"└─ <b>Segurar até o final</b>\n"
        
        msg += "\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        
        return msg
        
    except Exception as e:
        return f"\n\n⚠️ Erro na análise IA: {str(e)}\n"



# ═══════════════════════════════════════════════════════════════════════════
# 🎯 4 NOVOS MÓDULOS DE IA ESPECÍFICOS PARA PRÉ-JOGO
# ═══════════════════════════════════════════════════════════════════════════
#
# INSERIR após os 5 módulos existentes (depois da linha ~2810)
# 
# CONFIGURAÇÃO HÍBRIDA:
# - PRÉ-JOGO: Memory, Stake, Multi-Agente + 4 NOVOS
# - AO VIVO: Memory, Stake, Odd Tracker, Exit Strategy, Multi-Agente
#
# ═══════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 6: 🎯 IA ODDS MOVEMENT - Movimento de Linha (24h)
# ═══════════════════════════════════════════════════════════════════════════

def ia_odds_movement_analise(fid_jogo, odd_atual, estrategia):
    """
    Monitora movimento da odd nas últimas 24h.
    Detecta sharp money vs public money.
    Identifica se você está early ou late.
    """
    try:
        # Tenta buscar histórico de odd do cache/Firebase
        if 'odds_history' not in st.session_state:
            st.session_state['odds_history'] = {}
        
        history = st.session_state['odds_history'].get(str(fid_jogo), [])
        
        if len(history) >= 2:
            # Tem histórico
            odd_abertura = history[0]['odd']
            odd_atual_hist = history[-1]['odd']
            tempo_decorrido = len(history)  # Aproximação em horas
            
            # Calcula variação
            variacao_pct = ((odd_atual - odd_abertura) / odd_abertura) * 100
            
            # Classifica movimento
            if abs(variacao_pct) < 3:
                movimento = "ESTÁVEL"
                tendencia = "⚖️"
            elif variacao_pct <= -10:
                movimento = "CAINDO FORTE"
                tendencia = "📉"
            elif variacao_pct <= -5:
                movimento = "CAINDO"
                tendencia = "📉"
            elif variacao_pct >= 10:
                movimento = "SUBINDO FORTE"
                tendencia = "📈"
            elif variacao_pct >= 5:
                movimento = "SUBINDO"
                tendencia = "📈"
            else:
                movimento = "ESTÁVEL"
                tendencia = "⚖️"
            
            # Detecta tipo de dinheiro
            if variacao_pct <= -10:
                tipo_dinheiro = "🦈 SHARP MONEY (Profissionais)"
                interpretacao = "Você está LATE. Odd já caiu muito."
            elif variacao_pct >= 10:
                tipo_dinheiro = "📢 PUBLIC MONEY (Recreativos)"
                interpretacao = "Odd inflada. Oportunidade de VALUE!"
            else:
                tipo_dinheiro = "⚖️ BALANCEADO"
                interpretacao = "Mercado equilibrado."
            
            # Recomendação
            if variacao_pct <= -15:
                recomendacao = "EVITAR (odd caiu demais)"
                emoji_rec = "⛔"
            elif variacao_pct >= 15:
                recomendacao = "EXCELENTE (value bet)"
                emoji_rec = "💎"
            elif abs(variacao_pct) < 5:
                recomendacao = "BOA (odd estável)"
                emoji_rec = "✅"
            else:
                recomendacao = "OK (movimento moderado)"
                emoji_rec = "⚖️"
            
            return {
                'tem_dados': True,
                'odd_abertura': odd_abertura,
                'odd_atual': odd_atual,
                'variacao_pct': variacao_pct,
                'movimento': movimento,
                'tendencia': tendencia,
                'tipo_dinheiro': tipo_dinheiro,
                'interpretacao': interpretacao,
                'recomendacao': recomendacao,
                'emoji': emoji_rec,
                'tempo_monitorado': tempo_decorrido
            }
        else:
            # Sem histórico suficiente
            return {
                'tem_dados': False,
                'odd_atual': odd_atual,
                'mensagem': 'Sem histórico de 24h (jogo muito recente)'
            }
            
    except Exception as e:
        return {
            'tem_dados': False,
            'odd_atual': odd_atual,
            'mensagem': f'Erro: {str(e)}'
        }


# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 7: 📊 IA H2H ANALYZER - Análise de Confronto Direto
# ═══════════════════════════════════════════════════════════════════════════

def ia_h2h_analise(time_casa, time_fora, home_id, away_id, api_key):
    """
    Análise profunda dos últimos confrontos diretos.
    Identifica padrões específicos deste duelo.
    """
    try:
        # Busca H2H da API
        url = f"https://v3.football.api-sports.io/fixtures/headtohead"
        params = {"h2h": f"{home_id}-{away_id}", "last": 10}
        headers = {"x-apisports-key": api_key}
        
        response = requests.get(url, headers=headers, params=params, timeout=5)
        
        if response.status_code != 200:
            return {
                'tem_dados': False,
                'mensagem': 'API indisponível'
            }
        
        data = response.json()
        jogos_h2h = data.get('response', [])
        
        if len(jogos_h2h) < 3:
            return {
                'tem_dados': False,
                'mensagem': 'Poucos jogos H2H (mín. 3)'
            }
        
        # Analisa jogos
        vitorias_casa = 0
        vitorias_fora = 0
        empates = 0
        over_25 = 0
        btts = 0
        total_gols = 0
        
        for jogo in jogos_h2h:
            goals_home = jogo['goals']['home']
            goals_away = jogo['goals']['away']
            
            if goals_home is None or goals_away is None:
                continue
            
            total_gols += (goals_home + goals_away)
            
            # Vitórias (considerando mando)
            fixture_home_id = jogo['teams']['home']['id']
            if fixture_home_id == home_id:
                # Jogo em casa
                if goals_home > goals_away:
                    vitorias_casa += 1
                elif goals_away > goals_home:
                    vitorias_fora += 1
                else:
                    empates += 1
            else:
                # Jogo invertido (casa era visitante)
                if goals_away > goals_home:
                    vitorias_casa += 1
                elif goals_home > goals_away:
                    vitorias_fora += 1
                else:
                    empates += 1
            
            # Over 2.5
            if (goals_home + goals_away) > 2:
                over_25 += 1
            
            # BTTS
            if goals_home > 0 and goals_away > 0:
                btts += 1
        
        total_jogos = len(jogos_h2h)
        media_gols = total_gols / total_jogos if total_jogos > 0 else 0
        
        # Percentuais
        pct_vit_casa = (vitorias_casa / total_jogos) * 100
        pct_over = (over_25 / total_jogos) * 100
        pct_btts = (btts / total_jogos) * 100
        
        # Identifica padrões
        padroes = []
        if pct_btts >= 75:
            padroes.append("💎 BTTS fortíssimo (75%+)")
        if pct_over >= 70:
            padroes.append("🔥 Over 2.5 consistente (70%+)")
        if media_gols >= 3.5:
            padroes.append("⚡ Jogos sempre movimentados (3.5+ gols)")
        if pct_vit_casa >= 70:
            padroes.append("🏠 Casa domina este confronto (70%+)")
        
        if not padroes:
            padroes.append("⚖️ Sem padrões claros (histórico irregular)")
        
        # Recomendações baseadas em H2H
        recomendacoes = []
        if pct_btts >= 75:
            recomendacoes.append("BTTS (Sim) - 75%+ histórico")
        if pct_over >= 70:
            recomendacoes.append("Over 2.5 - 70%+ histórico")
        if pct_vit_casa >= 60:
            recomendacoes.append(f"Casa (1X2) - {pct_vit_casa:.0f}% histórico")
        
        return {
            'tem_dados': True,
            'total_jogos': total_jogos,
            'vitorias_casa': vitorias_casa,
            'vitorias_fora': vitorias_fora,
            'empates': empates,
            'pct_vit_casa': pct_vit_casa,
            'over_25': over_25,
            'pct_over': pct_over,
            'btts': btts,
            'pct_btts': pct_btts,
            'media_gols': media_gols,
            'padroes': padroes,
            'recomendacoes': recomendacoes
        }
        
    except Exception as e:
        return {
            'tem_dados': False,
            'mensagem': f'Erro API: {str(e)}'
        }


# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 8: 🌡️ IA MOMENTUM DETECTOR - Forma Recente dos Times
# ═══════════════════════════════════════════════════════════════════════════

def ia_momentum_analise(time_casa, time_fora, home_id, away_id, api_key):
    """
    Analisa últimos 5 jogos de cada time.
    Detecta momentum (melhorando, estável, caindo).
    Identifica tendências recentes.
    """
    try:
        # Função auxiliar para analisar sequência
        def analisar_sequencia(team_id, location='all'):
            url = f"https://v3.football.api-sports.io/fixtures"
            params = {
                "team": team_id,
                "last": 5 if location == 'all' else 5,
                "status": "FT"
            }
            
            if location == 'home':
                params['venue'] = 'home'
            elif location == 'away':
                params['venue'] = 'away'
            
            headers = {"x-apisports-key": api_key}
            
            response = requests.get(url, headers=headers, params=params, timeout=5)
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            jogos = data.get('response', [])
            
            if len(jogos) < 3:
                return None
            
            vitorias = 0
            empates = 0
            derrotas = 0
            gols_marcados = 0
            gols_sofridos = 0
            
            for jogo in jogos[:5]:
                home_id_fixture = jogo['teams']['home']['id']
                goals_home = jogo['goals']['home']
                goals_away = jogo['goals']['away']
                
                if goals_home is None or goals_away is None:
                    continue
                
                # Determina se o time jogou em casa ou fora
                if home_id_fixture == team_id:
                    # Jogou em casa
                    gols_marcados += goals_home
                    gols_sofridos += goals_away
                    
                    if goals_home > goals_away:
                        vitorias += 1
                    elif goals_home == goals_away:
                        empates += 1
                    else:
                        derrotas += 1
                else:
                    # Jogou fora
                    gols_marcados += goals_away
                    gols_sofridos += goals_home
                    
                    if goals_away > goals_home:
                        vitorias += 1
                    elif goals_away == goals_home:
                        empates += 1
                    else:
                        derrotas += 1
            
            total = vitorias + empates + derrotas
            if total == 0:
                return None
            
            media_gols_marcados = gols_marcados / total
            media_gols_sofridos = gols_sofridos / total
            
            # Classifica momentum
            pontos = (vitorias * 3) + (empates * 1)
            pct_pontos = (pontos / (total * 3)) * 100
            
            if pct_pontos >= 70:
                momentum = "🚀 CRESCENTE"
            elif pct_pontos >= 50:
                momentum = "⚖️ ESTÁVEL"
            else:
                momentum = "📉 DECRESCENTE"
            
            return {
                'vitorias': vitorias,
                'empates': empates,
                'derrotas': derrotas,
                'sequencia': f"{vitorias}V {empates}E {derrotas}D",
                'gols_marcados': gols_marcados,
                'gols_sofridos': gols_sofridos,
                'media_gols_marcados': media_gols_marcados,
                'media_gols_sofridos': media_gols_sofridos,
                'momentum': momentum,
                'pct_pontos': pct_pontos
            }
        
        # Analisa casa (em casa)
        forma_casa = analisar_sequencia(home_id, 'home')
        
        # Analisa fora (fora)
        forma_fora = analisar_sequencia(away_id, 'away')
        
        if not forma_casa or not forma_fora:
            return {
                'tem_dados': False,
                'mensagem': 'Dados insuficientes de forma'
            }
        
        # Compara momentum
        diff_momentum = forma_casa['pct_pontos'] - forma_fora['pct_pontos']
        
        if diff_momentum >= 30:
            comparacao = "🔥 Casa MUITO superior"
        elif diff_momentum >= 15:
            comparacao = "✅ Casa superior"
        elif diff_momentum <= -30:
            comparacao = "⚡ Fora MUITO superior"
        elif diff_momentum <= -15:
            comparacao = "⚠️ Fora superior"
        else:
            comparacao = "⚖️ Equilíbrio"
        
        # Impacto na aposta
        if abs(diff_momentum) >= 30:
            impacto = "ALTO - Diferença extrema de forma"
        elif abs(diff_momentum) >= 15:
            impacto = "MÉDIO - Diferença significativa"
        else:
            impacto = "BAIXO - Formas similares"
        
        return {
            'tem_dados': True,
            'casa': forma_casa,
            'fora': forma_fora,
            'comparacao': comparacao,
            'diff_momentum': diff_momentum,
            'impacto': impacto
        }
        
    except Exception as e:
        return {
            'tem_dados': False,
            'mensagem': f'Erro: {str(e)}'
        }


# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 9: 🔮 IA SMART ENTRY - Momento Ideal de Entrada
# ═══════════════════════════════════════════════════════════════════════════

def ia_smart_entry_decisao(
    odds_movement,
    h2h,
    momentum,
    memory,
    stake,
    multi_agente,
    estrategia
):
    """
    Combina TODOS os dados e calcula o momento ideal de entrada.
    Retorna: ENTRAR AGORA / ESPERAR 2-4H / ESPERAR LIVE / NÃO ENTRAR
    """
    try:
        opcoes = []
        
        # ─────────────────────────────────────────────────────────────────
        # OPÇÃO 1: ENTRAR AGORA
        # ─────────────────────────────────────────────────────────────────
        
        pontos_agora = 0
        razoes_agora = []
        
        # Consenso favorável?
        if multi_agente['consenso'] == 'favoravel':
            pontos_agora += 30
            razoes_agora.append("✅ Consenso 3/3 favorável")
        elif multi_agente['consenso'] == 'dividido':
            pontos_agora += 15
            razoes_agora.append("⚖️ Consenso 2/3")
        
        # H2H forte?
        if h2h['tem_dados'] and len(h2h.get('padroes', [])) > 0:
            if "💎" in str(h2h['padroes']) or "🔥" in str(h2h['padroes']):
                pontos_agora += 25
                razoes_agora.append("💎 Padrão H2H fortíssimo")
        
        # Momentum favorável?
        if momentum['tem_dados']:
            if "superior" in momentum.get('comparacao', '').lower():
                pontos_agora += 20
                razoes_agora.append("🚀 Momentum favorável")
        
        # Histórico pessoal bom?
        if memory['tem_historico'] and memory['winrate'] >= 65:
            pontos_agora += 15
            razoes_agora.append(f"📚 Seu histórico: {memory['winrate']:.0f}%")
        
        # Odd movement OK?
        if odds_movement['tem_dados']:
            if odds_movement.get('variacao_pct', 0) >= -5:
                pontos_agora += 10
                razoes_agora.append("📈 Odd ainda boa")
        
        opcoes.append({
            'opcao': 'ENTRAR AGORA',
            'pontos': pontos_agora,
            'emoji': '⭐',
            'razoes': razoes_agora,
            'stake_sugerido': stake['moderado']['porcentagem'],
            'stake_valor': stake['moderado']['valor']
        })
        
        # ─────────────────────────────────────────────────────────────────
        # OPÇÃO 2: ESPERAR LIVE
        # ─────────────────────────────────────────────────────────────────
        
        pontos_live = 0
        razoes_live = []
        
        # Odd caiu muito?
        if odds_movement['tem_dados']:
            if odds_movement.get('variacao_pct', 0) <= -10:
                pontos_live += 30
                razoes_live.append("📉 Odd caiu muito (-10%+)")
        
        # Consenso dividido?
        if multi_agente['consenso'] == 'dividido':
            pontos_live += 20
            razoes_live.append("⚖️ Consenso dividido (aguardar confirmação)")
        
        # Momentum equilibrado?
        if momentum['tem_dados']:
            if "Equilíbrio" in momentum.get('comparacao', ''):
                pontos_live += 15
                razoes_live.append("⚖️ Jogo pode ir para qualquer lado")
        
        opcoes.append({
            'opcao': 'ESPERAR LIVE',
            'pontos': pontos_live,
            'emoji': '⏰',
            'razoes': razoes_live,
            'aguardar': '15-20 min de jogo',
            'objetivo': 'Odd melhor no live'
        })
        
        # ─────────────────────────────────────────────────────────────────
        # OPÇÃO 3: NÃO ENTRAR
        # ─────────────────────────────────────────────────────────────────
        
        pontos_nao = 0
        razoes_nao = []
        
        # Consenso desfavorável?
        if multi_agente['consenso'] == 'desfavoravel':
            pontos_nao += 40
            razoes_nao.append("⛔ Consenso contra (0/3 ou 1/3)")
        
        # Odd caiu MUITO?
        if odds_movement['tem_dados']:
            if odds_movement.get('variacao_pct', 0) <= -15:
                pontos_nao += 30
                razoes_nao.append("⛔ Odd caiu demais (-15%+)")
        
        # Histórico pessoal ruim?
        if memory['tem_historico'] and memory['winrate'] < 45:
            pontos_nao += 20
            razoes_nao.append(f"⚠️ Histórico fraco: {memory['winrate']:.0f}%")
        
        # Momentum contra?
        if momentum['tem_dados']:
            if "Fora MUITO superior" in momentum.get('comparacao', ''):
                pontos_nao += 15
                razoes_nao.append("📉 Momentum desfavorável")
        
        opcoes.append({
            'opcao': 'NÃO ENTRAR',
            'pontos': pontos_nao,
            'emoji': '⛔',
            'razoes': razoes_nao,
            'alternativa': 'Esperar próximo jogo'
        })
        
        # ─────────────────────────────────────────────────────────────────
        # DECISÃO FINAL
        # ─────────────────────────────────────────────────────────────────
        
        # Ordena por pontos
        opcoes_sorted = sorted(opcoes, key=lambda x: x['pontos'], reverse=True)
        
        # Escolhe a melhor
        melhor_opcao = opcoes_sorted[0]
        
        # Se empate, prioriza ENTRAR AGORA
        if len(opcoes_sorted) >= 2 and opcoes_sorted[0]['pontos'] == opcoes_sorted[1]['pontos']:
            for op in opcoes_sorted:
                if op['opcao'] == 'ENTRAR AGORA':
                    melhor_opcao = op
                    break
        
        return {
            'decisao': melhor_opcao['opcao'],
            'confianca': melhor_opcao['pontos'],
            'todas_opcoes': opcoes_sorted,
            'recomendacao': melhor_opcao
        }
        
    except Exception as e:
        return {
            'decisao': 'ERRO',
            'confianca': 0,
            'mensagem': f'Erro: {str(e)}'
        }


# ═══════════════════════════════════════════════════════════════════════════
# 🎯 ORQUESTRADOR E FORMATADOR - PRÉ-JOGO (7 MÓDULOS)
# ═══════════════════════════════════════════════════════════════════════════
#
# INSERIR após os 4 novos módulos de pré-jogo
#
# ═══════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÃO ORQUESTRADORA: 🎯 IA COMPLETA PRÉ-JOGO (7 MÓDULOS)
# ═══════════════════════════════════════════════════════════════════════════

def ia_analise_completa_pre_jogo(
    estrategia,
    liga,
    time_casa,
    time_fora,
    home_id,
    away_id,
    fid_jogo,
    odd_atual,
    probabilidade_ia,
    api_key
):
    """
    Orquestra os 7 módulos de IA para PRÉ-JOGO.
    
    MÓDULOS:
    1. Memory (histórico pessoal)
    2. Stake Manager (gestão Kelly)
    3. Multi-Agente (3 especialistas)
    4. Odds Movement (movimento 24h) - NOVO
    5. H2H Analyzer (confronto direto) - NOVO
    6. Momentum Detector (forma dos times) - NOVO
    7. Smart Entry (momento ideal) - NOVO
    """
    try:
        # ─────────────────────────────────────────────────────────────────
        # MÓDULOS MANTIDOS (funcionam bem no pré-jogo)
        # ─────────────────────────────────────────────────────────────────
        
        # 1. Memory
        memory = ia_memory_analise(estrategia, liga, time_casa, time_fora)
        
        # 2. Stake Manager
        winrate_hist = memory['winrate'] if memory['tem_historico'] else None
        stake = ia_stake_manager(probabilidade_ia, odd_atual, winrate_hist)
        
        # 3. Multi-Agente
        multi_agente = ia_multi_agente_votacao(
            probabilidade_ia,
            winrate_hist,
            "Análise Pré-Jogo",
            "",
            estrategia
        )
        
        # ─────────────────────────────────────────────────────────────────
        # MÓDULOS NOVOS (específicos pré-jogo)
        # ─────────────────────────────────────────────────────────────────
        
        # 4. Odds Movement
        odds_movement = ia_odds_movement_analise(fid_jogo, odd_atual, estrategia)
        
        # 5. H2H Analyzer
        h2h = ia_h2h_analise(time_casa, time_fora, home_id, away_id, api_key)
        
        # 6. Momentum Detector
        momentum = ia_momentum_analise(time_casa, time_fora, home_id, away_id, api_key)
        
        # 7. Smart Entry (combina tudo)
        smart_entry = ia_smart_entry_decisao(
            odds_movement,
            h2h,
            momentum,
            memory,
            stake,
            multi_agente,
            estrategia
        )
        
        return {
            'memory': memory,
            'stake': stake,
            'multi_agente': multi_agente,
            'odds_movement': odds_movement,
            'h2h': h2h,
            'momentum': momentum,
            'smart_entry': smart_entry
        }
        
    except Exception as e:
        # Fallback seguro
        return {
            'memory': {'tem_historico': False, 'mensagem': 'Erro'},
            'stake': {
                'conservador': {'porcentagem': 2, 'valor': 20},
                'moderado': {'porcentagem': 5, 'valor': 50},
                'agressivo': {'porcentagem': 10, 'valor': 100}
            },
            'multi_agente': {'consenso': 'erro', 'mensagem': 'Erro'},
            'odds_movement': {'tem_dados': False, 'mensagem': 'Erro'},
            'h2h': {'tem_dados': False, 'mensagem': 'Erro'},
            'momentum': {'tem_dados': False, 'mensagem': 'Erro'},
            'smart_entry': {'decisao': 'ERRO', 'confianca': 0}
        }


# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÃO FORMATADOR: 📝 MENSAGEM TELEGRAM PRÉ-JOGO (7 MÓDULOS)
# ═══════════════════════════════════════════════════════════════════════════

def formatar_mensagem_ia_pre_jogo(analise_ia):
    """
    Formata a mensagem do Telegram com os 7 módulos de pré-jogo.
    
    Args:
        analise_ia: Resultado de ia_analise_completa_pre_jogo()
    
    Returns:
        String formatada para Telegram (HTML)
    """
    try:
        msg = "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "🧠 <b>ANÁLISE IA PRÉ-JOGO</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 1. HISTÓRICO PESSOAL
        # ─────────────────────────────────────────────────────────────────
        memory = analise_ia['memory']
        msg += f"\n📚 <b>SEU HISTÓRICO:</b>\n"
        if memory['tem_historico']:
            msg += f"├─ {memory['mensagem']}\n"
            msg += f"└─ Confiança: <b>{memory['confianca'].upper().replace('_', ' ')}</b>\n"
        else:
            msg += f"└─ {memory['mensagem']}\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 2. CONFRONTO DIRETO (H2H)
        # ─────────────────────────────────────────────────────────────────
        h2h = analise_ia['h2h']
        msg += f"\n📊 <b>HISTÓRICO H2H:</b>\n"
        if h2h['tem_dados']:
            msg += f"├─ Últimos {h2h['total_jogos']} jogos\n"
            msg += f"├─ Casa: {h2h['vitorias_casa']}V | Empates: {h2h['empates']} | Fora: {h2h['vitorias_fora']}V\n"
            msg += f"├─ Over 2.5: {h2h['pct_over']:.0f}% | BTTS: {h2h['pct_btts']:.0f}%\n"
            msg += f"├─ Média gols: {h2h['media_gols']:.1f}/jogo\n"
            
            if h2h['padroes']:
                msg += f"└─ <b>Padrões:</b>\n"
                for padrao in h2h['padroes'][:2]:  # Top 2
                    msg += f"   └─ {padrao}\n"
        else:
            msg += f"└─ {h2h['mensagem']}\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 3. FORMA RECENTE (MOMENTUM)
        # ─────────────────────────────────────────────────────────────────
        momentum = analise_ia['momentum']
        msg += f"\n🌡️ <b>FORMA RECENTE:</b>\n"
        if momentum['tem_dados']:
            casa = momentum['casa']
            fora = momentum['fora']
            
            msg += f"├─ Casa: {casa['sequencia']} | {casa['momentum']}\n"
            msg += f"│  └─ {casa['media_gols_marcados']:.1f} gols/jogo\n"
            msg += f"├─ Fora: {fora['sequencia']} | {fora['momentum']}\n"
            msg += f"│  └─ {fora['media_gols_marcados']:.1f} gols/jogo\n"
            msg += f"└─ <b>{momentum['comparacao']}</b>\n"
        else:
            msg += f"└─ {momentum['mensagem']}\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 4. MOVIMENTO DE ODD (24H)
        # ─────────────────────────────────────────────────────────────────
        odds_mv = analise_ia['odds_movement']
        msg += f"\n🎯 <b>MOVIMENTO DE ODD:</b>\n"
        if odds_mv['tem_dados']:
            msg += f"├─ Abertura: @{odds_mv['odd_abertura']:.2f}\n"
            msg += f"├─ Atual: @{odds_mv['odd_atual']:.2f}\n"
            msg += f"├─ Variação: {odds_mv['variacao_pct']:+.1f}% {odds_mv['tendencia']}\n"
            msg += f"├─ {odds_mv['tipo_dinheiro']}\n"
            msg += f"└─ {odds_mv['emoji']} <b>{odds_mv['recomendacao']}</b>\n"
        else:
            msg += f"└─ {odds_mv['mensagem']}\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 5. VOTAÇÃO DOS ESPECIALISTAS
        # ─────────────────────────────────────────────────────────────────
        multi = analise_ia['multi_agente']
        msg += f"\n🗳️ <b>VOTAÇÃO ESPECIALISTAS:</b>\n"
        
        for voto in multi['votos']:
            if voto['voto'] == 'aprovar':
                emoji_voto = "✅"
            elif voto['voto'] == 'rejeitar':
                emoji_voto = "⛔"
            else:
                emoji_voto = "⚖️"
            
            msg += f"├─ {voto['agente']}: {emoji_voto}\n"
            msg += f"│  └─ {voto['razao']}\n"
        
        msg += f"└─ <b>Consenso: {multi['emoji']} {multi['mensagem']}</b>\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 6. GESTÃO DE STAKE
        # ─────────────────────────────────────────────────────────────────
        stake = analise_ia['stake']
        msg += f"\n💰 <b>GESTÃO DE STAKE:</b>\n"
        msg += f"├─ 🛡️ Conservador: R$ {stake['conservador']['valor']:.2f} ({stake['conservador']['porcentagem']:.1f}%)\n"
        msg += f"├─ ⚖️ Moderado: R$ {stake['moderado']['valor']:.2f} ({stake['moderado']['porcentagem']:.1f}%) ⭐\n"
        msg += f"└─ 🚀 Agressivo: R$ {stake['agressivo']['valor']:.2f} ({stake['agressivo']['porcentagem']:.1f}%)\n"
        
        # ─────────────────────────────────────────────────────────────────
        # 7. MOMENTO IDEAL DE ENTRADA (SMART ENTRY)
        # ─────────────────────────────────────────────────────────────────
        smart = analise_ia['smart_entry']
        msg += f"\n🔮 <b>MOMENTO IDEAL:</b>\n"
        
        if smart['decisao'] != 'ERRO':
            rec = smart['recomendacao']
            msg += f"├─ <b>Recomendação: {rec['emoji']} {rec['opcao']}</b>\n"
            msg += f"├─ Confiança: {smart['confianca']} pontos\n"
            
            if rec['razoes']:
                msg += f"└─ Motivos:\n"
                for razao in rec['razoes'][:3]:  # Top 3
                    msg += f"   └─ {razao}\n"
            
            # Se tem stake sugerido
            if 'stake_sugerido' in rec:
                msg += f"\n💡 <b>Stake sugerido: {rec['stake_sugerido']:.1f}%</b> (R$ {rec['stake_valor']:.2f})\n"
            
            # Se é esperar live
            if rec['opcao'] == 'ESPERAR LIVE':
                msg += f"⏰ Aguardar: {rec.get('aguardar', 'Início do jogo')}\n"
        else:
            msg += f"└─ {smart.get('mensagem', 'Erro na análise')}\n"
        
        msg += "\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        
        return msg
        
    except Exception as e:
        return f"\n\n⚠️ Erro na análise IA: {str(e)}\n"


def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0

        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        ck_h = get_v(stats_h, 'Corner Kicks'); ck_a = get_v(stats_a, 'Corner Kicks')
        blk_h = get_v(stats_h, 'Blocked Shots'); blk_a = get_v(stats_a, 'Blocked Shots')
        faltas_h = get_v(stats_h, 'Fouls'); faltas_a = get_v(stats_a, 'Fouls')
        cards_h = get_v(stats_h, 'Yellow Cards') + get_v(stats_h, 'Red Cards')
        cards_a = get_v(stats_a, 'Yellow Cards') + get_v(stats_a, 'Red Cards')

        total_chutes = sh_h + sh_a
        total_chutes_gol = sog_h + sog_a
        total_bloqueados = blk_h + blk_a
        chutes_fora_h = max(0, sh_h - sog_h - blk_h)
        chutes_fora_a = max(0, sh_a - sog_a - blk_a)
        total_fora = chutes_fora_h + chutes_fora_a

        posse_h = 50
        try: posse_h = int(str(next((x['value'] for x in stats_h if x['type']=='Ball Possession'), "50%")).replace('%', ''))
        except: pass
        posse_a = 100 - posse_h

        rh, ra = momentum(j['fixture']['id'], sog_h, sog_a)
        gh = j['goals']['home']; ga = j['goals']['away']; total_gols = gh + ga

        def gerar_ordem_gol(gols_atuais, tipo="Over"):
            linha = gols_atuais + 0.5
            if tipo == "Over": return f"👉 <b>FAZER:</b> Entrar em GOLS (Over)\n✅ Aposta: <b>Mais de {linha} Gols</b>"
            elif tipo == "HT": return f"👉 <b>FAZER:</b> Entrar em GOLS 1º TEMPO\n✅ Aposta: <b>Mais de 0.5 Gols HT</b>"
            elif tipo == "Limite": return f"👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de {gols_atuais + 1.0} Gols</b> (Asiático)"
            return "Apostar em Gols."

        SINAIS = []
        golden_bet_ativada = False

        # 1. GOLDEN BET (sog>=3, bloq>=3, chutes>=14, tempo 60-78)
        if 60 <= tempo <= 78:
            pressao_real_h = (rh >= 2 and sog_h >= 3)
            pressao_real_a = (ra >= 2 and sog_a >= 3)
            if (pressao_real_h or pressao_real_a) and (total_gols >= 1 or total_chutes >= 14):
                if total_bloqueados >= 3:
                    SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"🛡️ {total_bloqueados} Bloqueios (Pressão Real)", "rh": rh, "ra": ra, "favorito": "GOLS"})
                    golden_bet_ativada = True

        # 2. JANELA DE OURO (65-78 min, chutes_gol>=3)
        if not golden_bet_ativada and (65 <= tempo <= 78) and abs(gh - ga) <= 1:
            if total_chutes_gol >= 3:
                SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"🔥 {total_chutes_gol} Chutes no Gol", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 3. JOGO MORNO (domínio 65, sog>4)
        dominio_claro = (posse_h > 65 or posse_a > 65) or (sog_h > 4 or sog_a > 4)
        if 50 <= tempo <= 78 and total_chutes <= 12 and (sog_h + sog_a) <= 3 and gh == ga and not dominio_claro:
            SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": f"👉 <b>FAZER:</b> Under Gols\n✅ Aposta: <b>Menos de {total_gols + 0.5} Gols</b>", "stats": "Jogo Travado", "rh": rh, "ra": ra, "favorito": "UNDER"})

        # 4. ARAME LISO (mantido)
        if 55 <= tempo <= 80 and total_chutes >= 10 and (sog_h + sog_a) <= 3 and total_gols <= 1:
            SINAIS.append({"tag": "🧊 Arame Liso", "ordem": f"👉 <b>FAZER:</b> Under Gols\n⚠️ <i>Muita finalização pra fora.</i>\n✅ Aposta: <b>Menos de {total_gols + 1.5} Gols</b>", "stats": f"{total_chutes} Chutes (Só {sog_h+sog_a} no gol)", "rh": 0, "ra": 0, "favorito": "UNDER"})

        # 5. VOVÔ - RESTRITA (78-88, posse>=50, sog>=2, chutes 5-16)
        if 78 <= tempo <= 88 and total_chutes >= 5 and total_chutes <= 16:
            diff = gh - ga
            if diff == 1 and ra < 1 and posse_h >= 50 and sog_h >= 2:
                SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito (Segurar)\n✅ Aposta: <b>Vitória Seca</b>", "stats": "Controle Total", "rh": rh, "ra": ra, "favorito": "FAVORITO"})
            elif diff == -1 and rh < 1 and posse_a >= 50 and sog_a >= 2:
                SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito (Segurar)\n✅ Aposta: <b>Vitória Seca</b>", "stats": "Controle Total", "rh": rh, "ra": ra, "favorito": "FAVORITO"})

        # 6. PORTEIRA ABERTA (tempo<=35, + caminho alternativo)
        if tempo <= 35:
            if total_gols >= 2 and (sog_h >= 1 and sog_a >= 1):
                SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": gerar_ordem_gol(total_gols), "stats": "Jogo Aberto", "rh": rh, "ra": ra, "favorito": "GOLS"})
            elif total_gols >= 1 and total_chutes >= 6 and total_chutes_gol >= 3:
                SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": gerar_ordem_gol(total_gols), "stats": "Jogo Ofensivo", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 7. GOL RELÂMPAGO (tempo<=15, chutes>=3)
        if total_gols == 0 and (tempo <= 15 and total_chutes >= 3):
            SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": gerar_ordem_gol(0, "HT"), "stats": "Início Intenso", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 8. BLITZ (sh>=7, rh>=2, sog>=2)
        if tempo <= 65:
            blitz_casa = (gh <= ga) and (rh >= 2 or sh_h >= 7) and sog_h >= 2
            blitz_fora = (ga <= gh) and (ra >= 2 or sh_a >= 7) and sog_a >= 2
            if blitz_casa:
                SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": gerar_ordem_gol(total_gols), "stats": "Pressão Limpa", "rh": rh, "ra": ra, "favorito": "GOLS"})
            elif blitz_fora:
                SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": gerar_ordem_gol(total_gols), "stats": "Pressão Limpa", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 9. TIROTEIO ELITE (12-30 min, chutes>=6, sog>=3)
        if 12 <= tempo <= 30 and total_chutes >= 6 and (sog_h + sog_a) >= 3:
            SINAIS.append({"tag": "🏹 Tiroteio Elite", "ordem": gerar_ordem_gol(total_gols), "stats": "Muitos Chutes", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 10. LAY GOLEADA (55-88, diff>=2, chutes>=12)
        if 55 <= tempo <= 88 and abs(gh - ga) >= 2 and total_chutes >= 12:
            time_perdendo_chuta = (gh < ga and sog_h >= 1) or (ga < gh and sog_a >= 1)
            if time_perdendo_chuta:
                SINAIS.append({"tag": "🔫 Lay Goleada", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": "Goleada Viva", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 11. SNIPER FINAL (tempo>=78, rh>=3, sog>=4)
        if tempo >= 78 and abs(gh - ga) <= 1:
            if total_fora <= 8 and ((rh >= 3) or (total_chutes_gol >= 4) or (ra >= 3)):
                SINAIS.append({"tag": "💎 Sniper Final", "ordem": "👉 <b>FAZER:</b> Over Gol Limite\n✅ Busque o Gol no Final", "stats": "Pontaria Ajustada", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 12. MASSACRE (20-45 min, domínio absoluto)
        if 20 <= tempo <= 45 and total_gols >= 1:
            if (sh_h >= 8 and sog_h >= 4 and posse_h >= 60) or (sh_a >= 8 and sog_a >= 4 and posse_a >= 60):
                SINAIS.append({"tag": "🔥 Massacre", "ordem": gerar_ordem_gol(total_gols, "HT"), "stats": "Domínio Absoluto", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 13. CHOQUE DE LÍDERES (15-45 min, jogo equilibrado)
        if 15 <= tempo <= 45 and abs(posse_h - posse_a) <= 10:
            if sog_h >= 2 and sog_a >= 2 and total_chutes >= 8:
                SINAIS.append({"tag": "⚔️ Choque Líderes", "ordem": gerar_ordem_gol(total_gols, "HT"), "stats": "Equilíbrio Ofensivo", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 14. BRIGA DE RUA (20-45 min, jogo agressivo)
        if 20 <= tempo <= 45:
            total_faltas = faltas_h + faltas_a
            if total_faltas >= 12 and total_chutes >= 6 and (sog_h + sog_a) >= 2:
                SINAIS.append({"tag": "🥊 Briga de Rua", "ordem": gerar_ordem_gol(total_gols, "HT"), "stats": f"🔥 {total_faltas} Faltas", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 15. CONTRA-ATAQUE LETAL (30-70 min, time perdendo perigoso)
        if 30 <= tempo <= 70:
            if gh < ga and sog_h >= 3 and sh_h >= 5:
                SINAIS.append({"tag": "⚡ Contra-Ataque Letal", "ordem": "👉 <b>FAZER:</b> Back Empate ou Zebra\n✅ Aposta: <b>Mandante (Recuperação)</b>", "stats": "Pressão do Perdedor", "rh": rh, "ra": ra, "favorito": "ZEBRA"})
            elif ga < gh and sog_a >= 3 and sh_a >= 5:
                SINAIS.append({"tag": "⚡ Contra-Ataque Letal", "ordem": "👉 <b>FAZER:</b> Back Empate ou Zebra\n✅ Aposta: <b>Visitante (Recuperação)</b>", "stats": "Pressão do Perdedor", "rh": rh, "ra": ra, "favorito": "ZEBRA"})

        return SINAIS
    except:
        return []


def deve_buscar_stats(tempo, gh, ga, status):
    if status == 'HT': return True
    if 0 <= tempo <= 95: return True
    return False

def fetch_stats_single(fid, api_key):
    try:
        url = "https://v3.football.api-sports.io/fixtures/statistics"
        r = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fid}, timeout=3)
        return fid, r.json().get('response', []), r.headers
    except: return fid, [], None

def atualizar_stats_em_paralelo(jogos_alvo, api_key):
    resultados = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_stats_single, j['fixture']['id'], api_key): j for j in jogos_alvo}
        time.sleep(0.1)
        for future in as_completed(futures):
            fid, stats, headers = future.result()
            if stats:
                resultados[fid] = stats
                update_api_usage(headers)
    return resultados

def _worker_telegram(token, chat_id, msg):
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except: pass

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    
    msgs_para_enviar = []
    if len(msg) <= 4090:
        msgs_para_enviar.append(msg)
    else:
        buffer = ""
        linhas = msg.split('\n')
        for linha in linhas:
            if len(buffer) + len(linha) + 1 > 4000:
                msgs_para_enviar.append(buffer)
                buffer = linha + "\n"
            else:
                buffer += linha + "\n"
        if buffer: msgs_para_enviar.append(buffer)

    for cid in ids:
        for m in msgs_para_enviar:
            t = threading.Thread(target=_worker_telegram, args=(token, cid, m))
            t.daemon = True; t.start()
            time.sleep(0.3) 

def salvar_snipers_do_texto(texto_ia):
    if not texto_ia or "Sem jogos" in texto_ia: return
    try:
        padrao_jogo = re.findall(r'⚽ Jogo: (.*?)(?:\n|$)', texto_ia)
        for i, jogo_nome in enumerate(padrao_jogo):
            item_sniper = {
                "FID": f"SNIPER_{random.randint(10000, 99999)}",
                "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": "08:00", 
                "Liga": "Sniper Matinal", "Jogo": jogo_nome.strip(), "Placar_Sinal": "0x0", 
                "Estrategia": "Sniper Matinal", "Resultado": "Pendente", 
                "Opiniao_IA": "Sniper", "Probabilidade": "Alta"
            }
            adicionar_historico(item_sniper)
    except: pass

# ==============================================================================
# [MELHORIA V2] Sniper Matinal - salvar com filtro de qualidade (não remove o original)
# ==============================================================================
def salvar_snipers_do_texto_v2(texto_ia):
    if not texto_ia or 'Sem jogos' in str(texto_ia):
        return

    zonas = ['ZONA DE GOLS', 'ZONA DE TRINCHEIRA', 'ZONA DE MATCH ODDS', 'ZONA DE CARTÕES', 'ZONA DE ESCANTEIOS', 'ZONA DE DEFESAS']
    texto = str(texto_ia)
    if not any(z in texto for z in zonas):
        return

    try:
        jogos_encontrados = []
        for zona in zonas:
            if zona not in texto:
                continue
            bloco = texto.split(zona, 1)[1]
            for z2 in zonas:
                if z2 != zona and z2 in bloco:
                    bloco = bloco.split(z2, 1)[0]
            for linha in bloco.splitlines():
                if 'Jogo:' in linha:
                    nome = linha.replace('⚽', '').strip()
                    if nome and nome not in jogos_encontrados:
                        jogos_encontrados.append(nome)
                if len(jogos_encontrados) >= 5:
                    break
            if len(jogos_encontrados) >= 5:
                break

        for jogo_nome in jogos_encontrados[:5]:
            item_sniper = {
                'FID': f"SNIPER_{random.randint(10000, 99999)}",
                'Data': get_time_br().strftime('%Y-%m-%d'),
                'Hora': '08:00',
                'Liga': 'Sniper Matinal',
                'Jogo': jogo_nome.strip(),
                'Placar_Sinal': '0x0',
                'Estrategia': 'Sniper Matinal',
                'Resultado': 'Pendente',
                'Opiniao_IA': 'Sniper',
                'Probabilidade': 'Alta',
                'Tipo_Sinal': 'MATINAL',
                'Confidence_Score': '85'
            }
            adicionar_historico(item_sniper)
    except:
        pass

# Ativa versão v2 (mantém a antiga intacta)
salvar_snipers_do_texto = salvar_snipers_do_texto_v2


def enviar_multipla_matinal(token, chat_ids, api_key):
    if st.session_state.get('multipla_matinal_enviada'): return
    dados_json, mapa_nomes = gerar_multipla_matinal_ia(api_key)
    if not dados_json or "jogos" not in dados_json: return
    jogos = dados_json['jogos']
    # [MELHORIA] Anti-correlação: remove jogos da mesma liga/horário para reduzir risco oculto
    jogos = filtrar_multiplas_nao_correlacionadas(jogos, janela_min=90)
    # Se sobrar menos de 2 jogos após o filtro, não força múltipla
    if len(jogos) < 2:
        return
    
    raw_prob = str(dados_json.get('probabilidade_combinada', '90'))
    if "alta" in raw_prob.lower(): prob = "90"
    elif "media" in raw_prob.lower() or "méd" in raw_prob.lower(): prob = "75"
    else: 
        prob = ''.join(filter(str.isdigit, raw_prob))
        if not prob: prob = "90"

    msg = "🚀 <b>MÚLTIPLA DE SEGURANÇA (IA)</b>\n"
    ids_compostos = []; nomes_compostos = []
    for idx, j in enumerate(jogos):
        icone = ["1️⃣", "2️⃣", "3️⃣"][idx] if idx < 3 else "👉"
        msg += f"\n{icone} <b>Jogo: {j['jogo']}</b>\n🎯 Seleção: Over 0.5 Gols\n📝 Motivo: {j['motivo']}\n"
        ids_compostos.append(str(j['fid'])); nomes_compostos.append(j['jogo'])
    
    msg += f"\n⚠️ <b>Conclusão:</b> Probabilidade combinada de {prob}%."
    
    # ═══════════════════════════════════════════════════════════════════════
    # [NOVA IA HÍBRIDA] Análise PRÉ-JOGO da múltipla (7 módulos)
    # ═══════════════════════════════════════════════════════════════════════
    
    if IA_ATIVADA and len(jogos) > 0:
        try:
            # Pega o primeiro jogo como referência
            jogo_ref = jogos[0]
            
            # Odd combinada (multiplicação)
            odd_combinada = 1.0
            for _ in jogos:
                odd_combinada *= 1.50  # Estimativa conservadora (Over 0.5 ~1.50)
            
            # FID do primeiro jogo (se disponível)
            fid_mult = int(jogo_ref.get('fid', 0))
            
            analise_mult = ia_analise_completa_pre_jogo(
                estrategia="Múltipla Matinal",
                liga="Mix (Múltipla)",
                time_casa=jogo_ref['jogo'].split(' x ')[0] if ' x ' in jogo_ref['jogo'] else "Mix",
                time_fora=jogo_ref['jogo'].split(' x ')[1] if ' x ' in jogo_ref['jogo'] else "Mix",
                home_id=None,  # Sem IDs (H2H/Momentum não funcionarão)
                away_id=None,
                fid_jogo=fid_mult,
                odd_atual=odd_combinada,
                probabilidade_ia=int(prob),
                api_key=api_key
            )
            
            msg += formatar_mensagem_ia_pre_jogo(analise_mult)
            
        except Exception as e:
            print(f"Erro IA Múltipla: {e}")
    
    enviar_telegram(token, chat_ids, msg)
    multipla_obj = {
        "id_unico": f"MULT_{'_'.join(ids_compostos)}",
        "tipo": "MATINAL",
        "fids": ids_compostos,
        "nomes": nomes_compostos,
        "status": "Pendente",
        "data": get_time_br().strftime('%Y-%m-%d'),
        # [NOVO] Metadados para auditoria
        "prob_combinada": str(dados_json.get('probabilidade_combinada', '90')),
        "recente_avg": (sum([int(j.get('recente', 50)) for j in jogos]) / len(jogos)) if jogos else 0,
        "motivo_ia": [j.get('motivo','') for j in jogos],
    }
    if 'multiplas_pendentes' not in st.session_state: st.session_state['multiplas_pendentes'] = []
    st.session_state['multiplas_pendentes'].append(multipla_obj)
    st.session_state['multipla_matinal_enviada'] = True

def enviar_alerta_alternativos(token, chat_ids, api_key):
    if st.session_state.get('alternativos_enviado'): return
    sinais = gerar_analise_mercados_alternativos_ia(api_key)
    if not sinais: return
    for s in sinais:
        # [FILTRO V2] Só envia sinais com fundamentação mínima
        if len(str(s.get('destaque',''))) < 50:
            continue
        msg = f"<b>{s['titulo']}</b>\n\n⚽ <b>{s['jogo']}</b>\n\n🔎 <b>Análise:</b>\n{s['destaque']}\n\n🎯 <b>INDICAÇÃO:</b> {s['indicacao']}"
        if s['tipo'] == 'GOLEIRO': msg += "\n⚠️ <i>Regra: Aposte no 'Goleiro do Time', não no nome do jogador.</i>"
        
        # ═══════════════════════════════════════════════════════════════════════
        # [NOVA IA HÍBRIDA] Análise PRÉ-JOGO para alternativos (7 módulos)
        # ═══════════════════════════════════════════════════════════════════════
        
        if IA_ATIVADA:
            try:
                # Extrai times do nome do jogo
                if ' x ' in s['jogo']:
                    time_c = s['jogo'].split(' x ')[0].strip()
                    time_f = s['jogo'].split(' x ')[1].strip()
                    
                    # Odd genérica (mercados alternativos costumam ter odds mais altas)
                    odd_estimada = 1.75
                    
                    # FID do jogo
                    fid_alt = int(s.get('fid', 0))
                    
                    analise_alt = ia_analise_completa_pre_jogo(
                        estrategia=s['titulo'],
                        liga="Mercado Alternativo",
                        time_casa=time_c,
                        time_fora=time_f,
                        home_id=None,  # Sem IDs (H2H/Momentum não funcionarão)
                        away_id=None,
                        fid_jogo=fid_alt,
                        odd_atual=odd_estimada,
                        probabilidade_ia=70,
                        api_key=api_key
                    )
                    
                    msg += formatar_mensagem_ia_pre_jogo(analise_alt)
                    
            except Exception as e:
                print(f"Erro IA Alternativo: {e}")
        
        enviar_telegram(token, chat_ids, msg)
        linha_alvo = "0"
        try: linha_alvo = re.findall(r"[-+]?\d*\.\d+|\d+", s['indicacao'])[0]
        except: pass
        item_alt = {
            "FID": f"ALT_{s['fid']}", "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": "08:05",
            "Liga": "Mercado Alternativo", "Jogo": s['jogo'], "Placar_Sinal": f"Meta: {linha_alvo}",
            "Estrategia": s['titulo'], "Resultado": "Pendente", "Opiniao_IA": "Aprovado", "Probabilidade": "Alta"
        }
        adicionar_historico(item_alt)
        time.sleep(2) 
    st.session_state['alternativos_enviado'] = True

def enviar_alavancagem(token, chat_ids, api_key):
    if st.session_state.get('alavancagem_enviada'): return
    lista_dados = gerar_bet_builder_alavancagem(api_key)
    if not lista_dados: 
        st.session_state['alavancagem_enviada'] = True; return
    for dados in lista_dados:
        msg = f"💎 <b>{dados['titulo']}</b>\n"
        msg += f"⚽ <b>{dados['jogo']}</b>\n\n"
        msg += "🛠️ <b>CRIAR APOSTA (Combinação):</b>\n"
        for sel in dados['selecoes']: msg += f"✅ {sel}\n"
        msg += f"\n🧠 <b>Motivo IA:</b> {dados['analise_ia']}\n"
        msg += "⚠️ <i>Gestão: Use apenas 'Gordura' (Stake Baixa). Alvo: Odd @3.50+</i>"
        enviar_telegram(token, chat_ids, msg)
        item_alavancagem = {
            "FID": str(dados['fid']), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": "10:00",
            "Liga": "Bet Builder Elite", "Jogo": dados['jogo'], "Placar_Sinal": "Combo Alavancagem", 
            "Estrategia": "Alavancagem", "Resultado": "Pendente", "Opiniao_IA": "Aprovado", "Probabilidade": "Alta (Top 3)"
        }
        adicionar_historico(item_alavancagem)
        time.sleep(3)
    st.session_state['alavancagem_enviada'] = True

def verificar_multipla_quebra_empate(jogos_live, token, chat_ids):
    candidatos = []
    for j in jogos_live:
        fid = j['fixture']['id']; stats = st.session_state.get(f"st_{fid}", [])
        if not stats: continue
        tempo = j['fixture']['status']['elapsed'] or 0; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
        if not (30 <= tempo <= 80) or gh != ga: continue 
        try:
            s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
            def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
            chutes_total = gv(s1, 'Total Shots') + gv(s2, 'Total Shots')
            if chutes_total >= (14 if (gh+ga)==0 else 18):
                candidatos.append({'fid': str(fid), 'jogo': f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", 'placar': f"{gh}x{ga}", 'stats': f"{chutes_total} Chutes", 'tempo': tempo, 'total_gols_ref': (gh+ga)})
                # [MELHORIA] Metadados para anti-correlação (liga/horário)
                try:
                    candidatos[-1]['league_id'] = j.get('league', {}).get('id')
                    dt_iso = j.get('fixture', {}).get('date')
                    try:
                        candidatos[-1]['kickoff'] = datetime.fromisoformat(str(dt_iso).replace('Z', '+00:00')) if dt_iso else None
                    except:
                        candidatos[-1]['kickoff'] = None
                except:
                    pass
        except: pass
    if len(candidatos) >= 2:
        # [MELHORIA] Anti-correlação: remove jogos da mesma liga/horário antes de montar a dupla
        candidatos = filtrar_multiplas_nao_correlacionadas(candidatos, janela_min=90)
        if len(candidatos) < 2:
            return
        dupla = candidatos[:2]
        id_dupla = f"LIVE_{dupla[0]['fid']}_{dupla[1]['fid']}"
        if id_dupla in st.session_state['multiplas_live_cache']: return
        msg = "🚀 <b>ALERTA: DUPLA QUEBRA-EMPATE</b>\nJogos empatados com alta pressão.\n"
        ids_save = []; nomes_save = []; gols_ref_save = {}
        for d in dupla:
            msg += f"\n⚽ <b>{d['jogo']} ({d['placar']})</b>\n⏰ {d['tempo']}' min | 🔥 {d['stats']}"
            ids_save.append(d['fid']); nomes_save.append(d['jogo']); gols_ref_save[d['fid']] = d['total_gols_ref']
        msg += "\n\n🎯 <b>Indicação:</b> Múltipla Over +0.5 Gols na partida"
        enviar_telegram(token, chat_ids, msg)
        st.session_state['multiplas_live_cache'][id_dupla] = True
        multipla_obj = {"id_unico": id_dupla, "tipo": "LIVE", "fids": ids_save, "nomes": nomes_save, "gols_ref": gols_ref_save, "status": "Pendente", "data": get_time_br().strftime('%Y-%m-%d')}
        if 'multiplas_pendentes' not in st.session_state: st.session_state['multiplas_pendentes'] = []
        st.session_state['multiplas_pendentes'].append(multipla_obj)

# --- NOVAS FUNÇÕES DO LABORATÓRIO E BI (MANTIDAS DO NOVO) ---

def analisar_bi_com_ia():
    """
    [MELHORIA #2 V2] Análise Profunda de BI com IA
    - Analisa performance por Estratégia + Liga
    - Identifica Golden Combos e Red Zones
    - Gera recomendações personalizadas
    """
    if not IA_ATIVADA:
        return "IA Offline. Não é possível gerar análise."

    df = st.session_state.get('historico_full', pd.DataFrame())
    if df is None or df.empty:
        return "Sem dados históricos para análise."

    try:
        df_analise = df.copy()
        df_analise = df_analise[df_analise['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
        if len(df_analise) < 10:
            return "Amostra muito pequena (mínimo 10 apostas). Continue operando."

        total_apostas = len(df_analise)
        greens = df_analise['Resultado'].str.contains('GREEN', na=False).sum()
        reds = df_analise['Resultado'].str.contains('RED', na=False).sum()
        winrate_global = (greens / total_apostas) * 100

        pivot_strat = df_analise.groupby('Estrategia')['Resultado'].apply(
            lambda x: pd.Series({
                'Total': len(x),
                'Greens': x.str.contains('GREEN', na=False).sum(),
                'Reds': x.str.contains('RED', na=False).sum(),
                'WR': (x.str.contains('GREEN', na=False).sum() / len(x)) * 100
            })
        ).unstack()
        pivot_strat = pivot_strat[pivot_strat['Total'] >= 3]

        pivot_liga = df_analise.groupby('Liga')['Resultado'].apply(
            lambda x: pd.Series({
                'Total': len(x),
                'Greens': x.str.contains('GREEN', na=False).sum(),
                'Reds': x.str.contains('RED', na=False).sum(),
                'WR': (x.str.contains('GREEN', na=False).sum() / len(x)) * 100
            })
        ).unstack()
        pivot_liga = pivot_liga[pivot_liga['Total'] >= 3]

        pivot_combo = df_analise.groupby(['Estrategia', 'Liga'])['Resultado'].apply(
            lambda x: pd.Series({
                'Total': len(x),
                'WR': (x.str.contains('GREEN', na=False).sum() / len(x)) * 100
            })
        ).unstack()
        pivot_combo = pivot_combo[pivot_combo['Total'] >= 3]

        golden_combos = pivot_combo[pivot_combo['WR'] >= 80].sort_values('WR', ascending=False)
        red_zones = pivot_combo[pivot_combo['WR'] < 40].sort_values('WR', ascending=True)

        top_5_estrategias = pivot_strat.nlargest(5, 'WR')[['Total', 'WR']].to_string() if not pivot_strat.empty else 'Sem dados.'
        worst_3_estrategias = pivot_strat.nsmallest(3, 'WR')[['Total', 'Reds', 'WR']].to_string() if not pivot_strat.empty else 'Sem dados.'
        top_5_ligas = pivot_liga.nlargest(5, 'WR')[['Total', 'WR']].to_string() if not pivot_liga.empty else 'Sem dados.'
        worst_3_ligas = pivot_liga.nsmallest(3, 'WR')[['Total', 'Reds', 'WR']].to_string() if not pivot_liga.empty else 'Sem dados.'

        golden_txt = golden_combos.head(5).to_string() if not golden_combos.empty else "Nenhum combo com WR >= 80% ainda."
        red_txt = red_zones.head(5).to_string() if not red_zones.empty else "Nenhuma zona crítica detectada."

        txt_tendencia = "Sem dados dos últimos 7 dias."
        try:
            df_analise['Data_DT'] = pd.to_datetime(df_analise['Data'], errors='coerce')
            hoje = pd.to_datetime(get_time_br().date())
            df_7d = df_analise[df_analise['Data_DT'] >= (hoje - timedelta(days=7))]
            if not df_7d.empty:
                wr_7d = (df_7d['Resultado'].str.contains('GREEN', na=False).sum() / len(df_7d)) * 100
                tendencia = "📈 SUBINDO" if wr_7d > winrate_global else "📉 CAINDO" if wr_7d < winrate_global else "➡️ ESTÁVEL"
                txt_tendencia = f"Winrate 7 dias: {wr_7d:.1f}% (Global: {winrate_global:.1f}%) - {tendencia}"
        except:
            txt_tendencia = "Erro ao calcular tendência."

        prompt = f"""ATUE COMO UM ANALISTA QUANT SÊNIOR (Hedge Fund).
Você tem acesso ao desempenho completo do usuário. Seja direto e acionável.

📊 DADOS GLOBAIS:
- Total de Apostas: {total_apostas}
- Greens: {greens} | Reds: {reds}
- Winrate Global: {winrate_global:.1f}%
- {txt_tendencia}

🏆 TOP 5 ESTRATÉGIAS (Melhores):
{top_5_estrategias}

💀 WORST 3 ESTRATÉGIAS (Piores):
{worst_3_estrategias}

🌍 TOP 5 LIGAS (Melhores):
{top_5_ligas}

⚠️ WORST 3 LIGAS (Piores):
{worst_3_ligas}

💎 GOLDEN COMBOS (WR >= 80%):
{golden_txt}

🚨 RED ZONES (WR < 40%):
{red_txt}

SUA MISSÃO (4 BLOCOS):
1) DIAGNÓSTICO (2 linhas)
2) GOLDEN COMBOS (repetir)
3) RED ZONES (parar)
4) AÇÃO IMEDIATA (1 mudança para hoje)

Seja curto e direto. Máximo 250 palavras.
"""

        response = model_ia.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(temperature=0.2, max_output_tokens=500)
        )
        st.session_state['gemini_usage']['used'] += 1
        analise_txt = response.text

        try:
            if 'analises_ia_bi' not in st.session_state:
                st.session_state['analises_ia_bi'] = []
            st.session_state['analises_ia_bi'].append({
                'data': get_time_br().strftime('%Y-%m-%d %H:%M'),
                'analise': analise_txt,
                'winrate_global': winrate_global,
                'total_apostas': total_apostas
            })
            if len(st.session_state['analises_ia_bi']) > 10:
                st.session_state['analises_ia_bi'] = st.session_state['analises_ia_bi'][-10:]
        except Exception as e:
            print(f"Erro ao salvar análise: {e}")

        return analise_txt

    except Exception as e:
        return f"Erro ao gerar análise: {str(e)}"

def criar_estrategia_nova_ia(foco_usuario):
    if not IA_ATIVADA or not db_firestore: return "Offline."
    try:
        df_hist = st.session_state.get('historico_full', pd.DataFrame())
        txt_hist = "Sem histórico."
        if not df_hist.empty: txt_hist = df_hist.groupby('Estrategia')['Resultado'].value_counts().unstack().fillna(0).to_string()
        docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(100).stream()
        data = [d.to_dict() for d in docs]
        prompt = f"ATUE COMO QUANT. O usuário quer estratégia de: {foco_usuario}. HISTÓRICO DELE: {txt_hist}. DADOS GLOBAIS: {str(data[:3])}. Crie 1 estratégia PRÉ-LIVE e 1 AO VIVO."
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro: {str(e)}"

def otimizar_estrategias_existentes_ia():
    if not IA_ATIVADA: return "IA Offline."
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados."
    reds = df[df['Resultado'].str.contains('RED', na=False)]['Estrategia'].value_counts().to_string()
    # [MELHORIA V2] Confidence Score Médio dos REDs (se a coluna existir)
    try:
        if 'Confidence_Score' in df.columns:
            reds_detalhados = df[df['Resultado'].str.contains('RED', na=False)].copy()
            if not reds_detalhados.empty:
                reds_detalhados['Conf_Score'] = pd.to_numeric(reds_detalhados['Confidence_Score'].astype(str).str.replace('%','', regex=False), errors='coerce')
                media_conf_reds = reds_detalhados['Conf_Score'].mean()
                if pd.notna(media_conf_reds):
                    reds = reds + f"\n\n🧠 Confidence Score Médio dos REDs: {media_conf_reds:.1f} (se <60, o sistema já alertava)"

    except:
        pass

    prompt = f"ATUE COMO GESTOR DE RISCO. Estou tomando RED nestas estratégias: {reds}. Sugira uma trava de segurança técnica."
    try:
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except: return "Erro IA."

def analisar_financeiro_com_ia(stake, banca):
    if not IA_ATIVADA: return "IA Offline."
    stats = st.session_state.get('last_fin_stats', {})
    prompt = f"ATUE COMO CONSULTOR FINANCEIRO. Banca: {banca}. Stake: {stake}. Lucro: {stats.get('lucro')}. Dê um conselho curto."
    try:
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except: return "Erro IA."

def enviar_relatorio_bi(token, chat_ids):
    msg = f"📊 RELATÓRIO BI\n\n{analisar_bi_com_ia()}"
    enviar_telegram(token, chat_ids, msg)

def enviar_relatorio_financeiro(token, chat_ids, cenario, lucro, roi, entradas):
    msg = f"💰 FECHAMENTO\nCenário: {cenario}\nLucro: R$ {lucro:.2f}\nROI: {roi:.2f}%"
    enviar_telegram(token, chat_ids, msg)

def enviar_analise_estrategia(token, chat_ids):
    msg = f"🔧 AJUSTE TÉCNICO\n\n{otimizar_estrategias_existentes_ia()}"
    enviar_telegram(token, chat_ids, msg)

# --- FIM DAS NOVAS FUNÇÕES ---

def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br()
    if 7 <= agora.hour < 11:
        if not st.session_state['matinal_enviado']:
            conteudo_ia, mapa_ids = gerar_insights_matinais_ia(api_key)
            if conteudo_ia and "Sem jogos" not in str(conteudo_ia) and "Erro" not in str(conteudo_ia):
                ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
                msg_final = f"🌅 <b>SNIPER MATINAL (IA + DADOS)</b>\n\n{conteudo_ia}"
                for cid in ids: enviar_telegram(token, cid, msg_final)
                salvar_snipers_do_texto(conteudo_ia)
                st.session_state['matinal_enviado'] = True
            else:
                print("Sniper Matinal: Nenhum jogo encontrado ou erro na IA.")

        if st.session_state['matinal_enviado'] and not st.session_state.get('multipla_matinal_enviada', False):
            time.sleep(5); enviar_multipla_matinal(token, chat_ids, api_key)
        if st.session_state['matinal_enviado'] and st.session_state['multipla_matinal_enviada'] and not st.session_state.get('alternativos_enviado', False):
            time.sleep(5); enviar_alerta_alternativos(token, chat_ids, api_key)
        if agora.hour >= 10 and not st.session_state.get('alavancagem_enviada', False):
            time.sleep(5); enviar_alavancagem(token, chat_ids, api_key)
    
    faixa_12h = (agora.hour == 12 or (agora.hour == 13 and agora.minute <= 30))
    faixa_16h = (agora.hour == 16 and agora.minute <= 30)

    if faixa_12h and not st.session_state.get('drop_enviado_12', False):
        drops = scanner_drop_odds_pre_live(api_key)
        if drops:
            for d in drops:
                msg = f"💰 <b>ESTRATÉGIA CASHOUT (DROP ODDS)</b>\n\n⚽ <b>{d['jogo']}</b>\n🏆 {d['liga']} | ⏰ {d['hora']}\n\n📉 <b>DESAJUSTE:</b>\n• Bet365: <b>@{d['odd_b365']:.2f}</b>\n• Pinnacle: <b>@{d['odd_pinnacle']:.2f}</b>\n• Drop: <b>{d['valor']:.1f}%</b>\n\n⚙️ <b>AÇÃO:</b>\n1️⃣ Compre vitória do <b>{d['lado']}</b>\n2️⃣ <b>+ BANKER:</b> Adicione 'Under 7.5 Gols'\n3️⃣ <b>SAÍDA:</b> Cashout ao igualar Pinnacle."
                enviar_telegram(token, chat_ids, msg)
                item_drop = {"FID": f"DROP_{d['fid']}", "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": "Trading Pré-Live", "Jogo": d['jogo'], "Placar_Sinal": f"Entrada: @{d['odd_b365']}", "Estrategia": "Drop Odds Cashout", "Resultado": "Pendente", "Opiniao_IA": "Aprovado", "Probabilidade": "Técnica"}
                adicionar_historico(item_drop)
        st.session_state['drop_enviado_12'] = True

    if faixa_16h and not st.session_state.get('drop_enviado_16', False):
        drops = scanner_drop_odds_pre_live(api_key)
        if drops:
            for d in drops:
                msg = f"💰 <b>ESTRATÉGIA CASHOUT (DROP ODDS)</b>\n\n⚽ <b>{d['jogo']}</b>\n🏆 {d['liga']} | ⏰ {d['hora']}\n\n📉 <b>DESAJUSTE:</b>\n• Bet365: <b>@{d['odd_b365']:.2f}</b>\n• Pinnacle: <b>@{d['odd_pinnacle']:.2f}</b>\n• Drop: <b>{d['valor']:.1f}%</b>\n\n⚙️ <b>AÇÃO:</b>\n1️⃣ Compre vitória do <b>{d['lado']}</b>\n2️⃣ <b>+ BANKER:</b> Adicione 'Under 7.5 Gols'\n3️⃣ <b>SAÍDA:</b> Cashout ao igualar Pinnacle."
                enviar_telegram(token, chat_ids, msg)
                item_drop = {"FID": f"DROP_{d['fid']}", "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": "Trading Pré-Live", "Jogo": d['jogo'], "Placar_Sinal": f"Entrada: @{d['odd_b365']}", "Estrategia": "Drop Odds Cashout", "Resultado": "Pendente", "Opiniao_IA": "Aprovado", "Probabilidade": "Técnica"}
                adicionar_historico(item_drop)
        st.session_state['drop_enviado_16'] = True

    hoje_str = agora.strftime('%Y-%m-%d')
    if st.session_state.get('last_check_date') != hoje_str:
        st.session_state['matinal_enviado'] = False; st.session_state['multipla_matinal_enviada'] = False
        st.session_state['alternativos_enviado'] = False; st.session_state['alavancagem_enviada'] = False
        st.session_state['drop_enviado_12'] = False; st.session_state['drop_enviado_16'] = False
        st.session_state['last_check_date'] = hoje_str

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    hist = st.session_state['historico_sinais']
    pendentes = [s for s in hist if s['Resultado'] == 'Pendente']
    if not pendentes: return
    
    hoje_str = get_time_br().strftime('%Y-%m-%d')
    updates_buffer = []
    mapa_live = {j['fixture']['id']: j for j in jogos_live}
    
    for s in pendentes:
        if s.get('Data') != hoje_str: continue
        if "Sniper" in s['Estrategia'] or "Alavancagem" in s['Estrategia'] or "Drop" in s['Estrategia']: continue 
        if "Mercado Alternativo" in s['Liga']: continue 
        
        fid = int(clean_fid(s.get('FID', 0)))
        strat = s['Estrategia']
        
        jogo_api = mapa_live.get(fid)
        if not jogo_api:
             try:
                 res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                 if res['response']: jogo_api = res['response'][0]
             except: pass
             
        if jogo_api:
            gh = jogo_api['goals']['home'] or 0
            ga = jogo_api['goals']['away'] or 0
            st_short = jogo_api['fixture']['status']['short']
            
            try: ph, pa = map(int, s['Placar_Sinal'].split('x'))
            except: ph, pa = 0, 0
            
            key_sinal = gerar_chave_universal(fid, strat, "SINAL")
            key_green = gerar_chave_universal(fid, strat, "GREEN")
            key_red = gerar_chave_universal(fid, strat, "RED")
            deve_enviar = (key_sinal in st.session_state.get('alertas_enviados', set()))
            
            res_final = None
            
            # --- LÓGICA DE APURAÇÃO CORRIGIDA --- [PATCH V5.2]
            tipo_estrategia = classificar_tipo_estrategia(strat)
            res_final = None
            
            if tipo_estrategia == 'RESULTADO':
                if st_short in ['FT', 'AET', 'PEN', 'ABD']:
                    if ph > pa:
                        res_final = '✅ GREEN' if gh > ga else '❌ RED'
                    elif pa > ph:
                        res_final = '✅ GREEN' if ga > gh else '❌ RED'
                    else:
                        res_final = '✅ GREEN' if gh == ga else '❌ RED'
            
            elif tipo_estrategia == 'UNDER':
                gols_no_sinal = ph + pa
                gols_atuais = gh + ga
                if gols_atuais > gols_no_sinal:
                    res_final = '❌ RED'
                elif st_short in ['FT', 'AET', 'PEN', 'ABD']:
                    res_final = '✅ GREEN' if gols_atuais == gols_no_sinal else '❌ RED'
            
            elif tipo_estrategia == 'OVER':
                gols_no_sinal = ph + pa
                gols_atuais = gh + ga
                if gols_atuais > gols_no_sinal:
                    res_final = '✅ GREEN'
                elif st_short in ['FT', 'AET', 'PEN', 'ABD']:
                    res_final = '❌ RED'
            
            else:
                if (gh + ga) > (ph + pa):
                    res_final = '✅ GREEN'
                elif st_short in ['FT', 'AET', 'PEN', 'ABD']:
                    res_final = '❌ RED'
            
            # --- ENVIO E SALVAMENTO ---
            if res_final:

                # [MELHORIA] Atualiza banca automaticamente após GREEN/RED (anti-duplicação)
                try:
                    if 'banca_updates' not in st.session_state:
                        st.session_state['banca_updates'] = set()
                    key_apura = gerar_chave_universal(fid, strat, "GREEN" if "GREEN" in res_final else "RED")
                    if key_apura not in st.session_state['banca_updates']:
                        st.session_state['banca_updates'].add(key_apura)
                
                        if 'banca_atual' not in st.session_state:
                            st.session_state['banca_atual'] = float(st.session_state.get('banca_inicial', 1000.0))
                        saldo = float(st.session_state.get('banca_atual', 1000.0))
                
                        # Stake (prioriza Stake_Recomendado_RS, fallback stake_padrao)
                        stake_val = None
                        try:
                            stake_str = str(s.get('Stake_Recomendado_RS', '')).replace('R$','').strip()
                            stake_str = stake_str.replace('.', '').replace(',', '.')
                            stake_val = float(stake_str) if stake_str else None
                        except:
                            stake_val = None
                        if not stake_val or stake_val <= 0:
                            stake_val = float(st.session_state.get('stake_padrao', 10.0))
                
                        odd_local = 1.50
                        try:
                            odd_local = float(str(s.get('Odd','1.50')).replace(',', '.'))
                        except:
                            odd_local = 1.50
                
                        if 'GREEN' in res_final:
                            saldo += stake_val * (odd_local - 1)
                        else:
                            saldo -= stake_val
                
                        st.session_state['banca_atual'] = float(saldo)
                
                        if 'historico_banca' not in st.session_state:
                            st.session_state['historico_banca'] = []
                        st.session_state['historico_banca'].append({
                            'data': get_time_br().strftime('%Y-%m-%d %H:%M'),
                            'saldo': float(saldo),
                            'resultado': res_final,
                            'stake': float(stake_val),
                            'odd': float(odd_local),
                            'jogo': s.get('Jogo',''),
                            'estrategia': strat,
                            'fid': str(fid)
                        })
                except:
                    pass
                s['Resultado'] = res_final; updates_buffer.append(s)
                
                if deve_enviar:
                    tipo_msg = "GREEN" if "GREEN" in res_final else "RED"
                    
                    if tipo_msg == "GREEN" and key_green not in st.session_state['alertas_enviados']:
                         enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {s['Jogo']}\n📈 Placar: {gh}x{ga}\n🎯 {strat}")
                         st.session_state['alertas_enviados'].add(key_green)
                         
                    elif tipo_msg == "RED" and key_red not in st.session_state['alertas_enviados']:
                         enviar_telegram(token, chats, f"❌ <b>RED CONFIRMADO</b>\n⚽ {s['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {strat}")
                         st.session_state['alertas_enviados'].add(key_red)
                         
    if updates_buffer: atualizar_historico_ram(updates_buffer)

def conferir_resultados_sniper(jogos_live, api_key):
    hist = st.session_state.get('historico_sinais', [])
    snipers = [s for s in hist if ("Sniper" in s['Estrategia'] or "JOGADOR" in s['Estrategia'] or "Mercado" in s['Liga']) and s['Resultado'] == "Pendente"]
    
    if not snipers: return
    updates = []
    ids_live = {str(j['fixture']['id']): j for j in jogos_live} 
    
    for s in snipers:
        # --- CORREÇÃO: PULA MERCADOS ALTERNATIVOS (Deixa a outra função cuidar) ---
        if "Mercado Alternativo" in s['Liga']: continue 
        # --------------------------------------------------------------------------

        if "SNIPER_" in str(s['FID']): continue 
        fid = str(s['FID']).replace("ALT_", "")
        jogo = ids_live.get(fid)
        
        if not jogo:
            try:
                res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                if res.get('response'): jogo = res['response'][0]
            except: pass
            
        if jogo:
            status = jogo['fixture']['status']['short']
            if status in ['FT', 'AET', 'PEN', 'INT', 'ABD', 'PST']:
                gh = jogo['goals']['home'] or 0
                ga = jogo['goals']['away'] or 0
                placar_final = f"{gh}x{ga}"
                res_final = "❌ RED" 
                
                if "Sniper Matinal" in s['Estrategia']:
                      if (gh + ga) >= 1: res_final = "✅ GREEN" 
                elif "JOGADOR" in s['Estrategia']:
                    res_final = f"🏁 FIM ({placar_final})"

                s['Resultado'] = res_final
                s['Placar_Sinal'] = f"Final: {placar_final}"
                updates.append(s)
                
    if updates: atualizar_historico_ram(updates)

def verificar_var_rollback(jogos_live, token, chats):
    if 'var_avisado_cache' not in st.session_state: st.session_state['var_avisado_cache'] = set()
    hist = st.session_state['historico_sinais']
    greens = [s for s in hist if 'GREEN' in str(s['Resultado'])]
    if not greens: return
    updates = []
    for s in greens:
        if "Morno" in s['Estrategia']: continue
        fid = int(clean_fid(s.get('FID', 0)))
        jogo_api = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        if jogo_api:
            gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
            try:
                ph, pa = map(int, s['Placar_Sinal'].split('x'))
                if (gh + ga) <= (ph + pa): 
                    assinatura_var = f"{fid}_{s['Estrategia']}_{gh}x{ga}"
                    if assinatura_var in st.session_state['var_avisado_cache']: continue
                    s['Resultado'] = 'Pendente'; updates.append(s)
                    key_green = gerar_chave_universal(fid, s['Estrategia'], "GREEN")
                    st.session_state['alertas_enviados'].discard(key_green)
                    st.session_state['var_avisado_cache'].add(assinatura_var)
                    enviar_telegram(token, chats, f"⚠️ <b>VAR ACIONADO | GOL ANULADO</b>\n⚽ {s['Jogo']}\n📉 Placar voltou: <b>{gh}x{ga}</b>")
            except: pass
    if updates: atualizar_historico_ram(updates)

def verificar_automacao_bi(token, chat_ids, stake_padrao):
    agora = get_time_br()
    hoje_str = agora.strftime('%Y-%m-%d')
    if st.session_state.get('last_check_date') != hoje_str:
        st.session_state['bi_enviado'] = False; st.session_state['ia_enviada'] = False
        st.session_state['financeiro_enviado'] = False; st.session_state['bigdata_enviado'] = False
        st.session_state['last_check_date'] = hoje_str
    
def verificar_mercados_alternativos(api_key):
    hist = st.session_state.get('historico_sinais', [])
    pendentes = [s for s in hist if s['Liga'] == 'Mercado Alternativo' and s['Resultado'] == 'Pendente']
    
    if not pendentes: return
    updates_buffer = []
    
    for s in pendentes:
        try:
            fid_real = str(s['FID']).replace("ALT_", "")
            
            meta = 0.5
            try: 
                txt_meta = str(s['Placar_Sinal']).split('Meta:')[1].split('|')[0].strip()
                meta = float(txt_meta)
            except: 
                import re
                nums = re.findall(r"[-+]?\d*\.\d+|\d+", str(s['Placar_Sinal']))
                if nums: meta = float(nums[0])

            url = "https://v3.football.api-sports.io/fixtures"
            r = requests.get(url, headers={"x-apisports-key": api_key}, params={"id": fid_real}).json()
            if not r.get('response'): continue
            
            jogo = r['response'][0]
            status = jogo['fixture']['status']['short']
            
            if status not in ['FT', 'AET', 'PEN']: continue
            
            url_stats = "https://v3.football.api-sports.io/fixtures/statistics"
            r_stats = requests.get(url_stats, headers={"x-apisports-key": api_key}, params={"fixture": fid_real}).json()
            if not r_stats.get('response'): continue
            
            stats_home = r_stats['response'][0]['statistics']
            stats_away = r_stats['response'][1]['statistics']
            
            def gv(lista, tipo): return next((x['value'] or 0 for x in lista if x['type'] == tipo), 0)
            
            resultado_final = "❌ RED" 
            valor_real = 0
            
            if "CARTÕES" in s['Estrategia'].upper() or "AÇOUGUEIRO" in s['Estrategia'].upper():
                cards_h = gv(stats_home, "Yellow Cards") + gv(stats_home, "Red Cards")
                cards_a = gv(stats_away, "Yellow Cards") + gv(stats_away, "Red Cards")
                valor_real = cards_h + cards_a
                
                if "MENOS" in str(s.get('Jogo', '')).upper() or "UNDER" in str(s.get('Jogo', '')).upper():
                    if valor_real < meta: resultado_final = "✅ GREEN"
                else:
                    if valor_real > meta: resultado_final = "✅ GREEN"
                
                s['Placar_Sinal'] = f"Meta: {meta} | Saiu: {valor_real}"

            elif "DEFESAS" in s['Estrategia'].upper() or "MURALHA" in s['Estrategia'].upper():
                saves_h = gv(stats_home, "Goalkeeper Saves")
                saves_a = gv(stats_away, "Goalkeeper Saves")
                valor_real = max(saves_h, saves_a) 
                
                if valor_real >= meta: resultado_final = "✅ GREEN"
                s['Placar_Sinal'] = f"Meta: {meta} | Defesas: {valor_real}"
            
            s['Resultado'] = resultado_final
            updates_buffer.append(s)
            
        except Exception as e: print(f"Erro ao conferir alternativo: {e}")
            
    if updates_buffer: atualizar_historico_ram(updates_buffer)

def validar_multiplas_pendentes(jogos_live, api_key, token, chat_ids):
    if 'multiplas_pendentes' not in st.session_state or not st.session_state['multiplas_pendentes']: return
    pendentes = st.session_state['multiplas_pendentes']
    mapa_live = {str(j['fixture']['id']): j for j in jogos_live}
    for m in pendentes:
        if m['status'] != 'Pendente': continue
        if m['data'] != get_time_br().strftime('%Y-%m-%d'): continue
        resultados_jogos = []
        placar_final_str = []
        for fid in m['fids']:
            jogo = mapa_live.get(fid)
            if not jogo:
                try:
                    res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                    if res.get('response'): jogo = res['response'][0]
                except: pass
            if not jogo: 
                resultados_jogos.append("PENDENTE")
                continue
            status_short = jogo['fixture']['status']['short']
            gh = jogo['goals']['home'] or 0
            ga = jogo['goals']['away'] or 0
            total_agora = gh + ga
            if m['tipo'] == "MATINAL":
                condicao_green = (total_agora >= 1) 
            else:
                gols_ref = m.get('gols_ref', {}).get(fid, 0)
                condicao_green = (total_agora > gols_ref)
            if condicao_green: resultados_jogos.append("GREEN")
            elif status_short in ['FT', 'AET', 'PEN', 'INT']: resultados_jogos.append("RED")
            else: resultados_jogos.append("PENDENTE")
            placar_final_str.append(f"{gh}x{ga}")
        if "RED" in resultados_jogos:
            msg = f"❌ <b>RED MÚLTIPLA FINALIZADA</b>\nUma das seleções não bateu.\n📉 Placar Final: {' / '.join(placar_final_str)}"
            enviar_telegram(token, chat_ids, msg)
            m['status'] = "RED"
            item_save = {"FID": m['id_unico'], "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": "Múltiplas", "Jogo": " + ".join(m['nomes']), "Placar_Sinal": " / ".join(placar_final_str), "Estrategia": f"Múltipla {m['tipo']}", "Resultado": "❌ RED", "HomeID": "", "AwayID": "", "Odd": "", "Odd_Atualizada": "", "Opiniao_IA": "Aprovado", "Probabilidade": "Alta"}
            adicionar_historico(item_save)
        elif "PENDENTE" not in resultados_jogos and all(x == "GREEN" for x in resultados_jogos):
            msg = f"✅ <b>GREEN MÚLTIPLA CONFIRMADO!</b>\nTodas as seleções bateram!\n📈 Placares: {' / '.join(placar_final_str)}"
            enviar_telegram(token, chat_ids, msg)
            m['status'] = "GREEN"
            item_save = {"FID": m['id_unico'], "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": "Múltiplas", "Jogo": " + ".join(m['nomes']), "Placar_Sinal": " / ".join(placar_final_str), "Estrategia": f"Múltipla {m['tipo']}", "Resultado": "✅ GREEN", "HomeID": "", "AwayID": "", "Odd": "", "Odd_Atualizada": "", "Opiniao_IA": "Aprovado", "Probabilidade": "Alta"}
            adicionar_historico(item_save)

# --- BARRA LATERAL (CONFIGURAÇÕES E BOTÕES MANUAIS) ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=True):
        st.session_state['API_KEY'] = st.text_input("Chave API:", value=st.session_state['API_KEY'], type="password")
        st.session_state['TG_TOKEN'] = st.text_input("Token Telegram:", value=st.session_state['TG_TOKEN'], type="password")
        st.session_state['TG_CHAT'] = st.text_input("Chat IDs:", value=st.session_state['TG_CHAT'])
        INTERVALO = st.slider("Ciclo (s):", 60, 300, 60)
        if st.button("🧹 Limpar Cache"): 
            st.cache_data.clear(); carregar_tudo(force=True); st.session_state['last_db_update'] = 0; st.toast("Cache Limpo!")
    
    with st.expander("🛠️ Ferramentas Manuais", expanded=False):
        if st.button("🌅 Testar Múltipla + Alternativos"):
            with st.spinner("Gerando alertas..."):
                verificar_alerta_matinal(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], st.session_state['API_KEY'])
                st.success("Alertas Matinais Disparados (Se houver)!")
        
        if st.button("🧠 Pedir Análise do BI"):
            if IA_ATIVADA:
                with st.spinner("🤖 Consultando dados..."):
                    analise = analisar_bi_com_ia(); st.markdown("### 📝 Relatório"); st.info(analise)
            else: st.error("IA Desconectada.")

        st.markdown("---")
        st.caption("🧪 Laboratório de Estratégias")
        foco_strat = st.selectbox("Qual o foco?", ["Escanteios", "Gols (Over)", "Gols (Under)", "Cartões", "Zebra/Momento"], key="foco_lab")
        if st.button(f"✨ Criar Estratégia de {foco_strat}"):
            if IA_ATIVADA:
                with st.spinner(f"🤖 A IA está varrendo o Big Data..."):
                    sugestao = criar_estrategia_nova_ia(foco_strat)
                    st.markdown(f"### 💡 Nova Estratégia"); st.info(sugestao)
            else: st.error("IA Desconectada.")

        if st.button("🔧 Otimizar Estratégias"):
            if IA_ATIVADA:
                with st.spinner("🤖 Cruzando performance..."):
                    sugestao = otimizar_estrategias_existentes_ia(); st.markdown("### 🛠️ Plano"); st.info(sugestao)
            else: st.error("IA Desconectada.")
        
        if st.button("🚀 Gerar Alavancagem (Jogo Único)"):
            if IA_ATIVADA:
                with st.spinner("🤖 Triangulando API + Big Data + Histórico Pessoal..."):
                    enviar_alavancagem(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], st.session_state['API_KEY'])
                    st.success("Análise de Alavancagem Realizada e Salva!")
            else: st.error("IA Desconectada.")
        
        st.markdown("---")
        if st.button("📉 Escanear Drop Odds (Estratégia Vídeo)"):
            if IA_ATIVADA:
                with st.spinner("Comparando Bet365 vs Pinnacle..."):
                    drops = scanner_drop_odds_pre_live(st.session_state['API_KEY'])
                    if drops:
                        st.success(f"Encontradas {len(drops)} oportunidades!")
                        for d in drops:
                            st.write(f"⚽ {d['jogo']} | Bet365: {d['odd_b365']} vs Pin: {d['odd_pinnacle']}")
                            msg = f"💰 <b>ESTRATÉGIA CASHOUT (DROP ODDS)</b>\n\n⚽ <b>{d['jogo']}</b>\n📉 <b>DESAJUSTE:</b>\n• Bet365: @{d['odd_b365']}\n• Pinnacle: @{d['odd_pinnacle']}\n• Drop: {d['valor']:.1f}%"
                            enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], msg)
                    else: st.warning("Nenhum desajuste de odd encontrado nas Ligas Top agora.")
            else: st.error("IA/API necessária.")

        if st.button("🔄 Forçar Backfill (Salvar Jogos Perdidos)"):
            with st.spinner("Buscando na API todos os jogos finalizados hoje..."):
                hoje_real = get_time_br().strftime('%Y-%m-%d')
                todos_jogos_hoje = buscar_agenda_cached(st.session_state['API_KEY'], hoje_real)
                ft_pendentes = [j for j in todos_jogos_hoje if j['fixture']['status']['short'] in ['FT', 'AET', 'PEN'] and str(j['fixture']['id']) not in st.session_state['jogos_salvos_bigdata']]
                if ft_pendentes:
                    st.info(f"Processando {len(ft_pendentes)} jogos...")
                    stats_recuperadas = atualizar_stats_em_paralelo(ft_pendentes, st.session_state['API_KEY'])
                    count_salvos = 0
                    for fid, stats in stats_recuperadas.items():
                        j_obj = next((x for x in ft_pendentes if str(x['fixture']['id']) == str(fid)), None)
                        if j_obj: salvar_bigdata(j_obj, stats) 
                        count_salvos += 1
                    st.success(f"✅ Recuperados e Salvos: {count_salvos} jogos!")
                else: st.warning("Nenhum jogo finalizado pendente.")
        
        if st.button("📊 Enviar Relatório BI"): enviar_relatorio_bi(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT']); st.toast("Enviado!")
        if st.button("💰 Enviar Relatório Fin."):
            if 'last_fin_stats' in st.session_state:
                s = st.session_state['last_fin_stats']
                enviar_relatorio_financeiro(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], s['cenario'], s['lucro'], s['roi'], s['entradas'])
                st.toast("Enviado!")
            else: st.error("Abra a aba Financeiro.")

        # --- BOTÃO DE EXPORTAÇÃO DE DADOS ---
        with st.expander("💾 Exportação de Dados (Segurança)", expanded=False):
            st.info("Use para baixar os dados e analisar na IA externa.")
            if st.button("📥 Gerar Arquivo do Firebase"):
                if db_firestore:
                    with st.spinner("Baixando dados da nuvem..."):
                        try:
                            # Pega TODOS os jogos salvos
                            docs = db_firestore.collection("BigData_Futebol").stream()
                            all_data = [d.to_dict() for d in docs]
                            
                            # Converte para JSON
                            json_str = json.dumps(all_data, indent=2, default=str)
                            
                            st.success(f"{len(all_data)} jogos recuperados!")
                            st.download_button(
                                label="⬇️ Clique para Salvar no PC (JSON)",
                                data=json_str,
                                file_name="bigdata_futebol_backup.json",
                                mime="application/json"
                            )
                        except Exception as e:
                            st.error(f"Erro ao baixar: {e}")
                else:
                    st.error("Firebase não conectado.")

    with st.expander("💰 Gestão de Banca", expanded=False):
        st.markdown("### Configurações")

        # Campos originais (mantidos)
        stake_padrao = st.number_input("Valor da Aposta (R$)", value=float(st.session_state.get('stake_padrao', 10.0)), step=5.0)
        banca_inicial = st.number_input("Banca Inicial (R$)", value=float(st.session_state.get('banca_inicial', 100.0)), step=50.0)
        st.session_state['stake_padrao'] = float(stake_padrao)
        st.session_state['banca_inicial'] = float(banca_inicial)

        # Defaults
        if 'banca_atual' not in st.session_state:
            st.session_state['banca_atual'] = float(st.session_state.get('banca_inicial', 1000.0))
        if 'modo_gestao_banca' not in st.session_state:
            st.session_state['modo_gestao_banca'] = 'fracionario'

        modo_gestao = st.radio(
            "Modo de Gestão:",
            ["Conservador (Flat 1-2%)", "Kelly Fracionário (Recomendado)", "Kelly Completo (Agressivo)"],
            index=1,
            help="""• Conservador: Sempre 1-2% (seguro mas cresce devagar)
• Kelly Fracionário: Metade do Kelly (equilibrado)
• Kelly Completo: Kelly puro (agressivo, maior risco)"""
        )

        if "Conservador" in modo_gestao:
            modo_codigo = "conservador"
        elif "Fracionário" in modo_gestao:
            modo_codigo = "fracionario"
        else:
            modo_codigo = "completo"
        st.session_state['modo_gestao_banca'] = modo_codigo

        banca_atual = st.number_input(
            "Banca Atual (R$)",
            value=float(st.session_state.get('banca_atual', banca_inicial)),
            step=50.0,
            help="Atualizada automaticamente após cada aposta (GREEN/RED)"
        )
        st.session_state['banca_atual'] = float(banca_atual)

        st.markdown('---')
        st.markdown('### 🧮 Simulador de Stake')
        col_sim1, col_sim2 = st.columns(2)
        prob_sim = col_sim1.slider('Probabilidade (%)', 50, 100, 75)
        odd_sim = col_sim2.number_input('Odd', 1.20, 5.00, 1.80, 0.10)

        stake_simulado = calcular_stake_recomendado(banca_atual, prob_sim, odd_sim, modo_codigo)
        st.markdown(f"**Resultado:** - Stake: **{stake_simulado['porcentagem']}%** = **R$ {stake_simulado['valor']:.2f}** | Retorno Green: **R$ {(stake_simulado['valor'] * (odd_sim - 1)):.2f}**")

        
    with st.expander("📶 Consumo API", expanded=False):
        verificar_reset_diario()
        u = st.session_state['api_usage']; perc = min(u['used'] / u['limit'], 1.0) if u['limit'] > 0 else 0
        st.progress(perc); st.caption(f"Utilizado: **{u['used']}** / {u['limit']} ({perc*100:.1f}%)")
    
    with st.expander("🤖 Consumo IA (Gemini)", expanded=False):
        u_ia = st.session_state['gemini_usage']; u_ia['limit'] = 10000 
        perc_ia = min(u_ia['used'] / u_ia['limit'], 1.0)
        st.progress(perc_ia); st.caption(f"Requições Hoje: **{u_ia['used']}** / {u_ia['limit']}")

    st.write("---")
    tg_ok, tg_nome = testar_conexao_telegram(st.session_state['TG_TOKEN'])
    if tg_ok: st.markdown(f'<div class="status-active">✈️ TELEGRAM: CONECTADO ({tg_nome})</div>', unsafe_allow_html=True)
    else: st.markdown(f'<div class="status-error">❌ TELEGRAM: ERRO ({tg_nome})</div>', unsafe_allow_html=True)
    if IA_ATIVADA: st.markdown('<div class="status-active">🤖 IA GEMINI ATIVA</div>', unsafe_allow_html=True)
    else: st.markdown('<div class="status-error">❌ IA DESCONECTADA</div>', unsafe_allow_html=True)
    if db_firestore: st.markdown('<div class="status-active">🔥 FIREBASE CONECTADO</div>', unsafe_allow_html=True)
    else: st.markdown('<div class="status-warning">⚠️ FIREBASE OFFLINE</div>', unsafe_allow_html=True)
    st.write("---")
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    st.markdown("---")
    st.markdown("### ⚠️ Zona de Perigo")
    if st.button("☢️ ZERAR ROBÔ", type="primary", use_container_width=True): st.session_state['confirmar_reset'] = True
    if st.session_state.get('confirmar_reset'):
        st.error("Tem certeza? Isso apaga TODO o histórico.")
        c1, c2 = st.columns(2)
        if c1.button("✅ SIM"): 
            st.cache_data.clear()
            st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
            salvar_aba("Historico", st.session_state['historico_full'])
            st.session_state['confirmar_reset'] = False; st.rerun()
        if c2.button("❌ NÃO"): st.session_state['confirmar_reset'] = False; st.rerun()

# --- LOOP PRINCIPAL DO ROBÔ ---
if st.session_state.ROBO_LIGADO:
    with placeholder_root.container():
        carregar_tudo()
        s_padrao = st.session_state.get('stake_padrao', 10.0)
        b_inicial = st.session_state.get('banca_inicial', 100.0)
        safe_token = st.session_state.get('TG_TOKEN', '')
        safe_chat = st.session_state.get('TG_CHAT', '')
        safe_api = st.session_state.get('API_KEY', '')

        verificar_automacao_bi(safe_token, safe_chat, s_padrao)
        verificar_alerta_matinal(safe_token, safe_chat, safe_api)
        
        ids_black = [normalizar_id(x) for x in st.session_state['df_black']['id'].values]
        df_obs = st.session_state.get('df_vip', pd.DataFrame()); count_obs = len(df_obs)
        df_safe_show = st.session_state.get('df_safe', pd.DataFrame()); count_safe = len(df_safe_show)
        ids_safe = [normalizar_id(x) for x in df_safe_show['id'].values]
        hoje_real = get_time_br().strftime('%Y-%m-%d')
        
        if 'historico_full' in st.session_state and not st.session_state['historico_full'].empty:
             df_full = st.session_state['historico_full']
             st.session_state['historico_sinais'] = df_full[df_full['Data'] == hoje_real].to_dict('records')[::-1]

        api_error = False; jogos_live = []
        try:
            url = "https://v3.football.api-sports.io/fixtures"
            resp = requests.get(url, headers={"x-apisports-key": safe_api}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
            update_api_usage(resp.headers); res = resp.json()
            raw_live = res.get('response', []) if not res.get('errors') else []
            dict_clean = {j['fixture']['id']: j for j in raw_live}
            jogos_live = list(dict_clean.values())
            api_error = bool(res.get('errors'))
            if api_error and "errors" in res: st.error(f"Detalhe do Erro: {res['errors']}")
        except Exception as e: jogos_live = []; api_error = True; st.error(f"Erro de Conexão: {e}")
# ==============================================================================
        # [CORREÇÃO CRÍTICA] BAIXAR ESTATÍSTICAS DOS JOGOS (CHUTES, CANTOS, ETC)
        # ==============================================================================
        if not api_error and jogos_live:
            # Seleciona apenas jogos que estão rolando (1º tempo, 2º tempo, Intervalo)
            jogos_para_baixar = [j for j in jogos_live if j['fixture']['status']['short'] in ['1H', '2H', 'HT', 'ET']]
            # [MELHORIA] Cache de estatísticas: baixa apenas jogos SEM stats em memória
            jogos_para_baixar = [j for j in jogos_para_baixar if f"st_{j['fixture']['id']}" not in st.session_state]
            
            if jogos_para_baixar:
                # Baixa as stats em paralelo (rápido) e salva na memória do robô
                stats_novos = atualizar_stats_em_paralelo(jogos_para_baixar, safe_api)
                for fid_stat, dados_stat in stats_novos.items():
                    st.session_state[f"st_{fid_stat}"] = dados_stat
        # ==============================================================================

        if not api_error: 
            # 1. Rotinas Padrão
            check_green_red_hibrido(jogos_live, safe_token, safe_chat, safe_api)
            conferir_resultados_sniper(jogos_live, safe_api) 
            verificar_var_rollback(jogos_live, safe_token, safe_chat)
            
            # 2. NOVAS ROTINAS
            verificar_multipla_quebra_empate(jogos_live, safe_token, safe_chat)
            validar_multiplas_pendentes(jogos_live, safe_api, safe_token, safe_chat)
            verificar_mercados_alternativos(safe_api)

            # ==============================================================================
            # [CORREÇÃO] SALVAR BIG DATA AUTOMATICAMENTE (jogos finalizados)
            # ==============================================================================
            if db_firestore:
                jogos_ft = [j for j in jogos_live if j['fixture']['status']['short'] in ['FT', 'AET', 'PEN']]
                for j_ft in jogos_ft:
                    fid_ft = str(j_ft['fixture']['id'])
                    if fid_ft not in st.session_state['jogos_salvos_bigdata']:
                        stats_ft = st.session_state.get(f"st_{j_ft['fixture']['id']}", [])
                        if not stats_ft:
                            try:
                                _, stats_ft, hdrs = fetch_stats_single(j_ft['fixture']['id'], safe_api)
                                if hdrs: update_api_usage(hdrs)
                            except: stats_ft = []
                        if stats_ft:
                            salvar_bigdata(j_ft, stats_ft)
            # ==============================================================================

        radar = []; agenda = []; candidatos_multipla = []; ids_no_radar = []
        if not api_error:
            jogos_para_atualizar = []
            agora_dt = datetime.now()
            
            # --- DEFINIÇÃO DA VARIÁVEL PROX (Faltava aqui) ---
            prox = buscar_agenda_cached(safe_api, hoje_real)
            agora = get_time_br()
            # -------------------------------------------------
            
            # Loop de Análise dos Jogos ao Vivo
            for j in jogos_live:
                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                if lid in ids_black: continue
                status_short = j['fixture']['status']['short']
                elapsed = j['fixture']['status']['elapsed']
                
                if status_short not in ['1H', '2H', 'HT', 'ET']: continue
                if (elapsed is None or elapsed == 0) and status_short != 'HT': continue

                nome_liga_show = j['league']['name']
                if lid in ids_safe: nome_liga_show += " 🛡️"
                elif lid in df_obs['id'].values: nome_liga_show += " ⚠️"
                else: nome_liga_show += " ❓" 
                ids_no_radar.append(fid)
                tempo = j['fixture']['status']['elapsed'] or 0; st_short = j['fixture']['status']['short']
                home = j['teams']['home']['name']; away = j['teams']['away']['name']
                placar = f"{j['goals']['home']}x{j['goals']['away']}"; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
                if st_short == 'FT': continue 
                
                stats = st.session_state.get(f"st_{fid}", [])
                status_vis = "👁️" if stats else "💤"
                
                rank_h = None; rank_a = None
                lista_sinais = []
                if stats:
                    lista_sinais = processar(j, stats, tempo, placar, rank_h, rank_a)
                    salvar_safe_league_basic(lid, j['league']['country'], j['league']['name'], tem_tabela=(rank_h is not None))
                    resetar_erros(lid)
                    if st_short == 'HT' and gh == 0 and ga == 0:
                        try:
                            s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
                            v1 = next((x['value'] for x in s1 if x['type']=='Total Shots'), 0) or 0
                            v2 = next((x['value'] for x in s2 if x['type']=='Total Shots'), 0) or 0
                            sg1 = next((x['value'] for x in s1 if x['type']=='Shots on Goal'), 0) or 0
                            sg2 = next((x['value'] for x in s2 if x['type']=='Shots on Goal'), 0) or 0
                            if (v1+v2) > 12 and (sg1+sg2) > 6: candidatos_multipla.append({'fid': fid, 'jogo': f"{home} x {away}", 'stats': f"{v1+v2} Chutes", 'indica': "Over 0.5 FT"})
                        except: pass
                else: gerenciar_erros(lid, j['league']['country'], j['league']['name'], fid)

                if lista_sinais:
                    status_vis = f"✅ {len(lista_sinais)} Sinais"
                    
                    dados_contextuais = analisar_tendencia_macro_micro(safe_api, j['teams']['home']['id'], j['teams']['away']['id'])
                    
                    txt_history = "Dados indisponíveis."
                    if dados_contextuais:
                        txt_history = f"""
                        CASA (Forma Real): {dados_contextuais['home']['resumo']}
                        FORA (Forma Real): {dados_contextuais['away']['resumo']}
                        """
                    
                    nota_home = buscar_rating_inteligente(safe_api, j['teams']['home']['id'])
                    nota_away = buscar_rating_inteligente(safe_api, j['teams']['away']['id'])
                    
                    txt_bigdata = consultar_bigdata_cenario_completo(j['teams']['home']['id'], j['teams']['away']['id'])

                    df_sheets = st.session_state.get('historico_full', pd.DataFrame())
                    txt_pessoal = "Neutro"
                    if not df_sheets.empty:
                        f_h = df_sheets[df_sheets['Jogo'].str.contains(home, na=False, case=False)]
                        if len(f_h) > 2:
                            greens = len(f_h[f_h['Resultado'].str.contains('GREEN', na=False)])
                            wr = (greens/len(f_h))*100
                            txt_pessoal = f"Winrate Pessoal com {home}: {wr:.0f}%"

                    extra_ctx = f"""
                    --- RAIO-X PROFUNDO ---
                    1. 🧬 DNA RECENTE (Últimos 5J): 
                    {txt_history}
                    
                    2. 💾 BIG DATA (Histórico Confrontos/Liga): 
                    {txt_bigdata}
                    
                    3. 👤 PERFIL DO USUÁRIO: 
                    {txt_pessoal}
                    
                    4. ⭐ RATING (Força do Elenco): 
                    {nota_home} (Casa) x {nota_away} (Fora)
                    """
                    
                    for s in lista_sinais:
                        prob = "..." 
                        liga_safe = j['league']['name'].replace("<", "").replace(">", "").replace("&", "e")
                        home_safe = home.replace("<", "").replace(">", "").replace("&", "e")
                        away_safe = away.replace("<", "").replace(">", "").replace("&", "e")
                        rh = s.get('rh', 0); ra = s.get('ra', 0)
                        
                        uid_normal = gerar_chave_universal(fid, s['tag'], "SINAL")
                        uid_super = f"SUPER_{uid_normal}"
                        
                        ja_enviado_total = False
                        if uid_normal in st.session_state['alertas_enviados']: ja_enviado_total = True
                        if not ja_enviado_total:
                            for item_hist in st.session_state['historico_sinais']:
                                key_hist = gerar_chave_universal(item_hist['FID'], item_hist['Estrategia'], "SINAL")
                                if key_hist == uid_normal:
                                    ja_enviado_total = True; st.session_state['alertas_enviados'].add(uid_normal); break
                        if ja_enviado_total: continue 
                        
                        st.session_state['alertas_enviados'].add(uid_normal)
                        
                        odd_atual_str = get_live_odds(fid, safe_api, s['tag'], gh+ga, tempo)
                        try: odd_val = float(odd_atual_str)
                        except: odd_val = 0.0
                        
                        destaque_odd = ""
                        emoji_sinal = "✅"
                        titulo_sinal = f"SINAL {s['tag'].upper()}"
                        # [PATCH V5.2] Usa descrição correta da aposta
                        desc_aposta = obter_descricao_aposta(s['tag'])
                        texto_acao_original = desc_aposta['ordem']
                        # Info do tipo para o usuário
                        tipo_info = ''
                        if desc_aposta['tipo'] == 'UNDER':
                            tipo_info = '❄️ <b>UNDER</b>: NÃO sai gol\n'
                        elif desc_aposta['tipo'] == 'RESULTADO':
                            tipo_info = '👴 <b>RESULTADO</b>: Manter vitória\n'
                        else:
                            tipo_info = '⚽ <b>OVER</b>: Sai gol\n'
                        bloco_aviso_odd = ""
                        # [MELHORIA V3] Odd mínima por estratégia
                        odd_min_estrat = obter_odd_minima(s['tag'])
                        odd_crit_estrat = max(1.10, odd_min_estrat - 0.20)
                        if odd_val > 0 and odd_val < odd_crit_estrat:
                            emoji_sinal = "⛔"
                            bloco_aviso_odd = f"⚠️ <b>ALERTA: ODD BAIXA (@{odd_val:.2f})</b>\n⏳ <i>Não entre agora. Aguarde ou ignore.</i>\n────────────────\n"
                        elif odd_val >= odd_crit_estrat and odd_val < odd_min_estrat:
                            emoji_sinal = "⏳"
                            bloco_aviso_odd = f"👀 <b>AGUARDE VALORIZAR (@{odd_val:.2f})</b>\n🎯 <i>Meta: Entrar acima de @{odd_min_estrat:.2f}</i>\n────────────────\n"
                        elif odd_val >= odd_min_estrat:
                            emoji_sinal = "✅"
                            bloco_aviso_odd = f"🔥 <b>ODD DE VALOR: @{odd_val:.2f}</b>\n────────────────\n"

                        if odd_val >= 1.80:
                            destaque_odd = "\n💎 <b>SUPER ODD DETECTADA! (EV+)</b>"
                            st.session_state['alertas_enviados'].add(uid_super)
                        
                        # ═══════════════════════════════════════════════════════════════
                        # [NOVA IA COMPLETA] Análise com 5 módulos inteligentes
                        # ═══════════════════════════════════════════════════════════════
                        
                        opiniao_txt = ""; prob_txt = "70%"; opiniao_db = "Neutro"
                        analise_ia_completa_resultado = None
                        
                        if IA_ATIVADA:
                            try:
                                time.sleep(0.2)
                                
                                # 1. Análise básica da IA (probabilidade)
                                dados_ia = {'jogo': f"{home} x {away}", 'placar': placar, 'tempo': f"{tempo}'", 'fid': fid, 'odd_atual': odd_val}
                                time_fav_ia = s.get('favorito', '')
                                opiniao_txt, prob_txt = consultar_ia_gemini(
                                    dados_ia, s['tag'], stats, rh, ra, 
                                    extra_context=extra_ctx, 
                                    time_favoravel=time_fav_ia
                                )
                                
                                # Extrai probabilidade numérica
                                try:
                                    prob_numerica = int(prob_txt.replace('%', '').strip())
                                except:
                                    prob_numerica = 70
                                
                                # 2. Análise COMPLETA (5 módulos)
                                analise_ia_completa_resultado = ia_analise_completa(
                                    estrategia=s['tag'],
                                    liga=j['league']['name'],
                                    time_casa=home,
                                    time_fora=away,
                                    placar=placar,
                                    tempo_jogo=tempo,
                                    odd_atual=odd_val,
                                    probabilidade_ia=prob_numerica,
                                    stats_jogo=s.get('stats', ''),
                                    big_data_info=txt_bigdata
                                )
                                
                                # 3. Decisão baseada no consenso multi-agente
                                consenso = analise_ia_completa_resultado['multi_agente']['consenso']
                                
                                if consenso == "favoravel":
                                    opiniao_db = "💎 Aprovado (Consenso)"
                                elif consenso == "dividido":
                                    opiniao_db = "⚖️ Arriscado (Dividido)"
                                else:
                                    opiniao_db = "⚠️ Neutro (Divergência)"
                                    
                            except Exception as e:
                                print(f"Erro IA Completa: {e}")
                                opiniao_db = "Erro IA"
                        else:
                            opiniao_db = "Offline"

                        # [NOVA IA] Status sempre Pendente (IA nunca veta, apenas informa)
                        status_inicial = "Pendente"

                        item = {
                            "FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'),
                            "Liga": j['league']['name'], "Jogo": f"{home} x {away} ({placar})", "Placar_Sinal": placar,
                            "Estrategia": s['tag'], 
                            "Resultado": status_inicial, 
                            "HomeID": str(j['teams']['home']['id']) if lid in ids_safe else "", "AwayID": str(j['teams']['away']['id']) if lid in ids_safe else "",
                            "Odd": odd_atual_str, "Odd_Atualizada": "", 
                            "Opiniao_IA": opiniao_db, 
                            "Probabilidade": prob_txt 
                        }
                        
                        if adicionar_historico(item):
                            try:
                                txt_winrate_historico = ""
                                if txt_pessoal != "Neutro": txt_winrate_historico = f" | 👤 {txt_pessoal}"

                                header_winrate = ""
                                df_h = st.session_state.get('historico_full', pd.DataFrame())
                                if not df_h.empty:
                                    strat_f = df_h[df_h['Estrategia'] == s['tag']]
                                    if len(strat_f) >= 3:
                                        greens_s = len(strat_f[strat_f['Resultado'].str.contains('GREEN', na=False)])
                                        wr_s = (greens_s / len(strat_f)) * 100
                                        header_winrate = f" | 🟢 <b>Strat: {wr_s:.0f}%</b>"

                                if not header_winrate and "Winrate Pessoal" in txt_pessoal:
                                    wr_val = txt_pessoal.split(':')[-1].strip()
                                    header_winrate = f" | 👤 <b>Time: {wr_val}</b>"

                                texto_momento = "Morno 🧊"
                                if rh > ra: texto_momento = "Pressão Casa 🔥"
                                elif ra > rh: texto_momento = "Pressão Visitante 🔥"
                                elif rh > 2 or ra > 2: texto_momento = "Jogo Aberto ⚡"

                                linha_bd = ""
                                if "MANDANTE" in txt_bigdata: linha_bd = f"• 💾 <b>Big Data:</b> Tendência confirmada.\n"

                                txt_stats_extras = ""
                                try:
                                    txt_stats_extras += f"\n📊 <b>Dados do Momento:</b> <i>{texto_momento}</i>"
                                    if nota_home != "N/A":
                                        txt_stats_extras += f"\n⭐ <b>Rating:</b> Casa {nota_home} | Fora {nota_away}"
                                    
                                    if 'dados_contextuais' in locals() and dados_contextuais:
                                        micro_h = dados_contextuais['home']['micro']
                                        micro_a = dados_contextuais['away']['micro']
                                        cards_h = dados_contextuais['home'].get('avg_cards', 0)
                                        cards_a = dados_contextuais['away'].get('avg_cards', 0)
                                        reds_h = dados_contextuais['home']['reds']
                                        reds_a = dados_contextuais['away']['reds']
                                        
                                        txt_stats_extras += "\n🔎 <b>Raio-X (Tendência):</b>"
                                        txt_stats_extras += f"\n📈 <b>Gols (Recente):</b> Casa {micro_h}% | Fora {micro_a}% (Over 1.5)"
                                        
                                        if cards_h > 0 or cards_a > 0:
                                            txt_stats_extras += f"\n🟨 <b>Cartões (Média):</b> {cards_h:.1f} vs {cards_a:.1f}"
                                            if reds_h >= 3 or reds_a >= 3:
                                                total_reds = reds_h + reds_a
                                                txt_stats_extras += f" 🟥 (PERIGO: {total_reds} Vermelhos em 10j)"

                                except Exception as e: print(f"Erro visual: {e}")
                    
                                msg = f"{emoji_sinal} <b>{titulo_sinal}</b>{header_winrate}\n"
                                msg += f"🏆 {liga_safe}\n"
                                msg += f"⚽ <b>{home_safe} 🆚 {away_safe}</b>\n"
                                msg += f"⏰ {tempo}' min | 🥅 Placar: {placar}\n\n"
                                msg += f"{tipo_info}"

                                msg += f"{bloco_aviso_odd}"
                                msg += f"{texto_acao_original}\n"
                                if destaque_odd: msg += f"{destaque_odd}\n"
                                msg += f"{txt_stats_extras}\n"
                                msg += "──────────────\n"
                                msg += f"📊 <b>Raio-X do Momento (Live):</b>\n"
                                msg += f"• 🔥 <b>Ataque:</b> {s.get('stats', 'Pressão')}\n"
                                msg += linha_bd
                                
                                # [NOVA IA] Análise resumida da IA antiga
                                if opiniao_txt:
                                    msg += f"\n{opiniao_txt}"
                                
                                # [NOVA IA COMPLETA] Análise dos 5 módulos
                                if analise_ia_completa_resultado:
                                    msg += formatar_mensagem_ia_completa(
                                        analise_ia_completa_resultado, 
                                        tipo_sinal="AO_VIVO"
                                    )
                                else:
                                    # Fallback se IA não rodou
                                    msg += f"\n\n🤖 <b>CLASSIFICAÇÃO IA: {opiniao_db}</b>"
                                
                                # [REGRA DE OURO] SEMPRE ENVIA O SINAL (IA nunca veta)
                                enviar_telegram(safe_token, safe_chat, msg)
                                
                                # Toast apropriado
                                if "Aprovado" in opiniao_db or "DIAMANTE" in opiniao_db:
                                    st.toast(f"✅ Sinal Enviado: {s['tag']} (IA Favorável)")
                                elif "Arriscado" in opiniao_db or "Dividido" in opiniao_db:
                                    st.toast(f"⚖️ Sinal Enviado: {s['tag']} (IA Dividida)")
                                else:
                                    st.toast(f"📤 Sinal Enviado: {s['tag']}")

                            except Exception as e: print(f"Erro ao enviar sinal: {e}")

                        elif uid_super not in st.session_state['alertas_enviados'] and odd_val >= 1.80:
                             st.session_state['alertas_enviados'].add(uid_super)
                             msg_super = (f"💎 <b>OPORTUNIDADE DE VALOR!</b>\n\n⚽ {home} 🆚 {away}\n📈 <b>A Odd subiu!</b> Entrada valorizada.\n🔥 <b>Estratégia:</b> {s['tag']}\n💰 <b>Nova Odd: @{odd_atual_str}</b>")
                             enviar_telegram(safe_token, safe_chat, msg_super)
                radar.append({"Liga": nome_liga_show, "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})
            
            if candidatos_multipla:
                novos = [c for c in candidatos_multipla if c['fid'] not in st.session_state['multiplas_enviadas']]
                if novos:
                    msg = "<b>🚀 OPORTUNIDADE DE MÚLTIPLA (HT) 🚀</b>\n" + "".join([f"\n⚽ {c['jogo']} ({c['stats']})\n⚠️ AÇÃO: {c['indica']}" for c in novos])
                    for c in novos: st.session_state['multiplas_enviadas'].add(c['fid'])
                    enviar_telegram(safe_token, safe_chat, msg)
            
            for p in prox:
                try:
                    if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] in ['NS', 'TBD'] and p['fixture']['id'] not in ids_no_radar:
                        if datetime.fromisoformat(p['fixture']['date']) > agora:
                            l_id = normalizar_id(p['league']['id']); l_nm = p['league']['name']
                            if l_id in ids_safe: l_nm += " 🛡️"
                            elif l_id in df_obs['id'].values: l_nm += " ⚠️"
                            agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": l_nm, "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
                except: pass
        
        if st.session_state.get('precisa_salvar'):
            if 'historico_full' in st.session_state and not st.session_state['historico_full'].empty:
                st.caption("⏳ Sincronizando dados pendentes...")
                salvar_aba("Historico", st.session_state['historico_full'])
        
        if api_error: st.markdown('<div class="status-error">🚨 API LIMITADA - AGUARDE</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        hist_hj = pd.DataFrame(st.session_state['historico_sinais'])
        t, g, r, w = calcular_stats(hist_hj)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t}</div><div class="metric-sub">{g} Green | {r} Red</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(radar)}</div><div class="metric-sub">Monitorando</div></div>', unsafe_allow_html=True)
        cor_winrate = "#00FF00" if w >= 50 else "#FFFF00"
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Assertividade Dia</div><div class="metric-value" style="color:{cor_winrate};">{w:.1f}%</div><div class="metric-sub">Winrate Diário</div></div>', unsafe_allow_html=True)
        
        st.write("")
        abas = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", f"💰 Financeiro", f"📜 Histórico ({len(hist_hj)})", "📈 BI & Analytics", f"🚫 Blacklist ({len(st.session_state['df_black'])})", f"🛡️ Seguras ({count_safe})", f"⚠️ Obs ({count_obs})", "💾 Big Data", "💬 Chat IA", "📉 Trading"])
        
        with abas[0]: 
            if radar: st.dataframe(pd.DataFrame(radar)[['Liga', 'Jogo', 'Tempo', 'Status']].astype(str), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        
        with abas[1]: 
            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos futuros hoje.")
        
        with abas[2]:
            st.markdown("### 💰 Evolução Financeira")
            c_fin1, c_fin2 = st.columns(2)
            stake_padrao = c_fin1.number_input("Valor da Aposta (Stake):", value=st.session_state.get('stake_padrao', 10.0), step=5.0)
            banca_inicial = c_fin2.number_input("Banca Inicial:", value=st.session_state.get('banca_inicial', 100.0), step=50.0)
            st.session_state['stake_padrao'] = stake_padrao; st.session_state['banca_inicial'] = banca_inicial
            modo_simulacao = st.radio("Cenário de Entrada:", ["Todos os sinais", "Apenas 1 sinal por jogo", "Até 2 sinais por jogo"], horizontal=True)
            filtrar_ia = st.checkbox("🤖 Somente Sinais APROVADOS pela IA")
            df_fin = st.session_state.get('historico_full', pd.DataFrame())
            if not df_fin.empty:
                df_fin = df_fin.copy()
                df_fin['Odd_Calc'] = df_fin.apply(lambda row: obter_odd_final_para_calculo(row['Odd'], row['Estrategia']), axis=1)
                df_fin = df_fin[df_fin['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
                df_fin = df_fin.sort_values(by=['FID', 'Hora'], ascending=[True, True])
                if filtrar_ia and 'Opiniao_IA' in df_fin.columns: df_fin = df_fin[df_fin['Opiniao_IA'] == 'Aprovado']
                if modo_simulacao == "Apenas 1 sinal por jogo": df_fin = df_fin.groupby('FID').head(1)
                elif modo_simulacao == "Até 2 sinais por jogo": df_fin = df_fin.groupby('FID').head(2)
                if not df_fin.empty:
                    lucros = []; saldo_atual = banca_inicial; historico_saldo = [banca_inicial]; qtd_greens = 0; qtd_reds = 0
                    for idx, row in df_fin.iterrows():
                        res = row['Resultado']; odd = row['Odd_Calc']
                        if 'GREEN' in res: lucro = (stake_padrao * odd) - stake_padrao; qtd_greens += 1
                        else: lucro = -stake_padrao; qtd_reds += 1
                        saldo_atual += lucro; lucros.append(lucro); historico_saldo.append(saldo_atual)
                    df_fin['Lucro'] = lucros; total_lucro = sum(lucros)
                    roi = (total_lucro / (len(df_fin) * stake_padrao)) * 100
                    st.session_state['last_fin_stats'] = {'cenario': modo_simulacao, 'lucro': total_lucro, 'roi': roi, 'entradas': len(df_fin)}
                    m1, m2, m3, m4 = st.columns(4)
                    cor_delta = "normal" if total_lucro >= 0 else "inverse"
                    m1.metric("Banca Atual", f"R$ {saldo_atual:.2f}")
                    m2.metric("Lucro Líquido", f"R$ {total_lucro:.2f}", delta=f"{roi:.1f}%", delta_color=cor_delta)
                    m3.metric("Entradas", len(df_fin))
                    m4.metric("Winrate", f"{(qtd_greens/len(df_fin)*100):.1f}%")
                    fig_fin = px.line(y=historico_saldo, x=range(len(historico_saldo)), title="Crescimento da Banca (Realista)")
                    fig_fin.update_layout(xaxis_title="Entradas", yaxis_title="Saldo (R$)", template="plotly_dark")
                    fig_fin.add_hline(y=banca_inicial, line_dash="dot", annotation_text="Início", line_color="gray")
                    st.plotly_chart(fig_fin, use_container_width=True)

            # [MELHORIA] Evolução da Banca (Tempo Real)
            if 'historico_banca' in st.session_state and st.session_state['historico_banca']:
                st.markdown('### 📈 Evolução da Banca (Tempo Real)')
                df_banca = pd.DataFrame(st.session_state['historico_banca'])
                try:
                    fig_banca = px.line(df_banca, y='saldo', title='Crescimento da Banca (Gestão Dinâmica)', labels={'index':'Apostas', 'saldo':'Saldo (R$)'})
                    fig_banca.update_layout(template='plotly_dark')
                    banca_ini_ref = float(st.session_state.get('banca_inicial', df_banca['saldo'].iloc[0]))
                    fig_banca.add_hline(y=banca_ini_ref, line_dash='dot', annotation_text='Início', line_color='gray')
                    st.plotly_chart(fig_banca, use_container_width=True)
                except:
                    pass
                else: st.info("Aguardando fechamento de sinais.")
            else: st.info("Sem dados históricos.")

        with abas[3]: 
            if not hist_hj.empty: 
                df_show = hist_hj.copy()
                if 'Jogo' in df_show.columns and 'Placar_Sinal' in df_show.columns: df_show['Jogo'] = df_show['Jogo'] + " (" + df_show['Placar_Sinal'].astype(str) + ")"
                colunas_esconder = ['FID', 'HomeID', 'AwayID', 'Data_Str', 'Data_DT', 'Odd_Atualizada', 'Placar_Sinal', 'Is_Green', 'Is_Red']
                cols_view = [c for c in df_show.columns if c not in colunas_esconder]
                st.dataframe(df_show[cols_view], use_container_width=True, hide_index=True)
            else: st.caption("Vazio.")

        with abas[4]: 
            st.markdown("### 📊 Inteligência de Mercado (V4 - Drill Down)")
            df_bi = st.session_state.get('historico_full', pd.DataFrame())
            if df_bi.empty: st.warning("Sem dados históricos.")
            else:
                try:
                    df_bi = df_bi.copy()
                    df_bi['Data_Str'] = df_bi['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
                    df_bi['Data_DT'] = pd.to_datetime(df_bi['Data_Str'], errors='coerce')
                    df_bi = df_bi.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
                    hoje = pd.to_datetime(get_time_br().date())
                    d_hoje = df_bi[df_bi['Data_DT'] == hoje]
                    d_7d = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=7))]
                    d_30d = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=30))]
                    
                    filtro = st.selectbox("📅 Período", ["Tudo", "Hoje", "7 Dias", "30 Dias"], key="bi_select")
                    if filtro == "Hoje": df_show = d_hoje
                    elif filtro == "7 Dias": df_show = d_7d
                    elif filtro == "30 Dias": df_show = d_30d
                    else: df_show = df_bi 
                    
                    if not df_show.empty:
                        if 'Probabilidade' in df_show.columns:
                            prob_min = st.slider("🎯 Filtrar Probabilidade IA (%)", 0, 100, 0)
                            if prob_min > 0:
                                def limpar_prob(x):
                                    try: return int(str(x).replace('%', ''))
                                    except: return 0
                                df_show = df_show[df_show['Probabilidade'].apply(limpar_prob) >= prob_min]

                        gr = df_show['Resultado'].str.contains('GREEN').sum(); rd = df_show['Resultado'].str.contains('RED').sum(); tt = len(df_show); ww = (gr/tt*100) if tt>0 else 0
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Sinais", tt); m2.metric("Greens", gr); m3.metric("Reds", rd); m4.metric("Assertividade", f"{ww:.1f}%")
                        st.divider()
                        
                        st.markdown("### 🏆 Melhores e Piores Ligas (Com Detalhe de Estratégia)")
                        df_finished = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        if not df_finished.empty:
                            stats_ligas = df_finished.groupby('Liga')['Resultado'].apply(lambda x: pd.Series({'Winrate': (x.str.contains('GREEN').sum() / len(x) * 100), 'Total': len(x), 'Reds': x.str.contains('RED').sum(), 'Greens': x.str.contains('GREEN').sum()})).unstack()
                            stats_ligas = stats_ligas[stats_ligas['Total'] >= 2]
                            
                            def get_top_strat(liga):
                                d_l = df_finished[df_finished['Liga'] == liga]
                                if d_l.empty: return "-"
                                s = d_l.groupby('Estrategia')['Resultado'].apply(lambda x: x.str.contains('GREEN').sum()/len(x)).sort_values(ascending=False)
                                return f"{s.index[0]} ({s.iloc[0]*100:.0f}%)"
                            
                            def get_worst_strat(liga):
                                d_l = df_finished[(df_finished['Liga'] == liga) & (df_finished['Resultado'].str.contains('RED'))]
                                if d_l.empty: return "Nenhuma"
                                s = d_l['Estrategia'].value_counts()
                                return f"{s.index[0]} ({s.iloc[0]} Reds)"

                            col_top, col_worst = st.columns(2)
                            with col_top:
                                st.caption("🌟 Top Ligas (Mais Lucrativas)")
                                top_ligas = stats_ligas.sort_values(by=['Winrate', 'Total'], ascending=[False, False]).head(10)
                                top_ligas['Top Estratégia'] = top_ligas.index.map(get_top_strat)
                                st.dataframe(top_ligas[['Winrate', 'Total', 'Top Estratégia']].style.format({'Winrate': '{:.0f}%', 'Total': '{:.0f}'}), use_container_width=True)
                            with col_worst:
                                st.caption("💀 Ligas Críticas (Mais Reds)")
                                worst_ligas = stats_ligas.sort_values(by=['Reds'], ascending=False).head(10)
                                worst_ligas['Pior Estratégia'] = worst_ligas.index.map(get_worst_strat)
                                st.dataframe(worst_ligas[['Reds', 'Total', 'Pior Estratégia']].style.format({'Reds': '{:.0f}', 'Total': '{:.0f}'}), use_container_width=True)
                        st.divider()
                        st.markdown("### 🧠 Auditoria da IA")
                        if 'Opiniao_IA' in df_show.columns:
                            df_audit = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
                            df_audit = df_audit[df_audit['Opiniao_IA'].isin(['Aprovado', 'Arriscado', 'Sniper'])]
                            if not df_audit.empty:
                                pivot = pd.crosstab(df_audit['Opiniao_IA'], df_audit['Resultado'], margins=False)
                                if '✅ GREEN' not in pivot.columns: pivot['✅ GREEN'] = 0
                                if '❌ RED' not in pivot.columns: pivot['❌ RED'] = 0
                                pivot['Total'] = pivot['✅ GREEN'] + pivot['❌ RED']
                                pivot['Winrate %'] = (pivot['✅ GREEN'] / pivot['Total'] * 100)
                                st.dataframe(pivot.style.format({'Winrate %': '{:.2f}%'}).highlight_max(axis=0, color='#1F4025'), use_container_width=True)
                        st.markdown("### 📈 Estratégias")
                        st_s = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        if not st_s.empty:
                            resumo_strat = st_s.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)
                            if '✅ GREEN' in resumo_strat.columns:
                                resumo_strat['Winrate'] = (resumo_strat['✅ GREEN'] / (resumo_strat.get('✅ GREEN',0) + resumo_strat.get('❌ RED',0)) * 100)
                                st.dataframe(resumo_strat.sort_values('Winrate', ascending=False).style.format({'Winrate': '{:.2f}%'}), use_container_width=True)
                except Exception as e: st.error(f"Erro BI: {e}")

        with abas[5]: st.dataframe(st.session_state['df_black'][['País', 'Liga', 'Motivo']], use_container_width=True, hide_index=True)
        
        with abas[6]: 
            df_safe_show = st.session_state.get('df_safe', pd.DataFrame()).copy()
            if not df_safe_show.empty:
                df_safe_show['Status Risco'] = df_safe_show['Strikes'].apply(lambda x: "🟢 100% Estável" if str(x)=='0' else f"⚠️ Atenção ({x}/10)")
                st.dataframe(df_safe_show[['País', 'Liga', 'Motivo', 'Status Risco']], use_container_width=True, hide_index=True)
            else: st.info("Nenhuma liga segura ainda.")

        with abas[7]: 
            df_vip_show = st.session_state.get('df_vip', pd.DataFrame()).copy()
            if not df_vip_show.empty: 
                df_vip_show['Strikes'] = df_vip_show['Strikes'].apply(formatar_inteiro_visual)
                st.dataframe(df_vip_show[['País', 'Liga', 'Data_Erro', 'Strikes']], use_container_width=True, hide_index=True)
            else: st.info("Nenhuma observação no momento.")

        with abas[8]: 
            st.markdown(f"### 💾 Banco de Dados de Partidas (Firebase)")
            st.caption("A IA usa esses dados para criar novas estratégias. Os dados são salvos na nuvem.")
            if db_firestore:
                col_fb1, col_fb2 = st.columns([1, 3])
                if col_fb1.button("🔄 Carregar/Atualizar Tabela"):
                    try:
                        with st.spinner("Baixando dados do Firebase..."):
                            try:
                                count_query = db_firestore.collection("BigData_Futebol").count(); res_count = count_query.get()
                                st.session_state['total_bigdata_count'] = res_count[0][0].value
                            except: pass
                            docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(50).stream()
                            data = [d.to_dict() for d in docs]
                            st.session_state['cache_firebase_view'] = data 
                            st.toast(f"Dados atualizados!")
                    except Exception as e: st.error(f"Erro ao ler Firebase: {e}")
                
                if st.session_state.get('total_bigdata_count', 0) > 0: st.metric("Total de Jogos Armazenados", st.session_state['total_bigdata_count'])

                if 'cache_firebase_view' in st.session_state and st.session_state['cache_firebase_view']:
                    st.success(f"📂 Visualizando {len(st.session_state['cache_firebase_view'])} registros (Cache Local)")
                    st.dataframe(pd.DataFrame(st.session_state['cache_firebase_view']), use_container_width=True)
                else: st.info("ℹ️ Clique no botão acima para visualizar os dados salvos.")
            else: st.warning("⚠️ Firebase não conectado.")

        with abas[9]: 
            st.markdown("### 💬 Chat Intelligence (Data Driven)")
            st.caption("Agora a IA tem acesso aos cálculos reais do seu Big Data.")
            
            if "messages" not in st.session_state:
                st.session_state["messages"] = [{"role": "assistant", "content": "Olá Tiago! Já processei seus dados. Pergunte sobre Escanteios, Gols ou Estratégias que eu calculo a probabilidade baseada no histórico."}]
            if len(st.session_state["messages"]) > 6:
                st.session_state["messages"] = st.session_state["messages"][-6:]

            for msg in st.session_state.messages: 
                st.chat_message(msg["role"]).write(msg["content"])
            
            if prompt := st.chat_input("Ex: Qual a melhor estratégia de Escanteios com base nos dados?"):
                if not IA_ATIVADA: st.error("IA Desconectada. Verifique a API Key.")
                else:
                    st.session_state.messages.append({"role": "user", "content": prompt})
                    st.chat_message("user").write(prompt)

                    # --- 1. PREPARAÇÃO DOS DADOS (O CÉREBRO MATEMÁTICO) ---
                    media_cantos = 0.0 
                    
                    txt_bigdata_resumo = "BIG DATA: Sem dados carregados."
                    dados_bd = st.session_state.get('cache_firebase_view', [])
                    total_bd = st.session_state.get('total_bigdata_count', 0)
                    
                    if dados_bd:
                        try:
                            # Converte JSON para DataFrame para fazer contas reais
                            df_calc = pd.DataFrame(dados_bd)
                            
                            # Extração de colunas aninhadas (Estatísticas)
                            def extrair_stats(row, chave):
                                return float(row.get('estatisticas', {}).get(chave, 0))
                            
                            df_calc['Cantos_Total'] = df_calc.apply(lambda x: extrair_stats(x, 'escanteios_total'), axis=1)
                            df_calc['Chutes_Total'] = df_calc.apply(lambda x: extrair_stats(x, 'chutes_total'), axis=1)
                            df_calc['Gols_Total'] = df_calc['placar_final'].apply(lambda x: sum(map(int, x.split('x'))) if 'x' in str(x) else 0)
                            
                            # CÁLCULOS REAIS
                            media_cantos = df_calc['Cantos_Total'].mean()
                            media_chutes = df_calc['Chutes_Total'].mean()
                            max_cantos = df_calc['Cantos_Total'].max()
                            
                            # Correlação Simples (Chutes geram Escanteios?)
                            correlacao = df_calc['Chutes_Total'].corr(df_calc['Cantos_Total'])
                            txt_corr = "Alta" if correlacao > 0.7 else "Média" if correlacao > 0.4 else "Baixa"

                            # Top Ligas para Cantos
                            top_ligas_cantos = df_calc.groupby('liga')['Cantos_Total'].mean().sort_values(ascending=False).head(3)
                            txt_top_ligas = ", ".join([f"{l} ({m:.1f})" for l, m in top_ligas_cantos.items()])

                            txt_bigdata_resumo = f"""
                            📊 DADOS REAIS PROCESSADOS ({len(df_calc)} jogos recentes):
                            - Média Global de Escanteios: {media_cantos:.2f} por jogo.
                            - Média de Chutes: {media_chutes:.2f} por jogo.
                            - Máximo Registrado: {max_cantos} escanteios.
                            - Correlação Chutes/Cantos: {correlacao:.2f} ({txt_corr}).
                            - Ligas com Mais Cantos: {txt_top_ligas}.
                            """
                        except Exception as e:
                            txt_bigdata_resumo = f"Erro ao calcular dados: {e}"

                    # --- 2. CONTEXTO DO PROMPT (A ORDEM PARA A IA) ---
                    # [MELHORIA] Snapshot de jogos AO VIVO para o Chat IA (se o robô estiver monitorando)
                    resumo_live = ''
                    try:
                        live_list = []
                        if 'jogos_live' in locals() and jogos_live:
                            for lj in jogos_live[:50]:
                                fid_l = lj['fixture']['id']
                                st_l = st.session_state.get(f"st_{fid_l}", [])
                                if not st_l: continue
                                try:
                                    s1 = st_l[0]['statistics']; s2 = st_l[1]['statistics']
                                    def gv(l, t):
                                        return next((x['value'] for x in l if x['type']==t), 0) or 0
                                    sog = gv(s1,'Shots on Goal') + gv(s2,'Shots on Goal')
                                    sh  = gv(s1,'Total Shots') + gv(s2,'Total Shots')
                                    tm  = lj['fixture']['status'].get('elapsed') or 0
                                    plac = f"{lj['goals']['home']}x{lj['goals']['away']}"
                                    live_list.append((sog, sh, tm, plac, f"{lj['teams']['home']['name']} x {lj['teams']['away']['name']}"))
                                except:
                                    pass
                        live_list.sort(key=lambda x: (x[0], x[1]), reverse=True)
                        if live_list:
                            top = live_list[:5]
                            resumo_live = 'JOGOS AO VIVO (Top Pressão): ' + ' | '.join([f"{nm} ({pl}, {tm}min, SOG:{sog}, SH:{sh})" for sog, sh, tm, pl, nm in top])
                    except:
                        resumo_live = ''
                    contexto_chat = f"""
                            ATUE COMO: Cientista de Dados Sênior do 'Neves Analytics'.
                    
                            SUA MISSÃO: 
                            Não dê aulas teóricas. Use os DADOS REAIS abaixo para responder.
                            Se o usuário pedir estratégia, crie uma baseada nos NÚMEROS apresentados.
                    
                            {txt_bigdata_resumo}
                            {resumo_live}

                    
                            PERGUNTA DO TIAGO: "{prompt}"
                    
                            FORMATO DA RESPOSTA:
                            1. 🔢 **Os Números:** (Cite a média e a correlação calculada acima).
                            2. 🎯 **O Veredicto:** (Vale a pena operar? Sim/Não).
                            3. 🛠️ **Estratégia Sugerida:** (Ex: "Como a média é {media_cantos:.1f}, busque a linha de Over X...").
                            Seja objetivo e numérico.
                            """

                    try:
                        with st.spinner("🤖 Calculando estatísticas e gerando resposta..."):
                            response = model_ia.generate_content(contexto_chat)
                            st.session_state['gemini_usage']['used'] += 1
                            msg_ia = response.text
                
                        st.session_state.messages.append({"role": "assistant", "content": msg_ia})
                        st.chat_message("assistant").write(msg_ia)
                        if len(st.session_state["messages"]) > 6:
                            time.sleep(0.5); st.rerun()
                    
                    except Exception as e: st.error(f"Erro na IA: {e}")

        with abas[10]:
            st.markdown("### 📈 Trading Pré-Live (Drop Odds)")
            st.caption("Apostas baseadas em variação de preço antes do jogo começar (Cashout Bet365).")
            
            c_trade1, c_trade2 = st.columns(2)
            if c_trade1.button("🔍 Escanear Mercado Agora (Manual)"):
                if IA_ATIVADA:
                    with st.spinner("Comparando Bet365 vs Pinnacle... Isso pode demorar."):
                        drops = scanner_drop_odds_pre_live(st.session_state['API_KEY'])
                        if drops:
                            st.success(f"Encontradas {len(drops)} oportunidades!")
                            for d in drops:
                                st.markdown(f"""
                                ---
                                ⚽ **{d['jogo']}** ({d['liga']}) | ⏰ {d['hora']}
                                📉 **Drop:** {d['valor']:.1f}%
                                • Bet365: **@{d['odd_b365']}**
                                • Pinnacle: **@{d['odd_pinnacle']}**
                                👉 *Entrar no {d['lado']} + Banker*
                                """)
                        else:
                            st.warning("Nenhum desajuste de odd encontrado nas Ligas Top agora.")
                else: st.error("IA/API necessária.")

        for i in range(INTERVALO, 0, -1):
            st.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()
else:
    with placeholder_root.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera. Configure na lateral.")