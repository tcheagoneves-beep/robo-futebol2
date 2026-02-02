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
# 2. INICIALIZAÇÃO DE VARIÁVEIS
# ==============================================================================
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

# VARIÁVEIS DE CONTROLE DE ESTRATÉGIAS
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

COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID', 'Odd', 'Odd_Atualizada', 'Opiniao_IA', 'Probabilidade']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo', 'Strikes', 'Jogos_Erro']
COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes', 'Jogos_Erro']
COLS_BLACK = ['id', 'País', 'Liga', 'Motivo']
LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]
DB_CACHE_TIME = 60
STATIC_CACHE_TIME = 600

MAPA_LOGICA_ESTRATEGIAS = {
    "🟣 Porteira Aberta": "Over Gols",
    "⚡ Gol Relâmpago": "Over HT",
    "💰 Janela de Ouro": "Over Limite",
    "🟢 Blitz Casa": "Over Gols",
    "🟢 Blitz Visitante": "Over Gols",
    "🔥 Massacre": "Over HT",
    "⚔️ Choque Líderes": "Over HT",
    "🥊 Briga de Rua": "Over HT",
    "❄️ Jogo Morno": "Under HT/FT",
    "💎 GOLDEN BET": "Over Limite",
    "🏹 Tiroteio Elite": "Over Gols",
    "⚡ Contra-Ataque Letal": "Back Zebra",
    "💎 Sniper Final": "Over Limite",
    "🦁 Back Favorito (Nettuno)": "Back Vencedor",
    "🔫 Lay Goleada": "Over Limite",
    "👴 Estratégia do Vovô": "Back Favorito (Segurança)",
    "🟨 Sniper de Cartões": "Over Cartões",
    "🧤 Muralha (Defesas)": "Over Defesas",
    "Alavancagem": "Bet Builder",
    "Drop Odds Cashout": "Trading"
}

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
    "🔫 Lay Goleada": {"min": 1.60, "max": 2.20},
    "👴 Estratégia do Vovô": {"min": 1.05, "max": 1.25},
    "🟨 Sniper de Cartões": {"min": 1.50, "max": 1.90},
    "🧤 Muralha (Defesas)": {"min": 1.60, "max": 2.10}
}
# ==============================================================================
# 2. FUNÇÕES AUXILIARES E FERRAMENTAS
# ==============================================================================

def get_time_br(): return datetime.now(pytz.timezone('America/Sao_Paulo'))
def clean_fid(x): 
    try: return str(int(float(x))) 
    except: return '0'
def normalizar_id(val):
    try: s_val = str(val).strip(); return "" if not s_val or s_val.lower()=='nan' else str(int(float(s_val)))
    except: return str(val).strip()
def formatar_inteiro_visual(val):
    try: return "0" if str(val)=='nan' or str(val)=='' else str(int(float(str(val))))
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

# --- TELEGRAM ---
def testar_conexao_telegram(token):
    if not token: return False, "Token Vazio"
    try:
        res = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=5)
        return (True, res.json()['result']['first_name']) if res.status_code == 200 else (False, f"Erro {res.status_code}")
    except: return False, "Sem Conexão"

def _worker_telegram(token, chat_id, msg):
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except: pass

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        t = threading.Thread(target=_worker_telegram, args=(token, cid, msg))
        t.daemon = True; t.start()

def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    df_raw = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

# --- TRADING (DROP ODDS) ---
def buscar_odds_comparativas(api_key, fixture_id):
    url = "https://v3.football.api-sports.io/odds"
    try:
        r365 = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fixture_id, "bookmaker": "8"}).json()
        rpin = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fixture_id, "bookmaker": "4"}).json()
        if r365.get('response'):
            vencedor = next((m for m in r365['response'][0]['bookmakers'][0]['bets'] if m['id'] == 1), None) 
            if vencedor:
                v_casa = float(next((v['odd'] for v in vencedor['values'] if v['value'] == 'Home'), 0))
                v_fora = float(next((v['odd'] for v in vencedor['values'] if v['value'] == 'Away'), 0))
                if rpin.get('response'):
                    vencedor_pin = next((m for m in rpin['response'][0]['bookmakers'][0]['bets'] if m['id'] == 1), None)
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
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        res = requests.get(url, headers={"x-apisports-key": api_key}, params={"date": agora.strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}).json()
        jogos = res.get('response', [])
        LIGAS_PERMITIDAS = [39, 140, 78, 135, 61, 2, 3] 
        oportunidades = []
        for j in jogos:
            if j['league']['id'] not in LIGAS_PERMITIDAS: continue
            try: hora_jogo = datetime.fromisoformat(j['fixture']['date'].replace('Z', '+00:00'))
            except: hora_jogo = datetime.strptime(j['fixture']['date'][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.utc)
            if j['fixture']['status']['short'] != 'NS': continue
            if not (3 <= (hora_jogo - datetime.now(pytz.utc)).total_seconds() / 3600 <= 8): continue 
            odd_b365, odd_pin, lado = buscar_odds_comparativas(api_key, j['fixture']['id'])
            if odd_b365 > 0 and lado:
                oportunidades.append({"fid": j['fixture']['id'], "jogo": f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", "liga": j['league']['name'], "hora": j['fixture']['date'][11:16], "lado": lado, "odd_b365": odd_b365, "odd_pinnacle": odd_pin, "valor": ((odd_b365 - odd_pin) / odd_pin) * 100})
        return oportunidades
    except: return []

# --- BANCO DE DADOS E SHEETS ---
def carregar_aba(nome_aba, colunas_esperadas):
    chave = {'Historico': 'historico_full', 'Seguras': 'df_safe', 'Obs': 'df_vip', 'Blacklist': 'df_black'}.get(nome_aba, '')
    try:
        df = conn.read(worksheet=nome_aba, ttl=10)
        if df.empty and chave in st.session_state: return st.session_state[chave]
        if not df.empty:
            for col in colunas_esperadas:
                if col not in df.columns: df[col] = "0" if col == 'Probabilidade' else ("1.20" if col == 'Odd' else "")
            return df.fillna("").astype(str)
        return pd.DataFrame(columns=colunas_esperadas)
    except: return st.session_state.get(chave, pd.DataFrame(columns=colunas_esperadas))

def salvar_aba(nome_aba, df_para_salvar):
    if nome_aba in ["Historico", "Seguras", "Obs"] and df_para_salvar.empty: return False
    if st.session_state.get('BLOQUEAR_SALVAMENTO', False):
        st.session_state['precisa_salvar'] = True; return False
    try:
        conn.update(worksheet=nome_aba, data=df_para_salvar)
        if nome_aba == "Historico": st.session_state['precisa_salvar'] = False
        return True
    except: st.session_state['precisa_salvar'] = True; return False

def salvar_blacklist(id_liga, pais, nome_liga, motivo_ban):
    df = st.session_state['df_black']; id_norm = normalizar_id(id_liga)
    if id_norm in df['id'].values: df.at[df[df['id'] == id_norm].index[0], 'Motivo'] = str(motivo_ban)
    else: df = pd.concat([df, pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': str(motivo_ban)}])], ignore_index=True)
    st.session_state['df_black'] = df; salvar_aba("Blacklist", df); sanitizar_conflitos()

def sanitizar_conflitos():
    df_b = st.session_state.get('df_black', pd.DataFrame()); df_v = st.session_state.get('df_vip', pd.DataFrame()); df_s = st.session_state.get('df_safe', pd.DataFrame())
    if df_b.empty: return
    for _, row in df_b.iterrows():
        id_b = normalizar_id(row['id']); motivo = str(row['Motivo'])
        mask_v = df_v['id'].apply(normalizar_id) == id_b
        if mask_v.any(): df_b.at[row.name, 'Motivo'] = f"Banida ({formatar_inteiro_visual(df_v.loc[mask_v, 'Strikes'].values[0])} Jogos Sem Dados)"; df_v = df_v[~mask_v]; salvar_aba("Obs", df_v)
        mask_s = df_s['id'].apply(normalizar_id) == id_b
        if mask_s.any(): df_s = df_s[~mask_s]; salvar_aba("Seguras", df_s)
    st.session_state['df_vip'] = df_v; st.session_state['df_safe'] = df_s

def salvar_safe_league_basic(id_liga, pais, nome_liga, tem_tabela=False):
    id_norm = normalizar_id(id_liga); df = st.session_state['df_safe']; txt = "Validada (Chutes + Tabela)" if tem_tabela else "Validada (Chutes)"
    if id_norm not in df['id'].values:
        final = pd.concat([df, pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': txt, 'Strikes': '0', 'Jogos_Erro': ''}])], ignore_index=True)
        if salvar_aba("Seguras", final): st.session_state['df_safe'] = final; sanitizar_conflitos()
    else:
        idx = df[df['id'] == id_norm].index[0]
        if df.at[idx, 'Motivo'] != txt: df.at[idx, 'Motivo'] = txt; salvar_aba("Seguras", df)

def resetar_erros(id_liga):
    id_norm = normalizar_id(id_liga); df = st.session_state.get('df_safe', pd.DataFrame())
    if not df.empty and id_norm in df['id'].values:
        idx = df[df['id'] == id_norm].index[0]
        if str(df.at[idx, 'Strikes']) != '0': df.at[idx, 'Strikes'] = '0'; df.at[idx, 'Jogos_Erro'] = ''; salvar_aba("Seguras", df)

def gerenciar_erros(id_liga, pais, nome_liga, fid_jogo):
    id_norm = normalizar_id(id_liga); fid_str = str(fid_jogo); df_s = st.session_state.get('df_safe', pd.DataFrame())
    if not df_s.empty and id_norm in df_s['id'].values:
        idx = df_s[df_s['id'] == id_norm].index[0]
        errs = str(df_s.at[idx, 'Jogos_Erro']).split(',') if str(df_s.at[idx, 'Jogos_Erro']).strip() else []
        if fid_str in errs: return
        errs.append(fid_str); strikes = len(errs)
        if strikes >= 10:
            df_s = df_s.drop(idx); salvar_aba("Seguras", df_s); st.session_state['df_safe'] = df_s
            df_v = st.session_state.get('df_vip', pd.DataFrame())
            final_v = pd.concat([df_v, pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Data_Erro': get_time_br().strftime('%Y-%m-%d'), 'Strikes': '1', 'Jogos_Erro': fid_str}])], ignore_index=True)
            salvar_aba("Obs", final_v); st.session_state['df_vip'] = final_v
        else: df_s.at[idx, 'Strikes'] = str(strikes); df_s.at[idx, 'Jogos_Erro'] = ",".join(errs); salvar_aba("Seguras", df_s)
        return
    df_v = st.session_state.get('df_vip', pd.DataFrame()); strikes = 0; errs = []
    if not df_v.empty and id_norm in df_v['id'].values:
        row = df_v[df_v['id'] == id_norm].iloc[0]; errs = str(row.get('Jogos_Erro', '')).strip().split(',') if str(row.get('Jogos_Erro', '')).strip() else []
    if fid_str in errs: return
    errs.append(fid_str); strikes = len(errs)
    if strikes >= 10: salvar_blacklist(id_liga, pais, nome_liga, f"Banida ({strikes} Jogos Sem Dados)")
    elif id_norm in df_v['id'].values:
        idx = df_v[df_v['id'] == id_norm].index[0]; df_v.at[idx, 'Strikes'] = str(strikes); df_v.at[idx, 'Jogos_Erro'] = ",".join(errs); df_v.at[idx, 'Data_Erro'] = get_time_br().strftime('%Y-%m-%d')
        salvar_aba("Obs", df_v); st.session_state['df_vip'] = df_v
    else:
        final = pd.concat([df_v, pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Data_Erro': get_time_br().strftime('%Y-%m-%d'), 'Strikes': '1', 'Jogos_Erro': fid_str}])], ignore_index=True)
        salvar_aba("Obs", final); st.session_state['df_vip'] = final

def carregar_tudo(force=False):
    now = time.time()
    if force or (now - st.session_state['last_static_update']) > STATIC_CACHE_TIME or 'df_black' not in st.session_state:
        st.session_state['df_black'] = carregar_aba("Blacklist", COLS_BLACK); st.session_state['df_safe'] = carregar_aba("Seguras", COLS_SAFE); st.session_state['df_vip'] = carregar_aba("Obs", COLS_OBS)
        for df in [st.session_state['df_black'], st.session_state['df_safe'], st.session_state['df_vip']]:
            if not df.empty: df['id'] = df['id'].apply(normalizar_id)
        sanitizar_conflitos(); st.session_state['last_static_update'] = now
    if 'historico_full' not in st.session_state or force:
        df = carregar_aba("Historico", COLS_HIST)
        if not df.empty and 'Data' in df.columns:
            df['FID'] = df['FID'].apply(clean_fid)
            st.session_state['historico_full'] = df
            hoje = get_time_br().strftime('%Y-%m-%d')
            st.session_state['historico_sinais'] = df[df['Data'] == hoje].to_dict('records')[::-1]
            st.session_state['alertas_enviados'] = {gerar_chave_universal(i['FID'], i['Estrategia'], "SINAL") for i in st.session_state['historico_sinais']}
        else: st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST); st.session_state['historico_sinais'] = []
    st.session_state['last_db_update'] = now

def adicionar_historico(item):
    if 'historico_full' not in st.session_state: st.session_state['historico_full'] = carregar_aba("Historico", COLS_HIST)
    st.session_state['historico_full'] = pd.concat([pd.DataFrame([item]), st.session_state['historico_full']], ignore_index=True)
    st.session_state['historico_sinais'].insert(0, item); st.session_state['precisa_salvar'] = True; return True

def atualizar_historico_ram(updates):
    if 'historico_full' not in st.session_state or not updates: return
    df = st.session_state['historico_full']; mapa = {f"{r['FID']}_{r['Estrategia']}": r for _, r in pd.DataFrame(updates).iterrows()}
    def att(row):
        k = f"{row['FID']}_{row['Estrategia']}"
        if k in mapa:
            if str(row['Resultado']) != str(mapa[k]['Resultado']): st.session_state['precisa_salvar'] = True
            return mapa[k]
        return row
    st.session_state['historico_full'] = df.apply(att, axis=1)

@st.cache_data(ttl=120) 
def buscar_agenda_cached(api_key, date_str):
    try: return requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"date": date_str, "timezone": "America/Sao_Paulo"}).json().get('response', [])
    except: return []

# --- IA GENERATIVA (HELPERS) ---
def consultar_bigdata_cenario_completo(home_id, away_id):
    if not db_firestore: return "Big Data Offline"
    try:
        dh = db_firestore.collection("BigData_Futebol").where("home_id", "==", str(home_id)).limit(20).stream()
        da = db_firestore.collection("BigData_Futebol").where("away_id", "==", str(away_id)).limit(20).stream()
        def sg(d, k): return float(d.get('estatisticas', {}).get(k, 0))
        h = {'q':0,'g':0,'c':0,'s':0}; a = {'q':0,'g':0,'c':0,'s':0}
        for d in dh: dd=d.to_dict(); h['q']+=1; h['g']+=int(dd.get('placar_final','0x0').split('x')[0]); h['c']+=sg(dd,'escanteios_casa'); h['s']+=sg(dd,'chutes_gol')
        for d in da: dd=d.to_dict(); a['q']+=1; a['g']+=int(dd.get('placar_final','0x0').split('x')[1]); a['c']+=sg(dd,'escanteios_fora'); a['s']+=sg(dd,'chutes_gol')
        if h['q']==0 and a['q']==0: return "Sem dados."
        th = f"MANDANTE ({h['q']}j): Gols {h['g']/h['q']:.1f} | Cantos {h['c']/h['q']:.1f}" if h['q'] else "N/D"
        ta = f"VISITANTE ({a['q']}j): Gols {a['g']/a['q']:.1f} | Cantos {a['c']/a['q']:.1f}" if a['q'] else "N/D"
        return f"{th} || {ta}"
    except: return "Erro BD."

def gerar_multipla_matinal_ia(api_key):
    if not IA_ATIVADA: return None, []
    try:
        jogos = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"date": get_time_br().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}).json().get('response', [])
        cands = [j for j in jogos if j['league']['id'] in [39,140,78,135,61,71,72,2,3]][:10]
        if len(cands) < 2: return None, []
        txt_jogos = "\n".join([f"- ID {j['fixture']['id']}: {j['teams']['home']['name']} x {j['teams']['away']['name']}" for j in cands])
        prompt = f"Crie uma Múltipla de Segurança (Over 0.5 Gols) com 3 jogos de: {txt_jogos}. JSON: {{'jogos': [{{'fid': 123, 'jogo': 'A x B'}}], 'probabilidade_combinada': '90'}}."
        res = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        return json.loads(res.text), {str(j['fixture']['id']): f"{j['teams']['home']['name']} x {j['teams']['away']['name']}" for j in cands}
    except: return None, []

def gerar_analise_mercados_alternativos_ia(api_key):
    if not IA_ATIVADA: return []
    try:
        jogos = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"date": get_time_br().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}).json().get('response', [])
        cands = [j for j in jogos if j['league']['id'] in [39,140,78,135,61,71,72,2,3] and j['fixture'].get('referee')][:5]
        if not cands: return []
        txt = "\n".join([f"- {j['teams']['home']['name']} x {j['teams']['away']['name']} (Ref: {j['fixture']['referee']}, ID: {j['fixture']['id']})" for j in cands])
        prompt = f"Analise mercados de Cartões e Goleiros: {txt}. Retorne JSON {{'sinais': [{{'fid': '123', 'titulo': 'SNIPER CARTÕES', 'indicacao': 'Over 4.5', 'jogo': 'A x B'}}]}}"
        return json.loads(model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json")).text).get('sinais', [])
    except: return []

def gerar_bet_builder_alavancagem(api_key):
    if not IA_ATIVADA: return []
    try:
        jogos = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"date": get_time_br().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}).json().get('response', [])
        cands = [j for j in jogos if j['league']['id'] in [39,140,78,135,61,71,72,2,3]][:3]
        if not cands: return []
        res = []
        for j in cands:
            bd = consultar_bigdata_cenario_completo(j['teams']['home']['id'], j['teams']['away']['id'])
            prompt = f"Crie Bet Builder para {j['teams']['home']['name']} x {j['teams']['away']['name']}. Dados: {bd}. JSON: {{'titulo': 'ALAVANCAGEM', 'selecoes': ['Vencedor', 'Gols'], 'analise_ia': 'Texto'}}"
            r = json.loads(model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json")).text)
            r['fid'] = j['fixture']['id']; r['jogo'] = f"{j['teams']['home']['name']} x {j['teams']['away']['name']}"
            res.append(r)
        return res
    except: return []
# ==============================================================================
# 4. INTELIGÊNCIA ARTIFICIAL, CÁLCULOS E ESTRATÉGIAS
# ==============================================================================

def buscar_rating_inteligente(api_key, team_id):
    if db_firestore:
        try:
            docs = db_firestore.collection("BigData_Futebol").where("home_id", "==", str(team_id)).limit(10).stream()
            notas = [float(d.to_dict().get('rating_home', 0)) for d in docs if float(d.to_dict().get('rating_home', 0)) > 0]
            if len(notas) >= 3: return f"{sum(notas)/len(notas):.2f}"
        except: pass
    try:
        res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"team": team_id, "last": "1", "status": "FT"}).json()
        if not res.get('response'): return "N/A"
        fid = res['response'][0]['fixture']['id']
        stats = requests.get("https://v3.football.api-sports.io/fixtures/players", headers={"x-apisports-key": api_key}, params={"fixture": fid}).json()
        for t in stats.get('response', []):
            if t['team']['id'] == team_id:
                ns = [float(p['statistics'][0]['games']['rating']) for p in t['players'] if p['statistics'][0]['games']['rating']]
                return f"{sum(ns)/len(ns):.2f}" if ns else "N/A"
        return "N/A"
    except: return "N/A"

def estimar_odd_teorica(estrategia, tempo_jogo):
    limites = MAPA_ODDS_TEORICAS.get(estrategia, {"min": 1.40, "max": 1.60})
    ft = 0.20 if int(str(tempo_jogo).replace("'", "")) > 80 else 0
    return "{:.2f}".format(random.uniform(limites['min'], limites['max']) + ft)

def get_live_odds(fixture_id, api_key, strategy_name, total_gols_atual=0, tempo_jogo=0):
    try:
        res = requests.get("https://v3.football.api-sports.io/odds/live", headers={"x-apisports-key": api_key}, params={"fixture": fixture_id}).json()
        tm = ["1st half", "first half"] if "Relâmpago" in strategy_name and total_gols_atual == 0 else ["match goals", "goals over/under"]
        tl = 0.5 if "Relâmpago" in strategy_name else total_gols_atual + 0.5
        if res.get('response'):
            for m in res['response'][0]['odds']:
                if any(x in m['name'].lower() for x in tm) and "over" in m['name'].lower():
                    for v in m['values']:
                        if abs(float(''.join(c for c in str(v['value']) if c.isdigit() or c == '.')) - tl) < 0.1:
                            return "{:.2f}".format(float(v['odd']))
        return estimar_odd_teorica(strategy_name, tempo_jogo)
    except: return estimar_odd_teorica(strategy_name, tempo_jogo)

def obter_odd_final_para_calculo(odd_registro, estrategia):
    try:
        if pd.isna(odd_registro) or str(odd_registro).strip() == "" or float(odd_registro) <= 1.01:
            l = MAPA_ODDS_TEORICAS.get(estrategia, {"min": 1.40, "max": 1.60}); return (l['min']+l['max'])/2
        return float(odd_registro)
    except: return 1.50

def consultar_ia_gemini(dados_jogo, estrategia, stats_raw, rh, ra, extra_context="", time_favoravel=""):
    if not IA_ATIVADA: return "", "N/A"
    try:
        s1 = stats_raw[0]['statistics']; s2 = stats_raw[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        cg = gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal'); t = int(str(dados_jogo.get('tempo','0')).replace("'", ""))
        if t > 20 and (gv(s1,'Total Shots')+gv(s2,'Total Shots')) < 2: return "\n🤖 <b>IA:</b> ⚠️ <b>Reprovado</b> (Jogo Morto).", "10%"
        
        prompt = f"""ATUE COMO TRADER ESPORTIVO (OVER GOLS).
CENÁRIO: {dados_jogo['jogo']} | {dados_jogo['placar']} | {t} min. Estratégia: {estrategia}.
DADOS: Chutes Gol {cg} | Pressão {rh}x{ra}.
CONTEXTO: {extra_context}
MISSÃO: Se o time perdendo pressiona (reação) ou jogo aberto, APROVE.
SAÍDA: VEREDICTO: [Aprovado/Arriscado/Reprovado] PROB: [0-100]% MOTIVO: [Frase curta]"""
        
        resp = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(temperature=0.1)).text.strip().replace("*","")
        prob = int(re.search(r'PROB:\s*(\d+)', resp).group(1)) if re.search(r'PROB:\s*(\d+)', resp) else 0
        ver = "Neutro"
        if "aprovado" in resp.lower(): ver = "Aprovado"
        elif "arriscado" in resp.lower(): ver = "Arriscado"
        elif "reprovado" in resp.lower(): ver = "Reprovado"
        if ver == "Arriscado" and prob >= 65: ver = "Aprovado"
        motivo = resp.split('MOTIVO:')[-1].strip().split('\n')[0] if 'MOTIVO:' in resp else "Análise técnica."
        emoji = "✅" if ver == "Aprovado" else "⚠️"
        return f"\n🤖 <b>ANÁLISE TÉCNICA:</b>\n{emoji} <b>{ver.upper()} ({prob}%)</b>\n📝 <i>{motivo}</i>", f"{prob}%"
    except: return "", "N/A"

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
        def gv(l, t): v=next((x['value'] for x in l if x['type']==t),0); return v if v else 0
        sh_h=gv(s1,'Total Shots'); sog_h=gv(s1,'Shots on Goal'); blk_h=gv(s1,'Blocked Shots')
        sh_a=gv(s2,'Total Shots'); sog_a=gv(s2,'Shots on Goal'); blk_a=gv(s2,'Blocked Shots')
        
        rh, ra = 0, 0
        mem = st.session_state['memoria_pressao'].get(j['fixture']['id'], {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
        now = datetime.now()
        if sog_h > mem['sog_h']: mem['h_t'].extend([now]*(sog_h-mem['sog_h']))
        if sog_a > mem['sog_a']: mem['a_t'].extend([now]*(sog_a-mem['sog_a']))
        mem['h_t'] = [t for t in mem['h_t'] if now-t <= timedelta(minutes=7)]; rh=len(mem['h_t'])
        mem['a_t'] = [t for t in mem['a_t'] if now-t <= timedelta(minutes=7)]; ra=len(mem['a_t'])
        st.session_state['memoria_pressao'][j['fixture']['id']] = mem
        mem['sog_h'], mem['sog_a'] = sog_h, sog_a

        gh=j['goals']['home']; ga=j['goals']['away']; total_gols=gh+ga
        SINAIS = []
        
        # Estratégias
        if 65<=tempo<=75 and ((rh>=3 and sog_h>=4) or (ra>=3 and sog_a>=4)) and (total_gols>=1 or (sh_h+sh_a)>=18):
             if (blk_h+blk_a)>=5: SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": f"Over Limite {total_gols+0.5}", "stats": f"🛡️ {blk_h+blk_a} Bloqueios", "rh": rh, "ra": ra, "favorito": "GOLS"})
        
        if not SINAIS and (70<=tempo<=75) and abs(gh-ga)<=1 and (sog_h+sog_a)>=4:
             SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": f"Over Limite {total_gols+0.5}", "stats": f"🔥 {sog_h+sog_a} Chutes Gol", "rh": rh, "ra": ra, "favorito": "GOLS"})

        if 55<=tempo<=75 and (sh_h+sh_a)<=10 and (sog_h+sog_a)<=2 and gh==ga:
             SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": f"Under {total_gols+0.5}", "stats": "Jogo Travado", "rh": rh, "ra": ra, "favorito": "UNDER"})

        if tempo<=30 and total_gols>=2:
             SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": f"Over {total_gols+0.5}", "stats": "Jogo Aberto", "rh": rh, "ra": ra, "favorito": "GOLS"})
        
        if tempo<=60 and ((gh<=ga and rh>=3) or (ga<=gh and ra>=3)) and gv(s1,'Shots against goalbar')==0:
             SINAIS.append({"tag": "🟢 Blitz", "ordem": f"Over {total_gols+0.5}", "stats": "Pressão Limpa", "rh": rh, "ra": ra, "favorito": "GOLS"})

        if 15<=tempo<=25 and (sh_h+sh_a)>=6 and (sog_h+sog_a)>=3:
             SINAIS.append({"tag": "🏹 Tiroteio Elite", "ordem": f"Over {total_gols+0.5}", "stats": "Muitos Chutes", "rh": rh, "ra": ra, "favorito": "GOLS"})

        if 60<=tempo<=88 and abs(gh-ga)>=3 and (sh_h+sh_a)>=14:
             SINAIS.append({"tag": "🔫 Lay Goleada", "ordem": f"Over Limite {total_gols+0.5}", "stats": "Goleada Viva", "rh": rh, "ra": ra, "favorito": "GOLS"})

        if 60<=tempo<=75:
            if ((sog_h-sog_a)<=-3 and rh>=5 and gh<=ga) or ((sog_h-sog_a)>=3 and ra>=5 and ga<=gh):
                 SINAIS.append({"tag": "⚡ Oportunidade Relâmpago", "ordem": f"Over Limite {total_gols+0.5}", "stats": "Caos Tático", "rh": rh, "ra": ra, "favorito": "GOLS"})

        return SINAIS
    except: return []

# --- MÓDULOS DE IA (GERAÇÃO DE TEXTO) ---
def analisar_bi_com_ia():
    if not IA_ATIVADA: return "IA Desconectada."
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados."
    try:
        hoje = get_time_br().strftime('%Y-%m-%d')
        resumo = df[df['Data'].astype(str).str.contains(hoje, na=False)].groupby('Estrategia')['Resultado'].apply(lambda x: f"{(x.str.contains('GREEN').sum()/len(x)*100):.1f}%").to_dict()
        return model_ia.generate_content(f"Analise o dia ({hoje}). Resumo: {json.dumps(resumo)}. Destaque o que funcionou.").text
    except Exception as e: return f"Erro BI: {e}"

def analisar_financeiro_com_ia(stake, banca):
    if not IA_ATIVADA: return "IA Desconectada."
    try: return model_ia.generate_content("Dê um conselho curto financeiro para trader esportivo.").text
    except: return "Erro Fin."

def criar_estrategia_nova_ia():
    if not IA_ATIVADA: return "IA Desconectada."
    return model_ia.generate_content("Crie uma estratégia nova de aposta em futebol baseada em dados.").text

def otimizar_estrategias_existentes_ia():
    if not IA_ATIVADA: return "IA Desconectada."
    return model_ia.generate_content("Como otimizar estratégias de Over Gols com base em chutes no gol?").text

# ==============================================================================
# FUNÇÕES DE VERIFICAÇÃO E AUTOMAÇÃO (AGORA NO LUGAR CERTO)
# ==============================================================================

def verificar_automacao_bi(token, chat_ids, stake_padrao):
    agora = get_time_br()
    hoje_str = agora.strftime('%Y-%m-%d')
    if st.session_state.get('last_check_date') != hoje_str:
        st.session_state['bi_enviado'] = False; st.session_state['ia_enviada'] = False
        st.session_state['financeiro_enviado'] = False; st.session_state['bigdata_enviado'] = False
        st.session_state['last_check_date'] = hoje_str
    if agora.hour == 23 and agora.minute >= 30 and not st.session_state['bi_enviado']:
        enviar_relatorio_bi(token, chat_ids); st.session_state['bi_enviado'] = True
    if agora.hour == 23 and agora.minute >= 40 and not st.session_state['financeiro_enviado']:
        analise = analisar_financeiro_com_ia(stake_padrao, st.session_state.get('banca_inicial', 100))
        enviar_telegram(token, chat_ids, f"💰 <b>CONSULTORIA</b>\n\n{analise}"); st.session_state['financeiro_enviado'] = True
    if agora.hour == 23 and agora.minute >= 55 and not st.session_state['bigdata_enviado']:
        enviar_analise_estrategia(token, chat_ids); st.session_state['bigdata_enviado'] = True

def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br()
    if 8 <= agora.hour < 11:
        if not st.session_state['matinal_enviado']:
            insights = gerar_insights_matinais_ia(api_key)
            if insights and "Sem jogos" not in insights:
                enviar_telegram(token, chat_ids, f"🌅 <b>SNIPER MATINAL</b>\n\n{insights}")
                salvar_snipers_do_texto(insights); st.session_state['matinal_enviado'] = True
        if st.session_state['matinal_enviado'] and not st.session_state['multipla_matinal_enviada']:
            time.sleep(5); enviar_multipla_matinal(token, chat_ids, api_key)
        if st.session_state['matinal_enviado'] and st.session_state['multipla_matinal_enviada'] and not st.session_state['alternativos_enviado']:
            time.sleep(5); enviar_alerta_alternativos(token, chat_ids, api_key)
        if agora.hour >= 10 and not st.session_state['alavancagem_enviada']:
            time.sleep(5); enviar_alavancagem(token, chat_ids, api_key)

    # TRADING (DROP ODDS) - Janela 12h-13h30 e 16h
    f12 = (agora.hour == 12 or (agora.hour == 13 and agora.minute <= 30))
    f16 = (agora.hour == 16 and agora.minute <= 30)
    
    if f12 and not st.session_state['drop_enviado_12']:
        drops = scanner_drop_odds_pre_live(api_key)
        if drops:
            for d in drops:
                enviar_telegram(token, chat_ids, f"💰 <b>TRADING DROP</b>\n⚽ {d['jogo']}\n📉 Drop: {d['valor']:.1f}%\n👉 {d['lado']} + Banker")
                adicionar_historico({"FID": f"DROP_{d['fid']}", "Data": hoje_str, "Hora": agora.strftime('%H:%M'), "Liga": "Trading", "Jogo": d['jogo'], "Estrategia": "Drop Odds Cashout", "Resultado": "Pendente", "Opiniao_IA": "Aprovado"})
        st.session_state['drop_enviado_12'] = True
        
    if f16 and not st.session_state['drop_enviado_16']:
        drops = scanner_drop_odds_pre_live(api_key)
        if drops:
            for d in drops:
                enviar_telegram(token, chat_ids, f"💰 <b>TRADING DROP</b>\n⚽ {d['jogo']}\n📉 Drop: {d['valor']:.1f}%\n👉 {d['lado']} + Banker")
                adicionar_historico({"FID": f"DROP_{d['fid']}", "Data": hoje_str, "Hora": agora.strftime('%H:%M'), "Liga": "Trading", "Jogo": d['jogo'], "Estrategia": "Drop Odds Cashout", "Resultado": "Pendente", "Opiniao_IA": "Aprovado"})
        st.session_state['drop_enviado_16'] = True

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    hist = st.session_state['historico_sinais']; pend = [s for s in hist if s['Resultado'] == 'Pendente']
    if not pend: return
    updates = []; mapa = {j['fixture']['id']: j for j in jogos_live}
    for s in pend:
        if s.get('Data') != get_time_br().strftime('%Y-%m-%d') or "Sniper" in s['Estrategia'] or "Alavancagem" in s['Estrategia'] or "Drop" in s['Estrategia'] or "Mercado" in s['Liga']: continue
        fid = int(clean_fid(s.get('FID', 0))); j = mapa.get(fid)
        if not j:
            try: j = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json().get('response', [None])[0]
            except: pass
        if j:
            gh=j['goals']['home'] or 0; ga=j['goals']['away'] or 0; st=j['fixture']['status']['short']
            try: ph, pa = map(int, s['Placar_Sinal'].split('x'))
            except: ph, pa = 0, 0
            res = None
            if (gh+ga) > (ph+pa) and "Under" not in s['Estrategia']: res = "✅ GREEN"
            elif st in ['FT','AET','PEN']: res = "✅ GREEN" if "Morno" in s['Estrategia'] else "❌ RED"
            if res:
                s['Resultado'] = res; updates.append(s)
                enviar_telegram(token, chats, f"{res} <b>CONFIRMADO</b>\n⚽ {s['Jogo']}\n📈 {gh}x{ga}")
    if updates: atualizar_historico_ram(updates)

def validar_multiplas_pendentes(jogos_live, api_key, token, chat_ids):
    if not st.session_state.get('multiplas_pendentes'): return
    for m in st.session_state['multiplas_pendentes']:
        if m['status']!='Pendente' or m['data']!=get_time_br().strftime('%Y-%m-%d'): continue
        res = []; txt = []
        for fid in m['fids']:
            j = next((x for x in jogos_live if str(x['fixture']['id'])==str(fid)), None)
            if not j: 
                try: j = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json().get('response',[None])[0]
                except: pass
            if not j: res.append("PENDENTE"); continue
            g = (j['goals']['home'] or 0) + (j['goals']['away'] or 0)
            if (m['tipo']=="MATINAL" and g>=1) or (m['tipo']!="MATINAL" and g > m['gols_ref'].get(fid,0)): res.append("GREEN")
            elif j['fixture']['status']['short'] in ['FT','AET','PEN']: res.append("RED")
            else: res.append("PENDENTE")
            txt.append(f"{j['goals']['home']}x{j['goals']['away']}")
        if "RED" in res:
            enviar_telegram(token, chat_ids, f"❌ <b>RED MÚLTIPLA</b>\n📉 {' / '.join(txt)}"); m['status']="RED"
            adicionar_historico({"FID": m['id_unico'], "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": "00:00", "Liga": "Múltiplas", "Jogo": " + ".join(m['nomes']), "Estrategia": "Múltipla", "Resultado": "❌ RED"})
        elif all(x=="GREEN" for x in res):
            enviar_telegram(token, chat_ids, f"✅ <b>GREEN MÚLTIPLA</b>\n📈 {' / '.join(txt)}"); m['status']="GREEN"
            adicionar_historico({"FID": m['id_unico'], "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": "00:00", "Liga": "Múltiplas", "Jogo": " + ".join(m['nomes']), "Estrategia": "Múltipla", "Resultado": "✅ GREEN"})

def verificar_mercados_alternativos(api_key):
    pend = [s for s in st.session_state.get('historico_sinais', []) if s['Liga']=='Mercado Alternativo' and s['Resultado']=='Pendente']
    upd = []
    for s in pend:
        try:
            fid = s['FID'].replace("ALT_",""); meta = float(str(s['Placar_Sinal']).split(':')[1])
            r = requests.get("https://v3.football.api-sports.io/fixtures/statistics", headers={"x-apisports-key": api_key}, params={"fixture": fid}).json()
            if not r.get('response'): continue
            sh = r['response'][0]['statistics']; sa = r['response'][1]['statistics']
            def gv(l, t): return next((x['value'] or 0 for x in l if x['type']==t),0)
            res = "❌ RED"
            if "CARTÕES" in s['Estrategia']:
                if (gv(sh,"Yellow Cards")+gv(sh,"Red Cards")+gv(sa,"Yellow Cards")+gv(sa,"Red Cards")) > meta: res = "✅ GREEN"
            elif "DEFESAS" in s['Estrategia']:
                if max(gv(sh,"Goalkeeper Saves"), gv(sa,"Goalkeeper Saves")) >= meta: res = "✅ GREEN"
            s['Resultado'] = res; upd.append(s)
        except: pass
    if upd: atualizar_historico_ram(upd)
# ==============================================================================
# 5. UI (INTERFACE), BARRA LATERAL E LOOP PRINCIPAL
# ==============================================================================

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
                with st.spinner("🤖 O Consultor Neves está analisando seus dados..."):
                    analise = analisar_bi_com_ia(); st.markdown("### 📝 Relatório do Consultor"); st.info(analise)
            else: st.error("IA Desconectada.")
        if st.button("🧪 Criar Nova Estratégia (Big Data)"):
            if IA_ATIVADA:
                with st.spinner("🤖 Analisando padrões globais no Big Data..."):
                    sugestao = criar_estrategia_nova_ia(); st.markdown("### 💡 Sugestão da IA"); st.success(sugestao)
            else: st.error("IA Desconectada.")
        if st.button("🔧 Otimizar Estratégias"):
            if IA_ATIVADA:
                with st.spinner("🤖 Cruzando performance real com Big Data..."):
                    sugestao_otimizacao = otimizar_estrategias_existentes_ia()
                    st.markdown("### 🛠️ Plano de Melhoria"); st.info(sugestao_otimizacao)
            else: st.error("IA Desconectada.")
        if st.button("🚀 Gerar Alavancagem (Jogo Único)"):
            if IA_ATIVADA:
                with st.spinner("🤖 Triangulando API + Big Data + Histórico Pessoal..."):
                    enviar_alavancagem(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], st.session_state['API_KEY'])
                    st.success("Análise de Alavancagem Realizada e Salva!")
            else: st.error("IA Desconectada.")
        
        # [NOVO: BOTÃO MANUAL DE TRADING]
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
        if st.button("📊 Enviar Relatório BI"): enviar_relatorio_bi(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT']); st.toast("Relatório Enviado!")
        if st.button("💰 Enviar Relatório Financeiro"):
            if 'last_fin_stats' in st.session_state:
                s = st.session_state['last_fin_stats']
                enviar_relatorio_financeiro(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], s['cenario'], s['lucro'], s['roi'], s['entradas'])
                st.toast("Relatório Financeiro Enviado!")
            else: st.error("Abra a aba Financeiro primeiro.")

    with st.expander("💰 Gestão de Banca", expanded=False):
        stake_padrao = st.number_input("Valor da Aposta (R$)", value=st.session_state.get('stake_padrao', 10.0), step=5.0)
        banca_inicial = st.number_input("Banca Inicial (R$)", value=st.session_state.get('banca_inicial', 100.0), step=50.0)
        st.session_state['stake_padrao'] = stake_padrao; st.session_state['banca_inicial'] = banca_inicial
        
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

        # Chamada das Automações (Agora as funções já foram lidas acima)
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

        if not api_error: 
            # 1. Rotinas Padrão
            check_green_red_hibrido(jogos_live, safe_token, safe_chat, safe_api)
            conferir_resultados_sniper(jogos_live, safe_api) 
            verificar_var_rollback(jogos_live, safe_token, safe_chat)
            
            # 2. NOVAS ROTINAS (Múltiplas e Mercados Especiais)
            verificar_multipla_quebra_empate(jogos_live, safe_token, safe_chat)
            validar_multiplas_pendentes(jogos_live, safe_api, safe_token, safe_chat)
            verificar_mercados_alternativos(safe_api)

        radar = []; agenda = []; candidatos_multipla = []; ids_no_radar = []
        if not api_error:
            jogos_para_atualizar = []
            agora_dt = datetime.now()
            
            # Loop de Análise dos Jogos ao Vivo
            for j in jogos_live:
                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                if lid in ids_black: continue
                status_short = j['fixture']['status']['short']
                elapsed = j['fixture']['status']['elapsed']
                
                if status_short not in ['1H', '2H', 'HT', 'ET']: continue
                
                ult_chk = st.session_state['controle_stats'].get(fid, datetime.min)
                gh = j['goals']['home']; ga = j['goals']['away']
                tempo_jogo = elapsed or 0
                t_esp = 180 
                eh_importante = (tempo_jogo <= 20) or (tempo_jogo >= 70 and abs(gh-ga)<=1) or (status_short == 'HT')
                memoria = st.session_state['memoria_pressao'].get(fid, {})
                pressao_recente = (len(memoria.get('h_t', [])) + len(memoria.get('a_t', []))) >= 4
                if eh_importante or pressao_recente: t_esp = 60 
                
                if deve_buscar_stats(tempo_jogo, gh, ga, status_short):
                    if (agora_dt - ult_chk).total_seconds() > t_esp:
                        jogos_para_atualizar.append(j)

            if jogos_para_atualizar:
                novas_stats = atualizar_stats_em_paralelo(jogos_para_atualizar, safe_api)
                for fid_up, stats_up in novas_stats.items():
                        st.session_state['controle_stats'][fid_up] = datetime.now()
                        st.session_state[f"st_{fid_up}"] = stats_up

            # Busca de Agenda e Backfill
            prox = buscar_agenda_cached(safe_api, hoje_real); agora = get_time_br()
            ft_para_salvar = []
            for p in prox:
                try:
                    if p['fixture']['status']['short'] in ['FT', 'AET', 'PEN'] and str(p['fixture']['id']) not in st.session_state['jogos_salvos_bigdata']:
                        ft_para_salvar.append(p)
                except: pass
            if ft_para_salvar:
                lote = random.sample(ft_para_salvar, min(len(ft_para_salvar), 3)) 
                stats_ft = atualizar_stats_em_paralelo(lote, safe_api)
                for fid, s in stats_ft.items():
                    j_obj = next((x for x in lote if x['fixture']['id'] == fid), None)
                    if j_obj: salvar_bigdata(j_obj, s)

            # Processamento de Estratégias
            STATUS_BOLA_ROLANDO = ['1H', '2H', 'HT', 'ET', 'P', 'BT']
            for j in jogos_live:
                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                if lid in ids_black: continue
                status_short = j['fixture']['status']['short']
                elapsed = j['fixture']['status']['elapsed']
                if status_short not in STATUS_BOLA_ROLANDO: continue
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
                    medias_gols = buscar_media_gols_ultimos_jogos(safe_api, j['teams']['home']['id'], j['teams']['away']['id'])
                    dados_50 = analisar_tendencia_50_jogos(safe_api, j['teams']['home']['id'], j['teams']['away']['id'])
                    nota_home = buscar_rating_inteligente(safe_api, j['teams']['home']['id'])
                    nota_away = buscar_rating_inteligente(safe_api, j['teams']['away']['id'])
                    txt_bigdata = consultar_bigdata_cenario_completo(j['teams']['home']['id'], j['teams']['away']['id'])
                    df_sheets = st.session_state.get('historico_full', pd.DataFrame())
                    txt_pessoal = "Neutro"
                    if not df_sheets.empty:
                        f_h = df_sheets[df_sheets['Jogo'].str.contains(home, na=False, case=False)]
                        if len(f_h) > 2:
                            greens = len(f_h[f_h['Resultado'].str.contains('GREEN', na=False)])
                            txt_pessoal = f"Winrate Pessoal com {home}: {(greens/len(f_h))*100:.0f}%"

                    txt_history = ""
                    if dados_50: txt_history = (f"API (50j): Casa(Over1.5: {dados_50['home']['over15_ft']}%) | Fora(Over1.5: {dados_50['away']['over15_ft']}%)")
                    
                    extra_ctx = f"FONTE 1 (API): {txt_history} | Rating: {nota_home}x{nota_away}\nFONTE 2 (BD): {txt_bigdata}\nFONTE 3: {txt_pessoal}"

                    for s in lista_sinais:
                        prob = "..." 
                        uid_normal = gerar_chave_universal(fid, s['tag'], "SINAL"); uid_super = f"SUPER_{uid_normal}"
                        
                        ja_enviado_total = False
                        if uid_normal in st.session_state['alertas_enviados']: ja_enviado_total = True
                        if not ja_enviado_total:
                            for item_hist in st.session_state['historico_sinais']:
                                if gerar_chave_universal(item_hist['FID'], item_hist['Estrategia'], "SINAL") == uid_normal:
                                    ja_enviado_total = True; st.session_state['alertas_enviados'].add(uid_normal); break
                        if ja_enviado_total: continue 
                        
                        st.session_state['alertas_enviados'].add(uid_normal)
                        odd_atual_str = get_live_odds(fid, safe_api, s['tag'], gh+ga, tempo)
                        try: odd_val = float(odd_atual_str)
                        except: odd_val = 0.0
                        destaque_odd = "\n💎 <b>SUPER ODD DETECTADA!</b>" if odd_val >= 1.80 else ""
                        if odd_val >= 1.80: st.session_state['alertas_enviados'].add(uid_super)
                        
                        opiniao_txt = ""; prob_txt = "..."; opiniao_db = "Neutro"
                        if IA_ATIVADA:
                            try:
                                time.sleep(0.2) 
                                opiniao_txt, prob_txt = consultar_ia_gemini({'jogo': f"{home} x {away}", 'placar': placar, 'tempo': f"{tempo}'"}, s['tag'], stats, rh, ra, extra_context=extra_ctx, time_favoravel=s.get('favorito', ''))
                                if "aprovado" in opiniao_txt.lower(): opiniao_db = "Aprovado"
                                elif "arriscado" in opiniao_txt.lower(): opiniao_db = "Arriscado"
                                else: opiniao_db = "Neutro"
                            except: pass
                        
                        item = {"FID": str(fid), "Data": hoje_real, "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{home} x {away} ({placar})", "Placar_Sinal": placar, "Estrategia": s['tag'], "Resultado": "Pendente", "HomeID": str(j['teams']['home']['id']), "AwayID": str(j['teams']['away']['id']), "Odd": odd_atual_str, "Odd_Atualizada": "", "Opiniao_IA": opiniao_db, "Probabilidade": prob_txt}
                        
                        if adicionar_historico(item):
                            try:
                                header_winrate = ""
                                if not header_winrate and "Winrate Pessoal" in txt_pessoal: header_winrate = f" | 👤 <b>Time: {txt_pessoal.split(':')[-1].strip()}</b>"
                                if not header_winrate and dados_50: header_winrate = f" | 📊 <b>API: {dados_50['home']['over15_ft']}%</b>"

                                texto_momento = "Morno 🧊"
                                if rh > ra: texto_momento = "Pressão Casa 🔥"
                                elif ra > rh: texto_momento = "Pressão Visitante 🔥"
                                elif rh > 2 or ra > 2: texto_momento = "Jogo Aberto ⚡"

                                msg = f"🚨 <b>SINAL {s['tag'].upper()}</b>{header_winrate}\n🏆 {j['league']['name']}\n⚽ <b>{home} 🆚 {away}</b>\n⏰ {tempo}' min | 🥅 Placar: {placar}\n\n{s['ordem']}\n{destaque_odd}\n──────────────\n📊 <b>Raio-X:</b>\n• 🔥 <b>Ataque:</b> {s.get('stats', 'Pressão')}\n• 🌡️ <b>Ritmo:</b> {texto_momento}\n"
                                if txt_history: msg += f"• 📉 <b>Histórico (50j):</b>\n{txt_history}\n"
                                if medias_gols and medias_gols['home'] != '?': msg += f"• ⚽ <b>Média Gols:</b> Casa {medias_gols['home']} | Fora {medias_gols['away']}\n"
                                msg += f"\n{opiniao_txt}" 
                                
                                if opiniao_db == "Aprovado": enviar_telegram(safe_token, safe_chat, msg); st.toast(f"✅ Sinal Enviado: {s['tag']}")
                                elif opiniao_db == "Arriscado": enviar_telegram(safe_token, safe_chat, msg + "\n👀 <i>Obs: Risco moderado.</i>"); st.toast(f"⚠️ Sinal Arriscado Enviado: {s['tag']}")
                                else: st.toast(f"🛑 Sinal Retido: {s['tag']}")

                            except Exception as e: print(f"Erro ao enviar sinal: {e}")
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
                st.caption("⏳ Sincronizando dados pendentes..."); salvar_aba("Historico", st.session_state['historico_full'])
        
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
            stake_padrao = c_fin1.number_input("Valor da Aposta (R$)", value=st.session_state.get('stake_padrao', 10.0), step=5.0)
            banca_inicial = c_fin2.number_input("Banca Inicial (R$)", value=st.session_state.get('banca_inicial', 100.0), step=50.0)
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
                            prob_min = st.slider("🎯 Filtrar Probabilidade Mínima IA (%)", 0, 100, 0)
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
            st.markdown("### 💬 Chat Intelligence (Auditor de Algoritmo)")
            if "messages" not in st.session_state:
                st.session_state["messages"] = [{"role": "assistant", "content": "Olá! Estou pronto para auditar seu código. Se houver Reds, me avise que eu gero a correção."}]
            if len(st.session_state["messages"]) > 6:
                st.session_state["messages"] = st.session_state["messages"][-6:]

            for msg in st.session_state.messages: 
                st.chat_message(msg["role"]).write(msg["content"])
            
            if prompt := st.chat_input("Ex: Crie um filtro para evitar o Red de hoje."):
                if not IA_ATIVADA: st.error("IA Desconectada. Verifique a API Key.")
                else:
                    st.session_state.messages.append({"role": "user", "content": prompt})
                    st.chat_message("user").write(prompt)

                    txt_radar = "RADAR VAZIO."
                    if radar: txt_radar = pd.DataFrame(radar).to_string(index=False)

                    txt_hoje = "SEM DADOS HOJE."
                    if 'historico_sinais' in st.session_state and st.session_state['historico_sinais']:
                        df_hj = pd.DataFrame(st.session_state['historico_sinais'])
                        cols = ['Liga', 'Jogo', 'Estrategia', 'Resultado', 'Placar_Sinal', 'Opiniao_IA']
                        cols_exist = [c for c in cols if c in df_hj.columns]
                        txt_hoje = df_hj[cols_exist].head(20).to_string(index=False) 

                    txt_bigdata = "BIG DATA OFFLINE."
                    total_bd = st.session_state.get('total_bigdata_count', 0)
                    dados_bd = st.session_state.get('cache_firebase_view', [])
                    
                    if dados_bd or total_bd > 0:
                         txt_bigdata = f"TOTAL REAL DE JOGOS NO BANCO: {total_bd} JOGOS.\n"
                         txt_bigdata += "Amostra visual (últimos 50): \n"
                         try:
                             df_bd = pd.DataFrame(dados_bd)
                             if 'estatisticas' in df_bd.columns:
                                 txt_bigdata += f"Exemplo de Estrutura de Dados: {df_bd.iloc[0]['estatisticas']}"
                         except: pass

                    contexto_chat = f"""
                    ATUE COMO: Engenheiro Sênior Python e Cientista de Dados do "Neves Analytics".
                    IMPORTANTE:
                    - O usuário possui um Big Data com {total_bd} jogos armazenados.
                    - Não diga que a amostra é pequena se o total for alto.
                    
                    CONTEXTO ATUAL:
                    1. PERFORMANCE HOJE (Google Sheets): 
                    {txt_hoje}
                    2. BIG DATA (Firebase): 
                    {txt_bigdata}
                    3. RADAR (Ao Vivo): 
                    {txt_radar}
                    
                    USUÁRIO PERGUNTOU: "{prompt}"
                    """

                    try:
                        with st.spinner("Gerando solução de código..."):
                            response = model_ia.generate_content(contexto_chat)
                            st.session_state['gemini_usage']['used'] += 1
                            msg_ia = response.text
                        
                        st.session_state.messages.append({"role": "assistant", "content": msg_ia})
                        st.chat_message("assistant").write(msg_ia)
                        if len(st.session_state["messages"]) > 6:
                            time.sleep(0.5); st.rerun()
                            
                    except Exception as e: st.error(f"Erro na IA: {e}")

        # --- NOVA ABA DE TRADING ---
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
