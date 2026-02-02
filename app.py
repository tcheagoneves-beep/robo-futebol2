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

# --- VARIÁVEIS PARA MÚLTIPLAS, TRADING E ALAVANCAGEM ---
if 'multipla_matinal_enviada' not in st.session_state: st.session_state['multipla_matinal_enviada'] = False
if 'multiplas_live_cache' not in st.session_state: st.session_state['multiplas_live_cache'] = {}
if 'multiplas_pendentes' not in st.session_state: st.session_state['multiplas_pendentes'] = []
if 'alternativos_enviado' not in st.session_state: st.session_state['alternativos_enviado'] = False
if 'alavancagem_enviada' not in st.session_state: st.session_state['alavancagem_enviada'] = False 

# [NOVO] Variáveis de controle do Trading (Drop Odds)
if 'drop_enviado_12' not in st.session_state: st.session_state['drop_enviado_12'] = False
if 'drop_enviado_16' not in st.session_state: st.session_state['drop_enviado_16'] = False
# -------------------------------------------------

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

# Mapa para referência
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
    "Drop Odds Cashout": "Trading" # [NOVO] Adicionado para o relatório
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

# [NOVO MÓDULO] ESTRATÉGIA CASHOUT / DROP ODDS (PRÉ-LIVE)
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

def consultar_bigdata_cenario_completo(home_id, away_id):
    """
    RAIO-X 360º: Analisa Gols, Cantos, Cartões, Chutes e Agressividade.
    """
    if not db_firestore: return "Big Data Offline"
    try:
        docs_h = db_firestore.collection("BigData_Futebol").where("home_id", "==", str(home_id)).limit(20).stream()
        docs_a = db_firestore.collection("BigData_Futebol").where("away_id", "==", str(away_id)).limit(20).stream()
        def safe_get(stats_dict, key):
            try: return float(stats_dict.get(key, 0))
            except: return 0.0
        h_data = {'qtd': 0, 'gols_pro': 0, 'cantos': 0, 'cards': 0, 'sog': 0, 'faltas': 0, 'imp': 0}
        for d in docs_h:
            dd = d.to_dict(); h_data['qtd'] += 1; st = dd.get('estatisticas', {})
            try: h_data['gols_pro'] += int(dd['placar_final'].split('x')[0])
            except: pass
            h_data['cantos'] += safe_get(st, 'escanteios_casa')
            h_data['cards'] += safe_get(st, 'cartoes_amarelos') + safe_get(st, 'cartoes_vermelhos')
            h_data['sog'] += safe_get(st, 'chutes_gol')
        a_data = {'qtd': 0, 'gols_pro': 0, 'cantos': 0, 'cards': 0, 'sog': 0, 'faltas': 0, 'imp': 0}
        for d in docs_a:
            dd = d.to_dict(); a_data['qtd'] += 1; st = dd.get('estatisticas', {})
            try: a_data['gols_pro'] += int(dd['placar_final'].split('x')[1]) 
            except: pass
            a_data['cantos'] += safe_get(st, 'escanteios_fora') 
            a_data['cards'] += safe_get(st, 'cartoes_amarelos') + safe_get(st, 'cartoes_vermelhos')
            a_data['sog'] += safe_get(st, 'chutes_gol')
        if h_data['qtd'] == 0 and a_data['qtd'] == 0: return "Sem dados suficientes."
        txt_h = "N/D"
        if h_data['qtd'] > 0: q = h_data['qtd']; txt_h = (f"MANDANTE (Casa, {q}j): Gols {h_data['gols_pro']/q:.1f} | Cantos {h_data['cantos']/q:.1f} | ChutesGol {h_data['sog']/q:.1f}")
        txt_a = "N/D"
        if a_data['qtd'] > 0: q = a_data['qtd']; txt_a = (f"VISITANTE (Fora, {q}j): Gols {a_data['gols_pro']/q:.1f} | Cantos {a_data['cantos']/q:.1f} | ChutesGol {a_data['sog']/q:.1f}")
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
            params = {"team": team_id, "last": "50", "status": "FT"}
            res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
            jogos = res.get('response', [])
            if not jogos: return {"over05_ht": 0, "over15_ft": 0, "ambas_marcam": 0, "avg_cards": 0, "avg_shots_goal": 0, "avg_saves_conceded": 0}
            stats = {"qtd": len(jogos), "over05_ht": 0, "over15_ft": 0, "ambas_marcam": 0}
            for j in jogos:
                gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
                g_ht_h = j['score']['halftime']['home'] or 0; g_ht_a = j['score']['halftime']['away'] or 0
                if (g_ht_h + g_ht_a) > 0: stats["over05_ht"] += 1
                if (gh + ga) >= 2: stats["over15_ft"] += 1
                if gh > 0 and ga > 0: stats["ambas_marcam"] += 1
            return {k: int((v / stats["qtd"]) * 100) if k not in ["qtd"] else v for k, v in stats.items()}
        return {"home": get_stats_50(home_id), "away": get_stats_50(away_id)}
    except: return None

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

def gerar_multipla_matinal_ia(api_key):
    if not IA_ATIVADA: return None, []
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        LIGAS_SEGURAS = [39, 140, 78, 135, 61, 71, 72, 2, 3] 
        jogos_candidatos = [j for j in jogos if j['league']['id'] in LIGAS_SEGURAS]
        if len(jogos_candidatos) < 2: return None, []
        
        lista_jogos_txt = ""
        mapa_jogos = {}
        for j in jogos_candidatos[:10]:
            fid = j['fixture']['id']
            home = j['teams']['home']['name']
            away = j['teams']['away']['name']
            mapa_jogos[fid] = f"{home} x {away}"
            try:
                stats_h = analisar_tendencia_50_jogos(api_key, j['teams']['home']['id'], j['teams']['away']['id'])
                if stats_h:
                    lista_jogos_txt += f"- ID {fid}: {home} x {away} | Over 1.5 FT: Casa {stats_h['home']['over15_ft']}% / Fora {stats_h['away']['over15_ft']}%\n"
            except: pass

        if not lista_jogos_txt: return None, []
        contexto_firebase = carregar_contexto_global_firebase()
        df_sheets = st.session_state.get('historico_full', pd.DataFrame())
        winrate_sheets = "N/A"
        if not df_sheets.empty:
            greens = len(df_sheets[df_sheets['Resultado'].str.contains('GREEN', na=False)])
            total = len(df_sheets[df_sheets['Resultado'].isin(['✅ GREEN', '❌ RED'])])
            if total > 0: winrate_sheets = f"{(greens/total)*100:.1f}%"

        prompt = f"""
        Atue como GESTOR DE RISCO E ESTRATÉGIA.
        OBJETIVO: Criar uma "Múltipla de Segurança" (Bingo Matinal) com 2 ou 3 jogos para HOJE.
        DADOS: Winrate Pessoal: {winrate_sheets}. Global Firebase: {contexto_firebase}.
        Jogos: {lista_jogos_txt}
        TAREFA: Escolha os 2 ou 3 jogos mais seguros para Over 0.5 Gols.
        FORMATO JSON: {{ "jogos": [ {{"fid": 123, "jogo": "A x B", "motivo": "..."}} ], "probabilidade_combinada": "90" }}
        """
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        st.session_state['gemini_usage']['used'] += 1
        return json.loads(response.text), mapa_jogos
    except Exception as e: return None, []

def gerar_analise_mercados_alternativos_ia(api_key):
    if not IA_ATIVADA: return []
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        
        LIGAS_TOP = [39, 140, 78, 135, 61, 71, 72, 2, 3]
        jogos_candidatos = [j for j in jogos if j['league']['id'] in LIGAS_TOP and j['fixture'].get('referee')]
        
        if not jogos_candidatos: return []
        
        amostra = random.sample(jogos_candidatos, min(len(jogos_candidatos), 5))
        
        dados_analise = ""
        for j in amostra:
            fid = j['fixture']['id']
            home = j['teams']['home']['name']
            away = j['teams']['away']['name']
            referee = j['fixture']['referee']
            dados_analise += f"- Jogo: {home} x {away} | Árbitro: {referee} | ID: {fid}\n"

        prompt = f"""
        ATUE COMO ANALISTA DE MERCADOS ESPECIAIS (Cartões e Goleiros).
        DADOS DE HOJE:
        {dados_analise}
        
        TAREFA:
        1. Identifique se algum desses Árbitros é conhecido por ser "Rigoroso" (Over Cartões).
        2. Identifique se algum desses confrontos sugere "Muitos Chutes" (Bom para Goleiro do time mais fraco).
        
        RETORNE APENAS AS OPORTUNIDADES CLARAS (Se houver).
        FORMATO JSON:
        {{
            "sinais": [
                {{
                    "fid": "123",
                    "tipo": "CARTAO" ou "GOLEIRO",
                    "titulo": "🟨 SNIPER DE CARTÕES" ou "🧤 MURALHA (DEFESAS)",
                    "jogo": "Time A x Time B",
                    "destaque": "Juiz Fulano tem média 6.5 cartões",
                    "indicacao": "Over 4.5 Cartões"
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
                if "7" in dados_bd or "8" in dados_bd or "9" in dados_bd: score += 2 
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
        top_picks = lista_provaveis[:3]
        
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
                if any(tm in m_name for tm in target_markets) and "over" in m_name:
                    for v in m['values']:
                        try:
                            line_raw = str(v['value']).lower().replace("over", "").strip()
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

def consultar_ia_gemini(dados_jogo, estrategia, stats_raw, rh, ra, extra_context="", time_favoravel=""):
    if not IA_ATIVADA: return "", "N/A"
    try:
        s1 = stats_raw[0]['statistics']; s2 = stats_raw[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        chutes_totais = gv(s1, 'Total Shots') + gv(s2, 'Total Shots')
        chutes_gol = gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal')
        chutes_fora = chutes_totais - chutes_gol
        tempo_str = str(dados_jogo.get('tempo', '0')).replace("'", "")
        tempo = int(tempo_str) if tempo_str.isdigit() else 0
        if tempo > 20 and chutes_totais < 2:
            return "\n🤖 <b>IA:</b> ⚠️ <b>Reprovado</b> - Jogo sem volume (Morto).", "10%"
    except: return "", "N/A"
    escanteios = gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks')
    
    prompt = f"""
    ATUE COMO UM ANALISTA DE RISCO SÊNIOR (FUTEBOL).
    SEJA DIRETO, FRIO E EXECUTIVO. NÃO SE AUTO-INTITULE.
    CENÁRIO:
    - Jogo: {dados_jogo['jogo']} ({dados_jogo['placar']}) aos {tempo} min.
    - Estratégia: {estrategia}
    DADOS TÉCNICOS:
    - Chutes no Gol (Perigo Real): {chutes_gol} (de {chutes_totais} totais)
    - Escanteios: {escanteios}
    - Momentum (Pressão): Casa {rh} x {ra} Fora
    CONTEXTO (BIG DATA/HISTÓRICO):
    {extra_context}
    SUA MISSÃO:
    Calcule internamente a probabilidade de Green.
    - Penalize ineficiência (muitos chutes pra fora).
    - Penalize histórico ruim no Big Data.
    - Valorize pressão alta (Momentum) + Chutes no Gol.
    SAÍDA OBRIGATÓRIA (Use exatamente este formato):
    VEREDICTO: [Aprovado/Arriscado/Reprovado]
    PROB: [Número]%
    MOTIVO: [Uma frase curta e tática explicando o porquê. NÃO mostre contas matemáticas. Ex: "Time da casa pressiona mas finaliza mal" ou "Pressão avassaladora confirma o Big Data".]
    """
    try:
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(temperature=0.0))
        st.session_state['gemini_usage']['used'] += 1
        texto = response.text.strip().replace("**", "").replace("*", "")
        prob_str = "N/A"; prob_val = 0
        match = re.search(r'PROB:\s*(\d+)', texto)
        if match: prob_val = int(match.group(1)); prob_str = f"{prob_val}%"
        if prob_val > 70 and chutes_gol == 0 and tempo > 30:
            prob_val = 40; texto = texto.replace("Aprovado", "Reprovado"); prob_str = "40%"
        veredicto = "Neutro"
        if "aprovado" in texto.lower() and "reprovado" not in texto.lower(): veredicto = "Aprovado"
        elif "arriscado" in texto.lower(): veredicto = "Arriscado"
        elif "reprovado" in texto.lower(): veredicto = "Reprovado"
        if veredicto == "Aprovado" and prob_val < 80: veredicto = "Arriscado"
        motivo = texto.split('MOTIVO:')[-1].strip().split('\n')[0] if 'MOTIVO:' in texto else "Análise técnica."
        emoji = "✅" if veredicto == "Aprovado" else "⚠️"
        return f"\n🤖 <b>ANÁLISE TÉCNICA:</b>\n{emoji} <b>{veredicto.upper()} ({prob_str})</b>\n📝 <i>{motivo}</i>", prob_str
    except: return "", "N/A"

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

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        ck_h = get_v(stats_h, 'Corner Kicks'); ck_a = get_v(stats_a, 'Corner Kicks')
        blk_h = get_v(stats_h, 'Blocked Shots'); blk_a = get_v(stats_a, 'Blocked Shots')
        post_h = get_v(stats_h, 'Shots against goalbar') 

        total_chutes = sh_h + sh_a; total_chutes_gol = sog_h + sog_a; total_bloqueados = blk_h + blk_a
        chutes_fora_h = max(0, sh_h - sog_h - blk_h); chutes_fora_a = max(0, sh_a - sog_a - blk_a)
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
        if 65 <= tempo <= 75:
            if ((rh >= 3 and sog_h >= 4) or (ra >= 3 and sog_a >= 4)) and (total_gols >= 1 or total_chutes >= 18):
                if total_bloqueados >= 5: 
                    SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"🛡️ {total_bloqueados} Bloqueios (Pressão Real)", "rh": rh, "ra": ra, "favorito": "GOLS"})
                    golden_bet_ativada = True
        if not golden_bet_ativada and (70 <= tempo <= 75) and abs(gh - ga) <= 1:
            if total_chutes_gol >= 4: 
                SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"🔥 {total_chutes_gol} Chutes no Gol", "rh": rh, "ra": ra, "favorito": "GOLS"})
        if 55 <= tempo <= 75 and total_chutes <= 10 and (sog_h + sog_a) <= 2 and gh == ga:
            SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": f"👉 <b>FAZER:</b> Under Gols\n✅ Aposta: <b>Menos de {total_gols + 0.5} Gols</b>", "stats": "Jogo Travado", "rh": rh, "ra": ra, "favorito": "UNDER"})
        if 70 <= tempo <= 80 and total_chutes < 18:
            diff = gh - ga
            if (diff == 1 and ra < 2 and posse_h >= 45) or (diff == -1 and rh < 2 and posse_a >= 45):
                 SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito (Segurar)\n✅ Aposta: <b>Vitória Seca</b>", "stats": "Controle Total", "rh": rh, "ra": ra, "favorito": "FAVORITO"})
        if tempo <= 30 and total_gols >= 2: 
            SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": gerar_ordem_gol(total_gols), "stats": "Jogo Aberto", "rh": rh, "ra": ra, "favorito": "GOLS"})
        if total_gols == 0 and (tempo <= 10 and total_chutes >= 3): 
            SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": gerar_ordem_gol(0, "HT"), "stats": "Início Intenso", "rh": rh, "ra": ra, "favorito": "GOLS"})
        if tempo <= 60:
            if (gh <= ga and (rh >= 3 or sh_h >= 8)) or (ga <= gh and (ra >= 3 or sh_a >= 8)): 
                if post_h == 0: 
                    tag_blitz = "🟢 Blitz Casa" if gh <= ga else "🟢 Blitz Visitante"
                    SINAIS.append({"tag": tag_blitz, "ordem": gerar_ordem_gol(total_gols), "stats": "Pressão Limpa", "rh": rh, "ra": ra, "favorito": "GOLS"})
        if 15 <= tempo <= 25 and total_chutes >= 6 and (sog_h + sog_a) >= 3:
             SINAIS.append({"tag": "🏹 Tiroteio Elite", "ordem": gerar_ordem_gol(total_gols), "stats": "Muitos Chutes", "rh": rh, "ra": ra, "favorito": "GOLS"})
        if 20 <= tempo <= 35 and gh == 0 and ga == 0 and (sog_h + sog_a) >= 6:
            SINAIS.append({"tag": "🔥 O Catalisador (IA)", "ordem": "👉 <b>FAZER:</b> Over 0.5 HT\n✅ Aposta: <b>Mais de 0.5 Gols</b>", "stats": "Pontaria Alta", "rh": rh, "ra": ra, "favorito": "GOLS"})
        if tempo >= 80 and abs(gh - ga) <= 1:
            if total_fora <= 6 and ((rh >= 4) or (total_chutes_gol >= 5)): 
                SINAIS.append({"tag": "💎 Sniper Final", "ordem": "👉 <b>FAZER:</b> Over Gol Limite\n✅ Busque o Gol no Final", "stats": "Pontaria Ajustada", "rh": rh, "ra": ra, "favorito": "GOLS"})
        if 60 <= tempo <= 88 and abs(gh - ga) >= 3 and (total_chutes >= 14):
             SINAIS.append({"tag": "🔫 Lay Goleada", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": "Goleada Viva", "rh": rh, "ra": ra, "favorito": "GOLS"})
        if 60 <= tempo <= 75:
            diff_sog = sog_h - sog_a
            condicao_casa_reage = (diff_sog <= -3) and (rh >= 5) and (gh <= ga)
            condicao_fora_reage = (diff_sog >= 3) and (ra >= 5) and (ga <= gh)
            if condicao_casa_reage or condicao_fora_reage:
                SINAIS.append({"tag": "⚡ Oportunidade Relâmpago", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": "Caos Tático: Pressão do Azarão", "rh": rh, "ra": ra, "favorito": "GOLS"})
        return SINAIS
    except: return []

# --- FIM PARTE 3 ---
# ==============================================================================
# 5. FUNÇÕES DE SUPORTE, AUTOMAÇÃO E INTERFACE (O CORPO)
# ==============================================================================

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

def enviar_analise_estrategia(token, chat_ids):
    sugestao = criar_estrategia_nova_ia()
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    msg = f"🧪 <b>LABORATÓRIO DE ESTRATÉGIAS (IA)</b>\n\n{sugestao}"
    for cid in ids: enviar_telegram(token, cid, msg)

def enviar_relatorio_financeiro(token, chat_ids, cenario, lucro, roi, entradas):
    msg = f"💰 <b>RELATÓRIO FINANCEIRO</b>\n\n📊 <b>Cenário:</b> {cenario}\n💵 <b>Lucro Líquido:</b> R$ {lucro:.2f}\n📈 <b>ROI:</b> {roi:.1f}%\n🎟️ <b>Entradas:</b> {entradas}\n\n<i>Cálculo baseado na gestão configurada.</i>"
    enviar_telegram(token, chat_ids, msg)

def enviar_relatorio_bi(token, chat_ids):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    try:
        df = df.copy()
        df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
        df['Data_DT'] = pd.to_datetime(df['Data_Str'], errors='coerce')
        df = df.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
        agora = get_time_br().date()
        hoje = pd.to_datetime(agora)
        d_hoje = df[df['Data_DT'] == hoje]
        d_semana = df[df['Data_DT'] >= (hoje - timedelta(days=7))]
        d_mes = df[df['Data_DT'] >= (hoje - timedelta(days=30))]
        
        def get_placar_str(d_slice):
            if d_slice.empty: return "Sem dados"
            finalizados = d_slice[d_slice['Resultado'].isin(['✅ GREEN', '❌ RED'])]
            g = finalizados['Resultado'].str.contains('GREEN').sum()
            r = finalizados['Resultado'].str.contains('RED').sum()
            t = g + r
            wr = (g/t*100) if t > 0 else 0
            return f"<b>{g}G - {r}R</b> ({wr:.1f}%)"

        def get_ia_stats(d_slice):
            if 'Opiniao_IA' not in d_slice.columns: return "N/A"
            aprovadas = d_slice[d_slice['Opiniao_IA'] == 'Aprovado']
            return get_placar_str(aprovadas)

        top_strats_txt = ""
        try:
            df_closed = df[df['Resultado'].isin(['✅ GREEN', '❌ RED'])]
            if not df_closed.empty:
                ranking = df_closed.groupby('Estrategia')['Resultado'].apply(lambda x: (x.str.contains('GREEN').sum() / len(x) * 100)).sort_values(ascending=False).head(5)
                lista_top = []
                for strat, wr in ranking.items():
                    qtd = len(df_closed[df_closed['Estrategia'] == strat])
                    lista_top.append(f"▪️ {strat}: {wr:.0f}% ({qtd}j)")
                top_strats_txt = "\n".join(lista_top)
        except: top_strats_txt = "Dados insuficientes"

        insight_text = analisar_bi_com_ia()

        msg = f"""📈 <b>RELATÓRIO BI AVANÇADO</b>

📆 <b>DIÁRIO (HOJE):</b>
• Geral: {get_placar_str(d_hoje)}
• 🤖 IA Aprovados: {get_ia_stats(d_hoje)}

🗓 <b>SEMANAL (7 Dias):</b>
• Geral: {get_placar_str(d_semana)}
• 🤖 IA Aprovados: {get_ia_stats(d_semana)}

📅 <b>MENSAL (30 Dias):</b>
• Geral: {get_placar_str(d_mes)}

🏆 <b>TOP 5 ESTRATÉGIAS (Série Histórica):</b>
{top_strats_txt}

🧠 <b>INSIGHT IA (Análise do Dia):</b>
{insight_text}
"""
        enviar_telegram(token, chat_ids, msg)
    except Exception as e: enviar_telegram(token, chat_ids, f"📈 RELATÓRIO BI (Simplificado)\n\n{analisar_bi_com_ia()}")

def _worker_telegram(token, chat_id, msg):
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except: pass

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        t = threading.Thread(target=_worker_telegram, args=(token, cid, msg))
        t.daemon = True; t.start()

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

def enviar_multipla_matinal(token, chat_ids, api_key):
    if st.session_state.get('multipla_matinal_enviada'): return
    dados_json, mapa_nomes = gerar_multipla_matinal_ia(api_key)
    if not dados_json or "jogos" not in dados_json: return
    jogos = dados_json['jogos']
    prob = dados_json.get('probabilidade_combinada', '90')
    msg = "🚀 <b>MÚLTIPLA DE SEGURANÇA (IA)</b>\n"
    ids_compostos = []; nomes_compostos = []
    for idx, j in enumerate(jogos):
        icone = ["1️⃣", "2️⃣", "3️⃣"][idx] if idx < 3 else "👉"
        msg += f"\n{icone} <b>Jogo: {j['jogo']}</b>\n🎯 Seleção: Over 0.5 Gols\n📝 Motivo: {j['motivo']}\n"
        ids_compostos.append(str(j['fid'])); nomes_compostos.append(j['jogo'])
    msg += f"\n⚠️ <b>Conclusão:</b> Probabilidade combinada de {prob}%."
    enviar_telegram(token, chat_ids, msg)
    multipla_obj = {"id_unico": f"MULT_{'_'.join(ids_compostos)}", "tipo": "MATINAL", "fids": ids_compostos, "nomes": nomes_compostos, "status": "Pendente", "data": get_time_br().strftime('%Y-%m-%d')}
    if 'multiplas_pendentes' not in st.session_state: st.session_state['multiplas_pendentes'] = []
    st.session_state['multiplas_pendentes'].append(multipla_obj)
    st.session_state['multipla_matinal_enviada'] = True

def enviar_alerta_alternativos(token, chat_ids, api_key):
    if st.session_state.get('alternativos_enviado'): return
    sinais = gerar_analise_mercados_alternativos_ia(api_key)
    if not sinais: return
    for s in sinais:
        msg = f"<b>{s['titulo']}</b>\n\n⚽ <b>{s['jogo']}</b>\n\n🔎 <b>Análise:</b>\n{s['destaque']}\n\n🎯 <b>INDICAÇÃO:</b> {s['indicacao']}"
        if s['tipo'] == 'GOLEIRO': msg += "\n⚠️ <i>Regra: Aposte no 'Goleiro do Time', não no nome do jogador.</i>"
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
        except: pass
    if len(candidatos) >= 2:
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

def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br()
    # 1. Sniper Matinal
    if 8 <= agora.hour < 11:
        if not st.session_state['matinal_enviado']:
            insights = gerar_insights_matinais_ia(api_key)
            if insights and "Sem jogos" not in insights:
                ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
                msg_final = f"🌅 <b>SNIPER MATINAL (IA + DADOS)</b>\n\n{insights}"
                for cid in ids: enviar_telegram(token, cid, msg_final)
                salvar_snipers_do_texto(insights)
                st.session_state['matinal_enviado'] = True
        if st.session_state['matinal_enviado'] and not st.session_state.get('multipla_matinal_enviada', False):
            time.sleep(5); enviar_multipla_matinal(token, chat_ids, api_key)
        if st.session_state['matinal_enviado'] and st.session_state['multipla_matinal_enviada'] and not st.session_state.get('alternativos_enviado', False):
            time.sleep(5); enviar_alerta_alternativos(token, chat_ids, api_key)
        if agora.hour >= 10 and not st.session_state.get('alavancagem_enviada', False):
            time.sleep(5); enviar_alavancagem(token, chat_ids, api_key)
    
    # 5. [NOVO] TRADING PRÉ-LIVE (DROP ODDS) - COM JANELA ESTENDIDA ATÉ 13:30
    # Se for meio-dia ou se for 13h (até 30min) E ainda não enviou...
    faixa_12h = (agora.hour == 12 or (agora.hour == 13 and agora.minute <= 30))
    # Se for 16h (até 30min) E ainda não enviou...
    faixa_16h = (agora.hour == 16 and agora.minute <= 30)

    # Verifica o trigger da faixa das 12h
    if faixa_12h and not st.session_state.get('drop_enviado_12', False):
        drops = scanner_drop_odds_pre_live(api_key)
        if drops:
            for d in drops:
                msg = f"💰 <b>ESTRATÉGIA CASHOUT (DROP ODDS)</b>\n\n⚽ <b>{d['jogo']}</b>\n🏆 {d['liga']} | ⏰ {d['hora']}\n\n📉 <b>DESAJUSTE:</b>\n• Bet365: <b>@{d['odd_b365']:.2f}</b>\n• Pinnacle: <b>@{d['odd_pinnacle']:.2f}</b>\n• Drop: <b>{d['valor']:.1f}%</b>\n\n⚙️ <b>AÇÃO:</b>\n1️⃣ Compre vitória do <b>{d['lado']}</b>\n2️⃣ <b>+ BANKER:</b> Adicione 'Under 7.5 Gols'\n3️⃣ <b>SAÍDA:</b> Cashout ao igualar Pinnacle."
                enviar_telegram(token, chat_ids, msg)
                item_drop = {"FID": f"DROP_{d['fid']}", "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": "Trading Pré-Live", "Jogo": d['jogo'], "Placar_Sinal": f"Entrada: @{d['odd_b365']}", "Estrategia": "Drop Odds Cashout", "Resultado": "Pendente", "Opiniao_IA": "Aprovado", "Probabilidade": "Técnica"}
                adicionar_historico(item_drop)
        st.session_state['drop_enviado_12'] = True # Marca como enviado para não repetir

    # Verifica o trigger da faixa das 16h
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
            gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
            st_short = jogo_api['fixture']['status']['short']
            try: ph, pa = map(int, s['Placar_Sinal'].split('x'))
            except: ph, pa = 0, 0
            key_sinal = gerar_chave_universal(fid, strat, "SINAL")
            key_green = gerar_chave_universal(fid, strat, "GREEN")
            key_red = gerar_chave_universal(fid, strat, "RED")
            deve_enviar = (key_sinal in st.session_state.get('alertas_enviados', set()))
            res_final = None
            if (gh + ga) > (ph + pa):
                if "Under" not in strat and "Morno" not in strat: res_final = "✅ GREEN"
                elif "Morno" in strat: res_final = "❌ RED" 
            if not res_final and st_short in ['FT', 'AET', 'PEN', 'ABD']:
                 if "Morno" in strat or "Under" in strat: res_final = "✅ GREEN"
                 elif "Vovô" in strat or "Back" in strat: 
                     if (ph > pa and gh > ga) or (pa > ph and ga > gh): res_final = "✅ GREEN"
                     else: res_final = "❌ RED"
                 else: res_final = "❌ RED" 
            if res_final:
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
    snipers = [s for s in hist if "Sniper" in s['Estrategia'] and s['Resultado'] == "Pendente"]
    if not snipers: return
    updates = []
    ids_live = {str(j['fixture']['id']): j for j in jogos_live} 
    for s in snipers:
        if "SNIPER_" in str(s['FID']): pass
        else:
            fid = str(s['FID'])
            jogo = ids_live.get(fid)
            if not jogo:
                try:
                    res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                    if res.get('response'): jogo = res['response'][0]
                except: pass
            if jogo:
                status = jogo['fixture']['status']['short']
                if status in ['FT', 'AET', 'PEN']:
                    gh = jogo['goals']['home'] or 0; ga = jogo['goals']['away'] or 0
                    res = '✅ GREEN' if (gh+ga) > 0 else '❌ RED'
                    s['Resultado'] = res
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
    if agora.hour == 23 and agora.minute >= 30 and not st.session_state['bi_enviado']:
        enviar_relatorio_bi(token, chat_ids); st.session_state['bi_enviado'] = True
    if agora.hour == 23 and agora.minute >= 40 and not st.session_state['financeiro_enviado']:
        analise_fin = analisar_financeiro_com_ia(stake_padrao, st.session_state.get('banca_inicial', 100))
        msg_fin = f"💰 <b>CONSULTORIA FINANCEIRA</b>\n\n{analise_fin}"
        enviar_telegram(token, chat_ids, msg_fin); st.session_state['financeiro_enviado'] = True
    if agora.hour == 23 and agora.minute >= 55 and not st.session_state['bigdata_enviado']:
        enviar_analise_estrategia(token, chat_ids); st.session_state['bigdata_enviado'] = True

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
        
        # [BOTÃO MANUAL DE TRADING]
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

        # Chamada das Automações
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

                # --- BLOCO DE ENVIO ATUALIZADO (LAYOUT EXECUTIVO + WINRATE CORRIGIDO) ---
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
                            wr = (greens/len(f_h))*100
                            txt_pessoal = f"Winrate Pessoal com {home}: {wr:.0f}%"

                    txt_history = ""
                    if dados_50:
                        txt_history = (f"API (50j): Casa(Over1.5: {dados_50['home']['over15_ft']}%) | Fora(Over1.5: {dados_50['away']['over15_ft']}%)")
                    
                    extra_ctx = f"""
                    FONTE 1 (API/SofaScore): {txt_history} | Rating: {nota_home}x{nota_away}
                    FONTE 2 (BIG DATA): {txt_bigdata}
                    FONTE 3 (HISTÓRICO PESSOAL): {txt_pessoal}
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
                        if odd_val >= 1.80:
                            destaque_odd = "\n💎 <b>SUPER ODD DETECTADA! (EV+)</b>"
                            st.session_state['alertas_enviados'].add(uid_super)
                        
                        opiniao_txt = ""; prob_txt = "..."; opiniao_db = "Neutro"
                        
                        if IA_ATIVADA:
                            try:
                                time.sleep(0.2) 
                                dados_ia = {'jogo': f"{home} x {away}", 'placar': placar, 'tempo': f"{tempo}'"}
                                time_fav_ia = s.get('favorito', '')
                                opiniao_txt, prob_txt = consultar_ia_gemini(dados_ia, s['tag'], stats, rh, ra, extra_context=extra_ctx, time_favoravel=time_fav_ia)
                                
                                if "aprovado" in opiniao_txt.lower(): opiniao_db = "Aprovado"
                                elif "arriscado" in opiniao_txt.lower(): opiniao_db = "Arriscado"
                                else: opiniao_db = "Neutro"
                            except: pass
                        
                        item = {
                            "FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'),
                            "Liga": j['league']['name'], "Jogo": f"{home} x {away} ({placar})", "Placar_Sinal": placar,
                            "Estrategia": s['tag'], "Resultado": "Pendente",
                            "HomeID": str(j['teams']['home']['id']) if lid in ids_safe else "", "AwayID": str(j['teams']['away']['id']) if lid in ids_safe else "",
                            "Odd": odd_atual_str, "Odd_Atualizada": "", "Opiniao_IA": opiniao_db, "Probabilidade": prob_txt 
                        }
                        
                        if adicionar_historico(item):
                            try:
                                txt_winrate_historico = ""
                                if txt_pessoal != "Neutro": txt_winrate_historico = f" | 👤 {txt_pessoal}"

                                # 1. Cabeçalho com Winrate (Prioridade: Estratégia > Pessoal > API)
                                header_winrate = ""
                                # Tenta pegar o Winrate da Estratégia (Global)
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
                                if not header_winrate and dados_50: 
                                    header_winrate = f" | 📊 <b>API: {dados_50['home']['over15_ft']}%</b>"

                                texto_momento = "Morno 🧊"
                                if rh > ra: texto_momento = "Pressão Casa 🔥"
                                elif ra > rh: texto_momento = "Pressão Visitante 🔥"
                                elif rh > 2 or ra > 2: texto_momento = "Jogo Aberto ⚡"

                                linha_bd = ""
                                if "MANDANTE" in txt_bigdata: linha_bd = f"• 💾 <b>Big Data:</b> Tendência confirmada.\n"

                                msg = f"🚨 <b>SINAL {s['tag'].upper()}</b>{header_winrate}\n"
                                msg += f"🏆 {liga_safe}\n"
                                msg += f"⚽ <b>{home_safe} 🆚 {away_safe}</b>\n"
                                msg += f"⏰ {tempo}' min | 🥅 Placar: {placar}\n\n"
                                msg += f"{s['ordem']}\n"
                                if destaque_odd: msg += f"{destaque_odd}\n"
                                msg += "──────────────\n" 
                                msg += f"📊 <b>Raio-X do Momento:</b>\n"
                                msg += f"• 🔥 <b>Ataque:</b> {s.get('stats', 'Pressão')}\n"
                                msg += f"• 🌡️ <b>Ritmo:</b> {texto_momento}\n" 
                                msg += linha_bd
                                msg += "\n" 
                                msg += f"{opiniao_txt}" 
                                
                                sent_status = False
                                if opiniao_db == "Aprovado":
                                    enviar_telegram(safe_token, safe_chat, msg)
                                    sent_status = True
                                    st.toast(f"✅ Sinal Enviado: {s['tag']}")
                                elif opiniao_db == "Arriscado":
                                    msg += "\n👀 <i>Obs: Risco moderado detectado.</i>"
                                    enviar_telegram(safe_token, safe_chat, msg)
                                    sent_status = True
                                    st.toast(f"⚠️ Sinal Arriscado Enviado: {s['tag']}")
                                else: st.toast(f"🛑 Sinal Retido: {s['tag']}")

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
