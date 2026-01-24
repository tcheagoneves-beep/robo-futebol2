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
import hashlib 

# ==============================================================================
# 1. CONFIGURAÇÃO INICIAL E CSS (SEU LAYOUT ORIGINAL)
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
if 'last_run' not in st.session_state: st.session_state['last_run'] = 0 # Variável crítica para o Timer

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

COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID', 'Odd', 'Odd_Atualizada', 'Opiniao_IA']
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
    "👴 Estratégia do Vovô": "Back Favorito (Segurança)"
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
    "👴 Estratégia do Vovô": {"min": 1.05, "max": 1.25}
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

def gerar_barra_pressao(rh, ra):
    return "" 

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
    """Calcula estatísticas para os contadores das abas e Dashboard"""
    if df_raw.empty: return 0, 0, 0, 0.0
    try:
        # Remove duplicatas para contagem correta
        df_clean = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
        greens = len(df_clean[df_clean['Resultado'].str.contains('GREEN', na=False)])
        reds = len(df_clean[df_clean['Resultado'].str.contains('RED', na=False)])
        total = len(df_clean)
        winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
        return total, greens, reds, winrate
    except: return 0, 0, 0, 0.0

# --- GERENCIAMENTO DE PLANILHAS E DADOS ---

def carregar_aba(nome_aba, colunas_esperadas):
    chave_memoria = ""
    if nome_aba == "Historico": chave_memoria = 'historico_full'
    elif nome_aba == "Seguras": chave_memoria = 'df_safe'
    elif nome_aba == "Obs": chave_memoria = 'df_vip'
    elif nome_aba == "Blacklist": chave_memoria = 'df_black'
    try:
        df = conn.read(worksheet=nome_aba, ttl=0)
        if not df.empty:
            for col in colunas_esperadas:
                if col not in df.columns:
                    df[col] = "1.20" if col == 'Odd' else ""
            return df.fillna("").astype(str)
        return pd.DataFrame(columns=colunas_esperadas)
    except Exception as e:
        if chave_memoria and chave_memoria in st.session_state:
            df_ram = st.session_state[chave_memoria]
            if not df_ram.empty: return df_ram
        st.session_state['BLOQUEAR_SALVAMENTO'] = True
        return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df_para_salvar):
    """
    SALVAMENTO INTELIGENTE (HASH):
    Evita que o robô trave salvando dados repetidos.
    """
    if nome_aba in ["Historico", "Seguras", "Obs"] and df_para_salvar.empty: return False
    if st.session_state.get('BLOQUEAR_SALVAMENTO', False):
        st.session_state['precisa_salvar'] = True 
        return False
    
    try:
        # Cria um "DNA" dos dados atuais
        data_hash = hashlib.md5(pd.util.hash_pandas_object(df_para_salvar, index=True).values).hexdigest()
        chave_hash = f'hash_last_save_{nome_aba}'
        last_hash = st.session_state.get(chave_hash, '')

        # Se o DNA for igual ao último salvo, não gasta tempo enviando
        if data_hash == last_hash:
            if nome_aba == "Historico": st.session_state['precisa_salvar'] = False
            return True 

        # Se mudou, salva
        conn.update(worksheet=nome_aba, data=df_para_salvar)
        st.session_state[chave_hash] = data_hash
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
            'fid': fid,
            'data_hora': get_time_br().strftime('%Y-%m-%d %H:%M'),
            'liga': sanitize(jogo_api['league']['name']),
            'home_id': str(jogo_api['teams']['home']['id']),
            'away_id': str(jogo_api['teams']['away']['id']),
            'jogo': f"{sanitize(jogo_api['teams']['home']['name'])} x {sanitize(jogo_api['teams']['away']['name'])}",
            'placar_final': f"{jogo_api['goals']['home']}x{jogo_api['goals']['away']}",
            'rating_home': str(rate_h),
            'rating_away': str(rate_a),
            'estatisticas': {
                'chutes_total': gv(s1, 'Total Shots') + gv(s2, 'Total Shots'),
                'chutes_gol': gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal'),
                'chutes_area': gv(s1, 'Shots insidebox') + gv(s2, 'Shots insidebox'),
                'escanteios_total': gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks'),
                'escanteios_casa': gv(s1, 'Corner Kicks'),
                'escanteios_fora': gv(s2, 'Corner Kicks'),
                'faltas_total': gv(s1, 'Fouls') + gv(s2, 'Fouls'),
                'cartoes_amarelos': gv(s1, 'Yellow Cards') + gv(s2, 'Yellow Cards'),
                'cartoes_vermelhos': gv(s1, 'Red Cards') + gv(s2, 'Red Cards'),
                'posse_casa': str(gv(s1, 'Ball Possession')),
                'ataques_perigosos': gv(s1, 'Dangerous Attacks') + gv(s2, 'Dangerous Attacks'),
                'impedimentos': gv(s1, 'Offsides') + gv(s2, 'Offsides'),
                'passes_pct_casa': str(gv(s1, 'Passes %')).replace('%',''),
                'passes_pct_fora': str(gv(s2, 'Passes %')).replace('%','')
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
            if not jogos: return {"over05_ht": 0, "over15_ft": 0, "ambas_marcam": 0}
            stats = {"qtd": len(jogos), "over05_ht": 0, "over15_ft": 0, "ambas_marcam": 0}
            for j in jogos:
                gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
                g_ht_h = j['score']['halftime']['home'] or 0; g_ht_a = j['score']['halftime']['away'] or 0
                if (g_ht_h + g_ht_a) > 0: stats["over05_ht"] += 1
                if (gh + ga) >= 2: stats["over15_ft"] += 1
                if gh > 0 and ga > 0: stats["ambas_marcam"] += 1
            return {k: int((v / stats["qtd"]) * 100) if k != "qtd" else v for k, v in stats.items()}
        return {"home": get_stats_50(home_id), "away": get_stats_50(away_id)}
    except: return None

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

@st.cache_data(ttl=120) 
def buscar_agenda_cached(api_key, date_str):
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        return requests.get(url, headers={"x-apisports-key": api_key}, params={"date": date_str, "timezone": "America/Sao_Paulo"}).json().get('response', [])
    except: return []

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
        valor = float(odd_registro)
        if valor <= 1.15: 
            limites = MAPA_ODDS_TEORICAS.get(estrategia, {"min": 1.40, "max": 1.60})
            return (limites['min'] + limites['max']) / 2
        return valor
    except: return 1.50

# ==============================================================================
# 3. LÓGICA DE ESTRATÉGIAS (O CÉREBRO) E MÓDULOS IA
# ==============================================================================

def consultar_ia_gemini(dados_jogo, estrategia, stats_raw, rh, ra, extra_context="", time_favoravel=""):
    if not IA_ATIVADA: return "", "N/A"
    try:
        s1 = stats_raw[0]['statistics']; s2 = stats_raw[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        chutes_totais = gv(s1, 'Total Shots') + gv(s2, 'Total Shots')
        tempo_str = str(dados_jogo.get('tempo', '0')).replace("'", "")
        tempo = int(tempo_str) if tempo_str.isdigit() else 0
        if tempo > 20 and chutes_totais == 0: return "\n🤖 <b>IA:</b> ⚠️ <b>Ignorado</b> (API Delay).", "N/A"
    except: return "", "N/A"

    chutes_area_casa = gv(s1, 'Shots insidebox')
    chutes_area_fora = gv(s2, 'Shots insidebox')
    escanteios = gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks')
    posse_casa = str(gv(s1, 'Ball Possession')).replace('%', '')
    dados_ricos = extrair_dados_completos(stats_raw)
    
    eh_under = "Morno" in estrategia or "Under" in estrategia
    objetivo = "O OBJETIVO É QUE NÃO SAIA GOL (UNDER)." if eh_under else "O OBJETIVO É QUE SAIA GOL (OVER)."
    
    criterio_aprovacao = ""
    if eh_under:
        criterio_aprovacao = """CRITÉRIO PARA UNDER (JOGO MORNO):
        - APROVADO se: Jogo travado, poucos chutes na área, ataques inofensivos.
        - ARRISCADO se: Jogo aberto, lá e cá, pressão forte ou histórico de muitos gols."""
    else:
        criterio_aprovacao = """CRITÉRIO PARA OVER (GOLS):
        - APROVADO se: Pressão forte, chutes na área, goleiro trabalhando.
        - ARRISCADO se: Jogo lento, posse de bola inútil no meio campo."""

    prompt = f"""
    Atue como um TRADER ESPORTIVO SÊNIOR (Mentalidade: EV+ MATEMÁTICO).
    Analise friamente os dados para validar a entrada.
    ESTRATÉGIA: {estrategia}. ⚠️ {objetivo}
    DADOS: {dados_jogo['jogo']} | Placar: {dados_jogo['placar']} | Tempo: {dados_jogo.get('tempo')}
    STATS: Pressão {rh}x{ra}, Chutes Área {chutes_area_casa}x{chutes_area_fora}, Cantos {escanteios}, Posse {posse_casa}%.
    CONTEXTO: {extra_context}. {dados_ricos}
    {criterio_aprovacao}
    DECISÃO BINÁRIA (Seja direto):
    FORMATO: Aprovado/Arriscado - [Explicação curta] | PROB: [0-100]%
    """

    try:
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(temperature=0.2), request_options={"timeout": 10})
        st.session_state['gemini_usage']['used'] += 1
        txt = response.text.strip().replace("**", "").replace("*", "")
        prob = re.search(r'PROB:\s*(\d+)%', txt).group(1)+'%' if re.search(r'PROB:\s*(\d+)%', txt) else "N/A"
        veredicto = "Aprovado" if "aprovado" in txt.lower()[:20] else "Arriscado"
        motivo = txt.replace("Aprovado", "").replace("Arriscado", "").split('|')[0].replace("-", "", 1).strip()
        emoji = "✅" if veredicto == "Aprovado" else "⚠️"
        return f"\n🤖 <b>ANÁLISE QUÂNTICA:</b>\n{emoji} <b>{veredicto.upper()}</b> - <i>{motivo}</i>", prob
    except: return "", "N/A"

def analisar_bi_com_ia():
    if not IA_ATIVADA: return "IA Desconectada."
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados."
    try:
        hoje_str = get_time_br().strftime('%Y-%m-%d')
        df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
        df_hoje = df[df['Data_Str'] == hoje_str]
        if df_hoje.empty: return "Sem sinais hoje."
        df_f = df_hoje[df_hoje['Resultado'].isin(['✅ GREEN', '❌ RED'])]
        total = len(df_f); greens = len(df_f[df_f['Resultado'].str.contains('GREEN')])
        resumo = df_f.groupby('Estrategia')['Resultado'].apply(lambda x: f"{(x.str.contains('GREEN').sum()/len(x)*100):.1f}%").to_dict()
        prompt = f"Analise o dia ({hoje_str}): Total: {total}, Greens: {greens}. Estratégias: {json.dumps(resumo, ensure_ascii=False)}. Resumo curto."
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro BI: {e}"

def analisar_financeiro_com_ia(stake, banca):
    if not IA_ATIVADA: return "IA Desconectada."
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados."
    try:
        hoje_str = get_time_br().strftime('%Y-%m-%d')
        df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
        df_hoje = df[df['Data_Str'] == hoje_str].copy()
        if df_hoje.empty: return "Sem operações hoje."
        lucro_total = 0.0; investido = 0.0
        for _, row in df_hoje.iterrows():
            res = str(row['Resultado'])
            odd_final = obter_odd_final_para_calculo(row['Odd'], row['Estrategia'])
            if 'GREEN' in res: lucro_total += (stake * odd_final) - stake; investido += stake
            elif 'RED' in res: lucro_total -= stake; investido += stake
        roi = (lucro_total / investido * 100) if investido > 0 else 0
        prompt_fin = f"Gestor Financeiro. Dia: Banca Ini: {banca} | Fim: {banca+lucro_total}. Lucro: {lucro_total}. ROI: {roi}%. Dê um conselho curto."
        response = model_ia.generate_content(prompt_fin)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro Fin: {e}"

def criar_estrategia_nova_ia():
    if not IA_ATIVADA: return "IA Desconectada."
    if not db_firestore: return "Firebase Offline."
    try:
        docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(150).stream()
        data_raw = [d.to_dict() for d in docs]
        if len(data_raw) < 10: return "Coletando dados... (Mínimo 10 jogos no BigData)"
        df = pd.DataFrame(data_raw)
        historico_para_ia = ""
        for _, row in df.head(100).iterrows():
            historico_para_ia += f"Jogo: {row['jogo']} | Placar: {row['placar_final']} | Stats: {json.dumps(row.get('estatisticas', {}))}\n"
        prompt = f"Analise esse Big Data de {len(df)} jogos. Encontre um padrão estatístico oculto e crie uma estratégia nova."
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro Big Data: {e}"

def gerar_insights_matinais_ia(api_key):
    if not IA_ATIVADA: return "IA Offline."
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        LIGAS_TOP = [39, 140, 78, 135, 61, 71, 72, 2, 3] 
        jogos_top = [j for j in jogos if j['league']['id'] in LIGAS_TOP]
        if not jogos_top: return "Sem jogos Elite hoje."
        jogos_selecionados = jogos_top[:3]
        dados_para_ia = ""
        for j in jogos_selecionados:
            home_nm = j['teams']['home']['name']; away_nm = j['teams']['away']['name']
            hid = j['teams']['home']['id']; aid = j['teams']['away']['id']
            stats_hist = analisar_tendencia_50_jogos(api_key, hid, aid)
            rating_h = buscar_rating_inteligente(api_key, hid); rating_a = buscar_rating_inteligente(api_key, aid)
            dados_para_ia += f"JOGO: {home_nm} x {away_nm} | Over 1.5: {stats_hist['home']['over15_ft']}%/{stats_hist['away']['over15_ft']}% | Ratings: {rating_h}/{rating_a}\n"
        prompt = f"Atue como SNIPER MATINAL. Analise: {dados_para_ia}. Dê 3 palpites com explicação."
        resp = model_ia.generate_content(prompt); st.session_state['gemini_usage']['used'] += 1
        return resp.text
    except: return "Erro Matinal."

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
        try: posse_h_val = next((x['value'] for x in stats_h if x['type']=='Ball Possession'), "50%"); posse_h = int(str(posse_h_val).replace('%', '')); posse_a = 100 - posse_h
        except: posse_h = 50; posse_a = 50
        arame_liso_casa = (posse_h >= 65 and sog_h < 2); arame_liso_fora = (posse_a >= 65 and sog_a < 2)
        rh, ra = momentum(j['fixture']['id'], sog_h, sog_a)
        gh = j['goals']['home']; ga = j['goals']['away']; total_gols = gh + ga; total_chutes = sh_h + sh_a
        
        def go(ga, t="Over"): 
            l = ga + 0.5
            if t == "Over": return f"👉 <b>FAZER:</b> Entrar em GOLS (Over)\n✅ Aposta: <b>Mais de {l} Gols</b>"
            if t == "HT": return f"👉 <b>FAZER:</b> Entrar em GOLS 1º TEMPO\n✅ Aposta: <b>Mais de 0.5 Gols HT</b>"
            if t == "Limite": return f"👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de {ga + 1.0} Gols</b> (Asiático)"
            return "Apostar."

        SINAIS = []; golden = False
        
        # ESTRATÉGIAS
        if 65 <= tempo <= 75:
            p_c = (rh >= 3 and sog_h >= 4) and not arame_liso_casa; p_f = (ra >= 3 and sog_a >= 4) and not arame_liso_fora
            if (p_c and sh_h > sh_a) or (p_f and sh_a > sh_h):
                 if total_gols >= 1 or total_chutes >= 18: SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": go(total_gols, "Limite"), "stats": "🔥 Pressão Favorito + Finalizações", "rh": rh, "ra": ra, "favorito": "GOLS"}); golden = True
        
        if not golden and (70 <= tempo <= 75) and abs(gh - ga) <= 1:
            if total_chutes >= 22 and (not arame_liso_casa and not arame_liso_fora): SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": go(total_gols, "Limite"), "stats": f"🔥 {total_chutes} Chutes Totais", "rh": rh, "ra": ra, "favorito": "GOLS"})
        
        if 55 <= tempo <= 75:
             if total_chutes <= 10 and (sog_h + sog_a) <= 2 and gh == ga: SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": f"👉 <b>FAZER:</b> Under Gols (Segurar)\n✅ Aposta: <b>Menos de {total_gols + 0.5} Gols</b>", "stats": f"Jogo Travado ({total_chutes} chutes)", "rh": rh, "ra": ra, "favorito": "UNDER"})
        
        if 70 <= tempo <= 80 and total_chutes < 18:
            d = gh - ga
            if d == 1 and ra < 2 and posse_h >= 45 and sog_a < 2: SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito\n✅ Aposta: <b>Vitória do CASA</b>", "stats": f"Controle Total", "rh": rh, "ra": ra, "favorito": "CASA"})
            elif d == -1 and rh < 2 and posse_a >= 45 and sog_h < 2: SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito\n✅ Aposta: <b>Vitória do VISITANTE</b>", "stats": f"Controle Total", "rh": rh, "ra": ra, "favorito": "VISITANTE"})
        
        if tempo <= 30 and total_gols >= 2: SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": go(total_gols), "stats": f"Jogo Aberto", "rh": rh, "ra": ra, "favorito": "GOLS"})
        if total_gols == 0 and (tempo <= 10 and total_chutes >= 3): SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": go(0, "HT"), "stats": "Início Intenso", "rh": rh, "ra": ra, "favorito": "GOLS"})
        
        if tempo <= 60:
            if gh <= ga and (rh >= 3 or (sh_h >= 8 and sog_h >= 3)) and not arame_liso_casa: SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": go(total_gols), "stats": "Pressão Casa", "rh": rh, "ra": ra, "favorito": "GOLS"})
            if ga <= gh and (ra >= 3 or (sh_a >= 8 and sog_a >= 3)) and not arame_liso_fora: SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": go(total_gols), "stats": "Pressão Visitante", "rh": rh, "ra": ra, "favorito": "GOLS"})
        
        if tempo >= 80 and abs(gh - ga) <= 1:
            if (rh >= 4 and sh_h >= 14) or (ra >= 4 and sh_a >= 14) or (ck_h + ck_a) >= 10: SINAIS.append({"tag": "💎 Sniper Final", "ordem": "👉 <b>FAZER:</b> Over Gol Limite (Asiático)", "stats": "Pressão Final", "rh": rh, "ra": ra, "favorito": "GOLS"})
        
        return SINAIS
    except: return []
# ==============================================================================
# 4. TELEGRAM, RESULTADOS E LOOP PRINCIPAL
# ==============================================================================

def _worker_telegram(token, chat_id, msg):
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except: pass

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        t = threading.Thread(target=_worker_telegram, args=(token, cid, msg))
        t.daemon = True; t.start()

def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
    st_short = jogo_api['fixture']['status']['short']
    fid = sinal['FID']; strat = sinal['Estrategia']
    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: ph, pa = 0, 0
    
    key_orig = gerar_chave_universal(fid, strat, "SINAL")
    key_green = gerar_chave_universal(fid, strat, "GREEN")
    key_red = gerar_chave_universal(fid, strat, "RED")
    
    deve_enviar_msg = (key_orig in st.session_state.get('alertas_enviados', set()))
    if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
    
    # 1. GOL (Bola na Rede)
    if (gh + ga) > (ph + pa):
        if any(x in strat for x in ["Vovô", "Back Favorito"]): return False
        
        # Lógica Correta para UNDER/MORNO
        if "Morno" in strat or "Under" in strat:
            if (gh+ga) >= 2: # Se for Under e sair muito gol = RED
                sinal['Resultado'] = '❌ RED'
                if deve_enviar_msg and key_red not in st.session_state['alertas_enviados']:
                    enviar_telegram(token, chats, f"❌ <b>RED | OVER 1.5 BATIDO</b>\n⚽ {sinal['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {strat}")
                    st.session_state['alertas_enviados'].add(key_red)
                st.session_state['precisa_salvar'] = True; return True
        else:
            # OVER (Padrão) -> Gol = GREEN
            sinal['Resultado'] = '✅ GREEN'
            if deve_enviar_msg and key_green not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📈 Placar: <b>{gh}x{ga}</b>\n🎯 {strat}")
                st.session_state['alertas_enviados'].add(key_green)
            st.session_state['precisa_salvar'] = True; return True

    # 2. Fim de Jogo (FT)
    if st_short in ['FT', 'AET', 'PEN', 'ABD']:
        # Match Odds
        if "Vovô" in strat or "Back" in strat:
            res_final = '❌ RED'
            if (ph > pa and gh > ga) or (pa > ph and ga > gh): res_final = '✅ GREEN'
            sinal['Resultado'] = res_final
            if deve_enviar_msg: enviar_telegram(token, chats, f"{res_final} | FINALIZADO\n⚽ {sinal['Jogo']}")
            st.session_state['precisa_salvar'] = True; return True
        
        # Se for Under/Morno e acabou sem estourar = GREEN
        if ("Morno" in strat or "Under" in strat):
             sinal['Resultado'] = '✅ GREEN'
             if deve_enviar_msg and key_green not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"✅ <b>GREEN | FINALIZADO</b>\n⚽ {sinal['Jogo']}\n🎯 {strat}")
                st.session_state['alertas_enviados'].add(key_green)
             st.session_state['precisa_salvar'] = True; return True
        
        # Over que não bateu até o fim = RED
        sinal['Resultado'] = '❌ RED'
        if deve_enviar_msg and key_red not in st.session_state['alertas_enviados']:
            enviar_telegram(token, chats, f"❌ <b>RED | ENCERRADO</b>\n⚽ {sinal['Jogo']}\n🎯 {strat}")
            st.session_state['alertas_enviados'].add(key_red)
        st.session_state['precisa_salvar'] = True; return True
    return False

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    hist = st.session_state.get('historico_sinais', [])
    pendentes = [s for s in hist if s['Resultado'] == 'Pendente' and "Sniper" not in s['Estrategia']]
    if not pendentes: return
    mapa_live = {j['fixture']['id']: j for j in jogos_live}; updates = []
    for s in pendentes:
        j_api = mapa_live.get(int(clean_fid(s.get('FID', 0))))
        if j_api and processar_resultado(s, j_api, token, chats): updates.append(s)
    if updates: atualizar_historico_ram(updates)

def conferir_resultados_sniper(jogos_live, api_key):
    hist = st.session_state.get('historico_sinais', [])
    snipers = [s for s in hist if "Sniper" in s['Estrategia'] and s['Resultado'] == "Pendente"]
    if not snipers: return
    updates = []
    ids_live = {str(j['fixture']['id']): j for j in jogos_live} 
    for s in snipers:
        jogo = ids_live.get(str(s['FID']))
        if jogo and jogo['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
            gh = jogo['goals']['home'] or 0; ga = jogo['goals']['away'] or 0
            try:
                p = s['Placar_Sinal'].split('x'); gs = int(p[0]) + int(p[1])
                res = '✅ GREEN' if (gh + ga) > gs else '❌ RED'
                s['Resultado'] = res; updates.append(s)
                if gerar_chave_universal(s['FID'], s['Estrategia'], "SINAL") in st.session_state.get('alertas_enviados', set()):
                    enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], f"{res} SNIPER: {s['Jogo']}")
                st.session_state['precisa_salvar'] = True
            except: pass
    if updates: atualizar_historico_ram(updates)

def verificar_var_rollback(jogos_live, token, chats):
    if 'var_avisado_cache' not in st.session_state: st.session_state['var_avisado_cache'] = set()
    greens = [s for s in st.session_state.get('historico_sinais', []) if 'GREEN' in str(s['Resultado'])]
    for s in greens:
        j = next((j for j in jogos_live if j['fixture']['id'] == int(clean_fid(s['FID']))), None)
        if j:
            gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
            try:
                ph, pa = map(int, s['Placar_Sinal'].split('x'))
                if (gh + ga) <= (ph + pa):
                    s['Resultado'] = 'Pendente'; st.session_state['precisa_salvar'] = True
                    if gerar_chave_universal(s['FID'], s['Estrategia'], "SINAL") in st.session_state.get('alertas_enviados', set()):
                        enviar_telegram(token, chats, f"⚠️ VAR GOL ANULADO: {s['Jogo']}")
            except: pass

def enviar_relatorio_bi(token, chat_ids):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    try:
        df = df.copy()
        df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
        df['Data_DT'] = pd.to_datetime(df['Data_Str'], errors='coerce')
        df = df.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
        hoje = pd.to_datetime(get_time_br().date())
        d_hoje = df[df['Data_DT'] == hoje]
        
        def fmt_placar(d):
            if d.empty: return "0G - 0R (0%)"
            g = d['Resultado'].str.contains('GREEN', na=False).sum()
            r = d['Resultado'].str.contains('RED', na=False).sum()
            t = g + r
            wr = (g/t*100) if t > 0 else 0
            return f"{g}G - {r}R ({wr:.0f}%)"
        
        insight_text = analisar_bi_com_ia()
        msg_texto = f"📈 <b>RELATÓRIO BI</b>\n📆 <b>HOJE:</b> {fmt_placar(d_hoje)}\n\n🧠 <b>INSIGHT IA:</b>\n{insight_text}"
        enviar_telegram(token, chat_ids, msg_texto)
    except Exception as e: st.error(f"Erro BI: {e}")

def verificar_automacao_bi(token, chat_ids, stake_padrao):
    agora = get_time_br()
    hoje_str = agora.strftime('%Y-%m-%d')
    if st.session_state['last_check_date'] != hoje_str:
        st.session_state.update({'bi_enviado': False, 'financeiro_enviado': False, 'last_check_date': hoje_str})
    if agora.hour == 23 and agora.minute >= 30 and not st.session_state['bi_enviado']:
        enviar_relatorio_bi(token, chat_ids); st.session_state['bi_enviado'] = True
    if agora.hour == 23 and agora.minute >= 40 and not st.session_state['financeiro_enviado']:
        analise_fin = analisar_financeiro_com_ia(stake_padrao, st.session_state.get('banca_inicial', 100))
        msg_fin = f"💰 <b>CONSULTORIA FINANCEIRA</b>\n\n{analise_fin}"
        enviar_telegram(token, chat_ids, msg_fin); st.session_state['financeiro_enviado'] = True
    if agora.hour == 23 and agora.minute >= 55 and not st.session_state['bigdata_enviado']:
        enviar_analise_estrategia(token, chat_ids); st.session_state['bigdata_enviado'] = True

def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br()
    if 8 <= agora.hour < 11 and not st.session_state['matinal_enviado']:
        insights = gerar_insights_matinais_ia(api_key)
        if insights and "Sem jogos" not in insights:
            ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
            for cid in ids: enviar_telegram(token, cid, f"🌅 <b>SNIPER MATINAL</b>\n\n{insights}")
            st.session_state['matinal_enviado'] = True

# --- 4.2 UI E LOOP DE EXECUÇÃO ---

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
        if st.button("🌅 Testar Sniper Matinal"):
            with st.spinner("Gerando..."): st.markdown(gerar_insights_matinais_ia(st.session_state['API_KEY']))
        if st.button("🧠 Pedir Análise do BI"):
            with st.spinner("Analisando..."): st.info(analisar_bi_com_ia())
        if st.button("🔄 Forçar Backfill (Salvar Jogos)"):
            with st.spinner("Recuperando dados..."):
                hj = get_time_br().strftime('%Y-%m-%d')
                tj = buscar_agenda_cached(st.session_state['API_KEY'], hj)
                pend = [j for j in tj if j['fixture']['status']['short'] in ['FT'] and str(j['fixture']['id']) not in st.session_state['jogos_salvos_bigdata']]
                if pend:
                    res = atualizar_stats_em_paralelo(pend[:5], st.session_state['API_KEY'])
                    for fid, s in res.items():
                        jo = next(x for x in pend if str(x['fixture']['id'])==str(fid))
                        salvar_bigdata(jo, s)
                    st.success(f"Recuperados: {len(pend)} jogos")
                else: st.warning("Nada pendente.")
        if st.button("📊 Enviar Relatório BI"): enviar_relatorio_bi(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'])
        if st.button("💰 Enviar Financeiro"):
            if 'last_fin_stats' in st.session_state:
                s = st.session_state['last_fin_stats']
                enviar_relatorio_financeiro(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], s['cenario'], s['lucro'], s['roi'], s['entradas'])

    with st.expander("💰 Gestão de Banca", expanded=False):
        stake_padrao = st.number_input("Valor da Aposta (Stake):", value=st.session_state.get('stake_padrao', 10.0), step=5.0)
        banca_inicial = st.number_input("Banca Inicial:", value=st.session_state.get('banca_inicial', 100.0), step=50.0)
        st.session_state['stake_padrao'] = stake_padrao; st.session_state['banca_inicial'] = banca_inicial
        
    with st.expander("📶 Consumo API", expanded=False):
        verificar_reset_diario()
        u = st.session_state['api_usage']; perc = min(u['used'] / u['limit'], 1.0) if u['limit'] > 0 else 0
        st.progress(perc); st.caption(f"Utilizado: **{u['used']}** / {u['limit']}")
    
    with st.expander("🤖 Consumo IA", expanded=False):
        u_ia = st.session_state['gemini_usage']; u_ia['limit'] = 10000 
        st.progress(min(u_ia['used'] / u_ia['limit'], 1.0)); st.caption(f"IA Hoje: {u_ia['used']}")

    st.write("---")
    
    tg_ok, tg_nome = testar_conexao_telegram(st.session_state['TG_TOKEN'])
    if tg_ok: st.markdown(f'<div class="status-active">✈️ TELEGRAM: CONECTADO ({tg_nome})</div>', unsafe_allow_html=True)
    else: st.markdown(f'<div class="status-error">❌ TELEGRAM: ERRO</div>', unsafe_allow_html=True)

    if IA_ATIVADA: st.markdown('<div class="status-active">🤖 IA GEMINI ATIVA</div>', unsafe_allow_html=True)
    else: st.markdown('<div class="status-error">❌ IA DESCONECTADA</div>', unsafe_allow_html=True)

    if db_firestore: st.markdown('<div class="status-active">🔥 FIREBASE CONECTADO</div>', unsafe_allow_html=True)
    else: st.markdown('<div class="status-warning">⚠️ FIREBASE OFFLINE</div>', unsafe_allow_html=True)
    
    st.write("---")
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    
    st.markdown("---")
    if st.button("☢️ ZERAR ROBÔ", type="primary"): 
        st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
        salvar_aba("Historico", st.session_state['historico_full']); st.rerun()

if st.session_state.ROBO_LIGADO:
    with placeholder_root.container():
        # --- LOOP INTELIGENTE (Não bloqueia, usa Timer no final) ---
        if 'last_run' not in st.session_state: st.session_state['last_run'] = 0
        
        # Se passou o tempo do ciclo, processa
        if time.time() - st.session_state['last_run'] >= INTERVALO:
            status_main = st.status("🚀 Iniciando processamento...", expanded=True)
            status_main.write("📂 Carregando caches...")
            carregar_tudo()
            
            s_padrao = st.session_state.get('stake_padrao', 10.0)
            b_inicial = st.session_state.get('banca_inicial', 100.0)
            safe_token = st.session_state.get('TG_TOKEN', '')
            safe_chat = st.session_state.get('TG_CHAT', '')
            safe_api = st.session_state.get('API_KEY', '')

            verificar_automacao_bi(safe_token, safe_chat, s_padrao)
            verificar_alerta_matinal(safe_token, safe_chat, safe_api)
            
            ids_black = [normalizar_id(x) for x in st.session_state['df_black']['id'].values]
            ids_safe = [normalizar_id(x) for x in st.session_state['df_safe']['id'].values]
            hoje_real = get_time_br().strftime('%Y-%m-%d')
            
            api_error = False; jogos_live = []
            try:
                status_main.write("📡 Conectando API Sports...")
                res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": safe_api}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
                update_api_usage(res.headers); jogos_live = res.json().get('response', [])
                api_error = bool(res.json().get('errors'))
            except Exception as e: api_error = True

            if not api_error: 
                status_main.write("🔎 Verificando Greens/Reds...")
                check_green_red_hibrido(jogos_live, safe_token, safe_chat, safe_api)
                conferir_resultados_sniper(jogos_live, safe_api) 
                verificar_var_rollback(jogos_live, safe_token, safe_chat)
            
            radar = []; agenda = []; candidatos_multipla = []
            if not api_error:
                status_main.write("🧠 Analisando Radar...")
                # --- PROCESSAMENTO DO RADAR ---
                for j in jogos_live:
                    lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                    if lid in ids_black: continue
                    status_short = j['fixture']['status']['short']
                    if status_short not in ['1H', '2H', 'HT', 'ET']: continue
                    
                    nome_liga = j['league']['name']
                    if lid in ids_safe: nome_liga += " 🛡️"
                    
                    tempo = j['fixture']['status']['elapsed'] or 0
                    placar = f"{j['goals']['home']}x{j['goals']['away']}"
                    
                    # Verifica se precisa atualizar stats
                    stats = []
                    if deve_buscar_stats(tempo, 0, 0, status_short):
                        chk = st.session_state['controle_stats'].get(fid, datetime.min)
                        if (datetime.now() - chk).total_seconds() > 60:
                            f, s_res, h = fetch_stats_single(fid, safe_api)
                            if s_res: st.session_state['controle_stats'][fid] = datetime.now(); st.session_state[f"st_{fid}"] = s_res; update_api_usage(h)
                    
                    stats = st.session_state.get(f"st_{fid}", [])
                    status_vis = "👁️" if stats else "💤"
                    
                    sinais = []
                    if stats:
                        sinais = processar(j, stats, tempo, placar)
                        salvar_safe_league_basic(lid, j['league']['country'], j['league']['name'])
                        resetar_erros(lid)
                    
                    if sinais:
                        status_vis = f"✅ {len(sinais)}"
                        # Busca dados extras para IA
                        medias = buscar_media_gols_ultimos_jogos(safe_api, j['teams']['home']['id'], j['teams']['away']['id'])
                        extra_ctx = f"Médias (10j): {medias['home']}/{medias['away']}"
                        
                        for s in sinais:
                            uid = gerar_chave_universal(fid, s['tag'], "SINAL")
                            if uid not in st.session_state['alertas_enviados']:
                                st.session_state['alertas_enviados'].add(uid)
                                rh = s.get('rh', 0); ra = s.get('ra', 0)
                                odd = get_live_odds(fid, safe_api, s['tag'], (j['goals']['home'] or 0)+(j['goals']['away'] or 0), tempo)
                                
                                # CHAMA IA
                                op, pb = consultar_ia_gemini({'jogo': f"{j['teams']['home']['name']}x{j['teams']['away']['name']}", 'placar': placar, 'tempo': tempo}, s['tag'], stats, rh, ra, extra_context=extra_ctx)
                                
                                item = {"FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']}x{j['teams']['away']['name']} ({placar})", "Placar_Sinal": placar, "Estrategia": s['tag'], "Resultado": "Pendente", "Odd": odd, "Opiniao_IA": "Aprovado" if "Aprovado" in op else "Arriscado"}
                                adicionar_historico(item)
                                
                                # Envia se aprovado
                                if "Aprovado" in op:
                                    msg = f"🚨 <b>SINAL {s['tag']}</b>\n⚽ {j['teams']['home']['name']} x {j['teams']['away']['name']}\n⏰ {tempo}' ({placar})\n{s['ordem']}\n💰 Odd: {odd}\n{op}"
                                    enviar_telegram(safe_token, safe_chat, msg)
                                    st.toast(f"✅ Enviado: {s['tag']}")
                    
                    radar.append({"Liga": nome_liga, "Jogo": f"{j['teams']['home']['name']} {placar} {j['teams']['away']['name']}", "Tempo": f"{tempo}'", "Status": status_vis})
                
                # Agenda
                prox = buscar_agenda_cached(safe_api, hoje_real)
                for p in prox:
                    if p['fixture']['status']['short'] == 'NS':
                        agenda.append({"Hora": p['fixture']['date'][11:16], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
                
                # Salva no Cache de Visualização
                st.session_state['radar_cache'] = radar
                st.session_state['agenda_cache'] = agenda

            if st.session_state.get('precisa_salvar'):
                status_main.write("💾 Sincronizando dados...")
                salvar_aba("Historico", st.session_state['historico_full'])
            
            st.session_state['last_run'] = time.time()
            status_main.update(label="✅ Ciclo Finalizado!", state="complete", expanded=False)

        # --- DASHBOARD RESTAURADO (Sempre visível) ---
        hist_hj = pd.DataFrame(st.session_state.get('historico_sinais', []))
        t, g, r, w = calcular_stats(hist_hj)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t}</div><div class="metric-sub">{g} Green | {r} Red</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(st.session_state.get("radar_cache", []))}</div><div class="metric-sub">Monitorando</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Ligas Seguras</div><div class="metric-value">{count_safe}</div><div class="metric-sub">Validadas</div></div>', unsafe_allow_html=True)
        
        st.write("")
        # --- ABAS (Com contadores restaurados) ---
        abas = st.tabs([f"📡 Radar ({len(st.session_state.get('radar_cache', []))})", f"📅 Agenda ({len(st.session_state.get('agenda_cache', []))})", f"💰 Financeiro", f"📜 Histórico ({len(hist_hj)})", "📈 BI & Analytics", f"🚫 Blacklist ({len(st.session_state['df_black'])})", f"🛡️ Seguras ({count_safe})", f"⚠️ Obs ({count_obs})", "💾 Big Data"])
        
        with abas[0]: 
            if st.session_state.get('radar_cache'): st.dataframe(pd.DataFrame(st.session_state['radar_cache']).astype(str), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        
        with abas[1]: 
            if st.session_state.get('agenda_cache'): st.dataframe(pd.DataFrame(st.session_state['agenda_cache']).astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos futuros hoje.")
        
        with abas[2]: # ABA FINANCEIRO RESTAURADA
            st.markdown("### 💰 Evolução Financeira")
            modo = st.radio("Filtro:", ["Todos", "Aprovados IA"], horizontal=True)
            df_f = st.session_state.get('historico_full', pd.DataFrame()).copy()
            if not df_f.empty:
                if modo == "Aprovados IA" and 'Opiniao_IA' in df_f.columns: df_f = df_f[df_f['Opiniao_IA'] == 'Aprovado']
                df_f = df_f[df_f['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                lucros = []
                for _, row in df_f.iterrows():
                    odd = float(row['Odd']) if row['Odd'] else 1.50
                    if 'GREEN' in row['Resultado']: lucros.append((s_padrao * odd) - s_padrao)
                    else: lucros.append(-s_padrao)
                st.metric("Lucro Líquido", f"R$ {sum(lucros):.2f}")
                if lucros: st.plotly_chart(px.line(y=pd.Series(lucros).cumsum(), title="Curva de Lucro"), use_container_width=True)

        with abas[3]: 
            if not hist_hj.empty: st.dataframe(hist_hj, use_container_width=True, hide_index=True)
            else: st.caption("Vazio.")

        with abas[4]: # ABA BI RESTAURADA
            df_bi = st.session_state.get('historico_full', pd.DataFrame())
            if not df_bi.empty:
                df_bi = df_bi[df_bi['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                if not df_bi.empty:
                    fig = px.pie(df_bi, names='Resultado', title="Winrate Geral", color='Resultado', color_discrete_map={'✅ GREEN':'#00FF00', '❌ RED':'#FF0000'})
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(df_bi.groupby('Estrategia')['Resultado'].value_counts().unstack().fillna(0), use_container_width=True)

        with abas[5]: st.dataframe(st.session_state['df_black'], use_container_width=True, hide_index=True)
        with abas[6]: st.dataframe(st.session_state['df_safe'], use_container_width=True, hide_index=True)
        with abas[7]: st.dataframe(st.session_state['df_vip'], use_container_width=True, hide_index=True)
        with abas[8]:
            if db_firestore and st.button("Ver Dados Firebase"):
                docs = db_firestore.collection("BigData_Futebol").limit(20).stream()
                st.dataframe([d.to_dict() for d in docs], use_container_width=True)

        # --- TIMER DE RODAPÉ (LÓGICA FINAL SEM PISCAR) ---
        tempo_restante = int(INTERVALO - (time.time() - st.session_state['last_run']))
        if tempo_restante > 0:
            for i in range(tempo_restante, 0, -1):
                placeholder_timer.markdown(f'<div class="footer-timer">⏳ Próxima varredura em {i}s</div>', unsafe_allow_html=True)
                time.sleep(1)
            st.rerun()
        else:
            st.rerun()
else:
    with placeholder_root.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera. Configure na lateral.")
