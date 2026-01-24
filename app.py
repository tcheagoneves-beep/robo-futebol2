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
    return "" # Visual Removido

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

def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    df_raw = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

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
        # Extração de dados crus
        s1 = stats_raw[0]['statistics']; s2 = stats_raw[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        
        chutes_totais = gv(s1, 'Total Shots') + gv(s2, 'Total Shots')
        tempo_str = str(dados_jogo.get('tempo', '0')).replace("'", "")
        tempo = int(tempo_str) if tempo_str.isdigit() else 0
        
        # Filtro básico de API morta
        if tempo > 20 and chutes_totais == 0:
            return "\n🤖 <b>IA:</b> ⚠️ <b>Ignorado</b> - Dados zerados (API Delay).", "N/A"
    except: return "", "N/A"

    chutes_area_casa = gv(s1, 'Shots insidebox')
    chutes_area_fora = gv(s2, 'Shots insidebox')
    escanteios = gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks')
    posse_casa = str(gv(s1, 'Ball Possession')).replace('%', '')
    dados_ricos = extrair_dados_completos(stats_raw)
    
    # --- NOVO PROMPT: MATEMÁTICA PURA E SEM MEDO ---
    prompt = f"""
    Atue como um ANALISTA DE DADOS ESTATÍSTICOS (Mentalidade: EV+ MATEMÁTICO).
    Esqueça gestão de banca, esqueça medo de red. Seu único objetivo é validar se a estatística suporta a entrada.

    DADOS DO JOGO:
    {dados_jogo['jogo']} | Placar: {dados_jogo['placar']} | Tempo: {dados_jogo.get('tempo')}
    Estratégia Indicada: {estrategia}
    Time Favorável no Sinal: {time_favoravel}
    
    ESTATÍSTICAS EM TEMPO REAL:
    - Pressão (Momentum): Casa {rh} x {ra} Visitante
    - Chutes na Área (Perigo Real): Casa {chutes_area_casa} x {chutes_area_fora} Visitante
    - Escanteios: {escanteios}
    - Posse: Casa {posse_casa}%
    
    CONTEXTO HISTÓRICO E ODDS (Obrigatório considerar):
    {extra_context}
    {dados_ricos}

    SUA ANÁLISE DEVE SER FRIA:
    1. CRUZAMENTO DE DADOS: O time indicado na estratégia está criando chances REAIS agora (Chutes na área/Pressão)?
    2. VALIDAÇÃO DE PADRÃO: Se o histórico (Contexto) diz que é time de Over, e o jogo está movimentado, APROVE.
    3. IGNORAR POSSE ESTÉRIL: Posse sem chute é irrelevante. Chute na área é ouro.

    DECISÃO BINÁRIA (Sem ficar em cima do muro):
    - Se a probabilidade matemática do evento ocorrer for > 60% baseado nos chutes/pressão: APROVADO.
    - Se o jogo estiver parado/morto sem chutes: ARRISCADO.

    FORMATO DE RESPOSTA (Rigoroso):
    Aprovado/Arriscado - [Análise técnica direta e curta, sem "eu acho", apenas fatos]
    PROB: [Número 0-100]%
    """

    try:
        response = model_ia.generate_content(
            prompt, 
            generation_config=genai.types.GenerationConfig(temperature=0.2), # Temperatura baixa para ser consistente
            request_options={"timeout": 10}
        )
        st.session_state['gemini_usage']['used'] += 1
        
        texto_completo = response.text.strip().replace("**", "").replace("*", "")
        
        prob_str = "..."
        match_prob = re.search(r'PROB:\s*(\d+)%', texto_completo)
        if match_prob: prob_str = f"{match_prob.group(1)}%"
            
        texto_limpo = re.sub(r'PROB:\s*\d+%', '', texto_completo).strip()
        
        # Lógica para forçar o aprovado se a IA não usar a palavra exata no começo
        veredicto = "Arriscado" 
        if "aprovado" in texto_limpo.lower()[:20]: veredicto = "Aprovado"
              
        motivo = texto_limpo.replace("Aprovado", "").replace("Arriscado", "").replace("-", "", 1).strip()
        emoji = "✅" if veredicto == "Aprovado" else "⚠️"
        
        return f"\n🤖 <b>ANÁLISE QUÂNTICA:</b>\n{emoji} <b>{veredicto.upper()}</b> - <i>{motivo}</i>", prob_str

    except Exception as e: return "", "N/A"
# --- FUNÇÕES AUXILIARES DE IA ---

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
        prompt = f"Analise o dia ({hoje_str}): Total: {total}, Greens: {greens}. Estratégias: {json.dumps(resumo, ensure_ascii=False)}. Destaque o que funcionou e o que falhou (ótica de Trader)."
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
        lucro_total = 0.0; investido = 0.0; qtd=0
        for _, row in df_hoje.iterrows():
            res = str(row['Resultado'])
            odd_final = obter_odd_final_para_calculo(row['Odd'], row['Estrategia'])
            if 'GREEN' in res:
                lucro_total += (stake * odd_final) - stake; investido += stake
            elif 'RED' in res:
                lucro_total -= stake; investido += stake
        roi = (lucro_total / investido * 100) if investido > 0 else 0
        prompt_fin = f"Gestor Financeiro. Dia: Banca Ini: {banca} | Fim: {banca+lucro_total}. Lucro: {lucro_total}. ROI: {roi}%. Dê um conselho sobre gestão de banca."
        response = model_ia.generate_content(prompt_fin)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro Fin: {e}"

def criar_estrategia_nova_ia():
    if not IA_ATIVADA: return "IA Desconectada."
    if not db_firestore: return "Firebase Offline."
    try:
        docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(200).stream()
        data_raw = [d.to_dict() for d in docs]
        if len(data_raw) < 10: return "Coletando dados... (Mínimo 10 jogos no BigData)"
        df = pd.DataFrame(data_raw)
        historico_para_ia = ""
        for _, row in df.head(150).iterrows():
            historico_para_ia += f"Jogo: {row['jogo']} | Placar: {row['placar_final']} | Stats: {json.dumps(row.get('estatisticas', {}))}\n"
        prompt = f"Analise esse Big Data de {len(df)} jogos. Encontre um padrão estatístico oculto (ex: posse x chutes) que resulte em gols e crie uma estratégia nova."
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro Big Data: {e}"

def otimizar_estrategias_existentes_ia():
    return "Funcionalidade em manutenção para upgrade de IA."

def gerar_insights_matinais_ia(api_key):
    if not IA_ATIVADA: return "IA Offline."
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        # Busca jogos do dia inteiro, não importa a hora
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        
        # Filtra Ligas Principais para o Sniper
        LIGAS_TOP = [39, 140, 78, 135, 61, 71, 72, 2, 3] 
        jogos_top = [j for j in jogos if j['league']['id'] in LIGAS_TOP]
        
        # Pega os 3 melhores jogos para analisar profundamente
        if not jogos_top: return "Sem jogos Elite para análise Sniper hoje."
        jogos_selecionados = jogos_top[:3]
        
        dados_para_ia = ""
        
        for j in jogos_selecionados:
            home_nm = j['teams']['home']['name']
            away_nm = j['teams']['away']['name']
            hid = j['teams']['home']['id']
            aid = j['teams']['away']['id']
            
            # Busca dados estatísticos (Cacheado)
            stats_hist = analisar_tendencia_50_jogos(api_key, hid, aid)
            rating_h = buscar_rating_inteligente(api_key, hid)
            rating_a = buscar_rating_inteligente(api_key, aid)
            
            dados_para_ia += f"""
            JOGO: {home_nm} x {away_nm} (Liga: {j['league']['name']})
            DADOS HISTÓRICOS:
            - {home_nm}: Over 1.5 FT em {stats_hist['home']['over15_ft']}% dos últimos jogos.
            - {away_nm}: Over 1.5 FT em {stats_hist['away']['over15_ft']}% dos últimos jogos.
            RATINGS (Força do Time):
            - {home_nm}: {rating_h}
            - {away_nm}: {rating_a}
            ------------------------------------------
            """

        prompt = f"""
        Atue como o SNIPER MATINAL (Especialista em Pré-Live).
        Use EXATAMENTE o formato abaixo para cada jogo. Seja direto.
        
        Analise estes dados:
        {dados_para_ia}

        FORMATO OBRIGATÓRIO DE RESPOSTA (Repita para cada jogo):
        
        ⚽ **[Time Casa] x [Time Fora]**
        📊 **Histórico:** [Time Casa] com [X]% de Over 1.5 e [Time Fora] com [Y]% de Over 1.5.
        ⭐ **Rating:** [Descreva o equilibrio baseado nos números de rating fornecidos].
        🧠 **API Advice:** [Sua conclusão lógica: Vitória de quem ou Gols?]
        
        🎯 **SNIPER:** [Sua aposta final - Ex: Over 1.5 Gols]
        
        (Pule uma linha entre jogos)
        """
        
        resp = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return resp.text
    except Exception as e: return f"Erro Matinal: {e}"

def momentum(fid, sog_h, sog_a):
    mem = st.session_state['memoria_pressao'].get(fid, {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
    if 'sog_h' not in mem: mem = {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []}
    now = datetime.now()
    # Detecta aumento de chutes no gol (Momentum Real)
    if sog_h > mem['sog_h']: mem['h_t'].extend([now]*(sog_h-mem['sog_h']))
    if sog_a > mem['sog_a']: mem['a_t'].extend([now]*(sog_a-mem['sog_a']))
    # Limpa dados antigos (> 7 min)
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
        
        # Posse de bola para filtro "Arame Liso"
        try:
            posse_h_val = next((x['value'] for x in stats_h if x['type']=='Ball Possession'), "50%")
            posse_h = int(str(posse_h_val).replace('%', ''))
            posse_a = 100 - posse_h
        except: posse_h = 50; posse_a = 50

        # --- FILTRO ARAME LISO (DRAKO/THEO) ---
        # Posse alta (>65%) sem chutes no gol (<2) = Posse Inútil.
        arame_liso_casa = (posse_h >= 65 and sog_h < 2)
        arame_liso_fora = (posse_a >= 65 and sog_a < 2)
        
        rh, ra = momentum(j['fixture']['id'], sog_h, sog_a)
        
        gh = j['goals']['home']; ga = j['goals']['away']
        total_gols = gh + ga
        total_chutes = sh_h + sh_a
        
        def gerar_ordem_gol(gols_atuais, tipo="Over"):
            linha = gols_atuais + 0.5
            if tipo == "Over": return f"👉 <b>FAZER:</b> Entrar em GOLS (Over)\n✅ Aposta: <b>Mais de {linha} Gols</b>"
            elif tipo == "HT": return f"👉 <b>FAZER:</b> Entrar em GOLS 1º TEMPO\n✅ Aposta: <b>Mais de 0.5 Gols HT</b>"
            elif tipo == "Limite":
                linha_limite = gols_atuais + 1.0
                return f"👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de {linha_limite} Gols</b> (Asiático)"
            return "Apostar em Gols."

        SINAIS = []
        golden_bet_ativada = False

        # --- GOLS: GOLDEN BET (A "Rainha") ---
        if 65 <= tempo <= 75:
            # Pressão absurda E não pode ser Arame Liso
            pressao_casa = (rh >= 3 and sog_h >= 4) and not arame_liso_casa
            pressao_fora = (ra >= 3 and sog_a >= 4) and not arame_liso_fora
            
            if (pressao_casa and sh_h > sh_a) or (pressao_fora and sh_a > sh_h):
                 if total_gols >= 1 or total_chutes >= 18:
                      SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": "🔥 Pressão Favorito + Finalizações", "rh": rh, "ra": ra, "favorito": "GOLS"})
                      golden_bet_ativada = True

        # --- GOLS: JANELA DE OURO (A "Vice") ---
        if not golden_bet_ativada and (70 <= tempo <= 75) and abs(gh - ga) <= 1:
            if total_chutes >= 22 and (not arame_liso_casa and not arame_liso_fora): 
                SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"🔥 {total_chutes} Chutes Totais", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # --- UNDER: JOGO MORNO (Caça ao Under) ---
        if 55 <= tempo <= 75:
             # Poucos chutes E (Arame liso é bom para under OU simplesmente ninguém ataca)
             if total_chutes <= 10 and (sog_h + sog_a) <= 2:
                 if gh == ga: 
                     linha_under = total_gols + 0.5
                     SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": f"👉 <b>FAZER:</b> Under Gols (Segurar)\n✅ Aposta: <b>Menos de {linha_under} Gols</b> (Ou Under Limite)", "stats": f"Jogo Travado ({total_chutes} chutes totais)", "rh": rh, "ra": ra, "favorito": "UNDER"})

        # --- MATCH ODDS: VOVÔ (BLINDADO - Anti Zebra) ---
        # Só entra se o adversário for estatisticamente NULO (menos de 2 chutes no gol)
        if 70 <= tempo <= 80 and total_chutes < 18:
            diff = gh - ga
            if diff == 1 and ra < 2 and posse_h >= 45 and sog_a < 2: 
                 SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito (Segurar)\n✅ Aposta: <b>Vitória do CASA</b>", "stats": f"Controle Total (Adv: {sog_a} SoG)", "rh": rh, "ra": ra, "favorito": "CASA"})
            elif diff == -1 and rh < 2 and posse_a >= 45 and sog_h < 2: 
                 SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito (Segurar)\n✅ Aposta: <b>Vitória do VISITANTE</b>", "stats": f"Controle Total (Adv: {sog_h} SoG)", "rh": rh, "ra": ra, "favorito": "VISITANTE"})

        # --- OUTRAS ESTRATÉGIAS ---
        if tempo <= 30 and total_gols >= 2: 
            SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": gerar_ordem_gol(total_gols), "stats": f"Jogo Aberto ({total_gols} gols)", "rh": rh, "ra": ra, "favorito": "GOLS"})
        
        if total_gols == 0 and (tempo <= 10 and total_chutes >= 3): 
            SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": gerar_ordem_gol(0, "HT"), "stats": "Início Intenso", "rh": rh, "ra": ra, "favorito": "GOLS"})
        
        if tempo <= 60:
            # Blitz exige pressão (rh/ra) E conversão (sog), não só posse
            if gh <= ga and (rh >= 3 or (sh_h >= 8 and sog_h >= 3)) and not arame_liso_casa: 
                SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": gerar_ordem_gol(total_gols), "stats": "Pressão Casa", "rh": rh, "ra": ra, "favorito": "GOLS"})
            if ga <= gh and (ra >= 3 or (sh_a >= 8 and sog_a >= 3)) and not arame_liso_fora: 
                SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": gerar_ordem_gol(total_gols), "stats": "Pressão Visitante", "rh": rh, "ra": ra, "favorito": "GOLS"})
        
        if 15 <= tempo <= 25 and total_chutes >= 6 and total_sog >= 3:
             SINAIS.append({"tag": "🏹 Tiroteio Elite", "ordem": gerar_ordem_gol(total_gols), "stats": "Muitos Chutes", "rh": rh, "ra": ra, "favorito": "GOLS"})
        
        # --- FIX SNIPER FINAL: GOL TARDIO ---
        if tempo >= 80 and abs(gh - ga) <= 1: 
            tem_bola_parada = (ck_h + ck_a) >= 10 # Aumentei régua de escanteios
            tem_pressao = (rh >= 4 and sh_h >= 14) or (ra >= 4 and sh_a >= 14) # Aumentei régua de chutes
            if tem_pressao or tem_bola_parada:
                SINAIS.append({"tag": "💎 Sniper Final", "ordem": "👉 <b>FAZER:</b> Over Gol Limite (Asiático)\n✅ Busque o Gol no Final", "stats": "Pressão Final", "rh": rh, "ra": ra, "favorito": "GOLS"})
        
        if 10 <= tempo <= 40 and gh == ga:
            # Back Nettuno: Exige dominância clara e chutes no gol
            if (posse_h >= 55) and (sog_h >= 3) and (sh_h >= 6) and (sh_a <= 1) and rh >= 2: 
                     SINAIS.append({"tag": "🦁 Back Favorito (Nettuno)", "ordem": "👉 <b>FAZER:</b> Back Casa", "stats": "Dominância", "rh": rh, "ra": ra, "favorito": "CASA"})
            elif (posse_a >= 55) and (sog_a >= 3) and (sh_a >= 6) and (sh_h <= 1) and ra >= 2:
                     SINAIS.append({"tag": "🦁 Back Favorito (Nettuno)", "ordem": "👉 <b>FAZER:</b> Back Visitante", "stats": "Dominância", "rh": rh, "ra": ra, "favorito": "VISITANTE"})

        if 60 <= tempo <= 88 and abs(gh - ga) >= 3 and (total_chutes >= 14):
             SINAIS.append({"tag": "🔫 Lay Goleada", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": "Goleada Viva", "rh": rh, "ra": ra, "favorito": "GOLS"})

        return SINAIS
    except: return []
# ==============================================================================
# 4. TELEGRAM, RESULTADOS, RELATÓRIOS E UI (FINAL)
# ==============================================================================

# --- 4.1 DEFINIÇÃO DAS FUNÇÕES DE TELEGRAM E RELATÓRIOS (Antes do uso) ---

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
    
    key_sinal_orig = gerar_chave_universal(fid, strat, "SINAL")
    key_green = gerar_chave_universal(fid, strat, "GREEN")
    key_red = gerar_chave_universal(fid, strat, "RED")
    
    # --- FILTRO ANTI-SPAM (CRUCIAL): ---
    # Só envia o resultado se o SINAL ORIGINAL foi enviado.
    # Se foi "Arriscado" e não foi pro Telegram, o resultado também não vai.
    deve_enviar_msg = (key_sinal_orig in st.session_state.get('alertas_enviados', set()))

    if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
    
    # 1. Detecção de GOL (Bola na Rede)
    if (gh + ga) > (ph + pa):
        
        # Match Odds: Ignora gol. Só resolve no FT.
        STRATS_MATCH_ODDS = ["Vovô", "Back Favorito"]
        if any(x in strat for x in STRATS_MATCH_ODDS):
            return False

        # Under/Morno: Gol é ruim (se bater a linha)
        if "Morno" in strat or "Under" in strat:
            if (gh+ga) >= 2: # Exemplo: Over 1.5 bateu
                sinal['Resultado'] = '❌ RED'
                if deve_enviar_msg and key_red not in st.session_state['alertas_enviados']:
                    enviar_telegram(token, chats, f"❌ <b>RED | OVER 1.5 BATIDO</b>\n⚽ {sinal['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {strat}")
                    st.session_state['alertas_enviados'].add(key_red)
                st.session_state['precisa_salvar'] = True
                return True
        else:
            # Over Gols Padrão: Gol é Green imediato
            sinal['Resultado'] = '✅ GREEN'
            if deve_enviar_msg and key_green not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📈 Placar: <b>{gh}x{ga}</b>\n🎯 {strat}")
                st.session_state['alertas_enviados'].add(key_green)
            st.session_state['precisa_salvar'] = True
            return True

    # 2. HT / FT (Final de período)
    STRATS_HT_ONLY = ["Gol Relâmpago", "Massacre", "Choque", "Briga"]
    eh_ht_strat = any(x in strat for x in STRATS_HT_ONLY)
    if eh_ht_strat and st_short in ['HT', '2H', 'FT', 'AET', 'PEN', 'ABD']:
        sinal['Resultado'] = '❌ RED'
        if deve_enviar_msg and key_red not in st.session_state['alertas_enviados']:
            enviar_telegram(token, chats, f"❌ <b>RED | INTERVALO (HT)</b>\n⚽ {sinal['Jogo']}\n📉 Placar HT: {gh}x{ga}\n🎯 {strat}")
            st.session_state['alertas_enviados'].add(key_red)
        st.session_state['precisa_salvar'] = True
        return True
        
    if st_short in ['FT', 'AET', 'PEN', 'ABD']:
        # Match Odds (Vovô/Back) - SÓ NO FINAL
        if "Vovô" in strat or "Back" in strat:
            ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
            resultado = '❌ RED'
            if ph > pa: # Casa ganhava
                if gh > ga: resultado = '✅ GREEN'
            elif pa > ph: # Visitante ganhava
                if ga > gh: resultado = '✅ GREEN'
            
            if resultado == '✅ GREEN':
                 if deve_enviar_msg and key_green not in st.session_state['alertas_enviados']:
                    enviar_telegram(token, chats, f"✅ <b>GREEN | FINALIZADO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {strat}")
                    st.session_state['alertas_enviados'].add(key_green)
            else:
                 if deve_enviar_msg and key_red not in st.session_state['alertas_enviados']:
                    enviar_telegram(token, chats, f"❌ <b>RED | ENCERRADO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {strat}")
                    st.session_state['alertas_enviados'].add(key_red)
            sinal['Resultado'] = resultado
            st.session_state['precisa_salvar'] = True
            return True

        # Under/Morno (Se terminou e não estourou, é Green)
        if ("Morno" in strat or "Under" in strat):
             sinal['Resultado'] = '✅ GREEN'
             if deve_enviar_msg and key_green not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"✅ <b>GREEN | FINALIZADO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {strat}")
                st.session_state['alertas_enviados'].add(key_green)
             st.session_state['precisa_salvar'] = True
             return True
        
        # Over que não bateu
        sinal['Resultado'] = '❌ RED'
        if deve_enviar_msg and key_red not in st.session_state['alertas_enviados']:
            enviar_telegram(token, chats, f"❌ <b>RED | ENCERRADO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {strat}")
            st.session_state['alertas_enviados'].add(key_red)
        st.session_state['precisa_salvar'] = True
        return True
    return False
def check_green_red_hibrido(jogos_live, token, chats, api_key):
    hist = st.session_state['historico_sinais']
    pendentes = [s for s in hist if s['Resultado'] == 'Pendente']
    if not pendentes: return
    hoje_str = get_time_br().strftime('%Y-%m-%d')
    updates_buffer = []
    mapa_live = {j['fixture']['id']: j for j in jogos_live}
    for s in pendentes:
        if s.get('Data') != hoje_str: continue
        if "Sniper" in s['Estrategia']: continue
        fid = int(clean_fid(s.get('FID', 0)))
        strat = s['Estrategia']
        
        # Verifica se já processou (evita reprocessamento)
        key_green = gerar_chave_universal(fid, strat, "GREEN")
        key_red = gerar_chave_universal(fid, strat, "RED")
        if key_green in st.session_state['alertas_enviados']: s['Resultado'] = '✅ GREEN'; updates_buffer.append(s); continue
        if key_red in st.session_state['alertas_enviados']: s['Resultado'] = '❌ RED'; updates_buffer.append(s); continue
        
        jogo_encontrado = mapa_live.get(fid)
        if not jogo_encontrado:
             try:
                 res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                 if res['response']: jogo_encontrado = res['response'][0]
             except: pass
        if jogo_encontrado:
            if processar_resultado(s, jogo_encontrado, token, chats): updates_buffer.append(s)
    if updates_buffer: atualizar_historico_ram(updates_buffer)

def conferir_resultados_sniper(jogos_live, api_key):
    hist = st.session_state.get('historico_sinais', [])
    snipers = [s for s in hist if "Sniper" in s['Estrategia'] and s['Resultado'] == "Pendente"]
    if not snipers: return
    updates = []
    ids_live = {str(j['fixture']['id']): j for j in jogos_live} 
    for s in snipers:
        fid = str(s['FID'])
        jogo = ids_live.get(fid)
        if not jogo:
            try:
                res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                if res.get('response'): jogo = res['response'][0]
            except: pass
        if not jogo: continue
        status = jogo['fixture']['status']['short']
        if status not in ['FT', 'AET', 'PEN', 'INT']: continue
        gh = jogo['goals']['home'] or 0; ga = jogo['goals']['away'] or 0; tg = gh + ga
        
        res_final = '❌ RED'
        try:
            # Tenta extrair placar original. Ex: "Time A x Time B (1x1)"
            placar_sinal = re.search(r'\((\d+)x(\d+)\)', s.get('Jogo', '')) 
            if not placar_sinal:
                 # Fallback: tenta deduzir do campo Placar_Sinal se existir
                 try: p = s['Placar_Sinal'].split('x'); gols_sinal = int(p[0]) + int(p[1])
                 except: gols_sinal = 99
            else:
                gols_sinal = int(placar_sinal.group(1)) + int(placar_sinal.group(2))
            
            # Sniper é Over Limite: Precisa de MAIS gols que no sinal
            if tg > gols_sinal: res_final = '✅ GREEN'
            else: res_final = '❌ RED'
        except: pass
            
        s['Resultado'] = res_final
        updates.append(s)
        
        # Filtro Anti-Spam também para Sniper
        key_sinal = gerar_chave_universal(fid, s['Estrategia'], "SINAL")
        if key_sinal in st.session_state.get('alertas_enviados', set()):
            enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], f"{res_final} <b>SNIPER FINALIZADO</b>\n⚽ {s['Jogo']}\n📉 Placar Final: {gh}x{ga}")
        
        st.session_state['precisa_salvar'] = True
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
                # Se o placar atual for MENOR que o do Green, houve VAR
                if (gh + ga) <= (ph + pa):
                    assinatura_var = f"{fid}_{s['Estrategia']}_{gh}x{ga}"
                    if assinatura_var in st.session_state['var_avisado_cache']:
                        if s['Resultado'] != 'Pendente': s['Resultado'] = 'Pendente'; updates.append(s)
                        continue 
                    s['Resultado'] = 'Pendente'; st.session_state['precisa_salvar'] = True
                    updates.append(s)
                    
                    key_green = gerar_chave_universal(fid, s['Estrategia'], "GREEN")
                    if 'alertas_enviados' in st.session_state: st.session_state['alertas_enviados'].discard(key_green)
                    st.session_state['var_avisado_cache'].add(assinatura_var)
                    
                    key_sinal = gerar_chave_universal(fid, s['Estrategia'], "SINAL")
                    if key_sinal in st.session_state.get('alertas_enviados', set()):
                        enviar_telegram(token, chats, f"⚠️ <b>VAR ACIONADO | GOL ANULADO</b>\n⚽ {s['Jogo']}\n📉 Placar voltou: <b>{gh}x{ga}</b>")
            except: pass
    if updates: atualizar_historico_ram(updates)

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
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_stats_single, j['fixture']['id'], api_key): j for j in jogos_alvo}
        time.sleep(0.2)
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
        hoje = pd.to_datetime(get_time_br().date())
        d_hoje = df[df['Data_DT'] == hoje]
        d_7d = df[df['Data_DT'] >= (hoje - timedelta(days=7))]
        
        def fmt_placar(d):
            if d.empty: return "0G - 0R (0%)"
            g = d['Resultado'].str.contains('GREEN', na=False).sum()
            r = d['Resultado'].str.contains('RED', na=False).sum()
            t = g + r
            wr = (g/t*100) if t > 0 else 0
            return f"{g}G - {r}R ({wr:.0f}%)"
            
        def fmt_ia_stats(periodo_df, label_periodo):
            if 'Opiniao_IA' not in periodo_df.columns: return ""
            d_fin = periodo_df[periodo_df['Resultado'].isin(['✅ GREEN', '❌ RED'])]
            stats_aprov = fmt_placar(d_fin[d_fin['Opiniao_IA'] == 'Aprovado'])
            stats_risk = fmt_placar(d_fin[d_fin['Opiniao_IA'] == 'Arriscado'])
            return f"🤖 IA ({label_periodo}):\n👍 Aprovados: {stats_aprov}\n⚠️ Arriscados: {stats_risk}"
        
        insight_text = analisar_bi_com_ia()
        
        txt_detalhe = ""
        df_closed = d_hoje[d_hoje['Resultado'].isin(['✅ GREEN', '❌ RED'])]
        if not df_closed.empty:
            strats_stats = df_closed.groupby('Estrategia').apply(
                lambda x: f"{(x['Resultado'].str.contains('GREEN').sum() / len(x) * 100):.0f}% ({x['Resultado'].str.contains('GREEN').sum()}/{len(x)})"
            ).to_dict()
            txt_detalhe = "\n\n📊 <b>ASSERTIVIDADE POR ESTRATÉGIA:</b>"
            for k, v in strats_stats.items():
                txt_detalhe += f"\n▪️ {k}: <b>{v}</b>"
        
        msg_texto = f"""📈 <b>RELATÓRIO BI AVANÇADO</b>\n📆 <b>HOJE:</b> {fmt_placar(d_hoje)}\n{fmt_ia_stats(d_hoje, "Hoje")}{txt_detalhe}\n\n🗓 <b>SEMANA:</b> {fmt_placar(d_7d)}\n\n🧠 <b>INSIGHT IA:</b>\n{insight_text}"""
        enviar_telegram(token, chat_ids, msg_texto)
    except Exception as e: st.error(f"Erro ao gerar BI: {e}")

def verificar_automacao_bi(token, chat_ids, stake_padrao):
    agora = get_time_br()
    hoje_str = agora.strftime('%Y-%m-%d')
    if st.session_state['last_check_date'] != hoje_str:
        st.session_state['bi_enviado'] = False
        st.session_state['ia_enviada'] = False
        st.session_state['financeiro_enviado'] = False
        st.session_state['bigdata_enviado'] = False
        st.session_state['matinal_enviado'] = False
        st.session_state['last_check_date'] = hoje_str
    if agora.hour == 23 and agora.minute >= 30 and not st.session_state['bi_enviado']:
        enviar_relatorio_bi(token, chat_ids)
        st.session_state['bi_enviado'] = True
    if agora.hour == 23 and agora.minute >= 40 and not st.session_state['financeiro_enviado']:
        analise_fin = analisar_financeiro_com_ia(stake_padrao, st.session_state.get('banca_inicial', 100))
        msg_fin = f"💰 <b>CONSULTORIA FINANCEIRA</b>\n\n{analise_fin}"
        enviar_telegram(token, chat_ids, msg_fin)
        st.session_state['financeiro_enviado'] = True
    if agora.hour == 23 and agora.minute >= 55 and not st.session_state['bigdata_enviado']:
        enviar_analise_estrategia(token, chat_ids)
        st.session_state['bigdata_enviado'] = True

def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br()
    if 8 <= agora.hour < 11 and not st.session_state['matinal_enviado']:
        insights = gerar_insights_matinais_ia(api_key)
        if insights and "Sem jogos" not in insights:
            ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
            msg_final = f"🌅 <b>SNIPER MATINAL (IA + DADOS)</b>\n\n{insights}"
            for cid in ids: enviar_telegram(token, cid, msg_final)
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
        # --- BOTAO NOVO: TESTAR SNIPER AGORA ---
        if st.button("🌅 Testar Sniper Matinal Agora"):
            if IA_ATIVADA:
                with st.spinner("Gerando Sniper Matinal (Formatado)..."):
                    insights = gerar_insights_matinais_ia(st.session_state['API_KEY'])
                    st.markdown(insights)
            else: st.error("IA Offline")
            
        if st.button("🧠 Pedir Análise do BI"):
            if IA_ATIVADA:
                with st.spinner("🤖 O Consultor Neves está analisando seus dados..."):
                    analise = analisar_bi_com_ia()
                    st.markdown("### 📝 Relatório do Consultor")
                    st.info(analise)
            else: st.error("IA Desconectada.")
            
        if st.button("🧪 Criar Nova Estratégia (Big Data)"):
            if IA_ATIVADA:
                with st.spinner("🤖 Analisando padrões globais no Big Data..."):
                    sugestao = criar_estrategia_nova_ia()
                    st.markdown("### 💡 Sugestão da IA")
                    st.success(sugestao)
            else: st.error("IA Desconectada.")
            
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
        st.progress(perc); st.caption(f"Utilizado: **{u['used']}** / {u['limit']}")
    
    with st.expander("🤖 Consumo IA (Gemini)", expanded=False):
        u_ia = st.session_state['gemini_usage']; u_ia['limit'] = 10000 
        perc_ia = min(u_ia['used'] / u_ia['limit'], 1.0)
        st.progress(perc_ia); st.caption(f"Requições Hoje: **{u_ia['used']}** / {u_ia['limit']}")
        if st.button("🔓 Destravar IA Agora"):
            st.session_state['ia_bloqueada_ate'] = None; st.toast("✅ IA Destravada!")

    st.write("---")
    
    tg_ok, tg_nome = testar_conexao_telegram(st.session_state['TG_TOKEN'])
    if tg_ok: 
        st.markdown(f'<div class="status-active">✈️ TELEGRAM: CONECTADO ({tg_nome})</div>', unsafe_allow_html=True)
    else: 
        st.markdown(f'<div class="status-error">❌ TELEGRAM: ERRO ({tg_nome})</div>', unsafe_allow_html=True)

    if IA_ATIVADA:
        st.markdown('<div class="status-active">🤖 IA GEMINI ATIVA</div>', unsafe_allow_html=True)
    else: 
        st.markdown('<div class="status-error">❌ IA DESCONECTADA</div>', unsafe_allow_html=True)

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

if st.session_state.ROBO_LIGADO:
    with placeholder_root.container():
        carregar_tudo()
        s_padrao = st.session_state.get('stake_padrao', 10.0)
        b_inicial = st.session_state.get('banca_inicial', 100.0)
        safe_token = st.session_state.get('TG_TOKEN', '')
        safe_chat = st.session_state.get('TG_CHAT', '')
        safe_api = st.session_state.get('API_KEY', '')

        # FUNÇÕES JÁ ESTÃO DEFINIDAS ACIMA, SEM RISCO DE NAMEERROR
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
            check_green_red_hibrido(jogos_live, safe_token, safe_chat, safe_api)
            conferir_resultados_sniper(jogos_live, safe_api) 
            verificar_var_rollback(jogos_live, safe_token, safe_chat)
        
        radar = []; agenda = []; candidatos_multipla = []; ids_no_radar = []
        if not api_error:
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
                
                stats = []
                ult_chk = st.session_state['controle_stats'].get(fid, datetime.min)
                
                t_esp = 180 
                eh_inicio = (tempo <= 20); eh_final = (tempo >= 70 and abs(gh - ga) <= 1); eh_ht = (st_short == 'HT')
                memoria = st.session_state['memoria_pressao'].get(fid, {})
                pressao_recente = (len(memoria.get('h_t', [])) + len(memoria.get('a_t', []))) >= 4
                
                if eh_inicio or eh_final or eh_ht or pressao_recente: t_esp = 60

                if deve_buscar_stats(tempo, gh, ga, st_short):
                    if (datetime.now() - ult_chk).total_seconds() > t_esp:
                           fid_res, s_res, h_res = fetch_stats_single(fid, safe_api)
                           if s_res:
                               st.session_state['controle_stats'][fid] = datetime.now()
                               st.session_state[f"st_{fid}"] = s_res
                               update_api_usage(h_res)
                
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
                    
                    txt_history = ""
                    if dados_50:
                        txt_history = (f"HISTÓRICO 50 JOGOS: Casa(Over1.5: {dados_50['home']['over15_ft']}%, HT: {dados_50['home']['over05_ht']}%) "
                                       f"| Fora(Over1.5: {dados_50['away']['over15_ft']}%, HT: {dados_50['away']['over05_ht']}%)")
                    txt_rating_ia = f"RATING (MÉDIA/ÚLTIMO): Casa {nota_home} | Fora {nota_away}"
                    extra_ctx = f"{txt_history}\n{txt_rating_ia}"

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
                        
                        opiniao_txt = "" 
                        prob_txt = "..."
                        opiniao_db = "Neutro"
                        
                        if IA_ATIVADA:
                            try:
                                time.sleep(0.3)
                                dados_ia = {'jogo': f"{home} x {away}", 'placar': placar, 'tempo': f"{tempo}'"}
                                time_fav_ia = s.get('favorito', '')
                                opiniao_txt, prob_txt = consultar_ia_gemini(dados_ia, s['tag'], stats, rh, ra, extra_context=extra_ctx, time_favoravel=time_fav_ia)
                                
                                if "aprovado" in opiniao_txt.lower(): opiniao_db = "Aprovado"
                                elif "arriscado" in opiniao_txt.lower(): opiniao_db = "Arriscado"
                                else: opiniao_db = "Neutro"
                            except: pass
                        
                        item = {"FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{home} x {away} ({placar})", "Placar_Sinal": placar, "Estrategia": s['tag'], "Resultado": "Pendente", "HomeID": str(j['teams']['home']['id']) if lid in ids_safe else "", "AwayID": str(j['teams']['away']['id']) if lid in ids_safe else "", "Odd": odd_atual_str, "Odd_Atualizada": "", "Opiniao_IA": opiniao_db}
                        
                        if adicionar_historico(item):
                            try:
                                if prob_txt != "..." and prob_txt != "N/A":
                                    prob_final_display = f"\n🔮 <b>Probabilidade IA: {prob_txt}</b>"
                                else:
                                    prob_final_display = buscar_inteligencia(s['tag'], j['league']['name'], f"{home} x {away}")
                                
                                texto_validacao = ""
                                if dados_50:
                                    h_stats = dados_50['home']; a_stats = dados_50['away']
                                    foco = "Geral"; pct_h = 0; pct_a = 0
                                    if "HT" in s['tag'] or "Relâmpago" in s['tag'] or "Massacre" in s['tag'] or "Choque" in s['tag']:
                                        foco = "Gols 1º Tempo (HT)"; pct_h = h_stats.get('over05_ht', 0); pct_a = a_stats.get('over05_ht', 0)
                                    elif "Morno" in s['tag']:
                                        foco = "Freq. Over 1.5 (Cuidado se alto)"; pct_h = h_stats.get('over15_ft', 0); pct_a = a_stats.get('over15_ft', 0)
                                    else:
                                        foco = "Freq. Over 1.5"; pct_h = h_stats.get('over15_ft', 0); pct_a = a_stats.get('over15_ft', 0)
                                    texto_validacao = f"\n\n🔎 <b>Raio-X (50 Jogos):</b>\n{foco}: Casa <b>{pct_h}%</b> | Fora <b>{pct_a}%</b>"
                                msg = (
                                    f"<b>🚨 SINAL {s['tag'].upper()}</b>\n\n"
                                    f"🏆 <b>{liga_safe}</b>\n"
                                    f"⚽ {home_safe} 🆚 {away_safe}\n"
                                    f"⏰ <b>{tempo}' min</b> (Placar: {placar})\n\n"
                                    f"{s['ordem']}\n"
                                    f"{destaque_odd}\n"
                                    f"📊 <i>Dados: {s['stats']}</i>\n"
                                    f"⚽ Médias (10j): Casa {medias_gols['home']} | Fora {medias_gols['away']}"
                                    f"{texto_validacao}\n"
                                    f"{prob_final_display}"
                                    f"{opiniao_txt}" 
                                )
                                # --- FILTRO: SÓ ENVIA PARA O TELEGRAM SE APROVADO ---
                                if opiniao_db == "Aprovado":
                                    enviar_telegram(safe_token, safe_chat, msg)
                                    st.toast(f"✅ Sinal Aprovado Enviado: {s['tag']}")
                                else:
                                    st.toast(f"⚠️ Sinal Retido (IA: {opiniao_db}): {s['tag']}")
                            except Exception as e:
                                print(f"Erro ao enviar sinal: {e}")
                        elif uid_super not in st.session_state['alertas_enviados'] and odd_val >= 1.80:
                             st.session_state['alertas_enviados'].add(uid_super)
                             msg_super = (f"💎 <b>OPORTUNIDADE DE VALOR!</b>\n\n⚽ {home} 🆚 {away}\n📈 <b>A Odd subiu!</b> Entrada valorizada.\n🔥 <b>Estratégia:</b> {s['tag']}\n💰 <b>Nova Odd: @{odd_atual_str}</b>")
                             # Super Odd é oportunidade rara, envia mesmo se IA não aprovar explicitamente (opcional, mantive enviando)
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
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Ligas Seguras</div><div class="metric-value">{count_safe}</div><div class="metric-sub">Validadas</div></div>', unsafe_allow_html=True)
        
        st.write("")
        abas = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", f"💰 Financeiro", f"📜 Histórico ({len(hist_hj)})", "📈 BI & Analytics", f"🚫 Blacklist ({len(st.session_state['df_black'])})", f"🛡️ Seguras ({count_safe})", f"⚠️ Obs ({count_obs})", "💾 Big Data (Firebase)"])
        
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
                colunas_esconder = ['FID', 'HomeID', 'AwayID', 'Data_Str', 'Data_DT', 'Odd_Atualizada', 'Placar_Sinal']
                cols_view = [c for c in df_show.columns if c not in colunas_esconder]
                st.dataframe(df_show[cols_view], use_container_width=True, hide_index=True)
            else: st.caption("Vazio.")

        with abas[4]: 
            st.markdown("### 📊 Inteligência de Mercado")
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
                    d_total = df_bi
                    
                    if 'bi_filter' not in st.session_state: st.session_state['bi_filter'] = "Tudo"
                    filtro = st.selectbox("📅 Período", ["Tudo", "Hoje", "7 Dias", "30 Dias"], key="bi_select")
                    if filtro == "Hoje": df_show = d_hoje
                    elif filtro == "7 Dias": df_show = d_7d
                    elif filtro == "30 Dias": df_show = d_30d
                    else: df_show = df_bi 
                    
                    if not df_show.empty:
                        gr = df_show['Resultado'].str.contains('GREEN').sum(); rd = df_show['Resultado'].str.contains('RED').sum(); tt = len(df_show); ww = (gr/tt*100) if tt>0 else 0
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Sinais", tt); m2.metric("Greens", gr); m3.metric("Reds", rd); m4.metric("Assertividade", f"{ww:.1f}%")
                        st.divider()
                        st.markdown("### 🏆 Melhores e Piores Ligas (Com Drill-Down)")
                        df_finished = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        if not df_finished.empty:
                            stats_ligas = df_finished.groupby('Liga')['Resultado'].apply(lambda x: pd.Series({'Winrate': (x.str.contains('GREEN').sum() / len(x) * 100), 'Total': len(x), 'Reds': x.str.contains('RED').sum(), 'Greens': x.str.contains('GREEN').sum()})).unstack()
                            stats_ligas = stats_ligas[stats_ligas['Total'] >= 2]
                            col_top, col_worst = st.columns(2)
                            with col_top:
                                st.caption("🌟 Top Ligas (Mais Lucrativas)")
                                top_ligas = stats_ligas.sort_values(by=['Winrate', 'Total'], ascending=[False, False]).head(10)
                                st.dataframe(top_ligas[['Winrate', 'Total', 'Greens']].style.format({'Winrate': '{:.2f}%', 'Total': '{:.0f}', 'Greens': '{:.0f}'}), use_container_width=True)
                            with col_worst:
                                st.caption("💀 Ligas Críticas")
                                worst_ligas = stats_ligas.sort_values(by=['Reds'], ascending=False).head(10)
                                dados_drill = []
                                for liga, row in worst_ligas.iterrows():
                                    if row['Reds'] > 0:
                                        erros_liga = df_finished[(df_finished['Liga'] == liga) & (df_finished['Resultado'].str.contains('RED'))]
                                        pior_strat = erros_liga['Estrategia'].value_counts().head(1)
                                        nome_strat = pior_strat.index[0] if not pior_strat.empty else "-"
                                        dados_drill.append({"Liga": liga, "Total Reds": int(row['Reds']), "Pior Estratégia": nome_strat})
                                if dados_drill: st.dataframe(pd.DataFrame(dados_drill), use_container_width=True, hide_index=True)
                                else: st.success("Nenhuma liga com Reds significativos.")
                        st.divider()

                        st.markdown("### 🧠 Auditoria da IA (Aprovações vs Resultado)")
                        if 'Opiniao_IA' in df_show.columns:
                            df_audit = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
                            categorias_desejadas = ['Aprovado', 'Arriscado', 'Sniper']
                            df_audit = df_audit[df_audit['Opiniao_IA'].isin(categorias_desejadas)]
                            if not df_audit.empty:
                                pivot = pd.crosstab(df_audit['Opiniao_IA'], df_audit['Resultado'], margins=False)
                                if '✅ GREEN' not in pivot.columns: pivot['✅ GREEN'] = 0
                                if '❌ RED' not in pivot.columns: pivot['❌ RED'] = 0
                                pivot['Total'] = pivot['✅ GREEN'] + pivot['❌ RED']
                                pivot['Winrate %'] = (pivot['✅ GREEN'] / pivot['Total'] * 100)
                                format_dict = {'Winrate %': '{:.2f}%', 'Total': '{:.0f}', '✅ GREEN': '{:.0f}', '❌ RED': '{:.0f}'}
                                st.dataframe(pivot.style.format(format_dict).highlight_max(axis=0, color='#1F4025'), use_container_width=True)
                            else: st.info("Nenhuma entrada Aprovada, Arriscada ou Sniper encontrada no período.")

                        st.markdown("### 📈 Performance por Estratégia")
                        st_s = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        if not st_s.empty:
                            resumo_strat = st_s.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)
                            if '✅ GREEN' in resumo_strat.columns and '❌ RED' in resumo_strat.columns:
                                resumo_strat['Winrate'] = (resumo_strat['✅ GREEN'] / (resumo_strat['✅ GREEN'] + resumo_strat['❌ RED']) * 100)
                                format_strat = {'Winrate': '{:.2f}%'}
                                for c in resumo_strat.columns:
                                    if c != 'Winrate': format_strat[c] = '{:.0f}'
                                st.dataframe(resumo_strat.sort_values('Winrate', ascending=False).style.format(format_strat), use_container_width=True)
                            cts = st_s.groupby(['Estrategia', 'Resultado']).size().reset_index(name='Qtd')
                            fig = px.bar(cts, x='Estrategia', y='Qtd', color='Resultado', color_discrete_map={'✅ GREEN': '#00FF00', '❌ RED': '#FF0000'}, title="Volume de Sinais", text='Qtd')
                            fig.update_layout(template="plotly_dark"); st.plotly_chart(fig, use_container_width=True)
                except Exception as e: st.error(f"Erro BI: {e}")

        with abas[5]: st.dataframe(st.session_state['df_black'][['País', 'Liga', 'Motivo']], use_container_width=True, hide_index=True)
        
        with abas[6]: 
            df_safe_show = st.session_state.get('df_safe', pd.DataFrame()).copy()
            if not df_safe_show.empty:
                def calc_risco(x):
                    try: v = int(float(str(x)))
                    except: v = 0
                    return "🟢 100% Estável" if v == 0 else f"⚠️ Atenção ({v}/10)"
                df_safe_show['Status Risco'] = df_safe_show['Strikes'].apply(calc_risco)
                st.dataframe(df_safe_show[['País', 'Liga', 'Motivo', 'Status Risco']], use_container_width=True, hide_index=True)
            else: st.info("Nenhuma liga segura ainda.")

        with abas[7]: 
            df_vip_show = st.session_state.get('df_vip', pd.DataFrame()).copy()
            if not df_vip_show.empty: 
                df_vip_show['Strikes_Num'] = pd.to_numeric(df_vip_show['Strikes'], errors='coerce').fillna(0).astype(int)
                df_vip_show = df_vip_show.sort_values(by='Strikes_Num', ascending=False)
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
                            total_docs = 0
                            try:
                                count_query = db_firestore.collection("BigData_Futebol").count()
                                res_count = count_query.get()
                                total_docs = res_count[0][0].value
                            except:
                                docs_all = db_firestore.collection("BigData_Futebol").select([]).stream()
                                total_docs = sum(1 for _ in docs_all)
                            st.session_state['total_bigdata_count'] = total_docs
                            
                            docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(50).stream()
                            data = [d.to_dict() for d in docs]
                            st.session_state['cache_firebase_view'] = data 
                            st.toast(f"Dados atualizados! Total: {total_docs}")
                    except Exception as e: st.error(f"Erro ao ler Firebase: {e}")
                
                if st.session_state.get('total_bigdata_count', 0) > 0:
                    st.metric("Total de Jogos Armazenados", st.session_state['total_bigdata_count'])

                if 'cache_firebase_view' in st.session_state and st.session_state['cache_firebase_view']:
                    st.success(f"📂 Visualizando {len(st.session_state['cache_firebase_view'])} registros (Cache Local)")
                    st.dataframe(pd.DataFrame(st.session_state['cache_firebase_view']), use_container_width=True)
                else: st.info("ℹ️ Clique no botão acima para visualizar os dados salvos (Isso consome leituras da cota).")
            else: st.warning("⚠️ Firebase não conectado.")

        for i in range(INTERVALO, 0, -1):
            st.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()
else:
    with placeholder_root.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera. Configure na lateral.")    
