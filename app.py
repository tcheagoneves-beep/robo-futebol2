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

# --- IMPORTAÇÕES FIREBASE ---
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
    .stDataFrame { font-size: 13px; }
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

# 2.1 Conexão Firebase
db_firestore = None
if "FIREBASE_CONFIG" in st.secrets:
    try:
        if not firebase_admin._apps:
            fb_creds = json.loads(st.secrets["FIREBASE_CONFIG"])
            cred = credentials.Certificate(fb_creds)
            firebase_admin.initialize_app(cred)
        db_firestore = firestore.client()
    except Exception as e: st.error(f"Erro Firebase: {e}")

# 3. Conexão IA e Google Sheets
IA_ATIVADA = False
try:
    if "GEMINI_KEY" in st.secrets:
        genai.configure(api_key=st.secrets["GEMINI_KEY"])
        model_ia = genai.GenerativeModel('gemini-2.0-flash') 
        IA_ATIVADA = True
except: IA_ATIVADA = False

conn = st.connection("gsheets", type=GSheetsConnection)

# --- COLUNAS ATUALIZADAS (COM OPINIAO_IA) ---
COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID', 'Odd', 'Odd_Atualizada', 'Opiniao_IA']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo', 'Strikes', 'Jogos_Erro']
COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes', 'Jogos_Erro']
COLS_BLACK = ['id', 'País', 'Liga', 'Motivo']
# Constantes Extras
LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]
DB_CACHE_TIME = 60
STATIC_CACHE_TIME = 600

# ==============================================================================
# 4. FUNÇÕES UTILITÁRIAS E DE DADOS
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

def gerar_barra_pressao(rh, ra):
    try:
        max_blocos = 5
        nivel_h = min(int(rh), max_blocos); nivel_a = min(int(ra), max_blocos)
        if nivel_h > nivel_a: return "🟩" * nivel_h + "⬜" * (max_blocos - nivel_h) + " (Casa)"
        elif nivel_a > nivel_h: return "🟥" * nivel_a + "⬜" * (max_blocos - nivel_a) + " (Visitante)"
        elif nivel_h > 0 and nivel_a > 0: return "🟨🟨 Jogo Aberto"
        else: return ""
    except: return ""

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

# --- EXTRAÇÃO DE DADOS COMPLETA (PEDIDO 2) ---
def extrair_dados_completos(stats_api):
    if not stats_api: return "Dados indisponíveis."
    try:
        s1 = stats_api[0]['statistics']; s2 = stats_api[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        
        texto = f"""
        📊 ESTATÍSTICAS COMPLETAS (Casa x Visitante):
        - Posse: {gv(s1, 'Ball Possession')} x {gv(s2, 'Ball Possession')}
        - Chutes Totais: {gv(s1, 'Total Shots')} x {gv(s2, 'Total Shots')}
        - Chutes no Gol: {gv(s1, 'Shots on Goal')} x {gv(s2, 'Shots on Goal')}
        - Chutes na Área: {gv(s1, 'Shots insidebox')} x {gv(s2, 'Shots insidebox')}
        - Chutes Bloqueados: {gv(s1, 'Blocked Shots')} x {gv(s2, 'Blocked Shots')}
        - Escanteios: {gv(s1, 'Corner Kicks')} x {gv(s2, 'Corner Kicks')}
        - Ataques: {gv(s1, 'Attacks')} x {gv(s2, 'Attacks')}
        - Ataques Perigosos: {gv(s1, 'Dangerous Attacks')} x {gv(s2, 'Dangerous Attacks')}
        - Faltas: {gv(s1, 'Fouls')} x {gv(s2, 'Fouls')}
        - Cartões (A/V): {gv(s1, 'Yellow Cards')}/{gv(s1, 'Red Cards')} x {gv(s2, 'Yellow Cards')}/{gv(s2, 'Red Cards')}
        - Passes (Precisão): {gv(s1, 'Passes %')} x {gv(s2, 'Passes %')}
        """
        return texto
    except: return "Erro stats."

# --- FUNÇÕES DE BANCO DE DADOS (COM CORREÇÃO DE COLUNAS) ---
def carregar_aba(nome_aba, colunas_esperadas):
    try:
        df = conn.read(worksheet=nome_aba, ttl=0)
        if not df.empty:
            # Garante que novas colunas (como Opiniao_IA) existam
            for col in colunas_esperadas:
                if col not in df.columns:
                    if col == 'Odd': df[col] = "1.20"
                    else: df[col] = ""
        if df.empty or len(df.columns) < len(colunas_esperadas): 
            return pd.DataFrame(columns=colunas_esperadas)
        return df.fillna("").astype(str)
    except: return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df_para_salvar):
    try: 
        if nome_aba == "Historico" and df_para_salvar.empty: return False
        conn.update(worksheet=nome_aba, data=df_para_salvar)
        return True
    except: return False

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
            strikes_raw = df_vip.loc[mask_vip, 'Strikes'].values[0]
            strikes = formatar_inteiro_visual(strikes_raw)
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
        st.toast(f"🚫 {nome_liga} Banida!")
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
        # Normalização de IDs
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
                fid_strat = f"{item['FID']}_{item['Estrategia']}"
                st.session_state['alertas_enviados'].add(fid_strat)
                if 'GREEN' in str(item['Resultado']): st.session_state['alertas_enviados'].add(f"RES_GREEN_{fid_strat}")
                if 'RED' in str(item['Resultado']): st.session_state['alertas_enviados'].add(f"RES_RED_{fid_strat}")
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
            if str(row['Resultado']) != str(nova_linha['Resultado']) or str(row['Odd']) != str(nova_linha['Odd']):
                st.session_state['precisa_salvar'] = True
            return nova_linha
        return row
    df_final = df_memoria.apply(atualizar_linha, axis=1)
    st.session_state['historico_full'] = df_final

# --- SALVAMENTO BIG DATA COMPLETO (PEDIDO 1) ---
def salvar_bigdata(jogo_api, stats):
    if not db_firestore: return
    try:
        fid = str(jogo_api['fixture']['id'])
        if fid in st.session_state['jogos_salvos_bigdata']: return 

        s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        
        # Estrutura Expandida para Big Data
        item_bigdata = {
            'fid': fid,
            'data_hora': get_time_br().strftime('%Y-%m-%d %H:%M'),
            'liga': jogo_api['league']['name'],
            'jogo': f"{jogo_api['teams']['home']['name']} x {jogo_api['teams']['away']['name']}",
            'placar_final': f"{jogo_api['goals']['home']}x{jogo_api['goals']['away']}",
            'estatisticas': {
                'chutes_total': gv(s1, 'Total Shots') + gv(s2, 'Total Shots'),
                'chutes_gol': gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal'),
                'chutes_fora': gv(s1, 'Shots off Goal') + gv(s2, 'Shots off Goal'),
                'chutes_area': gv(s1, 'Shots insidebox') + gv(s2, 'Shots insidebox'),
                'chutes_bloqueados': gv(s1, 'Blocked Shots') + gv(s2, 'Blocked Shots'),
                'escanteios': gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks'),
                'ataques': gv(s1, 'Attacks') + gv(s2, 'Attacks'),
                'ataques_perigosos': gv(s1, 'Dangerous Attacks') + gv(s2, 'Dangerous Attacks'),
                'faltas': gv(s1, 'Fouls') + gv(s2, 'Fouls'),
                'posse_casa': str(gv(s1, 'Ball Possession')),
                'cartoes_amarelos': gv(s1, 'Yellow Cards') + gv(s2, 'Yellow Cards'),
                'cartoes_vermelhos': gv(s1, 'Red Cards') + gv(s2, 'Red Cards'),
                'passes_total': gv(s1, 'Passes Total') + gv(s2, 'Passes Total'),
                'precisao_passes': f"{gv(s1, 'Passes %')}/{gv(s2, 'Passes %')}"
            }
        }
        db_firestore.collection("BigData_Futebol").document(fid).set(item_bigdata)
        st.session_state['jogos_salvos_bigdata'].add(fid)
    except Exception as e: print(f"Erro Firebase: {e}")

def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    df_raw = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

# --- CACHES ---
@st.cache_data(ttl=86400)
def buscar_ranking(api_key, league_id, season):
    try:
        url = "https://v3.football.api-sports.io/standings"
        params = {"league": league_id, "season": season}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        ranking = {}
        if res.get('response'):
            for team in res['response'][0]['league']['standings'][0]: ranking[team['team']['name']] = team['rank']
        return ranking
    except: return {}

@st.cache_data(ttl=3600) 
def buscar_agenda_cached(api_key, date_str):
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        return requests.get(url, headers={"x-apisports-key": api_key}, params={"date": date_str, "timezone": "America/Sao_Paulo"}).json().get('response', [])
    except: return []

# --- ODDS (PEDIDO 5 - Ajuste para 1.20) ---
def get_live_odds(fixture_id, api_key, strategy_name, total_gols_atual=0):
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
        
        best_odd = "0.00"
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
                    for v in m['values']:
                        try:
                            raw_odd = float(v['odd'])
                            if raw_odd > 50: raw_odd = raw_odd / 1000
                            if raw_odd > 1.20:
                                if best_odd == "0.00": best_odd = "{:.2f}".format(raw_odd)
                        except: pass
        if best_odd == "0.00": return "1.20" # AJUSTADO PARA 1.20
        return best_odd
    except: return "1.20"

def buscar_inteligencia(estrategia, liga, jogo):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "\n🔮 <b>Prob: Sem Histórico</b>"
    try:
        times = jogo.split(' x ')
        time_casa = times[0].split('(')[0].strip()
        time_visitante = times[1].split('(')[0].strip()
    except: return "\n🔮 <b>Prob: Erro Nome</b>"
    
    numerador = 0; denominador = 0; fontes = []
    f_casa = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_casa, na=False))]
    f_vis = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_visitante, na=False))]
    
    if len(f_casa) >= 3 or len(f_vis) >= 3:
        wr_c = (f_casa['Resultado'].str.contains('GREEN').sum()/len(f_casa)*100) if len(f_casa)>=3 else 0
        wr_v = (f_vis['Resultado'].str.contains('GREEN').sum()/len(f_vis)*100) if len(f_vis)>=3 else 0
        div = 2 if (len(f_casa)>=3 and len(f_vis)>=3) else 1
        numerador += ((wr_c + wr_v)/div) * 5; denominador += 5; fontes.append("Time")

    f_liga = df[(df['Estrategia'] == estrategia) & (df['Liga'] == liga)]
    if len(f_liga) >= 3:
        wr_l = (f_liga['Resultado'].str.contains('GREEN').sum()/len(f_liga)*100)
        numerador += wr_l * 3; denominador += 3; fontes.append("Liga")
    
    f_geral = df[df['Estrategia'] == estrategia]
    if len(f_geral) >= 1:
        wr_g = (f_geral['Resultado'].str.contains('GREEN').sum()/len(f_geral)*100)
        numerador += wr_g * 1; denominador += 1
        
    if denominador == 0: return "\n🔮 <b>Prob: Calculando...</b>"
    prob_final = numerador / denominador
    str_fontes = "+".join(fontes) if fontes else "Geral"
    return f"\n{'🔥' if prob_final >= 80 else '🔮' if prob_final > 40 else '⚠️'} <b>Prob: {prob_final:.0f}% ({str_fontes})</b>"

def calcular_odd_media_historica(estrategia):
    df = st.session_state.get('historico_full', pd.DataFrame())
    PADRAO_CONSERVADOR = 1.20 # AJUSTADO
    TETO_MAXIMO_FALLBACK = 1.50
    if df.empty: return PADRAO_CONSERVADOR
    try:
        df_strat = df[df['Estrategia'] == estrategia].copy()
        df_strat['Odd_Num'] = pd.to_numeric(df_strat['Odd'], errors='coerce')
        df_validas = df_strat[(df_strat['Odd_Num'] > 1.15) & (df_strat['Odd_Num'] < 50.0)]
        if df_validas.empty: return PADRAO_CONSERVADOR
        media = df_validas['Odd_Num'].mean()
        media_segura = min(media, TETO_MAXIMO_FALLBACK)
        if media_segura < 1.15: return PADRAO_CONSERVADOR
        return float(f"{media_segura:.2f}")
    except: return PADRAO_CONSERVADOR

def obter_odd_final_para_calculo(odd_registro, estrategia):
    try:
        valor = float(odd_registro)
        if valor <= 1.15: return calcular_odd_media_historica(estrategia)
        return valor
    except: return calcular_odd_media_historica(estrategia)

# --- IA COM AUDITORIA (PEDIDO 2 e 6) ---
def consultar_ia_gemini(dados_jogo, estrategia, stats_raw):
    if not IA_ATIVADA: return ""
    df = st.session_state.get('historico_full', pd.DataFrame())
    resumo_historico = "Sem dados históricos."
    if not df.empty:
        df_strat = df[df['Estrategia'] == estrategia]
        total = len(df_strat)
        if total > 0:
            greens = len(df_strat[df_strat['Resultado'].str.contains('GREEN', na=False)])
            reds = len(df_strat[df_strat['Resultado'].str.contains('RED', na=False)])
            winrate = (greens / total * 100)
            resumo_historico = f"Histórico: {winrate:.1f}% Winrate em {total} jogos."

    if st.session_state['ia_bloqueada_ate']:
        agora = datetime.now()
        if agora < st.session_state['ia_bloqueada_ate']: return ""
        else: st.session_state['ia_bloqueada_ate'] = None
        
    dados_ricos = extrair_dados_completos(stats_raw)
    
    prompt = f"""
    Trader Esportivo Sênior. Valide entrada no mercado de GOLS (Over).
    
    JOGO: {dados_jogo['jogo']} ({dados_jogo['tempo']}min - Placar: {dados_jogo['placar']})
    ESTRATÉGIA: {estrategia}
    {resumo_historico}
    
    STATS AO VIVO:
    {dados_ricos}
    
    CRITÉRIOS DE DECISÃO:
    1. Aprove se houver PRESSÃO REAL (Muitos chutes na área, escanteios, ataques perigosos).
    2. Recuse (Arriscado) se for pressão falsa (Posse inútil, chutes de longe, jogo parado com muitas faltas).
    3. Cartão vermelho para favorito = Arriscado.
    
    Responda ESTRITAMENTE neste formato: "Aprovado" ou "Arriscado" | [Motivo max 6 palavras]
    """
    
    try:
        response = model_ia.generate_content(prompt, request_options={"timeout": 12})
        st.session_state['gemini_usage']['used'] += 1
        return f"\n🤖 <b>IA:</b> {response.text.strip()}"
    except Exception as e:
        if "429" in str(e) or "quota" in str(e).lower():
            st.session_state['ia_bloqueada_ate'] = datetime.now() + timedelta(minutes=2)
            return "\n🤖 <b>IA:</b> (Pausa 2m)"
        return ""

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
        prompt = f"""
        Analise o dia ({hoje_str}): Total {total}, Greens {greens}.
        Estratégias: {json.dumps(resumo)}
        Dê 3 insights táticos curtos para melhorar amanhã.
        """
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
            if 'GREEN' in res:
                lucro_total += (stake * odd_final) - stake; investido += stake
            elif 'RED' in res:
                lucro_total -= stake; investido += stake
        roi = (lucro_total / investido * 100) if investido > 0 else 0
        prompt_fin = f"""
        Analista Financeiro. Hoje:
        Investido: R$ {investido:.2f} | Lucro: R$ {lucro_total:.2f} | ROI: {roi:.2f}%
        A curva de capital está {'Crescente' if lucro_total > 0 else 'Decrescente'}.
        Dê um feedback de gestão de risco curto e direto.
        """
        response = model_ia.generate_content(prompt_fin)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except: return "Erro Fin."

def criar_estrategia_nova_ia():
    if not IA_ATIVADA: return "IA Desconectada."
    if db_firestore:
        try:
            docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(100).stream()
            data = [d.to_dict() for d in docs]
            if len(data) < 5: return "Coletando dados no Firebase... Aguarde mais jogos."
            amostra = json.dumps(data)
        except: return "Erro ao ler Firebase."
    else: return "Firebase Offline."

    try:
        prompt_criacao = f"""
        Cientista de Dados. Analise esta amostra de 100 jogos (JSON): {amostra}
        Encontre um padrão ESTATÍSTICO GLOBAL onde a probabilidade de gol no final é > 80%.
        Saída: Nome da Estratégia, Regra Lógica (ex: Chutes > X e Posse < Y) e Explicação.
        """
        response = model_ia.generate_content(prompt_criacao)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except: return "Erro criação."

# --- TELEGRAM REPORT BI (PEDIDO 3) ---
def enviar_relatorio_bi(token, chat_ids):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    try:
        df = df.copy()
        df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
        df['Data_DT'] = pd.to_datetime(df['Data_Str'], errors='coerce')
        df = df.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
        
        hoje = pd.to_datetime(get_time_br().date())
        # Filtros de Tempo
        d_hoje = df[df['Data_DT'] == hoje]
        d_7d = df[df['Data_DT'] >= (hoje - timedelta(days=7))]
        d_30d = df[df['Data_DT'] >= (hoje - timedelta(days=30))]
        
        def calc_resumo(d):
            t = len(d); g = d['Resultado'].str.contains('GREEN').sum(); r = d['Resultado'].str.contains('RED').sum()
            wr = (g/t*100) if t>0 else 0
            return f"{g}G-{r}R ({wr:.0f}%)"

        msg = f"""📊 <b>RELATÓRIO DE PERFORMANCE</b>
        
        📆 <b>Hoje:</b> {calc_resumo(d_hoje)}
        WK <b>7 Dias:</b> {calc_resumo(d_7d)}
        MO <b>30 Dias:</b> {calc_resumo(d_30d)}
        ∞ <b>Total:</b> {calc_resumo(df)}
        """
        ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
        for cid in ids:
            t = threading.Thread(target=_worker_telegram, args=(token, cid, msg))
            t.daemon = True; t.start()
    except: pass

def _worker_telegram(token, chat_id, msg):
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=5)
    except: pass

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        t = threading.Thread(target=_worker_telegram, args=(token, cid, msg))
        t.daemon = True; t.start()

def enviar_analise_estrategia(token, chat_ids):
    sugestao = criar_estrategia_nova_ia()
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    msg = f"🧪 <b>LABORATÓRIO BIG DATA (IA)</b>\n\n{sugestao}"
    for cid in ids: enviar_telegram(token, cid, msg)

def enviar_relatorio_financeiro(token, chat_ids, cenario, lucro, roi, entradas):
    msg = f"💰 <b>RELATÓRIO FINANCEIRO</b>\n\n📊 <b>Cenário:</b> {cenario}\n💵 <b>Lucro Líquido:</b> R$ {lucro:.2f}\n📈 <b>ROI:</b> {roi:.1f}%\n🎟️ <b>Entradas:</b> {entradas}\n\n<i>Cálculo baseado na gestão configurada.</i>"
    enviar_telegram(token, chat_ids, msg)

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
    if agora.hour == 23 and agora.minute >= 35 and not st.session_state['ia_enviada']:
        analise = analisar_bi_com_ia()
        msg_ia = f"🧠 <b>CONSULTORIA DIÁRIA DA IA</b>\n\n{analise}"
        enviar_telegram(token, chat_ids, msg_ia)
        st.session_state['ia_enviada'] = True
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
        if insights and "Sem insights" not in insights:
            ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
            msg_final = f"🌅 <b>INSIGHTS MATINAIS (IA + API)</b>\n\n{insights}"
            for cid in ids: enviar_telegram(token, cid, msg_final)
            st.session_state['matinal_enviado'] = True

def gerar_insights_matinais_ia(api_key):
    if not IA_ATIVADA: return "IA Offline."
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        LIGAS_TOP = [71, 72, 39, 140, 78, 135, 61, 2]
        jogos_top = [j for j in jogos if j['league']['id'] in LIGAS_TOP][:5] 
        if not jogos_top: return "Nenhum jogo 'Top Tier' para análise matinal hoje."
        relatorio_final = ""
        for j in jogos_top:
            fid = j['fixture']['id']
            time_casa = j['teams']['home']['name']; time_fora = j['teams']['away']['name']
            ja_enviado = False
            for s in st.session_state['historico_sinais']:
                if str(s['FID']) == str(fid) and "Sniper" in s['Estrategia']:
                    ja_enviado = True; break
            if ja_enviado: continue
            url_pred = "https://v3.football.api-sports.io/predictions"
            res_pred = requests.get(url_pred, headers={"x-apisports-key": api_key}, params={"fixture": fid}).json()
            if res_pred.get('response'):
                pred = res_pred['response'][0]['predictions']
                comp = res_pred['response'][0]['comparison']
                info_jogo = f"JOGO: {time_casa} vs {time_fora} | API Diz: {pred['advice']} | Prob: {pred['percent']} | Ataque: {comp['att']['home']}x{comp['att']['away']}"
                prompt_matinal = f"""
                Analise: {info_jogo}
                Se tiver oportunidade MUITO CLARA, responda ESTRITAMENTE no formato:
                BET: [TIPO_APOSTA]
                Tipos aceitos: OVER 2.5, UNDER 2.5, CASA VENCE, FORA VENCE, AMBAS MARCAM.
                Se não, responda: SKIP
                """
                resp_ia = model_ia.generate_content(prompt_matinal)
                st.session_state['gemini_usage']['used'] += 1
                texto_ia = resp_ia.text.strip().upper()
                if "BET:" in texto_ia:
                    aposta = texto_ia.replace("BET:", "").strip()
                    relatorio_final += f"🎯 <b>SNIPER: {time_casa} x {time_fora}</b>\n👉 {aposta}\n\n"
                    item_bi = {
                        "FID": str(fid), "Data": hoje, "Hora": "08:00", 
                        "Liga": j['league']['name'], "Jogo": f"{time_casa} x {time_fora}", 
                        "Placar_Sinal": aposta, 
                        "Estrategia": "Sniper Matinal", "Resultado": "Pendente", 
                        "HomeID": str(j['teams']['home']['id']), "AwayID": str(j['teams']['away']['id']), 
                        "Odd": "1.70", "Odd_Atualizada": "", "Opiniao_IA": "Sniper"
                    }
                    adicionar_historico(item_bi)
                    time.sleep(1)
        return relatorio_final if relatorio_final else "Sem oportunidades claras no Sniper."
    except Exception as e: return f"Erro Matinal: {e}"

def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0
    ga = jogo_api['goals']['away'] or 0
    st_short = jogo_api['fixture']['status']['short']
    STRATS_HT_ONLY = ["Gol Relâmpago", "Massacre", "Choque Líderes", "Briga de Rua"]
    if "Múltipla" in sinal['Estrategia'] or "Sniper" in sinal['Estrategia']: return False
    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: return False
    fid = clean_fid(sinal['FID'])
    strat = str(sinal['Estrategia'])
    key_green = f"RES_GREEN_{fid}_{strat}"
    key_red = f"RES_RED_{fid}_{strat}"
    if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
    if (gh+ga) > (ph+pa):
        if "Morno" in sinal['Estrategia']: 
            if (gh+ga) >= 2:
                sinal['Resultado'] = '❌ RED'
                if key_red not in st.session_state['alertas_enviados']:
                    enviar_telegram(token, chats, f"❌ <b>RED | OVER 1.5 BATIDO</b>\n⚽ {sinal['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {sinal['Estrategia']}")
                    st.session_state['alertas_enviados'].add(key_red)
                    st.session_state['precisa_salvar'] = True
                return True
            else: return False 
        else:
            sinal['Resultado'] = '✅ GREEN'
            if key_green in st.session_state['alertas_enviados']: return True
            enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📈 Placar: <b>{gh}x{ga}</b>\n🎯 {sinal['Estrategia']}")
            st.session_state['alertas_enviados'].add(key_green)
            st.session_state['precisa_salvar'] = True 
            return True
    eh_ht_strat = any(x in sinal['Estrategia'] for x in STRATS_HT_ONLY)
    if eh_ht_strat and st_short in ['HT', '2H', 'FT', 'AET', 'PEN', 'ABD']:
        sinal['Resultado'] = '❌ RED'
        if key_red not in st.session_state['alertas_enviados']:
            enviar_telegram(token, chats, f"❌ <b>RED | INTERVALO (HT)</b>\n⚽ {sinal['Jogo']}\n📉 Placar HT: {gh}x{ga}\n🎯 {sinal['Estrategia']} (Não bateu no 1º Tempo)")
            st.session_state['alertas_enviados'].add(key_red)
            st.session_state['precisa_salvar'] = True 
        return True
    if st_short in ['FT', 'AET', 'PEN', 'ABD']:
        if "Morno" in sinal['Estrategia'] and (gh+ga) <= 1:
             sinal['Resultado'] = '✅ GREEN'
             if key_green not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"✅ <b>GREEN | UNDER BATIDO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}")
                st.session_state['alertas_enviados'].add(key_green)
                st.session_state['precisa_salvar'] = True 
             return True
        sinal['Resultado'] = '❌ RED'
        if key_red not in st.session_state['alertas_enviados']:
            enviar_telegram(token, chats, f"❌ <b>RED | ENCERRADO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {sinal['Estrategia']}")
            st.session_state['alertas_enviados'].add(key_red)
            st.session_state['precisa_salvar'] = True 
        return True
    return False

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    hist = st.session_state['historico_sinais']
    pendentes = [s for s in hist if s['Resultado'] == 'Pendente']
    if not pendentes: return
    hoje_str = get_time_br().strftime('%Y-%m-%d')
    agora = get_time_br()
    ids_live = [j['fixture']['id'] for j in jogos_live]
    updates_buffer = []
    for s in pendentes:
        if s.get('Data') != hoje_str: continue
        fid = int(clean_fid(s.get('FID', 0)))
        strat = str(s.get('Estrategia', ''))
        key_green = f"RES_GREEN_{str(fid)}_{strat}"
        if key_green in st.session_state.get('alertas_enviados', set()):
            s['Resultado'] = '✅ GREEN'
            updates_buffer.append(s)
            continue 
        if 'Odd_Atualizada' not in s: s['Odd_Atualizada'] = False
        try:
            hora_str = f"{s['Data']} {s['Hora']}"
            dt_sinal = datetime.strptime(hora_str, '%Y-%m-%d %H:%M')
            dt_sinal = pytz.timezone('America/Sao_Paulo').localize(dt_sinal)
            minutos_passados = (agora - dt_sinal).total_seconds() / 60
            
            odd_atual_memoria = float(str(s.get('Odd', 0)))
            if minutos_passados >= 3 and (not s.get('Odd_Atualizada') or odd_atual_memoria <= 1.15):
                jogo_live = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
                total_gols = (jogo_live['goals']['home'] or 0) + (jogo_live['goals']['away'] or 0) if jogo_live else 0
                nova_odd = get_live_odds(fid, api_key, s['Estrategia'], total_gols)
                if nova_odd != s['Odd']:
                    s['Odd'] = nova_odd
                    s['Odd_Atualizada'] = True
                    updates_buffer.append(s)
        except: pass
        jogo_encontrado = None
        if fid > 0 and fid in ids_live: jogo_encontrado = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        elif fid > 0:
            try:
                res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                if res['response']: jogo_encontrado = res['response'][0]
            except: pass
        if jogo_encontrado:
            if processar_resultado(s, jogo_encontrado, token, chats): updates_buffer.append(s)
    if updates_buffer: atualizar_historico_ram(updates_buffer)

def conferir_resultados_sniper(jogos_live):
    hist = st.session_state.get('historico_sinais', [])
    snipers_pendentes = [s for s in hist if s['Estrategia'] == "Sniper Matinal" and s['Resultado'] == "Pendente"]
    if not snipers_pendentes: return
    updates_buffer = []
    ids_live_ou_fim = {str(j['fixture']['id']): j for j in jogos_live} 
    for s in snipers_pendentes:
        fid = str(s['FID'])
        if fid in ids_live_ou_fim:
            jogo = ids_live_ou_fim[fid]
            status = jogo['fixture']['status']['short']
            if status in ['FT', 'AET', 'PEN']:
                gh = jogo['goals']['home'] or 0; ga = jogo['goals']['away'] or 0
                total_gols = gh + ga
                target = s['Placar_Sinal'] 
                resultado_final = None
                if "OVER 2.5" in target: resultado_final = '✅ GREEN' if total_gols > 2.5 else '❌ RED'
                elif "UNDER 2.5" in target: resultado_final = '✅ GREEN' if total_gols < 2.5 else '❌ RED'
                elif "AMBAS MARCAM" in target: resultado_final = '✅ GREEN' if (gh > 0 and ga > 0) else '❌ RED'
                elif "CASA VENCE" in target: resultado_final = '✅ GREEN' if gh > ga else '❌ RED'
                elif "FORA VENCE" in target: resultado_final = '✅ GREEN' if ga > gh else '❌ RED'
                if resultado_final:
                    s['Resultado'] = resultado_final
                    updates_buffer.append(s)
                    enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], 
                                    f"{resultado_final} <b>RESULTADO SNIPER</b>\n⚽ {s['Jogo']}\n🎯 {target}\n📉 Placar: {gh}x{ga}")
    if updates_buffer: atualizar_historico_ram(updates_buffer)

def verificar_var_rollback(jogos_live, token, chats):
    hist = st.session_state['historico_sinais']
    greens = [s for s in hist if 'GREEN' in str(s['Resultado'])]
    if not greens: return
    updates_buffer = []
    for s in greens:
        if "Morno" in s['Estrategia']: continue
        fid = int(clean_fid(s.get('FID', 0)))
        jogo_api = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        if jogo_api:
            gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
            try:
                ph, pa = map(int, s['Placar_Sinal'].split('x'))
                if (gh + ga) <= (ph + pa):
                    key_var = f"VAR_{fid}_{s['Estrategia']}_{gh}x{ga}"
                    s['Resultado'] = 'Pendente'
                    st.session_state['precisa_salvar'] = True
                    updates_buffer.append(s)
                    if key_var not in st.session_state['alertas_enviados']:
                        msg = (f"⚠️ <b>VAR ACIONADO | GOL ANULADO</b>\n\n⚽ {s['Jogo']}\n📉 Placar voltou para: <b>{gh}x{ga}</b>\n🔄 Status revertido para <b>PENDENTE</b>.")
                        enviar_telegram(token, chats, msg)
                        st.session_state['alertas_enviados'].add(key_var)
            except: pass
    if updates_buffer: atualizar_historico_ram(updates_buffer)

def reenviar_sinais(token, chats):
    hist = st.session_state['historico_sinais']
    if not hist: return st.toast("Sem sinais.")
    st.toast("Reenviando...")
    for s in reversed(hist):
        prob = buscar_inteligencia(s['Estrategia'], s['Liga'], s['Jogo'])
        enviar_telegram(token, chats, f"🔄 <b>REENVIO</b>\n\n🚨 {s['Estrategia']}\n⚽ {s['Jogo']}\n⚠️ Placar: {s.get('Placar_Sinal','?')}{prob}")
        time.sleep(0.5)

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

def deve_buscar_stats(tempo, gh, ga, status):
    if 5 <= tempo <= 15: return True
    if 70 <= tempo <= 85 and abs(gh - ga) <= 1: return True
    if status == 'HT': return True
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
        futures = {}
        for j in jogos_alvo:
            futures[executor.submit(fetch_stats_single, j['fixture']['id'], api_key)] = j
            time.sleep(0.2)
        for future in as_completed(futures):
            fid, stats, headers = future.result()
            if stats:
                resultados[fid] = stats
                update_api_usage(headers)
    return resultados

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        rh, ra = momentum(j['fixture']['id'], sog_h, sog_a)
        SINAIS = []
        if 75 <= tempo <= 85 and (sh_h+sh_a) >= 16:
            SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": "Over Limit", "stats": f"{sh_h+sh_a} Chutes", "rh": rh, "ra": ra})
        return SINAIS
    except: return []

# ==============================================================================
# 5. LOOP PRINCIPAL
# ==============================================================================
with st.sidebar:
    st.title("❄️ Neves Analytics")
    st.session_state['API_KEY'] = st.text_input("Chave API:", value=st.session_state['API_KEY'], type="password")
    st.session_state['TG_TOKEN'] = st.text_input("Token Telegram:", value=st.session_state['TG_TOKEN'], type="password")
    st.session_state['TG_CHAT'] = st.text_input("Chat IDs:", value=st.session_state['TG_CHAT'])
    INTERVALO = st.slider("Ciclo (s):", 60, 300, 60)
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    
    st.markdown("---")
    if st.button("🧪 Criar Nova Estratégia (Big Data)"):
        with st.spinner("Analisando Big Data..."):
            res = criar_estrategia_nova_ia()
            st.success(res)

if st.session_state.ROBO_LIGADO:
    with placeholder_root.container():
        # Carregamento estático
        if 'df_black' not in st.session_state:
            st.session_state['df_black'] = carregar_aba("Blacklist", COLS_BLACK)
            st.session_state['df_safe'] = carregar_aba("Seguras", COLS_SAFE)
            st.session_state['historico_full'] = carregar_aba("Historico", COLS_HIST)

        api_key = st.session_state['API_KEY']
        jogos_live = []
        try:
            url = "https://v3.football.api-sports.io/fixtures"
            res = requests.get(url, headers={"x-apisports-key": api_key}, params={"live": "all"}).json()
            jogos_live = res.get('response', [])
        except: st.error("Erro Conexão API")

        if jogos_live:
            # 1. BIG DATA (FIREBASE)
            jogos_para_stats = []
            for j in jogos_live:
                fid = j['fixture']['id']
                st_short = j['fixture']['status']['short']
                tempo = j['fixture']['status']['elapsed'] or 0
                if st_short == 'FT' and str(fid) not in st.session_state['jogos_salvos_bigdata']:
                    jogos_para_stats.append(j)
                elif deve_buscar_stats(tempo, j['goals']['home'], j['goals']['away'], st_short):
                    jogos_para_stats.append(j)

            if jogos_para_stats:
                novas_stats = atualizar_stats_em_paralelo(jogos_para_stats, api_key)
                for fid, stats in novas_stats.items():
                    jogo_ref = next((x for x in jogos_para_stats if x['fixture']['id'] == fid), None)
                    if jogo_ref and jogo_ref['fixture']['status']['short'] == 'FT':
                        salvar_bigdata(jogo_ref, stats)
                    else:
                        st.session_state[f"st_{fid}"] = stats

            # 2. PROCESSAMENTO DE SINAIS
            radar = []
            for j in jogos_live:
                fid = j['fixture']['id']; tempo = j['fixture']['status']['elapsed'] or 0
                if j['fixture']['status']['short'] == 'FT': continue
                
                stats = st.session_state.get(f"st_{fid}")
                if stats:
                    sinais = processar(j, stats, tempo, f"{j['goals']['home']}x{j['goals']['away']}")
                    for s in sinais:
                        uid = f"{fid}_{s['tag']}"
                        if uid not in st.session_state['alertas_enviados']:
                            opiniao_txt = consultar_ia_gemini({'jogo': f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", 'tempo': tempo, 'placar': f"{j['goals']['home']}x{j['goals']['away']}"}, s['tag'], stats)
                            
                            # Captura a opinião da IA para o banco de dados
                            opiniao_db = "Neutro"
                            if "Aprovado" in opiniao_txt: opiniao_db = "Aprovado"
                            elif "Arriscado" in opiniao_txt: opiniao_db = "Arriscado"
                            
                            odd_live = get_live_odds(fid, api_key, s['tag'])
                            
                            item = {
                                "FID": fid, "Data": get_time_br().strftime('%Y-%m-%d'), 
                                "Hora": get_time_br().strftime('%H:%M'), 
                                "Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", 
                                "Placar_Sinal": f"{j['goals']['home']}x{j['goals']['away']}", 
                                "Estrategia": s['tag'], "Resultado": "Pendente", 
                                "HomeID": str(j['teams']['home']['id']), "AwayID": str(j['teams']['away']['id']), 
                                "Odd": odd_live, "Odd_Atualizada": "", "Opiniao_IA": opiniao_db
                            }
                            
                            if adicionar_historico(item):
                                msg = f"🚨 <b>SINAL: {s['tag']}</b>\n⚽ {j['teams']['home']['name']} x {j['teams']['away']['name']}\n⏰ {tempo}' min | Odd: @{odd_live}{opiniao_txt}"
                                enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], msg)
                                st.session_state['alertas_enviados'].add(uid)
                
                radar.append({"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", "Tempo": f"{tempo}'"})

            # VISUALIZAÇÃO BI (Aba 4)
            abas = st.tabs(["Radar", "Agenda", "Financeiro", "Histórico", "BI & Analytics", "Blacklist", "Seguras", "Obs", "Big Data"])
            with abas[4]:
                st.markdown("### 🧠 Performance da IA (Auditoria)")
                df_hist = st.session_state.get('historico_full', pd.DataFrame())
                if not df_hist.empty and 'Opiniao_IA' in df_hist.columns:
                    df_ia = df_hist[df_hist['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                    if not df_ia.empty:
                        # Tabela Dinâmica: Opinião IA x Resultado
                        pivot = pd.crosstab(df_ia['Opiniao_IA'], df_ia['Resultado'], margins=True)
                        st.dataframe(pivot, use_container_width=True)
                        
                        # Gráfico de Assertividade por Estratégia
                        st.markdown("### 📊 Assertividade por Estratégia")
                        resumo_strat = df_ia.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)
                        if '✅ GREEN' in resumo_strat.columns and '❌ RED' in resumo_strat.columns:
                            resumo_strat['Winrate'] = (resumo_strat['✅ GREEN'] / (resumo_strat['✅ GREEN'] + resumo_strat['❌ RED']) * 100).round(1)
                            st.dataframe(resumo_strat[['✅ GREEN', '❌ RED', 'Winrate']].sort_values('Winrate', ascending=False), use_container_width=True)

        for i in range(INTERVALO, 0, -1):
            st.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()
else:
    with placeholder_root.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera. Configure na lateral.")
