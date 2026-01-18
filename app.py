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
    .stDataFrame { font-size: 13px; }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. INICIALIZAÇÃO DE VARIÁVEIS (SESSION STATE)
# ==============================================================================
if 'TG_TOKEN' not in st.session_state: st.session_state['TG_TOKEN'] = ""
if 'TG_CHAT' not in st.session_state: st.session_state['TG_CHAT'] = ""
if 'API_KEY' not in st.session_state: st.session_state['API_KEY'] = ""
if 'ROBO_LIGADO' not in st.session_state: st.session_state.ROBO_LIGADO = False
if 'last_db_update' not in st.session_state: st.session_state['last_db_update'] = 0
if 'last_static_update' not in st.session_state: st.session_state['last_static_update'] = 0 
if 'bi_enviado_data' not in st.session_state: st.session_state['bi_enviado_data'] = ""
if 'confirmar_reset' not in st.session_state: st.session_state['confirmar_reset'] = False
if 'precisa_salvar' not in st.session_state: st.session_state['precisa_salvar'] = False
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

# NOVA VARIÁVEL PARA O RADAR NÃO TRAVAR
if 'buffer_bigdata' not in st.session_state: st.session_state['buffer_bigdata'] = []

# ==============================================================================
# 3. CONFIGURAÇÃO IA & CONEXÃO
# ==============================================================================
IA_ATIVADA = False
try:
    if "GEMINI_KEY" in st.secrets:
        genai.configure(api_key=st.secrets["GEMINI_KEY"])
        model_ia = genai.GenerativeModel('gemini-2.0-flash') 
        IA_ATIVADA = True
    else:
        st.error("⚠️ Chave GEMINI_KEY não encontrada nos Secrets!")
except Exception as e:
    st.error(f"❌ Erro ao conectar na IA: {e}")
    IA_ATIVADA = False

conn = st.connection("gsheets", type=GSheetsConnection)

COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID', 'Odd', 'Odd_Atualizada']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo', 'Strikes', 'Jogos_Erro']
COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes', 'Jogos_Erro']
COLS_BLACK = ['id', 'País', 'Liga', 'Motivo']
COLS_BIGDATA = ['FID', 'Data', 'Liga', 'Jogo', 'Placar_Final', 'Chutes_Total', 'Chutes_Gol', 'Escanteios', 'Posse_Casa', 'Cartoes']

LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]
DB_CACHE_TIME = 60
STATIC_CACHE_TIME = 600

# ==============================================================================
# 4. DEFINIÇÃO DE TODAS AS FUNÇÕES
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

def extrair_dados_completos(stats_api):
    if not stats_api: return "Dados indisponíveis."
    try:
        s1 = stats_api[0]['statistics']; s2 = stats_api[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        texto = f"""
        - Posse: {gv(s1, 'Ball Possession')} x {gv(s2, 'Ball Possession')}
        - Chutes Totais: {gv(s1, 'Total Shots')} x {gv(s2, 'Total Shots')}
        - Chutes Gol: {gv(s1, 'Shots on Goal')} x {gv(s2, 'Shots on Goal')}
        - Cantos: {gv(s1, 'Corner Kicks')} x {gv(s2, 'Corner Kicks')}
        - Ataques P.: {gv(s1, 'Dangerous Attacks')} x {gv(s2, 'Dangerous Attacks')}
        - CV: {gv(s1, 'Red Cards')} x {gv(s2, 'Red Cards')}
        """
        return texto
    except: return "Erro stats."

# --- FUNÇÕES DE BANCO DE DADOS ---
def carregar_aba(nome_aba, colunas_esperadas):
    try:
        df = conn.read(worksheet=nome_aba, ttl=0)
        if not df.empty:
            for col in colunas_esperadas:
                if col not in df.columns:
                    if col == 'Odd': df[col] = "1.10"
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
            mask_pendentes = (df['Resultado'] == 'Pendente')
            mask_hoje = (df['Data'] == hoje)
            st.session_state['historico_sinais'] = df[mask_pendentes | mask_hoje].to_dict('records')[::-1]
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
        try:
            df_bd_load = carregar_aba("BigData", COLS_BIGDATA)
            if not df_bd_load.empty:
                fids_existentes = set(df_bd_load['FID'].astype(str).values)
                st.session_state['jogos_salvos_bigdata'].update(fids_existentes)
            st.session_state['jogos_salvos_bigdata_carregados'] = True
        except: pass
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
    if df_memoria.empty: return
    mapa_atualizacao = {f"{row['FID']}_{row['Estrategia']}": row for row in lista_atualizada_hoje}
    def atualizar_linha(row):
        chave = f"{row['FID']}_{row['Estrategia']}"
        if chave in mapa_atualizacao:
            nova_linha = mapa_atualizacao[chave]
            if str(row['Resultado']) != str(nova_linha['Resultado']) or str(row['Odd']) != str(nova_linha['Odd']):
                st.session_state['precisa_salvar'] = True
            return nova_linha
        return row
    st.session_state['historico_full'] = df_memoria.apply(atualizar_linha, axis=1)
    hoje = get_time_br().strftime('%Y-%m-%d')
    df = st.session_state['historico_full']
    mask_pendentes = (df['Resultado'] == 'Pendente')
    mask_hoje = (df['Data'] == hoje)
    st.session_state['historico_sinais'] = df[mask_pendentes | mask_hoje].to_dict('records')[::-1]

# ==============================================================================
# MELHORIA: FUNÇÃO DE BIG DATA USANDO BUFFER (PARA NÃO TRAVAR O RADAR)
# ==============================================================================
def salvar_bigdata(jogo_api, stats):
    """Guarda na memória RAM para salvar tudo de uma vez às 23:50"""
    try:
        fid = str(jogo_api['fixture']['id'])
        # Ignora se já estiver no Sheets ou no buffer de hoje
        if fid in st.session_state['jogos_salvos_bigdata'] or any(x['FID'] == fid for x in st.session_state['buffer_bigdata']):
            return 

        home = jogo_api['teams']['home']['name']; away = jogo_api['teams']['away']['name']
        placar = f"{jogo_api['goals']['home']}x{jogo_api['goals']['away']}"
        s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        
        chutes = gv(s1, 'Total Shots') + gv(s2, 'Total Shots')
        gol = gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal')
        cantos = gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks')
        cartoes = gv(s1, 'Yellow Cards') + gv(s2, 'Yellow Cards') + gv(s1, 'Red Cards') + gv(s2, 'Red Cards')
        posse = f"{gv(s1, 'Ball Possession')}/{gv(s2, 'Ball Possession')}"
        
        novo_item = {
            'FID': fid, 'Data': get_time_br().strftime('%Y-%m-%d'), 
            'Liga': jogo_api['league']['name'], 'Jogo': f"{home} x {away}",
            'Placar_Final': placar, 'Chutes_Total': chutes, 'Chutes_Gol': gol,
            'Escanteios': cantos, 'Posse_Casa': str(posse), 'Cartoes': cartoes
        }
        # Adiciona ao buffer em vez de salvar no Sheets agora
        st.session_state['buffer_bigdata'].append(novo_item)
    except: pass

def consolidar_bigdata_no_sheets():
    """Roda apenas no horário programado ou manualmente"""
    if not st.session_state['buffer_bigdata']: return False
    try:
        df_sheets = carregar_aba("BigData", COLS_BIGDATA)
        df_novos = pd.DataFrame(st.session_state['buffer_bigdata'])
        df_final = pd.concat([df_sheets, df_novos], ignore_index=True).drop_duplicates(subset=['FID'])
        if salvar_aba("BigData", df_final):
            st.session_state['jogos_salvos_bigdata'].update(df_novos['FID'].tolist())
            st.session_state['buffer_bigdata'] = []
            return True
    except: pass
    return False

def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    df_raw = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

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
        if best_odd == "0.00": return "1.10"
        return best_odd
    except: return "1.10"

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
    PADRAO_CONSERVADOR = 1.20
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

def consultar_ia_gemini(dados_jogo, estrategia, stats_raw):
    if not IA_ATIVADA: return ""
    df = st.session_state.get('historico_full', pd.DataFrame())
    resumo_historico = "Sem dados históricos suficientes."
    if not df.empty:
        df_strat = df[df['Estrategia'] == estrategia]
        if len(df_strat) > 0:
            winrate = (len(df_strat[df_strat['Resultado'].str.contains('GREEN', na=False)]) / len(df_strat) * 100)
            resumo_historico = f"Winrate histórico robô nessa estratégia: {winrate:.1f}%."
    if st.session_state['ia_bloqueada_ate']:
        if datetime.now() < st.session_state['ia_bloqueada_ate']: return ""
        else: st.session_state['ia_bloqueada_ate'] = None
    dados_ricos = extrair_dados_completos(stats_raw)
    prompt = f"Analise: {dados_jogo['jogo']} {dados_jogo['tempo']}' {dados_jogo['placar']}. Estratégia: {estrategia}. Stats: {dados_ricos}. {resumo_historico}. Responda Aprovado/Arriscado + 8 palavras."
    try:
        response = model_ia.generate_content(prompt, request_options={"timeout": 10})
        st.session_state['gemini_usage']['used'] += 1
        return f"\n🤖 <b>IA:</b> {response.text.strip()}"
    except: return ""

def analisar_bi_com_ia():
    if not IA_ATIVADA: return "IA Desconectada."
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados."
    try:
        df_f = df[df['Resultado'].isin(['✅ GREEN', '❌ RED'])]
        total = len(df_f); greens = len(df_f[df_f['Resultado'].str.contains('GREEN')])
        prompt = f"Resumo Robô: Total {total}, Greens {greens}. Dê 3 dicas para amanhã."
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except: return "Erro BI IA"

def analisar_financeiro_com_ia(stake, banca):
    if not IA_ATIVADA: return "IA Offline."
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados."
    try:
        prompt = f"Gestor: Banca {banca}, Stake {stake}. feedback saúde financeira curto."
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except: return "Erro Fin IA"

def criar_estrategia_nova_ia():
    if not IA_ATIVADA: return "IA Desconectada."
    df_bd = carregar_aba("BigData", COLS_BIGDATA)
    if len(df_bd) < 5: return "Preciso de mais jogos no Big Data."
    try:
        amostra = df_bd.tail(50).to_csv(index=False)
        prompt = f"Analise CSV: {amostra}. Crie 1 padrão estatístico lucrativo (Nome, Regra)."
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except: return "Erro Criar Estrategia"

def _worker_telegram(token, chat_id, msg):
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=5)
    except: pass

def _worker_telegram_photo(token, chat_id, photo_buffer, caption):
    try:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        files = {'photo': photo_buffer}
        data = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'HTML'}
        requests.post(url, files=files, data=data, timeout=10)
    except: pass

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        t = threading.Thread(target=_worker_telegram, args=(token, cid, msg))
        t.daemon = True; t.start()

def enviar_analise_estrategia(token, chat_ids):
    msg = f"🧪 <b>LABORATÓRIO DE ESTRATÉGIAS (IA)</b>\n\n{criar_estrategia_nova_ia()}"
    enviar_telegram(token, chat_ids, msg)

def enviar_relatorio_financeiro(token, chat_ids, cenario, lucro, roi, entradas):
    msg = f"💰 <b>RELATÓRIO FINANCEIRO</b>\n\n📊 <b>Cenário:</b> {cenario}\n💵 <b>Lucro Líquido:</b> R$ {lucro:.2f}\n📈 <b>ROI:</b> {roi:.1f}%\n🎟️ <b>Entradas:</b> {entradas}"
    enviar_telegram(token, chat_ids, msg)

def enviar_relatorio_bi(token, chat_ids):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    msg = f"📊 <b>RELATÓRIO BI</b>\n\n♾️ <b>TOTAL:</b> {len(df)}"
    enviar_telegram(token, chat_ids, msg)

def verificar_automacao_bi(token, chat_ids, stake_padrao):
    agora = get_time_br()
    hoje_str = agora.strftime('%Y-%m-%d')
    if st.session_state['last_check_date'] != hoje_str:
        st.session_state['bi_enviado'] = False; st.session_state['ia_enviada'] = False
        st.session_state['financeiro_enviado'] = False; st.session_state['bigdata_enviado'] = False
        st.session_state['matinal_enviado'] = False; st.session_state['last_check_date'] = hoje_str
    
    if agora.hour == 23 and agora.minute >= 30 and not st.session_state['bi_enviado']:
        enviar_relatorio_bi(token, chat_ids); st.session_state['bi_enviado'] = True
    if agora.hour == 23 and agora.minute >= 35 and not st.session_state['ia_enviada']:
        enviar_telegram(token, chat_ids, f"🧠 <b>CONSULTORIA IA</b>\n\n{analisar_bi_com_ia()}"); st.session_state['ia_enviada'] = True
    if agora.hour == 23 and agora.minute >= 40 and not st.session_state['financeiro_enviado']:
        enviar_telegram(token, chat_ids, f"💰 <b>FINANCEIRO</b>\n\n{analisar_financeiro_com_ia(stake_padrao, st.session_state['banca_inicial'])}"); st.session_state['financeiro_enviado'] = True
    
    # CONSOLIDAÇÃO DO BIG DATA (PROGRAMADO)
    if agora.hour == 23 and agora.minute >= 50 and not st.session_state['bigdata_enviado']:
        if consolidar_bigdata_no_sheets():
            st.session_state['bigdata_enviado'] = True
            st.toast("✅ Big Data Consolidado!")

def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br()
    if 8 <= agora.hour < 11 and not st.session_state['matinal_enviado']:
        enviar_telegram(token, chat_ids, f"🌅 <b>BOM DIA! INSIGHTS MATINAIS</b>\n\n{gerar_insights_matinais_ia(api_key)}")
        st.session_state['matinal_enviado'] = True

def gerar_insights_matinais_ia(api_key):
    if not IA_ATIVADA: return "IA Offline."
    return "O robô está mapeando os principais jogos de hoje. Fique atento aos sinais!"

def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
    st_short = jogo_api['fixture']['status']['short']
    STRATS_HT_ONLY = ["Gol Relâmpago", "Massacre", "Choque Líderes", "Briga de Rua"]
    if "Múltipla" in sinal['Estrategia'] or "Sniper" in sinal['Estrategia']: return False
    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: return False
    fid = clean_fid(sinal['FID']); strat = str(sinal['Estrategia'])
    key_green = f"RES_GREEN_{fid}_{strat}"; key_red = f"RES_RED_{fid}_{strat}"
    if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
    
    if (gh+ga) > (ph+pa):
        if "Morno" in sinal['Estrategia']: 
            if (gh+ga) >= 2:
                sinal['Resultado'] = '❌ RED'
                if key_red not in st.session_state['alertas_enviados']:
                    enviar_telegram(token, chats, f"❌ <b>RED</b>\n⚽ {sinal['Jogo']}\n📈 {gh}x{ga}"); st.session_state['alertas_enviados'].add(key_red); st.session_state['precisa_salvar'] = True
                return True
            else: return False 
        else:
            sinal['Resultado'] = '✅ GREEN'
            if key_green in st.session_state['alertas_enviados']: return True
            enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {sinal['Jogo']}\n📈 Placar: <b>{gh}x{ga}</b>"); st.session_state['alertas_enviados'].add(key_green); st.session_state['precisa_salvar'] = True 
            return True
    if any(x in sinal['Estrategia'] for x in STRATS_HT_ONLY) and st_short in ['HT', '2H', 'FT', 'AET', 'PEN']:
        sinal['Resultado'] = '❌ RED'
        if key_red not in st.session_state['alertas_enviados']:
            enviar_telegram(token, chats, f"❌ <b>RED HT</b>\n⚽ {sinal['Jogo']}"); st.session_state['alertas_enviados'].add(key_red); st.session_state['precisa_salvar'] = True 
        return True
    if st_short in ['FT', 'AET', 'PEN']:
        if "Morno" in sinal['Estrategia'] and (gh+ga) <= 1:
             sinal['Resultado'] = '✅ GREEN'
             if key_green not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"✅ <b>GREEN UNDER</b>\n⚽ {sinal['Jogo']}"); st.session_state['alertas_enviados'].add(key_green); st.session_state['precisa_salvar'] = True 
             return True
        sinal['Resultado'] = '❌ RED'
        if key_red not in st.session_state['alertas_enviados']:
            enviar_telegram(token, chats, f"❌ <b>RED FT</b>\n⚽ {sinal['Jogo']}"); st.session_state['alertas_enviados'].add(key_red); st.session_state['precisa_salvar'] = True 
        return True
    return False

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    df_full = st.session_state.get('historico_full', pd.DataFrame())
    if df_full.empty: return
    pendentes = df_full[df_full['Resultado'] == 'Pendente'].to_dict('records')
    if not pendentes: return
    agora = get_time_br(); ids_live = [j['fixture']['id'] for j in jogos_live]; updates_buffer = []
    for s in pendentes:
        fid = int(clean_fid(s.get('FID', 0))); strat = str(s.get('Estrategia', ''))
        key_green = f"RES_GREEN_{str(fid)}_{strat}"
        if key_green in st.session_state.get('alertas_enviados', set()):
            s['Resultado'] = '✅ GREEN'; updates_buffer.append(s); continue 
        jogo_encontrado = None
        if fid > 0 and fid in ids_live: jogo_encontrado = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        elif fid > 0:
            try:
                url_fix = "https://v3.football.api-sports.io/fixtures"
                res = requests.get(url_fix, headers={"x-apisports-key": api_key}, params={"id": fid}).json()
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
            if jogo['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
                gh = jogo['goals']['home'] or 0; ga = jogo['goals']['away'] or 0
                s['Resultado'] = 'Encerrado'; updates_buffer.append(s)
    if updates_buffer: atualizar_historico_ram(updates_buffer)

def verificar_var_rollback(jogos_live, token, chats):
    hist = st.session_state['historico_sinais']
    greens = [s for s in hist if 'GREEN' in str(s['Resultado'])]
    if not greens: return
    updates_buffer = []
    for s in greens:
        fid = int(clean_fid(s.get('FID', 0)))
        jogo_api = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        if jogo_api:
            gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
            try:
                ph, pa = map(int, s['Placar_Sinal'].split('x'))
                if (gh + ga) <= (ph + pa):
                    key_var = f"VAR_{fid}_{s['Estrategia']}_{gh}x{ga}"
                    s['Resultado'] = 'Pendente'; st.session_state['precisa_salvar'] = True; updates_buffer.append(s)
                    if key_var not in st.session_state['alertas_enviados']:
                        enviar_telegram(token, chats, f"⚠️ <b>VAR ACIONADO</b>\n⚽ {s['Jogo']}\n📉 Placar: {gh}x{ga}"); st.session_state['alertas_enviados'].add(key_var)
            except: pass
    if updates_buffer: atualizar_historico_ram(updates_buffer)

def reenviar_sinais(token, chats):
    hist = st.session_state['historico_sinais']
    if not hist: return st.toast("Sem sinais.")
    for s in reversed(hist):
        enviar_telegram(token, chats, f"🔄 <b>REENVIO</b>\n🚨 {s['Estrategia']}\n⚽ {s['Jogo']}"); time.sleep(0.5)

def momentum(fid, sog_h, sog_a):
    mem = st.session_state['memoria_pressao'].get(fid, {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
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
    if tempo <= 30 and (gh + ga) >= 2: return True
    if 70 <= tempo <= 85 and abs(gh - ga) <= 1: return True
    if tempo <= 60 and abs(gh - ga) <= 1: return True
    if status == 'HT' and gh == 0 and ga == 0: return True
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
            if stats: resultados[fid] = stats; update_api_usage(headers)
    return resultados

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        txt_stats = f"{sh_h+sh_a} Chutes ({sog_h+sog_a} no Gol)"
    except: return []
    fid = j['fixture']['id']; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
    rh, ra = momentum(fid, sog_h, sog_a); SINAIS = []
    if tempo <= 30 and (gh+ga) >= 2: SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": "🔥 Over Gols (Tendência)", "stats": txt_stats, "rh": rh, "ra": ra})
    if (gh + ga) == 0:
        if (tempo <= 2 and (sog_h + sog_a) >= 1) or (tempo <= 10 and (sh_h + sh_a) >= 2):
            SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": "Over 0.5 HT", "stats": txt_stats, "rh": rh, "ra": ra})
    if 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(gh-ga) <= 1: SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": "Over Gols Limite", "stats": txt_stats, "rh": rh, "ra": ra})
    if tempo <= 60:
        if gh <= ga and (rh >= 2 or sh_h >= 8): SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": "Over Gols", "stats": f"P: {rh}", "rh": rh, "ra": ra})
        if ga <= gh and (ra >= 2 or sh_a >= 8): SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": "Over Gols", "stats": f"P: {ra}", "rh": rh, "ra": ra})
    if rank_home and rank_away:
        if (rank_home <= 4 and rank_away >= 11) or (rank_away <= 4 and rank_home >= 11):
            if tempo <= 5 and (sh_h + sh_a) >= 1: SINAIS.append({"tag": "🔥 Massacre", "ordem": "Over 0.5 HT", "stats": f"R: {rank_home}x{rank_away}", "rh": rh, "ra": ra})
        if 5 <= tempo <= 15:
            if rank_home <= 4 and (rh >= 2 or sh_h >= 3): SINAIS.append({"tag": "🦁 Favorito", "ordem": "Over Gols", "stats": f"P: {rh}", "rh": rh, "ra": ra})
    if 75 <= tempo <= 85 and abs(gh - ga) <= 1:
        if (sh_h + sh_a) >= 16 and (sog_h + sog_a) >= 8: SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": "Gol no Final (Over Limit)", "stats": "🔥 Máxima", "rh": rh, "ra": ra})
    return SINAIS

def resetar_sistema_completo():
    st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST); st.session_state['historico_sinais'] = []
    st.session_state['df_black'] = pd.DataFrame(columns=COLS_BLACK); st.session_state['df_safe'] = pd.DataFrame(columns=COLS_SAFE)
    st.session_state['df_vip'] = pd.DataFrame(columns=COLS_OBS); st.session_state['buffer_bigdata'] = []
    try:
        conn.clear(worksheet="Historico"); conn.clear(worksheet="Blacklist"); conn.clear(worksheet="Seguras"); conn.clear(worksheet="Obs"); conn.clear(worksheet="BigData") 
    except: pass
    st.cache_data.clear(); st.toast("♻️ SISTEMA RESETADO!"); st.rerun()

# ==============================================================================
# 5. SIDEBAR E LOOP PRINCIPAL (EXECUÇÃO)
# ==============================================================================
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=True):
        st.session_state['API_KEY'] = st.text_input("Chave API:", value=st.session_state['API_KEY'], type="password")
        st.session_state['TG_TOKEN'] = st.text_input("Token Telegram:", value=st.session_state['TG_TOKEN'], type="password")
        st.session_state['TG_CHAT'] = st.text_input("Chat IDs:", value=st.session_state['TG_CHAT'])
        INTERVALO = st.slider("Ciclo (s):", 60, 300, 60) 
        if st.button("🧹 Limpar Cache"): st.cache_data.clear(); carregar_tudo(force=True); st.toast("Cache Limpo!")
        
        # BOTÃO PARA SALVAR O BIG DATA MANUALMENTE
        if st.session_state['buffer_bigdata']:
            if st.button("💾 Salvar Big Data Agora"):
                if consolidar_bigdata_no_sheets(): st.success("Salvo!"); time.sleep(1); st.rerun()
        
        st.write("---")
        if st.button("🧠 Pedir Análise do BI"):
            if IA_ATIVADA: st.info(analisar_bi_com_ia())
            else: st.error("IA Offline.")
        if st.button("🧪 Criar Estratégia"):
            if IA_ATIVADA: st.success(criar_estrategia_nova_ia())
            else: st.error("IA Offline.")

    with st.expander("💰 Gestão de Banca"):
        st.session_state['stake_padrao'] = st.number_input("Valor da Aposta (R$)", value=st.session_state.get('stake_padrao', 10.0), step=5.0)
        st.session_state['banca_inicial'] = st.number_input("Banca Inicial (R$)", value=st.session_state.get('banca_inicial', 100.0), step=50.0)
    
    with st.expander("🤖 Consumo"):
        verificar_reset_diario()
        st.caption(f"API: {st.session_state['api_usage']['used']}/{st.session_state['api_usage']['limit']}")
        st.caption(f"IA: {st.session_state['gemini_usage']['used']}/{st.session_state['gemini_usage']['limit']}")
        if st.button("🔓 Destravar IA"): st.session_state['ia_bloqueada_ate'] = None; st.toast("IA Destravada!")

    st.write("---")
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    if st.button("☢️ ZERAR ROBÔ", type="primary"): resetar_sistema_completo()

if st.session_state.ROBO_LIGADO:
    with placeholder_root.container():
        carregar_tudo()
        verificar_automacao_bi(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], st.session_state['stake_padrao'])
        verificar_alerta_matinal(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], st.session_state['API_KEY'])
        
        ids_black = [normalizar_id(x) for x in st.session_state['df_black']['id'].values]
        df_obs = st.session_state.get('df_vip', pd.DataFrame()); count_obs = len(df_obs)
        df_safe_show = st.session_state.get('df_safe', pd.DataFrame()); count_safe = len(df_safe_show)
        ids_safe = [normalizar_id(x) for x in df_safe_show['id'].values]
        
        api_error = False; jogos_live = []
        try:
            url = "https://v3.football.api-sports.io/fixtures"
            resp = requests.get(url, headers={"x-apisports-key": st.session_state['API_KEY']}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
            update_api_usage(resp.headers); res = resp.json(); jogos_live = res.get('response', []) if not res.get('errors') else []; api_error = bool(res.get('errors'))
        except: api_error = True

        if not api_error: 
            check_green_red_hibrido(jogos_live, st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], st.session_state['API_KEY'])
            conferir_resultados_sniper(jogos_live); verificar_var_rollback(jogos_live, st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'])
        
        radar = []; agenda = []; jogos_para_baixar = []
        if not api_error:
            for j in jogos_live:
                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                if lid in ids_black: continue
                tempo = j['fixture']['status']['elapsed'] or 0; st_short = j['fixture']['status']['short']
                gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
                
                if st_short == 'FT':
                    # Chama a nova função com buffer (não trava!)
                    salvar_bigdata(j, st.session_state.get(f"st_{fid}", []))
                    continue
                
                if deve_buscar_stats(tempo, gh, ga, st_short):
                    if (time.time() - st.session_state['controle_stats'].get(fid, 0)) > 180: jogos_para_baixar.append(j)
            
            if jogos_para_baixar:
                novas_stats = atualizar_stats_em_paralelo(jogos_para_baixar, st.session_state['API_KEY'])
                for fid, stats in novas_stats.items():
                    st.session_state['controle_stats'][fid] = time.time(); st.session_state[f"st_{fid}"] = stats

            for j in jogos_live:
                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                if lid in ids_black or j['fixture']['status']['short'] == 'FT': continue
                nome_liga_show = j['league']['name'] + (" 🛡️" if lid in ids_safe else " ⚠️" if lid in df_obs['id'].values else " ❓")
                tempo = j['fixture']['status']['elapsed'] or 0; placar = f"{j['goals']['home']}x{j['goals']['away']}"
                stats = st.session_state.get(f"st_{fid}", [])
                lista_sinais = processar(j, stats, tempo, placar) if stats else []
                
                if lista_sinais:
                    for s in lista_sinais:
                        uid = f"{fid}_{s['tag']}"
                        if uid not in st.session_state['alertas_enviados']:
                            st.session_state['alertas_enviados'].add(uid)
                            item = {"FID": fid, "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", "Placar_Sinal": placar, "Estrategia": s['tag'], "Resultado": "Pendente", "Odd": get_live_odds(fid, st.session_state['API_KEY'], s['tag'], (j['goals']['home']+j['goals']['away']))}
                            if adicionar_historico(item):
                                msg = f"🚨 <b>SINAL ENCONTRADO</b>\n\n🏆 {j['league']['name']}\n⚽ {j['teams']['home']['name']} x {j['teams']['away']['name']}\n🔥 {s['tag']}\n⏰ {tempo}' (Placar: {placar})\n💰 Odd: @{item['Odd']}{buscar_inteligencia(s['tag'], j['league']['name'], item['Jogo'])}{consultar_ia_gemini(item, s['tag'], stats)}"
                                enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], msg); st.toast(f"Sinal: {s['tag']}")
                
                radar.append({"Liga": nome_liga_show, "Jogo": f"{j['teams']['home']['name']} {placar} {j['teams']['away']['name']}", "Tempo": f"{tempo}'", "Status": "👁️" if stats else "💤"})

            hoje_real = get_time_br().strftime('%Y-%m-%d')
            prox = buscar_agenda_cached(st.session_state['API_KEY'], hoje_real); agora = get_time_br()
            for p in prox:
                try:
                    if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] in ['NS', 'TBD']:
                        if datetime.fromisoformat(p['fixture']['date']) > agora:
                            agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
                except: pass

        if st.session_state.get('precisa_salvar'):
            if salvar_aba("Historico", st.session_state['historico_full']): st.session_state['precisa_salvar'] = False; st.toast("💾 Histórico Salvo!")

        st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>' if not api_error else '<div class="status-error">🚨 API LIMITADA</div>', unsafe_allow_html=True)
        hist_hj = pd.DataFrame(st.session_state.get('historico_sinais', []))
        t, g, r, w = calcular_stats(hist_hj)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t}</div><div class="metric-sub">{g} Green | {r} Red</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(radar)}</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Big Data Buffer</div><div class="metric-value">{len(st.session_state["buffer_bigdata"])}</div><div class="metric-sub">Sincronização 23:50</div></div>', unsafe_allow_html=True)

        abas = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", f"💰 Financeiro", f"📜 Histórico ({len(hist_hj)})", "📈 BI", "🚫 Blacklist", "🛡️ Seguras", "💾 Big Data"])
        with abas[0]: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
        with abas[1]: st.dataframe(pd.DataFrame(agenda).sort_values('Hora'), use_container_width=True, hide_index=True)
        with abas[3]: st.dataframe(hist_hj, use_container_width=True, hide_index=True)
        with abas[7]: st.dataframe(pd.DataFrame(st.session_state['buffer_bigdata']), use_container_width=True)

        for i in range(INTERVALO, 0, -1):
            st.markdown(f'<div class="footer-timer">Próxima varredura em {i}s | Buffer BigData: {len(st.session_state["buffer_bigdata"])} jogos</div>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()
else:
    with placeholder_root.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera. Configure na lateral.")
