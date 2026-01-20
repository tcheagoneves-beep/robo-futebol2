import streamlit as st
import pandas as pd
import requests
import time
import os
import threading
import random 
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CORREÇÃO DE GRÁFICO (Backend AGG) ---
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
# GARANTIA DE PERSISTÊNCIA DOS ALERTAS
if 'alertas_enviados' not in st.session_state: 
    st.session_state['alertas_enviados'] = set()

# Função auxiliar para garantir que o set não seja limpo
def registrar_alerta(fid, tag):
    chave = f"{fid}_{tag}"
    st.session_state['alertas_enviados'].add(chave)
    return chave
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

# --- COLUNAS ---
COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID', 'Odd', 'Odd_Atualizada', 'Opiniao_IA']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo', 'Strikes', 'Jogos_Erro']
COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes', 'Jogos_Erro']
COLS_BLACK = ['id', 'País', 'Liga', 'Motivo']
COLS_BIGDATA = ['FID', 'Data', 'Liga', 'Jogo', 'Placar_Final', 'Chutes_Total', 'Chutes_Gol', 'Escanteios', 'Posse_Casa', 'Cartoes']

LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]
DB_CACHE_TIME = 60
STATIC_CACHE_TIME = 600

# --- MAPEAMENTO DE LÓGICA PARA IA ---
MAPA_LOGICA_ESTRATEGIAS = {
    "🟣 Porteira Aberta": "Tempo <= 30, Gols >= 2. Foco em jogo aberto.",
    "⚡ Gol Relâmpago": "Tempo <= 10. Chutes >= 2 ou SoG >= 1. Foco em gol cedo.",
    "💰 Janela de Ouro": "70-75 min, Chutes >= 18, Diferença gols <= 1. Jogo pegado.",
    "🟢 Blitz Casa": "Tempo <= 60, Casa perdendo ou empatando, Pressão(rh) >= 2 ou Chutes >= 8.",
    "🟢 Blitz Visitante": "Tempo <= 60, Visitante perdendo ou empatando, Pressão(ra) >= 2 ou Chutes >= 8.",
    "🔥 Massacre": "Favorito Top vs Zebra, Tempo <= 5, Chutes >= 1.",
    "⚔️ Choque Líderes": "Dois Tops, Tempo <= 7, Chutes >= 2.",
    "🥊 Briga de Rua": "Times Mid, Tempo <= 7, Chutes 2 a 3.",
    "❄️ Jogo Morno": "Times Z4, Tempo 15-16, 0 Chutes. Under HT.",
    "💎 GOLDEN BET": "75-85 min, Diferença <= 1, Chutes >= 16, SoG >= 8. Pressão extrema."
}

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

# ==============================================================================
# [CORREÇÃO] FUNÇÕES DE BANCO DE DADOS BLINDADAS (COM RETRY E MEMÓRIA)
# ==============================================================================

def carregar_aba(nome_aba, colunas_esperadas):
    # Mapeia qual variável da sessão guarda esses dados para fallback
    chave_memoria = ""
    if nome_aba == "Historico": chave_memoria = 'historico_full'
    elif nome_aba == "Seguras": chave_memoria = 'df_safe'
    elif nome_aba == "Obs": chave_memoria = 'df_vip'
    elif nome_aba == "Blacklist": chave_memoria = 'df_black'

    try:
        # Tenta ler do Google Sheets
        df = conn.read(worksheet=nome_aba, ttl=0)
        
        # Validação básica
        if df is None: raise Exception("API retornou None")

        # Se leu vazio mas temos certeza que não deveria (Histórico/Seguras), suspeitamos de erro
        if df.empty and nome_aba in ["Historico", "Seguras"] and chave_memoria in st.session_state and not st.session_state[chave_memoria].empty:
             raise Exception(f"Leitura de {nome_aba} retornou vazio (Suspeita de falha na API)")

        if not df.empty:
            for col in colunas_esperadas:
                if col not in df.columns:
                    if col == 'Odd': df[col] = "1.20"
                    else: df[col] = ""
            # SUCESSO
            return df.fillna("").astype(str)
            
        return pd.DataFrame(columns=colunas_esperadas)

    except Exception as e:
        print(f"⚠️ Falha ao ler {nome_aba}: {e}")
        
        # PLANO B: Usar a Memória RAM (Cache) se existir
        if chave_memoria and chave_memoria in st.session_state:
            df_ram = st.session_state[chave_memoria]
            if not df_ram.empty:
                st.toast(f"⚠️ {nome_aba}: Usando memória (Rede instável).", icon="💾")
                return df_ram
        
        # PLANO C: Se não tem memória, retorna vazio MAS ativa a trava de segurança para não salvar
        st.error(f"❌ Erro Crítico em '{nome_aba}'. Salvamento bloqueado até normalizar.")
        st.session_state['BLOQUEAR_SALVAMENTO'] = True
        return pd.DataFrame(columns=colunas_esperadas)
def salvar_aba(nome_aba, df_para_salvar):
    # 1. Proteção contra ZERAR dados
    if nome_aba in ["Historico", "Seguras", "Obs"] and df_para_salvar.empty:
        st.warning(f"⚠️ Salvamento abortado: Tentativa de limpar '{nome_aba}'. Dados mantidos na memória.")
        return False

    # 2. Se a leitura inicial falhou, não salvamos para não estragar a planilha
    if st.session_state.get('BLOQUEAR_SALVAMENTO', False):
        st.warning("⚠️ Modo de Segurança: Salvamento pausado.")
        st.session_state['precisa_salvar'] = True 
        return False

    # 3. Retry Loop
    max_tentativas = 3
    for i in range(max_tentativas):
        try:
            conn.update(worksheet=nome_aba, data=df_para_salvar)
            if nome_aba == "Historico": st.session_state['precisa_salvar'] = False
            return True
        except Exception as e:
            time.sleep(1)
    
    # Falhou todas
    st.toast(f"☁️ Erro ao salvar '{nome_aba}'. Tentarei no próximo ciclo.", icon="⏳")
    st.session_state['precisa_salvar'] = True
    return False

# ==============================================================================

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

# [MODIFICADO] Função com tratamento de erro e limpeza de dados
def salvar_bigdata(jogo_api, stats):
    if not db_firestore: return
    try:
        fid = str(jogo_api['fixture']['id'])
        if fid in st.session_state['jogos_salvos_bigdata']: return 

        s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        
        # Helper para evitar nulls no Firebase
        def sanitize(val): return str(val) if val is not None else "0"

        item_bigdata = {
            'fid': fid,
            'data_hora': get_time_br().strftime('%Y-%m-%d %H:%M'),
            'liga': sanitize(jogo_api['league']['name']),
            'jogo': f"{sanitize(jogo_api['teams']['home']['name'])} x {sanitize(jogo_api['teams']['away']['name'])}",
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
        st.toast(f"💾 BigData Salvo: {item_bigdata['jogo']}")
    except Exception as e:
        st.error(f"Erro ao salvar no Firebase (FID {jogo_api['fixture']['id']}): {e}")

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

# --- REDUZI O CACHE PARA 120s (2 min) PARA PEGAR JOGOS FINALIZADOS RAPIDAMENTE ---
@st.cache_data(ttl=120) 
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
        if best_odd == "0.00": return "1.20" 
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
    
    dados_ricos = extrair_dados_completos(stats_raw)
    nome_strat = estrategia.upper()
    
    # Histórico (Contexto de Winrate)
    df = st.session_state.get('historico_full', pd.DataFrame())
    resumo_historico = ""
    if not df.empty:
        df_strat = df[df['Estrategia'] == estrategia]
        if len(df_strat) > 0:
            greens = len(df_strat[df_strat['Resultado'].str.contains('GREEN', na=False)])
            winrate = (greens / len(df_strat) * 100)
            resumo_historico = f"Winrate histórico no bot: {winrate:.0f}%."

    # --- REGRA GLOBAL DE FORMATO ---
    regra_texto = "Responda SEM formatação (sem negrito, sem asteriscos). Seja direto."

    # --- PERFIL 1: ESTRATÉGIAS DE UNDER (JOGO MORNO) ---
    # Aqui a lógica é INVERSA: Queremos jogo ruim.
    if "MORNO" in nome_strat or "UNDER" in nome_strat:
        prompt = f"""
        Atue como Especialista em UNDER GOLS.
        Cenário: {dados_jogo['jogo']} | Stats: {dados_ricos}
        
        Sua Missão: Validar se o jogo vai continuar 0x0 ou com poucos gols.
        Critério: Se tiver muitos chutes (mesmo pra fora) ou jogo aberto, REJEITE (Arriscado).
        APROVE apenas se: Ataques inofensivos, bola presa no meio, times fracos.
        
        {regra_texto}
        Responda: [Aprovado/Arriscado] - [Motivo curto]
        """

    # --- PERFIL 2: ESTRATÉGIAS DE OVER (TODAS AS OUTRAS) ---
    # Golden, Blitz, Relâmpago, Massacre, Choque, etc.
    # MENTALIDADE: Não importa quem ganha. Importa se a bola vai entrar.
    else:
        # Contexto específico de tempo para ajudar a IA
        contexto_tempo = ""
        if "GOLDEN" in nome_strat or "JANELA" in nome_strat:
            contexto_tempo = "FIM DE JOGO (80min+). O time que perde precisa se expor. Cuidado apenas com 'Pressão Falsa' (chutes de longe)."
        elif any(x in nome_strat for x in ["RELÂMPAGO", "CHOQUE", "BRIGA"]):
            contexto_tempo = "INÍCIO DE JOGO. Buscamos intensidade imediata e defesas desatentas."
        else:
            contexto_tempo = "MEIO DO JOGO. Buscamos jogo aberto, trocação ou pressão forte."

        prompt = f"""
        Atue como um PROFISSIONAL DE OVER GOLS (Caçador de Gols).
        Estratégia: {estrategia} ({contexto_tempo})
        Dados: {dados_jogo['jogo']} | Placar: {dados_jogo['placar']}
        Stats: {dados_ricos} | {resumo_historico}
        
        REGRA DE OURO (VISÃO DE APOSTADOR):
        1. NÃO ANALISE QUEM VAI GANHAR. Analise se vai sair GOL.
        2. Defesa Ruim = APROVADO. Se o favorito ataca mal e toma contra-ataque, isso é ÓTIMO para gols.
        3. Jogo "Lá e Cá" (Trocação) = APROVADO.
        4. Favorito Perdendo = APROVADO (Vai se lançar ao ataque e deixar espaço).
        
        QUANDO REJEITAR (Arriscado):
        - Apenas se o jogo estiver TRAVADO, LENTO ou com times medrosos que não chutam.
        - No Fim de Jogo (Golden), rejeite se for só "chuveirinho" sem direção (Chutes fora >>> Chutes no gol).
        
        {regra_texto}
        Responda: [Aprovado/Arriscado] - [Motivo focado em chance de gol]
        """

    # --- EXECUÇÃO ---
    try:
        response = model_ia.generate_content(prompt, request_options={"timeout": 12})
        st.session_state['gemini_usage']['used'] += 1
        
        # Limpeza total da resposta
        texto_raw = response.text.strip()
        texto_limpo = texto_raw.replace("**", "").replace("*", "").replace("Motivo:", "").replace("Here's", "")
        
        # Veredito Inteligente
        veredicto = "Arriscado"
        if "Aprovado" in texto_limpo or "aprovado" in texto_limpo: veredicto = "Aprovado"
        
        # Extração do motivo
        motivo = texto_limpo
        for divisor in ["-", ":", "\n"]:
            if divisor in texto_limpo:
                partes = texto_limpo.split(divisor, 1)
                if len(partes) > 1: motivo = partes[1].strip(); break
        
        motivo = motivo.replace("Aprovado", "").replace("Arriscado", "").strip()
        emoji = "✅" if veredicto == "Aprovado" else "⚠️"
        
        return f"\n🤖 <b>IA:</b> {emoji} <b>{veredicto}</b> - {motivo[:130]}" 
        
    except Exception as e:
        if "429" in str(e): return "\n🤖 <b>IA:</b> (Ocupada)"
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
        # [CORREÇÃO AQUI]: ensure_ascii=False permite passar Emojis e Acentos reais para a IA
        resumo = df_f.groupby('Estrategia')['Resultado'].apply(lambda x: f"{(x.str.contains('GREEN').sum()/len(x)*100):.1f}%").to_dict()
        prompt = f"""
        Analise o dia ({hoje_str}):
        Total: {total}, Greens: {greens}
        Estratégias: {json.dumps(resumo, ensure_ascii=False)}
        3 dicas curtas para amanhã.
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
        lucro_total = 0.0; investido = 0.0; qtd=0; odds_greens = []
        for _, row in df_hoje.iterrows():
            res = str(row['Resultado'])
            odd_final = obter_odd_final_para_calculo(row['Odd'], row['Estrategia'])
            if 'GREEN' in res:
                lucro_total += (stake * odd_final) - stake; investido += stake
                if odd_final > 1: odds_greens.append(odd_final)
            elif 'RED' in res:
                lucro_total -= stake; investido += stake
        roi = (lucro_total / investido * 100) if investido > 0 else 0
        odd_media = (sum(odds_greens)/len(odds_greens)) if odds_greens else 0
        prompt_fin = f"""
        Gestor Financeiro. Dia:
        - Banca Ini: R$ {banca:.2f} | Fim: R$ {banca+lucro_total:.2f}
        - Stake: R$ {stake:.2f} | Entradas: {qtd}
        - Lucro: R$ {lucro_total:.2f} | ROI: {roi:.2f}% | Odd Média: {odd_media:.2f}
        A curva de capital está {'Crescente' if lucro_total > 0 else 'Decrescente'}.
        Feedback curto sobre saúde financeira.
        """
        response = model_ia.generate_content(prompt_fin)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro Fin: {e}"

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
        Cientista de Dados. Analise CSV (JSON): {amostra}
        MISSÃO: Padrão ESTATÍSTICO GLOBAL lucrativo (Cantos, Cartões).
        Saída: Nome, Regra e Lógica.
        """
        response = model_ia.generate_content(prompt_criacao)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro na criação: {e}"

def otimizar_estrategias_existentes_ia():
    if not IA_ATIVADA: return "⚠️ IA Desconectada."
    if not db_firestore: return "⚠️ Firebase Offline (Necessário para cruzar dados)."

    # 1. Carregar Histórico de Resultados (Sheets)
    df_hist = st.session_state.get('historico_full', pd.DataFrame())
    if df_hist.empty: return "Sem histórico suficiente para análise."
    
    # Filtra apenas jogos finalizados
    df_closed = df_hist[df_hist['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
    if df_closed.empty: return "Nenhum sinal finalizado para avaliar."

    # 2. Carregar Big Data (Firebase) para ter contexto estatístico
    # Pegamos os últimos 200 jogos para ter uma amostra relevante
    try:
        docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(200).stream()
        big_data_dict = {d.to_dict()['fid']: d.to_dict() for d in docs}
    except Exception as e: return f"Erro ao ler Firebase: {e}"

    # 3. Agrupar dados por Estratégia
    analise_pacote = {}
    
    estrategias_unicas = df_closed['Estrategia'].unique()
    
    for strat in estrategias_unicas:
        # Pula estratégias manuais ou desconhecidas
        if strat not in MAPA_LOGICA_ESTRATEGIAS: continue

        d_strat = df_closed[df_closed['Estrategia'] == strat]
        total = len(d_strat)
        if total < 5: continue # Ignora estratégias com poucos dados (pouca amostra)

        greens = d_strat[d_strat['Resultado'].str.contains('GREEN')]
        reds = d_strat[d_strat['Resultado'].str.contains('RED')]
        winrate = (len(greens) / total) * 100

        # Coleta estatísticas médias dos REDs para identificar falhas
        stats_reds = []
        for fid in reds['FID'].values:
            fid_str = str(fid)
            if fid_str in big_data_dict:
                stats_reds.append(big_data_dict[fid_str].get('estatisticas', {}))
        
        # Se tiver dados suficientes de falhas, empacota para a IA
        if stats_reds:
            # Calcula média simples de alguns indicadores nos jogos que deram RED
            try:
                avg_chutes = sum([x.get('chutes_total', 0) for x in stats_reds]) / len(stats_reds)
                avg_posse = sum([int(x.get('posse_casa', '0').replace('%','')) for x in stats_reds]) / len(stats_reds)
            except: 
                avg_chutes = 0; avg_posse = 0

            analise_pacote[strat] = {
                "Winrate Atual": f"{winrate:.1f}%",
                "Regra Atual": MAPA_LOGICA_ESTRATEGIAS[strat],
                "Total Entradas": total,
                "Qtd Reds": len(reds),
                "Perfil dos Jogos que deram RED (Médias)": {
                    "Chutes Totais no jogo": f"{avg_chutes:.1f}",
                    "Posse Casa": f"{avg_posse:.1f}%"
                },
                "Exemplo de um jogo RED": str(stats_reds[0]) if stats_reds else "N/A"
            }

    if not analise_pacote: return "Dados insuficientes (cruzamento Sheets x Firebase) para gerar insights."

    # 4. Prompt para o Gemini
    prompt_otimizacao = f"""
    Atue como um Especialista em Data Science focado em Apostas Esportivas.
    Eu tenho um Bot com estratégias definidas. Abaixo, apresento o desempenho delas e o perfil dos jogos onde elas FALHARAM (Reds).
    
    SEU OBJETIVO: Analisar os dados dos erros e sugerir UMA melhoria na lógica (código) para filtrar esses jogos ruins e aumentar o Winrate.
    
    DADOS DAS ESTRATÉGIAS:
    {json.dumps(analise_pacote, indent=2, ensure_ascii=False)}

    SAÍDA ESPERADA (Responda para cada estratégia listada):
    1. Nome da Estratégia
    2. Diagnóstico: Por que ela está falhando baseada nos dados dos Reds? (Ex: "Está entrando em jogos com poucos chutes")
    3. AÇÃO SUGERIDA: Sugira uma alteração nos parâmetros do IF (Ex: "Aumentar filtro de Chutes de 8 para 10" ou "Adicionar filtro de Posse > 40%").
    Seja técnico e direto.
    """

    try:
        response = model_ia.generate_content(prompt_otimizacao)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except Exception as e: return f"Erro na IA: {e}"

def _worker_telegram(token, chat_id, msg):
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=5)
    except: pass

def _worker_telegram_photo(token, chat_id, photo_buffer, caption):
    try:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        files = {'photo': photo_buffer}
        data = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'HTML'}
        # Envia a FOTO com legenda curta
        res = requests.post(url, files=files, data=data, timeout=15)
        if res.status_code != 200:
            st.error(f"Erro Telegram Foto ({res.status_code}): {res.text}")
    except Exception as e:
        st.error(f"Erro envio foto: {e}")

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        t = threading.Thread(target=_worker_telegram, args=(token, cid, msg))
        t.daemon = True; t.start()

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
        # Tratamento de datas
        df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
        df['Data_DT'] = pd.to_datetime(df['Data_Str'], errors='coerce')
        df = df.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
        
        hoje = pd.to_datetime(get_time_br().date())
        
        # --- DEFINIÇÃO DOS PERÍODOS ---
        d_hoje = df[df['Data_DT'] == hoje]
        d_7d = df[df['Data_DT'] >= (hoje - timedelta(days=7))]
        d_30d = df[df['Data_DT'] >= (hoje - timedelta(days=30))]
        d_total = df

        # Função auxiliar para formatar texto (Green/Red)
        def fmt_placar(d):
            if d.empty: return "0G - 0R (0%)"
            g = d['Resultado'].str.contains('GREEN', na=False).sum()
            r = d['Resultado'].str.contains('RED', na=False).sum()
            t = g + r
            wr = (g/t*100) if t > 0 else 0
            return f"{g}G - {r}R ({wr:.0f}%)"

        # --- NOVA LÓGICA: AUDITORIA DETALHADA DA IA ---
        def fmt_ia_stats(periodo_df, label_periodo):
            if 'Opiniao_IA' not in periodo_df.columns: return ""
            # Filtra apenas finalizados
            d_fin = periodo_df[periodo_df['Resultado'].isin(['✅ GREEN', '❌ RED'])]
            
            # Aprovados
            d_aprov = d_fin[d_fin['Opiniao_IA'] == 'Aprovado']
            stats_aprov = fmt_placar(d_aprov)
            
            # Arriscados
            d_risk = d_fin[d_fin['Opiniao_IA'] == 'Arriscado']
            stats_risk = fmt_placar(d_risk)
            
            return f"🤖 <b>IA ({label_periodo}):</b>\n👍 Aprovados: {stats_aprov}\n⚠️ Arriscados: {stats_risk}"

        msg_ia_hoje = fmt_ia_stats(d_hoje, "Hoje")
        msg_ia_7d = fmt_ia_stats(d_7d, "7 Dias")
        msg_ia_30d = fmt_ia_stats(d_30d, "30 Dias")
        msg_ia_total = fmt_ia_stats(d_total, "Geral")

        # --- NOVA LÓGICA: DETETIVE DE LIGAS (Qual estratégia está falhando?) ---
        df_finished = d_30d[d_30d['Resultado'].isin(['✅ GREEN', '❌ RED'])] # Analisa últimos 30 dias para ter amostra
        
        piores_ligas_msg = ""
        top_ligas_msg = ""

        if not df_finished.empty:
            # Agrupa por Liga
            grouped = df_finished.groupby('Liga')['Resultado'].apply(lambda x: pd.Series({
                'Winrate': (x.str.contains('GREEN').sum() / len(x) * 100),
                'Total': len(x),
                'Reds': x.str.contains('RED').sum()
            })).unstack()
            
            # Filtra ligas com pelo menos 3 jogos
            stats_ligas = grouped[grouped['Total'] >= 3]

            # Melhores Ligas
            melhores = stats_ligas.sort_values(by=['Winrate', 'Total'], ascending=[False, False]).head(3)
            top_ligas_msg = "\n".join([f"🏆 {liga}: {row['Winrate']:.0f}% ({int(row['Total'])}j)" for liga, row in melhores.iterrows()])

            # Piores Ligas + DRILL DOWN (Qual estratégia?)
            piores = stats_ligas.sort_values(by=['Reds'], ascending=False).head(3)
            lista_piores = []
            for liga, row in piores.iterrows():
                if row['Reds'] > 0:
                    # Pega os dados só dessa liga ruim
                    dados_liga = df_finished[(df_finished['Liga'] == liga) & (df_finished['Resultado'].str.contains('RED'))]
                    # Conta qual estratégia deu mais red nela
                    vilao = dados_liga['Estrategia'].value_counts().head(1)
                    nome_vilao = vilao.index[0] if not vilao.empty else "Geral"
                    qtd_vilao = vilao.values[0] if not vilao.empty else 0
                    
                    lista_piores.append(f"💀 {liga}: {int(row['Reds'])} Reds\n  ↳ <i>Culpa: {nome_vilao} ({qtd_vilao}R)</i>")
            
            piores_ligas_msg = "\n".join(lista_piores)

        # Gera gráfico (mantido igual)
        insight_text = analisar_bi_com_ia()
        
        if token and chat_ids:
            plt.style.use('dark_background')
            fig, ax = plt.subplots(figsize=(7, 4))
            stats_plot = d_30d[d_30d['Resultado'].isin(['✅ GREEN', '❌ RED'])]
            if not stats_plot.empty:
                c = stats_plot.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)
                c.plot(kind='bar', stacked=True, color=['#00FF00', '#FF0000'], ax=ax, width=0.6)
                ax.set_title(f'PERFORMANCE 30 DIAS', color='white', fontsize=12)
                plt.tight_layout()
                buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=100, facecolor='#0E1117'); buf.seek(0)
                
                msg_foto = "📊 <b>Gráfico de Performance (30 Dias)</b>"
                ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
                for cid in ids: 
                    buf.seek(0)
                    _worker_telegram_photo(token, cid, buf, msg_foto)
                
                # --- MENSAGEM DE TEXTO FINAL FORMATADA ---
                msg_texto = f"""📈 <b>RELATÓRIO BI AVANÇADO</b>
                
📆 <b>HOJE:</b> {fmt_placar(d_hoje)}
{msg_ia_hoje}

🗓 <b>SEMANA:</b> {fmt_placar(d_7d)}
{msg_ia_7d}

📅 <b>MÊS (30d):</b> {fmt_placar(d_30d)}
{msg_ia_30d}

♾ <b>TOTAL GERAL:</b> {fmt_placar(d_total)}
{msg_ia_total}

💎 <b>TOP LIGAS (Winrate):</b>
{top_ligas_msg}

⚠️ <b>LIGAS CRÍTICAS (Análise de Falha):</b>
{piores_ligas_msg}

🧠 <b>INSIGHT GERAL DA IA:</b>
{insight_text}
                """
                enviar_telegram(token, chat_ids, msg_texto)
                plt.close(fig)
            else:
                st.warning("Sem dados para gráfico.")
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
        st.toast("📊 Relatório BI Enviado!")
    if agora.hour == 23 and agora.minute >= 40 and not st.session_state['financeiro_enviado']:
        analise_fin = analisar_financeiro_com_ia(stake_padrao, st.session_state.get('banca_inicial', 100))
        msg_fin = f"💰 <b>CONSULTORIA FINANCEIRA</b>\n\n{analise_fin}"
        enviar_telegram(token, chat_ids, msg_fin)
        st.session_state['financeiro_enviado'] = True
        st.toast("💰 Relatório Financeiro Enviado!")
    if agora.hour == 23 and agora.minute >= 55 and not st.session_state['bigdata_enviado']:
        enviar_analise_estrategia(token, chat_ids)
        st.session_state['bigdata_enviado'] = True
        st.toast("🧪 Sugestão de Estratégia Enviada!")

def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br()
    if 8 <= agora.hour < 11 and not st.session_state['matinal_enviado']:
        insights = gerar_insights_matinais_ia(api_key)
        if insights and "Sem insights" not in insights:
            ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
            msg_final = f"🌅 <b>INSIGHTS MATINAIS (IA + API)</b>\n\n{insights}"
            for cid in ids: enviar_telegram(token, cid, msg_final)
            st.session_state['matinal_enviado'] = True
            st.toast("Insights Matinais Enviados!")

def gerar_insights_matinais_ia(api_key):
    if not IA_ATIVADA: return "IA Offline."
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        # 1. Busca Jogos do Dia
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        
        # Filtra Top Ligas (Premier League, La Liga, Bundesliga, Serie A, Ligue 1, Brasileirão, Champions)
        LIGAS_TOP = [39, 140, 78, 135, 61, 71, 72, 2, 3]
        jogos_top = [j for j in jogos if j['league']['id'] in LIGAS_TOP][:6] 
        
        if not jogos_top: return "Nenhum jogo 'Top Tier' para análise matinal hoje."
        
        relatorio_final = ""
        
        for j in jogos_top:
            fid = j['fixture']['id']
            time_casa = j['teams']['home']['name']
            time_fora = j['teams']['away']['name']
            
            # Evita duplicidade (se já mandou hoje, pula)
            ja_enviado = False
            for s in st.session_state['historico_sinais']:
                if str(s['FID']) == str(fid) and "Sniper" in s['Estrategia']:
                    ja_enviado = True; break
            if ja_enviado: continue

            # 2. Busca Odds Pré-Live (CRUCIAL)
            url_odds = "https://v3.football.api-sports.io/odds"
            res_odds = requests.get(url_odds, headers={"x-apisports-key": api_key}, params={"fixture": fid, "bookmaker": "6"}).json()
            
            odd_home = "N/A"
            odd_away = "N/A"
            str_odds = "Odds Indisponíveis"
            
            if res_odds.get('response'):
                try:
                    vals = res_odds['response'][0]['bookmakers'][0]['bets'][0]['values']
                    for v in vals:
                        if v['value'] == 'Home': odd_home = v['odd']
                        if v['value'] == 'Away': odd_away = v['odd']
                    str_odds = f"Odds: Casa @{odd_home} | Visitante @{odd_away}"
                except: pass

            # 3. Busca Estatísticas e Predições da API
            url_pred = "https://v3.football.api-sports.io/predictions"
            res_pred = requests.get(url_pred, headers={"x-apisports-key": api_key}, params={"fixture": fid}).json()
            
            if res_pred.get('response'):
                pred = res_pred['response'][0]['predictions']
                comp = res_pred['response'][0]['comparison']
                
                # Prepara o dossiê para a IA
                info_jogo = (f"JOGO: {time_casa} (Casa) vs {time_fora} (Fora)\n"
                             f"MERCADO: {str_odds}\n"
                             f"DADOS API: Advice: {pred['advice']} | Chance Vitoria: {pred['percent']}\n"
                             f"COMPARATIVO FORÇA (0-100%): \n"
                             f"- Ataque: {comp['att']['home']} vs {comp['att']['away']}\n"
                             f"- Defesa: {comp['def']['home']} vs {comp['def']['away']}\n"
                             f"- Forma: {comp['form']['home']} vs {comp['form']['away']}")
                
                prompt_matinal = f"""
                Atue como um Tipster Profissional de Elite. Analise este confronto:
                {info_jogo}

                SUA MISSÃO: Encontrar a MELHOR oportunidade de valor, não importa o mercado.
                
                REGRAS:
                1. Analise Odds vs Probabilidade Real. Se o favorito paga pouco (Odd < 1.30), ignore a vitória seca e procure Gols ou Handicap.
                2. Se os times têm defesa fraca, considere OVER GOLS ou AMBAS MARCAM.
                3. Se o jogo é muito desequilibrado, considere ESCANTEIOS ou CARTÕES (se fizer sentido com o estilo dos times).
                4. Seja CRIATIVO mas SEGURO.

                FORMATO DE RESPOSTA OBRIGATÓRIO (Se não tiver aposta boa, responda SKIP):
                BET: [MERCADO ESCOLHIDO] | MOTIVO: [EXPLICAÇÃO TÉCNICA CURTA EM 1 FRASE]

                Exemplos de Saída:
                - BET: CASA VENCE | MOTIVO: Casa tem 80% de forma e visitante perdeu as últimas 5.
                - BET: OVER 2.5 GOLS | MOTIVO: Duas defesas fracas e ataques com média alta de gols.
                - BET: OVER 9.5 ESCANTEIOS | MOTIVO: Times com alto volume de cruzamentos e finalizações.
                """
                
                # Chama o Gemini
                resp_ia = model_ia.generate_content(prompt_matinal)
                st.session_state['gemini_usage']['used'] += 1
                texto_ia = resp_ia.text.strip()
                
                # Processa a resposta
                if "BET:" in texto_ia.upper():
                    # Limpeza básica para separar a Aposta do Motivo
                    try:
                        partes = texto_ia.split('|')
                        aposta_raw = partes[0].replace("BET:", "").strip()
                        motivo_raw = partes[1].replace("MOTIVO:", "").strip() if len(partes) > 1 else "Análise técnica favorável."
                    except:
                        aposta_raw = texto_ia.replace("BET:", "").strip()
                        motivo_raw = "Oportunidade identificada pela IA."

                    # Adiciona ao relatório visual que vai pro Telegram
                    relatorio_final += f"🎯 <b>SNIPER: {time_casa} x {time_fora}</b>\n👉 <b>{aposta_raw}</b>\n💡 <i>{motivo_raw}</i>\n📊 {str_odds}\n\n"
                    
                    # Define uma odd estimada para registro (apenas para controle financeiro, já que não temos odd de cantos/cartões na API free)
                    odd_reg = "1.70" 
                    if "CASA" in aposta_raw.upper() and odd_home != "N/A": odd_reg = odd_home
                    elif "FORA" in aposta_raw.upper() and odd_away != "N/A": odd_reg = odd_away
                    
                    # Salva no histórico
                    item_bi = {
                        "FID": str(fid), "Data": hoje, "Hora": "08:00", 
                        "Liga": j['league']['name'], "Jogo": f"{time_casa} x {time_fora}", 
                        "Placar_Sinal": aposta_raw, # Agora salva "OVER 9.5 CANTOS" por exemplo
                        "Estrategia": "Sniper Matinal", "Resultado": "Pendente", 
                        "HomeID": str(j['teams']['home']['id']), "AwayID": str(j['teams']['away']['id']), 
                        "Odd": str(odd_reg), "Odd_Atualizada": "", "Opiniao_IA": "Sniper"
                    }
                    adicionar_historico(item_bi)
                    time.sleep(1.5) # Pausa leve para não sobrecarregar

        return relatorio_final if relatorio_final else "Sem oportunidades de alto valor encontradas hoje."
        
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

def conferir_resultados_sniper(jogos_live, api_key):
    hist = st.session_state.get('historico_sinais', [])
    # Filtra apenas Snipers Pendentes
    snipers_pendentes = [s for s in hist if "Sniper" in s['Estrategia'] and s['Resultado'] == "Pendente"]
    
    if not snipers_pendentes: return

    updates_buffer = []
    # Cria mapa dos jogos ao vivo para acesso rápido
    ids_live_ou_fim = {str(j['fixture']['id']): j for j in jogos_live} 
    
    for s in snipers_pendentes:
        fid = str(s['FID'])
        jogo = None

        # 1. Tenta achar no Live
        if fid in ids_live_ou_fim:
            jogo = ids_live_ou_fim[fid]
        
        # 2. Se não achar no Live, busca na API (Pode ter acabado e sumido do Live)
        else:
            try:
                url = "https://v3.football.api-sports.io/fixtures"
                # Busca direto pelo ID
                res = requests.get(url, headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                if res.get('response'):
                    jogo = res['response'][0]
                    # Atualiza o contador de uso da API para não estourar
                    update_api_usage(res.get('headers', {}))
            except Exception as e:
                print(f"Erro ao buscar Sniper ID {fid}: {e}")

        # Se não conseguiu dados do jogo de jeito nenhum, pula
        if not jogo: continue

        # 3. Verifica o Status
        status = jogo['fixture']['status']['short']
        
        # Só processa se o jogo terminou
        if status in ['FT', 'AET', 'PEN', 'INT']: # INT = Interrompido (às vezes conta) ou FT
            gh = jogo['goals']['home'] or 0
            ga = jogo['goals']['away'] or 0
            total_gols = gh + ga
            target = s['Placar_Sinal'].upper().strip() # Garante formatação
            
            resultado_final = None

            # Lógica de verificação
            if "OVER 2.5" in target: 
                resultado_final = '✅ GREEN' if total_gols > 2.5 else '❌ RED'
            elif "UNDER 2.5" in target: 
                resultado_final = '✅ GREEN' if total_gols < 2.5 else '❌ RED'
            elif "AMBAS MARCAM" in target: 
                resultado_final = '✅ GREEN' if (gh > 0 and ga > 0) else '❌ RED'
            elif "CASA VENCE" in target: 
                resultado_final = '✅ GREEN' if gh > ga else '❌ RED'
            elif "FORA VENCE" in target: 
                resultado_final = '✅ GREEN' if ga > gh else '❌ RED'
            
            # Se identificou o resultado
            if resultado_final:
                s['Resultado'] = resultado_final
                updates_buffer.append(s)
                
                # Envia notificação
                msg_sniper = f"{resultado_final} <b>RESULTADO SNIPER</b>\n⚽ {s['Jogo']}\n🎯 {target}\n📉 Placar Final: {gh}x{ga}"
                enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], msg_sniper)
                st.session_state['precisa_salvar'] = True

    # Atualiza a memória se houver mudanças
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
        def get_v(l, t): v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        rh, ra = momentum(j['fixture']['id'], sog_h, sog_a)
        SINAIS = []
        if tempo <= 30 and (j['goals']['home'] + j['goals']['away']) >= 2: 
            SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": "🔥 Over Gols (Tendência de Goleada)", "stats": f"{sh_h+sh_a} Chutes", "rh": rh, "ra": ra})
        if (j['goals']['home'] + j['goals']['away']) == 0:
            if (tempo <= 2 and (sog_h + sog_a) >= 1) or (tempo <= 10 and (sh_h + sh_a) >= 2):
                SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": "Over 0.5 HT (Entrar para sair gol no 1º tempo)", "stats": f"{sh_h+sh_a} Chutes", "rh": rh, "ra": ra})
        if 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(j['goals']['home']-j['goals']['away']) <= 1: 
            SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": "Over Gols (Gol no final - Limite)", "stats": f"{sh_h+sh_a} Chutes", "rh": rh, "ra": ra})
        if tempo <= 60:
            if j['goals']['home'] <= j['goals']['away'] and (rh >= 2 or sh_h >= 8): SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": "Over Gols (Gol maduro na partida)", "stats": f"Pressão: {rh}", "rh": rh, "ra": ra})
            if j['goals']['away'] <= j['goals']['home'] and (ra >= 2 or sh_a >= 8): SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": "Over Gols (Gol maduro na partida)", "stats": f"Pressão: {ra}", "rh": rh, "ra": ra})
        if rank_home and rank_away:
            is_top_home = rank_home <= 4; is_top_away = rank_away <= 4; is_bot_home = rank_home >= 11; is_bot_away = rank_away >= 11; is_mid_home = rank_home >= 5; is_mid_away = rank_away >= 5
            if (is_top_home and is_bot_away) or (is_top_away and is_bot_home):
                if tempo <= 5 and (sh_h + sh_a) >= 1: SINAIS.append({"tag": "🔥 Massacre", "ordem": "Over 0.5 HT (Favorito deve abrir placar)", "stats": f"Rank: {rank_home}x{rank_away}", "rh": rh, "ra": ra})
            if 5 <= tempo <= 15:
                if is_top_home and (rh >= 2 or sh_h >= 3): SINAIS.append({"tag": "🦁 Favorito", "ordem": "Over Gols (Partida)", "stats": f"Pressão: {rh}", "rh": rh, "ra": ra})
                if is_top_away and (ra >= 2 or sh_a >= 3): SINAIS.append({"tag": "🦁 Favorito", "ordem": "Over Gols (Partida)", "stats": f"Pressão: {ra}", "rh": rh, "ra": ra})
            if is_top_home and is_top_away and tempo <= 7:
                if (sh_h + sh_a) >= 2 and (sog_h + sog_a) >= 1: SINAIS.append({"tag": "⚔️ Choque Líderes", "ordem": "Over 0.5 HT (Jogo intenso)", "stats": f"{sh_h+sh_a} Chutes", "rh": rh, "ra": ra})
            if is_mid_home and is_mid_away:
                if tempo <= 7 and 2 <= (sh_h + sh_a) <= 3: SINAIS.append({"tag": "🥊 Briga de Rua", "ordem": "Over 0.5 HT (Trocação franca)", "stats": f"{sh_h+sh_a} Chutes", "rh": rh, "ra": ra})
                is_bot_home_morno = rank_home >= 10; is_bot_away_morno = rank_away >= 10
                if is_bot_home_morno and is_bot_away_morno:
                    if 15 <= tempo <= 16 and (sh_h + sh_a) == 0: SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": "Under 1.5 HT (Apostar que NÃO saem 2 gols no 1º tempo)", "stats": "0 Chutes (Times Z-4)", "rh": rh, "ra": ra})
        if 75 <= tempo <= 85 and abs(j['goals']['home'] - j['goals']['away']) <= 1:
            if (sh_h + sh_a) >= 16 and (sog_h + sog_a) >= 8: SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": "Gol no Final (Over Limit) (Aposta seca que sai mais um gol)", "stats": "🔥 Pressão Máxima", "rh": rh, "ra": ra})
        return SINAIS
    except: return []
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
        if st.button("🧹 Limpar Cache"): 
            st.cache_data.clear(); carregar_tudo(force=True); st.session_state['last_db_update'] = 0; st.toast("Cache Limpo!")
        st.write("---")
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
        
        # --- BOTÃO DE OTIMIZAÇÃO NA SIDEBAR ---
        if st.button("🔧 Otimizar Estratégias (IA)"):
            if IA_ATIVADA and db_firestore:
                with st.spinner("🕵️ Cruzando Greens/Reds com Big Data..."):
                    relatorio_otimizacao = otimizar_estrategias_existentes_ia()
                    
                    st.markdown("### 🛠️ Plano de Melhoria")
                    if "Erro" in relatorio_otimizacao or "Atenção" in relatorio_otimizacao:
                        st.warning(relatorio_otimizacao)
                    else:
                        st.success("Análise Concluída!")
                        with st.expander("Ver Sugestões Completas", expanded=True):
                            st.write(relatorio_otimizacao)
            else:
                st.error("Requer IA Ativa e Conexão Firebase.")
        
        # --- [NOVO] BOTÃO DE BACKFILL MANUAL ---
        if st.button("🔄 Forçar Backfill (Salvar Jogos Perdidos)"):
            with st.spinner("Buscando na API todos os jogos finalizados hoje..."):
                hoje_real = get_time_br().strftime('%Y-%m-%d')
                todos_jogos_hoje = buscar_agenda_cached(st.session_state['API_KEY'], hoje_real)
                
                # Filtra só o que acabou (FT) e ainda não tá no banco
                jogos_ft_pendentes = []
                for j in todos_jogos_hoje:
                    try:
                        if j['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
                            fid = str(j['fixture']['id'])
                            if fid not in st.session_state['jogos_salvos_bigdata']:
                                jogos_ft_pendentes.append(j)
                    except: pass
                
                if jogos_ft_pendentes:
                    st.info(f"Encontrados {len(jogos_ft_pendentes)} jogos finalizados não salvos. Processando...")
                    # Processa em paralelo (cuidado com o consumo da API aqui)
                    stats_recuperadas = atualizar_stats_em_paralelo(jogos_ft_pendentes, st.session_state['API_KEY'])
                    
                    count_salvos = 0
                    for fid, stats in stats_recuperadas.items():
                        j_obj = next((x for x in jogos_ft_pendentes if str(x['fixture']['id']) == str(fid)), None)
                        if j_obj: 
                            salvar_bigdata(j_obj, stats)
                            count_salvos += 1
                    st.success(f"✅ Recuperados e Salvos: {count_salvos} jogos!")
                else:
                    st.warning("Nenhum jogo finalizado pendente encontrado para hoje.")

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
        st.session_state['stake_padrao'] = stake_padrao
        st.session_state['banca_inicial'] = banca_inicial
        
    with st.expander("📶 Consumo API", expanded=False):
        verificar_reset_diario()
        u = st.session_state['api_usage']; perc = min(u['used'] / u['limit'], 1.0) if u['limit'] > 0 else 0
        st.progress(perc); st.caption(f"Utilizado: **{u['used']}** / {u['limit']}")
    
    with st.expander("🤖 Consumo IA (Gemini)", expanded=False):
        u_ia = st.session_state['gemini_usage']
        u_ia['limit'] = 10000 
        perc_ia = min(u_ia['used'] / u_ia['limit'], 1.0)
        st.progress(perc_ia)
        st.caption(f"Requições Hoje: **{u_ia['used']}** / {u_ia['limit']}")
        if st.button("🔓 Destravar IA Agora"):
            st.session_state['ia_bloqueada_ate'] = None
            st.toast("✅ IA Destravada Manualmente!")

    st.write("---")
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    
    # Status da IA e Firebase
    if IA_ATIVADA:
        if st.session_state['ia_bloqueada_ate']:
            ag = datetime.now()
            if ag < st.session_state['ia_bloqueada_ate']:
                m_rest = int((st.session_state['ia_bloqueada_ate'] - ag).total_seconds()/60)
                st.markdown(f'<div class="status-warning">⚠️ IA PAUSADA (Proteção) - Volta em {m_rest} min</div>', unsafe_allow_html=True)
            else: st.markdown('<div class="status-active">🤖 IA GEMINI ATIVA</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🤖 IA GEMINI ATIVA</div>', unsafe_allow_html=True)
    else: st.markdown('<div class="status-error">❌ IA DESCONECTADA</div>', unsafe_allow_html=True)

    if db_firestore:
        st.markdown('<div class="status-active">🔥 FIREBASE CONECTADO</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="status-warning">⚠️ FIREBASE OFFLINE</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### ⚠️ Zona de Perigo")
    if st.button("☢️ ZERAR ROBÔ", type="primary", use_container_width=True): st.session_state['confirmar_reset'] = True
    if st.session_state.get('confirmar_reset'):
        st.error("Tem certeza? Isso apaga TODO o histórico do Google Sheets.")
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

        api_error = False
        jogos_live = []
        try:
            url = "https://v3.football.api-sports.io/fixtures"
            resp = requests.get(url, headers={"x-apisports-key": safe_api}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
            update_api_usage(resp.headers); res = resp.json()
            raw_live = res.get('response', []) if not res.get('errors') else []
            
            # --- TRAVA DE DUPLICIDADE GLOBAL ---
            # Remove duplicatas IMEDIATAMENTE. O dicionário usa o ID como chave única.
            dict_clean = {j['fixture']['id']: j for j in raw_live}
            jogos_live = list(dict_clean.values())
            # -----------------------------------

            api_error = bool(res.get('errors'))
            if api_error and "errors" in res: st.error(f"Detalhe do Erro: {res['errors']}")
        except Exception as e: jogos_live = []; api_error = True; st.error(f"Erro de Conexão: {e}")

        if not api_error: 
            # Agora todas as funções abaixo usam a lista 'jogos_live' já limpa
            check_green_red_hibrido(jogos_live, safe_token, safe_chat, safe_api)
            conferir_resultados_sniper(jogos_live, safe_api) 
            verificar_var_rollback(jogos_live, safe_token, safe_chat)
        
        radar = []; agenda = []
        if not api_error:
            # 1. COLETA DE STATS FINAIS PARA BIG DATA (FIREBASE)
            # --- [MODIFICADO] CORREÇÃO BIG DATA OTIMIZADA (Randomizada) ---
            # Busca agenda de hoje apenas uma vez por ciclo (já tem cache)
            prox = buscar_agenda_cached(safe_api, hoje_real); agora = get_time_br()
            
            ft_para_salvar = []
            for p in prox:
                try:
                    # Verifica se acabou e se NÃO foi salvo ainda
                    if p['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
                        fid_str = str(p['fixture']['id'])
                        if fid_str not in st.session_state['jogos_salvos_bigdata']:
                            ft_para_salvar.append(p)
                except: pass
            
            # Processa em lotes aleatórios para não travar a fila e não estourar a cota
            if ft_para_salvar:
                # Se tiver mais que 3, pega 3 aleatórios. Se tiver menos, pega todos.
                lote = random.sample(ft_para_salvar, min(len(ft_para_salvar), 3)) 
                
                stats_ft = atualizar_stats_em_paralelo(lote, safe_api)
                for fid, s in stats_ft.items():
                    j_obj = next((x for x in lote if x['fixture']['id'] == fid), None)
                    if j_obj: salvar_bigdata(j_obj, s)

            # 2. PROCESSAMENTO DE SINAIS (APLICAÇÃO DO FILTRO NO RADAR)
            candidatos_multipla = []; ids_no_radar = []
            
            # --- FILTRO ANTI-TRAVAMENTO ---
            STATUS_BOLA_ROLANDO = ['1H', '2H', 'HT', 'ET', 'P', 'BT']
            
            for j in jogos_live:
                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                if lid in ids_black: continue
                
                # Aplica o filtro AQUI, antes de processar qualquer sinal ou adicionar ao radar
                status_short = j['fixture']['status']['short']
                elapsed = j['fixture']['status']['elapsed']
                
                # Regra 1: Só passa se o status for de jogo rolando
                if status_short not in STATUS_BOLA_ROLANDO: continue
                
                # Regra 2: Anti-Bug 0 minutos (exceto se for HT que é válido)
                if (elapsed is None or elapsed == 0) and status_short != 'HT': continue

                # Se passou, continua o fluxo normal
                nome_liga_show = j['league']['name']
                if lid in ids_safe: nome_liga_show += " 🛡️"
                elif lid in df_obs['id'].values: nome_liga_show += " ⚠️"
                else: nome_liga_show += " ❓" 
                ids_no_radar.append(fid)
                tempo = j['fixture']['status']['elapsed'] or 0; st_short = j['fixture']['status']['short']
                home = j['teams']['home']['name']; away = j['teams']['away']['name']
                placar = f"{j['goals']['home']}x{j['goals']['away']}"; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
                
                # (Redundante mas seguro, já filtramos acima)
                if st_short == 'FT': continue 
                
                # Verifica se deve atualizar stats (para sinais)
                stats = []
                ult_chk = st.session_state['controle_stats'].get(fid, datetime.min)
                t_esp = 60 if (69<=tempo<=76) else (90 if tempo<=15 else 180)

                if deve_buscar_stats(tempo, gh, ga, st_short):
                    if (datetime.now() - ult_chk).total_seconds() > t_esp:
                         # Busca stats apenas deste jogo
                         fid_res, s_res, h_res = fetch_stats_single(fid, safe_api)
                         if s_res:
                             st.session_state['controle_stats'][fid] = datetime.now()
                             st.session_state[f"st_{fid}"] = s_res
                             update_api_usage(h_res)
                
                stats = st.session_state.get(f"st_{fid}", [])
                status_vis = "👁️" if stats else "💤"
                rank_h = None; rank_a = None
                if j['league']['id'] in LIGAS_TABELA:
                    rk = buscar_ranking(safe_api, j['league']['id'], j['league']['season'])
                    rank_h = rk.get(home); rank_a = rk.get(away)
                lista_sinais = []
                if stats:
                    lista_sinais = processar(j, stats, tempo, placar, rank_h, rank_a)
                    salvar_safe_league_basic(lid, j['league']['country'], j['league']['name'], tem_tabela=(rank_h is not None))
                    resetar_erros(lid)
                    # Lógica de Múltipla HT
                    if st_short == 'HT' and gh == 0 and ga == 0:
                        try:
                            s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
                            v1 = next((x['value'] for x in s1 if x['type']=='Total Shots'), 0) or 0
                            v2 = next((x['value'] for x in s2 if x['type']=='Total Shots'), 0) or 0
                            sg1 = next((x['value'] for x in s1 if x['type']=='Shots on Goal'), 0) or 0
                            sg2 = next((x['value'] for x in s2 if x['type']=='Shots on Goal'), 0) or 0
                            if (v1+v2) > 12 and (sg1+sg2) > 6: candidatos_multipla.append({'fid': fid, 'jogo': f"{home} x {away}", 'stats': f"{v1+v2} Chutes", 'indica': "Over 0.5 FT"})
                        except: pass
                
                # [CORREÇÃO AQUI]: ELSE para tratar ligas sem stats
                else:
                    gerenciar_erros(lid, j['league']['country'], j['league']['name'], fid)

       if lista_sinais:
                    status_vis = f"✅ {len(lista_sinais)} Sinais"
                    
                    # --- BUSCA MÉDIA DE GOLS DOS ÚLTIMOS 10 JOGOS ---
                    medias_gols = buscar_media_gols_ultimos_jogos(safe_api, j['teams']['home']['id'], j['teams']['away']['id'])
                    
                    for s in lista_sinais:
                        rh = s.get('rh', 0); ra = s.get('ra', 0)
                        txt_pressao = gerar_barra_pressao(rh, ra) 
                        
                        # --- CRIAÇÃO DA CHAVE ÚNICA (Blindada) ---
                        tag_limpa = str(s['tag']).strip()
                        fid_str = str(fid).strip()
                        uid_normal = f"{fid_str}_{tag_limpa}"
                        uid_super = f"SUPER_{fid_str}_{tag_limpa}"
                        
                        # --- TRAVA DE SEGURANÇA 1: Verifica ANTES de processar ---
                        if uid_normal in st.session_state['alertas_enviados']:
                            continue 
                        
                        # --- TRAVA DE SEGURANÇA 2: Registra AGORA ---
                        st.session_state['alertas_enviados'].add(uid_normal)

                        # --- CÁLCULO DE ODDS ---
                        odd_atual_str = get_live_odds(fid, safe_api, s['tag'], gh+ga)
                        try: odd_val = float(odd_atual_str)
                        except: odd_val = 0.0
                        
                        destaque_odd = ""
                        if odd_val >= 1.80:
                            destaque_odd = "\n💎 <b>SUPER ODD DETECTADA! (EV+)</b>"
                            st.session_state['alertas_enviados'].add(uid_super)
                        
                        # Captura a opinião da IA
                        opiniao_txt = ""
                        opiniao_db = "Neutro"
                        if IA_ATIVADA:
                            try:
                                time.sleep(0.3)
                                dados_ia = {'jogo': f"{home} x {away}", 'placar': placar, 'tempo': f"{tempo}'"}
                                # Chama a função atualizada com a mentalidade de "Caçador de Gols"
                                opiniao_txt = consultar_ia_gemini(dados_ia, s['tag'], stats)
                                
                                if "Aprovado" in opiniao_txt: opiniao_db = "Aprovado"
                                elif "Arriscado" in opiniao_txt: opiniao_db = "Arriscado"
                            except: pass
                        
                        # Prepara item
                        item = {
                            "FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), 
                            "Hora": get_time_br().strftime('%H:%M'), 
                            "Liga": j['league']['name'], "Jogo": f"{home} x {away}", 
                            "Placar_Sinal": placar, "Estrategia": s['tag'], 
                            "Resultado": "Pendente", 
                            "HomeID": str(j['teams']['home']['id']) if lid in ids_safe else "", 
                            "AwayID": str(j['teams']['away']['id']) if lid in ids_safe else "", 
                            "Odd": odd_atual_str, "Odd_Atualizada": "", "Opiniao_IA": opiniao_db
                        }
                        
                        # Salva e Envia
                        if adicionar_historico(item):
                            prob = buscar_inteligencia(s['tag'], j['league']['name'], f"{home} x {away}")
                            msg = f"<b>🚨 SINAL ENCONTRADO 🚨</b>\n\n🏆 <b>{j['league']['name']}</b>\n⚽ {home} 🆚 {away}\n⏰ <b>{tempo}' minutos</b> (Placar: {placar})\n\n🔥 {s['tag'].upper()}\n⚠️ <b>AÇÃO:</b> {s['ordem']}{destaque_odd}\n\n💰 <b>Odd: @{odd_atual_str}</b>{txt_pressao}\n📊 <i>Dados: {s['stats']}</i>\n⚽ <b>Médias (10j):</b> Casa {medias_gols['home']} | Fora {medias_gols['away']}{prob}{opiniao_txt}"
                            enviar_telegram(safe_token, safe_chat, msg)
                            st.toast(f"Sinal Enviado: {s['tag']}")
                        
                        # Super Odd tardia
                        elif uid_super not in st.session_state['alertas_enviados'] and odd_val >= 1.80:
                             st.session_state['alertas_enviados'].add(uid_super)
                             msg_super = (f"💎 <b>OPORTUNIDADE DE VALOR!</b>\n\n⚽ {home} 🆚 {away}\n📈 <b>A Odd subiu!</b> Entrada valorizada.\n🔥 <b>Estratégia:</b> {s['tag']}\n💰 <b>Nova Odd: @{odd_atual_str}</b>\n<i>O jogo mantém o padrão da estratégia.</i>{txt_pressao}")
                             enviar_telegram(safe_token, safe_chat, msg_super)
                    
                    radar.append({"Liga": nome_liga_show, "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})
            
                # --- LOOP DE MÚLTIPLAS ---
                if candidatos_multipla:
                    novos = [c for c in candidatos_multipla if c['fid'] not in st.session_state['multiplas_enviadas']]
                    if novos:
                        msg = "<b>🚀 OPORTUNIDADE DE MÚLTIPLA (HT) 🚀</b>\n" + "".join([f"\n⚽ {c['jogo']} ({c['stats']})\n⚠️ AÇÃO: {c['indica']}" for c in novos])
                        for c in novos: st.session_state['multiplas_enviadas'].add(c['fid'])
                        enviar_telegram(safe_token, safe_chat, msg)
                
                # --- LOOP DE PRÓXIMOS JOGOS ---
                for p in prox:
                    try:
                        if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] in ['NS', 'TBD'] and p['fixture']['id'] not in ids_no_radar:
                            if datetime.fromisoformat(p['fixture']['date']) > agora:
                                l_id = normalizar_id(p['league']['id']); l_nm = p['league']['name']
                                if l_id in ids_safe: l_nm += " 🛡️"
                                elif l_id in df_obs['id'].values: l_nm += " ⚠️"
                                agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": l_nm, "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
                    except: pass
        
        # --- [BLOCO DE PERSISTÊNCIA E RETRY] ---
        # Tenta salvar dados pendentes se houver (inclusive histórico)
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
            st.markdown("### 💰 Evolução Financeira (Cálculo Ajustado)")
            c_fin1, c_fin2 = st.columns(2)
            stake_padrao = c_fin1.number_input("Valor da Aposta (Stake):", value=st.session_state.get('stake_padrao', 10.0), step=5.0)
            banca_inicial = c_fin2.number_input("Banca Inicial:", value=st.session_state.get('banca_inicial', 100.0), step=50.0)
            st.session_state['stake_padrao'] = stake_padrao
            st.session_state['banca_inicial'] = banca_inicial
            
            modo_simulacao = st.radio("Cenário de Entrada:", ["Todos os sinais", "Apenas 1 sinal por jogo", "Até 2 sinais por jogo"], horizontal=True)
            
            # --- [NOVO] FILTRO IA ---
            filtrar_ia = st.checkbox("🤖 Somente Sinais APROVADOS pela IA")
            
            df_fin = st.session_state.get('historico_full', pd.DataFrame())
            
            if not df_fin.empty:
                df_fin = df_fin.copy()
                
                # --- APLICAÇÃO DA NOVA LÓGICA DE ODD JUSTA COM TETO ---
                df_fin['Odd_Calc'] = df_fin.apply(lambda row: obter_odd_final_para_calculo(row['Odd'], row['Estrategia']), axis=1)
                
                df_fin = df_fin[df_fin['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
                df_fin = df_fin.sort_values(by=['FID', 'Hora'], ascending=[True, True])
                
                # Aplica o filtro da IA se marcado
                if filtrar_ia:
                    if 'Opiniao_IA' in df_fin.columns:
                        df_fin = df_fin[df_fin['Opiniao_IA'] == 'Aprovado']
                    else:
                        st.warning("⚠️ Coluna de opinião da IA não encontrada nos dados históricos.")

                if modo_simulacao == "Apenas 1 sinal por jogo": df_fin = df_fin.groupby('FID').head(1)
                elif modo_simulacao == "Até 2 sinais por jogo": df_fin = df_fin.groupby('FID').head(2)
                
                if not df_fin.empty:
                    lucros = []; saldo_atual = banca_inicial; historico_saldo = [banca_inicial]
                    qtd_greens = 0; qtd_reds = 0
                    
                    for idx, row in df_fin.iterrows():
                        res = row['Resultado']
                        odd = row['Odd_Calc']
                        
                        if 'GREEN' in res: 
                            lucro = (stake_padrao * odd) - stake_padrao
                            qtd_greens += 1
                        else: 
                            lucro = -stake_padrao
                            qtd_reds += 1
                            
                        saldo_atual += lucro
                        lucros.append(lucro)
                        historico_saldo.append(saldo_atual)
                    
                    df_fin['Lucro'] = lucros
                    total_lucro = sum(lucros)
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
                    fig_fin.add_hline(y=banca_inicial, line_dash="dot", annotation_text="Início", annotation_position="bottom right", line_color="gray")
                    st.plotly_chart(fig_fin, use_container_width=True)
                    
                    with st.expander("🕵️ Auditoria de Odds (Verifique se os valores estão realistas)"):
                        st.dataframe(df_fin[['Data', 'Jogo', 'Estrategia', 'Odd', 'Odd_Calc', 'Resultado', 'Lucro']], use_container_width=True)
                else: 
                    st.info("Aguardando fechamento de sinais para calcular financeiro (ou nenhum sinal aprovado pela IA encontrado).")
            else: 
                st.info("Sem dados históricos para cálculo.")
        with abas[3]: 
            if not hist_hj.empty: 
                df_show = hist_hj.copy()
                if 'Jogo' in df_show.columns and 'Placar_Sinal' in df_show.columns:
                    df_show['Jogo'] = df_show['Jogo'] + " (" + df_show['Placar_Sinal'].astype(str) + ")"
                colunas_esconder = ['FID', 'HomeID', 'AwayID', 'Data_Str', 'Data_DT', 'Odd_Atualizada', 'Placar_Sinal']
                cols_view = [c for c in df_show.columns if c not in colunas_esconder]
                df_show = df_show[cols_view]
                try: df_show['Odd'] = df_show['Odd'].astype(float)
                except: pass
                st.dataframe(df_show.style.format({"Odd": "{:.2f}"}), use_container_width=True, hide_index=True)
            else: st.caption("Vazio.")
        with abas[4]: 
            st.markdown("### 📊 Inteligência de Mercado")
            df_bi = st.session_state.get('historico_full', pd.DataFrame())
            if df_bi.empty: st.warning("Sem dados históricos.")
            else:
                try:
                    df_bi = df_bi.copy()
                    # Tratamento de datas
                    df_bi['Data_Str'] = df_bi['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
                    df_bi['Data_DT'] = pd.to_datetime(df_bi['Data_Str'], errors='coerce')
                    df_bi = df_bi.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
                    
                    hoje = pd.to_datetime(get_time_br().date())
                    
                    # --- DEFINIÇÃO DOS PERÍODOS ---
                    d_hoje = df_bi[df_bi['Data_DT'] == hoje]
                    d_7d = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=7))]
                    d_30d = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=30))]
                    d_total = df_bi

                    # Função auxiliar para formatar texto (Green/Red)
                    def fmt_placar(d):
                        if d.empty: return "0G - 0R (0%)"
                        g = d['Resultado'].str.contains('GREEN', na=False).sum()
                        r = d['Resultado'].str.contains('RED', na=False).sum()
                        t = g + r
                        wr = (g/t*100) if t > 0 else 0
                        return f"{g}G - {r}R ({wr:.0f}%)"

                    # --- NOVA LÓGICA: AUDITORIA DETALHADA DA IA ---
                    def fmt_ia_stats(periodo_df, label_periodo):
                        if 'Opiniao_IA' not in periodo_df.columns: return ""
                        # Filtra apenas finalizados
                        d_fin = periodo_df[periodo_df['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        
                        # Aprovados
                        d_aprov = d_fin[d_fin['Opiniao_IA'] == 'Aprovado']
                        stats_aprov = fmt_placar(d_aprov)
                        
                        # Arriscados
                        d_risk = d_fin[d_fin['Opiniao_IA'] == 'Arriscado']
                        stats_risk = fmt_placar(d_risk)
                        
                        return f"🤖 IA ({label_periodo}):\n👍 Aprovados: {stats_aprov}\n⚠️ Arriscados: {stats_risk}"

                    msg_ia_hoje = fmt_ia_stats(d_hoje, "Hoje")
                    msg_ia_7d = fmt_ia_stats(d_7d, "7 Dias")
                    msg_ia_30d = fmt_ia_stats(d_30d, "30 Dias")
                    msg_ia_total = fmt_ia_stats(d_total, "Geral")

                    if 'bi_filter' not in st.session_state: st.session_state['bi_filter'] = "Tudo"
                    filtro = st.selectbox("📅 Período", ["Tudo", "Hoje", "7 Dias", "30 Dias"], key="bi_select")
                    if filtro == "Hoje": df_show = d_hoje
                    elif filtro == "7 Dias": df_show = d_7d
                    elif filtro == "30 Dias": df_show = d_30d
                    else: df_show = df_bi 
                    
                    if not df_show.empty:
                        gr = df_show['Resultado'].str.contains('GREEN').sum(); rd = df_show['Resultado'].str.contains('RED').sum()
                        tt = len(df_show); ww = (gr/tt*100) if tt>0 else 0
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Sinais", tt); m2.metric("Greens", gr); m3.metric("Reds", rd); m4.metric("Assertividade", f"{ww:.1f}%")
                        st.divider()
                        
                        # --- ANÁLISE DE LIGAS DETALHADA ---
                        st.markdown("### 🏆 Melhores e Piores Ligas (Com Drill-Down)")
                        
                        df_finished = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        if not df_finished.empty:
                            # 1. Preparar dados
                            stats_ligas = df_finished.groupby('Liga')['Resultado'].apply(lambda x: pd.Series({
                                'Winrate': (x.str.contains('GREEN').sum() / len(x) * 100),
                                'Total': len(x),
                                'Reds': x.str.contains('RED').sum(),
                                'Greens': x.str.contains('GREEN').sum()
                            })).unstack()
                            
                            stats_ligas = stats_ligas[stats_ligas['Total'] >= 2] # Filtro mínimo
                            
                            col_top, col_worst = st.columns(2)
                            
                            with col_top:
                                st.caption("🌟 Top Ligas (Mais Lucrativas)")
                                top_ligas = stats_ligas.sort_values(by=['Winrate', 'Total'], ascending=[False, False]).head(10)
                                st.dataframe(top_ligas[['Winrate', 'Total', 'Greens']].style.format({'Winrate': '{:.2f}%', 'Total': '{:.0f}', 'Greens': '{:.0f}'}), use_container_width=True)
                                
                            with col_worst:
                                st.caption("💀 Ligas Críticas (Onde estamos errando?)")
                                worst_ligas = stats_ligas.sort_values(by=['Reds'], ascending=False).head(10)
                                
                                # Cria uma lista visual para mostrar o motivo
                                dados_drill = []
                                for liga, row in worst_ligas.iterrows():
                                    if row['Reds'] > 0:
                                        # Filtra erros dessa liga específica
                                        erros_liga = df_finished[(df_finished['Liga'] == liga) & (df_finished['Resultado'].str.contains('RED'))]
                                        # Acha a estratégia vilã
                                        pior_strat = erros_liga['Estrategia'].value_counts().head(1)
                                        nome_strat = pior_strat.index[0] if not pior_strat.empty else "-"
                                        qtd_strat = pior_strat.values[0] if not pior_strat.empty else 0
                                        
                                        dados_drill.append({
                                            "Liga": liga,
                                            "Total Reds": int(row['Reds']),
                                            "Pior Estratégia": nome_strat,
                                            "Reds na Estratégia": int(qtd_strat)
                                        })
                                
                                if dados_drill:
                                    df_drill = pd.DataFrame(dados_drill)
                                    st.dataframe(df_drill, use_container_width=True, hide_index=True)
                                else:
                                    st.success("Nenhuma liga com Reds significativos no período.")

                        st.divider()

                        # --- AUDITORIA DA IA POR PERÍODO (Visual) ---
                        st.markdown("### 🧠 Auditoria da IA (Aprovações vs Resultado)")
                        if 'Opiniao_IA' in df_show.columns:
                            df_audit = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                            if not df_audit.empty:
                                # Tabela pivot dinâmica
                                pivot = pd.crosstab(df_audit['Opiniao_IA'], df_audit['Resultado'], margins=True)
                                # Adiciona % de acerto visualmente
                                if '✅ GREEN' in pivot.columns and 'All' in pivot.columns:
                                    pivot['Winrate %'] = (pivot['✅ GREEN'] / pivot['All'] * 100)
                                
                                # Formatação aplicada aqui
                                format_dict = {'Winrate %': '{:.2f}%'}
                                for col in pivot.columns:
                                    if col != 'Winrate %':
                                        format_dict[col] = '{:.0f}'
                                    
                                st.dataframe(pivot.style.format(format_dict, na_rep="-").highlight_max(axis=0, color='#1F4025'), use_container_width=True)

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
                        
                        st.markdown("### ⚽ Raio-X por Jogo (Volume de Sinais)")
                        sinais_por_jogo = df_show['Jogo'].value_counts()
                        c_vol1, c_vol2, c_vol3 = st.columns(3)
                        c_vol1.metric("Jogos Únicos", len(sinais_por_jogo))
                        c_vol2.metric("Média Sinais/Jogo", f"{sinais_por_jogo.mean():.1f}")
                        c_vol3.metric("Máx Sinais num Jogo", sinais_por_jogo.max())
                        st.caption("📋 Detalhe dos Jogos com Mais Sinais")
                        detalhe = df_show.groupby('Jogo')['Resultado'].value_counts().unstack(fill_value=0)
                        detalhe['Total'] = detalhe.sum(axis=1)
                        if '✅ GREEN' not in detalhe: detalhe['✅ GREEN'] = 0
                        if '❌ RED' not in detalhe: detalhe['❌ RED'] = 0
                        st.dataframe(detalhe[['Total', '✅ GREEN', '❌ RED']].sort_values('Total', ascending=False).head(10), use_container_width=True)
                
                except Exception as e: 
                    st.error(f"Erro ao carregar BI: {e}")

        with abas[5]: 
            st.dataframe(st.session_state['df_black'][['País', 'Liga', 'Motivo']], use_container_width=True, hide_index=True)
        
        with abas[6]: 
            df_safe_show = st.session_state.get('df_safe', pd.DataFrame()).copy()
            if not df_safe_show.empty:
                def calc_risco(x):
                    try: v = int(float(str(x)))
                    except: v = 0
                    return "🟢 100% Estável" if v == 0 else f"⚠️ Atenção ({v}/10)"
                df_safe_show['Status Risco'] = df_safe_show['Strikes'].apply(calc_risco)
                st.dataframe(df_safe_show[['País', 'Liga', 'Motivo', 'Status Risco']], use_container_width=True, hide_index=True)
            else: 
                st.info("Nenhuma liga segura ainda.")

        with abas[7]: 
            df_vip_show = st.session_state.get('df_vip', pd.DataFrame()).copy()
            if not df_vip_show.empty: 
                df_vip_show['Strikes'] = df_vip_show['Strikes'].apply(formatar_inteiro_visual)
            st.dataframe(df_vip_show[['País', 'Liga', 'Data_Erro', 'Strikes']], use_container_width=True, hide_index=True)

        with abas[8]:
            st.markdown(f"### 💾 Banco de Dados de Partidas (Firebase)")
            st.caption("A IA usa esses dados para criar novas estratégias. Os dados são salvos na nuvem.")
            
            if db_firestore:
                # Botão para carregar sob demanda (Economiza MUITA leitura)
                col_fb1, col_fb2 = st.columns([1, 3])
                if col_fb1.button("🔄 Carregar/Atualizar Tabela"):
                    try:
                        with st.spinner("Baixando dados do Firebase..."):
                            # Carrega apenas os últimos 50 jogos
                            docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(50).stream()
                            data = [d.to_dict() for d in docs]
                            st.session_state['cache_firebase_view'] = data # Salva no cache da sessão
                            st.toast(f"Dados atualizados! ({len(data)} jogos)")
                    except Exception as e:
                        st.error(f"Erro ao ler Firebase: {e}")

                # Exibe o que está no cache (Memória RAM) sem gastar leitura do banco
                if 'cache_firebase_view' in st.session_state and st.session_state['cache_firebase_view']:
                    st.success(f"📂 Visualizando {len(st.session_state['cache_firebase_view'])} registros (Cache Local)")
                    st.dataframe(pd.DataFrame(st.session_state['cache_firebase_view']), use_container_width=True)
                else:
                    st.info("ℹ️ Clique no botão acima para visualizar os dados salvos (Isso consome leituras da cota).")
            else:
                st.warning("⚠️ Firebase não conectado.")

        for i in range(INTERVALO, 0, -1):
            st.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()
else:
    with placeholder_root.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera. Configure na lateral.")


