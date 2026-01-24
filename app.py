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
# 1. CONFIGURAÇÃO INICIAL E CSS (ORIGINAL INTACTO)
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
# 2. INICIALIZAÇÃO DE VARIÁVEIS (ORIGINAL INTACTO)
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
if 'last_run' not in st.session_state: st.session_state['last_run'] = 0
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

# ==============================================================================
# 2. FUNÇÕES AUXILIARES, DADOS E API (ORIGINAL + AJUSTES TÉCNICOS)
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

# --- GERENCIAMENTO DE PLANILHAS ---

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
    """SALVAMENTO OTIMIZADO COM HASH (SÍNCRONO E SEGURO)"""
    if nome_aba in ["Historico", "Seguras", "Obs"] and df_para_salvar.empty: return False
    if st.session_state.get('BLOQUEAR_SALVAMENTO', False):
        st.session_state['precisa_salvar'] = True 
        return False
    try:
        data_hash = hashlib.md5(pd.util.hash_pandas_object(df_para_salvar, index=True).values).hexdigest()
        chave_hash = f'hash_last_save_{nome_aba}'
        if st.session_state.get(chave_hash, '') == data_hash:
            if nome_aba == "Historico": st.session_state['precisa_salvar'] = False
            return True 
        conn.update(worksheet=nome_aba, data=df_para_salvar)
        st.session_state[chave_hash] = data_hash
        if nome_aba == "Historico": st.session_state['precisa_salvar'] = False
        return True
    except: 
        st.session_state['precisa_salvar'] = True
        return False

# --- CÉREBRO DA IA ADAPTATIVO ---

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

    eh_under = "Morno" in estrategia or "Under" in estrategia
    objetivo = "O OBJETIVO É UNDER (NÃO SAIR GOL)." if eh_under else "O OBJETIVO É OVER (SAIR GOL)."
    criterio = "APROVADO se o jogo estiver travado e sem chutes." if eh_under else "APROVADO se houver pressão e perigo real."

    prompt = f"""Atue como ANALISTA EV+. OBJETIVO: {objetivo}. 
    ESTRATÉGIA: {estrategia}. DADOS: {dados_jogo['jogo']} | Placar: {dados_jogo['placar']} | Tempo: {tempo}'.
    STATS: Pressão {rh}x{ra}, Chutes na área {gv(s1,'Shots insidebox')}x{gv(s2,'Shots insidebox')}, Posse {gv(s1,'Ball Possession')}.
    {extra_context}. REQUISITO: {criterio}. 
    RESPONDA APENAS: Aprovado/Arriscado - [Motivo curto] | PROB: [0-100]%"""

    try:
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(temperature=0.2), request_options={"timeout": 10})
        st.session_state['gemini_usage']['used'] += 1
        txt = response.text.strip().replace("**", "")
        prob = re.search(r'PROB:\s*(\d+)%', txt).group(1)+'%' if re.search(r'PROB:\s*(\d+)%', txt) else "N/A"
        veredicto = "Aprovado" if "aprovado" in txt.lower()[:20] else "Arriscado"
        motivo = txt.split('-')[-1].split('|')[0].strip()
        emoji = "✅" if veredicto == "Aprovado" else "⚠️"
        return f"\n🤖 <b>IA:</b> {emoji} <b>{veredicto.upper()}</b> - <i>{motivo}</i>", prob
    except: return "", "N/A"
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
            'fid': fid, 'data_hora': get_time_br().strftime('%Y-%m-%d %H:%M'),
            'liga': sanitize(jogo_api['league']['name']), 'home_id': str(jogo_api['teams']['home']['id']),
            'away_id': str(jogo_api['teams']['away']['id']), 'jogo': f"{sanitize(jogo_api['teams']['home']['name'])} x {sanitize(jogo_api['teams']['away']['name'])}",
            'placar_final': f"{jogo_api['goals']['home']}x{jogo_api['goals']['away']}", 'rating_home': str(rate_h), 'rating_away': str(rate_a),
            'estatisticas': {
                'chutes_total': gv(s1, 'Total Shots') + gv(s2, 'Total Shots'), 'chutes_gol': gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal'),
                'chutes_area': gv(s1, 'Shots insidebox') + gv(s2, 'Shots insidebox'), 'escanteios_total': gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks'),
                'escanteios_casa': gv(s1, 'Corner Kicks'), 'escanteios_fora': gv(s2, 'Corner Kicks'), 'faltas_total': gv(s1, 'Fouls') + gv(s2, 'Fouls'),
                'cartoes_amarelos': gv(s1, 'Yellow Cards') + gv(s2, 'Yellow Cards'), 'cartoes_vermelhos': gv(s1, 'Red Cards') + gv(s2, 'Red Cards'),
                'posse_casa': str(gv(s1, 'Ball Possession')), 'ataques_perigosos': gv(s1, 'Dangerous Attacks') + gv(s2, 'Dangerous Attacks'),
                'impedimentos': gv(s1, 'Offsides') + gv(s2, 'Offsides'), 'passes_pct_casa': str(gv(s1, 'Passes %')).replace('%',''), 'passes_pct_fora': str(gv(s2, 'Passes %')).replace('%','')
            }
        }
        db_firestore.collection("BigData_Futebol").document(fid).set(item_bigdata)
        st.session_state['jogos_salvos_bigdata'].add(fid)
    except: pass

def momentum(fid, sog_h, sog_a):
    mem = st.session_state['memoria_pressao'].get(fid, {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
    now = datetime.now()
    if sog_h > mem.get('sog_h', 0): mem['h_t'].extend([now]*(sog_h-mem['sog_h']))
    if sog_a > mem.get('sog_a', 0): mem['a_t'].extend([now]*(sog_a-mem['sog_a']))
    mem['h_t'] = [t for t in mem.get('h_t', []) if now - t <= timedelta(minutes=7)]
    mem['a_t'] = [t for t in mem.get('a_t', []) if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a
    st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_t']), len(mem['a_t'])

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal'); sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
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
# --- TELEGRAM, RESULTADOS E RELATÓRIOS ---

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
    deve_enviar = (key_orig in st.session_state.get('alertas_enviados', set()))
    if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
    
    if (gh + ga) > (ph + pa):
        if any(x in strat for x in ["Vovô", "Back Favorito"]): return False
        if "Morno" in strat or "Under" in strat:
            if (gh+ga) >= 2:
                sinal['Resultado'] = '❌ RED'
                if deve_enviar and gerar_chave_universal(fid, strat, "RED") not in st.session_state['alertas_enviados']:
                    enviar_telegram(token, chats, f"❌ <b>RED | OVER 1.5 BATIDO</b>\n⚽ {sinal['Jogo']}\n🎯 {strat}")
                    st.session_state['alertas_enviados'].add(gerar_chave_universal(fid, strat, "RED"))
                st.session_state['precisa_salvar'] = True; return True
        else:
            sinal['Resultado'] = '✅ GREEN'
            if deve_enviar and gerar_chave_universal(fid, strat, "GREEN") not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {sinal['Jogo']}\n📈 Placar: <b>{gh}x{ga}</b>\n🎯 {strat}")
                st.session_state['alertas_enviados'].add(gerar_chave_universal(fid, strat, "GREEN"))
            st.session_state['precisa_salvar'] = True; return True

    if st_short in ['FT', 'AET', 'PEN', 'ABD']:
        if "Vovô" in strat or "Back" in strat:
            resultado = '❌ RED'
            if (ph > pa and gh > ga) or (pa > ph and ga > gh): resultado = '✅ GREEN'
            sinal['Resultado'] = resultado
            if deve_enviar: enviar_telegram(token, chats, f"{resultado} | FINALIZADO\n⚽ {sinal['Jogo']}")
            st.session_state['precisa_salvar'] = True; return True
        if "Morno" in strat or "Under" in strat:
            sinal['Resultado'] = '✅ GREEN'
            if deve_enviar: enviar_telegram(token, chats, f"✅ GREEN | FINALIZADO\n⚽ {sinal['Jogo']}")
            st.session_state['precisa_salvar'] = True; return True
        sinal['Resultado'] = '❌ RED'
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
    ids_live = {str(j['fixture']['id']): j for j in jogos_live}; updates = []
    for s in snipers:
        jogo = ids_live.get(str(s['FID']))
        if jogo and jogo['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
            gh = jogo['goals']['home'] or 0; ga = jogo['goals']['away'] or 0
            p = s['Placar_Sinal'].split('x'); gs = int(p[0]) + int(p[1])
            res = '✅ GREEN' if (gh + ga) > gs else '❌ RED'
            s['Resultado'] = res; updates.append(s)
            if gerar_chave_universal(s['FID'], s['Estrategia'], "SINAL") in st.session_state.get('alertas_enviados', set()):
                enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], f"{res} SNIPER: {s['Jogo']}")
            st.session_state['precisa_salvar'] = True
    if updates: atualizar_historico_ram(updates)

def verificar_var_rollback(jogos_live, token, chats):
    greens = [s for s in st.session_state.get('historico_sinais', []) if 'GREEN' in str(s['Resultado'])]
    for s in greens:
        j = next((j for j in jogos_live if j['fixture']['id'] == int(clean_fid(s['FID']))), None)
        if j:
            gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
            ph, pa = map(int, s['Placar_Sinal'].split('x'))
            if (gh + ga) <= (ph + pa):
                s['Resultado'] = 'Pendente'; st.session_state['precisa_salvar'] = True
                if gerar_chave_universal(s['FID'], s['Estrategia'], "SINAL") in st.session_state.get('alertas_enviados', set()):
                    enviar_telegram(token, chats, f"⚠️ VAR GOL ANULADO: {s['Jogo']}")

def deve_buscar_stats(tempo, status): return status == 'HT' or 0 <= tempo <= 95

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
        for future in as_completed(futures):
            fid, stats, headers = future.result()
            if stats: resultados[fid] = stats; update_api_usage(headers)
    return resultados

def enviar_relatorio_bi(token, chat_ids):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    try:
        df_hj = df[df['Data'] == get_time_br().strftime('%Y-%m-%d')]
        g = df_hj['Resultado'].str.contains('GREEN').sum(); r = df_hj['Resultado'].str.contains('RED').sum()
        msg = f"📈 <b>RELATÓRIO BI</b>\n📅 Hoje: {g}G | {r}R\n🧠 IA: {analisar_bi_com_ia()}"
        enviar_telegram(token, chat_ids, msg)
    except: pass

def verificar_automacao_bi(token, chat_ids, stake):
    ag = get_time_br(); hj = ag.strftime('%Y-%m-%d')
    if st.session_state['last_check_date'] != hj:
        st.session_state.update({'bi_enviado': False, 'financeiro_enviado': False, 'last_check_date': hj})
    if ag.hour == 23 and ag.minute >= 30 and not st.session_state['bi_enviado']:
        enviar_relatorio_bi(token, chat_ids); st.session_state['bi_enviado'] = True

# --- 4.2 UI E LOOP DE EXECUÇÃO ---

with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=True):
        st.session_state['API_KEY'] = st.text_input("API Key:", value=st.session_state['API_KEY'], type="password")
        st.session_state['TG_TOKEN'] = st.text_input("Bot Token:", value=st.session_state['TG_TOKEN'], type="password")
        st.session_state['TG_CHAT'] = st.text_input("Chat IDs:", value=st.session_state['TG_CHAT'])
        INTERVALO = st.slider("Ciclo (s):", 60, 300, 60)
        if st.button("🧹 Limpar Cache"): st.cache_data.clear(); carregar_tudo(True); st.rerun()

    with st.expander("🛠️ Ferramentas Manuais"):
        if st.button("🌅 Sniper Matinal"): st.markdown(gerar_insights_matinais_ia(st.session_state['API_KEY']))
        if st.button("🔄 Backfill BigData"):
            tj = buscar_agenda_cached(st.session_state['API_KEY'], get_time_br().strftime('%Y-%m-%d'))
            pend = [j for j in tj if j['fixture']['status']['short'] in ['FT','PEN'] and str(j['fixture']['id']) not in st.session_state['jogos_salvos_bigdata']]
            for fid, s in atualizar_stats_em_paralelo(pend[:5], st.session_state['API_KEY']).items():
                jo = next(x for x in tj if x['fixture']['id'] == fid); salvar_bigdata(jo, s)
            st.success("Processado!")

    with st.expander("💰 Gestão"):
        st.session_state['stake_padrao'] = st.number_input("Stake", value=st.session_state['stake_padrao'])
        st.session_state['banca_inicial'] = st.number_input("Banca", value=st.session_state['banca_inicial'])

    with st.expander("📶 Consumo API"):
        verificar_reset_diario(); u = st.session_state['api_usage']; p = min(u['used']/u['limit'], 1.0) if u['limit']>0 else 0
        st.progress(p); st.caption(f"Uso: {u['used']} / {u['limit']} ({p*100:.1f}%)")

    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    if st.button("☢️ ZERAR ROBÔ", type="primary"):
        st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST); salvar_aba("Historico", st.session_state['historico_full']); st.rerun()

if st.session_state.ROBO_LIGADO:
    with placeholder_root.container():
        if time.time() - st.session_state['last_run'] >= INTERVALO:
            status_main = st.status("🚀 Processando Ciclo...", expanded=True)
            status_main.write("📂 Carregando caches...")
            carregar_tudo()
            verificar_automacao_bi(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], st.session_state['stake_padrao'])
            
            try:
                status_main.write("📡 Conectando API Sports...")
                res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": st.session_state['API_KEY']}, params={"live": "all", "timezone": "America/Sao_Paulo"}).json()
                jogos_live = res.get('response', [])
                check_green_red_hibrido(jogos_live, st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], st.session_state['API_KEY'])
                conferir_resultados_sniper(jogos_live, st.session_state['API_KEY'])
                verificar_var_rollback(jogos_live, st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'])
                
                status_main.write("🧠 Analisando Radar...")
                radar_data = []
                for j in jogos_live:
                    lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                    if lid in [normalizar_id(x) for x in st.session_state['df_black']['id'].values]: continue
                    tempo = j['fixture']['status']['elapsed'] or 0; status = j['fixture']['status']['short']
                    if status == 'FT': continue
                    
                    stats = []
                    if deve_buscar_stats(tempo, status):
                        u_chk = st.session_state['controle_stats'].get(fid, datetime.min)
                        if (datetime.now() - u_chk).total_seconds() > 60:
                            f, s_res, h = fetch_stats_single(fid, st.session_state['API_KEY'])
                            if s_res: st.session_state['controle_stats'][fid] = datetime.now(); st.session_state[f"st_{fid}"] = s_res; update_api_usage(h)
                    
                    stats = st.session_state.get(f"st_{fid}", [])
                    sinais = processar(j, stats, tempo, f"{j['goals']['home']}x{j['goals']['away']}") if stats else []
                    
                    if sinais:
                        for s in sinais:
                            uid = gerar_chave_universal(fid, s['tag'], "SINAL")
                            if uid not in st.session_state['alertas_enviados']:
                                st.session_state['alertas_enviados'].add(uid)
                                odd = get_live_odds(fid, st.session_state['API_KEY'], s['tag'], (j['goals']['home'] or 0)+(j['goals']['away'] or 0), tempo)
                                op, pb = consultar_ia_gemini({'jogo': f"{j['teams']['home']['name']}x{j['teams']['away']['name']}", 'placar': f"{j['goals']['home']}x{j['goals']['away']}"}, s['tag'], stats, s['rh'], s['ra'])
                                if "Aprovado" in op:
                                    msg = f"🚨 <b>SINAL {s['tag']}</b>\n⚽ {j['teams']['home']['name']}x{j['teams']['away']['name']}\n⏰ {tempo}'\n{s['ordem']}\n💰 Odd: {odd}{op}"
                                    enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], msg)
                                item = {"FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']}x{j['teams']['away']['name']}", "Placar_Sinal": f"{j['goals']['home']}x{j['goals']['away']}", "Estrategia": s['tag'], "Resultado": "Pendente", "Odd": odd, "Opiniao_IA": "Aprovado" if "Aprovado" in op else "Arriscado"}
                                adicionar_historico(item)
                    radar_data.append({"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {j['goals']['home']}x{j['goals']['away']} {j['teams']['away']['name']}", "Tempo": f"{tempo}'", "Status": "✅" if sinais else "👁️"})
                st.session_state['radar_cache'] = radar_data
            except: pass
            
            if st.session_state.get('precisa_salvar'):
                status_main.write("💾 Sincronizando Planilha...")
                salvar_aba("Historico", st.session_state['historico_full'])
            
            st.session_state['last_run'] = time.time()
            status_main.update(label="✅ Ciclo Finalizado!", state="complete", expanded=False)

        # DASHBOARD
        h_hj = pd.DataFrame(st.session_state.get('historico_sinais', []))
        t, g, r, w = calcular_stats(h_hj)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t}</div><div class="metric-sub">{g}G | {r}R</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(st.session_state.get("radar_cache", []))}</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Winrate</div><div class="metric-value">{w:.0f}%</div></div>', unsafe_allow_html=True)
        
        tabs = st.tabs(["📡 Radar", "💰 Financeiro", "📜 Histórico", "📈 BI & Analytics", "🚫 Blacklist", "🛡️ Seguras", "💾 Big Data"])
        with tabs[0]: st.dataframe(st.session_state.get('radar_cache', []), use_container_width=True, hide_index=True)
        with tabs[1]:
            st.write(f"### Lucro: R$ {((g*st.session_state['stake_padrao']*0.5) - (r*st.session_state['stake_padrao'])):.2f}")
            fig = px.line(y=[100, 110, 105, 120], title="Evolução Simulada"); st.plotly_chart(fig, use_container_width=True)
        with tabs[2]: st.dataframe(h_hj, use_container_width=True, hide_index=True)
        with tabs[3]: enviar_relatorio_bi(None, None) # Apenas gatilho visual se necessário
        with tabs[4]: st.dataframe(st.session_state['df_black'], use_container_width=True)
        with tabs[5]: st.dataframe(st.session_state['df_safe'], use_container_width=True)
        with tabs[6]: 
            if db_firestore and st.button("Baixar 50 do Firebase"):
                docs = db_firestore.collection("BigData_Futebol").limit(50).stream()
                st.dataframe([d.to_dict() for d in docs], use_container_width=True)

        tempo_restante = int(INTERVALO - (time.time() - st.session_state['last_run']))
        if tempo_restante > 0:
            st.markdown(f'<div class="footer-timer">⏳ Próxima varredura em {tempo_restante}s</div>', unsafe_allow_html=True)
            time.sleep(1); st.rerun()
        else: st.rerun()
else:
    st.title("❄️ Neves Analytics")
    st.info("💡 Robô em espera. Ligue na lateral.")        
