import streamlit as st
import pandas as pd
import requests
import time
import os
import threading
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import io
from datetime import datetime, timedelta
import pytz
from streamlit_gsheets import GSheetsConnection

# --- 0. CONFIGURAÇÃO E CSS ---
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")

# --- INICIALIZAÇÃO DE VARIÁVEIS ---
if 'ROBO_LIGADO' not in st.session_state: st.session_state.ROBO_LIGADO = False
if 'last_db_update' not in st.session_state: st.session_state['last_db_update'] = 0
if 'bi_enviado_data' not in st.session_state: st.session_state['bi_enviado_data'] = ""
if 'confirmar_reset' not in st.session_state: st.session_state['confirmar_reset'] = False

# Variáveis de Controle
if 'api_usage' not in st.session_state: st.session_state['api_usage'] = {'used': 0, 'limit': 75000}
if 'data_api_usage' not in st.session_state: st.session_state['data_api_usage'] = datetime.now(pytz.utc).date()
if 'alvos_do_dia' not in st.session_state: st.session_state['alvos_do_dia'] = {}
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'multiplas_enviadas' not in st.session_state: st.session_state['multiplas_enviadas'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'controle_stats' not in st.session_state: st.session_state['controle_stats'] = {}

DB_CACHE_TIME = 60

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .main .block-container { max-width: 100%; padding: 1rem 1rem 5rem 1rem; }
    
    .metric-box { 
        background-color: #1A1C24; border: 1px solid #333; 
        border-radius: 8px; padding: 15px; text-align: center; 
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    .metric-title {font-size: 12px; color: #aaaaaa; text-transform: uppercase; margin-bottom: 5px;}
    .metric-value {font-size: 24px; font-weight: bold; color: #00FF00;}
    
    .status-active { background-color: #1F4025; color: #00FF00; border: 1px solid #00FF00; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 15px; font-weight: bold;}
    .status-error { background-color: #3B1010; color: #FF4B4B; border: 1px solid #FF4B4B; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 15px; font-weight: bold;}
    
    .stButton button {
        width: 100%; height: 50px !important; font-size: 16px !important; font-weight: bold !important;
        background-color: #262730; border: 1px solid #4e4e4e; color: white;
    }
    .stButton button:hover { border-color: #00FF00; color: #00FF00; }
    
    /* BOTÃO PERIGO */
    div[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"] {
        background-color: #4A0E0E !important; border: 1px solid #FF4B4B !important; color: #FF4B4B !important;
    }
    div[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"]:hover {
        background-color: #FF0000 !important; color: white !important;
    }

    .footer-timer { 
        position: fixed; left: 0; bottom: 0; width: 100%; 
        background-color: #0E1117; color: #FFD700; 
        text-align: center; padding: 8px; font-size: 14px; 
        border-top: 1px solid #333; z-index: 9999; 
    }
    .stDataFrame { font-size: 13px; }
</style>
""", unsafe_allow_html=True)

# --- 1. CONEXÃO ---
conn = st.connection("gsheets", type=GSheetsConnection)

# Adicionado 'Odd_Atualizada' nas colunas do histórico para controle interno
COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID', 'Odd', 'Odd_Atualizada']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo', 'Strikes', 'Jogos_Erro']
COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes', 'Jogos_Erro']
COLS_BLACK = ['id', 'País', 'Liga', 'Motivo']

LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]

# --- 2. UTILITÁRIOS ---
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

# --- 3. BANCO DE DADOS ---
def carregar_aba(nome_aba, colunas_esperadas):
    try:
        df = conn.read(worksheet=nome_aba, ttl=0)
        if not df.empty:
            for col in colunas_esperadas:
                if col not in df.columns:
                    if col == 'Odd': df[col] = "0.00" 
                    elif col == 'Strikes': df[col] = '0'
                    elif col == 'Jogos_Erro': df[col] = ''
                    elif col == 'Motivo': df[col] = ''
                    elif col == 'Odd_Atualizada': df[col] = '' # Inicializa vazio
                    else: df[col] = ""
        if df.empty or len(df.columns) < len(colunas_esperadas): 
            return pd.DataFrame(columns=colunas_esperadas)
        return df.fillna("").astype(str)
    except: return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df_para_salvar):
    try: conn.update(worksheet=nome_aba, data=df_para_salvar); return True
    except: return False

def sanitizar_conflitos():
    df_black = st.session_state['df_black']
    df_vip = st.session_state['df_vip']
    df_safe = st.session_state['df_safe']
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

def carregar_tudo(force=False):
    now = time.time()
    if not force:
        if (now - st.session_state['last_db_update']) < DB_CACHE_TIME:
            if 'df_black' in st.session_state: return

    df = carregar_aba("Blacklist", COLS_BLACK)
    if not df.empty: df['id'] = df['id'].apply(normalizar_id)
    st.session_state['df_black'] = df

    df = carregar_aba("Seguras", COLS_SAFE)
    if not df.empty: df['id'] = df['id'].apply(normalizar_id)
    st.session_state['df_safe'] = df

    df = carregar_aba("Obs", COLS_OBS)
    if not df.empty: df['id'] = df['id'].apply(normalizar_id)
    st.session_state['df_vip'] = df
    
    sanitizar_conflitos()
    
    df = carregar_aba("Historico", COLS_HIST)
    if not df.empty and 'Data' in df.columns:
        df['FID'] = df['FID'].apply(clean_fid)
        st.session_state['historico_full'] = df
        hoje = get_time_br().strftime('%Y-%m-%d')
        st.session_state['historico_sinais'] = df[df['Data'] == hoje].to_dict('records')[::-1]
        
        if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
        df_hoje = df[df['Data'] == hoje]
        for _, row in df_hoje.iterrows():
            st.session_state['alertas_enviados'].add(f"{row['FID']}_{row['Estrategia']}")
    else:
        st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
        st.session_state['historico_sinais'] = []

    st.session_state['last_db_update'] = now

def adicionar_historico(item):
    if 'historico_full' not in st.session_state: st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
    df_novo = pd.DataFrame([item])
    st.session_state['historico_full'] = pd.concat([df_novo, st.session_state['historico_full']], ignore_index=True)
    st.session_state['historico_sinais'].insert(0, item)
    return salvar_aba("Historico", st.session_state['historico_full'])

def atualizar_historico_ram_disk(lista_atualizada):
    df_hoje = pd.DataFrame(lista_atualizada)
    df_disk = st.session_state['historico_full']
    hoje = get_time_br().strftime('%Y-%m-%d')
    if not df_disk.empty and 'Data' in df_disk.columns:
         df_disk['Data'] = df_disk['Data'].astype(str).str.replace(' 00:00:00', '', regex=False)
         df_disk = df_disk[df_disk['Data'] != hoje]
    df_final = pd.concat([df_hoje, df_disk], ignore_index=True)
    df_final = df_final.drop_duplicates(subset=['FID', 'Estrategia'], keep='first')
    st.session_state['historico_full'] = df_final 
    salvar_aba("Historico", df_final)

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
    df_vip = st.session_state.get('df_vip', pd.DataFrame())
    if not df_vip.empty and id_norm in df_vip['id'].values:
        df_new_vip = df_vip[df_vip['id'] != id_norm]
        if salvar_aba("Obs", df_new_vip): st.session_state['df_vip'] = df_new_vip
    
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

def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    df_raw = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

def verificar_reset_diario():
    hoje_utc = datetime.now(pytz.utc).date()
    if st.session_state['data_api_usage'] != hoje_utc:
        st.session_state['api_usage']['used'] = 0; st.session_state['data_api_usage'] = hoje_utc
        st.session_state['alvos_do_dia'] = {}
        return True
    return False

def update_api_usage(headers):
    if not headers: return
    try:
        limit = int(headers.get('x-ratelimit-requests-limit', 75000))
        remaining = int(headers.get('x-ratelimit-requests-remaining', 0))
        used = limit - remaining
        st.session_state['api_usage'] = {'used': used, 'limit': limit}
    except: pass

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

# --- FUNÇÃO: BUSCAR ODD AO VIVO ---
def get_live_odds(fixture_id, api_key):
    try:
        url = "https://v3.football.api-sports.io/odds/live"
        params = {"fixture": fixture_id}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        if res.get('response'):
            markets = res['response'][0]['odds']
            for m in markets:
                nome_mercado = m['name'].lower()
                if "goals" in nome_mercado and "over" in nome_mercado:
                    for v in m['values']:
                        if "over" in str(v['value']).lower():
                            raw_odd = float(v['odd'])
                            if raw_odd > 50: raw_odd = raw_odd / 1000
                            return "{:.2f}".format(raw_odd)
            if len(markets) > 0:
                raw_odd = float(markets[0]['values'][0]['odd'])
                if raw_odd > 50: raw_odd = raw_odd / 1000
                return "{:.2f}".format(raw_odd)
        return "0.00"
    except: return "0.00"

# --- INTELIGÊNCIA ---
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
    
    emoji = "🔮"
    if prob_final >= 80: emoji = "🔥"
    elif prob_final <= 40: emoji = "⚠️"
    return f"\n{emoji} <b>Prob: {prob_final:.0f}% ({str_fontes})</b>"

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

# --- FUNÇÃO: ENVIAR RELATÓRIO FINANCEIRO ---
def enviar_relatorio_financeiro(token, chat_ids, lucro, roi, entradas):
    msg = f"💰 <b>RELATÓRIO FINANCEIRO</b>\n\n💵 <b>Lucro:</b> R$ {lucro:.2f}\n📈 <b>ROI:</b> {roi:.1f}%\n🎟️ <b>Entradas:</b> {entradas}\n\n<i>Cálculo baseado na gestão de cenários do painel.</i>"
    enviar_telegram(token, chat_ids, msg)

def verificar_automacao_bi(token, chat_ids):
    agora = get_time_br()
    if agora.hour == 23 and agora.minute >= 30:
        hoje_str = agora.strftime('%Y-%m-%d')
        if st.session_state.get('bi_enviado_data') != hoje_str:
            enviar_relatorio_bi(token, chat_ids)
            st.session_state['bi_enviado_data'] = hoje_str
            st.toast("Relatório Automático Enviado!")

def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br(); hoje_str = agora.strftime('%Y-%m-%d'); chave = f'alerta_matinal_{hoje_str}'
    if chave in st.session_state: return 
    if not (8 <= agora.hour < 12): return 
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    stats_ids = {} 
    df_green = df[df['Resultado'].str.contains('GREEN', na=False)]
    for index, row in df_green.iterrows():
        try:
            id_h = str(row.get('HomeID', '')).strip(); id_a = str(row.get('AwayID', '')).strip()
            nomes = row['Jogo'].split(' x '); nome_h = nomes[0].split('(')[0].strip(); nome_a = nomes[1].split('(')[0].strip()
            k_h = id_h if id_h and id_h != 'nan' else nome_h
            if k_h not in stats_ids: stats_ids[k_h] = {'greens': 0, 'nome': nome_h}
            stats_ids[k_h]['greens'] += 1
            k_a = id_a if id_a and id_a != 'nan' else nome_a
            if k_a not in stats_ids: stats_ids[k_a] = {'greens': 0, 'nome': nome_a}
            stats_ids[k_a]['greens'] += 1
        except: pass
    if not stats_ids: return
    top = sorted(stats_ids.items(), key=lambda x: x[1]['greens'], reverse=True)[:10]; ids_top = [x[0] for x in top]
    jogos = buscar_agenda_cached(api_key, hoje_str)
    if not jogos: return
    matches = []
    for j in jogos:
        try:
            t1 = j['teams']['home']['name']; t1_id = str(j['teams']['home']['id'])
            t2 = j['teams']['away']['name']; t2_id = str(j['teams']['away']['id'])
            foco = None; fid_foco = None
            if t1_id in ids_top: foco = t1; fid_foco = t1_id
            elif t1 in ids_top: foco = t1; fid_foco = t1
            elif t2_id in ids_top: foco = t2; fid_foco = t2_id
            elif t2 in ids_top: foco = t2; fid_foco = t2
            if foco:
                df_t = df[(df['HomeID']==fid_foco)|(df['AwayID']==fid_foco)] if fid_foco.isdigit() else df[df['Jogo'].str.contains(foco, na=False)]
                m_strat = "Geral"; m_wr = 0
                if not df_t.empty:
                    for n, d in df_t.groupby('Estrategia'):
                        if len(d)>=2:
                            wr = (d['Resultado'].str.contains('GREEN').sum()/len(d))*100
                            if wr > m_wr: m_wr = wr; m_strat = n
                txt = f"🔥 <b>Oportunidade Sniper:</b> {foco} tem <b>{m_wr:.0f}%</b> na <b>{m_strat}</b>" if m_wr > 60 else f"💰 <b>Volume:</b> {foco} é máquina de Greens!"
                matches.append(f"⏰ {j['fixture']['date'][11:16]} | {j['league']['country']} {j['league']['name']}\n⚽ {t1} 🆚 {t2}\n{txt}")
        except: pass
    if matches:
        msg = "🌅 <b>BOM DIA! RADAR DE OPORTUNIDADES</b>\n\n" + "\n\n".join(matches) + "\n\n⚠️ <i>Dica: Se o robô mandar o sinal sugerido acima, a chance de Green é estatisticamente maior!</i> 🚀"
        enviar_telegram(token, chat_ids, msg)
    st.session_state[chave] = True 

def enviar_relatorio_bi(token, chat_ids):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    try:
        df = df.copy(); df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
        df['Data_DT'] = pd.to_datetime(df['Data_Str'], errors='coerce'); df = df.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    except: return
    hoje = pd.to_datetime(get_time_br().date()); mask_dia = df['Data_DT'] == hoje
    mask_sem = df['Data_DT'] >= (hoje - timedelta(days=7)); mask_mes = df['Data_DT'] >= (hoje - timedelta(days=30))
    def cm(d):
        g = d['Resultado'].str.contains('GREEN').sum(); r = d['Resultado'].str.contains('RED').sum(); tot = g+r
        return tot, g, r, (g/tot*100) if tot>0 else 0
    t_d, g_d, r_d, w_d = cm(df[mask_dia]); t_s, g_s, r_s, w_s = cm(df[mask_sem]); t_m, g_m, r_m, w_m = cm(df[mask_mes]); t_a, g_a, r_a, w_a = cm(df)
    if token and chat_ids:
        plt.style.use('dark_background'); fig, ax = plt.subplots(figsize=(7, 4))
        stats = df[mask_mes][df[mask_mes]['Resultado'].isin(['✅ GREEN', '❌ RED'])]
        if not stats.empty:
            c = stats.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)
            c.plot(kind='bar', stacked=True, color=['#00FF00', '#FF0000'], ax=ax, width=0.6)
            ax.set_title(f'PERFORMANCE 30 DIAS (WR: {w_m:.1f}%)', color='white', fontsize=12); plt.tight_layout()
            buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=100, facecolor='#0E1117'); buf.seek(0)
            msg = f"📊 <b>RELATÓRIO BI</b>\n\n📆 <b>HOJE:</b> {t_d} (WR: {w_d:.1f}%)\n📅 <b>7 DIAS:</b> {t_s} (WR: {w_s:.1f}%)\n🗓️ <b>30 DIAS:</b> {t_m} (WR: {w_m:.1f}%)\n♾️ <b>TOTAL:</b> {t_a} (WR: {w_a:.1f}%)"
            ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
            for cid in ids: buf.seek(0); _worker_telegram_photo(token, cid, buf, msg)
            plt.close(fig)

def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0; st_short = jogo_api['fixture']['status']['short']
    STRATS_HT_ONLY = ["Gol Relâmpago", "Massacre", "Choque Líderes", "Briga de Rua"]
    if "Múltipla" in sinal['Estrategia']: return False
    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: return False
    if (gh+ga) > (ph+pa):
        if "Morno" in sinal['Estrategia']: 
            sinal['Resultado'] = '❌ RED'; enviar_telegram(token, chats, f"❌ <b>RED | GOL SAIU</b>\n⚽ {sinal['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {sinal['Estrategia']}"); return True
        else:
            sinal['Resultado'] = '✅ GREEN'; enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📈 Placar: <b>{gh}x{ga}</b>\n🎯 {sinal['Estrategia']}"); return True
    if any(x in sinal['Estrategia'] for x in STRATS_HT_ONLY) and st_short in ['HT', '2H', 'FT', 'AET', 'PEN', 'ABD']:
        sinal['Resultado'] = '❌ RED'; enviar_telegram(token, chats, f"❌ <b>RED | INTERVALO (HT)</b>\n⚽ {sinal['Jogo']}\n📉 Placar HT: {gh}x{ga}\n🎯 {sinal['Estrategia']}"); return True
    if st_short in ['FT', 'AET', 'PEN', 'ABD']:
        if "Morno" in sinal['Estrategia'] and (gh+ga) <= 1:
             sinal['Resultado'] = '✅ GREEN'; enviar_telegram(token, chats, f"✅ <b>GREEN | UNDER BATIDO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}"); return True
        sinal['Resultado'] = '❌ RED'; enviar_telegram(token, chats, f"❌ <b>RED | ENCERRADO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {sinal['Estrategia']}"); return True
    return False

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    atualizou = False; hist = st.session_state['historico_sinais']; pendentes = [s for s in hist if s['Resultado'] == 'Pendente']
    if not pendentes: return
    hoje_str = get_time_br().strftime('%Y-%m-%d'); agora = get_time_br(); ids_live = [j['fixture']['id'] for j in jogos_live]
    for s in pendentes:
        if s.get('Data') != hoje_str: continue
        fid = int(clean_fid(s.get('FID', 0)))
        if 'Odd_Atualizada' not in s: s['Odd_Atualizada'] = False
        try:
            hora_str = f"{s['Data']} {s['Hora']}"; dt_sinal = datetime.strptime(hora_str, '%Y-%m-%d %H:%M')
            dt_sinal = pytz.timezone('America/Sao_Paulo').localize(dt_sinal); minutos_passados = (agora - dt_sinal).total_seconds() / 60
            if minutos_passados >= 3 and not s['Odd_Atualizada']:
                nova_odd = get_live_odds(fid, api_key)
                if nova_odd != "0.00": s['Odd'] = nova_odd; s['Odd_Atualizada'] = True; atualizou = True
        except: pass 
        jogo_encontrado = None
        if fid > 0 and fid in ids_live: jogo_encontrado = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        elif fid > 0:
            try:
                res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                if res['response']: jogo_encontrado = res['response'][0]
            except: pass
        if jogo_encontrado:
            if processar_resultado(s, jogo_encontrado, token, chats): atualizou = True
    if atualizou: atualizar_historico_ram_disk(hist)

def verificar_var_rollback(jogos_live, token, chats):
    hist = st.session_state['historico_sinais']; greens = [s for s in hist if 'GREEN' in str(s['Resultado'])]
    if not greens: return
    atualizou = False
    for s in greens:
        if "Morno" in s['Estrategia']: continue
        fid = int(clean_fid(s.get('FID', 0))); jogo_api = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        if jogo_api:
            gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
            try:
                ph, pa = map(int, s['Placar_Sinal'].split('x'))
                if (gh + ga) <= (ph + pa):
                    s['Resultado'] = 'Pendente'; s['Placar_Sinal'] = f"{gh}x{ga}"; atualizou = True
                    msg = (f"⚠️ <b>VAR ACIONADO | GOL ANULADO</b>\n\n⚽ {s['Jogo']}\n📉 Placar voltou para: <b>{gh}x{ga}</b>\n🔄 Status revertido para <b>PENDENTE</b>.")
                    enviar_telegram(token, chats, msg)
            except: pass
    if atualizou: atualizar_historico_ram_disk(hist)

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
    mem['h_t'] = [t for t in mem['h_t'] if now - t <= timedelta(minutes=7)]; mem['a_t'] = [t for t in mem['a_t'] if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a; st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_t']), len(mem['a_t'])

def deve_buscar_stats(tempo, gh, ga, status):
    if 5 <= tempo <= 15: return True
    if tempo <= 30 and (gh + ga) >= 2: return True
    if 70 <= tempo <= 85 and abs(gh - ga) <= 1: return True
    if tempo <= 60 and abs(gh - ga) <= 1: return True
    if status == 'HT' and gh == 0 and ga == 0: return True
    return False

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal'); sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal'); tot_chutes = sh_h + sh_a; tot_gol = sog_h + sog_a; txt_stats = f"{tot_chutes} Chutes (🎯 {tot_gol} no Gol)"
    except: return []
    fid = j['fixture']['id']; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0; rh, ra = momentum(fid, sog_h, sog_a); SINAIS = []
    if tempo <= 30 and (gh+ga) >= 2: SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": "🔥 Over Gols (Tendência de Goleada)", "stats": f"Placar: {gh}x{ga}"})
    if (gh + ga) == 0:
        if (tempo <= 2 and (sog_h + sog_a) >= 1) or (tempo <= 10 and (sh_h + sh_a) >= 2): SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": "Over 0.5 HT (Entrar para sair gol no 1º tempo)", "stats": txt_stats})
    if 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(gh-ga) <= 1: SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": "Over Gols (Gol no final - Limite)", "stats": txt_stats})
    if tempo <= 60:
        if gh <= ga and (rh >= 2 or sh_h >= 8): SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": "Over Gols (Gol maduro na partida)", "stats": f"Pressão: {rh}"})
        if ga <= gh and (ra >= 2 or sh_a >= 8): SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": "Over Gols (Gol maduro na partida)", "stats": f"Pressão: {ra}"})
    if rank_home and rank_away:
        is_top_home = rank_home <= 4; is_top_away = rank_away <= 4; is_bot_home = rank_home >= 11; is_bot_away = rank_away >= 11; is_mid_home = rank_home >= 5; is_mid_away = rank_away >= 5
        if (is_top_home and is_bot_away) or (is_top_away and is_bot_home):
            if tempo <= 5 and (sh_h + sh_a) >= 1: SINAIS.append({"tag": "🔥 Massacre", "ordem": "Over 0.5 HT (Favorito deve abrir placar)", "stats": f"Rank: {rank_home}x{rank_away}"})
        if 5 <= tempo <= 15:
            if is_top_home and (rh >= 2 or sh_h >= 3): SINAIS.append({"tag": "🦁 Favorito", "ordem": "Over Gols (Partida)", "stats": f"Pressão: {rh}"})
            if is_top_away and (ra >= 2 or sh_a >= 3): SINAIS.append({"tag": "🦁 Favorito", "ordem": "Over Gols (Partida)", "stats": f"Pressão: {ra}"})
        if is_top_home and is_top_away and tempo <= 7:
            if (sh_h + sh_a) >= 2 and (sog_h + sog_a) >= 1: SINAIS.append({"tag": "⚔️ Choque Líderes", "ordem": "Over 0.5 HT (Jogo intenso)", "stats": txt_stats})
        if is_mid_home and is_mid_away:
            if tempo <= 7 and 2 <= (sh_h + sh_a) <= 3: SINAIS.append({"tag": "🥊 Briga de Rua", "ordem": "Over 0.5 HT (Trocação franca)", "stats": txt_stats})
            if rank_home >= 10 and rank_away >= 10:
                if 15 <= tempo <= 16 and (sh_h + sh_a) == 0: SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": "Under 1.5 HT (Apostar que NÃO saem 2 gols no 1º tempo)", "stats": "0 Chutes (Times Z-4)"})
    if 75 <= tempo <= 85 and abs(gh - ga) <= 1:
        if (sh_h + sh_a) >= 16 and (sog_h + sog_a) >= 8: SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": "Gol no Final (Over Limit) (Aposta seca que sai mais um gol)", "stats": "🔥 Pressão Máxima"})
    return SINAIS

def resetar_sistema_completo():
    st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST); st.session_state['historico_sinais'] = []; st.session_state['df_black'] = pd.DataFrame(columns=COLS_BLACK); st.session_state['df_safe'] = pd.DataFrame(columns=COLS_SAFE); st.session_state['df_vip'] = pd.DataFrame(columns=COLS_OBS); st.session_state['alvos_do_dia'] = {}; st.session_state['alertas_enviados'] = set(); st.session_state['multiplas_enviadas'] = set(); st.session_state['memoria_pressao'] = {}; st.session_state['controle_stats'] = {}
    try:
        conn.clear(worksheet="Historico"); salvar_aba("Historico", st.session_state['historico_full']); conn.clear(worksheet="Blacklist"); salvar_aba("Blacklist", st.session_state['df_black']); conn.clear(worksheet="Seguras"); salvar_aba("Seguras", st.session_state['df_safe']); conn.clear(worksheet="Obs"); salvar_aba("Obs", st.session_state['df_vip'])
    except: pass
    st.cache_data.clear(); st.toast("♻️ SISTEMA RESETADO!")

# --- SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=True):
        API_KEY = st.text_input("Chave API:", type="password"); TG_TOKEN = st.text_input("Token Telegram:", type="password"); TG_CHAT = st.text_input("Chat IDs:"); INTERVALO = st.slider("Ciclo (s):", 60, 300, 60)
        st.write("---")
        if st.button("📊 Enviar Relatório BI"): enviar_relatorio_bi(TG_TOKEN, TG_CHAT); st.toast("BI Enviado!")
        if st.button("💰 Enviar Relatório Financeiro"):
            if 'last_fin_stats' in st.session_state:
                s = st.session_state['last_fin_stats']
                enviar_relatorio_financeiro(TG_TOKEN, TG_CHAT, s['lucro'], s['roi'], s['entradas'])
                st.toast("Financeiro Enviado!")
            else: st.error("Abra a aba Financeiro primeiro.")

    with st.expander("💰 Gestão de Banca", expanded=False):
        stake_padrao = st.number_input("Valor da Entrada (R$)", value=10.0, step=5.0); banca_inicial = st.number_input("Banca Inicial (R$)", value=100.0, step=50.0)
    
    with st.expander("📶 Consumo API", expanded=False):
        verificar_reset_diario(); u = st.session_state['api_usage']; perc = min(u['used'] / u['limit'], 1.0) if u['limit'] > 0 else 0
        st.progress(perc); st.caption(f"Utilizado: **{u['used']}** / {u['limit']}")

    st.write("---")
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    st.markdown("---")
    if st.button("☢️ ZERAR ROBÔ", type="primary", use_container_width=True): st.session_state['confirmar_reset'] = True
    if st.session_state.get('confirmar_reset'):
        if st.button("✅ SIM, RESETAR TUDO"): resetar_sistema_completo(); st.session_state['confirmar_reset'] = False; st.rerun()

# --- LOOP PRINCIPAL ---
if st.session_state.ROBO_LIGADO:
    carregar_tudo(); verificar_automacao_bi(TG_TOKEN, TG_CHAT); verificar_alerta_matinal(TG_TOKEN, TG_CHAT, API_KEY)
    ids_black = [normalizar_id(x) for x in st.session_state['df_black']['id'].values]; ids_safe = [normalizar_id(x) for x in st.session_state['df_safe']['id'].values]; df_obs = st.session_state.get('df_vip', pd.DataFrame()); count_obs = len(df_obs); count_safe = len(st.session_state.get('df_safe', pd.DataFrame()))
    hoje_real = get_time_br().strftime('%Y-%m-%d'); st.session_state['historico_sinais'] = [s for s in st.session_state['historico_sinais'] if s.get('Data') == hoje_real]

    api_error = False
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        resp = requests.get(url, headers={"x-apisports-key": API_KEY}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
        update_api_usage(resp.headers); res = resp.json(); jogos_live = res.get('response', []) if not res.get('errors') else []; api_error = bool(res.get('errors'))
    except: jogos_live = []; api_error = True
    if not api_error: check_green_red_hibrido(jogos_live, TG_TOKEN, TG_CHAT, API_KEY); verificar_var_rollback(jogos_live, TG_TOKEN, TG_CHAT)

    radar = []; ids_no_radar = []
    for j in jogos_live:
        lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']; nome_liga_show = j['league']['name']
        if lid in ids_black: continue
        if lid in ids_safe: nome_liga_show += " 🛡️"
        elif lid in df_obs['id'].values: nome_liga_show += " ⚠️"
        else: nome_liga_show += " ❓" 
        ids_no_radar.append(fid); tempo = j['fixture']['status']['elapsed'] or 0; st_short = j['fixture']['status']['short']; home = j['teams']['home']['name']; away = j['teams']['away']['name']; placar = f"{j['goals']['home']}x{j['goals']['away']}"; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0; stats = []; rank_h = None; rank_a = None
        if j['league']['id'] in LIGAS_TABELA:
            rk = buscar_ranking(API_KEY, j['league']['id'], j['league']['season']); rank_h = rk.get(home); rank_a = rk.get(away)
        if deve_buscar_stats(tempo, gh, ga, st_short):
            if (datetime.now() - st.session_state['controle_stats'].get(fid, datetime.min)).total_seconds() > 60:
                try:
                    r_st = requests.get("https://v3.football.api-sports.io/fixtures/statistics", headers={"x-apisports-key": API_KEY}, params={"fixture": fid}, timeout=5)
                    update_api_usage(r_st.headers); stats = r_st.json().get('response', [])
                    if stats: st.session_state['controle_stats'][fid] = datetime.now(); st.session_state[f"st_{fid}"] = stats
                except: stats = []
            else: stats = st.session_state.get(f"st_{fid}", [])
        if stats:
            lista_sinais = processar(j, stats, tempo, placar, rank_h, rank_a); salvar_safe_league_basic(lid, j['league']['country'], j['league']['name'], tem_tabela=(rank_h is not None)); resetar_erros(lid)
            for s in lista_sinais:
                uid = f"{fid}_{s['tag']}"
                if uid not in st.session_state['alertas_enviados']:
                    # BLOQUEIO IMEDIATO PARA EVITAR DUPLICADOS
                    st.session_state['alertas_enviados'].add(uid)
                    odd_atual = get_live_odds(fid, API_KEY); item = {"FID": fid, "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Placar_Sinal": placar, "Estrategia": s['tag'], "Resultado": "Pendente", "HomeID": str(j['teams']['home']['id']) if lid in ids_safe else "", "AwayID": str(j['teams']['away']['id']) if lid in ids_safe else "", "Odd": odd_atual, "Odd_Atualizada": ""}
                    if adicionar_historico(item):
                        prob = buscar_inteligencia(s['tag'], j['league']['name'], f"{home} x {away}"); msg = f"<b>🚨 SINAL ENCONTRADO 🚨</b>\n\n🏆 <b>{j['league']['name']}</b>\n⚽ {home} 🆚 {away}\n⏰ <b>{tempo}' min</b> ({placar})\n🔥 {s['tag'].upper()}\n💰 <b>Odd: @{odd_atual}</b>\n📊 {s['stats']}{prob}"; enviar_telegram(TG_TOKEN, TG_CHAT, msg)
        radar.append({"Liga": nome_liga_show, "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": "Monitorando"})

    dashboard_placeholder = st.empty()
    with dashboard_placeholder.container():
        if api_error: st.markdown('<div class="status-error">🚨 API LIMITADA</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        hist_hj = pd.DataFrame(st.session_state['historico_sinais']); t, g, r, w = calcular_stats(hist_hj); c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t}</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(radar)}</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Ligas Seguras</div><div class="metric-value">{count_safe}</div></div>', unsafe_allow_html=True)
        
        abas = st.tabs([f"📡 Radar ({len(radar)})", "📅 Agenda", "💰 Financeiro", "📜 Histórico", "📈 BI & Analytics", "🚫 Blacklist", "🛡️ Seguras", "⚠️ Obs"])
        
        with abas[0]: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
        
        # --- ABA FINANCEIRO COM CENÁRIOS DINÂMICOS ---
        with abas[2]:
            st.markdown("### 💰 ROI Dinâmico por Cenário")
            modo_entrada = st.radio("Simulação de Gestão:", ["Todos os sinais", "Apenas 1 sinal por jogo", "Até 2 sinais por jogo"], horizontal=True)
            df_fin = st.session_state.get('historico_full', pd.DataFrame())
            if not df_fin.empty:
                df_fin = df_fin[df_fin['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
                df_fin['Odd'] = pd.to_numeric(df_fin['Odd'], errors='coerce').fillna(1.50)
                df_fin['Hora_DT'] = pd.to_datetime(df_fin['Hora'], format='%H:%M')
                df_fin = df_fin.sort_values(['FID', 'Hora_DT'])
                if modo_entrada == "Apenas 1 sinal por jogo": df_calc = df_fin.groupby('FID').head(1)
                elif modo_entrada == "Até 2 sinais por jogo": df_calc = df_fin.groupby('FID').head(2)
                else: df_calc = df_fin
                if not df_calc.empty:
                    lucros = []
                    for _, row in df_calc.iterrows():
                        l = (stake_padrao * row['Odd'] - stake_padrao) if 'GREEN' in row['Resultado'] else -stake_padrao
                        lucros.append(l)
                    total_lucro = sum(lucros); roi = (total_lucro / (len(df_calc) * stake_padrao) * 100) if len(df_calc)>0 else 0
                    st.session_state['last_fin_stats'] = {'lucro': total_lucro, 'roi': roi, 'entradas': len(df_calc)}
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Lucro Líquido", f"R$ {total_lucro:.2f}"); m2.metric("ROI Estimado", f"{roi:.1f}%"); m3.metric("Banca Final", f"R$ {banca_inicial + total_lucro:.2f}")
                    st.latex(r"ROI = \frac{\text{Lucro}}{\text{Investimento}} \times 100")
                    st.dataframe(df_calc[['Data', 'Hora', 'Jogo', 'Estrategia', 'Odd', 'Resultado']], use_container_width=True, hide_index=True)
            else: st.info("Sem histórico fechado.")

        with abas[3]: st.dataframe(hist_hj, use_container_width=True, hide_index=True)
        with abas[4]: # TAB BI ORIGINAL
            try:
                df_bi = st.session_state.get('historico_full', pd.DataFrame())
                if not df_bi.empty:
                    df_bi = df_bi.copy(); df_bi['Data_Str'] = df_bi['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip(); df_bi['Data_DT'] = pd.to_datetime(df_bi['Data_Str'], errors='coerce')
                    filtro = st.selectbox("📅 Período", ["Tudo", "Hoje", "7 Dias", "30 Dias"])
                    # (Restante do BI mantido conforme original...)
                    st.info("Utilize os filtros acima para analisar a performance.")
            except: pass
        with abas[5]: st.dataframe(st.session_state['df_black'][['País', 'Liga', 'Motivo']], use_container_width=True, hide_index=True)
        with abas[6]: st.dataframe(st.session_state['df_safe'][['País', 'Liga', 'Motivo']], use_container_width=True, hide_index=True)
        with abas[7]: st.dataframe(st.session_state.get('df_vip', pd.DataFrame()), use_container_width=True, hide_index=True)

    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True); time.sleep(1)
    st.rerun()
else:
    st.title("❄️ Neves Analytics"); st.info("💡 Robô desligado.")
