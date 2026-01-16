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
        s_val = str(val).strip(); return str(int(float(s_val))) if s_val and s_val.lower() != 'nan' else ""
    except: return str(val).strip()

def formatar_inteiro_visual(val):
    try:
        return str(int(float(str(val)))) if str(val) not in ['nan', ''] else "0"
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
                    elif col in ['Jogos_Erro', 'Motivo', 'Odd_Atualizada']: df[col] = ''
                    else: df[col] = ""
        return df.fillna("").astype(str) if not df.empty else pd.DataFrame(columns=colunas_esperadas)
    except: return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df_para_salvar):
    try: conn.update(worksheet=nome_aba, data=df_para_salvar); return True
    except: return False

def carregar_tudo(force=False):
    now = time.time()
    if not force and (now - st.session_state['last_db_update']) < DB_CACHE_TIME:
        if 'df_black' in st.session_state: return

    st.session_state['df_black'] = carregar_aba("Blacklist", COLS_BLACK)
    st.session_state['df_safe'] = carregar_aba("Seguras", COLS_SAFE)
    st.session_state['df_vip'] = carregar_aba("Obs", COLS_OBS)
    
    df_h = carregar_aba("Historico", COLS_HIST)
    if not df_h.empty and 'Data' in df_h.columns:
        df_h['FID'] = df_h['FID'].apply(clean_fid)
        st.session_state['historico_full'] = df_h
        hoje = get_time_br().strftime('%Y-%m-%d')
        st.session_state['historico_sinais'] = df_h[df_h['Data'] == hoje].to_dict('records')[::-1]
        for _, row in df_h[df_h['Data'] == hoje].iterrows():
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
    df_hoje = pd.DataFrame(lista_atualizada); df_disk = st.session_state['historico_full']; hoje = get_time_br().strftime('%Y-%m-%d')
    if not df_disk.empty and 'Data' in df_disk.columns:
         df_disk['Data'] = df_disk['Data'].astype(str).str.replace(' 00:00:00', '', regex=False)
         df_disk = df_disk[df_disk['Data'] != hoje]
    df_final = pd.concat([df_hoje, df_disk], ignore_index=True).drop_duplicates(subset=['FID', 'Estrategia'], keep='first')
    st.session_state['historico_full'] = df_final; salvar_aba("Historico", df_final)

# --- UTILITÁRIOS API ---
def update_api_usage(headers):
    if not headers: return
    try:
        limit = int(headers.get('x-ratelimit-requests-limit', 75000)); used = limit - int(headers.get('x-ratelimit-requests-remaining', 0))
        st.session_state['api_usage'] = {'used': used, 'limit': limit}
    except: pass

def get_live_odds(fixture_id, api_key):
    try:
        url = "https://v3.football.api-sports.io/odds/live"; res = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fixture_id}).json()
        if res.get('response'):
            markets = res['response'][0]['odds']
            for m in markets:
                if "goals" in m['name'].lower() and "over" in m['name'].lower():
                    for v in m['values']:
                        if "over" in str(v['value']).lower():
                            odd = float(v['odd']); return "{:.2f}".format(odd / 1000 if odd > 50 else odd)
        return "1.50"
    except: return "1.50"

# --- LÓGICA DE SINAL ---
def processar(j, stats, tempo, placar):
    fid = j['fixture']['id']; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0; SINAIS = []
    if 75 <= tempo <= 85 and abs(gh - ga) <= 1:
        SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": "Gol no Final (Over Limit)", "stats": "Pressão Final"})
    return SINAIS

# --- SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=True):
        API_KEY = st.text_input("Chave API:", type="password"); TG_TOKEN = st.text_input("Token Telegram:", type="password"); TG_CHAT = st.text_input("Chat IDs:"); INTERVALO = st.slider("Ciclo (s):", 60, 300, 60)
        if st.button("📊 Enviar Relatório BI"): enviar_relatorio_bi(TG_TOKEN, TG_CHAT); st.toast("Enviado!")

    with st.expander("💰 Gestão de Banca", expanded=False):
        stake_padrao = st.number_input("Entrada (R$)", value=10.0); banca_inicial = st.number_input("Banca (R$)", value=100.0)
    
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)

# --- DASHBOARD ---
if st.session_state.ROBO_LIGADO:
    carregar_tudo()
    # (Simulação de Loop de API omitida para brevidade, mantendo foco na nova aba financeira solicitada)
    
    st.markdown(f'<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
    abas = st.tabs(["📡 Radar", "📅 Agenda", "💰 Financeiro", "📜 Histórico"])
    
    with abas[2]:
        st.markdown("### 💰 ROI Dinâmico por Estratégia de Entrada")
        
        # O SELETOR DE CENÁRIOS
        modo_entrada = st.radio(
            "Selecione seu modelo de entrada:",
            ["Entrar em TODOS os sinais", "Entrar em apenas 1 sinal por jogo", "Entrar em até 2 sinais por jogo"],
            horizontal=True
        )
        
        df_fin = st.session_state.get('historico_full', pd.DataFrame())
        if not df_fin.empty:
            df_fin = df_fin[df_fin['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
            df_fin['Odd'] = pd.to_numeric(df_fin['Odd'], errors='coerce').fillna(1.50)
            df_fin['Hora_DT'] = pd.to_datetime(df_fin['Hora'], format='%H:%M')
            
            # Agrupar por Jogo (FID) e ordenar por tempo
            df_fin = df_fin.sort_values(['FID', 'Hora_DT'])
            
            # Aplicar filtro baseado no modo
            if modo_entrada == "Entrar em apenas 1 sinal por jogo":
                df_calculo = df_fin.groupby('FID').head(1)
            elif modo_entrada == "Entrar em até 2 sinais por jogo":
                df_calculo = df_fin.groupby('FID').head(2)
            else:
                df_calculo = df_fin
            
            # Cálculos de ROI
            lucros = []
            investimento_total = len(df_calculo) * stake_padrao
            for _, row in df_calculo.iterrows():
                l = (stake_padrao * row['Odd']) - stake_padrao if 'GREEN' in row['Resultado'] else -stake_padrao
                lucros.append(l)
            
            lucro_total = sum(lucros)
            roi = (lucro_total / investimento_total * 100) if investimento_total > 0 else 0
            
            # Cards de Resultado
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Sinais Considerados", len(df_calculo))
            c2.metric("Lucro Líquido", f"R$ {lucro_total:.2f}")
            c3.metric("ROI", f"{roi:.1f}%")
            c4.metric("Banca Final", f"R$ {banca_inicial + lucro_total:.2f}")
            
            # Fórmula em LaTeX para documentação
            st.latex(r"ROI = \left( \frac{\text{Lucro Total}}{\text{Investimento Total}} \right) \times 100")
            
            st.divider()
            st.caption(f"Exibindo entradas filtradas para o cenário: **{modo_entrada}**")
            st.dataframe(df_calculo[['Data', 'Hora', 'Jogo', 'Estrategia', 'Odd', 'Resultado']], use_container_width=True, hide_index=True)
            
        else:
            st.info("Aguardando sinais finalizados para gerar relatório.")

    # Timer e Rerun
    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True); time.sleep(1)
    st.rerun()
