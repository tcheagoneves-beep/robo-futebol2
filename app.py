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
# 1. CONFIGURAÇÃO INICIAL, CSS E VARIÁVEIS
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

# --- INICIALIZAÇÃO DE SESSÃO ---
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

# --- CONFIGURAÇÃO FIREBASE E GEMINI ---
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

# --- DEFINIÇÃO DE COLUNAS E LISTAS ---
COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID', 'Odd', 'Odd_Atualizada', 'Opiniao_IA']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo', 'Strikes', 'Jogos_Erro']
COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes', 'Jogos_Erro']
COLS_BLACK = ['id', 'País', 'Liga', 'Motivo']
LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]
DB_CACHE_TIME = 60
STATIC_CACHE_TIME = 600

# --- MAPA DE ESTRATÉGIAS ATUALIZADO ---
MAPA_LOGICA_ESTRATEGIAS = {
    "🟣 Porteira Aberta": "Over Gols",
    "⚡ Gol Relâmpago": "Over HT",
    "💰 Janela de Ouro": "Over Limite",
    "🟢 Blitz Casa": "Over Gols",
    "🟢 Blitz Visitante": "Over Gols",
    "🔥 Massacre": "Over HT",
    "⚔️ Choque Líderes": "Over HT",
    "🥊 Briga de Rua": "Over HT",
    "❄️ Jogo Morno": "Under HT",
    "💎 GOLDEN BET": "Over Limite",
    "🏹 Tiroteio Elite": "Over Gols",
    "⚡ Contra-Ataque Letal": "Back Zebra",
    "🚩 Pressão Escanteios": "Cantos Asiáticos",
    "💎 Sniper Final": "Over Limite",
    "🦁 Back Favorito (Nettuno)": "Back Vencedor",
    "💀 Lay ao Morto": "Lay Perdedor",
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
    "🚩 Pressão Escanteios": {"min": 1.50, "max": 1.80},
    "💎 Sniper Final": {"min": 1.80, "max": 2.50},
    "💀 Lay ao Morto": {"min": 1.60, "max": 2.00},
    "🔫 Lay Goleada": {"min": 1.60, "max": 2.20},
    "👴 Estratégia do Vovô": {"min": 1.05, "max": 1.25}
}

# --- FUNÇÕES HELPERS BÁSICAS ---
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
# ==============================================================================
# 2. GERENCIAMENTO DE DADOS E ESTÁTISTICAS
# ==============================================================================

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

def adicionar_historico(item):
    if 'historico_full' not in st.session_state: st.session_state['historico_full'] = carregar_aba("Historico", COLS_HIST)
    df_memoria = st.session_state['historico_full']
    df_novo = pd.DataFrame([item])
    df_final = pd.concat([df_novo, df_memoria], ignore_index=True)
    st.session_state['historico_full'] = df_final
    st.session_state['historico_sinais'].insert(0, item)
    st.session_state['precisa_salvar'] = True 
    return True

def salvar_bigdata(jogo_api, stats):
    if not db_firestore: return
    try:
        fid = str(jogo_api['fixture']['id'])
        if fid in st.session_state['jogos_salvos_bigdata']: return 

        s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        def sanitize(val): return str(val) if val is not None else "0"
        
        rate_h = 0; rate_a = 0
        # (Lógica simplificada de rating para economizar espaço aqui)
        
        item_bigdata = {
            'fid': fid,
            'data_hora': get_time_br().strftime('%Y-%m-%d %H:%M'),
            'liga': sanitize(jogo_api['league']['name']),
            'home_id': str(jogo_api['teams']['home']['id']),
            'away_id': str(jogo_api['teams']['away']['id']),
            'jogo': f"{sanitize(jogo_api['teams']['home']['name'])} x {sanitize(jogo_api['teams']['away']['name'])}",
            'placar_final': f"{jogo_api['goals']['home']}x{jogo_api['goals']['away']}",
            'estatisticas': {
                'chutes_total': gv(s1, 'Total Shots') + gv(s2, 'Total Shots'),
                'chutes_gol': gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal'),
                'escanteios_total': gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks'),
                'posse_casa': str(gv(s1, 'Ball Possession')),
                'ataques_perigosos': gv(s1, 'Dangerous Attacks') + gv(s2, 'Dangerous Attacks')
            }
        }
        db_firestore.collection("BigData_Futebol").document(fid).set(item_bigdata)
        st.session_state['jogos_salvos_bigdata'].add(fid)
    except: pass

@st.cache_data(ttl=86400)
def analisar_tendencia_50_jogos(api_key, home_id, away_id):
    try:
        def get_stats_50(team_id):
            url = "https://v3.football.api-sports.io/fixtures"
            params = {"team": team_id, "last": "50", "status": "FT"}
            res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
            jogos = res.get('response', [])
            if not jogos: return {"over05_ht": 0, "over15_ft": 0}
            stats = {"qtd": len(jogos), "over05_ht": 0, "over15_ft": 0}
            for j in jogos:
                gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
                g_ht = (j['score']['halftime']['home'] or 0) + (j['score']['halftime']['away'] or 0)
                if g_ht > 0: stats["over05_ht"] += 1
                if (gh + ga) >= 2: stats["over15_ft"] += 1
            return {k: int((v / stats["qtd"]) * 100) if k != "qtd" else v for k, v in stats.items()}
        return {"home": get_stats_50(home_id), "away": get_stats_50(away_id)}
    except: return None

# ==============================================================================
# 3. LÓGICA DE ESTRATÉGIAS (O CÉREBRO)
# ==============================================================================

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
        
        # --- DADOS TÉCNICOS ---
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        ck_h = get_v(stats_h, 'Corner Kicks'); ck_a = get_v(stats_a, 'Corner Kicks')
        chutes_area_h = get_v(stats_h, 'Shots insidebox'); chutes_area_a = get_v(stats_a, 'Shots insidebox')
        
        rh, ra = momentum(j['fixture']['id'], sog_h, sog_a)
        
        gh = j['goals']['home']; ga = j['goals']['away']
        total_gols = gh + ga; total_chutes = sh_h + sh_a; total_sog = sog_h + sog_a
        diff_gols = gh - ga 

        try:
            posse_h_val = next((x['value'] for x in stats_h if x['type']=='Ball Possession'), "50%")
            posse_h = int(str(posse_h_val).replace('%', ''))
            posse_a = 100 - posse_h
        except: posse_h = 50; posse_a = 50
        
        # --- FUNÇÃO HELPER: ORDEM MASTIGADA PARA LEIGOS ---
        def gerar_ordem_gol(gols_atuais, tipo="Over"):
            linha = gols_atuais + 0.5
            if tipo == "Over": 
                return f"👉 <b>ENTRADA:</b> Mercado de Gols\n✅ Apostar em <b>Mais de {linha} Gols</b>"
            elif tipo == "HT": 
                return f"👉 <b>ENTRADA:</b> 1º Tempo (HT)\n✅ Apostar em <b>Mais de 0.5 Gols HT</b>"
            elif tipo == "Limite": 
                linha_limite = gols_atuais + 1.0
                return f"👉 <b>ENTRADA:</b> Gol Limite (Asiático)\n✅ Apostar em <b>Mais de {linha_limite} Gols</b>\n(⚠️ Se sair 1 gol, o dinheiro é devolvido)"
            return "Apostar em Gols."

        SINAIS = []

        # ==============================================================================
        # ESTRATÉGIA 1: VOVÔ (Segurança / Back Favorito)
        # ==============================================================================
        if 70 <= tempo <= 82:
            vovo_ativado = False; ordem_vovo = ""
            # Casa Vencendo por 2 gols (2x0, 3x1) e segura
            if diff_gols == 2:
                if (posse_h >= 45) and (ra < 2): 
                    vovo_ativado = True
                    ordem_vovo = "👉 <b>ENTRADA:</b> Manutenção de Resultado\n✅ <b>Back/A favor do CASA</b> (Ou Lay Visitante)"
            # Visitante Vencendo por 2 gols
            elif diff_gols == -2:
                if (posse_a >= 45) and (rh < 2):
                    vovo_ativado = True
                    ordem_vovo = "👉 <b>ENTRADA:</b> Manutenção de Resultado\n✅ <b>Back/A favor do VISITANTE</b> (Ou Lay Casa)"

            if vovo_ativado:
                SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": ordem_vovo, "stats": f"Placar Seguro {gh}x{ga} | Controle Total", "rh": rh, "ra": ra})

        # ==============================================================================
        # ESTRATÉGIA 2: MATCH ODDS & LAY (Leitura de Jogo)
        # ==============================================================================
        # Back Favorito (Nettuno)
        if 10 <= tempo <= 40 and gh == ga:
            if (posse_h >= 55) and (sog_h >= 2) and (sh_h >= 5) and (sh_a <= 1) and (rh >= 1): 
                 SINAIS.append({"tag": "🦁 Back Favorito (Nettuno)", "ordem": "👉 <b>ENTRADA:</b> Vencedor do Jogo (Match Odds)\n✅ <b>Apostar na Vitória do CASA</b>", "stats": f"Dominância: {sh_h} chutes vs {sh_a}", "rh": rh, "ra": ra})
            elif (posse_a >= 55) and (sog_a >= 2) and (sh_a >= 5) and (sh_h <= 1) and (ra >= 1):
                 SINAIS.append({"tag": "🦁 Back Favorito (Nettuno)", "ordem": "👉 <b>ENTRADA:</b> Vencedor do Jogo (Match Odds)\n✅ <b>Apostar na Vitória do VISITANTE</b>", "stats": f"Dominância: {sh_a} chutes vs {sh_h}", "rh": rh, "ra": ra})
        
        # Lay ao Morto (Defensiva)
        if 55 <= tempo <= 80 and (gh == ga or abs(diff_gols) == 1): 
            # Casa morto
            if (rh == 0) and (sog_h <= 2) and (ra >= 1):
                SINAIS.append({"tag": "💀 Lay ao Morto", "ordem": "👉 <b>ENTRADA:</b> Contra o Mandante\n✅ <b>Lay Casa</b> (ou Dupla Chance Visitante)", "stats": f"Mandante Inofensivo ({sog_h} SoG)", "rh": rh, "ra": ra})

        # Lay Goleada
        if 60 <= tempo <= 88 and abs(diff_gols) >= 3:
            if (total_chutes >= 14) and (rh > 0 or ra > 0):
                SINAIS.append({"tag": "🔫 Lay Goleada", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": "Jogo vivo apesar da goleada", "rh": rh, "ra": ra})

        # ==============================================================================
        # ESTRATÉGIA 3: MERCADO DE GOLS (Agressivo)
        # ==============================================================================
        
        # Golden Bet (Pressão pura)
        if 65 <= tempo <= 75:
            pressao_casa = (rh >= 3 and sog_h >= 4); pressao_fora = (ra >= 3 and sog_a >= 4)
            if (pressao_casa and sh_h > sh_a) or (pressao_fora and sh_a > sh_h):
                 if total_gols >= 1 or total_chutes >= 16:
                     SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": "🔥 Pressão Intensa do Favorito", "rh": rh, "ra": ra})

        # Porteira Aberta
        if tempo <= 25 and total_gols >= 2: 
            SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": gerar_ordem_gol(total_gols), "stats": f"{total_gols} Gols em {tempo}min", "rh": rh, "ra": ra})
        
        # Gol Relâmpago
        if total_gols == 0 and tempo <= 12 and total_chutes >= 4: 
             SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": gerar_ordem_gol(0, "HT"), "stats": "Início Frenético", "rh": rh, "ra": ra})
        
        # Janela de Ouro
        if 70 <= tempo <= 75 and abs(diff_gols) <= 1 and total_chutes >= 20: 
             SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"Volume Alto: {total_chutes} chutes", "rh": rh, "ra": ra})
        
        # Blitz
        if tempo <= 60:
            if gh <= ga and (rh >= 2 or sh_h >= 8): SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": gerar_ordem_gol(total_gols), "stats": f"Blitz Mandante ({rh})", "rh": rh, "ra": ra})
            if ga <= gh and (ra >= 2 or sh_a >= 8): SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": gerar_ordem_gol(total_gols), "stats": f"Blitz Visitante ({ra})", "rh": rh, "ra": ra})
        
        # SNIPER FINAL (Refinado com Vídeo 2: Bola Parada)
        if tempo >= 83 and abs(diff_gols) <= 1: 
            tem_bola_parada = (ck_h + ck_a) >= 10
            tem_pressao_final = (rh >= 3) or (ra >= 3)
            
            if (tem_pressao_final or tem_bola_parada) and total_chutes >= 15:
                # Se odd do Over for muito baixa, sugerimos DNB (Empate Anula)
                sugestao = "Over Limite (Asiático)" 
                msg_sniper = f"👉 <b>ENTRADA:</b> Reta Final\n✅ <b>{sugestao}</b>\n(⚠️ Reembolsa se sair 1 gol)"
                SINAIS.append({"tag": "💎 Sniper Final", "ordem": msg_sniper, "stats": f"Pressão Final (Cantos: {ck_h+ck_a})", "rh": rh, "ra": ra})
        
        # Pressão Escanteios
        if tempo >= 35:
            total_cantos = ck_h + ck_a; linha_cantos = total_cantos + 1
            if (ck_h >= 5 and chutes_area_h >= 3 and diff_gols <= 0) or (ck_a >= 5 and chutes_area_a >= 3 and diff_gols >= 0):
                SINAIS.append({"tag": "🚩 Pressão Escanteios", "ordem": f"👉 <b>ENTRADA:</b> Cantos Asiáticos\n✅ <b>Mais de {linha_cantos}.0 Cantos</b>\n(⚠️ Se sair exatamente {linha_cantos}, devolve)", "stats": "Pressão de Bola Parada", "rh": rh, "ra": ra})

        return SINAIS
    except Exception as e: return []
# ==============================================================================
# 4. TELEGRAM E MENSAGERIA
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

# ==============================================================================
# 5. VERIFICAÇÃO DE RESULTADOS (GREEN/RED)
# ==============================================================================

def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
    st_short = jogo_api['fixture']['status']['short']
    fid = sinal['FID']; strat = sinal['Estrategia']
    
    # Tenta ler placar do sinal (Ex: "1x0")
    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: ph, pa = 0, 0
    
    key_green = gerar_chave_universal(fid, strat, "GREEN")
    key_red = gerar_chave_universal(fid, strat, "RED")
    if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
    
    # 1. Lógica para OVER (Gols)
    if (gh + ga) > (ph + pa):
        # Se for Under/Morno, gol a mais é RED
        if "Morno" in strat or "Under" in strat:
            if (gh+ga) >= 2:
                sinal['Resultado'] = '❌ RED'
                if key_red not in st.session_state['alertas_enviados']:
                    enviar_telegram(token, chats, f"❌ <b>RED | {strat}</b>\n⚽ {sinal['Jogo']}\n📉 Saiu gol indesejado ({gh}x{ga})")
                    st.session_state['alertas_enviados'].add(key_red); st.session_state['precisa_salvar'] = True
                return True
        # Se for Over/Back, gol a mais é GREEN
        else:
            sinal['Resultado'] = '✅ GREEN'
            if key_green not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"✅ <b>GREEN NO {strat}!</b>\n⚽ {sinal['Jogo']}\n💰 <b>Lucro no Bolso!</b> Placar: {gh}x{ga}")
                st.session_state['alertas_enviados'].add(key_green); st.session_state['precisa_salvar'] = True
            return True

    # 2. Lógica para STRAT HT (Gol Relâmpago, Massacre) - Só vale se bater no 1º Tempo
    STRATS_HT_ONLY = ["Gol Relâmpago", "Massacre", "Choque", "Briga"]
    eh_ht_strat = any(x in strat for x in STRATS_HT_ONLY)
    
    if eh_ht_strat and st_short in ['HT', '2H', 'FT', 'AET', 'PEN', 'ABD']:
        # Se virou HT e não bateu over, é RED
        if (gh + ga) <= (ph + pa):
            sinal['Resultado'] = '❌ RED'
            if key_red not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"❌ <b>RED | {strat}</b>\n⚽ {sinal['Jogo']}\n📉 Intervalo sem gols.")
                st.session_state['alertas_enviados'].add(key_red); st.session_state['precisa_salvar'] = True
            return True
        
    # 3. Lógica para FINAL DE JOGO (FT)
    if st_short in ['FT', 'AET', 'PEN', 'ABD']:
        # Se for Under/Morno e acabou com poucos gols = GREEN
        if ("Morno" in strat or "Under" in strat) and (gh+ga) <= 1:
             sinal['Resultado'] = '✅ GREEN'
             if key_green not in st.session_state['alertas_enviados']:
                enviar_telegram(token, chats, f"✅ <b>GREEN | LEITURA PERFEITA</b>\n⚽ {sinal['Jogo']}\n🛡️ Jogo Morno Confirmado.")
                st.session_state['alertas_enviados'].add(key_green); st.session_state['precisa_salvar'] = True
             return True
        
        # Se for "Vovô" (Back Favorito) e o time ganhou
        if "Vovô" in strat or "Back" in strat:
            # Verifica quem era o favorito baseado no sinal ou ID
            # Simplificação: Se Back Casa e Casa Ganhou -> Green
            txt_sinal = sinal.get('Placar_Sinal', '').lower() # Usando campo placar ou ordem para guardar info
            # (Aqui a lógica depende de como salvamos, mas vamos assumir padrão Over se não for Back)
            pass 

        # Se chegou FT e não bateu a condição de Over/Back -> RED
        sinal['Resultado'] = '❌ RED'
        if key_red not in st.session_state['alertas_enviados']:
            enviar_telegram(token, chats, f"❌ <b>RED | FINAL DE JOGO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}")
            st.session_state['alertas_enviados'].add(key_red); st.session_state['precisa_salvar'] = True
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
        if "Sniper" in s['Estrategia']: continue # Sniper tem função própria
        
        fid = int(clean_fid(s.get('FID', 0)))
        # Se já tiver sido marcado via botão manual ou outro processo
        key_green = gerar_chave_universal(fid, s['Estrategia'], "GREEN")
        if key_green in st.session_state['alertas_enviados']: s['Resultado'] = '✅ GREEN'; updates_buffer.append(s); continue
        
        jogo_encontrado = mapa_live.get(fid)
        if not jogo_encontrado:
             try:
                 res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                 if res['response']: jogo_encontrado = res['response'][0]
             except: pass
        
        if jogo_encontrado:
            if processar_resultado(s, jogo_encontrado, token, chats): updates_buffer.append(s)
            
    if updates_buffer: atualizar_historico_ram(updates_buffer)

# ==============================================================================
# 6. MÓDULOS DE INTELIGÊNCIA ARTIFICIAL (GEMINI)
# ==============================================================================

def consultar_ia_gemini(dados_jogo, estrategia, stats_raw, rh, ra, extra_context=""):
    if not IA_ATIVADA: return ""
    try:
        dados_ricos = extrair_dados_completos(stats_raw)
        prompt = f"""
        Atue como um ANALISTA DE APOSTAS SÊNIOR (Focado em Valor Esperado).
        
        DADOS DO JOGO:
        {dados_jogo['jogo']} | Placar: {dados_jogo['placar']} | Tempo: {dados_jogo.get('tempo')}
        Estratégia Sugerida pelo Robô: {estrategia}
        
        ESTATÍSTICAS AO VIVO:
        Pressão (Momentum): Casa {rh} x Visitante {ra}
        {dados_ricos}
        {extra_context}

        SUA MISSÃO: Validar se vale a pena colocar dinheiro nessa entrada AGORA.
        
        SAÍDA OBRIGATÓRIA (Sintética):
        [Aprovado/Arriscado/Neutro] - [Motivo curto e direto]
        Ex: "Aprovado - Pressão absurda e zaga do visitante falhando."
        """
        response = model_ia.generate_content(prompt, request_options={"timeout": 10})
        st.session_state['gemini_usage']['used'] += 1
        
        texto = response.text.strip().replace("*", "")
        veredicto = "Neutro"
        if "Aprovado" in texto: veredicto = "Aprovado"
        elif "Arriscado" in texto: veredicto = "Arriscado"
        
        emoji = "✅" if veredicto == "Aprovado" else "⚠️" if veredicto == "Arriscado" else "⚖️"
        return f"\n🤖 <b>MENTORIA IA:</b>\n{emoji} <b>{veredicto.upper()}</b> - <i>{texto.split('-',1)[-1].strip() if '-' in texto else texto}</i>"
    except Exception as e: return "" 

def analisar_bi_com_ia():
    if not IA_ATIVADA: return "IA Offline."
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados."
    try:
        hoje_str = get_time_br().strftime('%Y-%m-%d')
        df_hoje = df[df['Data'].astype(str).str.contains(hoje_str)]
        resumo = df_hoje['Resultado'].value_counts().to_dict()
        prompt = f"Analise o dia de hoje ({hoje_str}). Resultados: {resumo}. Dê 3 dicas para melhorar amanhã com base na volatilidade."
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text
    except: return "Erro ao analisar BI."

def gerar_insights_matinais_ia(api_key):
    if not IA_ATIVADA: return "IA Offline."
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        # Busca jogos Top Tier do dia
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        LIGAS_TOP = [39, 140, 78, 135, 61, 71, 72, 2, 3] # Premier, La Liga, Serie A, etc.
        jogos_top = [j for j in jogos if j['league']['id'] in LIGAS_TOP][:5] 
        
        if not jogos_top: return "Sem jogos elite hoje."
        
        relatorio = ""
        for j in jogos_top:
            home = j['teams']['home']['name']; away = j['teams']['away']['name']
            fid = j['fixture']['id']
            # Pega odds pre-live
            odds_url = "https://v3.football.api-sports.io/odds"
            o_res = requests.get(odds_url, headers={"x-apisports-key": api_key}, params={"fixture": fid, "bookmaker": "6"}).json()
            odd_txt = "N/A"
            try: odd_txt = str(o_res['response'][0]['bookmakers'][0]['bets'][0]['values'])
            except: pass
            
            prompt = f"""
            Jogo: {home} x {away}
            Odds: {odd_txt}
            Aja como um Tipster Profissional. Dê um palpite de VALOR (Sniper) para este jogo.
            Seja breve. Formato: "Palpite: [Aposta] (Odd aprox) - [Motivo]"
            """
            resp = model_ia.generate_content(prompt)
            relatorio += f"🌅 <b>{home} x {away}</b>\n{resp.text.strip()}\n\n"
            time.sleep(1) # Respeitar rate limit da API
            
        return relatorio
    except Exception as e: return f"Erro Matinal: {e}"

# Auxiliares de Dados
def extrair_dados_completos(stats_api):
    if not stats_api: return "Dados indisponíveis."
    try:
        s1 = stats_api[0]['statistics']; s2 = stats_api[1]['statistics']
        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
        return f"📊 Posse {gv(s1,'Ball Possession')}x{gv(s2,'Ball Possession')} | Chutes {gv(s1,'Total Shots')}x{gv(s2,'Total Shots')} | Cantos {gv(s1,'Corner Kicks')}x{gv(s2,'Corner Kicks')}"
    except: return "Erro stats."

def atualizar_historico_ram(lista_atualizada):
    if 'historico_full' not in st.session_state: return
    df_mem = st.session_state['historico_full']
    for item in lista_atualizada:
        # Atualiza linha correspondente no DF
        mask = (df_mem['FID'] == item['FID']) & (df_mem['Estrategia'] == item['Estrategia'])
        if mask.any():
            df_mem.loc[mask, 'Resultado'] = item['Resultado']
    st.session_state['historico_full'] = df_mem
    st.session_state['precisa_salvar'] = True
# ==============================================================================
# 7. INTERFACE GRÁFICA E LOOP PRINCIPAL
# ==============================================================================
with st.sidebar:
    st.title("❄️ Neves Analytics")
    
    # --- CONFIGURAÇÕES ---
    with st.expander("⚙️ Configurações", expanded=True):
        st.session_state['API_KEY'] = st.text_input("Chave API (API-Sports):", value=st.session_state['API_KEY'], type="password")
        st.session_state['TG_TOKEN'] = st.text_input("Token Bot Telegram:", value=st.session_state['TG_TOKEN'], type="password")
        st.session_state['TG_CHAT'] = st.text_input("Chat IDs (separar por vírgula):", value=st.session_state['TG_CHAT'])
        INTERVALO = st.slider("Ciclo de Varredura (seg):", 60, 300, 60)
        
        if st.button("🧹 Limpar Cache (Reset)"): 
            st.cache_data.clear(); carregar_aba("Historico", COLS_HIST) # Força recarga
            st.session_state['last_db_update'] = 0; st.toast("Sistema limpo!")
    
    # --- FERRAMENTAS MANUAIS ---
    st.write("---")
    st.caption("🛠️ Ferramentas Manuais")
    
    if st.button("🧠 Pedir Análise do BI (IA)"):
        with st.spinner("Analisando performance..."):
            analise = analisar_bi_com_ia()
            st.info(analise)
            
    if st.button("🌅 Gerar Relatório Matinal"):
        with st.spinner("Escaneando melhores jogos do dia..."):
            msg = gerar_insights_matinais_ia(st.session_state['API_KEY'])
            if msg:
                enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], f"🌅 <b>RELATÓRIO MATINAL</b>\n\n{msg}")
                st.success("Enviado para o Telegram!")
            else: st.warning("Sem jogos relevantes.")

    with st.expander("💰 Gestão de Banca", expanded=False):
        stake_padrao = st.number_input("Valor da Aposta (R$)", value=st.session_state.get('stake_padrao', 10.0), step=5.0)
        banca_inicial = st.number_input("Banca Inicial (R$)", value=st.session_state.get('banca_inicial', 100.0), step=50.0)
        st.session_state['stake_padrao'] = stake_padrao; st.session_state['banca_inicial'] = banca_inicial
        
    with st.expander("📶 Consumo API", expanded=False):
        verificar_reset_diario()
        u = st.session_state['api_usage']; perc = min(u['used'] / u['limit'], 1.0) if u['limit'] > 0 else 0
        st.progress(perc); st.caption(f"Utilizado: **{u['used']}** / {u['limit']}")
    
    # --- STATUS DE CONEXÃO ---
    st.write("---")
    tg_ok, tg_nome = testar_conexao_telegram(st.session_state['TG_TOKEN'])
    if tg_ok: st.markdown(f'<div class="status-active">✈️ TELEGRAM: OK ({tg_nome})</div>', unsafe_allow_html=True)
    else: st.markdown(f'<div class="status-error">❌ TELEGRAM: ERRO</div>', unsafe_allow_html=True)

    if IA_ATIVADA: st.markdown('<div class="status-active">🤖 IA GEMINI: OK</div>', unsafe_allow_html=True)
    else: st.markdown('<div class="status-error">❌ IA OFF</div>', unsafe_allow_html=True)
    
    st.write("---")
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)

# --- LOOP PRINCIPAL (CORE) ---
if st.session_state.ROBO_LIGADO:
    with placeholder_root.container():
        # Carga Inicial de Dados
        carregar_aba("Historico", COLS_HIST); carregar_aba("Blacklist", COLS_BLACK); carregar_aba("Seguras", COLS_SAFE)
        
        # Variáveis Locais
        safe_key = st.session_state['API_KEY']
        safe_token = st.session_state['TG_TOKEN']
        safe_chats = st.session_state['TG_CHAT']
        
        # 1. Busca Jogos Ao Vivo
        jogos_live = []; api_error = False
        try:
            url = "https://v3.football.api-sports.io/fixtures"
            resp = requests.get(url, headers={"x-apisports-key": safe_key}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
            update_api_usage(resp.headers)
            jogos_live = resp.json().get('response', [])
        except Exception as e: api_error = True; st.error(f"Erro API: {e}")

        if not api_error:
            # 2. Verifica Resultados de Sinais Pendentes
            check_green_red_hibrido(jogos_live, safe_token, safe_chats, safe_key)
            
            radar = []
            
            # 3. Processamento de Cada Jogo
            for j in jogos_live:
                fid = j['fixture']['id']; liga_id = normalizar_id(j['league']['id'])
                
                # Filtros Básicos
                if liga_id in [normalizar_id(x) for x in st.session_state['df_black']['id'].values]: continue
                
                tempo = j['fixture']['status']['elapsed'] or 0
                placar = f"{j['goals']['home']}x{j['goals']['away']}"
                home = j['teams']['home']['name']; away = j['teams']['away']['name']
                
                # Cache de Stats (Evita chamar API toda hora)
                t_cache = 180 
                # Se for "Zona de Perigo" (Final de jogo ou Intervalo), atualiza mais rápido (60s)
                if (tempo >= 70 and abs(j['goals']['home'] - j['goals']['away']) <= 1) or tempo <= 20: t_cache = 60
                
                ult_chk = st.session_state['controle_stats'].get(fid, datetime.min)
                stats = st.session_state.get(f"st_{fid}", [])
                
                if (datetime.now() - ult_chk).total_seconds() > t_cache:
                    # Busca Stats Detalhadas
                    try:
                        r_st = requests.get("https://v3.football.api-sports.io/fixtures/statistics", headers={"x-apisports-key": safe_key}, params={"fixture": fid}, timeout=5)
                        update_api_usage(r_st.headers)
                        stats = r_st.json().get('response', [])
                        if stats:
                            st.session_state[f"st_{fid}"] = stats
                            st.session_state['controle_stats'][fid] = datetime.now()
                            # Salva no Big Data (Firebase) para a IA aprender
                            salvar_bigdata(j, stats)
                    except: pass
                
                # 4. Análise de Estratégias
                if stats:
                    sinais = processar(j, stats, tempo, placar)
                    status_radar = "👁️ Monitorando"
                    
                    if sinais:
                        status_radar = f"✅ {len(sinais)} SINAIS!"
                        for s in sinais:
                            uid = gerar_chave_universal(fid, s['tag'], "SINAL")
                            
                            # Verifica se já enviou
                            if uid not in st.session_state['alertas_enviados']:
                                # Consultoria IA (Opcional)
                                ia_txt = consultar_ia_gemini({'jogo': f"{home}x{away}", 'placar': placar, 'tempo': tempo}, s['tag'], stats, s['rh'], s['ra'])
                                
                                # Monta Mensagem
                                msg = (
                                    f"🚨 <b>ALERTA DE OPORTUNIDADE</b> 🚨\n\n"
                                    f"⚽ <b>{home} x {away}</b>\n"
                                    f"🏆 {j['league']['name']}\n"
                                    f"⏰ <b>{tempo}' min</b> (Placar: {placar})\n\n"
                                    f"📊 <b>Estratégia:</b> {s['tag']}\n"
                                    f"{s['ordem']}\n\n" # AQUI VEM O TEXTO EXPLICATIVO "FOR DUMMIES"
                                    f"📉 <i>Stats: {s['stats']}</i>"
                                    f"{ia_txt}"
                                )
                                enviar_telegram(safe_token, safe_chats, msg)
                                st.session_state['alertas_enviados'].add(uid)
                                st.toast(f"Sinal Enviado: {s['tag']}")
                                
                                # Salva no Histórico
                                item_hist = {
                                    "FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'),
                                    "Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Placar_Sinal": placar,
                                    "Estrategia": s['tag'], "Resultado": "Pendente", "Odd": "1.80", "Opiniao_IA": "Enviado"
                                }
                                adicionar_historico(item_hist)

                    radar.append({"Tempo": f"{tempo}'", "Liga": j['league']['name'], "Jogo": f"{home} {placar} {away}", "Status": status_radar})

            # 5. Salvar Dados (Persistence)
            if st.session_state.get('precisa_salvar'):
                salvar_aba("Historico", st.session_state['historico_full'])
        
        # --- DASHBOARD VISUAL ---
        st.markdown(f"### 📡 Radar ao Vivo ({len(radar)} Jogos)")
        
        # Métricas de Hoje
        df_hj = pd.DataFrame(st.session_state.get('historico_sinais', []))
        if not df_hj.empty:
            greens = df_hj['Resultado'].str.contains('GREEN').sum()
            reds = df_hj['Resultado'].str.contains('RED').sum()
            total = len(df_hj)
            c1, c2, c3 = st.columns(3)
            c1.metric("Sinais Hoje", total)
            c2.metric("✅ Greens", greens)
            c3.metric("❌ Reds", reds)

        # Abas
        tab1, tab2, tab3, tab4 = st.tabs(["📡 Radar", "📜 Histórico", "💰 Financeiro", "🛑 Blacklist"])
        
        with tab1:
            if radar: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
            else: st.info("Nenhum jogo ao vivo ou monitorado no momento.")
            
        with tab2:
            if not df_hj.empty: st.dataframe(df_hj[['Hora', 'Liga', 'Jogo', 'Estrategia', 'Resultado']], use_container_width=True, hide_index=True)
            
        with tab3:
            st.caption("Simulação baseada nos Greens/Reds de hoje")
            if not df_hj.empty:
                # Calculo Simples de ROI
                saldo = st.session_state['banca_inicial']
                stake = st.session_state['stake_padrao']
                lucro_est = (greens * stake * 0.80) - (reds * stake) # Odd média 1.80
                st.metric("Lucro Estimado Hoje", f"R$ {lucro_est:.2f}", delta=f"{(lucro_est/saldo*100):.1f}%")
        
        with tab4:
            st.dataframe(st.session_state['df_black'], use_container_width=True)

        # Timer de Atualização
        for i in range(INTERVALO, 0, -1):
            st.markdown(f'<div class="footer-timer">🔄 Atualizando em {i}s...</div>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()

else:
    with placeholder_root.container():
        st.title("❄️ Neves Analytics PRO")
        st.warning("⚠️ O Robô está DESLIGADO.")
        st.info("Configure a API na barra lateral e marque a opção 'Ligar Robô' para iniciar.")    
