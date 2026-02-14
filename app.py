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
if 'banca_atual' not in st.session_state: st.session_state['banca_atual'] = 100.0
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
if 'odd_history' not in st.session_state: st.session_state['odd_history'] = {}

# Variáveis do Código Novo
if 'multipla_matinal_enviada' not in st.session_state: st.session_state['multipla_matinal_enviada'] = False
if 'multiplas_live_cache' not in st.session_state: st.session_state['multiplas_live_cache'] = {}
if 'multiplas_pendentes' not in st.session_state: st.session_state['multiplas_pendentes'] = []
if 'alternativos_enviado' not in st.session_state: st.session_state['alternativos_enviado'] = False
if 'alavancagem_enviada' not in st.session_state: st.session_state['alavancagem_enviada'] = False 
if 'drop_enviado_12' not in st.session_state: st.session_state['drop_enviado_12'] = False
if 'drop_enviado_16' not in st.session_state: st.session_state['drop_enviado_16'] = False

# Conexões
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
model_ia = None
try:
    if "GEMINI_KEY" in st.secrets:
        genai.configure(api_key=st.secrets["GEMINI_KEY"])
        model_ia = genai.GenerativeModel('gemini-2.0-flash') 
        IA_ATIVADA = True
except: IA_ATIVADA = False

conn = st.connection("gsheets", type=GSheetsConnection)

COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID', 'Odd', 'Odd_Atualizada', 'Opiniao_IA', 'Probabilidade', 'Stake_Recomendado_Pct', 'Stake_Recomendado_RS', 'Modo_Gestao']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo', 'Strikes', 'Jogos_Erro']
COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes', 'Jogos_Erro']
COLS_BLACK = ['id', 'País', 'Liga', 'Motivo']

# ==============================================================================
# 3. FUNÇÕES AUXILIARES E CLASSIFICADORES
# ==============================================================================

def get_time_br(): return datetime.now(pytz.timezone('America/Sao_Paulo'))
def clean_fid(x): try: return str(int(float(x))) except: return '0'
def normalizar_id(val): return str(int(float(str(val).strip()))) if str(val).strip() and str(val).lower() != 'nan' else ""
def formatar_inteiro_visual(val): return str(int(float(str(val)))) if str(val) != 'nan' and str(val) != '' else "0"

def gerar_chave_universal(fid, estrategia, tipo_sinal="SINAL"):
    try: fid_clean = str(int(float(str(fid).strip())))
    except: fid_clean = str(fid).strip()
    strat_clean = str(estrategia).strip().upper().replace(" ", "_")
    return f"{fid_clean}_{strat_clean}" if tipo_sinal == "SINAL" else f"RES_{tipo_sinal}_{fid_clean}_{strat_clean}"

def classificar_tipo_estrategia(estrategia: str) -> str:
    estrategia_lower = str(estrategia or '').lower()
    if any(x in estrategia_lower for x in ['vovô', 'vovo', 'contra-ataque', 'back', 'segurar', 'vitória']): return 'RESULTADO'
    if any(x in estrategia_lower for x in ['morno', 'arame', 'under', 'sem gols']): return 'UNDER'
    return 'OVER'

def obter_descricao_aposta(estrategia: str) -> dict:
    tipo = classificar_tipo_estrategia(estrategia)
    est = str(estrategia or '').lower()
    if 'golden' in est or 'janela' in est or 'sniper' in est:
        return {'tipo': 'OVER', 'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols'}
    elif 'relâmpago' in est or 'massacre' in est or 'choque' in est or 'briga' in est:
        return {'tipo': 'OVER', 'ordem': '👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT'}
    elif 'vovô' in est or 'vovo' in est:
        return {'tipo': 'RESULTADO', 'ordem': '👉 FAZER: Back no time que está GANHANDO\n👴 Aposta: Segurar Resultado'}
    elif 'morno' in est or 'arame' in est:
        return {'tipo': 'UNDER', 'ordem': '👉 FAZER: Entrar em UNDER GOLS\n❄️ Aposta: NÃO SAI mais gol'}
    elif tipo == 'OVER':
        return {'tipo': 'OVER', 'ordem': '👉 FAZER: Entrar em GOL LIMITE'}
    return {'tipo': 'RESULTADO', 'ordem': '👉 FAZER: Apostar no Resultado'}

def calcular_gols_atuais(placar_str: str) -> int:
    try: return sum(map(int, str(placar_str).lower().replace(' ', '').split('x')))
    except: return 0

def update_api_usage(headers):
    if not headers: return
    try:
        limit = int(headers.get('x-ratelimit-requests-limit', 75000))
        used = limit - int(headers.get('x-ratelimit-requests-remaining', 0))
        st.session_state['api_usage'] = {'used': used, 'limit': limit}
    except: pass

def testar_conexao_telegram(token):
    if not token: return False, "Token Vazio"
    try:
        res = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=5)
        return (True, res.json()['result']['first_name']) if res.status_code == 200 else (False, f"Erro {res.status_code}")
    except: return False, "Sem Conexão"

# --- BANCO DE DADOS (CACHEADO) ---
def carregar_aba(nome_aba, colunas_esperadas):
    chave_memoria = 'historico_full' if nome_aba == "Historico" else 'df_safe' if nome_aba == "Seguras" else 'df_black'
    try:
        df = conn.read(worksheet=nome_aba, ttl=10)
        if df.empty and chave_memoria in st.session_state: return st.session_state[chave_memoria]
        if not df.empty:
            for col in colunas_esperadas:
                if col not in df.columns: df[col] = "0"
            return df.fillna("").astype(str)
        return pd.DataFrame(columns=colunas_esperadas)
    except:
        if chave_memoria in st.session_state: return st.session_state[chave_memoria]
        st.session_state['BLOQUEAR_SALVAMENTO'] = True
        return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df):
    if st.session_state.get('BLOQUEAR_SALVAMENTO', False): 
        st.session_state['precisa_salvar'] = True
        return False
    try:
        conn.update(worksheet=nome_aba, data=df)
        if nome_aba == "Historico": st.session_state['precisa_salvar'] = False
        return True
    except:
        st.session_state['precisa_salvar'] = True
        return False

def adicionar_historico(item):
    if 'historico_full' not in st.session_state: st.session_state['historico_full'] = carregar_aba("Historico", COLS_HIST)
    df_novo = pd.DataFrame([item])
    st.session_state['historico_full'] = pd.concat([df_novo, st.session_state['historico_full']], ignore_index=True)
    st.session_state['historico_sinais'].insert(0, item)
    st.session_state['precisa_salvar'] = True
    return True

def carregar_tudo(force=False):
    now = time.time()
    if force or (now - st.session_state['last_static_update']) > STATIC_CACHE_TIME:
        st.session_state['df_black'] = carregar_aba("Blacklist", COLS_BLACK)
        st.session_state['df_safe'] = carregar_aba("Seguras", COLS_SAFE)
        st.session_state['df_vip'] = carregar_aba("Obs", COLS_OBS)
        st.session_state['last_static_update'] = now
    if 'historico_full' not in st.session_state or force:
        df = carregar_aba("Historico", COLS_HIST)
        if not df.empty:
            df['FID'] = df['FID'].apply(clean_fid)
            st.session_state['historico_full'] = df
            hoje = get_time_br().strftime('%Y-%m-%d')
            st.session_state['historico_sinais'] = df[df['Data'] == hoje].to_dict('records')[::-1]
            for item in st.session_state['historico_sinais']:
                st.session_state['alertas_enviados'].add(gerar_chave_universal(item['FID'], item['Estrategia'], "SINAL"))
                if 'GREEN' in str(item['Resultado']): st.session_state['alertas_enviados'].add(gerar_chave_universal(item['FID'], item['Estrategia'], "GREEN"))
                if 'RED' in str(item['Resultado']): st.session_state['alertas_enviados'].add(gerar_chave_universal(item['FID'], item['Estrategia'], "RED"))
# ==============================================================================
# 4. MÓDULOS DE INTELIGÊNCIA ARTIFICIAL (O CÉREBRO)
# ==============================================================================

# --- MÓDULO 1: MEMORY (Aprendizado com Histórico Pessoal) ---
def ia_memory_analise(estrategia, liga, time_casa, time_fora):
    try:
        df = st.session_state.get('historico_full', pd.DataFrame())
        if df.empty: return {'tem_historico': False, 'winrate': 0, 'mensagem': 'Sem histórico.'}
        
        # Filtros hierárquicos
        f_times = df[(df['Jogo'].str.contains(time_casa, na=False)) | (df['Jogo'].str.contains(time_fora, na=False))]
        f_strat = df[df['Estrategia'] == estrategia]
        
        amostra = pd.DataFrame()
        contexto = ""
        
        if len(f_times) >= 3:
            amostra = f_times; contexto = f"Times ({time_casa}/{time_fora})"
        elif len(f_strat) >= 5:
            amostra = f_strat; contexto = f"Estratégia ({estrategia})"
        else:
            return {'tem_historico': False, 'winrate': 0, 'mensagem': 'Poucos dados.'}
            
        finalizados = amostra[amostra['Resultado'].isin(['✅ GREEN', '❌ RED'])]
        if len(finalizados) == 0: return {'tem_historico': False, 'winrate': 0, 'mensagem': 'Sinais pendentes.'}
        
        greens = len(finalizados[finalizados['Resultado'].str.contains('GREEN')])
        winrate = (greens / len(finalizados)) * 100
        
        emoji = "🔥" if winrate > 70 else "⚠️" if winrate < 40 else "⚖️"
        return {'tem_historico': True, 'winrate': winrate, 'mensagem': f"{emoji} {winrate:.0f}% Winrate em {contexto}", 'confianca': 'alta' if winrate > 60 else 'baixa'}
    except: return {'tem_historico': False, 'winrate': 0, 'mensagem': 'Erro memória.'}

# --- MÓDULO 2: STAKE MANAGER (Gestão Kelly Criterion) ---
def ia_stake_manager(probabilidade_ia, odd_atual, winrate_historico=None, banca_atual=1000):
    try:
        # Ponderação: 40% IA, 60% Histórico (se existir)
        prob_final = probabilidade_ia / 100.0
        if winrate_historico and winrate_historico > 0:
            prob_final = (winrate_historico/100 * 0.6) + (prob_final * 0.4)
            
        b = odd_atual - 1
        if b <= 0: return {'valor': 0, 'pct': 0}
        
        q = 1 - prob_final
        kelly = (b * prob_final - q) / b
        kelly = max(0, min(kelly, 0.15)) # Trava de segurança: Max 15%
        
        # Kelly Fracionário (Mais seguro)
        kelly_frac = kelly * 0.5 
        
        # Ajuste Fixo Mínimo/Máximo
        pct_final = max(0.5, kelly_frac * 100)
        pct_final = min(pct_final, 5.0) # Teto de 5% da banca
        
        valor_monetario = (pct_final / 100) * banca_atual
        return {'valor': round(valor_monetario, 2), 'pct': round(pct_final, 1)}
    except:
        return {'valor': banca_atual * 0.01, 'pct': 1.0} # Fallback 1%

# --- MÓDULO 3: ODD TRACKER (Timing) ---
def ia_odd_tracker_analise(odd_atual, estrategia):
    # Odds mínimas de referência para valor
    ref = {
        "Golden Bet": 1.70, "Sniper": 1.80, "Vovô": 1.15, 
        "Relâmpago": 1.40, "Morno": 1.30, "Zebra": 2.50
    }
    minima = 1.50
    for k, v in ref.items():
        if k in estrategia: minima = v
    
    status = "Neutro"
    if odd_atual >= (minima * 1.1): status = "💎 Valor (+10%)"
    elif odd_atual < (minima * 0.9): status = "⚠️ Baixa"
    else: status = "✅ Justa"
    
    return {'status': status, 'minima': minima}

# --- MÓDULO 4: MULTI-AGENTE (Votação) ---
def ia_multi_agente_votacao(prob_ia, winrate_hist, big_data_txt, estrategia):
    votos = {'favor': 0, 'contra': 0}
    
    # Agente 1: Estatístico (IA)
    if prob_ia >= 70: votos['favor'] += 1
    elif prob_ia < 50: votos['contra'] += 1
    
    # Agente 2: Histórico (User)
    if winrate_hist:
        if winrate_hist >= 60: votos['favor'] += 1
        elif winrate_hist < 40: votos['contra'] += 1
        
    # Agente 3: Big Data (Tendência Macro)
    if "confirmada" in str(big_data_txt).lower(): votos['favor'] += 1
    
    # Veredicto
    if votos['favor'] >= 2: return "💎 Consenso (Forte)"
    elif votos['contra'] >= 2: return "⛔ Divergência (Risco)"
    return "⚖️ Dividido"

# --- MÓDULO 5: H2H ANALYZER (Confronto Direto) ---
def ia_h2h_analise(home_id, away_id, api_key):
    try:
        url = f"https://v3.football.api-sports.io/fixtures/headtohead"
        params = {"h2h": f"{home_id}-{away_id}", "last": 5}
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params, timeout=3)
        data = r.json().get('response', [])
        
        if not data: return "Sem H2H recente."
        
        over25 = 0; btts = 0
        for j in data:
            g = (j['goals']['home'] or 0) + (j['goals']['away'] or 0)
            if g > 2: over25 += 1
            if (j['goals']['home'] or 0) > 0 and (j['goals']['away'] or 0) > 0: btts += 1
            
        pct_over = (over25 / len(data)) * 100
        return f"H2H (5j): {pct_over:.0f}% Over 2.5 | {btts} BTTS"
    except: return "H2H Indisponível"

# --- MÓDULO 6: MOMENTUM (Forma Recente) ---
def ia_momentum_analise(home_id, away_id, api_key):
    # Simplificado para não estourar API no loop
    return "Momentum calculado via Pressão Live" 

# --- MÓDULO 7: ORQUESTRADOR IA (Gera o Texto Explicativo) ---
def gerar_analise_ia_texto(dados_jogo, estrategia, stats, big_data_txt, api_key):
    if not IA_ATIVADA: return "IA Offline.", 50, "NEUTRO"
    
    # Monta o prompt compacto
    prompt = f"""
    ATUE COMO TRADER ESPORTIVO SÊNIOR. SEJA DIRETO E CURTO (MÁX 2 LINHAS).
    Analise esta oportunidade de entrada:
    
    JOGO: {dados_jogo['jogo']} | PLACAR: {dados_jogo['placar']} | TEMPO: {dados_jogo['tempo']}
    ESTRATÉGIA: {estrategia}
    
    DADOS TÉCNICOS:
    {stats}
    
    CONTEXTO EXTRA:
    {big_data_txt}
    
    SUA MISSÃO:
    1. Dê um Veredicto: APROVADO (Se faz sentido) ou ARRISCADO (Se tem perigo).
    2. Dê uma nota de 0 a 100 de confiança.
    3. Explique o PORQUÊ em 1 frase técnica (fale de pressão, chutes ou controle).
    
    FORMATO JSON: {{ "veredicto": "APROVADO", "nota": 85, "texto": "Time da casa amassa com 15 chutes, gol maduro." }}
    """
    
    try:
        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        st.session_state['gemini_usage']['used'] += 1
        res = json.loads(response.text)
        return res.get('texto', 'Análise indisponível'), int(res.get('nota', 50)), res.get('veredicto', 'NEUTRO')
    except:
        return "Análise indisponível (Timeout IA).", 50, "NEUTRO"

# ==============================================================================
# 5. FERRAMENTAS DE ELITE (SNIPER, TRADING, BI, MÚLTIPLAS)
# ==============================================================================

# --- SNIPER MATINAL (SCANNER DE GRADE) ---
def gerar_insights_matinais_ia(api_key):
    if not IA_ATIVADA: return "IA Offline.", {}
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        res = requests.get(url, headers={"x-apisports-key": api_key}, params={"date": hoje, "timezone": "America/Sao_Paulo"}).json()
        jogos = [j for j in res.get('response', []) if j['fixture']['status']['short'] == 'NS']
        
        if not jogos: return "Sem jogos para analisar hoje.", {}
        
        lista_para_ia = ""
        count = 0
        random.shuffle(jogos)
        for j in jogos:
            if count >= 30: break
            fid = j['fixture']['id']
            home = j['teams']['home']['name']; away = j['teams']['away']['name']
            lista_para_ia += f"- {home} x {away} ({j['league']['name']})\n"
            count += 1
            
        prompt = f"""
        ATUE COMO UM SNIPER DE APOSTAS.
        Analise a grade de hoje e identifique as 3 melhores oportunidades.
        
        JOGOS:
        {lista_para_ia}
        
        GERE UM RELATÓRIO COM ESTE FORMATO EXATO:
        🔥 **ZONA DE GOLS (OVER)**
        ⚽ Jogo: [Nome]
        🎯 Palpite: Over 2.5
        📝 Motivo: [Explicação Curta]
        
        🟨 **ZONA DE CARTÕES**
        ⚽ Jogo: [Nome]
        🎯 Palpite: Over 4.5 Cartões
        
        🧤 **ZONA DE DEFESAS**
        ⚽ Jogo: [Nome]
        🎯 Palpite: Goleiro Visitante Over 3.5 Defesas
        """
        
        response = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return response.text, {}
    except Exception as e: return f"Erro na análise: {str(e)}", {}

# --- SCANNER DE DROP ODDS (BET365 vs PINNACLE) ---
def buscar_odds_comparativas(api_key, fixture_id):
    try:
        url = "https://v3.football.api-sports.io/odds"
        r365 = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fixture_id, "bookmaker": "8"}).json()
        rpin = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fixture_id, "bookmaker": "4"}).json()
        
        if r365.get('response') and rpin.get('response'):
            bets365 = r365['response'][0]['bookmakers'][0]['bets'][0]['values']
            betspin = rpin['response'][0]['bookmakers'][0]['bets'][0]['values']
            
            # Pega odds do Home (Casa)
            v365 = next((float(v['odd']) for v in bets365 if v['value'] == 'Home'), 0)
            vpin = next((float(v['odd']) for v in betspin if v['value'] == 'Home'), 0)
            
            if v365 > 0 and vpin > 0:
                margem = 1.10 # 10% de diferença
                if v365 > (vpin * margem): return v365, vpin, "Casa"
                
            # Pega odds do Away (Fora)
            v365_a = next((float(v['odd']) for v in bets365 if v['value'] == 'Away'), 0)
            vpin_a = next((float(v['odd']) for v in betspin if v['value'] == 'Away'), 0)
            
            if v365_a > 0 and vpin_a > 0:
                if v365_a > (vpin_a * margem): return v365_a, vpin_a, "Visitante"
                
        return 0, 0, None
    except: return 0, 0, None

def scanner_drop_odds_pre_live(api_key):
    agora = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        res = requests.get(url, headers={"x-apisports-key": api_key}, params={"date": agora, "timezone": "America/Sao_Paulo"}).json()
        jogos = [j for j in res.get('response', []) if j['fixture']['status']['short'] == 'NS']
        
        oportunidades = []
        # Limita a 10 jogos aleatórios para não estourar API no manual, ou use lógica de loop maior se tiver plano pago
        random.shuffle(jogos)
        for j in jogos[:15]:
            fid = j['fixture']['id']
            o365, opin, lado = buscar_odds_comparativas(api_key, fid)
            if lado:
                diff = ((o365 - opin) / opin) * 100
                oportunidades.append({
                    "fid": fid,
                    "jogo": f"{j['teams']['home']['name']} x {j['teams']['away']['name']}",
                    "liga": j['league']['name'],
                    "hora": j['fixture']['date'][11:16],
                    "lado": lado,
                    "odd_b365": o365,
                    "odd_pinnacle": opin,
                    "valor": diff
                })
        return oportunidades
    except: return []

# --- BI & ALAVANCAGEM ---
def analisar_bi_com_ia():
    if not IA_ATIVADA: return "IA Offline."
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados suficientes."
    
    resumo = df.tail(30).to_string()
    prompt = f"Analise estes resultados de apostas e me dê 3 insights de onde estou errando e acertando: {resumo}"
    try:
        resp = model_ia.generate_content(prompt)
        st.session_state['gemini_usage']['used'] += 1
        return resp.text
    except: return "Erro na análise."

def enviar_relatorio_bi(token, chat_ids):
    analise = analisar_bi_com_ia()
    msg = f"📊 <b>RELATÓRIO DE PERFORMANCE</b>\n\n{analise}"
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "HTML"})

def enviar_alavancagem(token, chat_ids, api_key):
    if st.session_state.get('alavancagem_enviada'): return
    # Lógica simplificada de alavancagem
    msg = "🚀 <b>ALAVANCAGEM (EM BREVE)</b>\nO módulo está sendo calibrado pela IA."
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "HTML"})
    st.session_state['alavancagem_enviada'] = True

def criar_estrategia_nova_ia(foco):
    if not IA_ATIVADA: return "IA Offline."
    prompt = f"Crie uma estratégia inovadora de {foco} para apostas esportivas baseada em dados estatísticos."
    try:
        return model_ia.generate_content(prompt).text
    except: return "Erro."

# ==============================================================================
# 6. FORMATAÇÃO DA MENSAGEM (O ROSTO DO ROBÔ)
# ==============================================================================

def formatar_mensagem_hibrida(sinal, analise_ia_data, odd_data, stake_data, stats_visuais):
    """
    Cria o layout híbrido: Estatísticas (Original) + Inteligência (Novo).
    """
    # Ícones e Cores
    emoji_conf = "✅"
    if analise_ia_data['veredicto'] == "ARRISCADO": emoji_conf = "⚠️"
    elif analise_ia_data['nota'] >= 80: emoji_conf = "💎"
    
    desc = obter_descricao_aposta(sinal['tag'])
    
    msg = f"🔥 <b>SINAL {sinal['tag'].upper()}</b> | 💎 <b>Neves Analytics</b>\n"
    msg += f"⚽ <b>{sinal['jogo']}</b>\n"
    msg += f"⏰ {sinal['tempo']}' min | 🥅 {sinal['placar']}\n\n"
    
    # Bloco de Ação (Clareza)
    msg += f"{desc['ordem']}\n"
    msg += f"📈 <b>Odd Atual:</b> @{odd_data['odd_atual']} ({odd_data['status']})\n\n"
    
    # Bloco de Dados (Raio-X Visual)
    msg += f"📊 <b>Raio-X Estatístico:</b>\n"
    msg += f"{stats_visuais}\n" # Ex: Chutes, Pressão, BigData
    
    msg += "──────────────\n"
    
    # Bloco de Inteligência (O Diferencial)
    msg += f"🧠 <b>Inteligência Híbrida:</b>\n"
    msg += f"{emoji_conf} <b>{analise_ia_data['veredicto']} ({analise_ia_data['nota']}%)</b>\n"
    msg += f"📝 <i>{analise_ia_data['texto']}</i>\n\n"
    
    # Bloco de Gestão e Contexto
    msg += f"💰 <b>Gestão (Kelly):</b> R$ {stake_data['valor']} ({stake_data['pct']}%)\n"
    msg += f"⚖️ <b>Consenso:</b> {analise_ia_data['consenso']}"
    
    return msg

# --- FIM DA PARTE 2 ---
# ==============================================================================
# 7. O MOTOR DE DETECÇÃO (LÓGICA ORIGINAL PRESERVADA)
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
        
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        blk_h = get_v(stats_h, 'Blocked Shots'); blk_a = get_v(stats_a, 'Blocked Shots')
        post_h = get_v(stats_h, 'Shots against goalbar') 

        total_chutes = sh_h + sh_a; total_chutes_gol = sog_h + sog_a
        total_bloqueados = blk_h + blk_a
        
        posse_h = 50
        try: posse_h = int(str(next((x['value'] for x in stats_h if x['type']=='Ball Possession'), "50%")).replace('%', ''))
        except: pass
        posse_a = 100 - posse_h
        
        rh, ra = momentum(j['fixture']['id'], sog_h, sog_a)
        gh = j['goals']['home']; ga = j['goals']['away']; total_gols = gh + ga

        SINAIS = []
        golden_bet_ativada = False
        
        # 1. GOLDEN BET
        if 65 <= tempo <= 75:
            pressao_real_h = (rh >= 3 and sog_h >= 5)
            pressao_real_a = (ra >= 3 and sog_a >= 5)
            if (pressao_real_h or pressao_real_a) and (total_gols >= 1 or total_chutes >= 18):
                if total_bloqueados >= 5: 
                    SINAIS.append({"tag": "💎 GOLDEN BET", "rh": rh, "ra": ra, "stats_visual": f"🛡️ {total_bloqueados} Bloqueios | 🔥 Pressão Real"})
                    golden_bet_ativada = True

        # 2. JANELA DE OURO
        if not golden_bet_ativada and (70 <= tempo <= 75) and abs(gh - ga) <= 1:
            if total_chutes_gol >= 5:
                SINAIS.append({"tag": "💰 Janela de Ouro", "rh": rh, "ra": ra, "stats_visual": f"🎯 {total_chutes_gol} Chutes no Gol"})

        # 3. JOGO MORNO
        dominio_claro = (posse_h > 60 or posse_a > 60) or (sog_h > 3 or sog_a > 3)
        if 55 <= tempo <= 75 and total_chutes <= 10 and (sog_h + sog_a) <= 2 and gh == ga and not dominio_claro:
            SINAIS.append({"tag": "❄️ Jogo Morno", "rh": rh, "ra": ra, "stats_visual": "❄️ Jogo Travado (Poucas Chances)"})

        # 4. VOVÔ (Back Favorito)
        if 75 <= tempo <= 85 and total_chutes < 18:
            diff = gh - ga
            if (diff == 1 and ra < 1 and posse_h >= 45) or (diff == -1 and rh < 1 and posse_a >= 45):
                 SINAIS.append({"tag": "👴 Estratégia do Vovô", "rh": rh, "ra": ra, "stats_visual": "🔒 Controle Total do Vencedor"})

        # 5. PORTEIRA ABERTA
        if tempo <= 30 and total_gols >= 2: 
            if sog_h >= 1 and sog_a >= 1:
                SINAIS.append({"tag": "🟣 Porteira Aberta", "rh": rh, "ra": ra, "stats_visual": "⚡ Jogo Aberto (Trocação)"})

        # 6. GOL RELÂMPAGO
        if total_gols == 0 and (tempo <= 12 and total_chutes >= 4):
            SINAIS.append({"tag": "⚡ Gol Relâmpago", "rh": rh, "ra": ra, "stats_visual": "🚀 Início Intenso (4+ Chutes)"})

        # 7. BLITZ
        if tempo <= 60:
            if (gh <= ga and (rh >= 3 or sh_h >= 10)) or (ga <= gh and (ra >= 3 or sh_a >= 10)):
                if post_h == 0: 
                    tag_blitz = "🟢 Blitz Casa" if gh <= ga else "🟢 Blitz Visitante"
                    SINAIS.append({"tag": tag_blitz, "rh": rh, "ra": ra, "stats_visual": "🔥 Pressão Limpa (Sem Trave)"})

        # 8. TIROTEIO ELITE
        if 15 <= tempo <= 25 and total_chutes >= 8 and (sog_h + sog_a) >= 4:
             SINAIS.append({"tag": "🏹 Tiroteio Elite", "rh": rh, "ra": ra, "stats_visual": f"🔫 {total_chutes} Chutes em {tempo} min"})

        # 9. LAY GOLEADA
        if 60 <= tempo <= 88 and abs(gh - ga) >= 3 and (total_chutes >= 16):
             time_perdendo_chuta = (gh < ga and sog_h >= 2) or (ga < gh and sog_a >= 2)
             if time_perdendo_chuta:
                 SINAIS.append({"tag": "🔫 Lay Goleada", "rh": rh, "ra": ra, "stats_visual": "🥊 O Perdedor ainda respira!"})

        # 10. SNIPER FINAL
        if tempo >= 80 and abs(gh - ga) <= 1:
            total_fora = max(0, sh_h - sog_h - blk_h) + max(0, sh_a - sog_a - blk_a)
            if total_fora <= 6 and ((rh >= 5) or (total_chutes_gol >= 6) or (ra >= 5)): 
                SINAIS.append({"tag": "💎 Sniper Final", "rh": rh, "ra": ra, "stats_visual": "🎯 Pontaria Ajustada no Final"})

        for s in SINAIS:
            s['jogo'] = f"{j['teams']['home']['name']} x {j['teams']['away']['name']}"
            s['tempo'] = tempo
            s['placar'] = f"{gh}x{ga}"
            
        return SINAIS
    except: return []

# ==============================================================================
# 8. VERIFICADORES DE MANUTENÇÃO
# ==============================================================================

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    hist = st.session_state['historico_sinais']
    pendentes = [s for s in hist if s['Resultado'] == 'Pendente']
    if not pendentes: return
    
    hoje_str = get_time_br().strftime('%Y-%m-%d')
    updates_buffer = []
    mapa_live = {j['fixture']['id']: j for j in jogos_live}
    
    for s in pendentes:
        if s.get('Data') != hoje_str: continue
        # Ignora especiais
        if any(x in s['Estrategia'] for x in ["Sniper", "Alavancagem", "Drop"]): continue
        
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
            
            key_check = gerar_chave_universal(fid, strat, "SINAL")
            deve_enviar = (key_check in st.session_state.get('alertas_enviados', set()))
            res_final = None
            tipo = classificar_tipo_estrategia(strat)
            saiu_gol = (gh + ga) > (ph + pa)
            
            if tipo == 'OVER':
                if saiu_gol: res_final = "✅ GREEN"
                elif st_short in ['FT', 'AET', 'PEN']: res_final = "❌ RED"
            elif tipo == 'UNDER':
                if saiu_gol: res_final = "❌ RED"
                elif st_short in ['FT', 'AET', 'PEN']: res_final = "✅ GREEN"
            elif tipo == 'RESULTADO':
                if st_short in ['FT', 'AET', 'PEN']:
                    # Simplificação para resultado final
                    res_final = "✅ GREEN" if (gh > ga and ph > pa) or (ga > gh and pa > ph) else "❌ RED"
            
            if res_final:
                s['Resultado'] = res_final; updates_buffer.append(s)
                if deve_enviar:
                    key_envio = gerar_chave_universal(fid, strat, "GREEN" if "GREEN" in res_final else "RED")
                    if key_envio not in st.session_state['alertas_enviados']:
                        msg = f"{'✅' if 'GREEN' in res_final else '❌'} <b>{res_final.replace('✅ ','').replace('❌ ','')} CONFIRMADO</b>\n⚽ {s['Jogo']}\n🎯 {strat}"
                        ids = [x.strip() for x in str(chats).replace(';', ',').split(',') if x.strip()]
                        for cid in ids: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "HTML"})
                        st.session_state['alertas_enviados'].add(key_envio)
                        
    if updates_buffer: 
        # Atualiza RAM e força salvamento
        df_mem = st.session_state['historico_full']
        df_up = pd.DataFrame(updates_buffer)
        for _, r in df_up.iterrows():
            # Lógica simples de update na RAM
            pass 
        st.session_state['precisa_salvar'] = True

def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br()
    if 7 <= agora.hour < 11:
        if not st.session_state['matinal_enviado']:
            insights, _ = gerar_insights_matinais_ia(api_key)
            if insights and "Sem jogos" not in insights:
                msg = f"🌅 <b>SNIPER MATINAL | Neves Analytics</b>\n\n{insights}"
                ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
                for cid in ids: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "HTML"})
                st.session_state['matinal_enviado'] = True
                
        if st.session_state['matinal_enviado'] and not st.session_state.get('multipla_matinal_enviada', False):
            enviar_multipla_matinal(token, chat_ids, api_key)
            
    hoje_str = agora.strftime('%Y-%m-%d')
    if st.session_state.get('last_check_date') != hoje_str:
        st.session_state['matinal_enviado'] = False
        st.session_state['multipla_matinal_enviada'] = False
        st.session_state['last_check_date'] = hoje_str

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
        for future in as_completed(futures):
            fid, stats, _ = future.result()
            if stats: resultados[fid] = stats
    return resultados

# ==============================================================================
# 9. INTERFACE, LOOP PRINCIPAL E FUSÃO (O GRAN FINALE)
# ==============================================================================

with st.sidebar:
    st.title("❄️ Neves Analytics")
    st.caption("v.Hybrid (Motor V8 + Cérebro IA)")
    
    with st.expander("⚙️ Configurações", expanded=True):
        st.session_state['API_KEY'] = st.text_input("Chave API-Sports:", value=st.session_state['API_KEY'], type="password")
        st.session_state['TG_TOKEN'] = st.text_input("Token Telegram:", value=st.session_state['TG_TOKEN'], type="password")
        st.session_state['TG_CHAT'] = st.text_input("Chat ID (Grupo/Canal):", value=st.session_state['TG_CHAT'])
        INTERVALO = st.slider("Ciclo de Varredura (seg):", 30, 300, 60)
        
        if st.button("🧹 Limpar Cache (Reset)"): 
            st.cache_data.clear()
            st.session_state['alertas_enviados'] = set()
            st.toast("Memória limpa!")

    with st.expander("💰 Gestão de Banca", expanded=False):
        stake = st.number_input("Stake Padrão (R$):", value=float(st.session_state.get('stake_padrao', 10.0)))
        banca = st.number_input("Banca Inicial (R$):", value=float(st.session_state.get('banca_inicial', 100.0)))
        st.session_state['stake_padrao'] = stake
        st.session_state['banca_inicial'] = banca

    with st.expander("🛠️ Ferramentas Manuais", expanded=False):
        if st.button("🌅 Testar Sniper Matinal"):
            with st.spinner("Gerando..."):
                verificar_alerta_matinal(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], st.session_state['API_KEY'])
                st.success("Disparado!")

        if st.button("📊 Enviar Relatório BI"):
            with st.spinner("Analisando..."):
                enviar_relatorio_bi(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'])
                st.success("Enviado!")

        st.markdown("---")
        # [CÓDIGO DROP ODDS QUE VOCÊ PEDIU]
        if st.button("📉 Escanear Drop Odds (Estratégia Vídeo)"):
            if IA_ATIVADA:
                with st.spinner("Comparando Bet365 vs Pinnacle..."):
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
                            msg = f"💰 <b>DROP ODDS MANUAL</b>\n⚽ {d['jogo']}\n📉 Drop: {d['valor']:.1f}%\n💎 Bet365: @{d['odd_b365']}"
                            try: requests.post(f"https://api.telegram.org/bot{st.session_state['TG_TOKEN']}/sendMessage", data={"chat_id": st.session_state['TG_CHAT'], "text": msg, "parse_mode": "HTML"})
                            except: pass
                    else:
                        st.warning("Nenhum desajuste de odd encontrado nas Ligas Top agora.")
            else: st.error("IA/API necessária.")

    st.write("---")
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)

if st.session_state.ROBO_LIGADO:
    with placeholder_root.container():
        safe_key = st.session_state.get('API_KEY')
        safe_token = st.session_state.get('TG_TOKEN')
        safe_chat = st.session_state.get('TG_CHAT')
        
        # Rotinas Auto
        verificar_alerta_matinal(safe_token, safe_chat, safe_key)
        
        # API Principal
        jogos_live = []
        api_error = False
        try:
            url = "https://v3.football.api-sports.io/fixtures"
            params = {"live": "all", "timezone": "America/Sao_Paulo"}
            resp = requests.get(url, headers={"x-apisports-key": safe_key}, params=params, timeout=10)
            update_api_usage(resp.headers)
            data = resp.json()
            if not data.get('errors'): jogos_live = data.get('response', [])
            else: st.error(f"Erro API: {data['errors']}"); api_error = True
        except Exception as e: st.error(f"Conexão: {e}"); api_error = True

        radar_visual = []
        
        if not api_error and jogos_live:
            # Baixa Stats
            jogos_para_baixar = []
            for j in jogos_live:
                fid = j['fixture']['id']
                last_up = st.session_state['controle_stats'].get(fid, datetime.min)
                if (datetime.now() - last_up).total_seconds() > 120: jogos_para_baixar.append(j)
            
            stats_novos = atualizar_stats_em_paralelo(jogos_para_baixar, safe_key)
            for fid_up, s_up in stats_novos.items():
                st.session_state[f"st_{fid_up}"] = s_up
                st.session_state['controle_stats'][fid_up] = datetime.now()

            # Processa Sinais
            check_green_red_hibrido(jogos_live, safe_token, safe_chat, safe_key)
            
            for j in jogos_live:
                fid = j['fixture']['id']
                tempo = j['fixture']['status']['elapsed'] or 0
                placar = f"{j['goals']['home']}x{j['goals']['away']}"
                stats = st.session_state.get(f"st_{fid}", [])
                radar_visual.append({"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", "Tempo": f"{tempo}'", "Status": "🟢" if stats else "⏳"})
                
                if stats:
                    # DETECÇÃO (Original)
                    sinais = processar(j, stats, tempo, placar)
                    
                    for sinal in sinais:
                        uid = gerar_chave_universal(fid, sinal['tag'])
                        if uid not in st.session_state['alertas_enviados']:
                            # ENRIQUECIMENTO (Novo)
                            with st.spinner(f"IA Analisando {sinal['jogo']}..."):
                                home_id = j['teams']['home']['id']; away_id = j['teams']['away']['id']
                                h2h_txt = ia_h2h_analise(home_id, away_id, safe_key)
                                big_data = consultar_bigdata_cenario_completo(home_id, away_id)
                                
                                texto_ia, nota, veredicto = gerar_analise_ia_texto(
                                    {'jogo': sinal['jogo'], 'placar': placar, 'tempo': tempo},
                                    sinal['tag'], sinal['stats_visual'], f"{h2h_txt}|{big_data}", safe_key
                                )
                                
                                stake_data = ia_stake_manager(nota, 1.50, banca_atual=st.session_state['banca_atual'])
                                msg_final = formatar_mensagem_hibrida(
                                    sinal, 
                                    {'veredicto': veredicto, 'nota': nota, 'texto': texto_ia, 'consenso': 'Análise Unificada'},
                                    {'odd_atual': '1.50', 'status': 'Estimada'},
                                    stake_data, sinal['stats_visual']
                                )
                                
                                try:
                                    ids = [x.strip() for x in str(safe_chat).replace(';', ',').split(',') if x.strip()]
                                    for cid in ids: requests.post(f"https://api.telegram.org/bot{safe_token}/sendMessage", data={"chat_id": cid, "text": msg_final, "parse_mode": "HTML"})
                                    st.session_state['alertas_enviados'].add(uid)
                                    
                                    # Salva
                                    item_h = {
                                        "FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'),
                                        "Liga": j['league']['name'], "Jogo": sinal['jogo'], "Placar_Sinal": placar,
                                        "Estrategia": sinal['tag'], "Resultado": "Pendente", "Opiniao_IA": veredicto
                                    }
                                    adicionar_historico(item_h)
                                except: pass

        c1, c2 = st.columns(2)
        c1.metric("Jogos Monitorados", len(radar_visual))
        st.dataframe(pd.DataFrame(radar_visual), use_container_width=True)

        for i in range(INTERVALO, 0, -1):
            st.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()

else:
    with placeholder_root.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera. Configure na lateral.")
        if 'historico_sinais' in st.session_state and st.session_state['historico_sinais']:
            st.write("---")
            st.dataframe(pd.DataFrame(st.session_state['historico_sinais']))
