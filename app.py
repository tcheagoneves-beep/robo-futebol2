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

# --- 0. CONFIGURAÇÃO E CSS ---
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")

# --- CONTAINER MESTRE (SOLUÇÃO FINAL DO BUG VISUAL) ---
# Tudo o que é visual será renderizado aqui dentro.
# Isso garante que a tela anterior seja sempre limpa antes da nova.
placeholder_root = st.empty()

# --- CONFIGURAÇÃO DA IA (GEMINI) ---
IA_ATIVADA = False
try:
    if "GEMINI_KEY" in st.secrets:
        genai.configure(api_key=st.secrets["GEMINI_KEY"])
        # ATUALIZADO: Trocado para 'gemini-1.5-flash' para evitar erro 404 e melhorar velocidade
        model_ia = genai.GenerativeModel('gemini-1.5-flash') 
        IA_ATIVADA = True
    else:
        # Adicionado aviso caso a chave não seja encontrada
        st.error("⚠️ Chave GEMINI_KEY não encontrada no secrets.toml!")
except Exception as e:
    # Exibe o erro real em vez de silenciar
    st.error(f"❌ Erro na conexão com Gemini: {e}")
    IA_ATIVADA = False

# --- INICIALIZAÇÃO DE VARIÁVEIS ---
if 'ROBO_LIGADO' not in st.session_state: st.session_state.ROBO_LIGADO = False
if 'last_db_update' not in st.session_state: st.session_state['last_db_update'] = 0
if 'last_static_update' not in st.session_state: st.session_state['last_static_update'] = 0 
if 'bi_enviado_data' not in st.session_state: st.session_state['bi_enviado_data'] = ""
if 'confirmar_reset' not in st.session_state: st.session_state['confirmar_reset'] = False
if 'precisa_salvar' not in st.session_state: st.session_state['precisa_salvar'] = False

# Variáveis de Controle
if 'api_usage' not in st.session_state: st.session_state['api_usage'] = {'used': 0, 'limit': 75000}
if 'data_api_usage' not in st.session_state: st.session_state['data_api_usage'] = datetime.now(pytz.utc).date()
if 'alvos_do_dia' not in st.session_state: st.session_state['alvos_do_dia'] = {}
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'multiplas_enviadas' not in st.session_state: st.session_state['multiplas_enviadas'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'controle_stats' not in st.session_state: st.session_state['controle_stats'] = {}

# CACHE CONFIG
DB_CACHE_TIME = 60   
STATIC_CACHE_TIME = 600 

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
    .metric-sub {font-size: 12px; color: #cccccc; margin-top: 5px;}
    
    .status-active { background-color: #1F4025; color: #00FF00; border: 1px solid #00FF00; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 15px; font-weight: bold;}
    .status-error { background-color: #3B1010; color: #FF4B4B; border: 1px solid #FF4B4B; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 15px; font-weight: bold;}
    
    .stButton button {
        width: 100%; height: 50px !important; font-size: 16px !important; font-weight: bold !important;
        background-color: #262730; border: 1px solid #4e4e4e; color: white;
    }
    .stButton button:hover { border-color: #00FF00; color: #00FF00; }
    
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
        nivel_h = min(int(rh), max_blocos)
        nivel_a = min(int(ra), max_blocos)
        
        if nivel_h > nivel_a:
            barra = "🟩" * nivel_h + "⬜" * (max_blocos - nivel_h)
            return f"\n📊 Pressão: {barra} (Casa)"
        elif nivel_a > nivel_h:
            barra = "🟥" * nivel_a + "⬜" * (max_blocos - nivel_a)
            return f"\n📊 Pressão: {barra} (Visitante)"
        elif nivel_h > 0 and nivel_a > 0:
            return f"\n📊 Pressão: 🟨🟨 Jogo Aberto"
        else:
            return ""
    except: return ""

def consultar_ia_gemini(dados_jogo, estrategia):
    """
    Função que envia o contexto do jogo para a IA e pede uma validação estratégica.
    """
    if not IA_ATIVADA: return ""
    try:
        # Prompt de Engenharia para o Gemini
        prompt = f"""
        Aja como um analista profissional de futebol e apostas esportivas.
        Analise o seguinte cenário de jogo AO VIVO:
        
        PARTIDA: {dados_jogo['jogo']}
        CAMPEONATO: {dados_jogo['liga']}
        MOMENTO: {dados_jogo['tempo']}' do jogo.
        PLACAR ATUAL: {dados_jogo['placar']}
        
        ESTATÍSTICAS TÉCNICAS:
        {dados_jogo['stats']}
        
        O ROBÔ INDICOU A ESTRATÉGIA: "{estrategia}"
        
        Sua tarefa: Avalie se essa entrada faz sentido com base nos números apresentados.
        Considere pressão, finalizações e o tempo de jogo.
        
        Responda em UMA ÚNICA FRASE curta e direta (máx 15 palavras) dando seu veredito e o motivo principal.
        Exemplo: "Aprovado, o time da casa está amassando." ou "Arriscado, jogo muito travado no meio campo."
        """
        response = model_ia.generate_content(prompt)
        return f"\n🤖 <b>IA:</b> {response.text.strip()}"
    except:
        return ""

# --- 3. BANCO DE DADOS ---
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
        if nome_aba == "Historico" and df_para_salvar.empty:
            return False
        conn.update(worksheet=nome_aba, data=df_para_salvar)
        return True
    except: return False

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

    st.session_state['last_db_update'] = now

def adicionar_historico(item):
    if 'historico_full' not in st.session_state:
        st.session_state['historico_full'] = carregar_aba("Historico", COLS_HIST)
    
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
    except: return
    
    hoje = pd.to_datetime(get_time_br().date())
    mask_mes = df['Data_DT'] >= (hoje - timedelta(days=30))
    t_a = len(df)
    g_a = df['Resultado'].str.contains('GREEN').sum()
    w_a = (g_a/t_a*100) if t_a>0 else 0

    if token and chat_ids:
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(7, 4))
        stats = df[mask_mes][df[mask_mes]['Resultado'].isin(['✅ GREEN', '❌ RED'])]
        if not stats.empty:
            c = stats.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)
            c.plot(kind='bar', stacked=True, color=['#00FF00', '#FF0000'], ax=ax, width=0.6)
            ax.set_title(f'PERFORMANCE 30 DIAS', color='white', fontsize=12)
            ax.legend(title='', frameon=False)
            plt.tight_layout()
            buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=100, facecolor='#0E1117'); buf.seek(0)
            msg = f"📊 <b>RELATÓRIO BI</b>\n\n♾️ <b>TOTAL:</b> {t_a} (WR: {w_a:.1f}%)"
            ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
            for cid in ids:
                buf.seek(0); _worker_telegram_photo(token, cid, buf, msg)
            plt.close(fig)

def verificar_automacao_bi(token, chat_ids):
    agora = get_time_br()
    if agora.hour == 23 and agora.minute >= 30:
        hoje_str = agora.strftime('%Y-%m-%d')
        if st.session_state.get('bi_enviado_data') != hoje_str:
            enviar_relatorio_bi(token, chat_ids)
            st.session_state['bi_enviado_data'] = hoje_str
            st.toast("Relatório Automático Enviado!")

def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br()
    hoje_str = agora.strftime('%Y-%m-%d')
    chave = f'alerta_matinal_{hoje_str}'
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
    
    top = sorted(stats_ids.items(), key=lambda x: x[1]['greens'], reverse=True)[:10]
    ids_top = [x[0] for x in top]
    jogos = buscar_agenda_cached(api_key, hoje_str)
    if not jogos: return
    
    if 'alvos_do_dia' not in st.session_state: st.session_state['alvos_do_dia'] = {}
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
                if m_wr > 60: st.session_state['alvos_do_dia'][foco] = m_strat
                matches.append(f"⏰ {j['fixture']['date'][11:16]} | {j['league']['country']} {j['league']['name']}\n⚽ {t1} 🆚 {t2}\n{txt}")
        except: pass
    if matches:
        msg = "🌅 <b>BOM DIA! RADAR DE OPORTUNIDADES</b>\n\n" + "\n\n".join(matches) + "\n\n⚠️ <i>Dica: Se o robô mandar o sinal sugerido acima, a chance de Green é estatisticamente maior!</i> 🚀"
        enviar_telegram(token, chat_ids, msg)
    st.session_state[chave] = True 

def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0
    ga = jogo_api['goals']['away'] or 0
    st_short = jogo_api['fixture']['status']['short']
    STRATS_HT_ONLY = ["Gol Relâmpago", "Massacre", "Choque Líderes", "Briga de Rua"]
    
    if "Múltipla" in sinal['Estrategia']: return False
    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: return False

    fid = clean_fid(sinal['FID'])
    strat = str(sinal['Estrategia'])
    key_green = f"RES_GREEN_{fid}_{strat}"
    key_red = f"RES_RED_{fid}_{strat}"

    if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()

    if (gh+ga) > (ph+pa):
        if "Morno" in sinal['Estrategia']: 
            if (gh+ga) >= 2: # CORREÇÃO: Só dá RED se a soma de gols for 2 ou mais
                sinal['Resultado'] = '❌ RED'
                if key_red not in st.session_state['alertas_enviados']:
                    enviar_telegram(token, chats, f"❌ <b>RED | OVER 1.5 BATIDO</b>\n⚽ {sinal['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {sinal['Estrategia']}")
                    st.session_state['alertas_enviados'].add(key_red)
                    st.session_state['precisa_salvar'] = True
                return True
            else:
                return False 
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
            
            if (minutos_passados >= 3 and not s['Odd_Atualizada']) or (str(s['Odd']) == "0.00") or (str(s['Odd']) == "1.10"):
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
            if processar_resultado(s, jogo_encontrado, token, chats): 
                updates_buffer.append(s)
            
    if updates_buffer: atualizar_historico_ram(updates_buffer)

# --- CORREÇÃO DO VAR DUPLICADO (ADD CHAVE ÚNICA) ---
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
                    # CHAVE ÚNICA PARA O VAR
                    key_var = f"VAR_{fid}_{s['Estrategia']}_{gh}x{ga}"
                    
                    s['Resultado'] = 'Pendente'
                    st.session_state['precisa_salvar'] = True
                    updates_buffer.append(s)
                    
                    # SÓ ENVIA SE AINDA NÃO ENVIOU ESTE VAR ESPECÍFICO
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
    if tempo <= 30 and (gh + ga) >= 2: return True
    if 70 <= tempo <= 85 and abs(gh - ga) <= 1: return True
    if tempo <= 60 and abs(gh - ga) <= 1: return True
    if status == 'HT' and gh == 0 and ga == 0: return True
    return False

# --- FUNÇÃO AUXILIAR DE MULTI-THREADING (TURBO) ---
def fetch_stats_single(fid, api_key):
    try:
        url = "https://v3.football.api-sports.io/fixtures/statistics"
        r = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fid}, timeout=3)
        return fid, r.json().get('response', []), r.headers
    except:
        return fid, [], None

def atualizar_stats_em_paralelo(jogos_alvo, api_key):
    resultados = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {}
        for j in jogos_alvo:
            futures[executor.submit(fetch_stats_single, j['fixture']['id'], api_key)] = j
            time.sleep(0.2) # Respiro para a API
        
        for future in as_completed(futures):
            fid, stats, headers = future.result()
            if stats:
                resultados[fid] = stats
                update_api_usage(headers)
    return resultados

# --- DECISÃO DO ROBÔ ---
def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        tot_chutes = sh_h + sh_a; tot_gol = sog_h + sog_a
        txt_stats = f"{tot_chutes} Chutes (🎯 {tot_gol} no Gol)"
    except: return []
    fid = j['fixture']['id']; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
    rh, ra = momentum(fid, sog_h, sog_a)
    SINAIS = []
    
    if tempo <= 30 and (gh+ga) >= 2: 
        SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": "🔥 Over Gols (Tendência de Goleada)", "stats": txt_stats, "rh": rh, "ra": ra})

    if (gh + ga) == 0:
        if (tempo <= 2 and (sog_h + sog_a) >= 1) or (tempo <= 10 and (sh_h + sh_a) >= 2):
            SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": "Over 0.5 HT (Entrar para sair gol no 1º tempo)", "stats": txt_stats, "rh": rh, "ra": ra})

    if 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(gh-ga) <= 1: 
        SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": "Over Gols (Gol no final - Limite)", "stats": txt_stats, "rh": rh, "ra": ra})

    if tempo <= 60:
        if gh <= ga and (rh >= 2 or sh_h >= 8): SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": "Over Gols (Gol maduro na partida)", "stats": f"Pressão: {rh}", "rh": rh, "ra": ra})
        if ga <= gh and (ra >= 2 or sh_a >= 8): SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": "Over Gols (Gol maduro na partida)", "stats": f"Pressão: {ra}", "rh": rh, "ra": ra})

    if rank_home and rank_away:
        is_top_home = rank_home <= 4; is_top_away = rank_away <= 4; is_bot_home = rank_home >= 11; is_bot_away = rank_away >= 11; is_mid_home = rank_home >= 5; is_mid_away = rank_away >= 5
        if (is_top_home and is_bot_away) or (is_top_away and is_bot_home):
            if tempo <= 5 and (sh_h + sh_a) >= 1: SINAIS.append({"tag": "🔥 Massacre", "ordem": "Over 0.5 HT (Favorito deve abrir placar)", "stats": f"Rank: {rank_home}x{rank_away}", "rh": rh, "ra": ra})
        if 5 <= tempo <= 15:
            if is_top_home and (rh >= 2 or sh_h >= 3): SINAIS.append({"tag": "🦁 Favorito", "ordem": "Over Gols (Partida)", "stats": f"Pressão: {rh}", "rh": rh, "ra": ra})
            if is_top_away and (ra >= 2 or sh_a >= 3): SINAIS.append({"tag": "🦁 Favorito", "ordem": "Over Gols (Partida)", "stats": f"Pressão: {ra}", "rh": rh, "ra": ra})
        if is_top_home and is_top_away and tempo <= 7:
            if (sh_h + sh_a) >= 2 and (sog_h + sog_a) >= 1: SINAIS.append({"tag": "⚔️ Choque Líderes", "ordem": "Over 0.5 HT (Jogo intenso)", "stats": txt_stats, "rh": rh, "ra": ra})
        if is_mid_home and is_mid_away:
            if tempo <= 7 and 2 <= (sh_h + sh_a) <= 3: SINAIS.append({"tag": "🥊 Briga de Rua", "ordem": "Over 0.5 HT (Trocação franca)", "stats": txt_stats, "rh": rh, "ra": ra})
            is_bot_home_morno = rank_home >= 10; is_bot_away_morno = rank_away >= 10
            if is_bot_home_morno and is_bot_away_morno:
                if 15 <= tempo <= 16 and (sh_h + sh_a) == 0: SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": "Under 1.5 HT (Apostar que NÃO saem 2 gols no 1º tempo)", "stats": "0 Chutes (Times Z-4)", "rh": rh, "ra": ra})
    
    if 75 <= tempo <= 85 and abs(gh - ga) <= 1:
        if (sh_h + sh_a) >= 16 and (sog_h + sog_a) >= 8: SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": "Gol no Final (Over Limit) (Aposta seca que sai mais um gol)", "stats": "🔥 Pressão Máxima", "rh": rh, "ra": ra})
    
    return SINAIS

def resetar_sistema_completo():
    st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
    st.session_state['historico_sinais'] = []
    st.session_state['df_black'] = pd.DataFrame(columns=COLS_BLACK)
    st.session_state['df_safe'] = pd.DataFrame(columns=COLS_SAFE)
    st.session_state['df_vip'] = pd.DataFrame(columns=COLS_OBS)
    st.session_state['alvos_do_dia'] = {}
    st.session_state['alertas_enviados'] = set()
    st.session_state['multiplas_enviadas'] = set()
    st.session_state['memoria_pressao'] = {}
    st.session_state['controle_stats'] = {}
    
    try:
        conn.clear(worksheet="Historico"); salvar_aba("Historico", st.session_state['historico_full'])
        conn.clear(worksheet="Blacklist"); salvar_aba("Blacklist", st.session_state['df_black'])
        conn.clear(worksheet="Seguras"); salvar_aba("Seguras", st.session_state['df_safe'])
        conn.clear(worksheet="Obs"); salvar_aba("Obs", st.session_state['df_vip'])
    except:
        salvar_aba("Historico", st.session_state['historico_full'])
        salvar_aba("Blacklist", st.session_state['df_black'])
        salvar_aba("Seguras", st.session_state['df_safe'])
        salvar_aba("Obs", st.session_state['df_vip'])

    st.cache_data.clear()
    st.toast("♻️ SISTEMA COMPLETAMENTE RESETADO!")

# --- SIDEBAR E LAYOUT ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=True):
        API_KEY = st.text_input("Chave API:", type="password")
        TG_TOKEN = st.text_input("Token Telegram:", type="password")
        TG_CHAT = st.text_input("Chat IDs:")
        INTERVALO = st.slider("Ciclo (s):", 60, 300, 60) 
        if st.button("🧹 Limpar Cache"): 
            st.cache_data.clear(); carregar_tudo(force=True); st.session_state['last_db_update'] = 0; st.toast("Cache Limpo!")
        st.write("---")
        if st.button("📊 Enviar Relatório BI"): enviar_relatorio_bi(TG_TOKEN, TG_CHAT); st.toast("Relatório Enviado!")
        if st.button("💰 Enviar Relatório Financeiro"):
            if 'last_fin_stats' in st.session_state:
                s = st.session_state['last_fin_stats']
                enviar_relatorio_financeiro(TG_TOKEN, TG_CHAT, s['cenario'], s['lucro'], s['roi'], s['entradas'])
                st.toast("Relatório Financeiro Enviado!")
            else:
                st.error("Abra a aba Financeiro primeiro.")

    with st.expander("💰 Gestão de Banca", expanded=False):
        stake_padrao = st.number_input("Valor da Entrada (R$)", value=10.0, step=5.0)
        banca_inicial = st.number_input("Banca Inicial (R$)", value=100.0, step=50.0)
        
    with st.expander("📶 Consumo API", expanded=False):
        verificar_reset_diario()
        u = st.session_state['api_usage']; perc = min(u['used'] / u['limit'], 1.0) if u['limit'] > 0 else 0
        st.progress(perc); st.caption(f"Utilizado: **{u['used']}** / {u['limit']}")
    
    st.write("---")
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    
    st.markdown("---")
    st.markdown("### ⚠️ Zona de Perigo")
    if st.button("☢️ ZERAR ROBÔ", type="primary", use_container_width=True): st.session_state['confirmar_reset'] = True
    if st.session_state.get('confirmar_reset'):
        st.error("Tem certeza? Isso apaga TODO o histórico do Google Sheets.")
        c1, c2 = st.columns(2)
        if c1.button("✅ SIM"): resetar_sistema_completo(); st.session_state['confirmar_reset'] = False; st.rerun()
        if c2.button("❌ NÃO"): st.session_state['confirmar_reset'] = False; st.rerun()

# --- LOOP PRINCIPAL (VISUALIZAÇÃO CONTROLADA) ---
# A MÁGICA ACONTECE AQUI:
# O 'placeholder_root' é o único lugar onde o app desenha.
# A cada rerun, ele limpa e desenha de novo, impedindo duplicação.

placeholder_root.empty() # Limpa o frame anterior

if st.session_state.ROBO_LIGADO:
    # --- MODO: ROBÔ RODANDO ---
    with placeholder_root.container():
        carregar_tudo()
        verificar_automacao_bi(TG_TOKEN, TG_CHAT)
        verificar_alerta_matinal(TG_TOKEN, TG_CHAT, API_KEY)
        
        # Lógica invisível (não desenha nada, só processa)
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
            resp = requests.get(url, headers={"x-apisports-key": API_KEY}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
            update_api_usage(resp.headers); res = resp.json()
            jogos_live = res.get('response', []) if not res.get('errors') else []; api_error = bool(res.get('errors'))
            if api_error and "errors" in res: st.error(f"Detalhe do Erro: {res['errors']}")
        except Exception as e: jogos_live = []; api_error = True; st.error(f"Erro de Conexão: {e}")

        if not api_error: 
            check_green_red_hibrido(jogos_live, TG_TOKEN, TG_CHAT, API_KEY)
            verificar_var_rollback(jogos_live, TG_TOKEN, TG_CHAT)

        radar = []; agenda = []
        
        if not api_error:
            # Processamento de Stats (Multi-thread controlada)
            jogos_para_baixar = []
            for j in jogos_live:
                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                if lid in ids_black: continue
                tempo = j['fixture']['status']['elapsed'] or 0; st_short = j['fixture']['status']['short']
                gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
                t_esp = 60 if (69<=tempo<=76) else (90 if tempo<=15 else 180)
                ult_chk = st.session_state['controle_stats'].get(fid, datetime.min)
                if deve_buscar_stats(tempo, gh, ga, st_short):
                    if (datetime.now() - ult_chk).total_seconds() > t_esp: jogos_para_baixar.append(j)

            if jogos_para_baixar:
                novas_stats = atualizar_stats_em_paralelo(jogos_para_baixar, API_KEY)
                for fid, stats in novas_stats.items():
                    st.session_state['controle_stats'][fid] = datetime.now()
                    st.session_state[f"st_{fid}"] = stats

            candidatos_multipla = []; ids_no_radar = []
            
            # Loop Principal de Análise
            for j in jogos_live:
                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
                if lid in ids_black: continue
                
                nome_liga_show = j['league']['name']
                if lid in ids_safe: nome_liga_show += " 🛡️"
                elif lid in df_obs['id'].values: nome_liga_show += " ⚠️"
                else: nome_liga_show += " ❓" 
                ids_no_radar.append(fid)
                
                tempo = j['fixture']['status']['elapsed'] or 0; st_short = j['fixture']['status']['short']
                home = j['teams']['home']['name']; away = j['teams']['away']['name']
                placar = f"{j['goals']['home']}x{j['goals']['away']}"; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
                
                stats = st.session_state.get(f"st_{fid}", [])
                status_vis = "👁️" if stats else "💤"
                
                rank_h = None; rank_a = None
                if j['league']['id'] in LIGAS_TABELA:
                    rk = buscar_ranking(API_KEY, j['league']['id'], j['league']['season'])
                    rank_h = rk.get(home); rank_a = rk.get(away)
                
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
                
                if lista_sinais:
                    status_vis = f"✅ {len(lista_sinais)} Sinais"
                    for s in lista_sinais:
                        rh = s.get('rh', 0); ra = s.get('ra', 0)
                        txt_pressao = gerar_barra_pressao(rh, ra) 

                        uid_normal = f"{fid}_{s['tag']}"
                        uid_super = f"SUPER_ODD_{fid}_{s['tag']}"
                        
                        odd_atual_str = get_live_odds(fid, API_KEY, s['tag'], gh+ga)
                        try: odd_val = float(odd_atual_str)
                        except: odd_val = 0.0

                        if uid_normal not in st.session_state['alertas_enviados']:
                            destaque_odd = ""
                            if odd_val >= 1.80:
                                destaque_odd = "\n💎 <b>SUPER ODD DETECTADA! (EV+)</b>"
                                st.session_state['alertas_enviados'].add(uid_super)
                            
                            st.session_state['alertas_enviados'].add(uid_normal)
                            item = {"FID": fid, "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Placar_Sinal": placar, "Estrategia": s['tag'], "Resultado": "Pendente", "HomeID": str(j['teams']['home']['id']) if lid in ids_safe else "", "AwayID": str(j['teams']['away']['id']) if lid in ids_safe else "", "Odd": odd_atual_str, "Odd_Atualizada": ""}
                            
                            if adicionar_historico(item):
                                prob = buscar_inteligencia(s['tag'], j['league']['name'], f"{home} x {away}")
                                
                                opiniao_ia = ""
                                if IA_ATIVADA:
                                    dados_ia = {'jogo': f"{home} x {away}", 'liga': j['league']['name'], 'tempo': tempo, 'placar': placar, 'stats': s['stats']}
                                    opiniao_ia = consultar_ia_gemini(dados_ia, s['tag'])

                                msg = f"<b>🚨 SINAL ENCONTRADO 🚨</b>\n\n🏆 <b>{j['league']['name']}</b>\n⚽ {home} 🆚 {away}\n⏰ <b>{tempo}' minutos</b> (Placar: {placar})\n\n🔥 {s['tag'].upper()}\n⚠️ <b>AÇÃO:</b> {s['ordem']}{destaque_odd}\n\n💰 <b>Odd: @{odd_atual_str}</b>{txt_pressao}\n📊 <i>Dados: {s['stats']}</i>{prob}{opiniao_ia}"
                                enviar_telegram(TG_TOKEN, TG_CHAT, msg)
                                st.toast(f"Sinal: {s['tag']}")

                        elif uid_super not in st.session_state['alertas_enviados'] and odd_val >= 1.80:
                            st.session_state['alertas_enviados'].add(uid_super)
                            msg_super = (f"💎 <b>OPORTUNIDADE DE VALOR!</b>\n\n⚽ {home} 🆚 {away}\n📈 <b>A Odd subiu!</b> Entrada valorizada.\n🔥 <b>Estratégia:</b> {s['tag']}\n💰 <b>Nova Odd: @{odd_atual_str}</b>\n<i>O jogo mantém o padrão da estratégia.</i>{txt_pressao}")
                            enviar_telegram(TG_TOKEN, TG_CHAT, msg_super)
                            st.toast(f"💎 Odd Subiu: {s['tag']}")
                
                radar.append({"Liga": nome_liga_show, "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})

            if candidatos_multipla:
                novos = [c for c in candidatos_multipla if c['fid'] not in st.session_state['multiplas_enviadas']]
                if novos:
                    msg = "<b>🚀 OPORTUNIDADE DE MÚLTIPLA (HT) 🚀</b>\n" + "".join([f"\n⚽ {c['jogo']} ({c['stats']})\n⚠️ AÇÃO: {c['indica']}" for c in novos])
                    for c in novos: st.session_state['multiplas_enviadas'].add(c['fid'])
                    enviar_telegram(TG_TOKEN, TG_CHAT, msg)

            prox = buscar_agenda_cached(API_KEY, hoje_real); agora = get_time_br()
            for p in prox:
                try:
                    if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] in ['NS', 'TBD'] and p['fixture']['id'] not in ids_no_radar:
                        if datetime.fromisoformat(p['fixture']['date']) > agora:
                            l_id = normalizar_id(p['league']['id']); l_nm = p['league']['name']
                            if l_id in ids_safe: l_nm += " 🛡️"
                            elif l_id in df_obs['id'].values: l_nm += " ⚠️"
                            agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": l_nm, "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
                except: pass

        if st.session_state.get('precisa_salvar') and 'historico_full' in st.session_state:
            df_memoria = st.session_state['historico_full']
            if not df_memoria.empty:
                sucesso = salvar_aba("Historico", df_memoria)
                if sucesso:
                    st.session_state['precisa_salvar'] = False
                    st.toast("💾 Dados salvos com sucesso!")

        # === DESENHO DA TELA (VISUALIZAÇÃO) ===
        if api_error: st.markdown('<div class="status-error">🚨 API LIMITADA - AGUARDE</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        hist_hj = pd.DataFrame(st.session_state['historico_sinais'])
        t, g, r, w = calcular_stats(hist_hj)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t}</div><div class="metric-sub">{g} Green | {r} Red</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(radar)}</div><div class="metric-sub">Monitorando</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Ligas Seguras</div><div class="metric-value">{count_safe}</div><div class="metric-sub">Validadas</div></div>', unsafe_allow_html=True)
        
        st.write("")
        abas = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", f"💰 Financeiro", f"📜 Histórico ({len(hist_hj)})", "📈 BI & Analytics", f"🚫 Blacklist ({len(st.session_state['df_black'])})", f"🛡️ Seguras ({count_safe})", f"⚠️ Obs ({count_obs})"])
        
        with abas[0]: 
            if radar: st.dataframe(pd.DataFrame(radar)[['Liga', 'Jogo', 'Tempo', 'Status']].astype(str), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        with abas[1]: 
            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos futuros hoje.")
        
        with abas[2]:
            st.markdown("### 💰 Evolução Financeira")
            modo_simulacao = st.radio("Cenário de Entrada:", ["Todos os sinais", "Apenas 1 sinal por jogo", "Até 2 sinais por jogo"], horizontal=True)
            df_fin = st.session_state.get('historico_full', pd.DataFrame())
            if not df_fin.empty:
                df_fin = df_fin.copy()
                df_fin['Odd_Num'] = pd.to_numeric(df_fin['Odd'], errors='coerce').fillna(0.0)
                filtro_validas = df_fin['Odd_Num'] > 1.01
                if filtro_validas.any():
                    mapa_medias = df_fin[filtro_validas].groupby('Estrategia')['Odd_Num'].mean().to_dict()
                else: mapa_medias = {}
                df_fin = df_fin[df_fin['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
                df_fin = df_fin.sort_values(by=['FID', 'Hora'], ascending=[True, True])
                if modo_simulacao == "Apenas 1 sinal por jogo": df_fin = df_fin.groupby('FID').head(1)
                elif modo_simulacao == "Até 2 sinais por jogo": df_fin = df_fin.groupby('FID').head(2)
                
                if not df_fin.empty:
                    lucros = []; saldo_atual = banca_inicial; historico_saldo = [banca_inicial]
                    for idx, row in df_fin.iterrows():
                        res = row['Resultado']; odd = row['Odd_Num']; strat = row['Estrategia']
                        if odd <= 1.01: odd = mapa_medias.get(strat, 1.10)
                        if 'GREEN' in res: lucro = (stake_padrao * odd) - stake_padrao
                        else: lucro = -stake_padrao
                        saldo_atual += lucro; lucros.append(lucro); historico_saldo.append(saldo_atual)
                    
                    df_fin['Lucro'] = lucros; total_lucro = sum(lucros); roi = (total_lucro / (len(df_fin) * stake_padrao)) * 100
                    st.session_state['last_fin_stats'] = {'cenario': modo_simulacao, 'lucro': total_lucro, 'roi': roi, 'entradas': len(df_fin)}
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Banca Atual", f"R$ {saldo_atual:.2f}"); m2.metric("Lucro Líquido", f"R$ {total_lucro:.2f}", delta_color="normal")
                    m3.metric("ROI Estimado", f"{roi:.1f}%"); m4.metric("Entradas", len(df_fin))
                    fig_fin = px.line(y=historico_saldo, x=range(len(historico_saldo)), title="Crescimento da Banca")
                    fig_fin.update_layout(xaxis_title="Entradas", yaxis_title="Saldo (R$)", template="plotly_dark"); st.plotly_chart(fig_fin, use_container_width=True)
                    st.caption(f"ℹ️ As odds médias calculadas automaticamente foram: {', '.join([f'{k}: {v:.2f}' for k,v in mapa_medias.items()])}")
                else: st.info("Aguardando fechamento de sinais para calcular financeiro.")
            else: st.info("Sem dados históricos para cálculo.")

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
                    df_bi['Data_Str'] = df_bi['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
                    df_bi['Data_DT'] = pd.to_datetime(df_bi['Data_Str'], errors='coerce')
                    df_bi = df_bi.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
                    hoje_str = get_time_br().strftime('%Y-%m-%d')
                    df_bi_hoje = df_bi[df_bi['Data_Str'] == hoje_str]
                    hoje = pd.to_datetime(get_time_br().date())
                    if 'bi_filter' not in st.session_state: st.session_state['bi_filter'] = "Tudo"
                    filtro = st.selectbox("📅 Período", ["Tudo", "Hoje", "7 Dias", "30 Dias"], key="bi_select")
                    if filtro == "Hoje": df_show = df_bi_hoje
                    elif filtro == "7 Dias": df_show = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=7))]
                    elif filtro == "30 Dias": df_show = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=30))]
                    else: df_show = df_bi 
                    if not df_show.empty:
                        gr = df_show['Resultado'].str.contains('GREEN').sum(); rd = df_show['Resultado'].str.contains('RED').sum()
                        tt = len(df_show); ww = (gr/tt*100) if tt>0 else 0
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Sinais", tt); m2.metric("Greens", gr); m3.metric("Reds", rd); m4.metric("Assertividade", f"{ww:.1f}%")
                        st.divider()
                        st_s = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                        if not st_s.empty:
                            cts = st_s.groupby(['Estrategia', 'Resultado']).size().reset_index(name='Qtd')
                            fig = px.bar(cts, x='Estrategia', y='Qtd', color='Resultado', color_discrete_map={'✅ GREEN': '#00FF00', '❌ RED': '#FF0000'}, title="Performance por Estratégia", text='Qtd')
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
                        st.divider()
                        cb1, cb2 = st.columns(2)
                        with cb1:
                            st.caption("🏆 Melhores Ligas")
                            stats_l = df_show.groupby('Liga')['Resultado'].apply(lambda x: x.str.contains('GREEN').sum()/len(x)*100).reset_index(name='Winrate')
                            cnt_l = df_show['Liga'].value_counts().reset_index(name='Qtd')
                            final_l = stats_l.merge(cnt_l, left_on='Liga', right_on='Liga')
                            st.dataframe(final_l[final_l['Qtd']>=2].sort_values('Winrate', ascending=False).head(5).style.format({'Winrate': '{:.1f}%'}), hide_index=True, use_container_width=True)
                        with cb2:
                            st.caption("⚡ Top Estratégias")
                            stats_e = df_show.groupby('Estrategia')['Resultado'].apply(lambda x: x.str.contains('GREEN').sum()/len(x)*100).reset_index(name='Winrate')
                            st.dataframe(stats_e.sort_values('Winrate', ascending=False).style.format({'Winrate': '{:.1f}%'}), hide_index=True, use_container_width=True)
                        st.divider()
                        st.markdown("### 👑 Reis do Green (Times que mais lucram)")
                        df_g = df_show[df_show['Resultado'].str.contains('GREEN')]
                        lst_t = []
                        for j in df_g['Jogo']:
                            try: p = j.split(' x '); lst_t.extend([p[0].strip(), p[1].strip()])
                            except: pass
                        if lst_t:
                            top_r = pd.Series(lst_t).value_counts().reset_index()
                            top_r.columns = ['Time', 'Qtd Green']
                            st.dataframe(top_r.head(10), use_container_width=True, hide_index=True)
                except Exception as e: st.error(f"Erro ao carregar BI: {e}")

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
            else:
                st.info("Nenhuma liga segura ainda.")

        with abas[7]: 
            df_vip_show = st.session_state.get('df_vip', pd.DataFrame()).copy()
            if not df_vip_show.empty: df_vip_show['Strikes'] = df_vip_show['Strikes'].apply(formatar_inteiro_visual)
            st.dataframe(df_vip_show[['País', 'Liga', 'Data_Erro', 'Strikes']], use_container_width=True, hide_index=True)

        for i in range(INTERVALO, 0, -1):
            st.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
        st.rerun()

else:
    # --- MODO DESLIGADO (DENTRO DO CONTAINER MESTRE TAMBÉM) ---
    # Assim evita que a tela de espera "vaze" para baixo do robô
    with placeholder_root.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera. Configure na lateral.")
