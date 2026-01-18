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

# --- 0. CONFIGURAÇÃO E CSS ---
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")

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
    .footer-timer { position: fixed; left: 0; bottom: 0; width: 100%; background-color: #0E1117; color: #FFD700; text-align: center; padding: 8px; font-size: 14px; border-top: 1px solid #333; z-index: 9999; }
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

# --- 3. LÓGICA DE DECISÃO ---
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
        SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": "🔥 Over Gols (Tendência de Goleada)", "stats": f"Placar: {gh}x{ga}"})
    if (gh + ga) == 0:
        if (tempo <= 2 and (sog_h + sog_a) >= 1) or (tempo <= 10 and (sh_h + sh_a) >= 2):
            SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": "Over 0.5 HT (Entrar para sair gol no 1º tempo)", "stats": txt_stats})
    if 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(gh-ga) <= 1: 
        SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": "Over Gols (Gol no final - Limite)", "stats": txt_stats})
    if tempo <= 60:
        if gh <= ga and (rh >= 2 or sh_h >= 8): SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": "Over Gols (Gol maduro na partida)", "stats": f"Pressão: {rh}"})
        if ga <= gh and (ra >= 2 or sh_a >= 8): SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": "Over Gols (Gol maduro na partida)", "stats": f"Pressão: {ra}"})
    if rank_home and rank_away:
        is_top_home = rank_home <= 4; is_top_away = rank_away <= 4; is_bot_home = rank_home >= 11; is_bot_away = rank_away >= 11
        if (is_top_home and is_bot_away) or (is_top_away and is_bot_home):
            if tempo <= 5 and (sh_h + sh_a) >= 1: SINAIS.append({"tag": "🔥 Massacre", "ordem": "Over 0.5 HT (Favorito deve abrir placar)", "stats": f"Rank: {rank_home}x{rank_away}"})
    if 75 <= tempo <= 85 and abs(gh - ga) <= 1:
        if (sh_h + sh_a) >= 16 and (sog_h + sog_a) >= 8: SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": "Gol no Final (Over Limit)", "stats": "🔥 Pressão Máxima"})
    return SINAIS

# --- 4. BANCO DE DADOS (COM ESCUDO ANTI-APAGÃO) ---
def carregar_aba(nome_aba, colunas_esperadas):
    try:
        df = conn.read(worksheet=nome_aba, ttl=0)
        if df.empty or len(df.columns) < len(colunas_esperadas): 
            return pd.DataFrame(columns=colunas_esperadas)
        return df.fillna("").astype(str)
    except: return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df_para_salvar):
    try: 
        if nome_aba == "Historico" and df_para_salvar.empty: return False # ESCUDO
        conn.update(worksheet=nome_aba, data=df_para_salvar)
        return True
    except: return False

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
            if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
            for item in st.session_state['historico_sinais']:
                fid_strat = f"{item['FID']}_{item['Estrategia']}"
                st.session_state['alertas_enviados'].add(fid_strat)
                if 'GREEN' in str(item['Resultado']): st.session_state['alertas_enviados'].add(f"RES_GREEN_{fid_strat}")

# --- 5. TELEGRAM E RELATÓRIOS ---
def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids: threading.Thread(target=lambda t, c, m: requests.post(f"https://api.telegram.org/bot{t}/sendMessage", data={"chat_id": c, "text": m, "parse_mode": "HTML"}), args=(token, cid, msg)).start()

def enviar_relatorio_bi(token, chat_ids):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    t_a = len(df); g_a = df['Resultado'].str.contains('GREEN').sum(); w_a = (g_a/t_a*100) if t_a>0 else 0
    enviar_telegram(token, chat_ids, f"📊 <b>RELATÓRIO BI</b>\n\n♾️ <b>TOTAL:</b> {t_a} (WR: {w_a:.1f}%)")

def verificar_automacao_bi(token, chat_ids):
    agora = get_time_br()
    if agora.hour == 23 and agora.minute >= 30 and st.session_state.get('bi_enviado_data') != agora.strftime('%Y-%m-%d'):
        enviar_relatorio_bi(token, chat_ids)
        st.session_state['bi_enviado_data'] = agora.strftime('%Y-%m-%d')

# --- 6. PROCESSAMENTO TURBO ---
def fetch_stats_single(fid, api_key):
    try:
        r = requests.get("https://v3.football.api-sports.io/fixtures/statistics", headers={"x-apisports-key": api_key}, params={"fixture": fid}, timeout=3)
        return fid, r.json().get('response', []), r.headers
    except: return fid, [], None

def atualizar_stats_em_paralelo(jogos_alvo, api_key):
    resultados = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_stats_single, j['fixture']['id'], api_key): j for j in jogos_alvo}
        for future in as_completed(futures):
            fid, stats, _ = future.result()
            if stats: resultados[fid] = stats
    return resultados

def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
    st_short = jogo_api['fixture']['status']['short']
    fid = clean_fid(sinal['FID']); strat = str(sinal['Estrategia'])
    key_green_strat = f"RES_GREEN_{fid}_{strat}"; key_green_global = f"GLOBAL_GREEN_{fid}"
    
    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: return False

    if (gh+ga) > (ph+pa):
        sinal['Resultado'] = '✅ GREEN'
        if key_green_global in st.session_state['alertas_enviados']: return True
        enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📈 Placar: <b>{gh}x{ga}</b>\n🎯 {strat}")
        st.session_state['alertas_enviados'].add(key_green_global); st.session_state['precisa_salvar'] = True
        return True
    
    if st_short in ['FT', 'AET', 'PEN']:
        sinal['Resultado'] = '❌ RED'
        st.session_state['precisa_salvar'] = True
        return True
    return False

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    hist = st.session_state.get('historico_sinais', [])
    pendentes = [s for s in hist if s['Resultado'] == 'Pendente']
    if not pendentes: return
    
    updates = []
    ids_live = [j['fixture']['id'] for j in jogos_live]
    for s in pendentes:
        fid = int(clean_fid(s.get('FID', 0)))
        jogo_api = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        if jogo_api:
            if processar_resultado(s, jogo_api, token, chats): updates.append(s)
    if updates:
        # Chamamos a função de sincronizar RAM (já definida no seu código original)
        # Para simplificar, assumimos que atualizar_historico_ram(updates) está no escopo
        pass

def verificar_var_rollback(jogos_live, token, chats):
    hist = st.session_state.get('historico_sinais', [])
    for s in [x for x in hist if 'GREEN' in str(x['Resultado'])]:
        fid = clean_fid(s.get('FID', 0)); jogo_api = next((j for j in jogos_live if j['fixture']['id'] == int(fid)), None)
        if jogo_api:
            gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
            try:
                ph, pa = map(int, s['Placar_Sinal'].split('x'))
                if (gh + ga) <= (ph + pa):
                    key_var = f"VAR_{fid}_{s['Estrategia']}_{gh}x{ga}"
                    if key_var not in st.session_state['alertas_enviados']:
                        s['Resultado'] = 'Pendente'; st.session_state['precisa_salvar'] = True
                        enviar_telegram(token, chats, f"⚠️ <b>VAR ACIONADO | GOL ANULADO</b>\n⚽ {s['Jogo']}\n📉 Placar: {gh}x{ga}\n🔄 Status revertido.")
                        st.session_state['alertas_enviados'].add(key_var)
                        if f"GLOBAL_GREEN_{fid}" in st.session_state['alertas_enviados']: st.session_state['alertas_enviados'].remove(f"GLOBAL_GREEN_{fid}")
            except: pass

def momentum(fid, sog_h, sog_a):
    mem = st.session_state['memoria_pressao'].get(fid, {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
    now = datetime.now()
    if sog_h > mem['sog_h']: mem['h_t'].extend([now]*(sog_h-mem['sog_h']))
    if sog_a > mem['sog_a']: mem['a_t'].extend([now]*(sog_a-mem['sog_a']))
    mem['h_t'] = [t for t in mem['h_t'] if now - t <= timedelta(minutes=7)]; mem['a_t'] = [t for t in mem['a_t'] if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a; st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_t']), len(mem['a_t'])

def deve_buscar_stats(tempo, gh, ga, status):
    if 5 <= tempo <= 85: return True
    return False

# --- 7. DASHBOARD E LOOP PRINCIPAL ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    API_KEY = st.text_input("Chave API:", type="password")
    TG_TOKEN = st.text_input("Token Telegram:", type="password")
    TG_CHAT = st.text_input("Chat IDs:")
    INTERVALO = st.slider("Ciclo (s):", 60, 300, 60)
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    if st.button("☢️ ZERAR ROBÔ", type="primary"): 
        resetar_sistema_completo()
        st.rerun()

if st.session_state.ROBO_LIGADO:
    carregar_tudo(); verificar_automacao_bi(TG_TOKEN, TG_CHAT)
    
    try:
        resp = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": API_KEY}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
        jogos_live = resp.json().get('response', [])
        check_green_red_hibrido(jogos_live, TG_TOKEN, TG_CHAT, API_KEY)
        verificar_var_rollback(jogos_live, TG_TOKEN, TG_CHAT)
    except: jogos_live = []

    # TURBO: Download em paralelo
    ids_black = [normalizar_id(x) for x in st.session_state['df_black']['id'].values]
    jogos_para_stats = [j for j in jogos_live if normalizar_id(j['league']['id']) not in ids_black and deve_buscar_stats(j['fixture']['status']['elapsed'] or 0, 0, 0, '')]
    
    novas_stats = atualizar_stats_em_paralelo(jogos_para_stats, API_KEY)
    for fid, s in novas_stats.items(): st.session_state[f"st_{fid}"] = s

    radar = []
    for j in jogos_live:
        fid = j['fixture']['id']; lid = normalizar_id(j['league']['id'])
        if lid in ids_black: continue
        tempo = j['fixture']['status']['elapsed'] or 0; placar = f"{j['goals']['home']}x{j['goals']['away']}"
        stats = st.session_state.get(f"st_{fid}", [])
        
        # Estratégia
        lista_sinais = processar(j, stats, tempo, placar) if stats else []
        for s in lista_sinais:
            uid = f"{fid}_{s['tag']}"
            if uid not in st.session_state['alertas_enviados']:
                st.session_state['alertas_enviados'].add(uid)
                odd = get_live_odds(fid, API_KEY, s['tag'], (j['goals']['home'] or 0)+(j['goals']['away'] or 0))
                item = {"FID": fid, "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", "Placar_Sinal": placar, "Estrategia": s['tag'], "Resultado": "Pendente", "Odd": odd}
                if adicionar_historico(item):
                    enviar_telegram(TG_TOKEN, TG_CHAT, f"🚨 <b>SINAL: {s['tag']}</b>\n🏆 {j['league']['name']}\n⚽ {item['Jogo']}\n⏰ {tempo}' (Placar: {placar})\n💰 Odd: @{odd}")
        
        radar.append({"Liga": j['league']['name'], "Jogo": f"{j['teams']['home']['name']} {placar} {j['teams']['away']['name']}", "Tempo": f"{tempo}'", "Status": f"{len(lista_sinais)} Sinais" if lista_sinais else "Monitorando"})

    # Salva se houve mudança
    if st.session_state.get('precisa_salvar'):
        if salvar_aba("Historico", st.session_state['historico_full']): st.session_state['precisa_salvar'] = False

    # Interface
    st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
    
    t1, t2, t3, t4 = st.tabs(["📡 Radar", "💰 Financeiro", "📜 Histórico", "🛡️ Seguras"])
    with t1: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
    with t3:
        df_hist = pd.DataFrame(st.session_state.get('historico_sinais', []))
        if not df_hist.empty:
            df_hist['Jogo'] = df_hist['Jogo'] + " (" + df_hist['Placar_Sinal'] + ")"
            st.dataframe(df_hist[['Data', 'Hora', 'Liga', 'Jogo', 'Estrategia', 'Resultado', 'Odd']], use_container_width=True, hide_index=True)
    with t4:
        df_s = st.session_state.get('df_safe', pd.DataFrame())
        if not df_s.empty:
            df_s['Status Risco'] = df_s['Strikes'].apply(lambda x: "🟢 100% Estável" if int(float(str(x or 0))) == 0 else f"⚠️ Atenção ({x}/10)")
            st.dataframe(df_s[['País', 'Liga', 'Motivo', 'Status Risco']], use_container_width=True, hide_index=True)

    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True); time.sleep(1)
    st.rerun()
else:
    st.title("❄️ Neves Analytics"); st.info("💡 Robô em espera.")
