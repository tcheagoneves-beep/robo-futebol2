import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 0. PREVENÇÃO DE ERROS ---
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")
st.cache_data.clear()

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .metric-card {background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; text-align: center;}
    .metric-value {font-size: 24px; font-weight: bold; color: #00FF00;}
    .metric-label {font-size: 14px; color: #ccc;}
    .timer-box {position: fixed; bottom: 10px; right: 10px; background-color: #000; color: #FFD700; padding: 10px; border-radius: 5px; border: 1px solid #FFD700; font-weight: bold; z-index: 9999;}
</style>
""", unsafe_allow_html=True)

# --- 1. ARQUIVOS ---
BLACK_FILE = 'neves_blacklist.txt'
STRIKES_FILE = 'neves_strikes_vip.txt'
HIST_FILE = 'neves_historico_sinais.csv'
RELATORIO_FILE = 'neves_status_relatorio.txt'

# --- 2. LISTA VIP ---
LIGAS_VIP = [
    39, 78, 135, 140, 61, 2, 3, 9, 45, 48, 
    71, 72, 13, 11, 
    474, 475, 476, 477, 478, 479, 
    606, 610, 628, 55, 143 
]

# --- 3. CARREGAMENTO BLINDADO ---
def safe_load_csv(filepath, columns):
    if not os.path.exists(filepath): return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(filepath)
        # Se faltar coluna, reseta o arquivo
        if not set(columns).issubset(df.columns):
            return pd.DataFrame(columns=columns)
        return df.fillna("").astype(str)
    except:
        return pd.DataFrame(columns=columns)

def carregar_blacklist(): return safe_load_csv(BLACK_FILE, ['id', 'País', 'Liga'])
def carregar_strikes(): return safe_load_csv(STRIKES_FILE, ['id', 'País', 'Liga', 'Data_Erro', 'Strikes'])
def carregar_historico(): return safe_load_csv(HIST_FILE, ['Data', 'Hora', 'Liga', 'Jogo', 'Placar', 'Estrategia', 'Resultado'])

# --- 4. VARIÁVEIS DE SESSÃO ---
if 'ligas_imunes' not in st.session_state: st.session_state['ligas_imunes'] = {}
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'historico_sinais' not in st.session_state: st.session_state['historico_sinais'] = []

# --- 5. LÓGICA ---
def salvar_na_blacklist(id_liga, pais, nome_liga):
    df = carregar_blacklist()
    if str(id_liga) not in df['id'].values:
        novo = pd.DataFrame([{'id': str(id_liga), 'País': str(pais), 'Liga': str(nome_liga)}])
        pd.concat([df, novo], ignore_index=True).to_csv(BLACK_FILE, index=False)

def registrar_erro_vip(id_liga, pais, nome_liga):
    df = carregar_strikes()
    hoje = datetime.now().strftime('%Y-%m-%d')
    id_str = str(id_liga)
    
    if id_str in df['id'].values:
        idx = df.index[df['id'] == id_str].tolist()[0]
        if df.at[idx, 'Data_Erro'] != hoje:
            strikes = int(float(df.at[idx, 'Strikes'])) + 1
            df.at[idx, 'Data_Erro'] = hoje
            df.at[idx, 'Strikes'] = strikes
            df.to_csv(STRIKES_FILE, index=False)
            if strikes >= 2:
                salvar_na_blacklist(id_str, pais, nome_liga)
                st.toast(f"🚫 VIP Banida: {nome_liga}")
    else:
        novo = pd.DataFrame([{'id': id_str, 'País': str(pais), 'Liga': str(nome_liga), 'Data_Erro': hoje, 'Strikes': 1}])
        pd.concat([df, novo], ignore_index=True).to_csv(STRIKES_FILE, index=False)

def limpar_erro_vip(id_liga):
    if not os.path.exists(STRIKES_FILE): return
    try:
        df = pd.read_csv(STRIKES_FILE, dtype=str)
        if str(id_liga) in df['id'].values:
            df = df[df['id'] != str(id_liga)]
            df.to_csv(STRIKES_FILE, index=False)
    except: pass

def enviar_telegram(token, chat_ids, msg):
    if token and chat_ids:
        for cid in str(chat_ids).replace(';', ',').split(','):
            if cid.strip():
                try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid.strip(), "text": msg, "parse_mode": "Markdown"}, timeout=5)
                except: pass

def agora_brasil(): return datetime.utcnow() - timedelta(hours=3)

# --- 6. API ---
@st.cache_data(ttl=3600)
def buscar_proximos(key):
    if not key and not st.session_state.get('demo', False): return []
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": agora_brasil().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}
        return requests.get(url, headers={"x-apisports-key": key}, params=params).json().get('response', [])
    except: return []

def buscar_dados(endpoint, params, key):
    if st.session_state.get('demo', False):
        return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 20}}, "league": {"id": 1, "name": "Demo", "country": "BR"}, "goals": {"home": 1, "away": 1}, "teams": {"home": {"name": "H"}, "away": {"name": "A"}}}]
    if not key: return []
    try: return requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": key}, params=params).json().get('response', [])
    except: return []

# --- 7. PROCESSAMENTO ---
def processar_jogo(j, stats):
    f_id, tempo = j['fixture']['id'], j['fixture']['status']['elapsed']
    gh, ga = j['goals']['home'], j['goals']['away']
    
    # Validação de dados (Simples)
    tem_dados = False
    if stats:
        for t in stats:
            for s in t.get('statistics', []):
                if s['type'] in ['Shots on Goal', 'Total Shots'] and s['value'] is not None:
                    tem_dados = True
                    break
    
    if not tem_dados and not st.session_state.get('demo', False):
        return None, False # Sem dados válidos

    # Estratégias
    sinal = None
    total_gols = gh + ga
    
    if tempo <= 30 and total_gols >= 2:
        sinal = {"tag": "🟣 Porteira Aberta", "ordem": "🔥 ENTRADA SECA: Over Gols Limite", "stats": f"{gh}x{ga}"}
    elif 70 <= tempo <= 75 and abs(gh - ga) <= 1:
        # Simplifiquei para focar no erro
        sinal = {"tag": "💰 Janela de Ouro", "ordem": "Over Gols Asiático", "stats": "Pressão Final"}
        
    return sinal, True

# --- 8. INTERFACE ---
with st.sidebar:
    st.title("❄️ Neves PRO")
    with st.expander("⚙️ Config"):
        API_KEY = st.text_input("API Key:", type="password")
        TG_TOKEN = st.text_input("Telegram Token:", type="password")
        TG_CHAT = st.text_input("Chat IDs:")
        INTERVALO = st.slider("Ciclo (s):", 30, 300, 60)
        st.session_state['demo'] = st.checkbox("Simulação")
        if st.button("🗑️ Limpar Erros"):
            for f in [BLACK_FILE, STRIKES_FILE]: 
                if os.path.exists(f): os.remove(f)
            st.rerun()
    ROBO = st.checkbox("🚀 LIGAR")

main = st.empty()

if ROBO:
    # Carrega dados
    df_black = carregar_blacklist()
    ids_black = df_black['id'].values
    
    jogos = buscar_dados("fixtures", {"live": "all"}, API_KEY)
    radar = []
    
    for j in jogos:
        lid = str(j['league']['id'])
        if lid in ids_black: continue
        
        fid = j['fixture']['id']
        tempo = j['fixture']['status']['elapsed']
        
        # Filtro de Janela (CORRIGIDO AQUI O ERRO DE SINTAXE)
        status = j['fixture']['status']['short']
        dentro_janela = not (status in ['HT', 'BT'] or tempo > 80 or tempo < 5)
        
        sinal_tag = ""
        if dentro_janela:
            stats = buscar_dados("statistics", {"fixture": fid}, API_KEY)
            sinal, dados_ok = processar_jogo(j, stats)
            
            if not dados_ok:
                if int(lid) in LIGAS_VIP:
                    if lid not in st.session_state['ligas_imunes']: registrar_erro_vip(lid, j['league']['country'], j['league']['name'])
                elif tempo >= 45:
                    salvar_na_blacklist(lid, j['league']['country'], j['league']['name'])
            
            elif dados_ok:
                st.session_state['ligas_imunes'][lid] = True
                limpar_erro_vip(lid)
                
                if sinal:
                    sinal_tag = sinal['tag']
                    if fid not in st.session_state['alertas_enviados']:
                        msg = f"🚨 *{sinal['tag']}*\n⚽ {j['teams']['home']['name']} x {j['teams']['away']['name']}\n⚠️ {sinal['ordem']}"
                        enviar_telegram(TG_TOKEN, TG_CHAT, msg)
                        st.session_state['alertas_enviados'].add(fid)
                        # Salva histórico
                        novo_hist = {'Data': agora_brasil().strftime('%Y-%m-%d'), 'Hora': agora_brasil().strftime('%H:%M'), 
                                     'Liga': j['league']['name'], 'Jogo': f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", 
                                     'Estrategia': sinal['tag'], 'Resultado': 'Pendente'}
                        pd.DataFrame([novo_hist]).to_csv(HIST_FILE, mode='a', header=not os.path.exists(HIST_FILE), index=False)

        radar.append({
            "Liga": j['league']['name'],
            "Jogo": f"{j['teams']['home']['name']} {j['goals']['home']}x{j['goals']['away']} {j['teams']['away']['name']}",
            "Tempo": f"{tempo}'",
            "Sinal": sinal_tag
        })

    # Renderização (COM PROTEÇÃO CONTRA CRASH)
    with main.container():
        st.title("❄️ Neves Analytics PRO")
        st.markdown('<div class="status-box status-active">🟢 ATIVO</div><br>', unsafe_allow_html=True)
        
        hist = carregar_historico()
        # Filtra histórico de hoje
        hoje = agora_brasil().strftime('%Y-%m-%d')
        hist_hoje = [h for h in hist if h.get('Data') == hoje]

        with st.expander("📊 Métricas", expanded=False):
            c1, c2 = st.columns(2)
            c1.metric("Sinais Hoje", len(hist_hoje))
            c2.metric("Jogos Live", len(radar))

        t1, t2, t3, t4 = st.tabs(["📡 Radar", "📜 Histórico", "🚫 Blacklist", "⚠️ Obs"])
        
        with t1: st.dataframe(pd.DataFrame(radar).astype(str), use_container_width=True, hide_index=True) if radar else st.info("Buscando...")
        with t2: st.dataframe(pd.DataFrame(hist_hoje).astype(str), use_container_width=True, hide_index=True)
        
        # AQUI ESTAVA O ERRO DE ATTRIBUTE ERROR - AGORA BLINDADO
        with t3: 
            try:
                if not df_black.empty and 'País' in df_black.columns:
                    st.table(df_black.sort_values(['País', 'Liga']))
                else:
                    st.table(df_black) # Mostra sem ordenar se der erro
            except:
                st.write("Erro ao exibir Blacklist (Arquivo antigo sendo corrigido...)")
                
        with t4: st.table(carregar_strikes())

    # TIMER FLUTUANTE (SEMPRE APARECE)
    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="timer-box">Próxima: {i}s</div>', unsafe_allow_html=True)
        time.sleep(1)
    st.rerun()

else:
    with main.container():
        st.title("❄️ Neves Analytics PRO")
        st.warning("Desligado.")
