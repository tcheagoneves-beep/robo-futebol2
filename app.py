import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Neves Analytics PRO", layout="centered", page_icon="❄️")

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .status-box {padding: 15px; border-radius: 10px; text-align: center; margin-bottom: 20px; font-weight: bold;}
    .status-active {background-color: #1F4025; color: #00FF00; border: 1px solid #00FF00;}
    .timer-text { font-size: 14px; color: #FFD700; text-align: center; font-weight: bold; margin-top: 10px; border-top: 1px solid #333; padding-top: 10px;}
    .strategy-card { background-color: #1e1e1e; padding: 15px; border-radius: 8px; margin-bottom: 10px; border-left: 4px solid #00FF00; }
    .strategy-title { color: #00FF00; font-weight: bold; font-size: 16px; margin-bottom: 5px; }
    .strategy-desc { font-size: 13px; color: #cccccc; line-height: 1.6; }
</style>
""", unsafe_allow_html=True)

# --- 2. GESTÃO DE DADOS E MEMÓRIA ---
DB_FILE = 'neves_dados.txt'
BLACK_FILE = 'neves_blacklist.txt'
STRIKES_FILE = 'neves_strikes_vip.txt'

# --- 🛡️ LISTA VIP (EUROPA + BRASILEIRÃO + ESTADUAIS) ---
# Adicionei os IDs dos estaduais para protegê-los do banimento rápido
LIGAS_VIP = [
    39, 78, 135, 140, 61, 2, 3, 9, 45, 48, # Europa Principais
    71, 72, 13, 11, # Brasil A/B, Liberta, Sula
    474, 475, 476, 477, 478, 479, # Estaduais (Gaúcho, Mineiro, Paulista, Carioca, Catarinense, Baiano)
    606, 610, 628, 55, 143 # Paranaense, Brasiliense, Goiano, etc.
]

# --- AUTOCORREÇÃO DE MEMÓRIA ---
if 'ligas_imunes' in st.session_state:
    if st.session_state['ligas_imunes'] and isinstance(list(st.session_state['ligas_imunes'].values())[0], str):
        st.session_state['ligas_imunes'] = {} 

if 'alertas_enviados' not in st.session_state:
    st.session_state['alertas_enviados'] = set()

if 'memoria_pressao' not in st.session_state:
    st.session_state['memoria_pressao'] = {}

if 'erros_por_liga' not in st.session_state:
    st.session_state['erros_por_liga'] = {} 
if 'erros_vip' not in st.session_state:
    st.session_state['erros_vip'] = {}
if 'ligas_imunes' not in st.session_state:
    st.session_state['ligas_imunes'] = {} 

# --- FUNÇÕES DE ARQUIVO ---
def carregar_blacklist():
    if not os.path.exists(BLACK_FILE): return pd.DataFrame(columns=['id', 'País', 'Liga'])
    try: return pd.read_csv(BLACK_FILE)
    except: return pd.DataFrame(columns=['id', 'País', 'Liga'])

def salvar_na_blacklist(id_liga, pais, nome_liga):
    df = carregar_blacklist()
    if str(id_liga) not in df['id'].astype(str).values:
        novo = pd.DataFrame([{'id': str(id_liga), 'País': pais, 'Liga': nome_liga}])
        pd.concat([df, novo], ignore_index=True).to_csv(BLACK_FILE, index=False)

# --- STRIKES DE LONGO PRAZO ---
def carregar_strikes_vip():
    cols = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes']
    if not os.path.exists(STRIKES_FILE): return pd.DataFrame(columns=cols)
    try: 
        df = pd.read_csv(STRIKES_FILE)
        if 'Liga' not in df.columns: return pd.DataFrame(columns=cols)
        return df
    except: return pd.DataFrame(columns=cols)

def registrar_erro_vip(id_liga, pais, nome_liga):
    df = carregar_strikes_vip()
    hoje = datetime.now().strftime('%Y-%m-%d')
    id_liga = str(id_liga)
    
    if id_liga in df['id'].astype(str).values:
        idx = df.index[df['id'].astype(str) == id_liga].tolist()[0]
        ultima_data = df.at[idx, 'Data_Erro']
        strikes_atuais = df.at[idx, 'Strikes']
        
        # Só conta strike se for em OUTRO DIA
        if ultima_data != hoje:
            strikes_atuais += 1
            df.at[idx, 'Data_Erro'] = hoje
            df.at[idx, 'Strikes'] = strikes_atuais
            df.at[idx, 'Liga'] = nome_liga
            df.at[idx, 'País'] = pais
            df.to_csv(STRIKES_FILE, index=False)
            
            if strikes_atuais >= 2:
                salvar_na_blacklist(id_liga, pais, nome_liga)
                st.toast(f"🚫 VIP Banida (2 Rodadas falhas): {nome_liga}")
                return "BAN"
            else:
                st.toast(f"⚠️ VIP em Observação (Rodada 1 Falhou): {nome_liga}")
                return "WARN"
        else:
            return "HOJE_JA_FOI"
    else:
        novo = pd.DataFrame([{
            'id': id_liga, 
            'País': pais, 
            'Liga': nome_liga, 
            'Data_Erro': hoje, 
            'Strikes': 1
        }])
        pd.concat([df, novo], ignore_index=True).to_csv(STRIKES_FILE, index=False)
        st.toast(f"⚠️ VIP Alertada (Falta de dados): {nome_liga}")
        return "FIRST"

def limpar_erro_vip(id_liga):
    if not os.path.exists(STRIKES_FILE): return
    df = pd.read_csv(STRIKES_FILE)
    if str(id_liga) in df['id'].astype(str).values:
        df = df[df['id'].astype(str) != str(id_liga)]
        df.to_csv(STRIKES_FILE, index=False)

def enviar_telegram_real(token, chat_ids, mensagem):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                              data={"chat_id": cid.strip(), "text": mensagem, "parse_mode": "Markdown"}, timeout=5)
            except: pass

def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics PRO")
    
    with st.expander("✅ Status do Sistema", expanded=True):
        st.caption("Todas as estratégias estão armadas:")
        st.markdown("🟣 **A** - Porteira Aberta")
        st.markdown("🟢 **B** - Reação / Blitz")
        st.markdown("💰 **C** - Janela de Ouro")
        st.markdown("⚡ **D** - Gol Relâmpago")
    
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        
        st.markdown("---")
        if st.button("🔔 Testar Telegram"):
            enviar_telegram_real(tg_token, tg_chat_ids, "✅ *Neves PRO:* Estaduais Protegidos.")
            st.toast("Enviado!")

        INTERVALO = st.slider("Ciclo (seg):", 30, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        
        st.markdown("---")
        st.caption("Gestão de Dados:")
        
        col_res1, col_res2 = st.columns(2)
        with col_res1:
            if st.button("♻️ Reset Sessão"):
                st.session_state['alertas_enviados'] = set() 
                st.session_state['memoria_pressao'] = {}
                st.session_state['erros_por_liga'] = {}
                st.session_state['erros_vip'] = {}
                st.session_state['ligas_imunes'] = {}
                st.toast("Memória limpa!")
                time.sleep(1)
                st.rerun()
        
        with col_res2:
            if st.button("🗑️ Del. Blacklist"):
                if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
                if os.path.exists(STRIKES_FILE): os.remove(STRIKES_FILE)
                st.toast("Tudo apagado!")
                time.sleep(1)
                st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 4. API ---
@st.cache_data(ttl=3600)
def buscar_proximos(key):
    if not key and not MODO_DEMO: return []
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        data_hoje = agora_brasil().strftime('%Y-%m-%d')
        params = {"date": data_hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": key}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        return [
            {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 47}}, "league": {"id": 1, "name": "Liga Acréscimo", "country": "BR"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Fav (47')"}, "away": {"name": "Zebra"}}},
            {"fixture": {"id": 2, "status": {"short": "HT", "elapsed": 45}}, "league": {"id": 2, "name": "Liga Intervalo", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}
        ]
    if not API_KEY: return []
    try:
        res = requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_stats(fid, demo_stage=0):
    if MODO_DEMO:
        if fid == 1: 
            return [{"team": {"name": "Home"}, "statistics": [{"type": "Total Shots", "value": 12}, {"type": "Shots on Goal", "value": 6}]}, {"team": {"name": "Away"}, "statistics": [{"type": "Total Shots", "value": 2}, {"type": "Shots on Goal", "value": 0}]}]
        return []
    return buscar_dados("statistics", {"fixture": fid})

# --- 5. MOMENTUM ---
def atualizar_momentum(fid, sog_h_atual, sog_a_atual):
    agora = datetime.now()
    janela_tempo = timedelta(minutes=7)
    
    if fid not in st.session_state['memoria_pressao']:
        st.session_state['memoria_pressao'][fid] = {'sog_h_total': sog_h_atual, 'sog_a_total': sog_a_atual, 'sog_h_timestamps': [], 'sog_a_timestamps': []}
        return 0, 0 

    memoria = st.session_state['memoria_pressao'][fid]
    
    delta_h = max(0, sog_h_atual - memoria['sog_h_total'])
    delta_a = max(0, sog_a_atual - memoria['sog_a_total'])
    
    for _ in range(delta_h): memoria['sog_h_timestamps'].append(agora)
    for _ in range(delta_a): memoria['sog_a_timestamps'].append(agora)
    
    memoria['sog_h_total'] = sog_h_atual
    memoria['sog_a_total'] = sog_a_atual
    
    memoria['sog_h_timestamps'] = [t for t in memoria['sog_h_timestamps'] if agora - t <= janela_tempo]
    memoria['sog_a_timestamps'] = [t for t in memoria['sog_a_timestamps'] if agora - t <= janela_tempo]
    
    st.session_state['memoria_pressao'][fid] = memoria
    return len(memoria['sog_h_timestamps']), len(memoria['sog_a_timestamps'])

# --- 6. PROCESSADOR ---
def processar_jogo(j, stats):
    f_id = j['fixture']['id']
    tempo = j['fixture']['status'].get('elapsed', 0)
    home = j['teams']['home']['name']
    away = j['teams']['away']['name']
    gh = j['goals']['home'] or 0
    ga = j['goals']['away'] or 0
    total_gols = gh + ga
    
    try:
        def get_val(team_idx, type_name):
            if not stats or len(stats) <= team_idx: return 0
            try:
                data = next((item for item in stats[team_idx]['statistics'] if item["type"] == type_name), None)
                return data['value'] if data and data['value'] else 0
            except: return 0

        sh_h = get_val(0, "Total Shots")
        sog_h = get_val(0, "Shots on Goal")
        sh_a = get_val(1, "Total Shots")
        sog_a = get_val(1, "Shots on Goal")
        total_chutes = sh_h + sh_a
        
        recentes_h, recentes_a = atualizar_momentum(f_id, sog_h, sog_a)
        
        # A) PORTEIRA ABERTA
        if tempo <= 30 and total_gols >= 2:
            return {
                "tag": "🟣 Porteira Aberta",
                "ordem": "Adicionar em Múltipla Over Gols",
                "motivo": f"Jogo frenético ({gh}x{ga} com {tempo}').",
                "stats": f"{gh}x{ga}"
            }

        # D) GOL RELÂMPAGO
        if 5 <= tempo <= 15:
            if (sog_h >= 1 or sog_a >= 1):
                return {
                    "tag": "⚡ Gol Relâmpago",
                    "ordem": "Apostar em Over 0.5 HT (1º Tempo)",
                    "motivo": "Início elétrico, goleiro já trabalhou.",
                    "stats": f"Chutes Alvo: {sog_h + sog_a}"
                }

        # B) REAÇÃO DO GIGANTE / BLITZ
        if tempo <= 60:
            if (gh <= ga) and (recentes_h >= 2 or sh_h >= 6):
                oponente_vivo = (recentes_a >= 1 or sh_a >= 4)
                acao = "⚠️ Jogo Aberto: Apostar em Mais 1 Gol na Partida" if oponente_vivo else "✅ Apostar no Gol do Mandante"
                return {
                    "tag": "🟢 Reação/Blitz",
                    "ordem": acao,
                    "motivo": f"{home} amassando! {recentes_h} chutes no alvo (7') ou {sh_h} totais.",
                    "stats": f"Blitz Recente: {recentes_h} | Total: {sh_h}"
                }
            
            if (ga <= gh) and (recentes_a >= 2 or sh_a >= 6):
                oponente_vivo = (recentes_h >= 1 or sh_h >= 4)
                acao = "⚠️ Jogo Aberto: Apostar em Mais 1 Gol na Partida" if oponente_vivo else "✅ Apostar no Gol do Visitante"
                return {
                    "tag": "🟢 Reação/Blitz",
                    "ordem": acao,
                    "motivo": f"{away} amassando! {recentes_a} chutes no alvo (7') ou {sh_a} totais.",
                    "stats": f"Blitz Recente: {recentes_a} | Total: {sh_a}"
                }

        # C) JANELA DE OURO
        if 70 <= tempo <= 75:
            if total_chutes >= 18 and abs(gh - ga) <= 1:
                return {
                    "tag": "💰 Janela de Ouro",
                    "ordem": "Entrar em Mais 1.0 Gol (Asiático)",
                    "motivo": "Pressão final absurda e placar apertado.",
                    "stats": f"Total Chutes: {total_chutes}"
                }

    except: return None
    return None

# --- 7. EXECUÇÃO ---
main_placeholder = st.empty()

if ROBO_LIGADO:
    df_black = carregar_blacklist()
    ids_bloqueados = df_black['id'].astype(str).values
    
    jogos_live = buscar_dados("fixtures", {"live": "all"})
    radar = []
    
    for j in jogos_live:
        l_id = str(j['league']['id'])
        
        if l_id in ids_bloqueados: 
            continue
        
        f_id = j['fixture']['id']
        tempo = j['fixture']['status'].get('elapsed', 0)
        status_short = j['fixture']['status'].get('short', '')
        home = j['teams']['home']['name']
        away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"
        
        eh_intervalo = (status_short in ['HT', 'BT']) or (48 <= tempo <= 52)
        eh_aquecimento = (tempo < 5)
        eh_fim = (tempo > 80)
        dentro_janela = not (eh
