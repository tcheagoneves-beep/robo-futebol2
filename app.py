import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 0. CONFIGURAÇÃO VISUAL E RESET DE CACHE ---
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")
st.cache_data.clear()

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .metric-card {background-color: #1e1e1e; padding: 10px; border-radius: 8px; border: 1px solid #333; text-align: center;}
    .metric-value {font-size: 20px; font-weight: bold; color: #00FF00;}
    .metric-label {font-size: 12px; color: #ccc;}
    .status-active {background-color: #1F4025; color: #00FF00; border: 1px solid #00FF00; padding: 10px; text-align: center; border-radius: 5px; font-weight: bold; margin-bottom: 10px;}
    .timer-box {position: fixed; bottom: 20px; right: 20px; background-color: #000; color: #FFD700; padding: 15px; border-radius: 8px; border: 2px solid #FFD700; font-weight: bold; font-size: 18px; z-index: 9999;}
</style>
""", unsafe_allow_html=True)

# --- 1. ARQUIVOS E CONSTANTES ---
FILES = {
    'db': 'neves_dados.txt',
    'black': 'neves_blacklist.txt',
    'vip': 'neves_strikes_vip.txt',
    'hist': 'neves_historico_sinais.csv',
    'report': 'neves_status_relatorio.txt'
}

LIGAS_VIP = [39, 78, 135, 140, 61, 2, 3, 9, 45, 48, 71, 72, 13, 11, 474, 475, 476, 477, 478, 479, 606, 610, 628, 55, 143]

# --- 2. GERENCIAMENTO DE DADOS ROBUSTO ---
def init_session():
    defaults = {
        'ligas_imunes': {}, 'alertas_enviados': set(), 'memoria_pressao': {},
        'historico_sinais': [], 'erros_vip': {}, 'erros_por_liga': {}
    }
    for k, v in defaults.items():
        if k not in st.session_state: st.session_state[k] = v

def safe_read_csv(filepath, columns):
    """Lê CSV de forma segura. Se der erro, retorna DataFrame vazio com as colunas certas."""
    if not os.path.exists(filepath): return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(filepath)
        # Se as colunas não baterem, o arquivo está velho/corrompido. Reseta.
        if not set(columns).issubset(df.columns):
            return pd.DataFrame(columns=columns)
        return df.fillna("").astype(str)
    except:
        return pd.DataFrame(columns=columns)

def load_data():
    """Carrega todos os dados críticos na memória"""
    st.session_state['df_black'] = safe_read_csv(FILES['black'], ['id', 'País', 'Liga'])
    st.session_state['df_vip'] = safe_read_csv(FILES['vip'], ['id', 'País', 'Liga', 'Data_Erro', 'Strikes'])
    
    # Histórico precisa ser carregado e convertido para lista de dicionários
    df_hist = safe_read_csv(FILES['hist'], ['Data', 'Hora', 'Liga', 'Jogo', 'Placar', 'Estrategia', 'Resultado'])
    hoje = datetime.now().strftime('%Y-%m-%d')
    if not df_hist.empty and 'Data' in df_hist.columns:
        df_hist = df_hist[df_hist['Data'] == hoje]
    st.session_state['historico_sinais'] = df_hist.to_dict('records')

def save_blacklist(id_liga, pais, nome_liga):
    novo = pd.DataFrame([{'id': str(id_liga), 'País': str(pais), 'Liga': str(nome_liga)}])
    try:
        df_atual = safe_read_csv(FILES['black'], ['id', 'País', 'Liga'])
        if str(id_liga) not in df_atual['id'].values:
            pd.concat([df_atual, novo], ignore_index=True).to_csv(FILES['black'], index=False)
            st.session_state['df_black'] = pd.concat([df_atual, novo], ignore_index=True)
    except: pass

def save_historic(item):
    df_novo = pd.DataFrame([item])
    header = not os.path.exists(FILES['hist'])
    df_novo.to_csv(FILES['hist'], mode='a', header=header, index=False)

# --- 3. LÓGICA DE NEGÓCIO ---
def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "Markdown"}, timeout=3)
        except: pass

def processar_dados_jogo(j, stats, tempo, placar):
    # Lógica de extração segura
    try:
        if not stats: return None
        
        # Procura dados de chutes
        sh_h, sog_h, sh_a, sog_a = 0, 0, 0, 0
        dados_validos = False
        
        for idx, team_stats in enumerate(stats):
            for s in team_stats.get('statistics', []):
                if s['type'] == "Total Shots" and s['value'] is not None:
                    if idx == 0: sh_h = s['value']
                    else: sh_a = s['value']
                    dados_validos = True
                if s['type'] == "Shots on Goal" and s['value'] is not None:
                    if idx == 0: sog_h = s['value']
                    else: sog_a = s['value']
                    dados_validos = True
        
        if not dados_validos: return None

        gh = j['goals']['home'] or 0
        ga = j['goals']['away'] or 0
        
        # Estratégias
        sinal = None
        if tempo <= 30 and (gh + ga) >= 2:
            sinal = {"tag": "🟣 Porteira Aberta", "ordem": "🔥 ENTRADA SECA: Over Gols Limite", "stats": f"{gh}x{ga}"}
        elif 5 <= tempo <= 15 and (sog_h + sog_a) >= 1:
            sinal = {"tag": "⚡ Gol Relâmpago", "ordem": "Apostar Over 0.5 HT", "stats": f"Chutes Alvo: {sog_h+sog_a}"}
        elif 70 <= tempo <= 75 and (sh_h + sh_a) >= 18 and abs(gh - ga) <= 1:
            sinal = {"tag": "💰 Janela de Ouro", "ordem": "Over Gols Asiático", "stats": f"Total Chutes: {sh_h+sh_a}"}
        elif tempo <= 60:
            # Blitz Home
            if gh <= ga and (sog_h >= 4 or sh_h >= 10): # Simplificado para teste
                 sinal = {"tag": "🟢 Blitz Casa", "ordem": "Gol do Mandante", "stats": f"Chutes: {sh_h}"}
            # Blitz Away
            elif ga <= gh and (sog_a >= 4 or sh_a >= 10):
                 sinal = {"tag": "🟢 Blitz Visitante", "ordem": "Gol do Visitante", "stats": f"Chutes: {sh_a}"}
                 
        return sinal
    except: return None

# --- 4. INTERFACE ---
init_session()
load_data() # Carrega dados ao iniciar o script

with st.sidebar:
    st.title("❄️ Neves PRO")
    
    # RECOLOCANDO O STATUS QUE SUMIU
    with st.expander("ℹ️ Legenda das Estratégias", expanded=True):
        st.caption("🟢 **Blitz:** Pressão forte do time perdendo/empatando.")
        st.caption("🟣 **Porteira Aberta:** 2+ gols antes dos 30min.")
        st.caption("💰 **Janela de Ouro:** Pressão final (70-75min).")
        st.caption("⚡ **Gol Relâmpago:** Início elétrico (5-15min).")

    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("API Key:", type="password")
        TG_TOKEN = st.text_input("Telegram Token:", type="password")
        TG_CHAT = st.text_input("Chat IDs (separar por vírgula):")
        INTERVALO = st.slider("Ciclo (seg):", 30, 300, 60)
        
        c1, c2 = st.columns(2)
        if c1.button("♻️ Reset Memória"):
            for k in list(st.session_state.keys()): del st.session_state[k]
            st.rerun()
        if c2.button("🗑️ Limpar Arquivos"):
            for f in FILES.values(): 
                if os.path.exists(f): os.remove(f)
            st.rerun()

    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 5. CORE DO ROBÔ ---
main = st.empty()

if ROBO_LIGADO:
    # API Call
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"live": "all"}
        resp = requests.get(url, headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        jogos = resp.get('response', [])
    except: jogos = []

    radar = []
    ids_black = st.session_state['df_black']['id'].values

    for j in jogos:
        lid = str(j['league']['id'])
        fid = j['fixture']['id']
        if lid in ids_black: continue

        tempo = j['fixture']['status']['elapsed'] or 0
        status = j['fixture']['status']['short']
        home = j['teams']['home']['name']
        away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"

        # Lógica de Janela
        if status in ['HT', 'BT', 'FT'] or tempo > 90 or tempo < 1:
            visual_status = "💤"
        else:
            visual_status = "👁️"
            # Busca Stats
            try:
                url_stats = "https://v3.football.api-sports.io/fixtures/statistics"
                s_resp = requests.get(url_stats, headers={"x-apisports-key": API_KEY}, params={"fixture": fid}, timeout=5).json()
                stats = s_resp.get('response', [])
                
                sinal = processar_dados_jogo(j, stats, tempo, placar)
                
                # Regra de Banimento por falta de dados (45min)
                if not sinal and not stats and tempo >= 45 and int(lid) not in LIGAS_VIP:
                    save_blacklist(lid, j['league']['country'], j['league']['name'])
                    st.toast(f"🚫 Liga Banida (Sem dados): {j['league']['name']}")

                if sinal:
                    visual_status = "✅ " + sinal['tag']
                    if fid not in st.session_state['alertas_enviados']:
                        msg = f"🚨 *{sinal['tag']}*\n⚽ {home} {placar} {away}\n🏆 {j['league']['name']}\n⚠️ {sinal['ordem']}\n📊 {sinal['stats']}"
                        enviar_telegram(TG_TOKEN, TG_CHAT, msg)
                        st.session_state['alertas_enviados'].add(fid)
                        
                        hist_item = {
                            "Data": datetime.now().strftime('%Y-%m-%d'),
                            "Hora": datetime.now().strftime('%H:%M'),
                            "Liga": j['league']['name'],
                            "Jogo": f"{home} x {away}",
                            "Placar": placar,
                            "Estrategia": sinal['tag'],
                            "Resultado": "Pendente"
                        }
                        st.session_state['historico_sinais'].insert(0, hist_item)
                        save_historic(hist_item)
                        st.toast(f"Sinal Enviado: {sinal['tag']}")

            except Exception as e:
                # Silencia erros de API para não parar o robô
                pass

        radar.append({
            "Liga": j['league']['name'],
            "Jogo": f"{home} {placar} {away}",
            "Tempo": f"{tempo}'",
            "Status": visual_status
        })

    # --- DASHBOARD VISUAL ---
    with main.container():
        st.markdown('<div class="status-active">🟢 SISTEMA ATIVO E MONITORANDO</div>', unsafe_allow_html=True)
        
        # Métricas
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-card"><div class="metric-value">{len(st.session_state["historico_sinais"])}</div><div class="metric-label">Sinais Hoje</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-card"><div class="metric-value">{len(radar)}</div><div class="metric-label">Jogos Ao Vivo</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-card"><div class="metric-value">{len(LIGAS_VIP)}</div><div class="metric-label">Ligas VIP</div></div>', unsafe_allow_html=True)
        
        st.write("") # Espaço

        # Tabelas (Conversão segura para String)
        df_radar = pd.DataFrame(radar).astype(str)
        df_hist = pd.DataFrame(st.session_state['historico_sinais']).astype(str)
        df_black = st.session_state['df_black'].astype(str)

        t1, t2, t3 = st.tabs(["📡 Radar Ao Vivo", "📜 Histórico de Sinais", "🚫 Blacklist"])
        
        with t1:
            if not df_radar.empty: st.dataframe(df_radar, use_container_width=True, hide_index=True)
            else: st.info("Nenhum jogo compatível no momento.")
        
        with t2:
            if not df_hist.empty: st.dataframe(df_hist, use_container_width=True, hide_index=True)
            else: st.caption("Nenhum sinal gerado hoje.")
            
        with t3:
            if not df_black.empty: st.dataframe(df_black, use_container_width=True, hide_index=True)
            else: st.caption("Nenhuma liga banida.")

    # Timer Flutuante (Fora do container principal para garantir visibilidade)
    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="timer-box">Próxima: {i}s</div>', unsafe_allow_html=True)
        time.sleep(1)
    st.rerun()

else:
    with main.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure na lateral e ative o checkbox.")
