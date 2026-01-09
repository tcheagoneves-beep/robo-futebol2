import streamlit as st
import pandas as pd
import requests
import time
import os
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Neves Analytics",
    layout="centered",
    page_icon="❄️"
)

st.markdown("""
<style>
    .stApp {background-color: #121212; color: white;}
    .card {background-color: #1E1E1E; padding: 20px; border-radius: 15px; border: 1px solid #333; margin-bottom: 20px;}
    .titulo-time {font-size: 20px; font-weight: bold; color: #ffffff;}
    .odd-label {font-size: 12px; color: #aaa; background-color: #333; padding: 2px 6px; border-radius: 4px;}
    .placar {font-size: 35px; font-weight: 800; color: #FFD700; text-align: center;}
    .tempo {font-size: 14px; color: #FF4B4B; font-weight: bold; text-align: center;}
    .sinal-box {background-color: #00C853; color: white; padding: 10px; border-radius: 8px; text-align: center; font-weight: bold; margin-top: 15px;}
    .multipla-box {background-color: #9C27B0; color: white; padding: 10px; border-radius: 8px; text-align: center; font-weight: bold; margin-top: 15px;}
    .alerta-over-box {background-color: #FF9800; color: white; padding: 10px; border-radius: 8px; text-align: center; font-weight: bold; margin-top: 15px;}
    .status-online {color: #00FF00; font-weight: bold; animation: pulse 2s infinite; padding: 5px 10px; border: 1px solid #00FF00; text-align: center; margin-bottom: 20px; border-radius: 15px;}
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.7); } 70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); } 100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); } }
    .metric-val {font-size: 22px; font-weight: bold;}
    .metric-label {font-size: 10px; color: #888; text-transform: uppercase;}
    .stats-row { display: flex; justify-content: space-around; text-align: center; margin-top: 15px; padding-top: 15px; border-top: 1px solid #333; }
</style>
""", unsafe_allow_html=True)

# --- 2. AJUSTE DE FUSO HORÁRIO (BRASIL) ---
def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

# --- 3. FUNÇÕES DE BANCO DE DADOS E NOTIFICAÇÃO ---
DB_FILE = 'neves_dados.txt'

def enviar_msg_telegram(token, chat_ids, mensagem):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid.strip(), "text": mensagem, "parse_mode": "Markdown"})
            except: pass

def carregar_db():
    if not os.path.exists(DB_FILE):
        df = pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
        df.to_csv(DB_FILE, index=False)
        return df
    try: return pd.read_csv(DB_FILE)
    except: return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])

def salvar_sinal_db(fixture_id, jogo, sinal, gols_inicial):
    df = carregar_db()
    if not ((df['id'] == fixture_id) & (df['status'] == 'Pendente')).any():
        data_br = agora_brasil()
        novo_registro = {
            'id': fixture_id,
            'data': data_br.strftime('%Y-%m-%d'),
            'hora': data_br.strftime('%H:%M'),
            'jogo': jogo,
            'sinal': sinal,
            'gols_inicial': gols_inicial,
            'status': 'Pendente'
        }
        df = pd.concat([df, pd.DataFrame([novo_registro])], ignore_index=True)
        df.to_csv(DB_FILE, index=False)

def atualizar_status_db(lista_jogos_api, tg_token=None, tg_chat_ids=None):
    df = carregar_db()
    if df.empty: return df
    
    modificado = False
    pendentes = df[df['status'] == 'Pendente']
    
    for index, row in pendentes.iterrows():
        # Tenta achar o jogo na lista AO VIVO
        jogo_dados = next((j for j in lista_jogos_api if j['fixture']['id'] == row['id']), None)
        
        # Se não achar na lista ao vivo (pode ter acabado), teríamos que buscar individualmente.
        # Mas para economizar API, vamos atualizar apenas enquanto ele estiver no radar ao vivo ou
        # quando o robô rodar a rotina de limpeza (futuro).
        # Por enquanto, atualiza se estiver na lista 'live'.
        
        if jogo_dados:
            gols_agora = (jogo_dados['goals']['home'] or 0) + (jogo_dados['goals']['away'] or 0)
            status_match = jogo_dados['fixture']['status']['short']
            
            # --- LÓGICA DE GREEN ---
            if gols_agora > row['gols_inicial']:
                df.at[index, 'status'] = 'Green'
                modificado = True
                if tg_token and tg_chat_ids:
                    msg = f"✅ **GREEN! PAGOU!** 💰\n\n⚽ **{row['jogo']}**\n\nO sinal de **{row['sinal']}** bateu!\nDinheiro no bolso. A análise foi perfeita. 🚀"
                    enviar_msg_telegram(tg_token, tg_chat_ids, msg)

            # --- LÓGICA DE RED ---
            elif status_match in ['FT', 'AET', 'PEN']:
                df.at[index, 'status'] = 'Red'
                modificado = True
                if tg_token and tg_chat_ids:
                    msg = f"🔻 **RED - SEGUE O PLANO**\n\n⚽ **{row['jogo']}**\n\nO sinal não bateu desta vez.\nFaz parte do jogo. Cabeça fria e foco na próxima oportunidade! 💪"
                    enviar_msg_telegram(tg_token, tg_chat_ids, msg)
    
    if modificado:
        df.to_csv(DB_FILE, index=False)
    return df

def gerar_texto_relatorio():
    df = carregar_db()
    if df.empty: return None
    df['data'] = pd.to_datetime(df['data'])
    hoje = pd.Timestamp(agora_brasil().date())
    df_hoje = df[df['data'] == hoje]
    start_week = hoje - timedelta(days=hoje.weekday())
    df_semana = df[df['data'] >= start_week]
    df_mes = df[(df['data'].dt.month == hoje.month) & (df['data'].dt.year == hoje.year)]
    def calcular(d):
        total = len(d)
        g = len(d[d['status'] == 'Green'])
        r = len(d[d['status'] == 'Red'])
        taxa = (g / (g + r) * 100) if (g + r) > 0 else 0
        return g, r, taxa
    gh, rh, ph = calcular(df_hoje)
    gs, rs, ps = calcular(df_semana)
    gm, rm, pm = calcular(df_mes)
    msg = f"""
📊 **FECHAMENTO NEVES ANALYTICS** 📊
📅 {hoje.strftime('%d/%m/%Y')}

**HOJE:**
✅ Green: {gh}
🔻 Red: {rh}
💰 Assertividade: {ph:.1f}%

**SEMANA:**
📈 {gs} x {rs} ({ps:.1f}%)

**MÊS:**
🏆 {gm} Greens / {rm} Reds ({pm:.1f}%)
"""
    return msg

# --- 4. SIDEBAR E CONFIGURAÇÕES ---
with st.sidebar:
    st.header("⚙️ Configurações")
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("Chave API-SPORTS:", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.markdown("---")
    
    with st.expander("🔔 Telegram (Família)"):
        tg_token = st.text_input("Bot Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs (separados por vírgula):")
        if st.button("Testar Envio"):
            enviar_msg_telegram(tg_token, tg_chat_ids, "✅ Teste Neves Analytics!")
            st.success("Enviado!")
    
    st.markdown("---")
    st.header("🤖 Modo Automático")
    ROBO_LIGADO = st.checkbox("LIGAR ROBÔ", value=False)
    # AJUSTE ECONÔMICO: Slider começa em 150s para proteger a conta
    INTERVALO = st.slider("Ciclo (segundos):", 60, 300, 150)
    
    if st.button("🗑️ Limpar Histórico"):
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
            st.rerun()
    
    if st.button("📉 Enviar Relatório Agora"):
        if tg_token and tg_chat_ids:
            rel = gerar_texto_relatorio()
            if rel:
                enviar_msg_telegram(tg_token, tg_chat_ids, rel)
                st.success("Relatório enviado!")
            else: st.warning("Sem dados ainda.")
            
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)

# --- 5. FUNÇÕES DE API (ECONOMIA DE DADOS) ---

# CACHE PARA PRÓXIMOS JOGOS (Evita chamar a API a cada ciclo)
if 'cache_proximos' not in st.session_state:
    st.session_state['cache_proximos'] = {'data': [], 'hora_update': datetime.min}

def buscar_jogos_live(api_key):
    # Essa função busca APENAS jogos ao vivo (live=all). Muito mais leve.
    if MODO_DEMO: return gerar_sinais_teste()
    url = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": api_key}
    try: return requests.get(url, headers=headers, params={"live": "all"}).json().get('response', [])
    except: return []

def buscar_jogos_proximos(api_key):
    # Essa função busca jogos do dia todo, mas com CACHE de 60 minutos.
    agora = datetime.now()
    if (agora - st.session_state['cache_proximos']['hora_update']).total_seconds() < 3600: # 3600s = 1 hora
        return st.session_state['cache_proximos']['data']
    
    if MODO_DEMO: return []
    
    # Se passou 1 hora, baixa de novo
    url = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": api_key}
    data_br = agora_brasil().strftime('%Y-%m-%d')
    try:
        # Pega jogos de hoje no fuso BR
        jogos = requests.get(url, headers=headers, params={"date": data_br, "timezone": "America/Sao_Paulo"}).json().get('response', [])
        # Filtra apenas o que é NS (Not Started) para salvar na lista
        proximos = [j for j in jogos if j['fixture']['status']['short'] == 'NS']
        
        st.session_state['cache_proximos'] = {'data': proximos, 'hora_update': agora}
        return proximos
    except: return []

# FUNÇÕES TESTE E STATS
def gerar_sinais_teste(): 
    return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed
