import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL (DARK MODE) ---
st.set_page_config(
    page_title="Sniper de Gols Pro",
    layout="centered", # Deixa mais focado no meio (bom para celular)
    page_icon="⚽",
    initial_sidebar_state="collapsed"
)

# Estilo CSS para deixar mais bonito (Sumir menu padrão e ajustar cores)
st.markdown("""
<style>
    .stApp {background-color: #0e1117;}
    div[data-testid="stMetricValue"] {font-size: 24px;}
    .big-font {font-size:20px !important; font-weight: bold;}
    .success-box {padding: 15px; background-color: #d4edda; color: #155724; border-radius: 10px; border: 1px solid #c3e6cb;}
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Configurações")
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("API Key RapidAPI:", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.markdown("---")
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação (Visual)", value=True)

# --- FUNÇÕES (MESMA LÓGICA, SÓ MUDA O VISUAL DEPOIS) ---
def buscar_jogos(api_key):
    if MODO_DEMO: return gerar_sinais_teste()
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"date": datetime.today().strftime('%Y-%m-%d')}
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try: return requests.get(url, headers=headers, params=querystring).json().get('response', [])
    except: return []

def buscar_stats(api_key, fixture_id):
    if MODO_DEMO: return gerar_stats_teste(fixture_id)
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try: return requests.get(url, headers=headers, params={"fixture": fixture_id}).json().get('response', [])
    except: return []

def gerar_sinal_explicado(tempo, stats_casa, stats_fora, time_casa, time_fora):
    def v(d, k): val = d.get(k, 0); return int(str(val).replace('%','')) if val else 0

    chutes_total = v(stats_casa, 'Total Shots') + v(stats_fora, 'Total Shots')
    chutes_gol = v(stats_casa, 'Shots on Goal') + v(stats_fora, 'Shots on Goal')
    ataques_p = v(stats_casa, 'Dangerous Attacks') + v(stats_fora, 'Dangerous Attacks')
    atq_c = v(stats_casa, 'Dangerous Attacks')
    atq_f = v(stats_fora, 'Dangerous Attacks')
    
    sinal = None
    insight = ""

    # ESTRATÉGIAS
    if 5 <= tempo <= 20: # Inicio
        if chutes_gol >= 2 or chutes_total >= 4:
            sinal = "GOL CEDO (HT)"
            insight = f"Início frenético! {chutes_total} chutes em {tempo} min."
            
    elif 20 < tempo <= 45: # HT
        if ataques_p >= 25 and chutes_total >= 6:
            sinal = "OVER 0.5 HT (1º Tempo)"
            quem = time_casa if atq_c > atq_f else time_fora
            insight = f"Pressão absurda do {quem}. {ataques_p} ataques perigosos acumulados."

    elif tempo >= 60: # Final
        if chutes_total >= 14 or ataques_p >= 60:
            sinal = "GOL FINAL (FT)"
            insight = f"Jogo aberto na reta final. {chutes_total} finalizações totais."
            
    return sinal, insight, chutes_total, ataques_p, chutes_gol

# --- DADOS FALSOS PARA LAYOUT ---
def gerar_sinais_teste():
    return [
        {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 32}}, "league": {"name": "Premier League"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Arsenal", "logo": "https://media.api-sports.io/football/teams/42.png"}, "away": {"name": "Liverpool", "logo": "https://media.api-sports.io/football/teams/40.png"}}},
        {"fixture": {"id": 2, "status": {"short": "2H", "elapsed": 78}}, "league": {"name": "Brasileirão"}, "goals": {"home": 1, "away": 1}, "teams": {"home": {"name": "Flamengo", "logo": "https://media.api-sports.io/football/teams/127.png"}, "away": {"name": "Palmeiras", "logo": "https://media.api-sports.io/football/teams/121.png"}}}
    ]

def gerar_stats_teste(fid):
    if fid == 1: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 12}, {"type": "Shots on Goal", "value": 5}, {"type": "Dangerous Attacks", "value": 40}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 4}, {"type": "Dangerous Attacks", "value": 10}]}]
    elif fid == 2: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 15}, {"type": "Shots on Goal", "value": 8}, {"type": "Dangerous Attacks", "value": 65}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 10}, {"type": "Dangerous Attacks", "value": 50}]}]
    return []

# --- INTERFACE PRINCIPAL ---

st.title("⚽ Sniper de Gols")
st.caption("Monitoramento de pressão em tempo real")

if st.button("📡 RASTREAR OPORTUNIDADES", use_container_width=True, type="primary"):
    
    if not API_KEY and not MODO_DEMO:
        st.error("⚠️ Insira a API Key na barra lateral.")
    else:
        jogos = buscar_jogos(API_KEY)
        encontrou = False
        
        # Barra de progresso visual
        bar = st.progress(0)
        
        for i, jogo in enumerate(jogos):
            # Atualiza barra
            bar.progress(min((i+1)/len(jogos), 1.0))
            
            status = jogo['fixture']['status']['short']
            tempo = jogo['fixture']['status'].get('elapsed', 0)
            
            if status in ['1H', '2H'] and tempo:
                stats = buscar_stats(API_KEY, jogo['fixture']['id'])
                
                if stats:
                    s_casa = {i['type']: i['value'] for i in stats[0]['statistics']}
                    s_fora = {i['type']: i['value'] for i in stats[1]['statistics']}
                    
                    tc = jogo['teams']['home']['name']
                    tf = jogo['teams']['away']['name']
                    placar = f"{jogo['goals']['home']} - {jogo['goals']['away']}"
                    
                    sinal, motivo, chutes, atq_p, no_gol = gerar_sinal_explicado(tempo, s_casa, s_fora, tc, tf)
                    
                    if sinal:
                        encontrou = True
                        
                        # --- O NOVO LAYOUT DE CARTÃO (CARD) ---
                        with st.container(border=True):
                            
                            # 1. Cabeçalho do Jogo (Times e Placar)
                            col_time1, col_placar, col_time2 = st.columns([2, 1, 2])
                            with col_time1: st.write(f"**{tc}**")
                            with col_placar: 
                                st.markdown(f"<h2 style='text-align: center; margin:0;'>{placar}</h2>", unsafe_allow_html=True)
                                st.markdown(f"<p style='text-align: center; color: red;'>{tempo}'</p>", unsafe_allow_html=True)
                            with col_time2: st.write(f"**{tf}**", )
                            
                            st.divider()
                            
                            # 2. A Ordem de Aposta (Destaque Verde)
                            st.success(f"💰 **ENTRADA:** {sinal}")
                            st.caption(f"📢 **Motivo:** {motivo}")
                            
                            # 3. Métricas (Painel de Controle)
                            c1, c2, c3 = st.columns(3)
                            c1.metric("Chutes Totais", chutes)
                            c2.metric("No Gol", no_gol)
                            c3.metric("Atq. Perigosos", atq_p, delta="Pressão" if atq_p > 30 else None)
                            
                            st.caption(f"Liga: {jogo['league']['name']}")

        bar.empty()
        if not encontrou:
            st.info("Nenhuma oportunidade clara no momento. O mercado está calmo.")
