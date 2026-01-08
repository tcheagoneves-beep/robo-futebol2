import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Sniper de Gols - Pro",
    layout="centered",
    page_icon="⚽"
)

# Estilos CSS (Visual)
st.markdown("""
<style>
    .stApp {background-color: #121212; color: white;}
    .card {
        background-color: #1E1E1E;
        padding: 20px;
        border-radius: 15px;
        border: 1px solid #333;
        margin-bottom: 20px;
    }
    .titulo-time {font-size: 20px; font-weight: bold; color: #ffffff;}
    .placar {font-size: 35px; font-weight: 800; color: #FFD700; text-align: center;}
    .tempo {font-size: 14px; color: #FF4B4B; font-weight: bold; text-align: center;}
    .sinal-box {
        background-color: #00C853; 
        color: white; 
        padding: 10px; 
        border-radius: 8px; 
        text-align: center; 
        font-size: 18px; 
        font-weight: bold;
        margin-top: 15px;
        box-shadow: 0 4px 15px rgba(0, 200, 83, 0.4);
    }
    .insight-texto {
        font-size: 14px; 
        color: #CCCCCC; 
        margin-top: 10px; 
        line-height: 1.4;
        border-left: 3px solid #FFD700;
        padding-left: 10px;
    }
    .stats-row {
        display: flex; 
        justify-content: space-around; 
        text-align: center; 
        margin-top: 15px; 
        padding-top: 15px; 
        border-top: 1px solid #333;
    }
    .metric-val {font-size: 22px; font-weight: bold;}
    .metric-label {font-size: 11px; color: #888;}
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Painel de Controle")
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("Chave API (RapidAPI):", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.markdown("---")
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=True)

# --- DADOS ---
def buscar_jogos(api_key):
    if MODO_DEMO: return gerar_sinais_teste()
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try: return requests.get(url, headers=headers, params={"date": datetime.today().strftime('%Y-%m-%d')}).json().get('response', [])
    except: return []

def buscar_stats(api_key, fixture_id):
    if MODO_DEMO: return gerar_stats_teste(fixture_id)
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try: return requests.get(url, headers=headers, params={"fixture": fixture_id}).json().get('response', [])
    except: return []

# --- CÉREBRO ---
def gerar_analise_completa(tempo, s_casa, s_fora, t_casa, t_fora):
    def v(d, k): val = d.get(k, 0); return int(str(val).replace('%','')) if val else 0

    chutes_c = v(s_casa, 'Total Shots')
    chutes_f = v(s_fora, 'Total Shots')
    total_chutes = chutes_c + chutes_f
    
    gol_c = v(s_casa, 'Shots on Goal')
    gol_f = v(s_fora, 'Shots on Goal')
    total_gol = gol_c + gol_f
    
    atq_c = v(s_casa, 'Dangerous Attacks')
    atq_f = v(s_fora, 'Dangerous Attacks')
    total_atq = atq_c + atq_f
    
    sinal = None
    insight = ""

    # Lógica de Sinais
    if 5 <= tempo <= 25:
        if total_gol >= 3 or total_chutes >= 6:
            sinal = "GOL NO 1º TEMPO (HT)"
            insight = f"🔥 **Início Quente!** {total_chutes} chutes em apenas {tempo} minutos."

    elif 25 < tempo <= 45:
        if total_atq > 30 and total_chutes >= 8:
            sinal = "GOL ANTES DO INTERVALO"
            quem = t_casa if atq_c > atq_f else t_fora
            insight = f"🚨 **Pressão do {quem}!** O time amassou o adversário com {total_atq} ataques perigosos."

    elif tempo >= 65:
        if total_chutes >= 15 or total_atq >= 70:
            sinal = "GOL NO FINAL (FT)"
            insight = f"⚡ **Jogo Aberto!** {total_chutes} finalizações totais. Um dos times vai marcar no final."

    return sinal, insight, total_chutes, total_gol, total_atq

# --- TESTE ---
def gerar_sinais_teste():
    return [
        {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 34}}, "league": {"name": "Premier League"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Arsenal"}, "away": {"name": "Liverpool"}}},
        {"fixture": {"id": 2, "status": {"short": "2H", "elapsed": 82}}, "league": {"name": "Copa do Brasil"}, "goals": {"home": 1, "away": 1}, "teams": {"home": {"name": "Corinthians"}, "away": {"name": "Flamengo"}}}
    ]

def gerar_stats_teste(fid):
    if fid == 1: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 14}, {"type": "Shots on Goal", "value": 6}, {"type": "Dangerous Attacks", "value": 45}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 2}, {"type": "Dangerous Attacks", "value": 10}]}]
    elif fid == 2: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 12}, {"type": "Shots on Goal", "value": 5}, {"type": "Dangerous Attacks", "value": 50}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 10}, {"type": "Shots on Goal", "value": 4}, {"type": "Dangerous Attacks", "value": 45}]}]
    return []

# --- INTERFACE ---
st.title("🎯 Sniper de Gols")

if st.button("📡 RASTREAR AGORA", type="primary", use_container_width=True):
    
    if not API_KEY and not MODO_DEMO:
        st.error("⚠️ Configure a API Key na barra lateral.")
    else:
        jogos = buscar_jogos(API_KEY)
        achou = False
        bar = st.progress(0)
        
        for i, jogo in enumerate(jogos):
            bar.progress(min((i+1)/len(jogos), 1.0))
            
            tempo = jogo['fixture']['status'].get('elapsed', 0)
            status = jogo['fixture']['status']['short']
            
            if status in ['1H', '2H'] and tempo:
                stats = buscar_stats(API_KEY, jogo['fixture']['id'])
                if stats:
                    s_casa = {i['type']: i['value'] for i in stats[0]['statistics']}
                    s_fora = {i['type']: i['value'] for i in stats[1]['statistics']}
                    tc = jogo['teams']['home']['name']
                    tf = jogo['teams']['away']['name']
                    
                    sinal, motivo, chutes, no_gol, atq_p = gerar_analise_completa(tempo, s_casa, s_fora, tc, tf)
                    
                    if sinal:
                        achou = True
                        
                        # Construção do HTML sem indentação para evitar erro
                        html_card = f"""
                        <div class="card">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <div style="width:40%; text-align:left;"><div class="titulo-time">{tc}</div></div>
                                <div style="width:20%; text-align:center;">
                                    <div class="placar">{jogo['goals']['home']} - {jogo['goals']['away']}</div>
                                    <div class="tempo">{tempo}'</div>
                                </div>
                                <div style="width:40%; text-align:right;"><div class="titulo-time">{tf}</div></div>
                            </div>
                            
                            <div class="sinal-box">💰 {sinal}</div>
                            <div class="insight-texto">{motivo}</div>
                            
                            <div class="stats-row">
                                <div><div class="metric-label">CHUTES</div><div class="metric-val">{chutes}</div></div>
                                <div><div class="metric-label">NO GOL</div><div class="metric-val" style="color:#00C853;">{no_gol}</div></div>
                                <div><div class="metric-label">PERIGO</div><div class="metric-val" style="color:#FFD700;">{atq_p}</div></div>
                            </div>
                        </div>
                        """
                        st.markdown(html_card, unsafe_allow_html=True)

        bar.empty()
        if not achou:
            st.info("Nenhuma oportunidade encontrada.")
