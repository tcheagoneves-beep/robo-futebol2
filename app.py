import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Sniper de Gols - V10",
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
    .insight-titulo {
        color: #FFD700;
        font-weight: bold;
        margin-top: 15px;
        font-size: 14px;
        text-transform: uppercase;
    }
    .insight-texto {
        font-size: 15px; 
        color: #E0E0E0; 
        margin-top: 5px; 
        line-height: 1.5;
        background-color: #2b2b2b;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #FFD700;
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
    .metric-label {font-size: 10px; color: #888; text-transform: uppercase;}
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

# --- CÉREBRO ANALÍTICO V10 (Regra de Clássicos) ---
def gerar_analise_completa(tempo, s_casa, s_fora, t_casa, t_fora, score_casa, score_fora):
    def v(d, k): val = d.get(k, 0); return int(str(val).replace('%','')) if val else 0

    chutes_c = v(s_casa, 'Total Shots')
    chutes_f = v(s_fora, 'Total Shots')
    gol_c = v(s_casa, 'Shots on Goal')
    gol_f = v(s_fora, 'Shots on Goal')
    atq_c = v(s_casa, 'Dangerous Attacks')
    atq_f = v(s_fora, 'Dangerous Attacks')
    
    total_chutes = chutes_c + chutes_f
    total_gol_alvo = gol_c + gol_f
    
    sinal = None
    insight = ""

    # --- 1. DETECTOR DE REAÇÃO (Favorito Perdendo) ---
    if score_casa < score_fora:
        if (atq_c > atq_f * 1.5) or (chutes_c > 12 and chutes_c > chutes_f * 2):
            sinal = f"GOL DO {t_casa} (REAÇÃO)"
            insight = f"O favorito ({t_casa}) está perdendo mas tem **{chutes_c} finalizações** (Volume de massacre). O empate é iminente."
            
    elif score_fora < score_casa:
        if (atq_f > atq_c * 1.5) or (chutes_f > 12 and chutes_f > chutes_c * 2):
            sinal = f"GOL DO {t_fora} (REAÇÃO)"
            insight = f"O favorito ({t_fora}) reagiu ao gol sofrido e já soma **{atq_f} ataques perigosos**. Pressão total para empatar."

    # --- 2. GOL CEDO (Regra Clássicos/Top Times - Até 10 min) ---
    elif 5 <= tempo <= 15 and not sinal:
        
        # Regra do Usuário: 1 de cada lado OU 2 de um time só
        condicao_classico_equilibrado = (gol_c >= 1 and gol_f >= 1)
        condicao_massacre_solo = (gol_c >= 2 or gol_f >= 2)
        
        if condicao_classico_equilibrado or condicao_massacre_solo:
            sinal = "GOL CEDO (HT)"
            
            explicacao_regra = ""
            if condicao_classico_equilibrado:
                explicacao_regra = "Critério de Clássico: **1 chute no alvo de cada time**."
            else:
                quem = t_casa if gol_c >= 2 else t_fora
                qtd = gol_c if gol_c >= 2 else gol_f
                explicacao_regra = f"Critério de Pressão: O **{quem}** já chutou **{qtd} vezes no alvo** sozinho."
            
            insight = f"""
            Início frenético ({tempo} min). Goleiros já trabalhando.
            {explicacao_regra}<br>
            Cenário ideal para gol nos primeiros minutos (Defesas abertas ou qualidade técnica alta).
            """

    # --- 3. FIM DE JOGO ---
    elif tempo >= 65 and not sinal:
        if total_chutes >= 16:
            sinal = "GOL NO FINAL (FT)"
            insight = f"Jogo aberto com **{total_chutes} finalizações**. Um time ataca e o outro contra-ataca. Alta probabilidade de gol."

    return sinal, insight, total_chutes, total_gol_alvo, (atq_c + atq_f)

# --- DADOS DE TESTE V10 ---
def gerar_sinais_teste():
    return [
        {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 9}}, "league": {"name": "Premier League"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Arsenal"}, "away": {"name": "Chelsea"}}},
        {"fixture": {"id": 2, "status": {"short": "2H", "elapsed": 70}}, "league": {"name": "La Liga"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Real Madrid"}, "away": {"name": "Barcelona"}}}
    ]

def gerar_stats_teste(fid):
    # Cenário 1: Clássico Equilibrado (1 chute no gol de cada)
    if fid == 1: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 3}, {"type": "Shots on Goal", "value": 1}, {"type": "Dangerous Attacks", "value": 10}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 2}, {"type": "Shots on Goal", "value": 1}, {"type": "Dangerous Attacks", "value": 12}]}]
    
    # Cenário 2: Real Madrid perdendo e amassando (Reação)
    elif fid == 2: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 18}, {"type": "Shots on Goal", "value": 8}, {"type": "Dangerous Attacks", "value": 70}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 5}, {"type": "Shots on Goal", "value": 2}, {"type": "Dangerous Attacks", "value": 20}]}]
    return []

# --- INTERFACE ---
st.title("🎯 Sniper de Gols - Analista V10")

if st.button("📡 ANALISAR MERCADO", type="primary", use_container_width=True):
    
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
                    sc = jogo['goals']['home'] if jogo['goals']['home'] is not None else 0
                    sf = jogo['goals']['away'] if jogo['goals']['away'] is not None else 0
                    
                    sinal, motivo, chutes, no_gol, atq_p = gerar_analise_completa(tempo, s_casa, s_fora, tc, tf, sc, sf)
                    
                    if sinal:
                        achou = True
                        st.markdown(f"""
<div class="card">
<div style="display:flex; justify-content:space-between; align-items:center;">
<div style="width:40%; text-align:left;"><div class="titulo-time">{tc}</div></div>
<div style="width:20%; text-align:center;">
<div class="placar">{sc} - {sf}</div>
<div class="tempo">{tempo}'</div>
</div>
<div style="width:40%; text-align:right;"><div class="titulo-time">{tf}</div></div>
</div>
<div class="sinal-box">💰 {sinal}</div>
<div class="insight-titulo">📊 Motivo da Entrada:</div>
<div class="insight-texto">{motivo}</div>
<div class="stats-row">
<div><div class="metric-label">CHUTES TOTAIS</div><div class="metric-val">{chutes}</div></div>
<div><div class="metric-label">NO GOL (ALVO)</div><div class="metric-val" style="color:#00C853;">{no_gol}</div></div>
<div><div class="metric-label">ATQ. PERIGOSOS</div><div class="metric-val" style="color:#FFD700;">{atq_p}</div></div>
</div>
</div>
""", unsafe_allow_html=True)

        bar.empty()
        if not achou:
            st.info("Nenhuma oportunidade encontrada.")
