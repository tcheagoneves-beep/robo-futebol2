import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Sniper de Gols - V14",
    layout="centered",
    page_icon="⚽"
)

# Estilos CSS
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
    .odd-label {font-size: 12px; color: #aaa; background-color: #333; padding: 2px 6px; border-radius: 4px;}
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
        font-size: 15px; 
        color: #E0E0E0; 
        margin-top: 10px; 
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
    st.header("⚙️ Configurações")
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("API Key (RapidAPI):", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.markdown("---")
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=True)
    st.info("Regra V14: Gigantes só até 75 min e sem estar ganhando.")

# --- CONEXÕES API ---
def buscar_jogos(api_key):
    if MODO_DEMO: return gerar_sinais_teste()
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    try: return requests.get(url, headers={"X-RapidAPI-Key": api_key}, params={"date": datetime.today().strftime('%Y-%m-%d')}).json().get('response', [])
    except: return []

def buscar_stats(api_key, fixture_id):
    if MODO_DEMO: return gerar_stats_teste(fixture_id)
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    try: return requests.get(url, headers={"X-RapidAPI-Key": api_key}, params={"fixture": fixture_id}).json().get('response', [])
    except: return []

def buscar_odds_pre_match(api_key, fixture_id):
    if MODO_DEMO: return gerar_odds_teste(fixture_id)
    url = "https://api-football-v1.p.rapidapi.com/v3/odds"
    try:
        data = requests.get(url, headers={"X-RapidAPI-Key": api_key}, params={"fixture": fixture_id, "bookmaker": "1"}).json()
        if data.get('response'):
            bets = data['response'][0]['bookmakers'][0]['bets']
            winner_bet = next((b for b in bets if b['id'] == 1), None)
            if winner_bet:
                odd_casa = float(next((v['odd'] for v in winner_bet['values'] if v['value'] == 'Home'), 0))
                odd_fora = float(next((v['odd'] for v in winner_bet['values'] if v['value'] == 'Away'), 0))
                return odd_casa, odd_fora
        return 0, 0
    except: return 0, 0

# --- CÉREBRO V14 (SEGURANÇA TOTAL) ---
def analisar_partida(tempo, s_casa, s_fora, t_casa, t_fora, sc, sf, odd_casa, odd_fora):
    
    def v(d, k): val = d.get(k, 0); return int(str(val).replace('%','')) if val else 0
    gol_c = v(s_casa, 'Shots on Goal')
    gol_f = v(s_fora, 'Shots on Goal')
    chutes_c = v(s_casa, 'Total Shots')
    chutes_f = v(s_fora, 'Total Shots')
    atq_c = v(s_casa, 'Dangerous Attacks')
    atq_f = v(s_fora, 'Dangerous Attacks')
    
    total_chutes = chutes_c + chutes_f
    
    sinal = None
    insight = ""
    
    # Identificar Gigante
    favorito = None
    nome_favorito = ""
    eh_gigante = False
    
    if odd_casa > 0 and odd_fora > 0:
        if odd_casa <= 1.55: 
            favorito = "CASA"; nome_favorito = t_casa; eh_gigante = True
        elif odd_fora <= 1.55: 
            favorito = "FORA"; nome_favorito = t_fora; eh_gigante = True
        elif odd_casa <= 1.90: favorito = "CASA"; nome_favorito = t_casa
        elif odd_fora <= 1.90: favorito = "FORA"; nome_favorito = t_fora

    # --- CENÁRIO 1: GIGANTE PRESSIONANDO (Janela 70-75 min) ---
    # Só analisa se estiver dentro da janela exata. Passou de 75, esquece.
    if 70 <= tempo <= 75 and eh_gigante:
        
        # REGRA 1: PLACAR (Não pode estar ganhando)
        # Se Favorito é Casa, SC deve ser <= SF (Empate ou Perdendo)
        # Se Favorito é Fora, SF deve ser <= SC
        
        placar_favoravel_aposta = False
        if favorito == "CASA" and sc <= sf: placar_favoravel_aposta = True
        if favorito == "FORA" and sf <= sc: placar_favoravel_aposta = True
        
        if placar_favoravel_aposta:
            # REGRA 2: PRESSÃO EXTREMA
            stats_gigante_chutes = chutes_c if favorito == "CASA" else chutes_f
            stats_gigante_atq = atq_c if favorito == "CASA" else atq_f
            
            criterio_chutes = stats_gigante_chutes >= 20
            criterio_perigo = stats_gigante_atq >= 80
            
            if criterio_chutes or criterio_perigo:
                sinal = "GOL (GIGANTE EM APUROS)"
                insight = f"""
                **Janela de Ouro ({tempo} min):**<br>
                O Gigante ({nome_favorito}) **NÃO ESTÁ GANHANDO** e a pressão é absurda ({stats_gigante_chutes} chutes).
                O Robô detectou que o time precisa do gol urgentemente antes do fim. Entrada válida somente até os 75 min.
                """

    # --- CENÁRIO 2: FAVORITO PERDENDO (REAÇÃO) - Qualquer tempo antes dos 80 ---
    elif not sinal and tempo < 80 and ((favorito == "CASA" and sc < sf) or (favorito == "FORA" and sf < sc)):
        stats_fav_chutes = chutes_c if favorito == "CASA" else chutes_f
        if stats_fav_chutes >= 10:
            sinal = f"GOL DO {nome_favorito} (REAÇÃO)"
            insight = f"O Favorito ({nome_favorito}) está perdendo, mas amassando com {stats_fav_chutes} finalizações."

    # --- CENÁRIO 3: GOL CEDO (5-15 min) - Assimétrica ---
    elif 5 <= tempo <= 15 and not sinal:
        if atq_c >= atq_f: 
            nome_forte = t_casa; gol_forte = gol_c
            nome_fraco = t_fora; gol_fraco = gol_f
        else:
            nome_forte = t_fora; gol_forte = gol_f
            nome_fraco = t_casa; gol_fraco = gol_c
        
        bateu_regra = False
        texto_base = ""
        
        if gol_forte >= 1:
            bateu_regra = True
            texto_base = f"O time que pressiona ({nome_forte}) já chutou no alvo."
        if gol_fraco >= 2:
            bateu_regra = True
            texto_base = f"A Zebra ({nome_fraco}) surpreende com 2 chutes no alvo!"
            
        if bateu_regra:
            sinal = "GOL CEDO (HT)"
            insight = f"**Início Intenso ({tempo} min):** {texto_base} Critério batido."

    return sinal, insight, total_chutes, (gol_c + gol_f), (atq_c + atq_f)

# --- SIMULAÇÃO ---
def gerar_sinais_teste():
    return [
        {"fixture": {"id": 1, "status": {"short": "2H", "elapsed": 73}}, "league": {"name": "La Liga"}, "goals": {"home": 1, "away": 1}, "teams": {"home": {"name": "Real Madrid"}, "away": {"name": "Mallorca"}}},
        {"fixture": {"id": 2, "status": {"short": "2H", "elapsed": 78}}, "league": {"name": "Ligue 1"}, "goals": {"home": 1, "away": 1}, "teams": {"home": {"name": "PSG"}, "away": {"name": "Nantes"}}}
    ]

def gerar_odds_teste(fid):
    if fid == 1: return 1.25, 11.00 # Real Gigante
    if fid == 2: return 1.20, 13.00 # PSG Gigante
    return 0, 0

def gerar_stats_teste(fid):
    # Jogo 1: Real Madrid (73 min, 1x1). DENTRO DA JANELA E EMPATANDO. (DEVE DAR SINAL)
    if fid == 1: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 24}, {"type": "Shots on Goal", "value": 10}, {"type": "Dangerous Attacks", "value": 90}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 3}, {"type": "Shots on Goal", "value": 1}, {"type": "Dangerous Attacks", "value": 15}]}]
    
    # Jogo 2: PSG (78 min, 1x1). FORA DA JANELA (>75). (NÃO DEVE DAR SINAL)
    elif fid == 2: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 25}, {"type": "Shots on Goal", "value": 12}, {"type": "Dangerous Attacks", "value": 100}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 2}, {"type": "Shots on Goal", "value": 1}, {"type": "Dangerous Attacks", "value": 10}]}]
    return []

# --- INTERFACE ---
st.title("🎯 Sniper de Gols - V14")

if st.button("📡 RASTREAR", type="primary", use_container_width=True):
    
    if not API_KEY and not MODO_DEMO:
        st.error("⚠️ Coloque a API Key!")
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
                    odd_casa, odd_fora = buscar_odds_pre_match(API_KEY, jogo['fixture']['id'])
                    
                    s_casa = {i['type']: i['value'] for i in stats[0]['statistics']}
                    s_fora = {i['type']: i['value'] for i in stats[1]['statistics']}
                    tc = jogo['teams']['home']['name']
                    tf = jogo['teams']['away']['name']
                    sc = jogo['goals']['home'] or 0
                    sf = jogo['goals']['away'] or 0
                    
                    sinal, motivo, chutes, no_gol, atq_p = analisar_partida(tempo, s_casa, s_fora, tc, tf, sc, sf, odd_casa, odd_fora)
                    
                    if sinal:
                        achou = True
                        st.markdown(f"""
<div class="card">
<div style="display:flex; justify-content:space-between; align-items:center;">
<div style="width:40%; text-align:left;">
    <div class="titulo-time">{tc}</div>
    <span class="odd-label">Odd: {odd_casa:.2f}</span>
</div>
<div style="width:20%; text-align:center;">
    <div class="placar">{sc} - {sf}</div>
    <div class="tempo">{tempo}'</div>
</div>
<div style="width:40%; text-align:right;">
    <div class="titulo-time">{tf}</div>
    <span class="odd-label">Odd: {odd_fora:.2f}</span>
</div>
</div>

<div class="sinal-box">💰 {sinal}</div>

<div class="insight-texto">
    <b>Motivo:</b> {motivo}
</div>

<div class="stats-row">
<div><div class="metric-label">CHUTES</div><div class="metric-val">{chutes}</div></div>
<div><div class="metric-label">NO GOL</div><div class="metric-val" style="color:#00C853;">{no_gol}</div></div>
<div><div class="metric-label">PERIGO</div><div class="metric-val" style="color:#FFD700;">{atq_p}</div></div>
</div>
</div>
""", unsafe_allow_html=True)

        bar.empty()
        if not achou:
            st.info("Nenhuma oportunidade encontrada.")
