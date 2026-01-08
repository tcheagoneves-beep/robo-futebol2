import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Sniper de Gols - V13",
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
    st.info("Estratégia V13: Gigantes no Final & Regra dos 10 min.")

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

# --- CÉREBRO V13 (GIGANTES NO FINAL) ---
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
    
    # Identificar Gigante / Super Favorito
    # Odd abaixo de 1.55 indica favoritismo muito forte (Real, Barça, City, etc)
    favorito = None
    nome_favorito = ""
    eh_gigante = False
    
    if odd_casa > 0 and odd_fora > 0:
        if odd_casa <= 1.55: 
            favorito = "CASA"
            nome_favorito = t_casa
            eh_gigante = True
        elif odd_fora <= 1.55: 
            favorito = "FORA"
            nome_favorito = t_fora
            eh_gigante = True
        elif odd_casa <= 1.90: favorito = "CASA"; nome_favorito = t_casa
        elif odd_fora <= 1.90: favorito = "FORA"; nome_favorito = t_fora

    # --- CENÁRIO 1: GIGANTE PRESSIONANDO NO FINAL (70+ min) ---
    if tempo >= 70 and eh_gigante:
        # Se o Gigante estiver ganhando de goleada, ignora.
        # Só interessa se estiver empatando, perdendo, ou ganhando só por 1 gol e querendo matar o jogo.
        
        diff_placar = sc - sf if favorito == "CASA" else sf - sc
        
        if diff_placar <= 1: # Jogo vivo
            # Regra: PRESSÃO MUITO GRANDE (User: "Real, Barça, etc contra pequenos")
            
            stats_gigante_chutes = chutes_c if favorito == "CASA" else chutes_f
            stats_gigante_atq = atq_c if favorito == "CASA" else atq_f
            
            # Critérios de Pressão Extrema
            criterio_chutes = stats_gigante_chutes >= 20 # Muitos chutes
            criterio_perigo = stats_gigante_atq >= 80    # Sufoco total
            
            if criterio_chutes or criterio_perigo:
                sinal = "GOL NO FINAL (GIGANTE)"
                insight = f"""
                **Atenção: Gigante ({nome_favorito}) em Pressão Máxima!**<br>
                Jogo contra time menor, passado dos 70 min.
                O Favorito já acumula **{stats_gigante_chutes} chutes** e **{stats_gigante_atq} ataques perigosos**.
                A regra é clara: quando o grande pressiona assim no final, o gol é inevitável.
                """

    # --- CENÁRIO 2: FAVORITO PERDENDO (REAÇÃO) - Qualquer tempo ---
    elif not sinal and ((favorito == "CASA" and sc < sf) or (favorito == "FORA" and sf < sc)):
        stats_fav_chutes = chutes_c if favorito == "CASA" else chutes_f
        if stats_fav_chutes >= 10: # Pressão de reação
            sinal = f"GOL DO {nome_favorito} (REAÇÃO)"
            insight = f"O Favorito ({nome_favorito}) está perdendo, mas amassando com {stats_fav_chutes} finalizações. O empate está maduro."

    # --- CENÁRIO 3: GOL CEDO (5-15 min) - Regra Assimétrica ---
    elif 5 <= tempo <= 15 and not sinal:
        
        # Lógica de quem é o "Dono do Jogo" agora (quem ataca mais)
        if atq_c >= atq_f: 
            lado_forte = "CASA"; nome_forte = t_casa; gol_forte = gol_c
            lado_fraco = "FORA"; nome_fraco = t_fora; gol_fraco = gol_f
        else:
            lado_forte = "FORA"; nome_forte = t_fora; gol_forte = gol_f
            lado_fraco = "CASA"; nome_fraco = t_casa; gol_fraco = gol_c
        
        # Se o favorito de Odds também é o forte em campo, melhor ainda.
        texto_base = ""
        bateu_regra = False
        
        # Regra 1: Favorito deu 1 chute no gol
        if gol_forte >= 1:
            bateu_regra = True
            texto_base = f"O time que pressiona ({nome_forte}) já chutou no alvo."
            
        # Regra 2: Zebra deu 2 chutes no gol
        if gol_fraco >= 2:
            bateu_regra = True
            texto_base = f"A Zebra ({nome_fraco}) surpreende com 2 chutes no alvo!"
            
        if bateu_regra:
            sinal = "GOL CEDO (HT)"
            insight = f"""
            **Início Intenso ({tempo} min):**
            {texto_base}<br>
            Critério batido (1 chute do favorito ou 2 do adversário). Probabilidade alta de gol.
            """

    # --- CENÁRIO 4: JOGO ABERTO GENÉRICO (Se não for jogo de gigante) ---
    elif tempo >= 70 and not eh_gigante and not sinal:
        if total_chutes >= 18:
            sinal = "GOL NO FINAL (JOGO ABERTO)"
            insight = f"Jogo lá e cá com **{total_chutes} finalizações**. Defesas cansadas e ataques ativos."

    return sinal, insight, total_chutes, (gol_c + gol_f), (atq_c + atq_f)

# --- SIMULAÇÃO ---
def gerar_sinais_teste():
    return [
        {"fixture": {"id": 1, "status": {"short": "2H", "elapsed": 78}}, "league": {"name": "La Liga"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Real Madrid"}, "away": {"name": "Mallorca"}}},
        {"fixture": {"id": 2, "status": {"short": "1H", "elapsed": 12}}, "league": {"name": "Premier League"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Arsenal"}, "away": {"name": "Wolves"}}}
    ]

def gerar_odds_teste(fid):
    if fid == 1: return 1.25, 11.00 # Real Madrid Super Favorito
    if fid == 2: return 1.40, 7.00  # Arsenal Favorito
    return 0, 0

def gerar_stats_teste(fid):
    # Jogo 1: Real Madrid (78 min, 0x0). Tem que ter MUITA pressão.
    # Vamos dar 22 chutes para o Real. Regra DEVE ativar.
    if fid == 1: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 22}, {"type": "Shots on Goal", "value": 8}, {"type": "Dangerous Attacks", "value": 95}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 2}, {"type": "Shots on Goal", "value": 0}, {"type": "Dangerous Attacks", "value": 10}]}]
    
    # Jogo 2: Arsenal (12 min). Favorito chutou 1 no gol. Regra DEVE ativar.
    elif fid == 2: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 4}, {"type": "Shots on Goal", "value": 1}, {"type": "Dangerous Attacks", "value": 18}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 0}, {"type": "Shots on Goal", "value": 0}, {"type": "Dangerous Attacks", "value": 2}]}]
    return []

# --- INTERFACE ---
st.title("🎯 Sniper de Gols - V13 (Gigantes)")

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
