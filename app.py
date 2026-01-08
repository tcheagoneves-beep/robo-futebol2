import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Sniper de Gols - Odds Edition",
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
    st.info("Estratégia: Odds Pré-Live definem o Favorito.")

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

# NOVA FUNÇÃO: BUSCAR ODDS REAIS
def buscar_odds_pre_match(api_key, fixture_id):
    if MODO_DEMO: return gerar_odds_teste(fixture_id)
    url = "https://api-football-v1.p.rapidapi.com/v3/odds"
    try:
        # Busca odds da Bet365 (bookmaker=1)
        data = requests.get(url, headers={"X-RapidAPI-Key": api_key}, params={"fixture": fixture_id, "bookmaker": "1"}).json()
        if data.get('response'):
            bets = data['response'][0]['bookmakers'][0]['bets']
            # Procura a aposta "Match Winner" (id=1)
            winner_bet = next((b for b in bets if b['id'] == 1), None)
            if winner_bet:
                odd_casa = float(next((v['odd'] for v in winner_bet['values'] if v['value'] == 'Home'), 0))
                odd_fora = float(next((v['odd'] for v in winner_bet['values'] if v['value'] == 'Away'), 0))
                return odd_casa, odd_fora
        return 0, 0
    except: return 0, 0

# --- CÉREBRO V12 (ODDS REAIS) ---
def analisar_partida(tempo, s_casa, s_fora, t_casa, t_fora, sc, sf, odd_casa, odd_fora):
    
    # 1. Tratamento de Stats
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
    
    # --- DEFINIÇÃO DE FAVORITO PELAS ODDS ---
    favorito = None
    nome_favorito = ""
    eh_classico = False
    
    if odd_casa > 0 and odd_fora > 0:
        if odd_casa <= 1.90: 
            favorito = "CASA"
            nome_favorito = t_casa
        elif odd_fora <= 1.90: 
            favorito = "FORA"
            nome_favorito = t_fora
        elif abs(odd_casa - odd_fora) < 0.5: # Odds próximas (Ex: 2.40 vs 2.60)
            eh_classico = True
    else:
        # Se não tiver odd (fallback), usa stats
        if atq_c > atq_f * 1.5: favorito = "CASA"; nome_favorito = t_casa
        elif atq_f > atq_c * 1.5: favorito = "FORA"; nome_favorito = t_fora

    # --- CENÁRIO 1: GOL CEDO (5-15 min) ---
    if 5 <= tempo <= 15:
        # REGRA DO USUÁRIO BASEADA EM ODDS
        gatilho = False
        texto_gatilho = ""
        
        if favorito == "CASA":
            if gol_c >= 1: # Favorito chutou 1
                gatilho = True
                texto_gatilho = f"O Favorito ({t_casa} - Odd {odd_casa}) já chutou no gol."
            elif gol_f >= 2: # Zebra chutou 2
                gatilho = True
                texto_gatilho = f"A Zebra ({t_fora}) surpreende com 2 chutes no alvo!"
                
        elif favorito == "FORA":
            if gol_f >= 1: # Favorito chutou 1
                gatilho = True
                texto_gatilho = f"O Favorito ({t_fora} - Odd {odd_fora}) já chutou no gol."
            elif gol_c >= 2: # Zebra chutou 2
                gatilho = True
                texto_gatilho = f"A Zebra ({t_casa}) surpreende com 2 chutes no alvo!"
        
        # Se for clássico/equilibrado, exige 1 de cada ou 2 de alguém
        elif eh_classico:
            if (gol_c >= 1 and gol_f >= 1) or (gol_c + gol_f >= 3):
                gatilho = True
                texto_gatilho = "Jogo Equilibrado (Clássico) com chances claras para ambos."

        if gatilho:
            sinal = "GOL CEDO (HT)"
            insight = f"""
            **Critério de Odds Batido:**
            {texto_gatilho}<br>
            Jogo intenso com {tempo} minutos. Probabilidade alta de abertura de placar.
            """

    # --- CENÁRIO 2: FAVORITO PERDENDO (REAÇÃO) ---
    elif (favorito == "CASA" and sc < sf) or (favorito == "FORA" and sf < sc):
        # Time favorito perdendo... ele está amassando?
        quem_perde = s_casa if favorito == "CASA" else s_fora
        chutes_fav = chutes_c if favorito == "CASA" else chutes_f
        
        if chutes_fav >= 8 or (atq_c + atq_f) > 40:
            sinal = f"GOL DO {nome_favorito} (REAÇÃO)"
            insight = f"O Favorito (Odd {odd_casa if favorito=='CASA' else odd_fora}) está perdendo, mas amassa com {chutes_fav} finalizações."

    # --- CENÁRIO 3: FIM DE JOGO ---
    elif tempo >= 70:
        if total_chutes >= 18:
            sinal = "GOL NO FINAL (FT)"
            insight = f"Jogo totalmente aberto. {total_chutes} finalizações totais."

    return sinal, insight, total_chutes, (gol_c + gol_f), (atq_c + atq_f)

# --- SIMULAÇÃO DE DADOS + ODDS ---
def gerar_sinais_teste():
    return [
        {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 9}}, "league": {"name": "Premier League"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Man City"}, "away": {"name": "Luton"}}},
        {"fixture": {"id": 2, "status": {"short": "1H", "elapsed": 10}}, "league": {"name": "La Liga"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Alaves"}, "away": {"name": "Real Madrid"}}}
    ]

def gerar_odds_teste(fid):
    # Jogo 1: City Favoritaço (1.10 vs 15.0)
    if fid == 1: return 1.10, 15.00
    # Jogo 2: Real Madrid Favorito Fora (5.00 vs 1.40)
    elif fid == 2: return 5.00, 1.40
    return 0, 0

def gerar_stats_teste(fid):
    # Jogo 1: City (Fav) deu 1 chute no gol. TEM QUE DAR GREEN (Regra: Fav 1 chute).
    if fid == 1: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 3}, {"type": "Shots on Goal", "value": 1}, {"type": "Dangerous Attacks", "value": 15}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 0}, {"type": "Shots on Goal", "value": 0}, {"type": "Dangerous Attacks", "value": 0}]}]
    
    # Jogo 2: Real (Fav) não chutou. Alaves (Zebra) chutou 2 vezes. TEM QUE DAR GREEN (Regra: Zebra 2 chutes).
    elif fid == 2: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 3}, {"type": "Shots on Goal", "value": 2}, {"type": "Dangerous Attacks", "value": 10}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 1}, {"type": "Shots on Goal", "value": 0}, {"type": "Dangerous Attacks", "value": 5}]}]
    return []

# --- INTERFACE ---
st.title("🎯 Sniper de Gols - Odds")

if st.button("📡 RASTREAR (COM ODDS)", type="primary", use_container_width=True):
    
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
            
            # FILTRO DE TEMPO INTELIGENTE PARA POUPAR API DE ODDS
            # Só busca odd se o jogo estiver nos momentos chave ou no modo demo
            if status in ['1H', '2H'] and tempo:
                
                # 1. Busca Estatísticas
                stats = buscar_stats(API_KEY, jogo['fixture']['id'])
                
                if stats:
                    # 2. Busca Odds (AQUI GASTA COTA NO MODO REAL)
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
            st.info("Nenhuma oportunidade encontrada agora.")
