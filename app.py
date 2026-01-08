import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Sniper de Gols - V15",
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
    st.info("Regra V15: Reação Segura (Próximo Gol).")

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

# --- CÉREBRO V15 (REGRAS BLINDADAS) ---
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
    
    # Identificar Favorito
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

    # -------------------------------------------------------------------------
    # CENÁRIO 1: O SUPER FAVORITO LEVOU GOL NO 1º TEMPO (Reação Segura)
    # -------------------------------------------------------------------------
    # Regra: Está perdendo no 1º tempo. Se estiver amassando SOZINHO -> Próximo Gol.
    # Se estiver jogo aberto -> Over Gols.
    
    fav_perdendo = False
    if favorito == "CASA" and sc < sf: fav_perdendo = True
    if favorito == "FORA" and sf < sc: fav_perdendo = True
    
    if tempo <= 50 and fav_perdendo and eh_gigante:
        # Pega stats de quem é quem
        stats_fav_chutes = chutes_c if favorito == "CASA" else chutes_f
        stats_fav_atq = atq_c if favorito == "CASA" else atq_f
        
        stats_zebra_chutes = chutes_f if favorito == "CASA" else chutes_c
        stats_zebra_atq = atq_f if favorito == "CASA" else atq_c
        
        # O Favorito está amassando? (Pelo menos dobro de ataques e min de chutes)
        pressao_fav = (stats_fav_atq > stats_zebra_atq * 2) and (stats_fav_chutes >= 6)
        
        # A Zebra está morta ou viva?
        zebra_viva = (stats_zebra_chutes >= 4) or (stats_zebra_atq >= 15)
        
        if pressao_fav:
            if not zebra_viva:
                # CASO A: Favorito amassando zebra morta
                sinal = f"PRÓXIMO GOL: {nome_favorito}"
                insight = f"""
                **Reação do Gigante ({tempo} min):**<br>
                O {nome_favorito} tomou gol mas domina totalmente o jogo. A Zebra recuou e parou de atacar.
                **Não aposte em Over HT.** Aposte que o **Favorito marca o próximo gol** (pode ser no 1º ou 2º tempo).
                """
            else:
                # CASO B: Jogo Aberto (Lá e Cá)
                sinal = "GOL (QUALQUER UM) - JOGO ABERTO"
                insight = f"""
                **Cuidado:** O Favorito pressiona, mas a Zebra está perigosa no contra-ataque!
                Não aposte só no favorito. Como os dois times chegam, a aposta segura é em **Mais um Gol na partida**.
                """

    # -------------------------------------------------------------------------
    # CENÁRIO 2: JANELA DE OURO (70-75 min) - Gigante não ganhando
    # -------------------------------------------------------------------------
    elif 70 <= tempo <= 75 and eh_gigante and not sinal:
        # Verifica se NÃO está ganhando
        nao_ganhando = (favorito == "CASA" and sc <= sf) or (favorito == "FORA" and sf <= sc)
        
        if nao_ganhando:
            stats_gigante_chutes = chutes_c if favorito == "CASA" else chutes_f
            stats_gigante_atq = atq_c if favorito == "CASA" else atq_f
            
            if stats_gigante_chutes >= 18 or stats_gigante_atq >= 75:
                sinal = "GOL (PRESSÃO FINAL)"
                insight = f"O Gigante ({nome_favorito}) precisa do gol e abriu a caixa de ferramentas ({stats_gigante_chutes} chutes). Janela de entrada válida até 75'."

    # -------------------------------------------------------------------------
    # CENÁRIO 3: GOL CEDO (5-15 min) - Abertura de Placar
    # -------------------------------------------------------------------------
    elif 5 <= tempo <= 15 and not sinal:
        if atq_c >= atq_f: 
            nome_forte = t_casa; gol_forte = gol_c
            nome_fraco = t_fora; gol_fraco = gol_f
        else:
            nome_forte = t_fora; gol_forte = gol_f
            nome_fraco = t_casa; gol_fraco = gol_c
        
        bateu = False
        txt = ""
        
        if gol_forte >= 1: bateu = True; txt = f"Dominante ({nome_forte}) chutou no alvo."
        if gol_fraco >= 2: bateu = True; txt = f"Zebra ({nome_fraco}) chutou 2x no alvo."
            
        if bateu:
            sinal = "GOL CEDO (HT)"
            insight = f"Início Intenso ({tempo} min). {txt} Chance de gol alta."

    return sinal, insight, total_chutes, (gol_c + gol_f), (atq_c + atq_f)

# --- SIMULAÇÃO ---
def gerar_sinais_teste():
    return [
        {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 30}}, "league": {"name": "La Liga"}, "goals": {"home": 0, "away": 1}}, "teams": {"home": {"name": "Real Madrid"}, "away": {"name": "Almeria"}}}, # Cenário 1
        {"fixture": {"id": 2, "status": {"short": "1H", "elapsed": 30}}, "league": {"name": "Premier League"}, "goals": {"home": 0, "away": 1}}, "teams": {"home": {"name": "Man City"}, "away": {"name": "Spurs"}}}  # Cenário 2
    ]

def gerar_odds_teste(fid):
    return 1.20, 15.00 # Ambos Super Favoritos (Odd 1.20)

def gerar_stats_teste(fid):
    # Jogo 1: Real Madrid perdendo, amassando (15 chutes), Almeria morto (1 chute).
    # SINAL ESPERADO: PRÓXIMO GOL REAL MADRID.
    if fid == 1: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 15}, {"type": "Shots on Goal", "value": 6}, {"type": "Dangerous Attacks", "value": 45}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 1}, {"type": "Shots on Goal", "value": 1}, {"type": "Dangerous Attacks", "value": 5}]}]
    
    # Jogo 2: City perdendo, atacando muito, MAS Spurs também ataca (6 chutes).
    # SINAL ESPERADO: GOL (QUALQUER UM) - JOGO ABERTO.
    elif fid == 2: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 12}, {"type": "Shots on Goal", "value": 5}, {"type": "Dangerous Attacks", "value": 30}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 6}, {"type": "Shots on Goal", "value": 3}, {"type": "Dangerous Attacks", "value": 20}]}]
    return []

# --- INTERFACE ---
st.title("🎯 Sniper de Gols - V15")

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
