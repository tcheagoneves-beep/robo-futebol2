import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Sniper de Gols - Expert",
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
        font-size: 16px;
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

# --- CÉREBRO ANALÍTICO (Sua Estratégia Detalhada) ---
def gerar_analise_completa(tempo, s_casa, s_fora, t_casa, t_fora):
    def v(d, k): val = d.get(k, 0); return int(str(val).replace('%','')) if val else 0

    chutes_c = v(s_casa, 'Total Shots')
    chutes_f = v(s_fora, 'Total Shots')
    
    gol_c = v(s_casa, 'Shots on Goal')
    gol_f = v(s_fora, 'Shots on Goal')
    soma_gol = gol_c + gol_f
    
    atq_c = v(s_casa, 'Dangerous Attacks')
    atq_f = v(s_fora, 'Dangerous Attacks')
    
    sinal = None
    insight = ""

    # --- ESTRATÉGIA 1: INÍCIO (A Regra dos 10 Minutos) ---
    if 5 <= tempo <= 15:
        # Regra do Pai: Mínimo 1 chute no gol do favorito ou Soma >= 2 (Clássico)
        
        # Tenta identificar favorito pelos Ataques Perigosos (quem ataca mais)
        favorito = t_casa if atq_c > atq_f else t_fora
        chutes_gol_fav = gol_c if atq_c > atq_f else gol_f
        
        condicao_favorito = (chutes_gol_fav >= 1)
        condicao_classico = (soma_gol >= 2)
        
        if condicao_favorito or condicao_classico:
            sinal = "GOL CEDO (HT)"
            
            detalhe = ""
            if condicao_classico:
                detalhe = f"Temos **{soma_gol} chutes no gol** somados (critério de clássico batido)."
            elif condicao_favorito:
                detalhe = f"O favorito ({favorito}) já deu **{chutes_gol_fav} chute(s) no alvo** antes dos 10 min."
                
            insight = f"""
            **Por que entrar?**<br>
            O jogo está com {tempo} minutos. Pela sua estratégia, o início precisa ser agressivo.
            {detalhe}
            As estatísticas mostram que os goleiros já estão trabalhando. A chance de sair gol na pressão inicial é altíssima.
            """

    # --- ESTRATÉGIA 2: FIM DO 1º TEMPO (Pressão Acumulada) ---
    elif 25 < tempo <= 45:
        soma_atq = atq_c + atq_f
        soma_chutes = chutes_c + chutes_f
        
        if soma_atq > 30 and soma_chutes >= 8:
            sinal = "GOL ANTES DO INTERVALO"
            quem_pressiona = t_casa if atq_c > atq_f else t_fora
            diff = abs(atq_c - atq_f)
            
            insight = f"""
            **Análise de Pressão:**<br>
            O {quem_pressiona} está "amassando" o adversário com {diff} ataques perigosos a mais.
            Já tivemos {soma_chutes} finalizações no total. O gol está maduro e deve sair antes do apito do intervalo devido ao cansaço da defesa.
            """

    # --- ESTRATÉGIA 3: FINAL DE JOGO (Tudo ou Nada) ---
    elif tempo >= 65:
        soma_chutes = chutes_c + chutes_f
        if soma_chutes >= 15:
            sinal = "GOL NO FINAL (FT)"
            insight = f"""
            **Leitura de Jogo:**<br>
            Jogo extremamente aberto com **{soma_chutes} finalizações**.
            Um dos times está exposto buscando o resultado. Nesse cenário, sua estratégia aponta 80% de chance de gol tardio (seja empate ou contra-ataque).
            """

    return sinal, insight, (chutes_c + chutes_f), soma_gol, (atq_c + atq_f)

# --- DADOS DE TESTE (Ajustados para suas regras) ---
def gerar_sinais_teste():
    return [
        {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 9}}, "league": {"name": "Premier League"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Arsenal"}, "away": {"name": "Liverpool"}}},
        {"fixture": {"id": 2, "status": {"short": "2H", "elapsed": 75}}, "league": {"name": "Brasileirão"}, "goals": {"home": 1, "away": 1}, "teams": {"home": {"name": "Flamengo"}, "away": {"name": "Palmeiras"}}}
    ]

def gerar_stats_teste(fid):
    # Cenário 1: Jogo com 9 min. Arsenal (Favorito) chutou 2 bolas no gol. BATE A REGRA.
    if fid == 1: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 4}, {"type": "Shots on Goal", "value": 2}, {"type": "Dangerous Attacks", "value": 15}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 1}, {"type": "Shots on Goal", "value": 0}, {"type": "Dangerous Attacks", "value": 5}]}]
    
    # Cenário 2: Final de jogo com muitos chutes
    elif fid == 2: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 12}, {"type": "Shots on Goal", "value": 6}, {"type": "Dangerous Attacks", "value": 60}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 10}, {"type": "Shots on Goal", "value": 5}, {"type": "Dangerous Attacks", "value": 55}]}]
    return []

# --- INTERFACE ---
st.title("🎯 Sniper de Gols - Analista Pro")

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
                    
                    sinal, motivo, chutes, no_gol, atq_p = gerar_analise_completa(tempo, s_casa, s_fora, tc, tf)
                    
                    if sinal:
                        achou = True
                        
                        # VISUAL: HTML COLADO NA ESQUERDA PARA NAO DAR ERRO
                        st.markdown(f"""
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
<div class="insight-titulo">🤖 Análise do Robô:</div>
<div class="insight-texto">{motivo}</div>
<div class="stats-row">
<div><div class="metric-label">CHUTES TOTAIS</div><div class="metric-val">{chutes}</div></div>
<div><div class="metric-label">NO GOL (ALVO)</div><div class="metric-val" style="color:#00C853;">{no_gol}</div></div>
<div><div class="metric-label">PERIGO</div><div class="metric-val" style="color:#FFD700;">{atq_p}</div></div>
</div>
</div>
""", unsafe_allow_html=True)

        bar.empty()
        if not achou:
            st.info("Nenhuma oportunidade encontrada.")
