import streamlit as st
import pandas as pd
import requests
import random
from datetime import datetime

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="Robô de Sinais - Gols", layout="wide", page_icon="🤑")

with st.sidebar:
    st.header("⚙️ Configurações")
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("Cole sua API Key:", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.markdown("---")
    MODO_DEMO = st.checkbox("🛠️ Forçar Modo Simulação", value=False, help="Use isso enquanto sua API não aprova.")
    
    st.success("Estratégia Ativa: Sinais de Gols (HT/FT)")

# LIGAS VIP
LIGAS_VIP = [39, 40, 78, 79, 140, 141, 94, 88, 179, 103, 307, 201, 203, 169, 98, 292, 71, 72, 2, 3, 13, 11]

# --- FUNÇÕES DE DADOS ---

def buscar_jogos(api_key):
    if MODO_DEMO: return gerar_sinais_teste()
    
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"date": datetime.today().strftime('%Y-%m-%d')}
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try:
        return requests.get(url, headers=headers, params=querystring).json().get('response', [])
    except: return []

def buscar_stats(api_key, fixture_id):
    if MODO_DEMO: return gerar_stats_teste(fixture_id)
    
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try:
        return requests.get(url, headers=headers, params={"fixture": fixture_id}).json().get('response', [])
    except: return []

# --- O CÉREBRO: GERADOR DE SINAIS (SUA ESTRATÉGIA) ---
def gerar_sinal(tempo, stats_casa, stats_fora):
    
    # 1. Extrair Números
    def v(d, k): 
        val = d.get(k, 0)
        if val is None: return 0
        return int(str(val).replace('%',''))

    chutes_total = v(stats_casa, 'Total Shots') + v(stats_fora, 'Total Shots')
    chutes_gol = v(stats_casa, 'Shots on Goal') + v(stats_fora, 'Shots on Goal')
    ataques_p = v(stats_casa, 'Dangerous Attacks') + v(stats_fora, 'Dangerous Attacks')
    
    sinal = None
    motivo = ""

    # --- ESTRATÉGIA 1: GOL CEDO (5-15 min) ---
    # "Tem que ter chute no gol logo no inicio"
    if 5 <= tempo <= 15:
        if chutes_gol >= 2 or chutes_total >= 4:
            sinal = "💰 ENTRADA: GOL LIMIT (HT)"
            motivo = "Início Frenético (Muitos chutes cedo)"

    # --- ESTRATÉGIA 2: OVER GOLS HT (15-40 min) ---
    # Jogo aberto, muitos ataques perigosos
    elif 15 < tempo <= 40:
        if ataques_p >= 25 and chutes_total >= 6:
            sinal = "💰 ENTRADA: OVER 0.5 HT (1º Tempo)"
            motivo = f"Pressão Alta ({ataques_p} ataques perigosos)"

    # --- ESTRATÉGIA 3: A REVOLTA DO FAVORITO / FINAL (FT) ---
    # Final de jogo, favorito empatando ou perdendo, amassando
    elif tempo >= 60:
        if chutes_total >= 15 or ataques_p >= 60:
            sinal = "💰 ENTRADA: GOL FINAL (FT)"
            motivo = f"Jogo Aberto ({chutes_total} finalizações)"
            
    return sinal, motivo, chutes_total, ataques_p

# --- SIMULADORES PARA VOCÊ VER OS SINAIS ---
def gerar_sinais_teste():
    return [
        {"fixture": {"id": 1, "date": "2026-01-08T15:00:00", "status": {"short": "1H", "elapsed": 25}}, "league": {"name": "Premier League"}, "teams": {"home": {"name": "Arsenal"}, "away": {"name": "Liverpool"}}},
        {"fixture": {"id": 2, "date": "2026-01-08T16:00:00", "status": {"short": "2H", "elapsed": 75}}, "league": {"name": "UAE Pro League"}, "teams": {"home": {"name": "Al Ain"}, "away": {"name": "Al Dhafra"}}},
        {"fixture": {"id": 3, "date": "2026-01-08T16:00:00", "status": {"short": "1H", "elapsed": 10}}, "league": {"name": "Série B"}, "teams": {"home": {"name": "Santos"}, "away": {"name": "Mirassol"}}}
    ]

def gerar_stats_teste(fid):
    # ID 1 = Arsenal (Pressão HT)
    if fid == 1: 
        return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 5}, {"type": "Dangerous Attacks", "value": 20}]}, 
                {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 3}, {"type": "Dangerous Attacks", "value": 15}]}]
    # ID 2 = Al Ain (Pressão Final)
    elif fid == 2:
        return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 12}, {"type": "Dangerous Attacks", "value": 50}]}, 
                {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 4}, {"type": "Dangerous Attacks", "value": 20}]}]
    # ID 3 = Jogo Morto (Sem sinal)
    else:
        return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 0}, {"type": "Dangerous Attacks", "value": 2}]}, 
                {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 0}, {"type": "Dangerous Attacks", "value": 1}]}]

# --- INTERFACE ---
st.title("🤖 Central de Sinais: Gols")

if MODO_DEMO:
    st.warning("⚠️ MODO SIMULAÇÃO: Sinais gerados artificialmente para teste.")

if st.button("📡 ESCANEAR MERCADO E GERAR SINAIS"):
    if not API_KEY and not MODO_DEMO:
        st.error("Coloque a API Key!")
    else:
        jogos = buscar_jogos(API_KEY)
        sinais_encontrados = 0
        
        with st.status("Analisando jogos em tempo real...", expanded=True) as status:
            
            for jogo in jogos:
                # Pega status do jogo (tempo)
                status_short = jogo['fixture']['status']['short']
                tempo = jogo['fixture']['status'].get('elapsed', 0)
                
                # Só analisa jogos AO VIVO (1H ou 2H)
                if status_short in ['1H', '2H'] and tempo:
                    
                    # Busca estatísticas
                    stats = buscar_stats(API_KEY, jogo['fixture']['id'])
                    
                    if stats:
                        s_casa = {i['type']: i['value'] for i in stats[0]['statistics']}
                        s_fora = {i['type']: i['value'] for i in stats[1]['statistics']}
                        
                        # APLICA A INTELIGÊNCIA
                        ordem, motivo, chutes, atq_p = gerar_sinal(tempo, s_casa, s_fora)
                        
                        if ordem:
                            sinais_encontrados += 1
                            st.divider()
                            col1, col2 = st.columns([3, 1])
                            
                            with col1:
                                st.markdown(f"### ⚽ {jogo['teams']['home']['name']} x {jogo['teams']['away']['name']}")
                                st.caption(f"Liga: {jogo['league']['name']} | Tempo: {tempo} min")
                                
                                # A ORDEM DE APOSTA GRANDE E VERDE
                                st.success(f"## {ordem}")
                                st.write(f"**Motivo:** {motivo}")
                                
                            with col2:
                                st.metric("Chutes Totais", chutes)
                                st.metric("Ataques Perigosos", atq_p)
            
            status.update(label="Escaneamento concluído!", state="complete", expanded=False)

        if sinais_encontrados == 0:
            st.info("Nenhum jogo atende aos critérios da sua estratégia agora. Aguarde.")
