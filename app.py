import streamlit as st
import pandas as pd
import requests
import random
from datetime import datetime

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="Robô de Sinais - Português", layout="wide", page_icon="⚽")

with st.sidebar:
    st.header("⚙️ Configurações")
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("Cole sua API Key:", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.markdown("---")
    MODO_DEMO = st.checkbox("🛠️ Forçar Modo Simulação", value=False, help="Use isso para testar o visual se a API estiver travada.")
    
    st.success("Estratégia: Sinais em Português + Explicação Detalhada")

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

# --- O CÉREBRO TRADUZIDO (SUA ESTRATÉGIA) ---
def gerar_sinal_explicado(tempo, stats_casa, stats_fora, time_casa, time_fora):
    
    # 1. Extrair Números
    def v(d, k): 
        val = d.get(k, 0)
        if val is None: return 0
        return int(str(val).replace('%',''))

    chutes_c = v(stats_casa, 'Total Shots')
    chutes_f = v(stats_fora, 'Total Shots')
    chutes_total = chutes_c + chutes_f
    
    gol_c = v(stats_casa, 'Shots on Goal')
    gol_f = v(stats_fora, 'Shots on Goal')
    chutes_gol_total = gol_c + gol_f
    
    atq_c = v(stats_casa, 'Dangerous Attacks')
    atq_f = v(stats_fora, 'Dangerous Attacks')
    ataques_p = atq_c + atq_f
    
    sinal = None
    insight = ""

    # --- ESTRATÉGIA 1: GOL CEDO (5-15 min) ---
    if 5 <= tempo <= 15:
        if chutes_gol_total >= 2 or chutes_total >= 4:
            sinal = "💰 APOSTAR: Gol logo no Início"
            insight = (f"**Por que apostar?** O jogo mal começou ({tempo}min) e já tivemos {chutes_total} chutes! "
                       f"As defesas estão abertas e os times entraram agressivos. A chance de sair gol cedo é altíssima.")

    # --- ESTRATÉGIA 2: GOL NO 1º TEMPO (15-40 min) ---
    elif 15 < tempo <= 40:
        # Pressão Absurda
        if ataques_p >= 25 and chutes_total >= 6:
            sinal = "💰 APOSTAR: Vai sair gol no 1º Tempo"
            
            # Monta a fofoca detalhada
            quem_pressiona = time_casa if atq_c > atq_f else time_fora
            insight = (f"**Por que apostar?** O jogo está pegando fogo! Já são {ataques_p} ataques perigosos acumulados. "
                       f"O time **{quem_pressiona}** está amassando o adversário. "
                       f"Com {chutes_total} finalizações até agora, a bola vai entrar a qualquer momento.")

    # --- ESTRATÉGIA 3: GOL NO FINAL (60-85 min) ---
    elif tempo >= 60:
        if chutes_total >= 14 or ataques_p >= 60:
            sinal = "💰 APOSTAR: Gol no Final do Jogo"
            insight = (f"**Por que apostar?** Estamos na reta final e o volume de jogo explodiu. "
                       f"São {chutes_total} chutes no total! Um dos times está desesperado pelo resultado e "
                       f"se expondo ao contra-ataque. Cenário clássico de gol tardio.")
            
    return sinal, insight, chutes_total, ataques_p

# --- SIMULADORES ATUALIZADOS ---
def gerar_sinais_teste():
    return [
        {"fixture": {"id": 1, "date": "2026-01-08T15:00:00", "status": {"short": "1H", "elapsed": 25}}, "league": {"name": "Premier League"}, "teams": {"home": {"name": "Arsenal"}, "away": {"name": "Liverpool"}}},
        {"fixture": {"id": 2, "date": "2026-01-08T16:00:00", "status": {"short": "2H", "elapsed": 75}}, "league": {"name": "UAE Pro League"}, "teams": {"home": {"name": "Al Ain"}, "away": {"name": "Al Dhafra"}}}
    ]

def gerar_stats_teste(fid):
    # Arsenal amassando
    if fid == 1: 
        return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 8}, {"type": "Shots on Goal", "value": 4}, {"type": "Dangerous Attacks", "value": 30}]}, 
                {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 2}, {"type": "Dangerous Attacks", "value": 5}]}]
    # Al Ain no final
    elif fid == 2:
        return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 15}, {"type": "Dangerous Attacks", "value": 65}]}, 
                {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 5}, {"type": "Dangerous Attacks", "value": 20}]}]
    else: return []

# --- INTERFACE ---
st.title("🤖 Robô de Sinais: Análise Explicada")

if MODO_DEMO:
    st.info("ℹ️ MODO TESTE: Usando dados fictícios para validar o texto.")

if st.button("📡 ESCANEAR OPORTUNIDADES"):
    if not API_KEY and not MODO_DEMO:
        st.error("Coloque a API Key!")
    else:
        jogos = buscar_jogos(API_KEY)
        achou_algo = False
        
        with st.status("Lendo estatísticas dos jogos...", expanded=True) as status:
            
            for jogo in jogos:
                status_short = jogo['fixture']['status']['short']
                tempo = jogo['fixture']['status'].get('elapsed', 0)
                
                # Só analisa jogos com bola rolando
                if status_short in ['1H', '2H'] and tempo:
                    stats = buscar_stats(API_KEY, jogo['fixture']['id'])
                    
                    if stats:
                        s_casa = {i['type']: i['value'] for i in stats[0]['statistics']}
                        s_fora = {i['type']: i['value'] for i in stats[1]['statistics']}
                        
                        nome_casa = jogo['teams']['home']['name']
                        nome_fora = jogo['teams']['away']['name']
                        
                        # CHAMA A INTELIGÊNCIA TRADUZIDA
                        ordem, explicacao, chutes, atq_p = gerar_sinal_explicado(tempo, s_casa, s_fora, nome_casa, nome_fora)
                        
                        if ordem:
                            achou_algo = True
                            st.divider()
                            
                            # CABEÇALHO DO JOGO
                            st.markdown(f"### ⚽ {nome_casa} x {nome_fora}")
                            st.caption(f"Liga: {jogo['league']['name']} | ⏱️ Tempo: {tempo} min")
                            
                            # A ORDEM DE APOSTA (CARD VERDE)
                            st.success(f"## {ordem}")
                            
                            # OS INSIGHTS (A EXPLICAÇÃO)
                            with st.chat_message("assistant"):
                                st.write(explicacao)
                                st.markdown(f"""
                                * **Estatísticas Reais:**
                                    * 🎯 Chutes Totais: **{chutes}**
                                    * 🔥 Ataques Perigosos: **{atq_p}**
                                """)
            
            status.update(label="Fim da análise!", state="complete", expanded=False)

        if not achou_algo:
            st.info("Nenhuma oportunidade clara encontrada agora. O mercado está morno.")
