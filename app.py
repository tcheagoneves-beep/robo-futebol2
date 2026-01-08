import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL (TEMA ESCURO PREMIUM) ---
st.set_page_config(
    page_title="Sniper de Gols - Pro",
    layout="centered",
    page_icon="⚽"
)

# CSS Para deixar elegante (Estilo App Nativo)
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
    .titulo-time {font-size: 22px; font-weight: bold; color: #ffffff;}
    .placar {font-size: 38px; font-weight: 800; color: #FFD700; text-align: center;}
    .tempo {font-size: 16px; color: #FF4B4B; font-weight: bold; text-align: center;}
    .sinal-box {
        background-color: #00C853; 
        color: white; 
        padding: 15px; 
        border-radius: 10px; 
        text-align: center; 
        font-size: 20px; 
        font-weight: bold;
        margin-top: 15px;
        box-shadow: 0 4px 15px rgba(0, 200, 83, 0.4);
    }
    .insight-texto {
        font-size: 15px; 
        color: #CCCCCC; 
        margin-top: 10px; 
        line-height: 1.5;
        border-left: 3px solid #FFD700;
        padding-left: 10px;
    }
    .metric-label {font-size: 12px; color: #888;}
    .metric-val {font-size: 24px; font-weight: bold;}
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
    st.info("Estratégia: Leitura de Pressão (Chutes e Ataques)")

# --- INTEGRAÇÃO DE DADOS ---
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

# --- CÉREBRO: O NARRADOR INTELIGENTE ---
def gerar_analise_completa(tempo, s_casa, s_fora, t_casa, t_fora):
    # Função auxiliar para limpar dados
    def v(d, k): val = d.get(k, 0); return int(str(val).replace('%','')) if val else 0

    # Extração de Dados
    chutes_c = v(s_casa, 'Total Shots')
    chutes_f = v(s_fora, 'Total Shots')
    total_chutes = chutes_c + chutes_f
    
    gol_c = v(s_casa, 'Shots on Goal')
    gol_f = v(s_fora, 'Shots on Goal')
    total_gol = gol_c + gol_f
    
    atq_c = v(s_casa, 'Dangerous Attacks')
    atq_f = v(s_fora, 'Dangerous Attacks')
    
    posse_c = v(s_casa, 'Ball Possession')
    
    sinal = None
    insight = ""

    # --- LÓGICA DE SINAL ---
    
    # 1. GOL CEDO (HT)
    if 5 <= tempo <= 25:
        if total_gol >= 3 or total_chutes >= 6:
            sinal = "GOL NO 1º TEMPO (HT)"
            insight = f"🔥 **Início Incendiário!** O jogo mal começou ({tempo} min) e já temos **{total_chutes} finalizações**. As defesas estão batendo cabeça. A chance de gol nos próximos minutos é altíssima."

    # 2. OVER 1º TEMPO (HT - Pressão)
    elif 25 < tempo <= 45:
        if (atq_c + atq_f) > 30 and total_chutes >= 8:
            sinal = "GOL AINDA NO 1º TEMPO"
            
            # Narrativa dinâmica
            quem = t_casa if atq_c > atq_f else t_fora
            vitima = t_fora if atq_c > atq_f else t_casa
            diff = abs(atq_c - atq_f)
            
            insight = f"🚨 **Pressão Absurda do {quem}!** Eles têm {diff} ataques perigosos a mais que o {vitima}. O goleiro já trabalhou {total_gol} vezes. O gol está maduro, deve sair antes do intervalo."

    # 3. GOL NO FINAL (FT)
    elif tempo >= 65:
        if total_chutes >= 15 or (atq_c + atq_f) >= 70:
            sinal = "GOL NO FINAL DO JOGO"
            insight = f"⚡ **Jogo Aberto (Tudo ou Nada)!** Estamos na reta final com **{total_chutes} chutes acumulados**. Um dos times se lançou ao ataque e deixou a defesa exposta. A estatística mostra tendência clara de gol tardio."

    return sinal, insight, total_chutes, total_gol, (atq_c + atq_f)

# --- DADOS DE TESTE (SIMULAÇÃO) ---
def gerar_sinais_teste():
    return [
        {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 34}}, "league": {"name": "Premier League"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Arsenal"}, "away": {"name": "Liverpool"}}},
        {"fixture": {"id": 2, "status": {"short": "2H", "elapsed": 82}}, "league": {"name": "Copa do Brasil"}, "goals": {"home": 1, "away": 1}, "teams": {"home": {"name": "Corinthians"}, "away": {"name": "Flamengo"}}}
    ]

def gerar_stats_teste(fid):
    # Cenário 1: Arsenal amassando no HT
    if fid == 1: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 14}, {"type": "Shots on Goal", "value": 6}, {"type": "Dangerous Attacks", "value": 45}, {"type": "Ball Possession", "value": "65%"}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 2}, {"type": "Dangerous Attacks", "value": 10}]}]
    # Cenário 2: Jogo aberto no final
    elif fid == 2: return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 12}, {"type": "Shots on Goal", "value": 5}, {"type": "Dangerous Attacks", "value": 50}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 10}, {"type": "Shots on Goal", "value": 4}, {"type": "Dangerous Attacks", "value": 45}]}]
    return []

# --- INTERFACE ---
st.title("🎯 Sniper de Gols")
st.write("Monitorando oportunidades de mercado em tempo real.")

if st.button("📡 RASTREAR AGORA", type="primary", use_container_width=True):
    
    if not API_KEY and not MODO_DEMO:
        st.error("⚠️ Configure a API Key na barra lateral.")
    else:
        jogos = buscar_jogos(API_KEY)
        achou = False
        
        # Barra de progresso fake p/ UX
        bar = st.progress(0)
        
        for i, jogo in enumerate(jogos):
            bar.progress(min((i+1)/len(jogos), 1.0))
            
            # Filtro básico de tempo
            tempo = jogo['fixture']['status'].get('elapsed', 0)
            status = jogo['fixture']['status']['short']
            
            if status in ['1H', '2H'] and tempo:
                stats = buscar_stats(API_KEY, jogo['fixture']['id'])
                
                if stats:
                    s_casa = {i['type']: i['value'] for i in stats[0]['statistics']}
                    s_fora = {i['type']: i['value'] for i in stats[1]['statistics']}
                    
                    tc = jogo['teams']['home']['name']
                    tf = jogo['teams']['away']['name']
                    
                    # CHAMA O NARRADOR
                    sinal, motivo, chutes, no_gol, atq_p = gerar_analise_completa(tempo, s_casa, s_fora, tc, tf)
                    
                    if sinal:
                        achou = True
                        
                        # --- HTML PURO PARA O CARD (MAIS BONITO) ---
                        st.markdown(f"""
                        <div class="card">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <div style="text-align:left; width:40%;">
                                    <div class="titulo-time">{tc}</div>
                                </div>
                                <div style="text-align:center; width:20%;">
                                    <div class="placar">{jogo['goals']['home']} - {jogo['goals']['away']}</div>
                                    <div class="tempo">{tempo}'</div>
                                </div>
                                <div style="text-align:right; width:40%;">
                                    <div class="titulo-time">{tf}</div>
                                </div>
                            </div>
                            
                            <div class="sinal-box">💰 {sinal}</div>
                            
                            <div class="insight-texto">
                                {motivo}
                            </div>
                            
                            <hr style="border-color: #333;">
                            
                            <div style="display:flex; justify-content:space-around; text-align:center;">
                                <div>
                                    <div class="metric-label">CHUTES TOTAIS</div>
                                    <div class="metric-val">{chutes}</div>
                                </div>
                                <div>
                                    <div class="metric-label">NO GOL</div>
                                    <div class="metric-val" style="color:#00C853;">{no_gol}</div>
                                </div>
                                <div>
                                    <div class="metric-label">ATAQUES PERIGOSOS</div>
                                    <div class="metric-val" style="color:#FFD700;">{atq_p}</div>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
        
        bar.empty()
        if not achou:
            st.info("O Robô escaneou o mercado e não encontrou oportunidades claras de gol agora.")
