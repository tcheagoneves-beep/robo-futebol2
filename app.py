import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Sniper de Gols - V18 Radar",
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
    .multipla-box {
        background-color: #9C27B0; 
        color: white; 
        padding: 10px; 
        border-radius: 8px; 
        text-align: center; 
        font-size: 18px; 
        font-weight: bold;
        margin-top: 15px;
        box-shadow: 0 4px 15px rgba(156, 39, 176, 0.4);
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
    st.header("⚙️ Configurações (Direct)")
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("Cole sua Chave API-SPORTS aqui:", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.markdown("---")
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
    st.info("V18: Com Radar de Jogos.")

# --- CONEXÕES API (DIRECT) ---
def buscar_jogos(api_key):
    if MODO_DEMO: return gerar_sinais_teste()
    url = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": api_key} 
    try: return requests.get(url, headers=headers, params={"date": datetime.today().strftime('%Y-%m-%d')}).json().get('response', [])
    except: return []

def buscar_stats(api_key, fixture_id):
    if MODO_DEMO: return gerar_stats_teste(fixture_id)
    url = "https://v3.football.api-sports.io/fixtures/statistics"
    headers = {"x-apisports-key": api_key}
    try: return requests.get(url, headers=headers, params={"fixture": fixture_id}).json().get('response', [])
    except: return []

def buscar_odds_pre_match(api_key, fixture_id):
    if MODO_DEMO: return gerar_odds_teste(fixture_id)
    url = "https://v3.football.api-sports.io/odds"
    headers = {"x-apisports-key": api_key}
    try:
        data = requests.get(url, headers=headers, params={"fixture": fixture_id, "bookmaker": "1"}).json()
        if data.get('response'):
            bets = data['response'][0]['bookmakers'][0]['bets']
            winner_bet = next((b for b in bets if b['id'] == 1), None)
            if winner_bet:
                odd_casa = float(next((v['odd'] for v in winner_bet['values'] if v['value'] == 'Home'), 0))
                odd_fora = float(next((v['odd'] for v in winner_bet['values'] if v['value'] == 'Away'), 0))
                return odd_casa, odd_fora
        return 0, 0
    except: return 0, 0

# --- CÉREBRO ---
def analisar_partida(tempo, s_casa, s_fora, t_casa, t_fora, sc, sf, odd_casa, odd_fora):
    
    def v(d, k): val = d.get(k, 0); return int(str(val).replace('%','')) if val else 0
    gol_c = v(s_casa, 'Shots on Goal')
    gol_f = v(s_fora, 'Shots on Goal')
    chutes_c = v(s_casa, 'Total Shots')
    chutes_f = v(s_fora, 'Total Shots')
    atq_c = v(s_casa, 'Dangerous Attacks')
    atq_f = v(s_fora, 'Dangerous Attacks')
    
    total_chutes = chutes_c + chutes_f
    sinal = None; insight = ""; tipo_sinal = "normal"
    
    favorito = None; nome_favorito = ""; eh_gigante = False
    if odd_casa > 0 and odd_fora > 0:
        if odd_casa <= 1.55: favorito = "CASA"; nome_favorito = t_casa; eh_gigante = True
        elif odd_fora <= 1.55: favorito = "FORA"; nome_favorito = t_fora; eh_gigante = True
        elif odd_casa <= 1.90: favorito = "CASA"; nome_favorito = t_casa
        elif odd_fora <= 1.90: favorito = "FORA"; nome_favorito = t_fora

    # 1. Múltipla
    gols_totais = sc + sf
    if tempo <= 30 and gols_totais >= 2:
        sinal = "CANDIDATO P/ MÚLTIPLA (2+ Gols)"
        tipo_sinal = "multipla"
        insight = f"**Porteira Aberta!** {gols_totais} gols em {tempo} min."

    # 2. Reação Gigante
    elif tempo <= 50 and eh_gigante and not sinal:
        fav_perdendo = (favorito == "CASA" and sc < sf) or (favorito == "FORA" and sf < sc)
        if fav_perdendo:
            fc = chutes_c if favorito == "CASA" else chutes_f
            fa = atq_c if favorito == "CASA" else atq_f
            zc = chutes_f if favorito == "CASA" else chutes_c
            if fc >= 6 and fa > 30 and zc < 4:
                sinal = f"PRÓXIMO GOL: {nome_favorito}"
                insight = f"Gigante ({nome_favorito}) perde mas domina."

    # 3. Janela 70-75
    elif 70 <= tempo <= 75 and eh_gigante and not sinal:
        nao_ganhando = (favorito == "CASA" and sc <= sf) or (favorito == "FORA" and sf <= sc)
        if nao_ganhando:
            stats_chutes = chutes_c if favorito == "CASA" else chutes_f
            if stats_chutes >= 18:
                sinal = "GOL (GIGANTE PRESSIONA)"
                insight = f"Gigante precisa do gol. {stats_chutes} chutes acumulados."

    # 4. Gol Cedo
    elif 5 <= tempo <= 15 and not sinal:
        if atq_c >= atq_f: 
            forte=t_casa; g_forte=gol_c; fraco=t_fora; g_fraco=gol_f
        else:
            forte=t_fora; g_forte=gol_f; fraco=t_casa; g_fraco=gol_c
        txt = ""
        if g_forte >= 1: txt = f"Dominante ({forte}) chutou no alvo."
        if g_fraco >= 2: txt = f"Zebra ({fraco}) chutou 2x no alvo."
        if txt:
            sinal = "GOL CEDO (HT)"
            insight = f"Início Intenso ({tempo} min). {txt}"

    return sinal, insight, total_chutes, (gol_c + gol_f), (atq_c + atq_f), tipo_sinal

# --- SIMULAÇÃO ---
def gerar_sinais_teste():
    return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 28}}, "league": {"name": "Teste"}, "goals": {"home": 1, "away": 1}, "teams": {"home": {"name": "Ajax"}, "away": {"name": "PSV"}}}]
def gerar_odds_teste(fid): return (2.5, 2.5)
def gerar_stats_teste(fid): return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 10}, {"type": "Shots on Goal", "value": 5}, {"type": "Dangerous Attacks", "value": 30}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 8}, {"type": "Shots on Goal", "value": 4}, {"type": "Dangerous Attacks", "value": 20}]}]

# --- INTERFACE ---
st.title("🎯 Sniper de Gols - V18 Radar")

if st.button("📡 RASTREAR E ATUALIZAR", type="primary", use_container_width=True):
    if not API_KEY and not MODO_DEMO:
        st.error("⚠️ Coloque a API Key!")
    else:
        with st.spinner('Escaneando todos os jogos do mundo...'):
            jogos = buscar_jogos(API_KEY)
            
        achou = False
        radar_jogos = []
        proximos_jogos = []
        
        # Filtros de Radar
        for jogo in jogos:
            status = jogo['fixture']['status']['short']
            
            # Lista de Próximos (NS = Not Started)
            if status == 'NS':
                hora = datetime.fromtimestamp(jogo['fixture']['timestamp']).strftime('%H:%M')
                proximos_jogos.append({
                    "Hora": hora,
                    "Liga": jogo['league']['name'],
                    "Jogo": f"{jogo['teams']['home']['name']} vs {jogo['teams']['away']['name']}"
                })

            # Processamento de AO VIVO
            tempo = jogo['fixture']['status'].get('elapsed', 0)
            if status in ['1H', '2H'] and tempo:
                
                # Adiciona ao Radar (Para mostrar que está vendo)
                radar_jogos.append({
                    "Liga": jogo['league']['name'],
                    "Tempo": f"{tempo}' ({status})",
                    "Jogo": f"{jogo['teams']['home']['name']} {jogo['goals']['home']} x {jogo['goals']['away']} {jogo['teams']['away']['name']}"
                })
                
                # Executa Sniper
                stats = buscar_stats(API_KEY, jogo['fixture']['id'])
                if stats:
                    odd_casa, odd_fora = buscar_odds_pre_match(API_KEY, jogo['fixture']['id'])
                    s_casa = {i['type']: i['value'] for i in stats[0]['statistics']}
                    s_fora = {i['type']: i['value'] for i in stats[1]['statistics']}
                    tc = jogo['teams']['home']['name']; tf = jogo['teams']['away']['name']
                    sc = jogo['goals']['home'] or 0; sf = jogo['goals']['away'] or 0
                    
                    sinal, motivo, chutes, no_gol, atq_p, tipo = analisar_partida(tempo, s_casa, s_fora, tc, tf, sc, sf, odd_casa, odd_fora)
                    
                    if sinal:
                        achou = True
                        cls = "multipla-box" if tipo == "multipla" else "sinal-box"
                        st.markdown(f"""
<div class="card">
<div style="display:flex; justify-content:space-between; align-items:center;">
<div style="width:40%; text-align:left;"><div class="titulo-time">{tc}</div><span class="odd-label">{odd_casa:.2f}</span></div>
<div style="width:20%; text-align:center;"><div class="placar">{sc} - {sf}</div><div class="tempo">{tempo}'</div></div>
<div style="width:40%; text-align:right;"><div class="titulo-time">{tf}</div><span class="odd-label">{odd_fora:.2f}</span></div>
</div>
<div class="{cls}">{sinal}</div>
<div class="insight-texto"><b>Motivo:</b> {motivo}</div>
<div class="stats-row">
<div><div class="metric-label">CHUTES</div><div class="metric-val">{chutes}</div></div>
<div><div class="metric-label">NO GOL</div><div class="metric-val" style="color:#00C853;">{no_gol}</div></div>
<div><div class="metric-label">PERIGO</div><div class="metric-val" style="color:#FFD700;">{atq_p}</div></div>
</div></div>""", unsafe_allow_html=True)
        
        if not achou:
            st.warning("Nenhuma oportunidade 'Sniper' encontrada no momento.")
        
        st.markdown("---")
        
        # --- MOSTRAR O RADAR (TABELAS) ---
        c1, c2 = st.tabs(["📡 Radar (Ao Vivo)", "📅 Próximos Jogos (Hoje)"])
        
        with c1:
            if radar_jogos:
                st.caption(f"Analisando {len(radar_jogos)} jogos ao vivo agora:")
                df_radar = pd.DataFrame(radar_jogos)
                st.dataframe(df_radar, hide_index=True, use_container_width=True)
            else:
                st.info("Nenhum jogo ao vivo (1H ou 2H) neste momento.")
                
        with c2:
            if proximos_jogos:
                st.caption(f"Jogos agendados para hoje:")
                df_prox = pd.DataFrame(proximos_jogos)
                st.dataframe(df_prox, hide_index=True, use_container_width=True)
            else:
                st.info("Não encontrei mais jogos agendados para hoje na lista.")
