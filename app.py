import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Sniper de Gols - EcoMode",
    layout="centered",
    page_icon="💸"
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
    .status-online {
        color: #00FF00;
        font-weight: bold;
        animation: pulse 2s infinite;
        padding: 5px 10px;
        border-radius: 15px;
        border: 1px solid #00FF00;
        text-align: center;
        margin-bottom: 20px;
    }
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.7); }
        70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); }
        100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); }
    }
    .metric-val {font-size: 22px; font-weight: bold;}
    .metric-label {font-size: 10px; color: #888; text-transform: uppercase;}
    .stats-row { display: flex; justify-content: space-around; text-align: center; margin-top: 15px; padding-top: 15px; border-top: 1px solid #333; }
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Configurações")
    
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("Chave API-SPORTS:", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.markdown("---")
    
    with st.expander("🔔 Telegram (Família)"):
        tg_token = st.text_input("Bot Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs (separados por vírgula):")
        if st.button("Testar Envio"):
            if tg_token and tg_chat_ids:
                for cid in tg_chat_ids.split(','):
                    try: requests.post(f"https://api.telegram.org/bot{tg_token}/sendMessage", data={"chat_id": cid.strip(), "text": "✅ Teste Sniper!"})
                    except: pass
                st.success("Teste enviado!")

    st.markdown("---")
    st.header("🤖 Modo Automático")
    ROBO_LIGADO = st.checkbox("LIGAR ROBÔ", value=False)
    # Aumentei o tempo mínimo para 60s para economizar
    INTERVALO = st.slider("Ciclo (segundos):", 60, 300, 120)
    
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)

# --- FUNÇÃO ENVIO TELEGRAM ---
def enviar_telegram_multi(token, ids_string, msg):
    if token and ids_string:
        for chat_id in ids_string.split(','):
            if chat_id.strip():
                try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id.strip(), "text": msg, "parse_mode": "Markdown"})
                except: pass

# --- CONEXÕES API COM OTIMIZAÇÃO ---
def buscar_jogos(api_key):
    if MODO_DEMO: return gerar_sinais_teste()
    url = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": api_key} 
    # Filtra apenas jogos "Live" para economizar processamento
    try: return requests.get(url, headers=headers, params={"live": "all"}).json().get('response', [])
    except: return []

def buscar_stats(api_key, fixture_id):
    if MODO_DEMO: return gerar_stats_teste(fixture_id)
    url = "https://v3.football.api-sports.io/fixtures/statistics"
    headers = {"x-apisports-key": api_key}
    try: return requests.get(url, headers=headers, params={"fixture": fixture_id}).json().get('response', [])
    except: return []

# --- CACHE DE ODDS (ECONOMIA DE 50%) ---
if 'odds_cache' not in st.session_state: st.session_state['odds_cache'] = {}

def buscar_odds_cached(api_key, fixture_id):
    if MODO_DEMO: return gerar_odds_teste(fixture_id)
    
    # Se já tem na memória, usa a memória (GRÁTIS)
    if fixture_id in st.session_state['odds_cache']:
        return st.session_state['odds_cache'][fixture_id]
    
    # Se não tem, busca na API (GASTA 1 CRÉDITO)
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
                # Salva no cache
                st.session_state['odds_cache'][fixture_id] = (odd_casa, odd_fora)
                return odd_casa, odd_fora
    except: pass
    return 0, 0

# --- CÉREBRO ---
def analisar_partida(tempo, s_casa, s_fora, t_casa, t_fora, sc, sf, odd_casa, odd_fora):
    def v(d, k): val = d.get(k, 0); return int(str(val).replace('%','')) if val else 0
    gol_c = v(s_casa, 'Shots on Goal'); gol_f = v(s_fora, 'Shots on Goal')
    chutes_c = v(s_casa, 'Total Shots'); chutes_f = v(s_fora, 'Total Shots')
    atq_c = v(s_casa, 'Dangerous Attacks'); atq_f = v(s_fora, 'Dangerous Attacks')
    total_chutes = chutes_c + chutes_f
    sinal = None; insight = ""; tipo_sinal = "normal"
    
    favorito = None; nome_favorito = ""; eh_gigante = False
    if odd_casa > 0 and odd_fora > 0:
        if odd_casa <= 1.55: favorito = "CASA"; nome_favorito = t_casa; eh_gigante = True
        elif odd_fora <= 1.55: favorito = "FORA"; nome_favorito = t_fora; eh_gigante = True
        elif odd_casa <= 1.90: favorito = "CASA"; nome_favorito = t_casa
        elif odd_fora <= 1.90: favorito = "FORA"; nome_favorito = t_fora

    # Regras Estratégicas
    if tempo <= 30 and (sc + sf) >= 2:
        sinal = "CANDIDATO P/ MÚLTIPLA (2+ Gols)"
        tipo_sinal = "multipla"
        insight = f"Porteira Aberta! {sc+sf} gols em {tempo} min."

    elif tempo <= 50 and eh_gigante and not sinal:
        fav_perdendo = (favorito == "CASA" and sc < sf) or (favorito == "FORA" and sf < sc)
        if fav_perdendo:
            fc = chutes_c if favorito == "CASA" else chutes_f
            fa = atq_c if favorito == "CASA" else atq_f
            zc = chutes_f if favorito == "CASA" else chutes_c
            if fc >= 6 and fa > 30 and zc < 4:
                sinal = f"PRÓXIMO GOL: {nome_favorito}"
                insight = f"Gigante ({nome_favorito}) perde mas domina."

    elif 70 <= tempo <= 75 and eh_gigante and not sinal:
        nao_ganhando = (favorito == "CASA" and sc <= sf) or (favorito == "FORA" and sf <= sc)
        if nao_ganhando:
            stats_chutes = chutes_c if favorito == "CASA" else chutes_f
            if stats_chutes >= 18:
                sinal = "GOL (GIGANTE PRESSIONA)"
                insight = f"Gigante precisa do gol urgente."

    elif 5 <= tempo <= 15 and not sinal:
        if atq_c >= atq_f: forte=t_casa; g_forte=gol_c; fraco=t_fora; g_fraco=gol_f
        else: forte=t_fora; g_forte=gol_f; fraco=t_casa; g_fraco=gol_c
        txt = ""
        if g_forte >= 1: txt = f"Dominante ({forte}) chutou no alvo."
        if g_fraco >= 2: txt = f"Zebra ({fraco}) chutou 2x no alvo."
        if txt:
            sinal = "GOL CEDO (HT)"
            insight = f"Início Intenso ({tempo} min). {txt}"

    return sinal, insight, total_chutes, (gol_c + gol_f), (atq_c + atq_f), tipo_sinal

def traduzir_instrucao(sinal, time_fav=""):
    if "PRÓXIMO GOL" in sinal: return f"Apostar no **Próximo Gol** a favor do **{time_fav}**."
    elif "MÚLTIPLA" in sinal: return "Adicionar na **Múltipla de Mais Gols**."
    elif "GOL CEDO" in sinal: return "Entrar em **Over 0.5 HT** (Gol no 1º Tempo)."
    elif "GIGANTE" in sinal: return "Entrar em **Mais 1 Gol** (Gol Limite)."
    else: return "Entrar em **Mais Gols**."

# --- SIMULAÇÃO ---
def gerar_sinais_teste(): return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 35}}, "league": {"name": "Simulacao"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Real"}, "away": {"name": "Zebra"}}}]
def gerar_odds_teste(fid): return (1.20, 15.00)
def gerar_stats_teste(fid): return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 15}, {"type": "Shots on Goal", "value": 6}, {"type": "Dangerous Attacks", "value": 45}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 1}, {"type": "Shots on Goal", "value": 1}, {"type": "Dangerous Attacks", "value": 5}]}]

# --- SCANNER OTIMIZADO ---
def executar_scanner():
    if not API_KEY and not MODO_DEMO:
        st.error("⚠️ Coloque a API Key!")
        return

    # Busca APENAS jogos ao vivo (Economia no filtro inicial)
    jogos = buscar_jogos(API_KEY)
    
    achou = False
    radar_jogos = []
    
    for jogo in jogos:
        status = jogo['fixture']['status']['short']
        tempo = jogo['fixture']['status'].get('elapsed', 0)
        
        # Filtro 1: O jogo está rolando?
        if status in ['1H', '2H'] and tempo:
            
            # FILTRO 2 (ECONOMIA MÁXIMA): SÓ GASTA API SE O TEMPO FOR RELEVANTE
            # As estratégias são: 5-15' (Cedo), <=30' (Múltipla), <=50' (Reação), 70-75' (Final)
            # Zonas de Interesse: 5 a 50 E 70 a 75.
            # Se o jogo estiver nos "Buracos" (ex: 55min, 80min), PULA e economiza a API.
            
            tempo_relevante = (5 <= tempo <= 50) or (70 <= tempo <= 75)
            
            if tempo_relevante:
                radar_jogos.append({"Liga": jogo['league']['name'], "Tempo": f"{tempo}'", "Jogo": f"{jogo['teams']['home']['name']} {jogo['goals']['home']}x{jogo['goals']['away']} {jogo['teams']['away']['name']}"})
                
                # 1. Busca Odds do Cache (Não gasta se já tiver)
                odd_casa, odd_fora = buscar_odds_cached(API_KEY, jogo['fixture']['id'])
                
                # 2. Busca Stats (Gasta 1 crédito, mas só se passou pelo filtro de tempo)
                stats = buscar_stats(API_KEY, jogo['fixture']['id'])
                
                if stats:
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

                        if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
                        chave = f"{jogo['fixture']['id']}_{sinal}"
                        if tg_token and tg_chat_ids and chave not in st.session_state['alertas_enviados']:
                            nome_fav_msg = tc if odd_casa < odd_fora else tf
                            inst = traduzir_instrucao(sinal, nome_fav_msg)
                            msg_tg = f"🚨 **SNIPER ALERT!**\n\n⚽ {tc} {sc}x{sf} {tf}\n⏰ {tempo}'\n💰 **{sinal}**\n\n✅ {inst}\n\n📊 Chutes: {chutes} | Perigo: {atq_p}"
                            enviar_telegram_multi(tg_token, tg_chat_ids, msg_tg)
                            st.session_state['alertas_enviados'].add(chave)

    if not achou: st.info(f"Monitorando... (Economia Ativa: Analisando apenas jogos em janelas estratégicas)")
    if radar_jogos:
        with st.expander(f"📡 Radar Filtrado ({len(radar_jogos)} jogos na janela)", expanded=False):
            st.dataframe(pd.DataFrame(radar_jogos), hide_index=True, use_container_width=True)

# --- INTERFACE ---
st.title("💸 Sniper de Gols - EcoMode")

if ROBO_LIGADO:
    st.markdown('<div class="status-online">🟢 ROBÔ ONLINE (MODO ECONÔMICO)</div>', unsafe_allow_html=True)
    st.caption(f"Atualizando a cada {INTERVALO}s...")
    executar_scanner()
    time.sleep(INTERVALO)
    st.rerun()
else:
    st.markdown('<div style="color: #FF4B4B; text-align: center; margin-bottom: 20px;">🔴 ROBÔ PAUSADO</div>', unsafe_allow_html=True)
    if st.button("📡 RASTREAR MANUALMENTE", type="primary", use_container_width=True):
        executar_scanner()
