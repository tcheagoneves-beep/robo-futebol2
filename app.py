import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Sniper de Gols - AutoBot",
    layout="centered",
    page_icon="🤖"
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
    
    /* Animação de Pulso para o Status Online */
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.7); }
        70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); }
        100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); }
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
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Configurações")
    
    # API KEY
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("Chave API-SPORTS:", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.markdown("---")
    
    # TELEGRAM
    with st.expander("🔔 Telegram"):
        tg_token = st.text_input("Bot Token:", type="password")
        tg_chat_id = st.text_input("Chat ID:")
        if st.button("Testar Envio"):
            if tg_token and tg_chat_id:
                try:
                    url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
                    requests.post(url, data={"chat_id": tg_chat_id, "text": "✅ Sniper Conectado!"})
                    st.success("Enviado!")
                except: st.error("Erro no envio.")
    
    st.markdown("---")
    
    # CONTROLE DO ROBÔ (AUTO-REFRESH)
    st.header("🤖 Controle do Robô")
    ROBO_LIGADO = st.checkbox("LIGAR MODO AUTOMÁTICO", value=False)
    INTERVALO = st.slider("Atualizar a cada (segundos):", 30, 300, 60)
    
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)

# --- FUNÇÃO ENVIO TELEGRAM ---
def enviar_telegram(token, chat_id, msg):
    if token and chat_id:
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(url, data={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"})
        except: pass

# --- CONEXÕES API ---
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
        if atq_c >= atq_f: forte=t_casa; g_forte=gol_c; fraco=t_fora; g_fraco=gol_f
        else: forte=t_fora; g_forte=gol_f; fraco=t_casa; g_fraco=gol_c
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

# --- LÓGICA PRINCIPAL (SCANNER) ---
def executar_scanner():
    if not API_KEY and not MODO_DEMO:
        st.error("⚠️ Coloque a API Key!")
        return

    jogos = buscar_jogos(API_KEY)
    achou = False
    radar_jogos = []
    
    for jogo in jogos:
        status = jogo['fixture']['status']['short']
        tempo = jogo['fixture']['status'].get('elapsed', 0)
        
        if status in ['1H', '2H'] and tempo:
            radar_jogos.append({"Liga": jogo['league']['name'], "Tempo": f"{tempo}'", "Jogo": f"{jogo['teams']['home']['name']} {jogo['goals']['home']}x{jogo['goals']['away']} {jogo['teams']['away']['name']}"})
            
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

                    # Envia Telegram
                    if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
                    chave = f"{jogo['fixture']['id']}_{sinal}"
                    if tg_token and tg_chat_id and chave not in st.session_state['alertas_enviados']:
                        msg = f"🎯 **SNIPER ALERT!**\n\n⚽ {tc} {sc}x{sf} {tf}\n⏰ {tempo}'\n💰 **{sinal}**\n\n💡 {motivo}"
                        enviar_telegram(tg_token, tg_chat_id, msg)
                        st.session_state['alertas_enviados'].add(chave)

    if not achou:
        st.info("Nenhuma oportunidade 'Sniper' agora. O robô continua monitorando...")

    # Exibe Radar
    if radar_jogos:
        with st.expander(f"📡 Radar Ao Vivo ({len(radar_jogos)} jogos monitorados)", expanded=False):
            st.dataframe(pd.DataFrame(radar_jogos), hide_index=True, use_container_width=True)

# --- INTERFACE PRINCIPAL ---
st.title("🤖 Sniper de Gols - AutoBot")

if ROBO_LIGADO:
    st.markdown('<div class="status-online">🟢 ROBÔ ONLINE E OPERANDO</div>', unsafe_allow_html=True)
    st.caption(f"Atualizando automaticamente a cada {INTERVALO} segundos. Não feche esta aba.")
    
    # Executa a Análise
    executar_scanner()
    
    # Aguarda e Recarrega
    time.sleep(INTERVALO)
    st.rerun()

else:
    st.markdown('<div style="color: #FF4B4B; text-align: center; margin-bottom: 20px;">🔴 ROBÔ PAUSADO</div>', unsafe_allow_html=True)
    if st.button("📡 RASTREAR APENAS UMA VEZ", type="primary", use_container_width=True):
        with st.spinner('Escaneando...'):
            executar_scanner()
