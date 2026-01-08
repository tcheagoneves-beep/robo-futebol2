import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Sniper de Gols - Família",
    layout="centered",
    page_icon="👨‍👦"
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
    
    # API KEY
    if 'api_key' not in st.session_state: st.session_state['api_key'] = ""
    API_KEY = st.text_input("Chave API-SPORTS:", value=st.session_state['api_key'], type="password")
    if API_KEY: st.session_state['api_key'] = API_KEY
    
    st.markdown("---")
    
    # TELEGRAM MULTI-USUÁRIO
    with st.expander("🔔 Telegram (Família)"):
        tg_token = st.text_input("Bot Token:", type="password")
        # Campo modificado para aceitar vários IDs
        tg_chat_ids = st.text_input("Chat IDs (separe por vírgula):", help="Ex: 123456, 987654")
        
        if st.button("Testar Envio para Todos"):
            if tg_token and tg_chat_ids:
                lista_ids = tg_chat_ids.split(',')
                sucesso = 0
                for chat_id in lista_ids:
                    chat_id = chat_id.strip() # Remove espaços
                    if chat_id:
                        try:
                            url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
                            resp = requests.post(url, data={"chat_id": chat_id, "text": "✅ Sniper Conectado! Pai e Filho no Green! 💰", "parse_mode": "Markdown"})
                            if resp.status_code == 200: sucesso += 1
                        except: pass
                
                if sucesso > 0: st.success(f"Enviado para {sucesso} pessoas!")
                else: st.error("Erro. Verifique se todos deram /start no robô.")
    
    st.markdown("---")
    
    # AUTO BOT
    st.header("🤖 Modo Automático")
    ROBO_LIGADO = st.checkbox("LIGAR ROBÔ", value=False)
    INTERVALO = st.slider("Ciclo (segundos):", 30, 300, 60)
    
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)

# --- FUNÇÃO ENVIO TELEGRAM (MULTI) ---
def enviar_telegram_multi(token, ids_string, msg):
    if token and ids_string:
        lista_ids = ids_string.split(',')
        for chat_id in lista_ids:
            chat_id = chat_id.strip()
            if chat_id:
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
        insight = f"Porteira Aberta! {gols_totais} gols em {tempo} min."

    # 2. Reação Gigante
    elif tempo <= 50 and eh_gigante and not sinal:
        fav_perdendo = (favorito == "CASA" and sc < sf) or (favorito == "FORA" and sf < sc)
        if fav_perdendo:
            fc = chutes_c if favorito == "CASA" else chutes_f
            fa = atq_c if favorito == "CASA" else atq_f
            zc = chutes_f if favorito == "CASA" else chutes_c
            if fc >= 6 and fa > 30 and zc < 4:
                sinal = f"PRÓXIMO GOL: {nome_favorito}"
                insight = f"Gigante ({nome_favorito}) perde mas domina totalmente."

    # 3. Janela 70-75
    elif 70 <= tempo <= 75 and eh_gigante and not sinal:
        nao_ganhando = (favorito == "CASA" and sc <= sf) or (favorito == "FORA" and sf <= sc)
        if nao_ganhando:
            stats_chutes = chutes_c if favorito == "CASA" else chutes_f
            if stats_chutes >= 18:
                sinal = "GOL (GIGANTE PRESSIONA)"
                insight = f"Gigante precisa do gol urgente."

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

# --- TRADUTOR DE SINAIS ---
def traduzir_instrucao(sinal, time_fav=""):
    if "PRÓXIMO GOL" in sinal:
        return f"Apostar no mercado **Próximo Gol** (Next Goal) a favor do **{time_fav}**."
    elif "MÚLTIPLA" in sinal:
        return "Adicionar este jogo na sua **Múltipla de Mais Gols** (Over 2.5 ou 3.5)."
    elif "GOL CEDO" in sinal:
        return "Entrar no mercado **Gol no 1º Tempo** (Over 0.5 HT)."
    elif "GIGANTE PRESSIONA" in sinal:
        return "Entrar em **Mais 1 Gol na partida** (Asian Goal ou Gol Limite)."
    else:
        return "Entrar em **Mais Gols** (Over Gols) na partida."

# --- SIMULAÇÃO ---
def gerar_sinais_teste():
    return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 35}}, "league": {"name": "La Liga"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Real Madrid"}, "away": {"name": "Almeria"}}}]
def gerar_odds_teste(fid): return (1.20, 15.00)
def gerar_stats_teste(fid): return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 15}, {"type": "Shots on Goal", "value": 6}, {"type": "Dangerous Attacks", "value": 45}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 1}, {"type": "Shots on Goal", "value": 1}, {"type": "Dangerous Attacks", "value": 5}]}]

# --- SCANNER PRINCIPAL ---
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
            radar_jogos.append({"Liga": jogo['league']['name'], "Tempo": f"{tempo}'", "Jogo": f"{jogo['teams']['home']['name']} vs {jogo['teams']['away']['name']}"})
            
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

                    # --- ENVIO TELEGRAM (MULTI) ---
                    if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
                    chave = f"{jogo['fixture']['id']}_{sinal}"
                    
                    if tg_token and tg_chat_ids and chave not in st.session_state['alertas_enviados']:
                        nome_fav_msg = tc if odd_casa < odd_fora else tf
                        instrucao_acao = traduzir_instrucao(sinal, nome_fav_msg)
                        
                        msg_tg = f"""
🚨 **ALERTA DE OPORTUNIDADE** 🚨

⚽ **{tc} x {tf}**
🏆 {jogo['league']['name']}
⏰ **{tempo}'** (Placar: {sc}-{sf})

💰 **SINAL:** {sinal}

✅ **O QUE FAZER:**
{instrucao_acao}

🧠 **MOTIVO:**
{motivo}

📊 **Estatísticas:**
• Chutes Totais: {chutes}
• Chutes no Gol: {no_gol}
• Ataques Perigosos: {atq_p}
"""
                        enviar_telegram_multi(tg_token, tg_chat_ids, msg_tg)
                        st.session_state['alertas_enviados'].add(chave)

    if not achou: st.info("Monitorando o mercado...")
    if radar_jogos:
        with st.expander(f"📡 Radar Ao Vivo ({len(radar_jogos)} jogos)", expanded=False):
            st.dataframe(pd.DataFrame(radar_jogos), hide_index=True, use_container_width=True)

# --- INTERFACE ---
st.title("🤖 Sniper de Gols - Tipster Família")

if ROBO_LIGADO:
    st.markdown('<div class="status-online">🟢 ROBÔ ONLINE</div>', unsafe_allow_html=True)
    st.caption(f"Atualizando a cada {INTERVALO}s...")
    executar_scanner()
    time.sleep(INTERVALO)
    st.rerun()
else:
    st.markdown('<div style="color: #FF4B4B; text-align: center; margin-bottom: 20px;">🔴 ROBÔ PAUSADO</div>', unsafe_allow_html=True)
    if st.button("📡 RASTREAR MANUALMENTE", type="primary", use_container_width=True):
        with st.spinner('Analisando...'):
            executar_scanner()
