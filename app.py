import streamlit as st
import pandas as pd
import requests
import time
import os
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(
    page_title="Neves Analytics",
    layout="centered",
    page_icon="❄️"
)

st.markdown("""
<style>
    .stApp {background-color: #121212; color: white;}
    .card {background-color: #1E1E1E; padding: 20px; border-radius: 15px; border: 1px solid #333; margin-bottom: 20px;}
    .titulo-time {font-size: 20px; font-weight: bold; color: #ffffff;}
    .odd-label {font-size: 12px; color: #aaa; background-color: #333; padding: 2px 6px; border-radius: 4px;}
    .placar {font-size: 35px; font-weight: 800; color: #FFD700; text-align: center;}
    .tempo {font-size: 14px; color: #FF4B4B; font-weight: bold; text-align: center;}
    .sinal-box {background-color: #00C853; color: white; padding: 10px; border-radius: 8px; text-align: center; font-weight: bold; margin-top: 15px;}
    .multipla-box {background-color: #9C27B0; color: white; padding: 10px; border-radius: 8px; text-align: center; font-weight: bold; margin-top: 15px;}
    .alerta-over-box {background-color: #FF9800; color: white; padding: 10px; border-radius: 8px; text-align: center; font-weight: bold; margin-top: 15px;}
    .status-online {color: #00FF00; font-weight: bold; animation: pulse 2s infinite; padding: 5px 10px; border: 1px solid #00FF00; text-align: center; margin-bottom: 20px; border-radius: 15px;}
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.7); } 70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); } 100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); } }
    .metric-val {font-size: 22px; font-weight: bold;}
    .metric-label {font-size: 10px; color: #888; text-transform: uppercase;}
    .stats-row { display: flex; justify-content: space-around; text-align: center; margin-top: 15px; padding-top: 15px; border-top: 1px solid #333; }
</style>
""", unsafe_allow_html=True)

# --- 2. AJUSTE DE FUSO HORÁRIO (BRASIL) ---
def agora_brasil():
    # Subtrai 3 horas do horário UTC do servidor
    return datetime.utcnow() - timedelta(hours=3)

# --- 3. FUNÇÕES DE BANCO DE DADOS E NOTIFICAÇÃO ---
DB_FILE = 'neves_dados.txt'

def enviar_msg_telegram(token, chat_ids, mensagem):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid.strip(), "text": mensagem, "parse_mode": "Markdown"})
            except: pass

def carregar_db():
    if not os.path.exists(DB_FILE):
        df = pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
        df.to_csv(DB_FILE, index=False)
        return df
    try: return pd.read_csv(DB_FILE)
    except: return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])

def salvar_sinal_db(fixture_id, jogo, sinal, gols_inicial):
    df = carregar_db()
    if not ((df['id'] == fixture_id) & (df['status'] == 'Pendente')).any():
        data_br = agora_brasil()
        novo_registro = {
            'id': fixture_id,
            'data': data_br.strftime('%Y-%m-%d'),
            'hora': data_br.strftime('%H:%M'), # Hora correta BR
            'jogo': jogo,
            'sinal': sinal,
            'gols_inicial': gols_inicial,
            'status': 'Pendente'
        }
        df = pd.concat([df, pd.DataFrame([novo_registro])], ignore_index=True)
        df.to_csv(DB_FILE, index=False)

def atualizar_status_db(lista_jogos_api, tg_token=None, tg_chat_ids=None):
    df = carregar_db()
    if df.empty: return df
    
    modificado = False
    pendentes = df[df['status'] == 'Pendente']
    
    for index, row in pendentes.iterrows():
        jogo_dados = next((j for j in lista_jogos_api if j['fixture']['id'] == row['id']), None)
        if jogo_dados:
            gols_agora = (jogo_dados['goals']['home'] or 0) + (jogo_dados['goals']['away'] or 0)
            status_match = jogo_dados['fixture']['status']['short']
            
            # --- LÓGICA DE GREEN ---
            if gols_agora > row['gols_inicial']:
                df.at[index, 'status'] = 'Green'
                modificado = True
                if tg_token and tg_chat_ids:
                    msg = f"✅ **GREEN! PAGOU!** 💰\n\n⚽ **{row['jogo']}**\n\nO sinal de **{row['sinal']}** bateu!\nDinheiro no bolso. A análise foi perfeita. 🚀"
                    enviar_msg_telegram(tg_token, tg_chat_ids, msg)

            # --- LÓGICA DE RED ---
            elif status_match in ['FT', 'AET', 'PEN']:
                df.at[index, 'status'] = 'Red'
                modificado = True
                if tg_token and tg_chat_ids:
                    msg = f"🔻 **RED - SEGUE O PLANO**\n\n⚽ **{row['jogo']}**\n\nO sinal não bateu desta vez.\nFaz parte do jogo. Cabeça fria e foco na próxima oportunidade! 💪"
                    enviar_msg_telegram(tg_token, tg_chat_ids, msg)
    
    if modificado:
        df.to_csv(DB_FILE, index=False)
    return df

def gerar_texto_relatorio():
    df = carregar_db()
    if df.empty: return None
    
    df['data'] = pd.to_datetime(df['data'])
    
    hoje = pd.Timestamp(agora_brasil().date())
    
    df_hoje = df[df['data'] == hoje]
    start_week = hoje - timedelta(days=hoje.weekday())
    df_semana = df[df['data'] >= start_week]
    df_mes = df[(df['data'].dt.month == hoje.month) & (df['data'].dt.year == hoje.year)]
    
    def calcular(d):
        total = len(d)
        g = len(d[d['status'] == 'Green'])
        r = len(d[d['status'] == 'Red'])
        taxa = (g / (g + r) * 100) if (g + r) > 0 else 0
        return g, r, taxa

    gh, rh, ph = calcular(df_hoje)
    gs, rs, ps = calcular(df_semana)
    gm, rm, pm = calcular(df_mes)
    
    msg = f"""
📊 **FECHAMENTO NEVES ANALYTICS** 📊
📅 {hoje.strftime('%d/%m/%Y')}

**HOJE:**
✅ Green: {gh}
🔻 Red: {rh}
💰 Assertividade: {ph:.1f}%

**SEMANA:**
📈 {gs} x {rs} ({ps:.1f}%)

**MÊS:**
🏆 {gm} Greens / {rm} Reds ({pm:.1f}%)
"""
    return msg

# --- 4. SIDEBAR E CONFIGURAÇÕES ---
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
            enviar_msg_telegram(tg_token, tg_chat_ids, "✅ Teste Neves Analytics!")
            st.success("Enviado!")
    
    st.markdown("---")
    st.header("🤖 Modo Automático")
    ROBO_LIGADO = st.checkbox("LIGAR ROBÔ", value=False)
    INTERVALO = st.slider("Ciclo (segundos):", 60, 300, 60)
    
    if st.button("📉 Enviar Relatório Agora"):
        if tg_token and tg_chat_ids:
            rel = gerar_texto_relatorio()
            if rel:
                enviar_msg_telegram(tg_token, tg_chat_ids, rel)
                st.success("Relatório enviado!")
            else: st.warning("Sem dados ainda.")
            
    MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)

# --- 5. FUNÇÕES DE API ---
def gerar_sinais_teste(): 
    return [{"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 35}}, "league": {"name": "Simulacao"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Real"}, "away": {"name": "Almeria"}}}]
def gerar_odds_teste(fid): return (1.20, 15.00)
def gerar_stats_teste(fid): 
    return [{"team": {"name": "C"}, "statistics": [{"type": "Total Shots", "value": 15}, {"type": "Shots on Goal", "value": 6}, {"type": "Dangerous Attacks", "value": 50}]}, {"team": {"name": "F"}, "statistics": [{"type": "Total Shots", "value": 5}, {"type": "Shots on Goal", "value": 3}, {"type": "Dangerous Attacks", "value": 20}]}]

def buscar_jogos(api_key):
    if MODO_DEMO: return gerar_sinais_teste()
    url = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": api_key}
    
    data_hoje_br = agora_brasil().strftime('%Y-%m-%d')
    # AQUI ESTÁ A CORREÇÃO: timezone SP para pegar jogos da noite!
    try: return requests.get(url, headers=headers, params={"date": data_hoje_br, "timezone": "America/Sao_Paulo"}).json().get('response', [])
    except: return []

def buscar_stats(api_key, fixture_id):
    if MODO_DEMO: return gerar_stats_teste(fixture_id)
    url = "https://v3.football.api-sports.io/fixtures/statistics"
    headers = {"x-apisports-key": api_key}
    try: return requests.get(url, headers=headers, params={"fixture": fixture_id}).json().get('response', [])
    except: return []

if 'odds_cache' not in st.session_state: st.session_state['odds_cache'] = {}

def buscar_odds_cached(api_key, fixture_id):
    if MODO_DEMO: return gerar_odds_teste(fixture_id)
    if fixture_id in st.session_state['odds_cache']:
        return st.session_state['odds_cache'][fixture_id]
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
                st.session_state['odds_cache'][fixture_id] = (odd_casa, odd_fora)
                return odd_casa, odd_fora
    except: pass
    return 0, 0

# --- 6. CÉREBRO ---
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
            za = atq_f if favorito == "CASA" else atq_c
            if fc >= 6 and fa > 30:
                zebra_viva = (zc >= 4) or (za >= 15)
                if not zebra_viva:
                    sinal = f"PRÓXIMO GOL: {nome_favorito}"
                    insight = f"Gigante ({nome_favorito}) perde mas domina. Zebra inofensiva."
                    tipo_sinal = "normal"
                else:
                    sinal = "JOGO ABERTO (OVER GOLS)"
                    insight = f"Favorito desesperado, mas Zebra perigosa! Over Gols."
                    tipo_sinal = "over"
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
    elif "JOGO ABERTO" in sinal: return "Entrar em **Over 2.5** ou **Over HT**."
    elif "GOL CEDO" in sinal: return "Entrar em **Over 0.5 HT**."
    elif "GIGANTE" in sinal: return "Entrar em **Mais 1 Gol**."
    else: return "Entrar em **Mais Gols**."

# --- 7. EXECUÇÃO PRINCIPAL ---
st.title("❄️ Neves Analytics")

if ROBO_LIGADO:
    if not API_KEY and not MODO_DEMO:
        st.error("⚠️ Coloque a API Key na barra lateral!")
    else:
        st.markdown('<div class="status-online">🟢 SISTEMA ONLINE</div>', unsafe_allow_html=True)
        st.caption(f"Ciclo: {INTERVALO}s | Banco: neves_dados.txt (Fuso BR)")
        
        jogos = buscar_jogos(API_KEY)
        atualizar_status_db(jogos, tg_token, tg_chat_ids)
        
        # Relatório Automático 22h
        if 'relatorio_enviado_hoje' not in st.session_state: st.session_state['relatorio_enviado_hoje'] = None
        hora_br = int(agora_brasil().strftime('%H'))
        data_br = agora_brasil().strftime('%Y-%m-%d')
        
        if hora_br >= 22 and st.session_state['relatorio_enviado_hoje'] != data_br:
            rel = gerar_texto_relatorio()
            if rel and tg_token and tg_chat_ids:
                enviar_msg_telegram(tg_token, tg_chat_ids, rel)
                st.session_state['relatorio_enviado_hoje'] = data_br

        achou = False
        radar = []
        prox = []
        
        for jogo in jogos:
            status = jogo['fixture']['status']['short']
            tempo = jogo['fixture']['status'].get('elapsed', 0)
            
            if status == 'NS':
                # CORREÇÃO DA HORA DE EXIBIÇÃO (-3h)
                ts = jogo['fixture']['timestamp']
                hora_j = (datetime.fromtimestamp(ts) - timedelta(hours=3)).strftime('%H:%M')
                prox.append({"Hora": hora_j, "Liga": jogo['league']['name'], "Jogo": f"{jogo['teams']['home']['name']} vs {jogo['teams']['away']['name']}"})
            
            elif status in ['1H', '2H'] and tempo:
                info = {"Liga": jogo['league']['name'], "Tempo": f"{tempo}'", "Jogo": f"{jogo['teams']['home']['name']} {jogo['goals']['home']}x{jogo['goals']['away']} {jogo['teams']['away']['name']}", "Status": "👁️"}
                zona_quente = (tempo <= 50) or (70 <= tempo <= 75)
                
                if zona_quente:
                    stats = buscar_stats(API_KEY, jogo['fixture']['id'])
                    if stats:
                        odd_casa, odd_fora = buscar_odds_cached(API_KEY, jogo['fixture']['id'])
                        
                        s_casa = {i['type']: i['value'] for i in stats[0]['statistics']}
                        s_fora = {i['type']: i['value'] for i in stats[1]['statistics']}
                        tc = jogo['teams']['home']['name']; tf = jogo['teams']['away']['name']
                        sc = jogo['goals']['home'] or 0; sf = jogo['goals']['away'] or 0
                        
                        sinal, motivo, chutes, no_gol, atq_p, tipo = analisar_partida(tempo, s_casa, s_fora, tc, tf, sc, sf, odd_casa, odd_fora)
                        
                        if sinal:
                            achou = True
                            cls = "multipla-box" if tipo=="multipla" else "alerta-over-box" if tipo=="over" else "sinal-box"
                            st.markdown(f"""<div class="card"><div style="display:flex; justify-content:space-between;"><div style="width:40%"><div class="titulo-time">{tc}</div><span class="odd-label">{odd_casa:.2f}</span></div><div style="width:20%;text-align:center"><div class="placar">{sc}-{sf}</div><div class="tempo">{tempo}'</div></div><div style="width:40%;text-align:right"><div class="titulo-time">{tf}</div><span class="odd-label">{odd_fora:.2f}</span></div></div><div class="{cls}">{sinal}</div><div class="insight-texto">{motivo}</div><div class="stats-row"><div><div class="metric-label">CHUTES</div><div class="metric-val">{chutes}</div></div><div><div class="metric-label">PERIGO</div><div class="metric-val" style="color:#FFD700;">{atq_p}</div></div></div></div>""", unsafe_allow_html=True)
                            
                            salvar_sinal_db(jogo['fixture']['id'], f"{tc} x {tf}", sinal, sc+sf)
                            
                            if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
                            chave = f"{jogo['fixture']['id']}_{sinal}"
                            if tg_token and tg_chat_ids and chave not in st.session_state['alertas_enviados']:
                                fav = tc if odd_casa < odd_fora else tf
                                msg = f"🚨 **NEVES ANALYTICS**\n\n⚽ {tc} {sc}x{sf} {tf}\n⏰ {tempo}'\n💰 **{sinal}**\n\n✅ {traduzir_instrucao(sinal, fav)}\n\n📊 Chutes: {chutes} | Perigo: {atq_p}"
                                enviar_msg_telegram(tg_token, tg_chat_ids, msg)
                                st.session_state['alertas_enviados'].add(chave)
                else: info["Status"] = "💤"
                radar.append(info)
        
        if not achou: st.info("Monitorando mercado...")
        
        t1, t2, t3 = st.tabs(["📡 Ao Vivo", "📅 Próximos", "📊 Performance"])
        
        with t1: 
            if radar:
                df_radar = pd.DataFrame(radar)
                st.dataframe(df_radar, hide_index=True, use_container_width=True)
            else:
                st.caption("Sem jogos ao vivo.")
                
        with t2:
            if prox:
                df_prox = pd.DataFrame(sorted(prox, key=lambda x: x['Hora']))
                st.dataframe(df_prox, hide_index=True, use_container_width=True)
            else:
                st.caption("Sem jogos futuros na lista de hoje (BR).")
        
        with t3:
            df_hist = carregar_db()
            if not df_hist.empty:
                g = len(df_hist[df_hist['status']=='Green']); r = len(df_hist[df_hist['status']=='Red'])
                st.metric("Total Greens", g); st.metric("Total Reds", r)
                # Gráfico
                if g > 0 or r > 0:
                    fig = px.pie(names=['Green', 'Red', 'Pendente'], values=[g, r, len(df_hist[df_hist['status']=='Pendente'])], color_discrete_sequence=['#00C853', '#D50000', '#FFD600'])
                    st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_hist, hide_index=True, use_container_width=True)
            else: st.info("Sem dados.")

        time.sleep(INTERVALO)
        st.rerun()

else:
    st.markdown('<div style="color: #FF4B4B; text-align: center; margin-bottom: 20px;">🔴 SISTEMA PAUSADO</div>', unsafe_allow_html=True)
    if st.button("Rastrear Manual"): st.rerun()
