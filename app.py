import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Neves Analytics PRO", layout="centered", page_icon="❄️")

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .status-box {padding: 15px; border-radius: 10px; text-align: center; margin-bottom: 20px; font-weight: bold;}
    .status-active {background-color: #1F4025; color: #00FF00; border: 1px solid #00FF00;}
    .timer-text { font-size: 14px; color: #FFD700; text-align: center; font-weight: bold; margin-top: 10px; border-top: 1px solid #333; padding-top: 10px;}
    .strategy-box { background-color: #1c1c1c; padding: 10px; border-radius: 5px; margin-top: 10px; border-left: 3px solid #00FF00; }
</style>
""", unsafe_allow_html=True)

# --- 2. GESTÃO DE DADOS E MEMÓRIA ---
DB_FILE = 'neves_dados.txt'
BLACK_FILE = 'neves_blacklist.txt'

if 'alertas_enviados' not in st.session_state:
    st.session_state['alertas_enviados'] = set()

if 'memoria_pressao' not in st.session_state:
    st.session_state['memoria_pressao'] = {}

def carregar_blacklist():
    if not os.path.exists(BLACK_FILE): return pd.DataFrame(columns=['id', 'País', 'Liga'])
    try: return pd.read_csv(BLACK_FILE)
    except: return pd.DataFrame(columns=['id', 'País', 'Liga'])

def salvar_na_blacklist(id_liga, pais, nome_liga):
    df = carregar_blacklist()
    if str(id_liga) not in df['id'].astype(str).values:
        novo = pd.DataFrame([{'id': str(id_liga), 'País': pais, 'Liga': nome_liga}])
        pd.concat([df, novo], ignore_index=True).to_csv(BLACK_FILE, index=False)

def enviar_telegram_real(token, chat_ids, mensagem):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                              data={"chat_id": cid.strip(), "text": mensagem, "parse_mode": "Markdown"}, timeout=5)
            except: pass

def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics PRO")
    
    # Checklist simples na lateral
    with st.expander("✅ Status das Estratégias", expanded=True):
        st.caption("Todas as estratégias estão armadas:")
        st.markdown("🟣 **A** - Porteira Aberta")
        st.markdown("🟢 **B** - Reação / Blitz")
        st.markdown("💰 **C** - Janela de Ouro")
        st.markdown("⚡ **D** - Gol Relâmpago")
    
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        
        st.markdown("---")
        if st.button("🔔 Testar Telegram"):
            enviar_telegram_real(tg_token, tg_chat_ids, "✅ *Neves PRO:* Interface Educativa Ativa.")
            st.toast("Enviado!")

        INTERVALO = st.slider("Ciclo (seg):", 30, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        
        if st.button("🗑️ Resetar Tudo"):
            if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
            st.session_state['alertas_enviados'] = set() 
            st.session_state['memoria_pressao'] = {}
            st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 4. API ---
@st.cache_data(ttl=3600)
def buscar_proximos(key):
    if not key and not MODO_DEMO: return []
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        data_hoje = agora_brasil().strftime('%Y-%m-%d')
        params = {"date": data_hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": key}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        # SIMULAÇÃO
        return [
            {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 47}}, "league": {"id": 1, "name": "Liga Acréscimo", "country": "BR"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Fav (47')"}, "away": {"name": "Zebra"}}},
            {"fixture": {"id": 2, "status": {"short": "HT", "elapsed": 45}}, "league": {"id": 2, "name": "Liga Intervalo", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}
        ]
    if not API_KEY: return []
    try:
        res = requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_stats(fid, demo_stage=0):
    if MODO_DEMO:
        if fid == 1: 
            return [{"team": {"name": "Home"}, "statistics": [{"type": "Total Shots", "value": 12}, {"type": "Shots on Goal", "value": 6}]}, {"team": {"name": "Away"}, "statistics": [{"type": "Total Shots", "value": 2}, {"type": "Shots on Goal", "value": 0}]}]
        return []
    return buscar_dados("statistics", {"fixture": fid})

# --- 5. MOMENTUM ---
def atualizar_momentum(fid, sog_h_atual, sog_a_atual):
    agora = datetime.now()
    janela_tempo = timedelta(minutes=7)
    
    if fid not in st.session_state['memoria_pressao']:
        st.session_state['memoria_pressao'][fid] = {'sog_h_total': sog_h_atual, 'sog_a_total': sog_a_atual, 'sog_h_timestamps': [], 'sog_a_timestamps': []}
        return 0, 0 

    memoria = st.session_state['memoria_pressao'][fid]
    
    delta_h = max(0, sog_h_atual - memoria['sog_h_total'])
    delta_a = max(0, sog_a_atual - memoria['sog_a_total'])
    
    for _ in range(delta_h): memoria['sog_h_timestamps'].append(agora)
    for _ in range(delta_a): memoria['sog_a_timestamps'].append(agora)
    
    memoria['sog_h_total'] = sog_h_atual
    memoria['sog_a_total'] = sog_a_atual
    
    memoria['sog_h_timestamps'] = [t for t in memoria['sog_h_timestamps'] if agora - t <= janela_tempo]
    memoria['sog_a_timestamps'] = [t for t in memoria['sog_a_timestamps'] if agora - t <= janela_tempo]
    
    st.session_state['memoria_pressao'][fid] = memoria
    return len(memoria['sog_h_timestamps']), len(memoria['sog_a_timestamps'])

# --- 6. PROCESSADOR ---
def processar_jogo(j, stats):
    f_id = j['fixture']['id']
    tempo = j['fixture']['status'].get('elapsed', 0)
    home = j['teams']['home']['name']
    away = j['teams']['away']['name']
    gh = j['goals']['home'] or 0
    ga = j['goals']['away'] or 0
    total_gols = gh + ga
    
    try:
        def get_val(team_idx, type_name):
            if not stats or len(stats) <= team_idx: return 0
            try:
                data = next((item for item in stats[team_idx]['statistics'] if item["type"] == type_name), None)
                return data['value'] if data and data['value'] else 0
            except: return 0

        sh_h = get_val(0, "Total Shots")
        sog_h = get_val(0, "Shots on Goal")
        sh_a = get_val(1, "Total Shots")
        sog_a = get_val(1, "Shots on Goal")
        total_chutes = sh_h + sh_a
        
        recentes_h, recentes_a = atualizar_momentum(f_id, sog_h, sog_a)
        
        # A) PORTEIRA ABERTA
        if tempo <= 30 and total_gols >= 2:
            return {
                "tag": "🟣 Porteira Aberta",
                "ordem": "Adicionar em Múltipla Over Gols",
                "motivo": f"Jogo frenético ({gh}x{ga} com {tempo}').",
                "stats": f"{gh}x{ga}"
            }

        # D) GOL RELÂMPAGO
        if 5 <= tempo <= 15:
            if (sog_h >= 1 or sog_a >= 1):
                return {
                    "tag": "⚡ Gol Relâmpago",
                    "ordem": "Apostar em Over 0.5 HT (1º Tempo)",
                    "motivo": "Início elétrico, goleiro já trabalhou.",
                    "stats": f"Chutes Alvo: {sog_h + sog_a}"
                }

        # B) REAÇÃO DO GIGANTE / BLITZ
        if tempo <= 60:
            if (gh <= ga) and (recentes_h >= 2 or sh_h >= 6):
                oponente_vivo = (recentes_a >= 1 or sh_a >= 4)
                acao = "⚠️ Jogo Aberto: Entrar em OVER GOLS" if oponente_vivo else "💎 BLITZ HOME: Back Home ou Gol Limite"
                return {
                    "tag": "🟢 Reação/Blitz",
                    "ordem": acao,
                    "motivo": f"{home} amassando! {recentes_h} chutes no alvo (7') ou {sh_h} totais.",
                    "stats": f"Blitz Recente: {recentes_h} | Total: {sh_h}"
                }
            
            if (ga <= gh) and (recentes_a >= 2 or sh_a >= 6):
                oponente_vivo = (recentes_h >= 1 or sh_h >= 4)
                acao = "⚠️ Jogo Aberto: Entrar em OVER GOLS" if oponente_vivo else "💎 BLITZ AWAY: Back Away ou Gol Limite"
                return {
                    "tag": "🟢 Reação/Blitz",
                    "ordem": acao,
                    "motivo": f"{away} amassando! {recentes_a} chutes no alvo (7') ou {sh_a} totais.",
                    "stats": f"Blitz Recente: {recentes_a} | Total: {sh_a}"
                }

        # C) JANELA DE OURO
        if 70 <= tempo <= 75:
            if total_chutes >= 18 and abs(gh - ga) <= 1:
                return {
                    "tag": "💰 Janela de Ouro",
                    "ordem": "Entrar em Mais 1.0 Gol (Asiático)",
                    "motivo": "Pressão final absurda e placar apertado.",
                    "stats": f"Total Chutes: {total_chutes}"
                }

    except: return None
    return None

# --- 7. EXECUÇÃO ---
main_placeholder = st.empty()

if ROBO_LIGADO:
    df_black = carregar_blacklist()
    ids_bloqueados = df_black['id'].astype(str).values
    
    jogos_live = buscar_dados("fixtures", {"live": "all"})
    radar = []
    
    # --- PROCESSAMENTO ---
    for j in jogos_live:
        l_id = str(j['league']['id'])
        if l_id in ids_bloqueados: continue
        
        f_id = j['fixture']['id']
        tempo = j['fixture']['status'].get('elapsed', 0)
        status_short = j['fixture']['status'].get('short', '')
        home = j['teams']['home']['name']
        away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"
        
        eh_intervalo = (status_short in ['HT', 'BT']) or (48 <= tempo <= 52)
        eh_aquecimento = (tempo < 5)
        eh_fim = (tempo > 80)
        dentro_janela = not (eh_intervalo or eh_aquecimento or eh_fim)
        
        sinal = None
        icone_visual = "👁️"
        
        if eh_aquecimento: icone_visual = "⏳"
        elif eh_intervalo: icone_visual = "💤"
        elif eh_fim: icone_visual = "🏁"
        
        if dentro_janela:
            stats = buscar_stats(f_id)
            if not stats and not MODO_DEMO:
                salvar_na_blacklist(l_id, j['league']['country'], j['league']['name'])
                continue
            
            sinal = processar_jogo(j, stats)
            
            if sinal:
                icone_visual = "✅"
                if f_id not in st.session_state['alertas_enviados']:
                    msg = (
                        f"🚨 *NEVES ANALYTICS PRO* 🚨\n\n"
                        f"⚽ *{home}* {placar} *{away}*\n"
                        f"🏆 {j['league']['name']}\n"
                        f"⏰ {tempo}'\n\n"
                        f"🧩 *Estratégia:* {sinal['tag']}\n"
                        f"⚠️ *ORDEM:*\n"
                        f"✅ *{sinal['ordem']}*\n\n"
                        f"📊 *Motivo:* {sinal['motivo']}\n"
                        f"📈 *Dados:* {sinal['stats']}"
                    )
                    enviar_telegram_real(tg_token, tg_chat_ids, msg)
                    st.session_state['alertas_enviados'].add(f_id)
                    st.toast(f"Sinal Enviado: {sinal['tag']}")

        mem = st.session_state['memoria_pressao'].get(f_id, {})
        rec_h = len(mem.get('sog_h_timestamps', [])) if mem else 0
        rec_a = len(mem.get('sog_a_timestamps', [])) if mem else 0
        info_mom = f" | ⚡ {rec_h}x{rec_a}" if (rec_h + rec_a) > 0 else ""

        radar.append({
            "Liga": j['league']['name'],
            "Jogo": f"{home} {placar} {away}",
            "Tempo": f"{tempo}'",
            "Status": f"{icone_visual} {sinal['tag'] if sinal else ''}{info_mom}"
        })

    # --- AGENDA ---
    prox_raw = buscar_proximos(API_KEY)
    prox_filtrado = []
    hora_atual = agora_brasil().strftime('%H:%M')
    
    for p in prox_raw:
        lid = str(p['league']['id'])
        status = p['fixture']['status']['short']
        data_jogo_raw = p['fixture']['date']
        
        if lid in ids_bloqueados: continue
        if status != 'NS': continue
        
        hora_jogo = data_jogo_raw[11:16]
        if hora_jogo <= hora_atual: continue 
        
        prox_filtrado.append({
            "Hora": hora_jogo, 
            "Liga": p['league']['name'], 
            "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"
        })

    # --- EXIBIÇÃO ---
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.markdown('<div class="status-box status-active">🟢 SISTEMA DE MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        t1, t2, t3 = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(prox_filtrado)})", f"🚫 Blacklist ({len(df_black)})"])
        
        with t1:
            if radar:
                st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
            else:
                st.info("Monitorando jogos...")
            
        with t2:
            if prox_filtrado:
                st.dataframe(pd.DataFrame(prox_filtrado).sort_values("Hora"), use_container_width=True, hide_index=True)
            else:
                st.caption("Sem mais jogos por hoje.")
            
        with t3:
            if not df_black.empty:
                st.table(df_black)
            else:
                st.caption("Limpo.")

        relogio = st.empty()
        for i in range(INTERVALO, 0, -1):
            relogio.markdown(f'<div class="timer-text">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)

        # --- NOVA ÁREA: DETALHAMENTO DAS ESTRATÉGIAS (RODAPÉ) ---
        with st.expander("📘 Guia de Inteligência: Como o Robô decide?", expanded=False):
            st.markdown("""
            ### 1. 🟣 A - Porteira Aberta
            * **Cenário:** Jogo frenético antes dos 30 minutos.
            * **Gatilho:** Tempo < 30' e Placar com 2 ou mais gols (ex: 2x0, 1x1).
            * **Ação:** Buscar Múltiplas de Over Gols.

            ### 2. ⚡ D - Gol Relâmpago
            * **Cenário:** Início elétrico de partida.
            * **Gatilho:** Entre 5' e 15', com chutes no alvo imediatos.
            * **Ação:** Over 0.5 HT (Gol no 1º Tempo).
            
            ### 3. 🟢 B - Reação do Gigante / Blitz
            * **Cenário:** Time perdendo ou empatando, mas massacrando.
            * **Gatilho (< 60'):** * **Volume:** 6+ chutes totais.
                * **Blitz (Momentum):** 2+ chutes no alvo nos últimos **7 minutos**.
            * **Ação:** Back ao time que pressiona (se oponente estiver morto) ou Over Gols (se jogo estiver lá e cá).

            ### 4. 💰 C - Janela de Ouro
            * **Cenário:** Reta final com jogo indefinido e pressão.
            * **Gatilho:** Entre 70' e 75', jogo empatado ou diferença de 1 gol, com **18+ chutes totais**.
            * **Ação:** Over Limite (Gol Asiático).
            """)
    
    st.rerun()

else:
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure e ligue na lateral.")
