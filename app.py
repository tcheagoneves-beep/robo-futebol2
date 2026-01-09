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
</style>
""", unsafe_allow_html=True)

# --- 2. GESTÃO DE DADOS E MEMÓRIA ---
DB_FILE = 'neves_dados.txt'
BLACK_FILE = 'neves_blacklist.txt'

# Memória de Alertas
if 'alertas_enviados' not in st.session_state:
    st.session_state['alertas_enviados'] = set()

# MEMÓRIA DE PRESSÃO (O SEGREDO DA LEITURA DE FLUXO)
# Estrutura: { 'fixture_id': { 'home_sog_history': [timestamp1, timestamp2], 'away_sog_history': [], 'last_check': timestamp } }
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
    
    with st.expander("🧠 Inteligência de Fluxo", expanded=True):
        st.info(
            """
            **Detector de Blitz (Novo):**
            O robô agora ignora chutes antigos.
            Ele só alerta se identificar **2+ Chutes no Alvo**
            numa janela recente de **7 minutos**.
            
            _Isso evita falsos positivos de times que chutaram
            no 1º tempo e pararam._
            """
        )
    
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        
        st.markdown("---")
        if st.button("🔔 Testar Telegram"):
            enviar_telegram_real(tg_token, tg_chat_ids, "✅ *Neves PRO:* Detector de Blitz Ativo.")
            st.toast("Enviado!")

        INTERVALO = st.slider("Ciclo (seg):", 30, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        
        if st.button("🗑️ Resetar Tudo"):
            if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
            st.session_state['alertas_enviados'] = set() 
            st.session_state['memoria_pressao'] = {} # Reseta a memória de fluxo
            st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 4. API ---
@st.cache_data(ttl=3600)
def buscar_proximos(key):
    if not key and not MODO_DEMO: return []
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": agora_brasil().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": key}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        # SIMULAÇÃO: Cenário onde o time começa a chutar AGORA
        return [
            {"fixture": {"id": 999, "status": {"short": "2H", "elapsed": 65}}, 
             "league": {"id": 1, "name": "Liga Blitz", "country": "BR"}, 
             "goals": {"home": 0, "away": 1}, 
             "teams": {"home": {"name": "Time Reação"}, "away": {"name": "Time Fav"}}}
        ]
    if not API_KEY: return []
    try:
        res = requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_stats(fid, demo_stage=0):
    if MODO_DEMO:
        # Simula aumento de chutes conforme o tempo passa (para testar a memória)
        # Se você rodar várias vezes, simula a atualização
        base = 2
        now = int(time.time())
        if now % 120 > 60: base = 4 # Aumenta 2 chutes de repente
        
        return [
            {"team": {"name": "Home"}, "statistics": [{"type": "Total Shots", "value": 10}, {"type": "Shots on Goal", "value": base}, {"type": "Dangerous Attacks", "value": 45}]},
            {"team": {"name": "Away"}, "statistics": [{"type": "Total Shots", "value": 5}, {"type": "Shots on Goal", "value": 1}, {"type": "Dangerous Attacks", "value": 15}]}
        ]
    return buscar_dados("statistics", {"fixture": fid})

# --- 5. GESTOR DE MOMENTUM (A NOVA INTELIGÊNCIA) ---
def atualizar_momentum(fid, sog_h_atual, sog_a_atual):
    agora = datetime.now()
    janela_tempo = timedelta(minutes=7) # Janela de 7 minutos exigida
    
    # Inicializa se não existir
    if fid not in st.session_state['memoria_pressao']:
        st.session_state['memoria_pressao'][fid] = {
            'sog_h_total': sog_h_atual,
            'sog_a_total': sog_a_atual,
            'sog_h_timestamps': [],
            'sog_a_timestamps': []
        }
        return 0, 0 # Primeiro ciclo não tem histórico de "novos" chutes

    memoria = st.session_state['memoria_pressao'][fid]
    
    # 1. Detectar NOVOS chutes (Delta)
    delta_h = max(0, sog_h_atual - memoria['sog_h_total'])
    delta_a = max(0, sog_a_atual - memoria['sog_a_total'])
    
    # 2. Registrar o momento dos novos chutes
    for _ in range(delta_h): memoria['sog_h_timestamps'].append(agora)
    for _ in range(delta_a): memoria['sog_a_timestamps'].append(agora)
    
    # 3. Atualizar totais conhecidos
    memoria['sog_h_total'] = sog_h_atual
    memoria['sog_a_total'] = sog_a_atual
    
    # 4. LIMPEZA: Remover chutes velhos (> 7 min)
    memoria['sog_h_timestamps'] = [t for t in memoria['sog_h_timestamps'] if agora - t <= janela_tempo]
    memoria['sog_a_timestamps'] = [t for t in memoria['sog_a_timestamps'] if agora - t <= janela_tempo]
    
    # 5. Salvar de volta
    st.session_state['memoria_pressao'][fid] = memoria
    
    # Retorna quantos chutes ocorreram NOS ÚLTIMOS 7 MIN
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

        # Stats Totais
        sh_h = get_val(0, "Total Shots")
        sog_h = get_val(0, "Shots on Goal")
        sh_a = get_val(1, "Total Shots")
        sog_a = get_val(1, "Shots on Goal")
        total_chutes = sh_h + sh_a
        
        # --- CÁLCULO DE MOMENTUM (BLITZ) ---
        # Quantos chutes no alvo aconteceram nos últimos 7 minutos?
        recentes_h, recentes_a = atualizar_momentum(f_id, sog_h, sog_a)
        
        # A) PORTEIRA ABERTA (< 30 min, 2+ Gols)
        if tempo <= 30 and total_gols >= 2:
            return {
                "tag": "🟣 Porteira Aberta",
                "ordem": "Adicionar em Múltipla Over Gols",
                "motivo": f"Jogo frenético ({gh}x{ga} com {tempo}').",
                "stats": f"{gh}x{ga}"
            }

        # D) GOL RELÂMPAGO (5 a 15 min)
        if 5 <= tempo <= 15:
            if (sog_h >= 1 or sog_a >= 1):
                return {
                    "tag": "⚡ Gol Relâmpago",
                    "ordem": "Apostar em Over 0.5 HT (1º Tempo)",
                    "motivo": "Times ligados, goleiro já trabalhou.",
                    "stats": f"Chutes Alvo: {sog_h + sog_a}"
                }

        # B) REAÇÃO DO GIGANTE (Até 65 min) - AGORA COM FILTRO DE 7 MINUTOS
        if tempo <= 65:
            # HOME PRESSIONANDO
            # Critério: 2 chutes no alvo nos últimos 7 min (BLITZ) OU Pressão absurda acumulada
            blitz_h = (recentes_h >= 2) 
            if (gh <= ga) and blitz_h:
                oponente_vivo = (recentes_a >= 1) # Oponente respondeu nos últimos 7 min?
                acao = "⚠️ Jogo Aberto: Entrar em OVER GOLS" if oponente_vivo else "💎 BLITZ HOME: Back Home ou Gol Limite"
                return {
                    "tag": "🟢 Reação/Blitz",
                    "ordem": acao,
                    "motivo": f"{home} deu {recentes_h} chutes no alvo nos últimos 7 min!",
                    "stats": f"Blitz Recente: {recentes_h} chutes (7')"
                }
            
            # AWAY PRESSIONANDO
            blitz_a = (recentes_a >= 2)
            if (ga <= gh) and blitz_a:
                oponente_vivo = (recentes_h >= 1)
                acao = "⚠️ Jogo Aberto: Entrar em OVER GOLS" if oponente_vivo else "💎 BLITZ AWAY: Back Away ou Gol Limite"
                return {
                    "tag": "🟢 Reação/Blitz",
                    "ordem": acao,
                    "motivo": f"{away} deu {recentes_a} chutes no alvo nos últimos 7 min!",
                    "stats": f"Blitz Recente: {recentes_a} chutes (7')"
                }

        # C) JANELA DE OURO (70 a 75 min)
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

# --- 6. EXECUÇÃO ---
main_placeholder = st.empty()

if ROBO_LIGADO:
    df_black = carregar_blacklist()
    ids_bloqueados = df_black['id'].astype(str).values
    
    jogos_live = buscar_dados("fixtures", {"live": "all"})
    radar = []
    
    for j in jogos_live:
        l_id = str(j['league']['id'])
        if l_id in ids_bloqueados: continue
        
        f_id = j['fixture']['id']
        tempo = j['fixture']['status'].get('elapsed', 0)
        home = j['teams']['home']['name']
        away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"
        
        dentro_janela = (5 <= tempo <= 65) or (70 <= tempo <= 75)
        
        sinal = None
        icone_visual = "👁️"
        
        if tempo < 5: icone_visual = "⏳"
        elif tempo > 80: icone_visual = "🏁"
        
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
                        f"🔥 *ALERTA DE BLITZ (7')* 🔥\n\n"
                        f"⚽ *{home}* {placar} *{away}*\n"
                        f"🏆 {j['league']['name']}\n"
                        f"⏰ {tempo}'\n\n"
                        f"🧩 *Estratégia:* {sinal['tag']}\n"
                        f"⚠️ *ORDEM:*\n"
                        f"✅ *{sinal['ordem']}*\n\n"
                        f"📊 *Inteligência:*\n"
                        f"{sinal['motivo']}"
                    )
                    enviar_telegram_real(tg_token, tg_chat_ids, msg)
                    st.session_state['alertas_enviados'].add(f_id)
                    st.toast(f"Sinal Enviado: {sinal['tag']}")

        # Mostra Momentum Visual se houver
        mem = st.session_state['memoria_pressao'].get(f_id, {})
        rec_h = len(mem.get('sog_h_timestamps', [])) if mem else 0
        rec_a = len(mem.get('sog_a_timestamps', [])) if mem else 0
        
        info_mom = ""
        if rec_h >= 1 or rec_a >= 1: info_mom = f" | ⚡ {rec_h}x{rec_a} (Recentes)"

        radar.append({
            "Liga": j['league']['name'],
            "Jogo": f"{home} {placar} {away}",
            "Tempo": f"{tempo}'",
            "Status": f"{icone_visual} {sinal['tag'] if sinal else ''} {info_mom}"
        })

    prox = buscar_proximos(API_KEY)
    prox_f = [{"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"} 
              for p in prox if str(p['league']['id']) not in ids_bloqueados and p['fixture']['status']['short'] == 'NS']

    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.markdown('<div class="status-box status-active">🟢 SISTEMA DE MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        t1, t2, t3 = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(prox_f)})", f"🚫 Blacklist ({len(df_black)})"])
        
        with t1:
            if radar:
                st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
            else:
                st.info("Aguardando novas oportunidades...")
            
        with t2:
            st.dataframe(pd.DataFrame(prox_f).sort_values("Hora"), use_container_width=True, hide_index=True) if prox_f else st.caption("Vazio.")
            
        with t3:
            st.table(df_black) if not df_black.empty else st.caption("Limpo.")

        relogio = st.empty()
        for i in range(INTERVALO, 0, -1):
            relogio.markdown(f'<div class="timer-text">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
    
    st.rerun()

else:
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera.")
