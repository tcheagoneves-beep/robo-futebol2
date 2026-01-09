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
    .strategy-tag { font-size: 11px; padding: 2px 6px; border-radius: 4px; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# --- 2. GESTÃO DE DADOS ---
DB_FILE = 'neves_dados.txt'
BLACK_FILE = 'neves_blacklist.txt'

if 'alertas_enviados' not in st.session_state:
    st.session_state['alertas_enviados'] = set()

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
    
    with st.expander("🧠 Cérebro (Estratégias)", expanded=True):
        st.markdown("""
        🟣 **A - Porteira Aberta (<30')**
        _2+ gols cedo: Múltipla Over._
        
        ⚡ **D - Gol Relâmpago (5-15')**
        _Início frenético: Over HT._
        
        🟢 **B - Reação Gigante (<50')**
        _Fav. perdendo + Pressão: Back ou Over._
        
        💰 **C - Janela de Ouro (70-75')**
        _Fav. não ganha + 18 chutes: Over Limite._
        """)
    
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        
        if st.button("🔔 Testar Telegram"):
            enviar_telegram_real(tg_token, tg_chat_ids, "✅ *Neves PRO:* Sistema Online.")
            st.toast("Enviado!")

        INTERVALO = st.slider("Ciclo (seg):", 30, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação (Teste das 4 Estratégias)", value=False)
        
        if st.button("🗑️ Resetar Tudo"):
            if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
            st.session_state['alertas_enviados'] = set() 
            st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 4. API & DATA MINING ---
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
        # SIMULAÇÃO DAS 4 ESTRATÉGIAS PARA TESTE
        return [
            # A: Porteira Aberta (25min, 1x1)
            {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 25}}, "league": {"id": 1, "name": "Liga A", "country": "BR"}, "goals": {"home": 1, "away": 1}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}},
            # B: Reação Gigante (40min, Fav perdendo 0x1, amassando)
            {"fixture": {"id": 2, "status": {"short": "1H", "elapsed": 40}}, "league": {"id": 2, "name": "Liga B", "country": "UK"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Man City (Fav)"}, "away": {"name": "Zebra FC"}}},
            # C: Janela Ouro (72min, 0x0, Pressão total)
            {"fixture": {"id": 3, "status": {"short": "2H", "elapsed": 72}}, "league": {"id": 3, "name": "Liga C", "country": "ES"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Real Madrid"}, "away": {"name": "Getafe"}}},
            # D: Gol Relâmpago (10min, 0x0, Chute no alvo)
            {"fixture": {"id": 4, "status": {"short": "1H", "elapsed": 10}}, "league": {"id": 4, "name": "Liga D", "country": "DE"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Bayern"}, "away": {"name": "Dortmund"}}},
            # Jogo Normal (Sem sinal)
            {"fixture": {"id": 5, "status": {"short": "1H", "elapsed": 30}}, "league": {"id": 5, "name": "Liga Nada", "country": "IT"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time X"}, "away": {"name": "Time Y"}}}
        ]
    if not API_KEY: return []
    try:
        res = requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_stats(fid, demo_type=None):
    if MODO_DEMO:
        # Stats Simulados para cada cenário
        if fid == 1: return [] # Não precisa de stats para Porteira Aberta (só gols)
        if fid == 2: # Reação Gigante: Home amassando (8 chutes, 40 AP), Away morto (1 chute)
            return [{"team": {"name": "Home"}, "statistics": [{"type": "Total Shots", "value": 8}, {"type": "Dangerous Attacks", "value": 40}, {"type": "Shots on Goal", "value": 4}]},
                    {"team": {"name": "Away"}, "statistics": [{"type": "Total Shots", "value": 1}, {"type": "Dangerous Attacks", "value": 5}, {"type": "Shots on Goal", "value": 0}]}]
        if fid == 3: # Janela Ouro: 20 chutes totais
            return [{"team": {"name": "Home"}, "statistics": [{"type": "Total Shots", "value": 15}]}, {"team": {"name": "Away"}, "statistics": [{"type": "Total Shots", "value": 5}]}]
        if fid == 4: # Relâmpago: 1 chute no alvo
            return [{"team": {"name": "Home"}, "statistics": [{"type": "Shots on Goal", "value": 1}]}, {"team": {"name": "Away"}, "statistics": [{"type": "Shots on Goal", "value": 1}]}]
        return []
    return buscar_dados("statistics", {"fixture": fid})

# --- 5. O CÉREBRO (Processador de Estratégias) ---
def processar_jogo(j, stats):
    f_id = j['fixture']['id']
    tempo = j['fixture']['status'].get('elapsed', 0)
    home = j['teams']['home']['name']
    away = j['teams']['away']['name']
    gh = j['goals']['home'] or 0
    ga = j['goals']['away'] or 0
    total_gols = gh + ga
    
    # Extrair Stats (com segurança)
    try:
        if not stats: return None
        
        # Helper para pegar valor
        def get_val(team_idx, type_name):
            try:
                data = next((item for item in stats[team_idx]['statistics'] if item["type"] == type_name), None)
                return data['value'] if data and data['value'] else 0
            except: return 0

        # Dados do Mandante (Fav assumido na lógica ou maior volume)
        sh_h = get_val(0, "Total Shots")
        sog_h = get_val(0, "Shots on Goal")
        da_h = get_val(0, "Dangerous Attacks")
        
        # Dados do Visitante
        sh_a = get_val(1, "Total Shots")
        sog_a = get_val(1, "Shots on Goal")
        da_a = get_val(1, "Dangerous Attacks")
        
        total_chutes = sh_h + sh_a
        
        # --- LÓGICA DAS 4 ESTRATÉGIAS ---

        # A) 🟣 PORTEIRA ABERTA (< 30 min, 2+ Gols)
        # Não precisa de stats profundos, apenas placar.
        if tempo <= 30 and total_gols >= 2:
            return {
                "tag": "🟣 Porteira Aberta",
                "ordem": "Adicionar em Múltipla Over Gols",
                "motivo": f"Jogo frenético ({gh}x{ga} com {tempo}').",
                "stats": f"{gh}x{ga}"
            }

        # D) ⚡ GOL RELÂMPAGO (5 a 15 min)
        if 5 <= tempo <= 15:
            # Gatilho: Dominante chutou no alvo OU Zebra atrevida (2 no alvo)
            if (sog_h >= 1 or sog_a >= 1): # Simplificado para qualquer chute no alvo cedo
                return {
                    "tag": "⚡ Gol Relâmpago",
                    "ordem": "Apostar em Over 0.5 HT (1º Tempo)",
                    "motivo": "Times ligados, goleiro já trabalhou.",
                    "stats": f"Chutes Alvo: {sog_h + sog_a}"
                }

        # B) 🟢 REAÇÃO DO GIGANTE (Até 50 min)
        # Assumindo que quem tem pressão absurda é o favorito
        if tempo <= 50:
            # Cenário: Um time perdendo, mas amassando
            fav_perdendo_h = (gh < ga) and (sh_h >= 6) and (da_h >= 30)
            fav_perdendo_a = (ga < gh) and (sh_a >= 6) and (da_a >= 30)
            
            if fav_perdendo_h:
                zebra_viva = (sh_a >= 4) # Zebra chutou 4 ou mais?
                acao = "Entrar em OVER GOLS (Jogo Aberto)" if zebra_viva else "Apostar PRÓXIMO GOL DO FAVORITO"
                return {
                    "tag": "🟢 Reação do Gigante",
                    "ordem": acao,
                    "motivo": f"{home} perde mas amassa ({sh_h} chutes). Zebra {'VIVA' if zebra_viva else 'MORTA'}.",
                    "stats": f"Chutes: {sh_h} vs {sh_a}"
                }
            
            elif fav_perdendo_a:
                zebra_viva = (sh_h >= 4)
                acao = "Entrar em OVER GOLS (Jogo Aberto)" if zebra_viva else "Apostar PRÓXIMO GOL DO FAVORITO"
                return {
                    "tag": "🟢 Reação do Gigante",
                    "ordem": acao,
                    "motivo": f"{away} perde mas amassa ({sh_a} chutes). Zebra {'VIVA' if zebra_viva else 'MORTA'}.",
                    "stats": f"Chutes: {sh_h} vs {sh_a}"
                }

        # C) 💰 JANELA DE OURO (70 a 75 min)
        if 70 <= tempo <= 75:
            # Se jogo empatado ou alguém perdendo (não ganhando fácil) e muito volume
            if total_chutes >= 18 and abs(gh - ga) <= 1: # Diferença máx de 1 gol ou empate
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
    
    # Busca Jogos
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
        
        # Filtros de Tempo para economizar API (Só chama stats nas janelas úteis)
        # Janelas: 5-15 (D), <30 (A), <50 (B), 70-75 (C)
        # Se estiver fora de todas essas janelas e não for intervalo, ignora stats
        dentro_janela = (5 <= tempo <= 50) or (70 <= tempo <= 75)
        
        sinal = None
        icone_visual = "👁️"
        
        # Lógica visual simples
        if tempo < 5: icone_visual = "⏳"
        elif tempo > 80: icone_visual = "🏁"
        
        if dentro_janela:
            # Busca Stats
            stats = buscar_stats(f_id)
            if not stats and not MODO_DEMO:
                # Blacklist se API falhar
                salvar_na_blacklist(l_id, j['league']['country'], j['league']['name'])
                continue
            
            # PROCESSA ESTRATÉGIAS
            sinal = processar_jogo(j, stats)
            
            if sinal:
                icone_visual = "✅" # Sinal encontrado!
                
                # ENVIO TELEGRAM (Se novo)
                if f_id not in st.session_state['alertas_enviados']:
                    msg = (
                        f"🚨 *NEVES ANALYTICS PRO* 🚨\n\n"
                        f"⚽ *{home}* {placar} *{away}*\n"
                        f"🏆 {j['league']['name']}\n"
                        f"⏰ {tempo}'\n\n"
                        f"🧩 *Estratégia:* {sinal['tag']}\n"
                        f"⚠️ *ORDEM:* {sinal['ordem']}\n\n"
                        f"💡 *Motivo:* {sinal['motivo']}\n"
                        f"📊 *Dados:* {sinal['stats']}"
                    )
                    enviar_telegram_real(tg_token, tg_chat_ids, msg)
                    st.session_state['alertas_enviados'].add(f_id)
                    st.toast(f"Sinal Enviado: {sinal['tag']}")

        radar.append({
            "Liga": j['league']['name'],
            "Jogo": f"{home} {placar} {away}",
            "Tempo": f"{tempo}'",
            "Status": f"{icone_visual} {sinal['tag'] if sinal else ''}"
        })

    # Próximos
    prox = buscar_proximos(API_KEY)
    prox_f = [{"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"} 
              for p in prox if str(p['league']['id']) not in ids_bloqueados and p['fixture']['status']['short'] == 'NS']

    # EXIBIÇÃO
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.markdown('<div class="status-box status-active">🟢 SISTEMA DE MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        t1, t2, t3 = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(prox_f)})", f"🚫 Blacklist ({len(df_black)})"])
        
        with t1:
            if radar: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
            else: st.info("Nenhum jogo encaixa nas estratégias agora.")
            
        with t2:
            st.dataframe(pd.DataFrame(prox_f).sort_values("Hora"), use_container_width=True, hide_index=True) if prox_f else st.caption("Vazio.")
            
        with t3:
            st.table(df_black) if not df_black.empty else st.caption("Limpo.")

        # Timer
        relogio = st.empty()
        for i in range(INTERVALO, 0, -1):
            relogio.markdown(f'<div class="timer-text">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
    
    st.rerun()

else:
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera.")
