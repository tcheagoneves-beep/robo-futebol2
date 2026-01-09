import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Neves Analytics", layout="centered", page_icon="❄️")

st.markdown("""
<style>
    .stApp {background-color: #121212; color: white;}
    .status-online {color: #00FF00; font-weight: bold; animation: pulse 2s infinite; padding: 10px; border: 1px solid #00FF00; text-align: center; margin-bottom: 20px; border-radius: 15px;}
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0.4); } 70% { box-shadow: 0 0 0 10px rgba(0, 255, 0, 0); } 100% { box-shadow: 0 0 0 0 rgba(0, 255, 0, 0); } }
    .timer-text { font-size: 14px; color: #FFD700; text-align: center; font-weight: bold; margin-top: 10px; border-top: 1px solid #333; padding-top: 10px;}
</style>
""", unsafe_allow_html=True)

# --- 2. GESTÃO DE ARQUIVOS ---
DB_FILE = 'neves_dados.txt'
BLACK_FILE = 'neves_blacklist.txt'

def carregar_db():
    if not os.path.exists(DB_FILE):
        return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])
    try: return pd.read_csv(DB_FILE)
    except: return pd.DataFrame(columns=['id', 'data', 'hora', 'jogo', 'sinal', 'gols_inicial', 'status'])

def carregar_blacklist():
    if not os.path.exists(BLACK_FILE):
        return pd.DataFrame(columns=['id', 'País', 'Liga'])
    try: return pd.read_csv(BLACK_FILE)
    except: return pd.DataFrame(columns=['id', 'País', 'Liga'])

def salvar_na_blacklist(id_liga, pais, nome_liga):
    df = carregar_blacklist()
    if str(id_liga) not in df['id'].astype(str).values:
        novo = pd.DataFrame([{'id': str(id_liga), 'País': pais, 'Liga': nome_liga}])
        pd.concat([df, novo], ignore_index=True).to_csv(BLACK_FILE, index=False)

def enviar_teste_telegram(token, chat_ids):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                              data={"chat_id": cid.strip(), "text": "✅ Neves Analytics: Teste OK!"}, timeout=5)
            except: pass

def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    
    with st.expander("ℹ️ Legenda de Status", expanded=True):
        st.info(
            """
            **Guia de Ícones:**
            ⏳ **Início (0-10')** _Aguardando._
            👁️ **Monitorando** _Jogo Ativo._
            🔥 **PRESSÃO** _Muitos chutes!_
            💤 **Stand By** _Intervalo._
            🚫 **Bloqueado** _Sem dados._
            """
        )
    
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        
        # Critério de Pressão (Novo)
        CRITERIO_CHUTES = st.number_input("Mínimo de Chutes para 🔥:", value=8, min_value=1)

        if st.button("🔔 Testar Telegram"):
            enviar_teste_telegram(tg_token, tg_chat_ids)
            st.toast("Enviado!")

        INTERVALO = st.slider("Ciclo (seg):", 30, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        
        if st.button("🗑️ Resetar Tudo"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
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
        # Simulação de jogo com pressão
        return [
            {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 25}}, "league": {"id": 1, "name": "Liga Quente", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}},
            {"fixture": {"id": 2, "status": {"short": "1H", "elapsed": 30}}, "league": {"id": 2, "name": "Liga Fria", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time C"}, "away": {"name": "Time D"}}},
        ]
    if not API_KEY: return []
    try:
        res = requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_stats_simulado(fid):
    # Simula stats: Jogo 1 tem 12 chutes (pressão), Jogo 2 tem 2 chutes
    if fid == 1: return [{"statistics": [{"type": "Total Shots", "value": 8}]}, {"statistics": [{"type": "Total Shots", "value": 4}]}] 
    return [{"statistics": [{"type": "Total Shots", "value": 1}]}, {"statistics": [{"type": "Total Shots", "value": 1}]}]

# --- 5. EXECUÇÃO ---
main_placeholder = st.empty()

if ROBO_LIGADO:
    df_black = carregar_blacklist()
    ids_bloqueados = df_black['id'].astype(str).values
    hist_df = carregar_db()

    jogos_live = buscar_dados("fixtures", {"live": "all"})
    radar = []
    
    for j in jogos_live:
        l_id = str(j['league']['id'])
        
        # 1. Filtro Blacklist (Ignora se estiver bloqueada)
        if l_id in ids_bloqueados:
            continue
            
        f_id = j['fixture']['id']
        tempo = j['fixture']['status'].get('elapsed', 0)
        status_short = j['fixture']['status'].get('short', '')
        sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
        
        icone_status = "👁️"
        pressao_txt = "-" # Coluna nova para indicar pressão
        
        # 2. Definição de Janelas
        if tempo < 10: 
            icone_status = "⏳"
        elif (40 <= tempo <= 55) or (status_short in ['HT', 'BT']):
            icone_status = "💤"
        elif tempo > 85: 
            icone_status = "🏁"
        else:
            # 3. MOMENTO DA VERDADE: Se está monitorando (10-85 min), BUSCA STATS!
            stats = []
            if MODO_DEMO:
                stats = buscar_stats_simulado(f_id)
            else:
                stats = buscar_dados("statistics", {"fixture": f_id})
            
            if not stats:
                # Se tentou buscar e veio vazio: BANIR PARA SEMPRE
                salvar_na_blacklist(l_id, j['league']['country'], j['league']['name'])
                continue 
            else:
                # Análise de Pressão
                try:
                    chutes_home = stats[0]['statistics'][2]['value'] or 0 # Pega Total Shots (ajustar index se precisar)
                    chutes_away = stats[1]['statistics'][2]['value'] or 0
                    if (type(chutes_home) == int) and (type(chutes_away) == int):
                        total_chutes = chutes_home + chutes_away
                        if total_chutes >= CRITERIO_CHUTES:
                            icone_status = "🔥"
                            pressao_txt = f"{total_chutes} Chutes"
                except:
                    # Em alguns casos a API muda a ordem, pegamos genericamente
                    pass

        radar.append({
            "Liga": j['league']['name'], 
            "Jogo": f"{j['teams']['home']['name']} {sc}x{sf} {j['teams']['away']['name']}", 
            "Tempo": f"{tempo}'", 
            "Status": icone_status,
            "Info": pressao_txt
        })

    prox_raw = buscar_proximos(API_KEY)
    prox_filtrado = []
    for p in prox_raw:
        if str(p['league']['id']) not in ids_bloqueados and p['fixture']['status']['short'] == 'NS':
            prox_filtrado.append({
                "Hora": p['fixture']['date'][11:16], 
                "Liga": p['league']['name'], 
                "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"
            })

    with main_placeholder.container():
        st.title("❄️ Neves Analytics")
        st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        t1, t2, t3, t4 = st.tabs([f"📡 Ao Vivo ({len(radar)})", f"📅 Próximos ({len(prox_filtrado)})", "📊 Histórico", f"🚫 Blacklist ({len(df_black)})"])
        
        with t1:
            if radar: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        with t2:
            if prox_filtrado: st.dataframe(pd.DataFrame(prox_filtrado).sort_values("Hora"), use_container_width=True, hide_index=True)
            else: st.caption("Agenda vazia.")
        with t3:
            if not hist_df.empty: st.dataframe(hist_df.sort_values(by=['data', 'hora'], ascending=False), use_container_width=True, hide_index=True)
            else: st.caption("Vazio.")
        with t4:
            if not df_black.empty: st.table(df_black[['País', 'Liga']])
            else: st.caption("Limpo.")

        timer_box = st.empty()
        for i in range(INTERVALO, 0, -1):
            timer_box.markdown(f'<div class="timer-text">⏳ Atualizando em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
            
    st.rerun()

else:
    with main_placeholder.container():
        st.title("❄️ Neves Analytics")
        st.info("💡 Robô em espera.")
