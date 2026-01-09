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

# --- 2. GESTÃO DE ARQUIVOS E MEMÓRIA ---
DB_FILE = 'neves_dados.txt'
BLACK_FILE = 'neves_blacklist.txt'

# Memória para não repetir alertas do mesmo jogo
if 'alertas_enviados' not in st.session_state:
    st.session_state['alertas_enviados'] = set()

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

# Função de Envio Real
def enviar_telegram_real(token, chat_ids, mensagem):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try:
                url = f"https://api.telegram.org/bot{token}/sendMessage"
                payload = {"chat_id": cid.strip(), "text": mensagem, "parse_mode": "Markdown"}
                requests.post(url, data=payload, timeout=5)
            except: pass

def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    
    with st.expander("ℹ️ Legenda", expanded=True):
        st.markdown(
            """
            **Fases:**
            ⏳ **0-5'**: Aquecimento
            👁️ **5-80'**: Ativo
            💤 **40-55'**: Pausa
            🏁 **>80'**: Fim

            **Sinais:**
            🔥 **PRESSÃO**: Ataque!
            🚫 **Bloqueado**: Sem dados
            """
        )
    
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        
        st.markdown("---")
        META_CHUTES = st.number_input("Gatilho de Chutes (Soma):", value=10, min_value=1)

        if st.button("🔔 Testar Telegram"):
            # Exemplo exato de como vai chegar
            msg_teste = (
                "🔥 *ALERTA DE PRESSÃO* 🔥\n\n"
                "⚽ *Time A* x *Time B*\n"
                "🏆 Liga Teste\n"
                "⏰ 32'\n\n"
                "⚠️ *ORDEM:*\n"
                "✅ *ENTRAR EM MAIS 1 GOL*\n\n"
                "📊 *Estatísticas:*\n"
                "Chutes Totais: 15 (Meta: 10)"
            )
            enviar_telegram_real(tg_token, tg_chat_ids, msg_teste)
            st.toast("Teste enviado! Verifique o Telegram.")

        INTERVALO = st.slider("Ciclo (seg):", 30, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        
        if st.button("🗑️ Resetar Tudo"):
            if os.path.exists(DB_FILE): os.remove(DB_FILE)
            if os.path.exists(BLACK_FILE): os.remove(BLACK_FILE)
            st.session_state['alertas_enviados'] = set() 
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
        # Simula jogo com pressão para testar o envio
        return [
            {"fixture": {"id": 101, "status": {"short": "1H", "elapsed": 25}}, "league": {"id": 1, "name": "Liga Teste", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}},
            {"fixture": {"id": 102, "status": {"short": "1H", "elapsed": 10}}, "league": {"id": 2, "name": "Liga Fria", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time C"}, "away": {"name": "Time D"}}}
        ]
    if not API_KEY: return []
    try:
        res = requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_stats_reais(fid):
    if MODO_DEMO:
        if fid == 101: return [{"statistics": [{"type": "Total Shots", "value": 8}]}, {"statistics": [{"type": "Total Shots", "value": 5}]}] # 13 chutes (Dispara Telegram)
        return [{"statistics": [{"type": "Total Shots", "value": 1}]}, {"statistics": [{"type": "Total Shots", "value": 1}]}]
    
    return buscar_dados("statistics", {"fixture": fid})

# --- 5. EXECUÇÃO ---
main_placeholder = st.empty()

if ROBO_LIGADO:
    df_black = carregar_blacklist()
    ids_bloqueados = df_black['id'].astype(str).values
    hist_df = carregar_db()

    # Processar AO VIVO
    jogos_live = buscar_dados("fixtures", {"live": "all"})
    radar = []
    
    for j in jogos_live:
        l_id = str(j['league']['id'])
        
        # Filtro Blacklist
        if l_id in ids_bloqueados:
            continue
            
        f_id = j['fixture']['id']
        tempo = j['fixture']['status'].get('elapsed', 0)
        status_short = j['fixture']['status'].get('short', '')
        
        # Placar
        home_name = j['teams']['home']['name']
        away_name = j['teams']['away']['name']
        sc, sf = j['goals']['home'] or 0, j['goals']['away'] or 0
        
        icone = "👁️" 
        info_extra = ""
        
        # DEFINIÇÃO DE JANELAS
        if tempo < 5:
            icone = "⏳"
        elif (40 <= tempo <= 55) or (status_short in ['HT', 'BT']):
            icone = "💤"
        elif tempo > 80:
            icone = "🏁"
        else:
            # JANELA ATIVA: Busca Stats
            stats = buscar_stats_reais(f_id)
            
            if not stats:
                salvar_na_blacklist(l_id, j['league']['country'], j['league']['name'])
                continue 
            else:
                try:
                    s1 = next((item for item in stats[0]['statistics'] if item["type"] == "Total Shots"), None)
                    s2 = next((item for item in stats[1]['statistics'] if item["type"] == "Total Shots"), None)
                    
                    v1 = s1['value'] if s1 and s1['value'] else 0
                    v2 = s2['value'] if s2 and s2['value'] else 0
                    total_chutes = v1 + v2
                    
                    # --- GATILHO DE PRESSÃO & TELEGRAM ---
                    if total_chutes >= META_CHUTES:
                        icone = "🔥"
                        info_extra = f" ({total_chutes})"
                        
                        # Verifica se já enviou alerta deste jogo
                        if f_id not in st.session_state['alertas_enviados']:
                            
                            # MENSAGEM HÍBRIDA (VISUAL + ORDEM)
                            msg_telegram = (
                                f"🔥 *ALERTA DE PRESSÃO* 🔥\n\n"
                                f"⚽ *{home_name}* x *{away_name}*\n"
                                f"🏆 {j['league']['name']}\n"
                                f"⏰ {tempo}'\n\n"
                                f"⚠️ *ORDEM:*\n"
                                f"✅ *ENTRAR EM MAIS 1 GOL*\n\n"
                                f"📊 *Estatísticas:*\n"
                                f"Chutes Totais: {total_chutes} (Meta: {META_CHUTES})"
                            )
                            
                            enviar_telegram_real(tg_token, tg_chat_ids, msg_telegram)
                            st.session_state['alertas_enviados'].add(f_id) # Marca como enviado
                            st.toast(f"Alerta enviado: {home_name} x {away_name}")
                    else:
                        info_extra = f" ({total_chutes})"
                except:
                    pass

        radar.append({
            "Liga": j['league']['name'], 
            "Jogo": f"{home_name} {sc}x{sf} {away_name}", 
            "Tempo": f"{tempo}'", 
            "Status": icone + info_extra
        })

    # Processar PRÓXIMOS
    prox_raw = buscar_proximos(API_KEY)
    prox_filtrado = []
    for p in prox_raw:
        if str(p['league']['id']) not in ids_bloqueados and p['fixture']['status']['short'] == 'NS':
            prox_filtrado.append({
                "Hora": p['fixture']['date'][11:16], 
                "Liga": p['league']['name'], 
                "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"
            })

    # EXIBIÇÃO
    with main_placeholder.container():
        st.title("❄️ Neves Analytics")
        st.markdown('<div class="status-online">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        t1, t2, t3, t4 = st.tabs([f"📡 Ao Vivo ({len(radar)})", f"📅 Próximos ({len(prox_filtrado)})", "📊 Histórico", f"🚫 Blacklist ({len(df_black)})"])
        
        with t1:
            if radar: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
            else: st.info("Nenhum jogo na janela de operação.")
        with t2:
            if prox_filtrado: st.dataframe(pd.DataFrame(prox_filtrado).sort_values("Hora"), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos permitidos.")
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
        st.info("💡 Robô em espera. Configure e ligue na lateral.")
