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
    .timer-text { font-size: 20px; color: #FFD700; text-align: center; font-weight: bold; margin-top: 20px; padding: 10px; border: 1px solid #333; border-radius: 10px; background-color: #1e1e1e;}
    .strategy-card { background-color: #1e1e1e; padding: 15px; border-radius: 8px; margin-bottom: 10px; border-left: 4px solid #00FF00; }
    .strategy-title { color: #00FF00; font-weight: bold; font-size: 16px; margin-bottom: 5px; }
    .strategy-desc { font-size: 13px; color: #cccccc; line-height: 1.6; }
</style>
""", unsafe_allow_html=True)

# --- 2. ARQUIVOS (COM PROTEÇÃO) ---
BLACK_FILE = 'neves_blacklist.txt'
STRIKES_FILE = 'neves_strikes_vip.txt'
HIST_FILE = 'neves_historico_sinais.csv'
RELATORIO_FILE = 'neves_status_relatorio.txt'

# --- 3. LISTA VIP ---
LIGAS_VIP = [39, 78, 135, 140, 61, 2, 3, 9, 45, 48, 71, 72, 13, 11, 474, 475, 476, 477, 478, 479, 606, 610, 628, 55, 143]

# --- 4. FUNÇÕES DE DADOS BLINDADAS ---
def safe_read_csv(filepath, columns):
    if not os.path.exists(filepath): return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(filepath)
        if not set(columns).issubset(df.columns): return pd.DataFrame(columns=columns)
        return df.fillna("").astype(str)
    except: return pd.DataFrame(columns=columns)

def carregar_blacklist(): return safe_read_csv(BLACK_FILE, ['id', 'País', 'Liga'])
def carregar_strikes(): return safe_read_csv(STRIKES_FILE, ['id', 'País', 'Liga', 'Data_Erro', 'Strikes'])
def carregar_historico(): return safe_read_csv(HIST_FILE, ['Data', 'Hora', 'Liga', 'Jogo', 'Placar', 'Estrategia', 'Resultado'])

def salvar_na_blacklist(id_liga, pais, nome_liga):
    df = carregar_blacklist()
    if str(id_liga) not in df['id'].values:
        novo = pd.DataFrame([{'id': str(id_liga), 'País': str(pais), 'Liga': str(nome_liga)}])
        pd.concat([df, novo], ignore_index=True).to_csv(BLACK_FILE, index=False)

def salvar_sinal_historico(item):
    df = pd.DataFrame([item])
    df.to_csv(HIST_FILE, mode='a', header=not os.path.exists(HIST_FILE), index=False)

# --- 5. TELEGRAM ---
def enviar_telegram(token, chat_ids, msg):
    if token and chat_ids:
        ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
        for cid in ids:
            try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "Markdown"}, timeout=3)
            except: pass

def verificar_relatorio_enviado():
    if not os.path.exists(RELATORIO_FILE): return False
    try:
        with open(RELATORIO_FILE, 'r') as f: return f.read().strip() == datetime.now().strftime('%Y-%m-%d')
    except: return False

def marcar_relatorio_enviado():
    with open(RELATORIO_FILE, 'w') as f: f.write(datetime.now().strftime('%Y-%m-%d'))

def enviar_relatorio(token, chat_ids):
    hist = carregar_historico()
    hoje = datetime.now().strftime('%Y-%m-%d')
    # Filtra histórico na memória
    if not hist.empty:
        hist = hist[hist['Data'] == hoje]
    
    msg = f"📊 *RELATÓRIO DO DIA ({hoje})*\n\n"
    if hist.empty: msg += "💤 Nenhum sinal hoje."
    else:
        msg += f"🚀 Total: {len(hist)}\n\n"
        for _, row in hist.iterrows():
            msg += f"⏰ {row['Hora']} | {row['Jogo']}\n🎯 {row['Estrategia']}\n\n"
    
    enviar_telegram(token, chat_ids, msg)
    marcar_relatorio_enviado()
    st.toast("Relatório Enviado!")

# --- 6. STATES ---
if 'ligas_imunes' not in st.session_state: st.session_state['ligas_imunes'] = {}
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}

# --- 7. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves PRO")
    
    with st.expander("✅ Status Estratégias", expanded=True):
        st.markdown("🟣 **A** - Porteira Aberta")
        st.markdown("🟢 **B** - Reação / Blitz")
        st.markdown("💰 **C** - Janela de Ouro")
        st.markdown("⚡ **D** - Gol Relâmpago")

    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API:", type="password")
        TG_TOKEN = st.text_input("Token Telegram:", type="password")
        TG_CHAT = st.text_input("Chat IDs:")
        
        st.markdown("---")
        if st.button("🔔 Testar Telegram"):
            enviar_telegram(TG_TOKEN, TG_CHAT, "✅ Teste: Neves PRO Ativo!")
            st.toast("Teste enviado!")
            
        INTERVALO = st.slider("Ciclo (s):", 30, 300, 60)
        st.session_state['demo'] = st.checkbox("Modo Simulação", value=False)
        
        st.markdown("---")
        if st.button("🗑️ Limpar Tudo"):
            for f in [BLACK_FILE, STRIKES_FILE, HIST_FILE, RELATORIO_FILE]:
                if os.path.exists(f): os.remove(f)
            st.session_state['alertas_enviados'] = set()
            st.rerun()

    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 8. LÓGICA PRINCIPAL ---
def agora_brasil(): return datetime.utcnow() - timedelta(hours=3)

# Funções API
def get_api(endpoint, params):
    if st.session_state['demo']: return [] 
    if not API_KEY: return []
    try: return requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json().get('response', [])
    except: return []

# Momentum e Processamento
def atualizar_momentum(fid, sog_h, sog_a):
    if fid not in st.session_state['memoria_pressao']:
        st.session_state['memoria_pressao'][fid] = {'sog_h': sog_h, 'sog_a': sog_a, 'h_times': [], 'a_times': []}
        return 0, 0
    mem = st.session_state['memoria_pressao'][fid]
    now = datetime.now()
    for _ in range(max(0, sog_h - mem['sog_h'])): mem['h_times'].append(now)
    for _ in range(max(0, sog_a - mem['sog_a'])): mem['a_times'].append(now)
    mem['h_times'] = [t for t in mem['h_times'] if now - t <= timedelta(minutes=7)]
    mem['a_times'] = [t for t in mem['a_times'] if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a
    st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_times']), len(mem['a_times'])

main_placeholder = st.empty()

if ROBO_LIGADO:
    df_black = carregar_blacklist()
    ids_black = df_black['id'].values if not df_black.empty else []
    
    # Busca Jogos
    jogos = get_api("fixtures", {"live": "all", "timezone": "America/Sao_Paulo"}) if not st.session_state['demo'] else []
    
    radar = []
    
    for j in jogos:
        lid = str(j['league']['id'])
        if lid in ids_black: continue
        
        fid = j['fixture']['id']
        tempo = j['fixture']['status']['elapsed']
        home = j['teams']['home']['name']
        away = j['teams']['away']['name']
        gh = j['goals']['home'] or 0
        ga = j['goals']['away'] or 0
        
        # Filtros básicos
        if tempo > 80 or tempo < 5: continue
        
        # Busca Stats
        stats = get_api("statistics", {"fixture": fid})
        
        # Validação de Dados
        valid_stats = False
        sh_h, sog_h, sh_a, sog_a = 0, 0, 0, 0
        if stats:
            for t in stats:
                for s in t.get('statistics', []):
                    if s['type'] == "Total Shots" and s['value'] is not None:
                        valid_stats = True
                        if t['team']['name'] == home: sh_h = s['value']
                        else: sh_a = s['value']
                    if s['type'] == "Shots on Goal" and s['value'] is not None:
                        valid_stats = True
                        if t['team']['name'] == home: sog_h = s['value']
                        else: sog_a = s['value']

        # Lógica de Banimento (Sem dados > 45min)
        if not valid_stats and not st.session_state['demo']:
            if tempo >= 45 and int(lid) not in LIGAS_VIP:
                salvar_na_blacklist(lid, j['league']['country'], j['league']['name'])
            continue # Pula se não tem dados
        
        if valid_stats: st.session_state['ligas_imunes'][lid] = True
        
        # Estratégias
        rec_h, rec_a = atualizar_momentum(fid, sog_h, sog_a)
        sinal = None
        
        if tempo <= 30 and (gh+ga) >= 2:
            sinal = {"tag": "🟣 Porteira Aberta", "ordem": "🔥 Entrada Seca: Over Gols", "stats": f"{gh}x{ga}"}
        elif 5 <= tempo <= 15 and (sog_h+sog_a) >= 1:
            sinal = {"tag": "⚡ Gol Relâmpago", "ordem": "Over 0.5 HT", "stats": f"Chutes: {sog_h+sog_a}"}
        elif 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(gh-ga) <= 1:
            sinal = {"tag": "💰 Janela de Ouro", "ordem": "Over Gols Asiático", "stats": f"Total: {sh_h+sh_a}"}
        elif tempo <= 60:
            if gh <= ga and (rec_h >= 2 or sh_h >= 8):
                sinal = {"tag": "🟢 Blitz Casa", "ordem": "Gol Mandante", "stats": f"Pressão: {rec_h}"}
            elif ga <= gh and (rec_a >= 2 or sh_a >= 8):
                sinal = {"tag": "🟢 Blitz Visitante", "ordem": "Gol Visitante", "stats": f"Pressão: {rec_a}"}

        # Envio e Registro
        visual_sinal = ""
        if sinal:
            visual_sinal = sinal['tag']
            if fid not in st.session_state['alertas_enviados']:
                msg = f"🚨 *{sinal['tag']}*\n⚽ {home} {gh}x{ga} {away}\n🏆 {j['league']['name']}\n⚠️ {sinal['ordem']}\n📈 {sinal['stats']}"
                enviar_telegram(TG_TOKEN, TG_CHAT, msg)
                st.session_state['alertas_enviados'].add(fid)
                
                # Salva Histórico
                item = {
                    "Data": datetime.now().strftime('%Y-%m-%d'),
                    "Hora": datetime.now().strftime('%H:%M'),
                    "Liga": j['league']['name'],
                    "Jogo": f"{home} x {away}",
                    "Placar": f"{gh}x{ga}",
                    "Estrategia": sinal['tag'],
                    "Resultado": "Pendente"
                }
                salvar_sinal_historico(item)
                st.toast(f"Sinal Enviado: {sinal['tag']}")

        radar.append({
            "Liga": j['league']['name'],
            "Jogo": f"{home} {gh}x{ga} {away}",
            "Tempo": f"{tempo}'",
            "Sinal": visual_sinal,
            "Momento": f"⚡ {rec_h}x{rec_a}"
        })

    # Busca Agenda
    prox = get_api("fixtures", {"date": agora_brasil().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"})
    agenda = []
    limite = (agora_brasil() - timedelta(minutes=15)).strftime('%H:%M')
    for p in prox:
        if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] == 'NS' and p['fixture']['date'][11:16] >= limite:
            agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})

    # Automação Relatório
    if not radar and not agenda:
        if not verificar_relatorio_enviado():
            enviar_relatorio(TG_TOKEN, TG_CHAT)

    # --- RENDERIZAÇÃO ---
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.markdown('<div class="status-box status-active">🟢 SISTEMA DE MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        hist_df = carregar_historico()
        # Filtra hoje visualmente
        if not hist_df.empty:
            hoje = datetime.now().strftime('%Y-%m-%d')
            hist_df = hist_df[hist_df['Data'] == hoje]

        t1, t2, t3, t4 = st.tabs([
            f"📡 Radar ({len(radar)})", 
            f"📅 Agenda ({len(agenda)})", 
            f"📜 Histórico ({len(hist_df)})",
            f"🚫 Blacklist ({len(df_black)})"
        ])
        
        with t1: st.dataframe(pd.DataFrame(radar).astype(str), use_container_width=True, hide_index=True) if radar else st.info("Buscando jogos...")
        with t2: st.dataframe(pd.DataFrame(agenda).astype(str).sort_values("Hora"), use_container_width=True, hide_index=True) if agenda else st.caption("Sem jogos.")
        with t3: st.dataframe(hist_df.astype(str), use_container_width=True, hide_index=True) if not hist_df.empty else st.caption("Nenhum sinal hoje.")
        with t4: st.table(df_black.sort_values(['País', 'Liga'])) if not df_black.empty else st.caption("Limpo.")

        # MANUAL DE VOLTA
        with st.expander("📘 Manual de Inteligência (Detalhes Técnicos)", expanded=False):
            c1, c2 = st.columns(2)
            c1.markdown("### 🟣 Porteira Aberta\nJogo frenético com 2 gols ou mais antes dos 30 minutos.")
            c1.markdown("### 🟢 Reação / Blitz\nFavorito perdendo/empatando com alta pressão recente.")
            c2.markdown("### 💰 Janela de Ouro\n70-75min, jogo empatado ou 1 gol diferença, muitos chutes.")
            c2.markdown("### ⚡ Gol Relâmpago\n5-15min, início elétrico com chutes no alvo.")

        # TIMER NO FUNDO DA PÁGINA
        relogio = st.empty()
        for i in range(INTERVALO, 0, -1):
            relogio.markdown(f'<div class="timer-text">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
    
    st.rerun()

else:
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure e ligue na lateral.")
