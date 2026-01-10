import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 0. CONFIGURAÇÃO E LIMPEZA TOTAL ---
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")

# CSS Clássico (Timer lá embaixo, Cards bonitos)
st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .metric-box {
        background-color: #1e1e1e; 
        border: 1px solid #333; 
        border-radius: 8px; 
        padding: 15px; 
        text-align: center;
    }
    .metric-title {font-size: 14px; color: #aaaaaa;}
    .metric-value {font-size: 24px; font-weight: bold; color: #00FF00;}
    
    .status-active {
        background-color: #1F4025; color: #00FF00; 
        border: 1px solid #00FF00; padding: 10px; 
        text-align: center; border-radius: 5px; font-weight: bold;
    }
    
    /* Timer lá no rodapé, bem visível */
    .footer-timer {
        position: fixed; left: 0; bottom: 0; width: 100%;
        background-color: #000000; color: #FFD700;
        text-align: center; padding: 10px; font-size: 18px;
        font-weight: bold; border-top: 2px solid #FFD700;
        z-index: 9999;
    }
</style>
""", unsafe_allow_html=True)

# --- 1. ARQUIVOS ---
FILES = {
    'black': 'neves_blacklist.txt',
    'vip': 'neves_strikes_vip.txt',
    'hist': 'neves_historico_sinais.csv',
    'report': 'neves_status_relatorio.txt'
}

# --- 2. LISTA VIP ---
LIGAS_VIP = [39, 78, 135, 140, 61, 2, 3, 9, 45, 48, 71, 72, 13, 11, 474, 475, 476, 477, 478, 479, 606, 610, 628, 55, 143]

# --- 3. LIMPEZA DE ARQUIVOS CORROMPIDOS (A CURA) ---
def nuke_corrupted_files():
    """Remove arquivos se estiverem com formato errado para evitar AttributeError"""
    if 'limpeza_feita' not in st.session_state:
        for name, path in FILES.items():
            if os.path.exists(path):
                try:
                    # Tenta ler. Se falhar ou tiver colunas erradas, deleta.
                    if name == 'hist': pd.read_csv(path)
                    elif name == 'black': pd.read_csv(path)
                except:
                    os.remove(path)
        st.session_state['limpeza_feita'] = True

nuke_corrupted_files()

# --- 4. FUNÇÕES DE DADOS (SEGURAS) ---
def load_csv_safe(path, cols):
    if not os.path.exists(path): return pd.DataFrame(columns=cols)
    try:
        df = pd.read_csv(path)
        # Garante que todas as colunas existem
        for c in cols:
            if c not in df.columns: df[c] = ""
        return df[cols].fillna("").astype(str)
    except:
        return pd.DataFrame(columns=cols)

def carregar_tudo():
    st.session_state['df_black'] = load_csv_safe(FILES['black'], ['id', 'País', 'Liga'])
    st.session_state['df_vip'] = load_csv_safe(FILES['vip'], ['id', 'País', 'Liga', 'Data_Erro', 'Strikes'])
    st.session_state['df_hist'] = load_csv_safe(FILES['hist'], ['Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado'])

def salvar_blacklist(id_liga, pais, nome_liga):
    novo = pd.DataFrame([{'id': str(id_liga), 'País': str(pais), 'Liga': str(nome_liga)}])
    df = st.session_state['df_black']
    if str(id_liga) not in df['id'].values:
        pd.concat([df, novo], ignore_index=True).to_csv(FILES['black'], index=False)
        st.session_state['df_black'] = load_csv_safe(FILES['black'], ['id', 'País', 'Liga'])

def salvar_historico(item):
    df_novo = pd.DataFrame([item])
    df_novo.to_csv(FILES['hist'], mode='a', header=not os.path.exists(FILES['hist']), index=False)
    carregar_tudo() # Recarrega para atualizar tela

# --- 5. LÓGICA DE NEGÓCIO ---
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'df_hist' not in st.session_state: carregar_tudo()

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "Markdown"}, timeout=3)
        except: pass

def verificar_resultados(jogos_live, token, chats):
    """Verifica Green/Red e envia Telegram"""
    df = st.session_state['df_hist']
    if df.empty: return

    atualizou = False
    for idx, row in df.iterrows():
        if row['Resultado'] == 'Pendente':
            # Acha o jogo
            jogo = next((j for j in jogos_live if j['teams']['home']['name'] in row['Jogo']), None)
            if jogo:
                gh = jogo['goals']['home'] or 0
                ga = jogo['goals']['away'] or 0
                
                # Tenta extrair placar do sinal
                try:
                    ph, pa = map(int, row['Placar_Sinal'].split('x'))
                except: continue

                # GREEN: Saiu gol
                if (gh + ga) > (ph + pa):
                    df.at[idx, 'Resultado'] = '✅ GREEN'
                    msg = f"✅ *GREEN CONFIRMADO!* \n\n⚽ {row['Jogo']}\n📊 Placar: {gh}x{ga}\n🎯 {row['Estrategia']}"
                    enviar_telegram(token, chats, msg)
                    atualizou = True
                
                # RED: Acabou sem gol
                elif jogo['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
                    df.at[idx, 'Resultado'] = '❌ RED'
                    msg = f"❌ *RED* \n\n⚽ {row['Jogo']}\n📉 {row['Estrategia']} não bateu."
                    enviar_telegram(token, chats, msg)
                    atualizou = True
    
    if atualizou:
        df.to_csv(FILES['hist'], index=False)
        st.session_state['df_hist'] = df

def enviar_relatorio_final(token, chat_ids):
    df = st.session_state['df_hist']
    hoje = datetime.now().strftime('%Y-%m-%d')
    if df.empty: return
    
    # Filtra hoje
    df_hoje = df[df['Data'] == hoje]
    if df_hoje.empty: return

    total = len(df_hoje)
    greens = len(df_hoje[df_hoje['Resultado'] == '✅ GREEN'])
    reds = len(df_hoje[df_hoje['Resultado'] == '❌ RED'])
    pendentes = total - (greens + reds)
    assertividade = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0

    msg = f"📊 *FECHAMENTO DO DIA* ({hoje})\n\n"
    msg += f"🚀 Sinais: {total}\n✅ Greens: {greens}\n❌ Reds: {reds}\n⏳ Pendentes: {pendentes}\n"
    msg += f"📈 Assertividade: {assertividade:.1f}%"
    
    enviar_telegram(token, chat_ids, msg)
    with open(FILES['report'], 'w') as f: f.write(hoje)

# --- 6. ESTRATÉGIAS ---
def momentum(fid, sog_h, sog_a):
    mem = st.session_state['memoria_pressao'].get(fid, {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
    now = datetime.now()
    # Adiciona timestamps se aumentou chute
    if sog_h > mem['sog_h']: mem['h_t'].extend([now] * (sog_h - mem['sog_h']))
    if sog_a > mem['sog_a']: mem['a_t'].extend([now] * (sog_a - mem['sog_a']))
    # Limpa antigos (>7 min)
    mem['h_t'] = [t for t in mem['h_t'] if now - t <= timedelta(minutes=7)]
    mem['a_t'] = [t for t in mem['a_t'] if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a
    st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_t']), len(mem['a_t'])

def analisar_jogo(j, stats, tempo, placar):
    if not stats: return None
    
    # Extração Segura de Chutes
    sh_h, sog_h, sh_a, sog_a = 0, 0, 0, 0
    dados_ok = False
    for idx, t in enumerate(stats):
        for s in t.get('statistics', []):
            if s['type'] == 'Total Shots' and s['value'] is not None:
                dados_ok = True
                if idx==0: sh_h = s['value']
                else: sh_a = s['value']
            if s['type'] == 'Shots on Goal' and s['value'] is not None:
                dados_ok = True
                if idx==0: sog_h = s['value']
                else: sog_a = s['value']
    
    if not dados_ok: return None

    fid = j['fixture']['id']
    gh = j['goals']['home'] or 0
    ga = j['goals']['away'] or 0
    rec_h, rec_a = momentum(fid, sog_h, sog_a)
    
    # Regras
    if tempo <= 30 and (gh+ga) >= 2:
        return {"tag": "🟣 Porteira Aberta", "ordem": "🔥 ENTRADA SECA: Over Gols", "stats": f"{gh}x{ga}"}
    if 5 <= tempo <= 15 and (sog_h+sog_a) >= 1:
        return {"tag": "⚡ Gol Relâmpago", "ordem": "Apostar Over 0.5 HT", "stats": f"Chutes Alvo: {sog_h+sog_a}"}
    if 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(gh-ga) <= 1:
        return {"tag": "💰 Janela de Ouro", "ordem": "Over Gols Asiático", "stats": f"Total: {sh_h+sh_a}"}
    if tempo <= 60:
        if gh <= ga and (rec_h >= 2 or sh_h >= 8):
            return {"tag": "🟢 Blitz Casa", "ordem": "Gol Mandante/Over", "stats": f"Pressão: {rec_h}"}
        if ga <= gh and (rec_a >= 2 or sh_a >= 8):
            return {"tag": "🟢 Blitz Visitante", "ordem": "Gol Visitante/Over", "stats": f"Pressão: {rec_a}"}
    return None

# --- 7. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves PRO")
    with st.expander("⚙️ Configurações", expanded=True):
        API_KEY = st.text_input("API Key:", type="password")
        TG_TOKEN = st.text_input("Token Telegram:", type="password")
        TG_CHAT = st.text_input("Chat IDs:")
        INTERVALO = st.slider("Ciclo (s):", 30, 300, 60)
        
        c1, c2 = st.columns(2)
        if c1.button("🔔 Teste"): enviar_telegram(TG_TOKEN, TG_CHAT, "✅ Teste OK")
        if c2.button("📤 Relatório"): enviar_relatorio_final(TG_TOKEN, TG_CHAT)
        
        st.markdown("---")
        if st.button("🗑️ RESETAR TUDO (Correção)"):
            for f in FILES.values(): 
                if os.path.exists(f): os.remove(f)
            st.session_state.clear()
            st.rerun()

    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 8. DASHBOARD PRINCIPAL ---
main = st.empty()

if ROBO_LIGADO:
    carregar_tudo()
    
    # 1. Busca Jogos
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"live": "all", "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        jogos = res.get('response', [])
    except: jogos = []

    # 2. Verifica Green/Red
    verificar_resultados(jogos, TG_TOKEN, TG_CHAT)

    radar = []
    ids_black = st.session_state['df_black']['id'].values

    for j in jogos:
        lid = str(j['league']['id'])
        if lid in ids_black: continue
        
        fid = j['fixture']['id']
        tempo = j['fixture']['status']['elapsed'] or 0
        home = j['teams']['home']['name']
        away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"
        
        # Filtros de tempo
        if tempo > 80 or tempo < 2: continue
        
        # Busca stats
        stats = []
        try:
            url_s = "https://v3.football.api-sports.io/fixtures/statistics"
            stats = requests.get(url_s, headers={"x-apisports-key": API_KEY}, params={"fixture": fid}).json().get('response', [])
        except: pass
        
        # Processa
        sinal = analisar_jogo(j, stats, tempo, placar)
        
        # Regra 45 Min (Banimento)
        if not sinal and not stats and tempo >= 45 and int(lid) not in LIGAS_VIP:
            salvar_blacklist(lid, j['league']['country'], j['league']['name'])
            st.toast(f"Liga Banida: {j['league']['name']}")
            continue

        visual_status = "👁️"
        if sinal:
            visual_status = "✅ " + sinal['tag']
            if fid not in st.session_state['alertas_enviados']:
                msg = f"🚨 *{sinal['tag']}*\n⚽ {home} {placar} {away}\n🏆 {j['league']['name']}\n⚠️ {sinal['ordem']}\n📈 {sinal['stats']}"
                enviar_telegram(TG_TOKEN, TG_CHAT, msg)
                st.session_state['alertas_enviados'].add(fid)
                
                item = {
                    "Data": datetime.now().strftime('%Y-%m-%d'), "Hora": datetime.now().strftime('%H:%M'),
                    "Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Placar_Sinal": placar,
                    "Estrategia": sinal['tag'], "Resultado": "Pendente"
                }
                salvar_historico(item)
                st.toast(f"Sinal: {sinal['tag']}")

        radar.append({
            "Liga": j['league']['name'], "Jogo": f"{home} {placar} {away}",
            "Tempo": f"{tempo}'", "Status": visual_status
        })

    # Agenda
    agenda = []
    try:
        url_a = "https://v3.football.api-sports.io/fixtures"
        p_a = {"date": datetime.now().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}
        res_a = requests.get(url_a, headers={"x-apisports-key": API_KEY}, params=p_a).json().get('response', [])
        limite = (datetime.utcnow() - timedelta(minutes=15)).strftime('%H:%M')
        for p in res_a:
            if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] == 'NS' and p['fixture']['date'][11:16] >= limite:
                agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
    except: pass

    # Envio Relatório Automático
    if not radar and not agenda:
        if not os.path.exists(FILES['report']) or open(FILES['report']).read() != datetime.now().strftime('%Y-%m-%d'):
            enviar_relatorio_final(TG_TOKEN, TG_CHAT)

    # --- VISUAL ---
    with main.container():
        st.markdown('<div class="status-active">🟢 SISTEMA ATIVO</div>', unsafe_allow_html=True)
        
        # Monitor de Sinais (Restaurado)
        hoje = datetime.now().strftime('%Y-%m-%d')
        df_h = st.session_state['df_hist']
        df_hoje = df_h[df_h['Data'] == hoje] if not df_h.empty and 'Data' in df_h.columns else pd.DataFrame()
        
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-value">{len(df_hoje)}</div><div class="metric-title">Sinais Hoje</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-value">{len(radar)}</div><div class="metric-title">Jogos Live</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-value">{len(LIGAS_VIP)}</div><div class="metric-title">Ligas VIP</div></div>', unsafe_allow_html=True)
        
        st.write("")

        # Abas
        t1, t2, t3, t4, t5 = st.tabs([
            f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", 
            f"📜 Histórico ({len(df_hoje)})", f"🚫 Blacklist ({len(st.session_state['df_black'])})", "⚙️ Dados"
        ])
        
        with t1: st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True) if radar else st.info("Monitorando...")
        with t2: st.dataframe(pd.DataFrame(agenda).sort_values('Hora'), use_container_width=True, hide_index=True) if agenda else st.caption("Sem jogos.")
        with t3: st.dataframe(df_hoje, use_container_width=True, hide_index=True) if not df_hoje.empty else st.caption("Nenhum sinal.")
        with t4: st.dataframe(st.session_state['df_black'], use_container_width=True, hide_index=True)
        with t5: st.json(st.session_state['ligas_imunes'])

        # Manual Restaurado
        with st.expander("📘 Manual de Estratégias", expanded=False):
            c1, c2 = st.columns(2)
            c1.markdown("### 🟣 Porteira Aberta\n2+ gols antes dos 30'.")
            c1.markdown("### 🟢 Blitz\nFavorito perdendo com pressão.")
            c2.markdown("### 💰 Janela de Ouro\n70-75' com pressão e placar apertado.")
            c2.markdown("### ⚡ Gol Relâmpago\n5-15', início elétrico.")

    # Timer Restaurado no Rodapé
    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
        time.sleep(1)
    st.rerun()

else:
    with main.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure na lateral.")
