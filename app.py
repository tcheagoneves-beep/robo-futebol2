import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 0. CONFIGURAÇÃO E LIMPEZA DE CACHE ---
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")
st.cache_data.clear()

# --- CSS VISUAL (RESTAURADO AO MODELO QUE VOCÊ GOSTA) ---
st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .status-active {
        background-color: #1F4025; color: #00FF00; 
        border: 1px solid #00FF00; padding: 10px; 
        text-align: center; border-radius: 5px; font-weight: bold;
        margin-bottom: 10px;
    }
    .metric-card {
        background-color: #1e1e1e; border: 1px solid #333; 
        border-radius: 8px; padding: 15px; text-align: center;
    }
    .metric-value {font-size: 24px; font-weight: bold; color: #00FF00;}
    .metric-label {font-size: 14px; color: #ccc;}
    /* Timer fixo lá embaixo */
    .footer-timer {
        position: fixed; left: 0; bottom: 0; width: 100%;
        background-color: #000000; color: #FFD700;
        text-align: center; padding: 10px; font-size: 18px;
        font-weight: bold; border-top: 2px solid #FFD700;
        z-index: 9999;
    }
    .strategy-title { color: #00FF00; font-weight: bold; font-size: 16px; margin-bottom: 5px; }
    .strategy-desc { font-size: 13px; color: #cccccc; line-height: 1.6; }
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

# --- 3. FUNÇÕES DE DADOS SEGURAS ---
def safe_read_csv(filepath, columns):
    if not os.path.exists(filepath): return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(filepath)
        # Se as colunas não baterem, reseta o arquivo
        if not set(columns).issubset(df.columns):
            return pd.DataFrame(columns=columns)
        return df.fillna("").astype(str)
    except:
        return pd.DataFrame(columns=columns)

def carregar_dados_iniciais():
    """Carrega dados na sessão se não existirem"""
    if 'df_black' not in st.session_state:
        st.session_state['df_black'] = safe_read_csv(FILES['black'], ['id', 'País', 'Liga'])
    
    if 'df_vip' not in st.session_state:
        st.session_state['df_vip'] = safe_read_csv(FILES['vip'], ['id', 'País', 'Liga', 'Data_Erro', 'Strikes'])
    
    if 'historico_sinais' not in st.session_state:
        df = safe_read_csv(FILES['hist'], ['Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado'])
        # Converte para lista de dicionários para manipulação fácil
        st.session_state['historico_sinais'] = df.to_dict('records')

def salvar_blacklist(id_liga, pais, nome_liga):
    novo = pd.DataFrame([{'id': str(id_liga), 'País': str(pais), 'Liga': str(nome_liga)}])
    try:
        df_atual = st.session_state['df_black']
        if str(id_liga) not in df_atual['id'].values:
            df_final = pd.concat([df_atual, novo], ignore_index=True)
            df_final.to_csv(FILES['black'], index=False)
            st.session_state['df_black'] = df_final
    except: pass

def salvar_historico_arquivo():
    if 'historico_sinais' in st.session_state:
        df = pd.DataFrame(st.session_state['historico_sinais'])
        df.to_csv(FILES['hist'], index=False)

# --- 4. INICIALIZAÇÃO DE VARIÁVEIS ---
if 'ligas_imunes' not in st.session_state: st.session_state['ligas_imunes'] = {}
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
carregar_dados_iniciais()

# --- 5. LÓGICA TELEGRAM E GREENS ---
def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "Markdown"}, timeout=3)
        except: pass

def verificar_greens_reds(jogos_live, token, chat_ids):
    """Atualiza status dos sinais (Pendente -> Green/Red)"""
    atualizou = False
    historico = st.session_state['historico_sinais']
    
    for sinal in historico:
        if sinal['Resultado'] == 'Pendente':
            # Tenta achar o jogo na lista da API
            jogo = next((j for j in jogos_live if j['teams']['home']['name'] in sinal['Jogo']), None)
            
            if jogo:
                gh = jogo['goals']['home'] or 0
                ga = jogo['goals']['away'] or 0
                
                # Pega placar do momento do sinal
                try:
                    ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
                except: continue

                # GREEN: Saiu gol
                if (gh + ga) > (ph + pa):
                    sinal['Resultado'] = '✅ GREEN'
                    msg = f"✅ *GREEN!* \n⚽ {sinal['Jogo']}\n📈 Placar Atual: {gh}x{ga}\n🎯 {sinal['Estrategia']}"
                    enviar_telegram(token, chat_ids, msg)
                    atualizou = True
                
                # RED: Jogo acabou
                elif jogo['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
                    sinal['Resultado'] = '❌ RED'
                    msg = f"❌ *RED* \n⚽ {sinal['Jogo']}\n📉 Não bateu."
                    enviar_telegram(token, chat_ids, msg)
                    atualizou = True
    
    if atualizou:
        salvar_historico_arquivo()

def enviar_relatorio_dia(token, chat_ids):
    hoje = datetime.now().strftime('%Y-%m-%d')
    hist = [h for h in st.session_state['historico_sinais'] if h['Data'] == hoje]
    
    if not hist: return
    
    greens = len([h for h in hist if 'GREEN' in h['Resultado']])
    reds = len([h for h in hist if 'RED' in h['Resultado']])
    pendentes = len(hist) - (greens + reds)
    
    msg = f"📊 *RELATÓRIO ({hoje})*\n\n"
    msg += f"🚀 Sinais: {len(hist)}\n✅ Greens: {greens}\n❌ Reds: {reds}\n⏳ Pendentes: {pendentes}"
    
    enviar_telegram(token, chat_ids, msg)
    with open(FILES['report'], 'w') as f: f.write(hoje)

# --- 6. ESTRATÉGIAS ---
def momentum(fid, sog_h, sog_a):
    mem = st.session_state['memoria_pressao'].get(fid, {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
    now = datetime.now()
    if sog_h > mem['sog_h']: mem['h_t'].extend([now] * (sog_h - mem['sog_h']))
    if sog_a > mem['sog_a']: mem['a_t'].extend([now] * (sog_a - mem['sog_a']))
    mem['h_t'] = [t for t in mem['h_t'] if now - t <= timedelta(minutes=7)]
    mem['a_t'] = [t for t in mem['a_t'] if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a
    st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_t']), len(mem['a_t'])

def processar_jogo(j, stats, tempo, placar):
    if not stats: return None
    
    # Validação de Qualidade
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
    
    with st.expander("✅ Legenda de Estratégias", expanded=True):
        st.caption("🟣 Porteira Aberta (2+ gols <30')")
        st.caption("🟢 Blitz/Reação (Pressão do favorito)")
        st.caption("💰 Janela de Ouro (70-75' intenso)")
        st.caption("⚡ Gol Relâmpago (Início elétrico)")

    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API:", type="password")
        TG_TOKEN = st.text_input("Token Telegram:", type="password")
        TG_CHAT = st.text_input("Chat IDs:")
        INTERVALO = st.slider("Ciclo (s):", 30, 300, 60)
        
        c1, c2 = st.columns(2)
        if c1.button("Teste Msg"): enviar_telegram(TG_TOKEN, TG_CHAT, "✅ Teste OK")
        if c2.button("Relatório"): enviar_relatorio_dia(TG_TOKEN, TG_CHAT)
        
        st.markdown("---")
        if st.button("🗑️ RESETAR ERROS"):
            for f in FILES.values(): 
                if os.path.exists(f): os.remove(f)
            st.session_state.clear()
            st.rerun()

    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 8. DASHBOARD PRINCIPAL ---
main = st.empty()

if ROBO_LIGADO:
    # 1. API
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"live": "all", "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        jogos = res.get('response', [])
    except: jogos = []

    # 2. Check Greens/Reds
    verificar_greens_reds(jogos, TG_TOKEN, TG_CHAT)

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
        
        # Filtros
        if tempo > 80 or tempo < 5: continue
        
        # Stats
        try:
            url_s = "https://v3.football.api-sports.io/fixtures/statistics"
            stats = requests.get(url_s, headers={"x-apisports-key": API_KEY}, params={"fixture": fid}, timeout=5).json().get('response', [])
        except: stats = []
        
        sinal = processar_jogo(j, stats, tempo, placar)
        
        # Banimento Automático (45min sem dados)
        if not sinal and not stats and tempo >= 45 and int(lid) not in LIGAS_VIP:
            salvar_blacklist(lid, j['league']['country'], j['league']['name'])
            continue
        
        # Sinais
        vis_status = "👁️"
        if sinal:
            vis_status = "✅ " + sinal['tag']
            if fid not in st.session_state['alertas_enviados']:
                msg = f"🚨 *{sinal['tag']}*\n⚽ {home} {placar} {away}\n🏆 {j['league']['name']}\n⚠️ {sinal['ordem']}\n📈 {sinal['stats']}"
                enviar_telegram(TG_TOKEN, TG_CHAT, msg)
                st.session_state['alertas_enviados'].add(fid)
                
                item = {
                    "Data": datetime.now().strftime('%Y-%m-%d'), "Hora": datetime.now().strftime('%H:%M'),
                    "Liga": j['league']['name'], "Jogo": f"{home} x {away}",
                    "Placar_Sinal": placar, "Estrategia": sinal['tag'], "Resultado": "Pendente"
                }
                st.session_state['historico_sinais'].insert(0, item)
                salvar_historico_arquivo()
                st.toast(f"Sinal Enviado: {sinal['tag']}")

        radar.append({
            "Liga": j['league']['name'],
            "Jogo": f"{home} {placar} {away}",
            "Tempo": f"{tempo}'",
            "Status": vis_status
        })

    # Agenda
    agenda = []
    try:
        url_a = "https://v3.football.api-sports.io/fixtures"
        p_a = {"date": datetime.now().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}
        res_a = requests.get(url_a, headers={"x-apisports-key": API_KEY}, params=p_a).json().get('response', [])
        limit = (datetime.utcnow() - timedelta(minutes=15)).strftime('%H:%M')
        for p in res_a:
            if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] == 'NS' and p['fixture']['date'][11:16] >= limit:
                agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
    except: pass

    # Relatório Auto
    if not radar and not agenda:
        if not os.path.exists(FILES['report']) or open(FILES['report']).read() != datetime.now().strftime('%Y-%m-%d'):
            enviar_relatorio_dia(TG_TOKEN, TG_CHAT)

    # --- EXIBIÇÃO ---
    with main.container():
        st.markdown('<div class="status-active">🟢 SISTEMA ATIVO</div>', unsafe_allow_html=True)
        
        hoje = datetime.now().strftime('%Y-%m-%d')
        # Filtra histórico para exibir apenas hoje
        hist_hoje = [x for x in st.session_state['historico_sinais'] if x['Data'] == hoje]
        
        # MONITOR DE SINAIS (PLACAR)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-card"><div class="metric-value">{len(hist_hoje)}</div><div class="metric-label">Sinais Hoje</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-card"><div class="metric-value">{len(radar)}</div><div class="metric-label">Jogos Live</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-card"><div class="metric-value">{len(st.session_state["df_black"])}</div><div class="metric-label">Blacklist</div></div>', unsafe_allow_html=True)
        
        st.write("")

        # ABAS
        t1, t2, t3, t4, t5 = st.tabs([
            f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", 
            f"📜 Histórico ({len(hist_hoje)})", f"🚫 Blacklist ({len(st.session_state['df_black'])})", "🛡️ Seguras"
        ])
        
        # USO DE IF/ELSE EXPLÍCITO PARA EVITAR ATTRIBUTE ERROR NO PYTHON 3.13
        with t1:
            if radar: st.dataframe(pd.DataFrame(radar).astype(str), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        
        with t2:
            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Agenda vazia.")
            
        with t3:
            if hist_hoje: st.dataframe(pd.DataFrame(hist_hoje).astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Nenhum sinal hoje.")
            
        with t4:
            if not st.session_state['df_black'].empty: st.dataframe(st.session_state['df_black'].astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Limpo.")
            
        with t5:
            if st.session_state['ligas_imunes']: st.write(st.session_state['ligas_imunes'])
            else: st.caption("Nenhuma.")

        # MANUAL
        with st.expander("📘 Manual de Estratégias", expanded=False):
            c1, c2 = st.columns(2)
            c1.markdown("### 🟣 Porteira Aberta\n2+ gols antes dos 30'.")
            c1.markdown("### 🟢 Blitz\nFavorito perdendo com pressão.")
            c2.markdown("### 💰 Janela de Ouro\n70-75' com pressão e placar apertado.")
            c2.markdown("### ⚡ Gol Relâmpago\n5-15', início elétrico.")

    # TIMER NO RODAPÉ
    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
        time.sleep(1)
    st.rerun()

else:
    with main.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure na lateral.")
