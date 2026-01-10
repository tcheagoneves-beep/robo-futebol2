import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 0. CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")
st.cache_data.clear()

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    
    /* Cards de Métricas */
    .metric-box {
        background-color: #1A1C24; border: 1px solid #333; 
        border-radius: 8px; padding: 15px; text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2); margin-bottom: 5px;
    }
    .metric-title {font-size: 11px; color: #aaaaaa; text-transform: uppercase; letter-spacing: 1px;}
    .metric-value {font-size: 22px; font-weight: bold; color: #00FF00;}
    
    /* Timer Fixo */
    .footer-timer {
        position: fixed; left: 0; bottom: 0; width: 100%;
        background-color: #0E1117; color: #FFD700;
        text-align: center; padding: 8px; font-size: 14px;
        font-weight: bold; border-top: 1px solid #333;
        z-index: 9999;
    }
    
    /* Tabelas */
    .stDataFrame { font-size: 12px; }
    
    /* Status */
    .status-active {
        background-color: #1F4025; color: #00FF00; padding: 8px; 
        text-align: center; border-radius: 6px; font-weight: bold; margin-bottom: 10px;
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

# --- 3. DADOS ---
def load_safe(path, cols):
    if not os.path.exists(path): return pd.DataFrame(columns=cols)
    try:
        df = pd.read_csv(path)
        if not set(cols).issubset(df.columns): return pd.DataFrame(columns=cols)
        return df.fillna("").astype(str)
    except: return pd.DataFrame(columns=cols)

def carregar_tudo():
    st.session_state['df_black'] = load_safe(FILES['black'], ['id', 'País', 'Liga'])
    st.session_state['df_vip'] = load_safe(FILES['vip'], ['id', 'País', 'Liga', 'Data_Erro', 'Strikes'])
    
    # Carrega histórico completo para estatísticas
    df = load_safe(FILES['hist'], ['Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado'])
    st.session_state['historico_sinais'] = df.to_dict('records')

def salvar_blacklist(id_liga, pais, nome_liga):
    novo = pd.DataFrame([{'id': str(id_liga), 'País': str(pais), 'Liga': str(nome_liga)}])
    try:
        df = st.session_state['df_black']
        if str(id_liga) not in df['id'].values:
            final = pd.concat([df, novo], ignore_index=True)
            final.to_csv(FILES['black'], index=False)
            st.session_state['df_black'] = final
    except: pass

def salvar_strike(id_liga, pais, nome_liga, strikes):
    df = st.session_state['df_vip']
    hoje = datetime.now().strftime('%Y-%m-%d')
    id_str = str(id_liga)
    if id_str in df['id'].values: df = df[df['id'] != id_str]
    
    novo = pd.DataFrame([{
        'id': id_str, 'País': str(pais), 'Liga': str(nome_liga), 
        'Data_Erro': hoje, 'Strikes': str(strikes)
    }])
    final = pd.concat([df, novo], ignore_index=True)
    final.to_csv(FILES['vip'], index=False)
    st.session_state['df_vip'] = final

def salvar_historico(item):
    df = pd.DataFrame([item])
    df.to_csv(FILES['hist'], mode='a', header=not os.path.exists(FILES['hist']), index=False)

# --- 4. ESTATÍSTICAS ---
def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

# --- 5. TELEGRAM ---
def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "Markdown"}, timeout=3)
        except: pass

def check_green_red(jogos, token, chats):
    atualizou = False
    hist = st.session_state['historico_sinais']
    
    for s in hist:
        if s['Resultado'] == 'Pendente':
            jogo = next((j for j in jogos if j['teams']['home']['name'] in s['Jogo']), None)
            if jogo:
                gh = jogo['goals']['home'] or 0
                ga = jogo['goals']['away'] or 0
                try: ph, pa = map(int, s['Placar_Sinal'].split('x'))
                except: continue
                
                # Green: Saiu Gol
                if (gh+ga) > (ph+pa):
                    s['Resultado'] = '✅ GREEN'
                    msg = f"✅ *GREEN!* \n⚽ {s['Jogo']}\n🏆 {s['Liga']}\n📈 {gh}x{ga} (Era {s['Placar_Sinal']})\n🎯 {s['Estrategia']}"
                    enviar_telegram(token, chats, msg)
                    atualizou = True
                
                # Red: Acabou
                elif jogo['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
                    s['Resultado'] = '❌ RED'
                    msg = f"❌ *RED* \n⚽ {s['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {s['Estrategia']}"
                    enviar_telegram(token, chats, msg)
                    atualizou = True
    
    if atualizou:
        pd.DataFrame(hist).to_csv(FILES['hist'], index=False)

def reenviar_sinais(token, chats):
    hoje = datetime.now().strftime('%Y-%m-%d')
    hist = [h for h in st.session_state['historico_sinais'] if h['Data'] == hoje]
    if not hist: return st.toast("Sem sinais hoje.")
    st.toast("Reenviando...")
    for s in reversed(hist):
        msg = f"🔄 *REENVIO*\n\n🚨 *{s['Estrategia']}*\n⚽ {s['Jogo']}\n⚠️ Placar Sinal: {s.get('Placar_Sinal','?')}"
        enviar_telegram(token, chats, msg)
        time.sleep(1)

def relatorio_final(token, chats):
    hoje = datetime.now().strftime('%Y-%m-%d')
    hist = [h for h in st.session_state['historico_sinais'] if h['Data'] == hoje]
    if not hist: return
    
    df_hj = pd.DataFrame(hist)
    tot, g, r, wr = calcular_stats(df_hj)
    
    msg = f"📊 *FECHAMENTO DO MERCADO* ({hoje})\n\n🚀 Sinais: {tot}\n✅ Greens: {g}\n❌ Reds: {r}\n🎯 Assertividade: {wr:.1f}%"
    enviar_telegram(token, chats, msg)
    with open(FILES['report'], 'w') as f: f.write(hoje)

# --- 6. CORE ---
if 'ligas_imunes' not in st.session_state: st.session_state['ligas_imunes'] = {}
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
carregar_tudo()

def momentum(fid, sog_h, sog_a):
    mem = st.session_state['memoria_pressao'].get(fid, {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
    if 'sog_h' not in mem: mem = {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []} # Cura erro
    now = datetime.now()
    if sog_h > mem['sog_h']: mem['h_t'].extend([now]*(sog_h-mem['sog_h']))
    if sog_a > mem['sog_a']: mem['a_t'].extend([now]*(sog_a-mem['sog_a']))
    mem['h_t'] = [t for t in mem['h_t'] if now - t <= timedelta(minutes=7)]
    mem['a_t'] = [t for t in mem['a_t'] if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a
    st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_t']), len(mem['a_t'])

def processar(j, stats, tempo, placar):
    if not stats: return None
    sog_h, sog_a, sh_h, sh_a, ok = 0, 0, 0, 0, False
    for i, t in enumerate(stats):
        for s in t.get('statistics', []):
            if s['type']=='Total Shots' and s['value'] is not None:
                ok=True
                if i==0: sh_h=s['value']
                else: sh_a=s['value']
            if s['type']=='Shots on Goal' and s['value'] is not None:
                ok=True
                if i==0: sog_h=s['value']
                else: sog_a=s['value']
    
    if not ok: return None
    
    fid = j['fixture']['id']
    gh = j['goals']['home'] or 0
    ga = j['goals']['away'] or 0
    rh, ra = momentum(fid, sog_h, sog_a)
    
    if tempo <= 30 and (gh+ga) >= 2: return {"tag": "🟣 Porteira Aberta", "ordem": "🔥 ENTRADA SECA: Over Gols", "stats": f"{gh}x{ga}"}
    if 5 <= tempo <= 15 and (sog_h+sog_a) >= 1: return {"tag": "⚡ Gol Relâmpago", "ordem": "Over 0.5 HT", "stats": f"Chutes: {sog_h+sog_a}"}
    if 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(gh-ga) <= 1: return {"tag": "💰 Janela de Ouro", "ordem": "Over Gols Asiático", "stats": f"Total: {sh_h+sh_a}"}
    if tempo <= 60:
        if gh <= ga and (rh >= 2 or sh_h >= 8): return {"tag": "🟢 Blitz Casa", "ordem": "Gol Mandante", "stats": f"Pressão: {rh}"}
        if ga <= gh and (ra >= 2 or sh_a >= 8): return {"tag": "🟢 Blitz Visitante", "ordem": "Gol Visitante", "stats": f"Pressão: {ra}"}
    return None

def gerenciar_strikes(id_liga, pais, nome_liga):
    df = st.session_state['df_vip']
    hoje = datetime.now().strftime('%Y-%m-%d')
    id_str = str(id_liga)
    strikes = 0
    data_antiga = ""
    if id_str in df['id'].values:
        row = df[df['id'] == id_str].iloc[0]
        strikes = int(row['Strikes'])
        data_antiga = row['Data_Erro']
    if data_antiga == hoje: return
    novo_strike = strikes + 1
    if novo_strike >= 2:
        salvar_blacklist(id_liga, pais, nome_liga)
        st.toast(f"🚫 {nome_liga} Banida")
    else:
        salvar_strike(id_liga, pais, nome_liga, novo_strike)
        st.toast(f"⚠️ {nome_liga} Strike 1/2")

# --- 7. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves PRO")
    
    with st.expander("⚙️ Configurações", expanded=True):
        API_KEY = st.text_input("Chave API:", type="password")
        TG_TOKEN = st.text_input("Token Telegram:", type="password")
        TG_CHAT = st.text_input("Chat IDs:")
        INTERVALO = st.slider("Ciclo (s):", 30, 300, 60)
        
        c1, c2 = st.columns(2)
        if c1.button("🔄 Reenviar\nSinais"): reenviar_sinais(TG_TOKEN, TG_CHAT)
        if c2.button("🗑️ Limpar\nBlacklist"):
            if os.path.exists(FILES['black']): os.remove(FILES['black'])
            st.session_state['df_black'] = pd.DataFrame(columns=['id', 'País', 'Liga'])
            st.rerun()

    with st.expander("📘 Manual", expanded=False):
        st.write("🟣 **Porteira:** 2 gols < 30min")
        st.write("🟢 **Blitz:** Pressão forte")
        st.write("💰 **Janela:** 70-75min intenso")
        st.write("⚡ **Relâmpago:** 5-15' elétrico")

    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 8. DASHBOARD ---
main = st.empty()

if ROBO_LIGADO:
    carregar_tudo()
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10).json()
        jogos = res.get('response', [])
    except: jogos = []

    check_green_red(jogos, TG_TOKEN, TG_CHAT)

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
        
        if tempo > 80 or tempo < 2: continue
        
        try:
            stats = requests.get("https://v3.football.api-sports.io/fixtures/statistics", headers={"x-apisports-key": API_KEY}, params={"fixture": fid}).json().get('response', [])
        except: stats = []
        
        sinal = processar(j, stats, tempo, placar)
        
        if not sinal and not stats and tempo >= 45: gerenciar_strikes(lid, j['league']['country'], j['league']['name'])
        if stats: st.session_state['ligas_imunes'][lid] = {'País': j['league']['country'], 'Liga': j['league']['name']}
        
        status_vis = "👁️"
        if sinal:
            status_vis = "✅ " + sinal['tag']
            if fid not in st.session_state['alertas_enviados']:
                msg = f"🚨 *{sinal['tag']}*\n⚽ {home} {placar} {away}\n🏆 {j['league']['name']}\n⚠️ {sinal['ordem']}\n📈 {sinal['stats']}"
                enviar_telegram(TG_TOKEN, TG_CHAT, msg)
                st.session_state['alertas_enviados'].add(fid)
                item = {"Data": datetime.now().strftime('%Y-%m-%d'), "Hora": datetime.now().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Placar_Sinal": placar, "Estrategia": sinal['tag'], "Resultado": "Pendente"}
                st.session_state['historico_sinais'].insert(0, item)
                salvar_historico(item)
                st.toast(f"Sinal: {sinal['tag']}")

        radar.append({"Liga": j['league']['name'], "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})

    # Agenda
    agenda = []
    try:
        prox = requests.get(url, headers={"x-apisports-key": API_KEY}, params={"date": datetime.now().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}).json().get('response', [])
        limit = (datetime.utcnow() - timedelta(minutes=15)).strftime('%H:%M')
        for p in prox:
            if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] == 'NS' and p['fixture']['date'][11:16] >= limit:
                agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
    except: pass

    if not radar and not agenda:
        if not os.path.exists(FILES['report']) or open(FILES['report']).read() != datetime.now().strftime('%Y-%m-%d'):
            relatorio_final(TG_TOKEN, TG_CHAT)

    # --- RENDERIZAÇÃO ---
    with main.container():
        st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        # PREPARAÇÃO DE DADOS PARA ESTATÍSTICAS
        df_full = pd.DataFrame(st.session_state['historico_sinais'])
        if not df_full.empty:
            hoje = datetime.now().strftime('%Y-%m-%d')
            # Filtro Hoje
            df_hoje = df_full[df_full['Data'] == hoje]
            # Filtro 7 Dias (Simples)
            df_full['Data_Dt'] = pd.to_datetime(df_full['Data'], errors='coerce')
            data_7dias = pd.to_datetime(hoje) - timedelta(days=7)
            df_7dias = df_full[df_full['Data_Dt'] >= data_7dias]
            # Filtro Mês
            df_mes = df_full[df_full['Data_Dt'].dt.month == datetime.now().month]
        else:
            df_hoje = df_7dias = df_mes = pd.DataFrame(columns=['Resultado'])

        # CÁLCULO DE MÉTRICAS
        t_hj, g_hj, r_hj, w_hj = calcular_stats(df_hoje)
        t_7d, g_7d, r_7d, w_7d = calcular_stats(df_7dias)
        t_ms, g_ms, r_ms, w_ms = calcular_stats(df_mes)
        t_all, g_all, r_all, w_all = calcular_stats(df_full)

        # 1. ABAS DE NAVEGAÇÃO
        t1, t2, t3, t4, t5, t6, t7 = st.tabs([
            f"📡 Radar ({len(radar)})", 
            f"📈 Estatísticas", 
            f"📜 Histórico ({len(df_hoje)})",
            f"📅 Agenda ({len(agenda)})", 
            f"🚫 Blacklist ({len(st.session_state['df_black'])})", 
            f"🛡️ Seguras ({len(st.session_state['ligas_imunes'])})", 
            "⚠️ Obs"
        ])
        
        with t1:
            if radar: st.dataframe(pd.DataFrame(radar).astype(str), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        
        # ABA NOVA: ESTATÍSTICAS AVANÇADAS
        with t2:
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.markdown(f'<div class="metric-box"><div class="metric-title">HOJE</div><div class="metric-value">{w_hj:.0f}%</div><div style="font-size:12px">{g_hj}G - {r_hj}R</div></div>', unsafe_allow_html=True)
            with c2:
                st.markdown(f'<div class="metric-box"><div class="metric-title">7 DIAS</div><div class="metric-value">{w_7d:.0f}%</div><div style="font-size:12px">{g_7d}G - {r_7d}R</div></div>', unsafe_allow_html=True)
            with c3:
                st.markdown(f'<div class="metric-box"><div class="metric-title">MÊS ATUAL</div><div class="metric-value">{w_ms:.0f}%</div><div style="font-size:12px">{g_ms}G - {r_ms}R</div></div>', unsafe_allow_html=True)
            with c4:
                st.markdown(f'<div class="metric-box"><div class="metric-title">GERAL</div><div class="metric-value">{w_all:.0f}%</div><div style="font-size:12px">{g_all}G - {r_all}R</div></div>', unsafe_allow_html=True)
            
            st.caption("Baseado nos dados salvos no histórico.")

        with t3:
            if not df_hoje.empty: st.dataframe(df_hoje.astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Nenhum sinal hoje.")
            
        with t4:
            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos.")
            
        with t5:
            if not st.session_state['df_black'].empty: st.dataframe(st.session_state['df_black'].astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Limpo.")
            
        with t6:
            if st.session_state['ligas_imunes']: 
                safe_l = [{'id': k, 'País': v['País'], 'Liga': v['Liga']} for k,v in st.session_state['ligas_imunes'].items()]
                st.dataframe(pd.DataFrame(safe_l).astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Nenhuma.")
            
        with t7:
            if not st.session_state['df_vip'].empty: st.dataframe(st.session_state['df_vip'].astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Tudo ok.")

    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
        time.sleep(1)
    st.rerun()

else:
    with main.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure na lateral.")
