import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 0. CONFIGURAÇÃO VISUAL (CENTRALIZADO E LIMPO) ---
st.set_page_config(page_title="Neves Analytics PRO", layout="centered", page_icon="❄️")
st.cache_data.clear()

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    
    /* Cards de Métricas Compactos */
    .metric-box {
        background-color: #1A1C24; 
        border: 1px solid #333; 
        border-radius: 8px; 
        padding: 10px; 
        text-align: center;
        margin-bottom: 5px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    .metric-title {font-size: 11px; color: #aaaaaa; text-transform: uppercase; letter-spacing: 1px;}
    .metric-value {font-size: 20px; font-weight: bold; color: #00FF00;}
    
    /* Status Ativo */
    .status-active {
        background-color: #1F4025; color: #00FF00; 
        border: 1px solid #00FF00; padding: 8px; 
        text-align: center; border-radius: 6px; font-weight: bold;
        font-size: 14px; margin-bottom: 15px;
    }
    
    /* Timer Fixo no Rodapé */
    .footer-timer {
        position: fixed; left: 0; bottom: 0; width: 100%;
        background-color: #0E1117; color: #FFD700;
        text-align: center; padding: 8px; font-size: 14px;
        font-weight: bold; border-top: 1px solid #333;
        z-index: 9999;
    }
    
    /* Ajuste de Tabelas */
    .stDataFrame { font-size: 12px; }
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
    
    df = load_safe(FILES['hist'], ['Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado'])
    hoje = datetime.now().strftime('%Y-%m-%d')
    if not df.empty:
        df = df[df['Data'] == hoje]
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

# --- 4. LÓGICA DE INTEGRIDADE (2 RODADAS) ---
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
    
    if data_antiga == hoje: return # Já anotou hoje

    novo_strike = strikes + 1
    if novo_strike >= 2:
        salvar_blacklist(id_liga, pais, nome_liga)
        st.toast(f"🚫 {nome_liga} Banida (2 Rodadas)")
    else:
        salvar_strike(id_liga, pais, nome_liga, novo_strike)
        st.toast(f"⚠️ {nome_liga} Strike 1/2")

# --- 5. TELEGRAM ---
def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "Markdown"}, timeout=3)
        except: pass

def reenviar_sinais(token, chats):
    hist = st.session_state['historico_sinais']
    if not hist: return st.toast("Nada para reenviar.")
    st.toast(f"Reenviando {len(hist)} sinais...")
    for s in reversed(hist):
        msg = f"🔄 *REENVIO*\n\n🚨 *{s['Estrategia']}*\n⚽ {s['Jogo']}\n🏆 {s['Liga']}\n⚠️ {s.get('Placar_Sinal','?')}"
        enviar_telegram(token, chats, msg)
        time.sleep(1)

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
                
                if (gh+ga) > (ph+pa):
                    s['Resultado'] = '✅ GREEN'
                    enviar_telegram(token, chats, f"✅ *GREEN!* \n⚽ {s['Jogo']}\n💰 {s['Estrategia']}")
                    atualizou = True
                elif jogo['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
                    s['Resultado'] = '❌ RED'
                    enviar_telegram(token, chats, f"❌ *RED* \n⚽ {s['Jogo']}\n📉 {s['Estrategia']}")
                    atualizou = True
    if atualizou:
        pd.DataFrame(hist).to_csv(FILES['hist'], index=False)

def relatorio_final(token, chats):
    hoje = datetime.now().strftime('%Y-%m-%d')
    hist = [h for h in st.session_state['historico_sinais'] if h['Data'] == hoje]
    if not hist: return
    
    greens = len([h for h in hist if 'GREEN' in h['Resultado']])
    reds = len([h for h in hist if 'RED' in h['Resultado']])
    total = len(hist)
    winrate = (greens / (greens+reds) * 100) if (greens+reds) > 0 else 0
    
    msg = f"📊 *RELATÓRIO ({hoje})*\n\n🚀 Sinais: {total}\n✅ Greens: {greens}\n❌ Reds: {reds}\n🎯 Winrate: {winrate:.1f}%"
    enviar_telegram(token, chats, msg)
    with open(FILES['report'], 'w') as f: f.write(hoje)

# --- 6. CORE ---
if 'ligas_imunes' not in st.session_state: st.session_state['ligas_imunes'] = {}
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
carregar_tudo()

# --- CORREÇÃO DO KEYERROR: MOMENTUM BLINDADO ---
def momentum(fid, sog_h, sog_a):
    # Tenta pegar a memória. Se vier vazia OU com chaves erradas (da versão antiga), reseta.
    mem = st.session_state['memoria_pressao'].get(fid)
    
    # Validação de integridade do dicionário (A CURA DO ERRO)
    if not mem or 'sog_h' not in mem or 'sog_a' not in mem:
        mem = {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []}
    
    now = datetime.now()
    
    # Lógica de acúmulo
    if sog_h > mem['sog_h']: 
        mem['h_t'].extend([now] * (sog_h - mem['sog_h']))
    if sog_a > mem['sog_a']: 
        mem['a_t'].extend([now] * (sog_a - mem['sog_a']))
    
    # Limpeza de tempo
    mem['h_t'] = [t for t in mem['h_t'] if now - t <= timedelta(minutes=7)]
    mem['a_t'] = [t for t in mem['a_t'] if now - t <= timedelta(minutes=7)]
    
    # Atualiza estado
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

# --- 8. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves PRO")
    
    with st.expander("⚙️ Configurações", expanded=True):
        API_KEY = st.text_input("Chave API:", type="password")
        TG_TOKEN = st.text_input("Token Telegram:", type="password")
        TG_CHAT = st.text_input("Chat IDs:")
        INTERVALO = st.slider("Ciclo (s):", 30, 300, 60)
        
        c1, c2 = st.columns(2)
        if c1.button("🔄 Reenviar Sinais"): reenviar_sinais(TG_TOKEN, TG_CHAT)
        if c2.button("🗑️ Limpar Blacklist"):
            if os.path.exists(FILES['black']): os.remove(FILES['black'])
            st.session_state['df_black'] = pd.DataFrame(columns=['id', 'País', 'Liga'])
            st.rerun()

    with st.expander("📘 Manual", expanded=False):
        st.markdown("**🟣 Porteira:** 2 gols < 30min")
        st.markdown("**🟢 Blitz:** Pressão forte")
        st.markdown("**💰 Janela:** 70-75min intenso")
        st.markdown("**⚡ Relâmpago:** 5-15' elétrico")

    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 9. DISPLAY ---
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
        
        if not sinal and not stats and tempo >= 45:
            gerenciar_strikes(lid, j['league']['country'], j['league']['name'])
        
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
        
        hist_hoje = [x for x in st.session_state['historico_sinais'] if x['Data'] == datetime.now().strftime('%Y-%m-%d')]
        
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-value">{len(hist_hoje)}</div><div class="metric-title">Sinais Hoje</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-value">{len(radar)}</div><div class="metric-title">Jogos Live</div></div>', unsafe_allow_html=True)
        # CORREÇÃO: Mostra Ligas Seguras no Card
        c3.markdown(f'<div class="metric-box"><div class="metric-value">{len(st.session_state["ligas_imunes"])}</div><div class="metric-title">Ligas Seguras</div></div>', unsafe_allow_html=True)
        
        st.write("")

        t1, t2, t3, t4, t5, t6 = st.tabs([
            f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", 
            f"📜 Histórico ({len(hist_hoje)})", f"🚫 Blacklist ({len(st.session_state['df_black'])})", 
            f"🛡️ Seguras ({len(st.session_state['ligas_imunes'])})", f"⚠️ Obs ({len(st.session_state['df_vip'])})"
        ])
        
        # CORREÇÃO DO ERRO VISUAL: USANDO IF/ELSE EXPLÍCITO
        with t1:
            if radar: st.dataframe(pd.DataFrame(radar).astype(str), use_container_width=True, hide_index=True)
            else: st.info("Monitorando jogos...")
        
        with t2:
            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos.")
            
        with t3:
            if hist_hoje: st.dataframe(pd.DataFrame(hist_hoje).astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Nenhum sinal hoje.")
            
        with t4:
            if not st.session_state['df_black'].empty: st.dataframe(st.session_state['df_black'].astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Limpo.")
            
        with t5:
            if st.session_state['ligas_imunes']: 
                safe_l = [{'id': k, 'País': v['País'], 'Liga': v['Liga']} for k,v in st.session_state['ligas_imunes'].items()]
                st.dataframe(pd.DataFrame(safe_l).astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Nenhuma.")
            
        with t6:
            if not st.session_state['df_vip'].empty: st.dataframe(st.session_state['df_vip'].astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Limpo.")

    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
        time.sleep(1)
    st.rerun()

else:
    with main.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure na lateral.")
