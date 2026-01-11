import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta
import pytz

# --- 0. CONFIGURAÇÃO E CSS ---
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .main .block-container { max-width: 100%; padding: 1rem 2rem 5rem 2rem; }
    
    .metric-box {
        background-color: #1A1C24; border: 1px solid #333; 
        border-radius: 8px; padding: 15px; text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    .metric-title {font-size: 13px; color: #aaaaaa; text-transform: uppercase; margin-bottom: 5px;}
    .metric-value {font-size: 26px; font-weight: bold; color: #00FF00;}
    
    .status-active {
        background-color: #1F4025; color: #00FF00; 
        border: 1px solid #00FF00; padding: 10px; 
        text-align: center; border-radius: 6px; font-weight: bold;
        margin-bottom: 20px;
    }
    
    .status-error {
        background-color: #3B1010; color: #FF4B4B; 
        border: 1px solid #FF4B4B; padding: 10px; 
        text-align: center; border-radius: 6px; font-weight: bold;
        margin-bottom: 20px;
    }
    
    .stButton button {
        width: 100%; height: auto !important;
        white-space: pre-wrap !important; 
        font-size: 13px !important; font-weight: bold !important;
        padding: 10px 5px !important; line-height: 1.4 !important;
    }
    
    .footer-timer {
        position: fixed; left: 0; bottom: 0; width: 100%;
        background-color: #0E1117; color: #FFD700;
        text-align: center; padding: 10px; font-size: 16px;
        font-weight: bold; border-top: 1px solid #333;
        z-index: 9999;
    }
    
    .stDataFrame { font-size: 14px; }
</style>
""", unsafe_allow_html=True)

# --- 1. ARQUIVOS ---
FILES = {
    'black': 'neves_blacklist.txt',
    'vip': 'neves_strikes_vip.txt',
    'hist': 'neves_historico_sinais.csv',
    'safe': 'neves_ligas_seguras.txt',
    'report': 'neves_status_relatorio.txt'
}

COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo'] 
LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]

# --- 2. DADOS E UTILITÁRIOS ---
def get_time_br():
    """Retorna datetime atual com fuso horário de SP"""
    return datetime.now(pytz.timezone('America/Sao_Paulo'))

def load_safe(path, cols):
    if not os.path.exists(path): return pd.DataFrame(columns=cols)
    try:
        df = pd.read_csv(path)
        for c in cols:
            if c not in df.columns: df[c] = ""
        return df.fillna("").astype(str)
    except: return pd.DataFrame(columns=cols)

def clean_fid(x):
    try: return str(int(float(x)))
    except: return '0'

def carregar_tudo():
    st.session_state['df_black'] = load_safe(FILES['black'], ['id', 'País', 'Liga'])
    st.session_state['df_vip'] = load_safe(FILES['vip'], ['id', 'País', 'Liga', 'Data_Erro', 'Strikes'])
    st.session_state['df_safe'] = load_safe(FILES['safe'], COLS_SAFE)
    
    df = load_safe(FILES['hist'], COLS_HIST)
    hoje = get_time_br().strftime('%Y-%m-%d')
    
    if not df.empty and 'Data' in df.columns:
        df['FID'] = df['FID'].apply(clean_fid)
        st.session_state['historico_full'] = df
        df_hoje = df[df['Data'] == hoje].copy()
        st.session_state['historico_sinais'] = df_hoje.to_dict('records')[::-1]
    else:
        st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
        st.session_state['historico_sinais'] = []

def salvar_seguro(df, path):
    try:
        df.to_csv(path, index=False)
        return True
    except: return False

def adicionar_historico(item):
    try:
        df_disk = load_safe(FILES['hist'], COLS_HIST)
        df_new = pd.DataFrame([item])
        df_final = pd.concat([df_new, df_disk], ignore_index=True)
        
        if salvar_seguro(df_final, FILES['hist']):
            st.session_state['historico_sinais'].insert(0, item)
            st.session_state['historico_full'] = df_final
            return True
        return False
    except: return False

def atualizar_historico_ram_disk(lista_atualizada):
    df_hoje = pd.DataFrame(lista_atualizada)
    df_disk = load_safe(FILES['hist'], COLS_HIST)
    hoje = get_time_br().strftime('%Y-%m-%d')
    
    if not df_disk.empty:
        df_disk = df_disk[df_disk['Data'] != hoje]
    
    df_final = pd.concat([df_hoje, df_disk], ignore_index=True)
    salvar_seguro(df_final, FILES['hist'])
    st.session_state['historico_full'] = df_final

def salvar_blacklist(id_liga, pais, nome_liga):
    novo = pd.DataFrame([{'id': str(id_liga), 'País': str(pais), 'Liga': str(nome_liga)}])
    df = st.session_state['df_black']
    if str(id_liga) not in df['id'].values:
        final = pd.concat([df, novo], ignore_index=True)
        salvar_seguro(final, FILES['black'])
        st.session_state['df_black'] = final

def salvar_safe_league(id_liga, pais, nome_liga, tem_stats, tem_tabela):
    id_str = str(id_liga)
    motivos = []
    if tem_stats: motivos.append("Chutes")
    if tem_tabela: motivos.append("Tabela")
    motivo_str = " + ".join(motivos) if motivos else "Validada"

    novo = pd.DataFrame([{'id': id_str, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': motivo_str}])
    df = st.session_state['df_safe']
    
    if id_str in df['id'].values:
        idx = df[df['id'] == id_str].index[0]
        if df.at[idx, 'Motivo'] != motivo_str:
            df.at[idx, 'Motivo'] = motivo_str
            salvar_seguro(df, FILES['safe'])
            st.session_state['df_safe'] = df
    else:
        final = pd.concat([df, novo], ignore_index=True)
        salvar_seguro(final, FILES['safe'])
        st.session_state['df_safe'] = final

def salvar_strike(id_liga, pais, nome_liga, strikes):
    df = st.session_state['df_vip']
    hoje = get_time_br().strftime('%Y-%m-%d')
    id_str = str(id_liga)
    if id_str in df['id'].values: df = df[df['id'] != id_str]
    novo = pd.DataFrame([{'id': id_str, 'País': str(pais), 'Liga': str(nome_liga), 'Data_Erro': hoje, 'Strikes': str(strikes)}])
    final = pd.concat([df, novo], ignore_index=True)
    salvar_seguro(final, FILES['vip'])
    st.session_state['df_vip'] = final

# --- 3. ESTATÍSTICAS ---
def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

# --- 4. INTELIGÊNCIA E CACHE ---
@st.cache_data(ttl=86400)
def buscar_ranking(api_key, league_id, season):
    try:
        url = "https://v3.football.api-sports.io/standings"
        params = {"league": league_id, "season": season}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        ranking = {}
        if res.get('response'):
            standings = res['response'][0]['league']['standings'][0]
            for team in standings:
                ranking[team['team']['name']] = team['rank']
        return ranking
    except: return {}

# Cache para a Agenda do Dia (Economiza API)
@st.cache_data(ttl=3600) 
def buscar_agenda_cached(api_key, date_str):
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        return requests.get(url, headers={"x-apisports-key": api_key}, 
                          params={"date": date_str, "timezone": "America/Sao_Paulo"}).json().get('response', [])
    except: return []

def deve_buscar_stats(tempo, gh, ga, status):
    if 5 <= tempo <= 15: return True
    if tempo <= 30 and (gh + ga) >= 2: return True
    if 70 <= tempo <= 75 and abs(gh - ga) <= 1: return True
    if tempo <= 60 and abs(gh - ga) <= 1: return True
    if status == 'HT' and gh == 0 and ga == 0: return True
    return False

# --- 5. TELEGRAM ---
def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "HTML"}, timeout=3)
        except: pass

def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0
    ga = jogo_api['goals']['away'] or 0
    
    if "Múltipla" in sinal['Estrategia']: return False

    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: return False

    if (gh+ga) > (ph+pa):
        sinal['Resultado'] = '✅ GREEN'
        msg = f"✅ <b>GREEN CONFIRMADO!</b>\n\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📈 Placar Atual: <b>{gh}x{ga}</b>\n🎯 {sinal['Estrategia']}"
        enviar_telegram(token, chats, msg)
        return True
    
    status = jogo_api['fixture']['status']['short']
    if status in ['FT', 'AET', 'PEN', 'ABD']:
        sinal['Resultado'] = '❌ RED'
        msg = f"❌ <b>RED | ENCERRADO</b>\n\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {sinal['Estrategia']}"
        enviar_telegram(token, chats, msg)
        return True
    return False

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    atualizou = False
    hist = st.session_state['historico_sinais']
    pendentes = [s for s in hist if s['Resultado'] == 'Pendente']
    if not pendentes: return

    ids_live = [j['fixture']['id'] for j in jogos_live]
    
    for s in pendentes:
        fid = int(clean_fid(s.get('FID', 0)))
        jogo_encontrado = None
        
        # 1. Busca no Live
        if fid > 0 and fid in ids_live:
            jogo_encontrado = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        # 2. Busca API
        elif fid > 0:
            try:
                res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                if res['response']: jogo_encontrado = res['response'][0]
            except: pass
        # 3. Legado (Fallback)
        if not jogo_encontrado and fid == 0:
            try:
                p = {"date": get_time_br().strftime('%Y-%m-%d'), "status": "FT-AET-PEN", "timezone": "America/Sao_Paulo"}
                jogos_ft = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params=p).json().get('response', [])
                time_casa = s['Jogo'].split(' x ')[0].strip()
                jogo_encontrado = next((j for j in jogos_ft if time_casa in j['teams']['home']['name']), None)
            except: pass

        if jogo_encontrado:
            if processar_resultado(s, jogo_encontrado, token, chats): atualizou = True
    
    if atualizou: atualizar_historico_ram_disk(hist)

def reenviar_sinais(token, chats):
    hist = st.session_state['historico_sinais']
    if not hist: return st.toast("Sem sinais hoje.")
    st.toast("Reenviando...")
    for s in reversed(hist):
        msg = f"🔄 <b>REENVIO DE SINAL</b>\n\n🚨 <b>{s['Estrategia']}</b>\n⚽ {s['Jogo']}\n⚠️ Placar Sinal: {s.get('Placar_Sinal','?')}"
        enviar_telegram(token, chats, msg)
        time.sleep(1)

def relatorio_final(token, chats):
    hoje = get_time_br().strftime('%Y-%m-%d')
    hist = st.session_state['historico_sinais']
    if not hist: return
    df_hj = pd.DataFrame(hist)
    tot, g, r, wr = calcular_stats(df_hj)
    msg = f"📊 <b>FECHAMENTO DO MERCADO</b> ({hoje})\n\n🚀 Sinais: {tot}\n✅ Greens: {g}\n❌ Reds: {r}\n🎯 Assertividade: {wr:.1f}%"
    enviar_telegram(token, chats, msg)
    with open(FILES['report'], 'w') as f: f.write(hoje)

# --- 6. CORE ---
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'multiplas_enviadas' not in st.session_state: st.session_state['multiplas_enviadas'] = set()
if 'controle_stats' not in st.session_state: st.session_state['controle_stats'] = {} 
carregar_tudo()

def momentum(fid, sog_h, sog_a):
    mem = st.session_state['memoria_pressao'].get(fid, {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []})
    if 'sog_h' not in mem: mem = {'sog_h': sog_h, 'sog_a': sog_a, 'h_t': [], 'a_t': []}
    now = datetime.now()
    if sog_h > mem['sog_h']: mem['h_t'].extend([now]*(sog_h-mem['sog_h']))
    if sog_a > mem['sog_a']: mem['a_t'].extend([now]*(sog_a-mem['sog_a']))
    mem['h_t'] = [t for t in mem['h_t'] if now - t <= timedelta(minutes=7)]
    mem['a_t'] = [t for t in mem['a_t'] if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a
    st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_t']), len(mem['a_t'])

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']
        stats_a = stats[1]['statistics']
        def get_v(l, t): 
            v = next((x['value'] for x in l if x['type']==t), 0)
            return v if v is not None else 0
        sh_h = get_v(stats_h, 'Total Shots')
        sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots')
        sog_a = get_v(stats_a, 'Shots on Goal')
    except: return []
    
    fid = j['fixture']['id']
    gh = j['goals']['home'] or 0
    ga = j['goals']['away'] or 0
    rh, ra = momentum(fid, sog_h, sog_a)
    SINAIS = []

    # ESTRATÉGIAS
    if tempo <= 30 and (gh+ga) >= 2: 
        SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": "🔥 ENTRADA SECA: Over Gols", "stats": f"{gh}x{ga}"})
    if 5 <= tempo <= 15 and (sog_h+sog_a) >= 1: 
        SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": "Apostar Over 0.5 HT", "stats": f"Chutes: {sog_h+sog_a}"})
    if 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(gh-ga) <= 1: 
        SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": "Over Gols Asiático", "stats": f"Total: {sh_h+sh_a}"})
    if tempo <= 60:
        if gh <= ga and (rh >= 2 or sh_h >= 8): 
            SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": "Gol Mandante", "stats": f"Pressão: {rh}"})
        if ga <= gh and (ra >= 2 or sh_a >= 8): 
            SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": "Gol Visitante", "stats": f"Pressão: {ra}"})

    if rank_home and rank_away:
        is_top_home = rank_home <= 4
        is_top_away = rank_away <= 4
        is_bot_home = rank_home >= 11
        is_bot_away = rank_away >= 11
        is_mid_home = rank_home >= 5
        is_mid_away = rank_away >= 5
        
        if (is_top_home and is_bot_away) or (is_top_away and is_bot_home):
            if tempo <= 5 and (sh_h + sh_a) >= 1:
                SINAIS.append({"tag": "🔥 Massacre (Top vs Bot)", "ordem": "Over 0.5 HT + Over 1.5 FT", "stats": f"Rank: {rank_home}x{rank_away}"})
        
        if 5 <= tempo <= 15:
            if is_top_home and (rh >= 2 or sh_h >= 3):
                SINAIS.append({"tag": "🦁 Favorito Pressão", "ordem": "Gol do Favorito (Home)", "stats": f"Pressão: {rh}"})
            if is_top_away and (ra >= 2 or sh_a >= 3):
                SINAIS.append({"tag": "🦁 Favorito Pressão", "ordem": "Gol do Favorito (Away)", "stats": f"Pressão: {ra}"})

        if is_top_home and is_top_away and tempo <= 7:
            if (sh_h + sh_a) >= 2 and (sog_h + sog_a) >= 1:
                SINAIS.append({"tag": "⚔️ Choque Líderes", "ordem": "Over 0.5 HT", "stats": f"Chutes: {sh_h+sh_a}"})
            if sog_h >= 1 and sog_a >= 1:
                SINAIS.append({"tag": "⚔️ Choque Insano", "ordem": "Over 0.5 HT + Over 1.5 FT", "stats": f"SOG: {sog_h}x{sog_a}"})

        if is_mid_home and is_mid_away:
            if tempo <= 7 and 2 <= (sh_h + sh_a) <= 3:
                SINAIS.append({"tag": "🥊 Briga de Rua", "ordem": "Over 0.5 HT", "stats": f"Chutes: {sh_h+sh_a}"})
            if tempo <= 10 and (sh_h + sh_a) == 0:
                SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": "Under 1.5 HT", "stats": "0 Chutes"})

    return SINAIS

def gerenciar_strikes(id_liga, pais, nome_liga):
    df = st.session_state['df_vip']
    hoje = get_time_br().strftime('%Y-%m-%d')
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
        st.toast(f"🚫 {nome_liga} Banida (Qualidade de Dados)")
    else:
        salvar_strike(id_liga, pais, nome_liga, novo_strike)
        st.toast(f"⚠️ {nome_liga} Strike 1/2 (Sem dados)")

# --- 7. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves PRO")
    with st.expander("⚙️ Configurações", expanded=True):
        API_KEY = st.text_input("Chave API:", type="password")
        TG_TOKEN = st.text_input("Token Telegram:", type="password")
        TG_CHAT = st.text_input("Chat IDs:")
        # SUGESTÃO: Mantenha em 120 ou 180 para segurança extra
        INTERVALO = st.slider("Ciclo (s):", 60, 300, 120) 
        c1, c2 = st.columns(2)
        if c1.button("🔄 Reenviar\nSinais"): reenviar_sinais(TG_TOKEN, TG_CHAT)
        if c2.button("🗑️ Limpar\nDados"):
            for f in FILES.values(): 
                if os.path.exists(f): os.remove(f)
            st.rerun()
            
    with st.expander("✅ Estratégias Ativas", expanded=True):
        st.markdown("""
        **Geral:**
        ✔️ Porteira Aberta (<30min)
        ✔️ Gol Relâmpago (5-15min)
        ✔️ Blitz (Pressão)
        ✔️ Janela de Ouro (70-75min)
        ✔️ **Múltiplas HT (0x0 Explosivo)**
        
        **Tabela (Premium):**
        ✔️ Massacre (Top vs Bot)
        ✔️ Favorito Pressão
        ✔️ Choque de Líderes
        ✔️ Briga de Rua (Meio)
        ✔️ Jogo Morno (Lay Over)
        """)
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 8. DASHBOARD ---
main = st.empty()

if ROBO_LIGADO:
    carregar_tudo()
    api_error = False
    
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10).json()
        if "errors" in res and res['errors']:
            if isinstance(res['errors'], dict) and 'rateLimit' in str(res['errors']): api_error = True
            elif isinstance(res['errors'], list) and res['errors']: api_error = True
            jogos_live = []
        else:
            jogos_live = res.get('response', [])
    except: 
        jogos_live = []
        api_error = True

    if not api_error: check_green_red_hibrido(jogos_live, TG_TOKEN, TG_CHAT, API_KEY)

    radar = []
    ids_black = st.session_state['df_black']['id'].values
    candidatos_multipla = []
    ids_no_radar = [] 

    for j in jogos_live:
        lid = str(j['league']['id'])
        if lid in ids_black: continue
        
        fid = j['fixture']['id']
        ids_no_radar.append(fid)
        
        tempo = j['fixture']['status']['elapsed'] or 0
        status_short = j['fixture']['status']['short']
        home = j['teams']['home']['name']
        away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"
        gh = j['goals']['home'] or 0
        ga = j['goals']['away'] or 0
        
        stats = []
        lista_sinais = []
        status_vis = "👁️"
        
        rank_h, rank_a = None, None
        tem_tabela = False
        if j['league']['id'] in LIGAS_TABELA:
            season = j['league']['season']
            ranking = buscar_ranking(API_KEY, j['league']['id'], season)
            rank_h = ranking.get(home)
            rank_a = ranking.get(away)
            if rank_h and rank_a: tem_tabela = True

        # --- BLOCO OTIMIZADO DE GESTÃO DE API ---
        
        # 1. Define intervalo inteligente baseado no momento do jogo
        tempo_espera = 180 # Padrão: 3 minutos (economiza muito)
        
        # Aumenta frequência na Janela de Ouro (Crítico: janela de apenas 5 min)
        if 69 <= tempo <= 76: 
            tempo_espera = 60 
        # Aumenta frequência no início (Gol Relâmpago)
        elif tempo <= 15: 
            tempo_espera = 90 
            
        ultimo_check = st.session_state['controle_stats'].get(fid, datetime.min)
        agora_dt = datetime.now()
        segundos_desde_ultimo = (agora_dt - ultimo_check).total_seconds()
        
        pode_buscar_api = segundos_desde_ultimo > tempo_espera
        
        # Chave para guardar a última estatística válida na memória
        chave_memoria_stats = f"stats_cache_{fid}"

        if deve_buscar_stats(tempo, gh, ga, status_short):
            if pode_buscar_api:
                try:
                    # BUSCA NOVA NA API
                    stats_req = requests.get("https://v3.football.api-sports.io/fixtures/statistics", headers={"x-apisports-key": API_KEY}, params={"fixture": fid}, timeout=5).json().get('response', [])
                    if stats_req:
                        stats = stats_req
                        # Atualiza timestamp e cache
                        st.session_state['controle_stats'][fid] = agora_dt
                        st.session_state[chave_memoria_stats] = stats
                    else:
                        stats = []
                except: 
                    stats = []
            else:
                # USA O CACHE (Não gasta API, mas mantém o robô "vendo" o jogo)
                stats = st.session_state.get(chave_memoria_stats, [])
        else:
            stats = [] 
            
        # -----------------------------------------

        if stats: # Só processa se tem dados (novos ou cacheados)
            lista_sinais = processar(j, stats, tempo, placar, rank_h, rank_a)
            salvar_safe_league(lid, j['league']['country'], j['league']['name'], True, tem_tabela)
            
            # MÚLTIPLAS HT
            if status_short == 'HT' and gh == 0 and ga == 0:
                try:
                    sh_h = next((x['value'] for x in stats[0]['statistics'] if x['type']=='Total Shots'), 0) or 0
                    sh_a = next((x['value'] for x in stats[1]['statistics'] if x['type']=='Total Shots'), 0) or 0
                    sog_h = next((x['value'] for x in stats[0]['statistics'] if x['type']=='Shots on Goal'), 0) or 0
                    sog_a = next((x['value'] for x in stats[1]['statistics'] if x['type']=='Shots on Goal'), 0) or 0
                    
                    if (sh_h + sh_a) > 12 and (sog_h + sog_a) > 6:
                        total_sog = sog_h + sog_a
                        if total_sog > 0:
                            if (sog_h / total_sog) >= 0.65: indicacao = f"Gol do {home}"
                            elif (sog_a / total_sog) >= 0.65: indicacao = f"Gol do {away}"
                            else: indicacao = "Over 0.5 FT"
                        else: indicacao = "Over 0.5 FT"
                        candidatos_multipla.append({
                            'fid': fid,
                            'jogo': f"{home} x {away}",
                            'stats': f"{sh_h+sh_a} Chutes ({sog_h+sog_a} Gol)",
                            'indica': indicacao
                        })
                except: pass
        else:
            status_vis = "💤"

        if not lista_sinais and not stats and tempo >= 45 and status_short != 'HT': 
            gerenciar_strikes(lid, j['league']['country'], j['league']['name'])
        
        if lista_sinais:
            status_vis = f"✅ {len(lista_sinais)} Sinais"
            for sinal in lista_sinais:
                id_unico = f"{fid}_{sinal['tag']}"
                if id_unico not in st.session_state['alertas_enviados']:
                    item = {"FID": fid, "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Placar_Sinal": placar, "Estrategia": sinal['tag'], "Resultado": "Pendente"}
                    
                    if adicionar_historico(item):
                        msg = f"<b>🚨 SINAL ENCONTRADO 🚨</b>\n\n🏆 <b>{j['league']['name']}</b>\n⚽ {home} 🆚 {away}\n⏰ <b>{tempo}' minutos</b> (Placar: {placar})\n\n🔥 <b>{sinal['tag'].upper()}</b>\n⚠️ <b>AÇÃO:</b> {sinal['ordem']}\n\n📊 <i>Dados: {sinal['stats']}</i>"
                        enviar_telegram(TG_TOKEN, TG_CHAT, msg)
                        st.session_state['alertas_enviados'].add(id_unico)
                        st.toast(f"Sinal: {sinal['tag']}")

        radar.append({"Liga": j['league']['name'], "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})

    if candidatos_multipla:
        novos_multipla = [c for c in candidatos_multipla if c['fid'] not in st.session_state['multiplas_enviadas']]
        if novos_multipla:
            msg_multi = "<b>🚀 OPORTUNIDADE DE MÚLTIPLA (HT) 🚀</b>\n<i>Jogos 0x0 no Intervalo com Alta Pressão</i>\n"
            for c in candidatos_multipla:
                msg_multi += f"\n⚽ <b>{c['jogo']}</b>\n📊 {c['stats']}\n🎯 <b>{c['indica']}</b>\n"
                st.session_state['multiplas_enviadas'].add(c['fid'])
            msg_multi += "\n⚠️ <i>Sugerimos combinar estes jogos!</i>"
            enviar_telegram(TG_TOKEN, TG_CHAT, msg_multi)
            st.toast("Múltipla Detectada!")

    # AGENDA FILTRADA (Só Futuros do Dia) - COM CACHE
    agenda = []
    if not api_error:
        hoje_br = get_time_br().strftime('%Y-%m-%d')
        prox = buscar_agenda_cached(API_KEY, hoje_br)
        agora = get_time_br()
        
        for p in prox:
            try:
                pid = p['fixture']['id']
                if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] in ['NS', 'TBD'] and pid not in ids_no_radar:
                    game_dt = datetime.fromisoformat(p['fixture']['date'])
                    if game_dt > agora:
                        agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
            except: pass

    if not radar and not agenda and not api_error:
        if not os.path.exists(FILES['report']) or open(FILES['report']).read() != get_time_br().strftime('%Y-%m-%d'):
            relatorio_final(TG_TOKEN, TG_CHAT)

    with main.container():
        if api_error: st.markdown('<div class="status-error">🚨 API LIMITADA - AGUARDE</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        hist_hoje = pd.DataFrame(st.session_state['historico_sinais'])
        t_hj, g_hj, r_hj, w_hj = calcular_stats(hist_hoje)

        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t_hj}</div><div class="metric-sub">{g_hj} Green | {r_hj} Red</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(radar)}</div><div class="metric-sub">Monitorando</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Ligas Seguras</div><div class="metric-value">{len(st.session_state["df_safe"])}</div><div class="metric-sub">Validadas</div></div>', unsafe_allow_html=True)
        
        st.write("")

        t1, t2, t3, t4, t5, t6, t7 = st.tabs([
            f"📡 Radar ({len(radar)})", 
            f"📅 Agenda ({len(agenda)})", 
            f"📜 Histórico ({len(hist_hoje)})", 
            "📈 Estatísticas", 
            f"🚫 Blacklist ({len(st.session_state['df_black'])})", 
            f"🛡️ Seguras ({len(st.session_state['df_safe'])})", 
            f"⚠️ Obs ({len(st.session_state['df_vip'])})"
        ])
        
        with t1:
            if radar: st.dataframe(pd.DataFrame(radar).astype(str), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        
        with t2:
            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos futuros hoje.")
        
        with t3:
            if not hist_hoje.empty: st.dataframe(hist_hoje.astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Vazio.")
        
        with t4:
            df_full = st.session_state['historico_full']
            if not df_full.empty:
                t_all, g_all, r_all, w_all = calcular_stats(df_full)
                st.markdown(f"**Geral:** {w_all:.1f}% ({g_all}G - {r_all}R)")
            else: st.caption("Sem dados.")
        
        with t5: st.dataframe(st.session_state['df_black'], use_container_width=True, hide_index=True)
        with t6: st.dataframe(st.session_state['df_safe'], use_container_width=True, hide_index=True)
        with t7: st.dataframe(st.session_state['df_vip'], use_container_width=True, hide_index=True)

    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
        time.sleep(1)
    st.rerun()

else:
    with main.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure na lateral.")
