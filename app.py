import streamlit as st
import pandas as pd
import requests
import time
import os
import threading
import matplotlib.pyplot as plt
import plotly.express as px
import io
from datetime import datetime, timedelta
import pytz
from streamlit_gsheets import GSheetsConnection

# --- 0. CONFIGURAÇÃO E CSS ---
st.set_page_config(page_title="Neves Analytics", layout="wide", page_icon="❄️")

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .main .block-container { max-width: 100%; padding: 1rem 1rem 5rem 1rem; }
    
    .metric-box { 
        background-color: #1A1C24; border: 1px solid #333; 
        border-radius: 8px; padding: 15px; text-align: center; 
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    .metric-title {font-size: 12px; color: #aaaaaa; text-transform: uppercase; margin-bottom: 5px;}
    .metric-value {font-size: 24px; font-weight: bold; color: #00FF00;}
    
    .status-active { background-color: #1F4025; color: #00FF00; border: 1px solid #00FF00; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 15px; font-weight: bold;}
    .status-error { background-color: #3B1010; color: #FF4B4B; border: 1px solid #FF4B4B; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 15px; font-weight: bold;}
    
    .stButton button {
        width: 100%; 
        white-space: normal !important; 
        height: auto !important;        
        min-height: 45px;              
        font-size: 14px !important; 
        font-weight: bold !important;
        padding: 5px 10px !important;
        line-height: 1.2 !important;
    }
    
    .footer-timer { 
        position: fixed; left: 0; bottom: 0; width: 100%; 
        background-color: #0E1117; color: #FFD700; 
        text-align: center; padding: 8px; font-size: 14px; 
        border-top: 1px solid #333; z-index: 9999; 
    }
    .stDataFrame { font-size: 13px; }
</style>
""", unsafe_allow_html=True)

# --- 1. CONEXÃO NUVEM ---
conn = st.connection("gsheets", type=GSheetsConnection)

COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo'] 
COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes']
LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]

# --- 2. UTILITÁRIOS ---
def get_time_br(): return datetime.now(pytz.timezone('America/Sao_Paulo'))
def clean_fid(x): 
    try: return str(int(float(x))) 
    except: return '0'

# --- 3. BANCO DE DADOS (NUVEM & RAM DISK) ---
def carregar_aba(nome_aba, colunas_esperadas):
    try:
        df = conn.read(worksheet=nome_aba, ttl=0)
        if df.empty or len(df.columns) < len(colunas_esperadas): return pd.DataFrame(columns=colunas_esperadas)
        return df.fillna("").astype(str)
    except: return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df_para_salvar):
    try: conn.update(worksheet=nome_aba, data=df_para_salvar); return True
    except: return False

def carregar_tudo():
    if 'df_black' not in st.session_state: st.session_state['df_black'] = carregar_aba("Blacklist", ['id', 'País', 'Liga'])
    if 'df_safe' not in st.session_state: st.session_state['df_safe'] = carregar_aba("Seguras", COLS_SAFE)
    if 'df_vip' not in st.session_state: st.session_state['df_vip'] = carregar_aba("Obs", COLS_OBS)
    
    if 'historico_full' not in st.session_state:
        df = carregar_aba("Historico", COLS_HIST)
        if not df.empty and 'Data' in df.columns:
            df['FID'] = df['FID'].apply(clean_fid)
            st.session_state['historico_full'] = df
            hoje = get_time_br().strftime('%Y-%m-%d')
            st.session_state['historico_sinais'] = df[df['Data'] == hoje].to_dict('records')[::-1]
        else:
            st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
            st.session_state['historico_sinais'] = []

def adicionar_historico(item):
    df_antigo = st.session_state.get('historico_full', pd.DataFrame(columns=COLS_HIST))
    df_novo = pd.DataFrame([item])
    df_final = pd.concat([df_novo, df_antigo], ignore_index=True)
    if salvar_aba("Historico", df_final):
        st.session_state['historico_full'] = df_final
        st.session_state['historico_sinais'].insert(0, item)
        return True
    return False

def atualizar_historico_ram_disk(lista_atualizada):
    df_hoje = pd.DataFrame(lista_atualizada)
    df_disk = st.session_state['historico_full']
    hoje = get_time_br().strftime('%Y-%m-%d')
    if not df_disk.empty: df_disk = df_disk[df_disk['Data'] != hoje]
    df_final = pd.concat([df_hoje, df_disk], ignore_index=True)
    if salvar_aba("Historico", df_final): st.session_state['historico_full'] = df_final

def salvar_blacklist(id_liga, pais, nome_liga):
    df = st.session_state['df_black']
    if str(id_liga) not in df['id'].values:
        novo = pd.DataFrame([{'id': str(id_liga), 'País': str(pais), 'Liga': str(nome_liga)}])
        final = pd.concat([df, novo], ignore_index=True)
        if salvar_aba("Blacklist", final): st.session_state['df_black'] = final

def salvar_safe_league(id_liga, pais, nome_liga, tem_stats, tem_tabela):
    id_str = str(id_liga); motivos = []
    if tem_stats: motivos.append("Chutes")
    if tem_tabela: motivos.append("Tabela")
    motivo_str = " + ".join(motivos) if motivos else "Validada"
    df = st.session_state['df_safe']
    if id_str in df['id'].values:
        idx = df[df['id'] == id_str].index[0]
        if df.at[idx, 'Motivo'] != motivo_str:
            df.at[idx, 'Motivo'] = motivo_str
            if salvar_aba("Seguras", df): st.session_state['df_safe'] = df
    else:
        novo = pd.DataFrame([{'id': id_str, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': motivo_str}])
        final = pd.concat([df, novo], ignore_index=True)
        if salvar_aba("Seguras", final): st.session_state['df_safe'] = final

def salvar_strike(id_liga, pais, nome_liga, strikes):
    df = st.session_state['df_vip']
    hoje = get_time_br().strftime('%Y-%m-%d')
    id_str = str(id_liga)
    if id_str in df['id'].values: df = df[df['id'] != id_str]
    novo = pd.DataFrame([{'id': id_str, 'País': str(pais), 'Liga': str(nome_liga), 'Data_Erro': hoje, 'Strikes': str(strikes)}])
    final = pd.concat([df, novo], ignore_index=True)
    if salvar_aba("Obs", final): st.session_state['df_vip'] = final

def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

# --- 4. INTELIGÊNCIA PREDIITIVA (PONDERADA + STREAK) ---
def buscar_inteligencia(estrategia, liga, jogo):
    """Calcula Média Ponderada + Analisa Sequência (Streak)"""
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "\n🔮 <b>Prob: Sem Histórico</b>"
    
    try:
        times = jogo.split(' x ')
        time_casa = times[0].split('(')[0].strip()
        time_visitante = times[1].split('(')[0].strip()
    except:
        return "\n🔮 <b>Prob: Erro Nome</b>"
    
    numerador = 0; denominador = 0; fontes = []

    # Histórico dos times
    f_casa = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_casa, na=False))]
    f_vis = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_visitante, na=False))]
    
    wr_casa = 0; tem_casa = False
    if len(f_casa) >= 3:
        wr_casa = (f_casa['Resultado'].str.contains('GREEN').sum() / len(f_casa)) * 100
        tem_casa = True

    wr_vis = 0; tem_vis = False
    if len(f_vis) >= 3:
        wr_vis = (f_vis['Resultado'].str.contains('GREEN').sum() / len(f_vis)) * 100
        tem_vis = True
    
    if tem_casa or tem_vis:
        divisao = 2 if (tem_casa and tem_vis) else 1
        media_times = (wr_casa + wr_vis) / divisao
        numerador += media_times * 5
        denominador += 5
        fontes.append("Time")

    # Histórico da Liga
    f_liga = df[(df['Estrategia'] == estrategia) & (df['Liga'] == liga)]
    if len(f_liga) >= 3:
        wr_liga = (f_liga['Resultado'].str.contains('GREEN').sum() / len(f_liga)) * 100
        numerador += wr_liga * 3
        denominador += 3
        fontes.append("Liga")
        
    # Histórico Geral
    f_geral = df[df['Estrategia'] == estrategia]
    if len(f_geral) >= 1:
        wr_geral = (f_geral['Resultado'].str.contains('GREEN').sum() / len(f_geral)) * 100
        numerador += wr_geral * 1
        denominador += 1
        
    if denominador == 0: return "\n🔮 <b>Prob: Calculando...</b>"
    prob_final = numerador / denominador
    str_fontes = "+".join(fontes) if fontes else "Geral"
    
    # --- CÁLCULO DE MOMENTUM (STREAK) ---
    # Junta os dois times e vê a sequência recente
    f_times = pd.concat([f_casa, f_vis]).sort_values(by='Data', ascending=False)
    streak_msg = ""
    
    if not f_times.empty:
        last_results = f_times['Resultado'].head(5).tolist()
        streak_count = 0
        tipo_streak = None
        for res in last_results:
            if "GREEN" in res:
                if tipo_streak == "RED": break
                tipo_streak = "GREEN"
                streak_count += 1
            elif "RED" in res:
                if tipo_streak == "GREEN": break
                tipo_streak = "RED"
                streak_count += 1
        
        if streak_count >= 2:
            if tipo_streak == "GREEN": streak_msg = f" | 🔥 Vem de {streak_count} Greens!"
            else: streak_msg = f" | ❄️ Vem de {streak_count} Reds..."

    emoji = "🔮"
    if prob_final >= 80: emoji = "🔥"
    elif prob_final <= 40: emoji = "⚠️"
        
    return f"\n{emoji} <b>Prob: {prob_final:.0f}% ({str_fontes}){streak_msg}</b>"

# --- 5. TELEGRAM E RELATÓRIOS ---
def _worker_telegram(token, chat_id, msg):
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=5)
    except: pass

def _worker_telegram_photo(token, chat_id, photo_buffer, caption):
    try:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        files = {'photo': photo_buffer}
        data = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'HTML'}
        requests.post(url, files=files, data=data, timeout=10)
    except Exception as e: print(f"Erro foto: {e}")

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        t = threading.Thread(target=_worker_telegram, args=(token, cid, msg))
        t.daemon = True; t.start()

def enviar_relatorio_bi(token, chat_ids):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    
    df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
    hoje = pd.to_datetime(get_time_br().date())
    
    # Filtros de Tempo (Dia, Semana, Mês, Total)
    mask_dia = df['Data'] == hoje
    mask_sem = df['Data'] >= (hoje - timedelta(days=7))
    mask_mes = df['Data'] >= (hoje - timedelta(days=30))
    
    def calc_metrics(d):
        g = d['Resultado'].str.contains('GREEN').sum()
        r = d['Resultado'].str.contains('RED').sum()
        tot = g + r
        wr = (g/tot * 100) if tot > 0 else 0
        return tot, g, r, wr

    t_d, g_d, r_d, w_d = calc_metrics(df[mask_dia])
    t_s, g_s, r_s, w_s = calc_metrics(df[mask_sem])
    t_m, g_m, r_m, w_m = calc_metrics(df[mask_mes])
    t_a, g_a, r_a, w_a = calc_metrics(df)

    # Gráfico (Tendência 30 Dias)
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(7, 4))
    colors = {'✅ GREEN': '#00FF00', '❌ RED': '#FF0000'}
    
    stats_strat = df[mask_mes][df[mask_mes]['Resultado'].isin(['✅ GREEN', '❌ RED'])]
    if not stats_strat.empty:
        counts = stats_strat.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)
        counts.plot(kind='bar', stacked=True, color=[colors.get(x, '#888') for x in counts.columns], ax=ax, width=0.6)
        ax.set_title(f'PERFORMANCE 30 DIAS (WR: {w_m:.1f}%)', color='white', fontsize=12, pad=15)
        ax.set_xlabel('')
        ax.tick_params(axis='x', rotation=45, labelsize=9, colors='#cccccc')
        ax.grid(axis='y', linestyle='--', alpha=0.2)
        ax.legend(title='', frameon=False, loc='upper right')
        for spine in ax.spines.values(): spine.set_visible(False)
        plt.tight_layout()
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight', facecolor='#0E1117')
        buf.seek(0)
        
        msg = f"""📊 <b>RELATÓRIO BI (ACUMULADO)</b>

📆 <b>HOJE</b>
🎯 {t_d} Sinais | ✅ {g_d} | ❌ {r_d}
💰 Assertividade: <b>{w_d:.1f}%</b>

📅 <b>7 DIAS</b>
🎯 {t_s} Sinais | ✅ {g_s} | ❌ {r_s}
💰 Assertividade: <b>{w_s:.1f}%</b>

🗓️ <b>30 DIAS</b>
🎯 {t_m} Sinais | ✅ {g_m} | ❌ {r_m}
💰 Assertividade: <b>{w_m:.1f}%</b>

♾️ <b>TOTAL GERAL</b>
🎯 {t_a} Sinais | ✅ {g_a} | ❌ {r_a}
💰 Assertividade: <b>{w_a:.1f}%</b>"""
        
        ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
        for cid in ids:
            buf.seek(0)
            _worker_telegram_photo(token, cid, buf, msg)
        plt.close(fig)

def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
    status = jogo_api['fixture']['status']['short']
    if "Múltipla" in sinal['Estrategia']: return False
    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: return False

    # --- JOGO MORNO (UNDER 1.5) ---
    if sinal['Estrategia'] == "❄️ Jogo Morno":
        if (gh + ga) >= 2:
            sinal['Resultado'] = '❌ RED'
            msg = f"❌ <b>RED | FUROU O UNDER</b>\n\n⚽ {sinal['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {sinal['Estrategia']}"
            enviar_telegram(token, chats, msg); return True
        if status == 'HT' and (gh + ga) <= 1:
            sinal['Resultado'] = '✅ GREEN'
            msg = f"✅ <b>GREEN CONFIRMADO!</b>\n\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📉 Placar HT: <b>{gh}x{ga}</b>\n🎯 {sinal['Estrategia']} (Bateu Under 1.5)"
            enviar_telegram(token, chats, msg); return True
        return False

    # --- GERAL (OVER GOLS) ---
    if (gh+ga) > (ph+pa):
        sinal['Resultado'] = '✅ GREEN'
        msg = f"✅ <b>GREEN CONFIRMADO!</b>\n\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📈 Placar Atual: <b>{gh}x{ga}</b>\n🎯 {sinal['Estrategia']}"
        enviar_telegram(token, chats, msg); return True
    if status in ['FT', 'AET', 'PEN', 'ABD']:
        sinal['Resultado'] = '❌ RED'
        msg = f"❌ <b>RED | ENCERRADO</b>\n\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {sinal['Estrategia']}"
        enviar_telegram(token, chats, msg); return True
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
        if fid > 0 and fid in ids_live: jogo_encontrado = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        elif fid > 0:
            try:
                res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                if res['response']: jogo_encontrado = res['response'][0]
            except: pass
        if jogo_encontrado:
            if processar_resultado(s, jogo_encontrado, token, chats): atualizou = True
    if atualizou: atualizar_historico_ram_disk(hist)

def reenviar_sinais(token, chats):
    hist = st.session_state['historico_sinais']
    if not hist: return st.toast("Sem sinais.")
    st.toast("Reenviando...")
    for s in reversed(hist):
        prob = buscar_inteligencia(s['Estrategia'], s['Liga'], s['Jogo'])
        msg = f"🔄 <b>REENVIO</b>\n\n🚨 {s['Estrategia']}\n⚽ {s['Jogo']}\n⚠️ Placar: {s.get('Placar_Sinal','?')}{prob}"
        enviar_telegram(token, chats, msg); time.sleep(0.5)

# --- 6. CORE ---
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'multiplas_enviadas' not in st.session_state: st.session_state['multiplas_enviadas'] = set()
if 'controle_stats' not in st.session_state: st.session_state['controle_stats'] = {} 
if 'api_usage' not in st.session_state: st.session_state['api_usage'] = {'used': 0, 'limit': 75000}
if 'data_api_usage' not in st.session_state: st.session_state['data_api_usage'] = datetime.now(pytz.utc).date()

carregar_tudo()

def verificar_reset_diario():
    hoje_utc = datetime.now(pytz.utc).date()
    if 'data_api_usage' not in st.session_state: st.session_state['data_api_usage'] = hoje_utc
    if st.session_state['data_api_usage'] != hoje_utc:
        st.session_state['api_usage']['used'] = 0
        st.session_state['data_api_usage'] = hoje_utc
        return True
    return False

def update_api_usage(headers):
    if not headers: return
    try:
        limit = int(headers.get('x-ratelimit-requests-limit', 75000))
        remaining = int(headers.get('x-ratelimit-requests-remaining', 0))
        used = limit - remaining
        st.session_state['api_usage'] = {'used': used, 'limit': limit}
        st.session_state['data_api_usage'] = datetime.now(pytz.utc).date()
    except: pass

@st.cache_data(ttl=86400)
def buscar_ranking(api_key, league_id, season):
    try:
        url = "https://v3.football.api-sports.io/standings"
        params = {"league": league_id, "season": season}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        ranking = {}
        if res.get('response'):
            for team in res['response'][0]['league']['standings'][0]: ranking[team['team']['name']] = team['rank']
        return ranking
    except: return {}

@st.cache_data(ttl=3600) 
def buscar_agenda_cached(api_key, date_str):
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        return requests.get(url, headers={"x-apisports-key": api_key}, params={"date": date_str, "timezone": "America/Sao_Paulo"}).json().get('response', [])
    except: return []

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

def deve_buscar_stats(tempo, gh, ga, status):
    if 5 <= tempo <= 15: return True
    if tempo <= 30 and (gh + ga) >= 2: return True
    if 70 <= tempo <= 75 and abs(gh - ga) <= 1: return True
    if tempo <= 60 and abs(gh - ga) <= 1: return True
    if status == 'HT' and gh == 0 and ga == 0: return True
    return False

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): 
            v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        
        # --- ESTATÍSTICA DE PONTARIA ---
        tot_chutes = sh_h + sh_a
        tot_gol = sog_h + sog_a
        txt_stats = f"{tot_chutes} Chutes (🎯 {tot_gol} no Gol)"
        
    except: return []
    fid = j['fixture']['id']; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
    rh, ra = momentum(fid, sog_h, sog_a)
    SINAIS = []
    
    if tempo <= 30 and (gh+ga) >= 2: 
        SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": "🔥 Over Gols", "stats": f"Placar: {gh}x{ga}"})
    
    if 5 <= tempo <= 15 and (sog_h+sog_a) >= 1 and (gh + ga) == 0: 
        SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": "Over 0.5 HT", "stats": txt_stats})
    
    if 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(gh-ga) <= 1: 
        SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": "Over Gols", "stats": txt_stats})
        
    if tempo <= 60:
        if gh <= ga and (rh >= 2 or sh_h >= 8): SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": "Over Gols", "stats": f"Pressão: {rh}"})
        if ga <= gh and (ra >= 2 or sh_a >= 8): SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": "Over Gols", "stats": f"Pressão: {ra}"})
        
    if rank_home and rank_away:
        is_top_home = rank_home <= 4; is_top_away = rank_away <= 4
        is_bot_home = rank_home >= 11; is_bot_away = rank_away >= 11
        is_mid_home = rank_home >= 5; is_mid_away = rank_away >= 5
        
        if (is_top_home and is_bot_away) or (is_top_away and is_bot_home):
            if tempo <= 5 and (sh_h + sh_a) >= 1: 
                SINAIS.append({"tag": "🔥 Massacre", "ordem": "Over 0.5 HT", "stats": f"Rank: {rank_home}x{rank_away}"})
        
        if 5 <= tempo <= 15:
            if is_top_home and (rh >= 2 or sh_h >= 3): SINAIS.append({"tag": "🦁 Favorito", "ordem": "Over Gols", "stats": f"Pressão: {rh}"})
            if is_top_away and (ra >= 2 or sh_a >= 3): SINAIS.append({"tag": "🦁 Favorito", "ordem": "Over Gols", "stats": f"Pressão: {ra}"})
        
        if is_top_home and is_top_away and tempo <= 7:
            if (sh_h + sh_a) >= 2 and (sog_h + sog_a) >= 1: 
                SINAIS.append({"tag": "⚔️ Choque Líderes", "ordem": "Over 0.5 HT", "stats": txt_stats})
        
        if is_mid_home and is_mid_away:
            if tempo <= 7 and 2 <= (sh_h + sh_a) <= 3: 
                SINAIS.append({"tag": "🥊 Briga de Rua", "ordem": "Over 0.5 HT", "stats": txt_stats})
            
            if tempo <= 15 and (sh_h + sh_a) == 0: 
                SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": "Under 1.5 HT", "stats": "0 Chutes"})
            
    return SINAIS

def gerenciar_strikes(id_liga, pais, nome_liga):
    df = st.session_state.get('df_vip', pd.DataFrame())
    hoje = get_time_br().strftime('%Y-%m-%d')
    id_str = str(id_liga); strikes = 0; data_antiga = ""
    if not df.empty and id_str in df['id'].values:
        row = df[df['id'] == id_str].iloc[0]
        strikes = int(row['Strikes']); data_antiga = row['Data_Erro']
    if data_antiga == hoje: return
    novo_strike = strikes + 1
    salvar_strike(id_liga, pais, nome_liga, novo_strike)
    st.toast(f"⚠️ {nome_liga} Strike {novo_strike}")
    if novo_strike >= 2: salvar_blacklist(id_liga, pais, nome_liga); st.toast(f"🚫 {nome_liga} Banida")

# --- 7. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics")
    with st.expander("⚙️ Configurações", expanded=True):
        API_KEY = st.text_input("Chave API:", type="password")
        TG_TOKEN = st.text_input("Token Telegram:", type="password")
        TG_CHAT = st.text_input("Chat IDs:")
        INTERVALO = st.slider("Ciclo (s):", 60, 300, 60) 
        c1, c2 = st.columns(2)
        if c1.button("🔄 Reenviar"): reenviar_sinais(TG_TOKEN, TG_CHAT)
        if c2.button("🧹 Cache"): st.cache_data.clear()
        
        st.write("---")
        if st.button("📊 Enviar Relatório BI"):
            enviar_relatorio_bi(TG_TOKEN, TG_CHAT)
            st.toast("Relatório Enviado!")
            
    verificar_reset_diario()
    with st.expander("📶 Consumo API", expanded=False):
        u = st.session_state['api_usage']
        perc = min(u['used'] / u['limit'], 1.0) if u['limit'] > 0 else 0
        st.progress(perc)
        st.caption(f"Utilizado: **{u['used']}** / {u['limit']}")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 8. DASHBOARD ---
if ROBO_LIGADO:
    carregar_tudo()

    # --- FILTRO DE LIMPEZA DIÁRIA ---
    hoje_real = get_time_br().strftime('%Y-%m-%d')
    st.session_state['historico_sinais'] = [
        s for s in st.session_state['historico_sinais'] 
        if s['Data'] == hoje_real
    ]

    api_error = False
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        resp = requests.get(url, headers={"x-apisports-key": API_KEY}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
        update_api_usage(resp.headers) 
        res = resp.json()
        if "errors" in res and res['errors']: api_error = True; jogos_live = []
        else: jogos_live = res.get('response', [])
    except: jogos_live = []; api_error = True

    if not api_error: check_green_red_hibrido(jogos_live, TG_TOKEN, TG_CHAT, API_KEY)

    radar = []; ids_black = st.session_state['df_black']['id'].values
    candidatos_multipla = []; ids_no_radar = [] 

    for j in jogos_live:
        lid = str(j['league']['id']); fid = j['fixture']['id']
        if lid in ids_black: continue
        ids_no_radar.append(fid)
        tempo = j['fixture']['status']['elapsed'] or 0
        status_short = j['fixture']['status']['short']
        home = j['teams']['home']['name']; away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"
        gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
        stats = []; lista_sinais = []; status_vis = "👁️"
        
        rank_h, rank_a = None, None; tem_tabela = False
        if j['league']['id'] in LIGAS_TABELA:
            season = j['league']['season']
            ranking = buscar_ranking(API_KEY, j['league']['id'], season)
            rank_h = ranking.get(home); rank_a = ranking.get(away)
            if rank_h and rank_a: tem_tabela = True
            
            # --- RANKING NO NOME ---
            if rank_h: home = f"{home} ({rank_h}º)"
            if rank_a: away = f"{away} ({rank_a}º)"

        tempo_espera = 180 
        if 69 <= tempo <= 76: tempo_espera = 60 
        elif tempo <= 15: tempo_espera = 90 
        ultimo_check = st.session_state['controle_stats'].get(fid, datetime.min)
        agora_dt = datetime.now()
        pode_buscar_api = (agora_dt - ultimo_check).total_seconds() > tempo_espera
        chave_memoria_stats = f"stats_cache_{fid}"

        if deve_buscar_stats(tempo, gh, ga, status_short):
            if pode_buscar_api:
                try:
                    resp_stats = requests.get("https://v3.football.api-sports.io/fixtures/statistics", headers={"x-apisports-key": API_KEY}, params={"fixture": fid}, timeout=5)
                    update_api_usage(resp_stats.headers)
                    stats_req = resp_stats.json().get('response', [])
                    if stats_req:
                        stats = stats_req
                        st.session_state['controle_stats'][fid] = agora_dt
                        st.session_state[chave_memoria_stats] = stats
                    else: stats = []
                except: stats = []
            else: stats = st.session_state.get(chave_memoria_stats, [])
        else: stats = [] 

        if stats:
            lista_sinais = processar(j, stats, tempo, placar, rank_h, rank_a)
            salvar_safe_league(lid, j['league']['country'], j['league']['name'], True, tem_tabela)
            if status_short == 'HT' and gh == 0 and ga == 0:
                try:
                    sh_h = next((x['value'] for x in stats[0]['statistics'] if x['type']=='Total Shots'), 0) or 0
                    sh_a = next((x['value'] for x in stats[1]['statistics'] if x['type']=='Total Shots'), 0) or 0
                    sog_h = next((x['value'] for x in stats[0]['statistics'] if x['type']=='Shots on Goal'), 0) or 0
                    sog_a = next((x['value'] for x in stats[1]['statistics'] if x['type']=='Shots on Goal'), 0) or 0
                    if (sh_h + sh_a) > 12 and (sog_h + sog_a) > 6:
                        candidatos_multipla.append({'fid': fid, 'jogo': f"{home} x {away}", 'stats': f"{sh_h+sh_a} Chutes", 'indica': "Over 0.5 FT"})
                except: pass
        else: status_vis = "💤"

        if not lista_sinais and not stats and tempo >= 45 and status_short != 'HT': gerenciar_strikes(lid, j['league']['country'], j['league']['name'])
        
        if lista_sinais:
            status_vis = f"✅ {len(lista_sinais)} Sinais"
            for sinal in lista_sinais:
                id_unico = f"{fid}_{sinal['tag']}"
                if id_unico not in st.session_state['alertas_enviados']:
                    item = {"FID": fid, "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Placar_Sinal": placar, "Estrategia": sinal['tag'], "Resultado": "Pendente"}
                    if adicionar_historico(item):
                        # --- INTELIGÊNCIA PONDERADA ---
                        prob_msg = buscar_inteligencia(sinal['tag'], j['league']['name'], f"{home} x {away}")
                        
                        msg = f"<b>🚨 SINAL ENCONTRADO 🚨</b>\n\n🏆 <b>{j['league']['name']}</b>\n⚽ {home} 🆚 {away}\n⏰ <b>{tempo}' minutos</b> (Placar: {placar})\n\n🔥 <b>{sinal['tag'].upper()}</b>\n⚠️ <b>AÇÃO:</b> {sinal['ordem']}\n\n📊 <i>Dados: {sinal['stats']}</i>{prob_msg}"
                        enviar_telegram(TG_TOKEN, TG_CHAT, msg)
                        st.session_state['alertas_enviados'].add(id_unico)
                        st.toast(f"Sinal: {sinal['tag']}")
        radar.append({"Liga": j['league']['name'], "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})

    if candidatos_multipla:
        novos = [c for c in candidatos_multipla if c['fid'] not in st.session_state['multiplas_enviadas']]
        if novos:
            msg = "<b>🚀 OPORTUNIDADE DE MÚLTIPLA (HT) 🚀</b>\n"
            for c in novos: msg += f"\n⚽ {c['jogo']} ({c['stats']})"; st.session_state['multiplas_enviadas'].add(c['fid'])
            enviar_telegram(TG_TOKEN, TG_CHAT, msg)

    agenda = []
    if not api_error:
        hoje_br = get_time_br().strftime('%Y-%m-%d')
        prox = buscar_agenda_cached(API_KEY, hoje_br)
        agora = get_time_br()
        for p in prox:
            try:
                pid = p['fixture']['id']
                if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] in ['NS', 'TBD'] and pid not in ids_no_radar:
                    if datetime.fromisoformat(p['fixture']['date']) > agora:
                        agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
            except: pass

    # RELATORIO AUTOMÁTICO 23:59
    hora_atual = get_time_br().hour
    if hora_atual >= 23:
        arquivo_hoje = get_time_br().strftime('%Y-%m-%d')
        chave_relatorio = f"relatorio_enviado_{arquivo_hoje}"
        if chave_relatorio not in st.session_state:
            enviar_relatorio_bi(TG_TOKEN, TG_CHAT)
            st.session_state[chave_relatorio] = True

    # DISPLAY
    dashboard_placeholder = st.empty()
    with dashboard_placeholder.container():
        if api_error: st.markdown('<div class="status-error">🚨 API LIMITADA - AGUARDE</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        hist_hoje = pd.DataFrame(st.session_state['historico_sinais'])
        t_hj, g_hj, r_hj, w_hj = calcular_stats(hist_hoje)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t_hj}</div><div class="metric-sub">{g_hj} Green | {r_hj} Red</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(radar)}</div><div class="metric-sub">Monitorando</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Ligas Seguras</div><div class="metric-value">{len(st.session_state["df_safe"])}</div><div class="metric-sub">Validadas</div></div>', unsafe_allow_html=True)
        
        st.write("")
        abas = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", f"📜 Histórico ({len(hist_hoje)})", "📈 BI & Analytics", f"🚫 Blacklist ({len(st.session_state['df_black'])})", f"🛡️ Seguras ({len(st.session_state['df_safe'])})", f"⚠️ Obs ({len(st.session_state.get('df_vip', []))})"])
        
        with abas[0]: 
            if radar: st.dataframe(pd.DataFrame(radar).astype(str), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        with abas[1]: 
            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos futuros hoje.")
        with abas[2]: 
            if not hist_hoje.empty: st.dataframe(hist_hoje.astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Vazio.")
        
        # --- BI COM PLOTLY ---
        with abas[3]: 
            st.markdown("### 📊 Inteligência de Mercado")
            df_bi = st.session_state.get('historico_full', pd.DataFrame())
            if df_bi.empty: st.warning("Sem dados históricos na nuvem.")
            else:
                dias = st.selectbox("📅 Período", ["Tudo", "Hoje", "7 Dias", "30 Dias"])
                df_bi['Data'] = pd.to_datetime(df_bi['Data'], errors='coerce')
                hoje_bi = pd.to_datetime(get_time_br().date())
                if dias == "Hoje": df_show = df_bi[df_bi['Data'] == hoje_bi]
                elif dias == "7 Dias": df_show = df_bi[df_bi['Data'] >= (hoje_bi - timedelta(days=7))]
                elif dias == "30 Dias": df_show = df_bi[df_bi['Data'] >= (hoje_bi - timedelta(days=30))]
                else: df_show = df_bi
                
                if not df_show.empty:
                    greens_bi = df_show['Resultado'].str.contains('GREEN').sum()
                    reds_bi = df_show['Resultado'].str.contains('RED').sum()
                    tot_bi = greens_bi + reds_bi
                    wr_bi = (greens_bi / tot_bi * 100) if tot_bi > 0 else 0
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Sinais", tot_bi); m2.metric("Greens", greens_bi); m3.metric("Reds", reds_bi); m4.metric("Assertividade", f"{wr_bi:.1f}%")
                    st.divider()
                    
                    stats_strat = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                    if not stats_strat.empty:
                        counts = stats_strat.groupby(['Estrategia', 'Resultado']).size().reset_index(name='Qtd')
                        fig = px.bar(counts, x='Estrategia', y='Qtd', color='Resultado', 
                                     color_discrete_map={'✅ GREEN': '#00FF00', '❌ RED': '#FF0000'},
                                     title="Performance por Estratégia", text='Qtd')
                        fig.update_layout(xaxis_title=None, yaxis_title=None, template="plotly_dark")
                        st.plotly_chart(fig, use_container_width=True)

                    cb1, cb2 = st.columns(2)
                    with cb1:
                        st.caption("🏆 Melhores Ligas")
                        stats = df_show.groupby('Liga')['Resultado'].apply(lambda x: x.str.contains('GREEN').sum() / len(x) * 100).reset_index(name='Winrate')
                        counts = df_show['Liga'].value_counts().reset_index(name='Qtd')
                        final = stats.merge(counts, left_on='Liga', right_on='Liga')
                        st.dataframe(final[final['Qtd'] >= 2].sort_values('Winrate', ascending=False).head(5), hide_index=True, use_container_width=True)
                    with cb2:
                        st.caption("⚡ Top Estratégias")
                        stats_s = df_show.groupby('Estrategia')['Resultado'].apply(lambda x: x.str.contains('GREEN').sum() / len(x) * 100).reset_index(name='Winrate')
                        st.dataframe(stats_s.sort_values('Winrate', ascending=False), hide_index=True, use_container_width=True)
        
        with abas[4]: st.dataframe(st.session_state['df_black'], use_container_width=True, hide_index=True)
        with abas[5]: st.dataframe(st.session_state['df_safe'], use_container_width=True, hide_index=True)
        with abas[6]: st.dataframe(st.session_state.get('df_vip', pd.DataFrame()), use_container_width=True, hide_index=True)

    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
        time.sleep(1)
    st.rerun()

else:
    st.title("❄️ Neves Analytics")
    st.info("💡 Robô em espera. Configure na lateral.")
