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

COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo'] 
COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes']
LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]

# --- 2. UTILITÁRIOS ---
def get_time_br(): return datetime.now(pytz.timezone('America/Sao_Paulo'))
def clean_fid(x): 
    try: return str(int(float(x))) 
    except: return '0'

# --- FUNÇÃO TRATOR: NORMALIZAÇÃO DE ID ---
def normalizar_id(val):
    """Garante que qualquer ID (78, 78.0, ' 78 ') vire string limpa '78'"""
    try:
        s_val = str(val).strip()
        if not s_val or s_val.lower() == 'nan': return ""
        # Converte para float depois int para remover decimais (.0)
        return str(int(float(s_val)))
    except:
        return str(val).strip()

# --- 3. BANCO DE DADOS (NUVEM & RAM DISK) ---
def carregar_aba(nome_aba, colunas_esperadas):
    try:
        df = conn.read(worksheet=nome_aba, ttl=0)
        if not df.empty:
            for col in colunas_esperadas:
                if col not in df.columns:
                    df[col] = ""
        
        if df.empty or len(df.columns) < len(colunas_esperadas): return pd.DataFrame(columns=colunas_esperadas)
        return df.fillna("").astype(str)
    except: return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df_para_salvar):
    try: conn.update(worksheet=nome_aba, data=df_para_salvar); return True
    except: return False

def carregar_tudo():
    # --- CARREGA E NORMALIZA LISTAS DE REFERÊNCIA ---
    if 'df_black' not in st.session_state: 
        df = carregar_aba("Blacklist", ['id', 'País', 'Liga'])
        if not df.empty: df['id'] = df['id'].apply(normalizar_id)
        st.session_state['df_black'] = df
    else:
        st.session_state['df_black']['id'] = st.session_state['df_black']['id'].apply(normalizar_id)

    if 'df_safe' not in st.session_state: 
        df = carregar_aba("Seguras", COLS_SAFE)
        if not df.empty: df['id'] = df['id'].apply(normalizar_id)
        st.session_state['df_safe'] = df
    else:
        st.session_state['df_safe']['id'] = st.session_state['df_safe']['id'].apply(normalizar_id)

    if 'df_vip' not in st.session_state: 
        df = carregar_aba("Obs", COLS_OBS)
        if not df.empty: df['id'] = df['id'].apply(normalizar_id)
        st.session_state['df_vip'] = df
    else:
        st.session_state['df_vip']['id'] = st.session_state['df_vip']['id'].apply(normalizar_id)
    
    # --- CARREGA HISTÓRICO ---
    if 'historico_full' not in st.session_state:
        df = carregar_aba("Historico", COLS_HIST)
        if not df.empty and 'Data' in df.columns:
            df['FID'] = df['FID'].apply(clean_fid)
            try:
                df['Data'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
                df = df.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
            except: pass
            
            st.session_state['historico_full'] = df
            
            hoje = get_time_br().strftime('%Y-%m-%d')
            st.session_state['historico_sinais'] = df[df['Data'] == hoje].to_dict('records')[::-1]
            
            if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
            df_hoje = df[df['Data'] == hoje]
            for _, row in df_hoje.iterrows():
                id_blindagem = f"{row['FID']}_{row['Estrategia']}"
                st.session_state['alertas_enviados'].add(id_blindagem)
        else:
            st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
            st.session_state['historico_sinais'] = []

def adicionar_historico(item):
    df_antigo = st.session_state.get('historico_full', pd.DataFrame(columns=COLS_HIST))
    df_novo = pd.DataFrame([item])
    df_final = pd.concat([df_novo, df_antigo], ignore_index=True)
    df_final = df_final.drop_duplicates(subset=['FID', 'Estrategia'], keep='first')
    if salvar_aba("Historico", df_final):
        st.session_state['historico_full'] = df_final
        st.session_state['historico_sinais'].insert(0, item)
        return True
    return False

def atualizar_historico_ram_disk(lista_atualizada):
    df_hoje = pd.DataFrame(lista_atualizada)
    df_disk = st.session_state['historico_full']
    hoje = get_time_br().strftime('%Y-%m-%d')
    if not df_disk.empty and 'Data' in df_disk.columns:
         df_disk['Data'] = df_disk['Data'].astype(str).str.replace(' 00:00:00', '', regex=False)
         df_disk = df_disk[df_disk['Data'] != hoje]
    df_final = pd.concat([df_hoje, df_disk], ignore_index=True)
    df_final = df_final.drop_duplicates(subset=['FID', 'Estrategia'], keep='first')
    if salvar_aba("Historico", df_final): st.session_state['historico_full'] = df_final

def salvar_blacklist(id_liga, pais, nome_liga):
    df = st.session_state['df_black']
    id_norm = normalizar_id(id_liga)
    if id_norm not in df['id'].values:
        novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga)}])
        final = pd.concat([df, novo], ignore_index=True)
        if salvar_aba("Blacklist", final): st.session_state['df_black'] = final

def salvar_safe_league(id_liga, pais, nome_liga, tem_stats, tem_tabela):
    id_norm = normalizar_id(id_liga)
    motivos = []
    if tem_stats: motivos.append("Chutes")
    if tem_tabela: motivos.append("Tabela")
    motivo_str = " + ".join(motivos) if motivos else "Validada"
    
    df = st.session_state['df_safe']
    if id_norm in df['id'].values:
        idx = df[df['id'] == id_norm].index[0]
        if df.at[idx, 'Motivo'] != motivo_str:
            df.at[idx, 'Motivo'] = motivo_str
            if salvar_aba("Seguras", df): st.session_state['df_safe'] = df
    else:
        novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': motivo_str}])
        final = pd.concat([df, novo], ignore_index=True)
        if salvar_aba("Seguras", final): st.session_state['df_safe'] = final

def salvar_strike(id_liga, pais, nome_liga, strikes):
    df = st.session_state['df_vip']
    hoje = get_time_br().strftime('%Y-%m-%d')
    id_norm = normalizar_id(id_liga)
    if id_norm in df['id'].values: df = df[df['id'] != id_norm]
    novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Data_Erro': hoje, 'Strikes': str(strikes)}])
    final = pd.concat([df, novo], ignore_index=True)
    if salvar_aba("Obs", final): st.session_state['df_vip'] = final

def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    df_raw = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

# --- 4. INTELIGÊNCIA PREDIITIVA ---
def buscar_inteligencia(estrategia, liga, jogo):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "\n🔮 <b>Prob: Sem Histórico</b>"
    try:
        times = jogo.split(' x ')
        time_casa = times[0].split('(')[0].strip()
        time_visitante = times[1].split('(')[0].strip()
    except: return "\n🔮 <b>Prob: Erro Nome</b>"
    
    numerador = 0; denominador = 0; fontes = []
    f_casa = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_casa, na=False))]
    f_vis = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_visitante, na=False))]
    
    if len(f_casa) >= 3 or len(f_vis) >= 3:
        wr_c = (f_casa['Resultado'].str.contains('GREEN').sum()/len(f_casa)*100) if len(f_casa)>=3 else 0
        wr_v = (f_vis['Resultado'].str.contains('GREEN').sum()/len(f_vis)*100) if len(f_vis)>=3 else 0
        div = 2 if (len(f_casa)>=3 and len(f_vis)>=3) else 1
        numerador += ((wr_c + wr_v)/div) * 5; denominador += 5; fontes.append("Time")

    f_liga = df[(df['Estrategia'] == estrategia) & (df['Liga'] == liga)]
    if len(f_liga) >= 3:
        wr_l = (f_liga['Resultado'].str.contains('GREEN').sum()/len(f_liga)*100)
        numerador += wr_l * 3; denominador += 3; fontes.append("Liga")
        
    f_geral = df[df['Estrategia'] == estrategia]
    if len(f_geral) >= 1:
        wr_g = (f_geral['Resultado'].str.contains('GREEN').sum()/len(f_geral)*100)
        numerador += wr_g * 1; denominador += 1
        
    if denominador == 0: return "\n🔮 <b>Prob: Calculando...</b>"
    prob_final = numerador / denominador
    str_fontes = "+".join(fontes) if fontes else "Geral"
    
    f_times = pd.concat([f_casa, f_vis])
    try:
        f_times['Data_Temp'] = pd.to_datetime(f_times['Data'], errors='coerce')
        f_times = f_times.sort_values(by='Data_Temp', ascending=False)
    except: pass

    streak_msg = ""
    if not f_times.empty:
        last_results = f_times['Resultado'].head(5).tolist()
        streak_count = 0; tipo_streak = None
        for res in last_results:
            if "GREEN" in res:
                if tipo_streak == "RED": break
                tipo_streak = "GREEN"; streak_count += 1
            elif "RED" in res:
                if tipo_streak == "GREEN": break
                tipo_streak = "RED"; streak_count += 1
        if streak_count >= 2:
            streak_msg = f" | {'🔥' if tipo_streak == 'GREEN' else '❄️'} Vem de {streak_count} {tipo_streak.title()}s!"

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

# --- RADAR MATINAL ---
def verificar_alerta_matinal(token, chat_ids, api_key):
    agora = get_time_br()
    hoje_str = agora.strftime('%Y-%m-%d')
    chave = f'alerta_matinal_{hoje_str}'
    if chave in st.session_state: return 
    if not (8 <= agora.hour < 12): return 

    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    
    stats_ids = {} 
    df_green = df[df['Resultado'].str.contains('GREEN', na=False)]
    for index, row in df_green.iterrows():
        try:
            id_h = str(row.get('HomeID', '')).strip()
            id_a = str(row.get('AwayID', '')).strip()
            nomes = row['Jogo'].split(' x ')
            nome_h = nomes[0].split('(')[0].strip(); nome_a = nomes[1].split('(')[0].strip()
            
            k_h = id_h if id_h and id_h != 'nan' else nome_h
            if k_h not in stats_ids: stats_ids[k_h] = {'greens': 0, 'nome': nome_h}
            stats_ids[k_h]['greens'] += 1
            
            k_a = id_a if id_a and id_a != 'nan' else nome_a
            if k_a not in stats_ids: stats_ids[k_a] = {'greens': 0, 'nome': nome_a}
            stats_ids[k_a]['greens'] += 1
        except: pass
    if not stats_ids: return
    
    top = sorted(stats_ids.items(), key=lambda x: x[1]['greens'], reverse=True)[:10]
    ids_top = [x[0] for x in top]
    
    jogos = buscar_agenda_cached(api_key, hoje_str)
    if not jogos: return
    
    if 'alvos_do_dia' not in st.session_state: st.session_state['alvos_do_dia'] = {}
    matches = []
    for j in jogos:
        try:
            t1 = j['teams']['home']['name']; t1_id = str(j['teams']['home']['id'])
            t2 = j['teams']['away']['name']; t2_id = str(j['teams']['away']['id'])
            
            foco = None; fid_foco = None
            if t1_id in ids_top: foco = t1; fid_foco = t1_id
            elif t1 in ids_top: foco = t1; fid_foco = t1
            elif t2_id in ids_top: foco = t2; fid_foco = t2_id
            elif t2 in ids_top: foco = t2; fid_foco = t2
            
            if foco:
                df_t = df[(df['HomeID']==fid_foco)|(df['AwayID']==fid_foco)] if fid_foco.isdigit() else df[df['Jogo'].str.contains(foco, na=False)]
                m_strat = "Geral"; m_wr = 0
                if not df_t.empty:
                    for n, d in df_t.groupby('Estrategia'):
                        if len(d)>=2:
                            wr = (d['Resultado'].str.contains('GREEN').sum()/len(d))*100
                            if wr > m_wr: m_wr = wr; m_strat = n
                
                txt = f"🔥 <b>Oportunidade Sniper:</b> {foco} tem <b>{m_wr:.0f}%</b> na <b>{m_strat}</b>" if m_wr > 60 else f"💰 <b>Volume:</b> {foco} é máquina de Greens!"
                if m_wr > 60: st.session_state['alvos_do_dia'][foco] = m_strat
                matches.append(f"⏰ {j['fixture']['date'][11:16]} | {j['league']['country']} {j['league']['name']}\n⚽ {t1} 🆚 {t2}\n{txt}")
        except: pass
            
    if matches:
        msg = "🌅 <b>BOM DIA! RADAR DE OPORTUNIDADES</b>\n\n" + "\n\n".join(matches) + "\n\n⚠️ <i>Dica: Se o robô mandar o sinal sugerido acima, a chance de Green é estatisticamente maior!</i> 🚀"
        enviar_telegram(token, chat_ids, msg)
    st.session_state[chave] = True 

def enviar_relatorio_bi(token, chat_ids):
    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return
    try:
        df['Data_DT'] = pd.to_datetime(df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip(), errors='coerce')
        df = df.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    except: return
    
    hoje = pd.to_datetime(get_time_br().date())
    m_d = df['Data_DT'] == hoje
    m_s = df['Data_DT'] >= (hoje - timedelta(days=7))
    m_m = df['Data_DT'] >= (hoje - timedelta(days=30))
    
    def cm(d):
        g = d['Resultado'].str.contains('GREEN').sum(); r = d['Resultado'].str.contains('RED').sum()
        tot = g+r; wr = (g/tot*100) if tot>0 else 0
        return tot, g, r, wr

    t_d, g_d, r_d, w_d = cm(df[m_d]); t_s, g_s, r_s, w_s = cm(df[m_s])
    t_m, g_m, r_m, w_m = cm(df[m_m]); t_a, g_a, r_a, w_a = cm(df)

    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(7, 4))
    stats = df[m_m][df[m_m]['Resultado'].isin(['✅ GREEN', '❌ RED'])]
    if not stats.empty:
        c = stats.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)
        c.plot(kind='bar', stacked=True, color=['#00FF00', '#FF0000'], ax=ax, width=0.6)
        ax.set_title(f'PERFORMANCE 30 DIAS (WR: {w_m:.1f}%)', color='white')
        ax.set_xlabel(''); ax.tick_params(axis='x', rotation=45, colors='#ccc')
        ax.legend(title='', frameon=False)
        plt.tight_layout()
        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=100, facecolor='#0E1117'); buf.seek(0)
        
        msg = f"📊 <b>RELATÓRIO BI</b>\n\n📆 <b>HOJE:</b> {t_d} (WR: {w_d:.1f}%)\n📅 <b>7 DIAS:</b> {t_s} (WR: {w_s:.1f}%)\n🗓️ <b>30 DIAS:</b> {t_m} (WR: {w_m:.1f}%)\n♾️ <b>TOTAL:</b> {t_a} (WR: {w_a:.1f}%)"
        ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
        for cid in ids:
            buf.seek(0); _worker_telegram_photo(token, cid, buf, msg)
        plt.close(fig)

def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
    st_short = jogo_api['fixture']['status']['short']
    if "Múltipla" in sinal['Estrategia']: return False
    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: return False

    if sinal['Estrategia'] == "❄️ Jogo Morno":
        if (gh + ga) >= 2:
            sinal['Resultado'] = '❌ RED'
            enviar_telegram(token, chats, f"❌ <b>RED | FUROU O UNDER</b>\n\n⚽ {sinal['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {sinal['Estrategia']}")
            return True
        if st_short == 'HT' and (gh + ga) <= 1:
            sinal['Resultado'] = '✅ GREEN'
            enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📉 Placar HT: <b>{gh}x{ga}</b>\n🎯 {sinal['Estrategia']} (Bateu Under 1.5)")
            return True
        return False

    if (gh+ga) > (ph+pa):
        sinal['Resultado'] = '✅ GREEN'
        enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📈 Placar Atual: <b>{gh}x{ga}</b>\n🎯 {sinal['Estrategia']}")
        return True
    if st_short in ['FT', 'AET', 'PEN', 'ABD']:
        sinal['Resultado'] = '❌ RED'
        enviar_telegram(token, chats, f"❌ <b>RED | ENCERRADO</b>\n\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {sinal['Estrategia']}")
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
        enviar_telegram(token, chats, f"🔄 <b>REENVIO</b>\n\n🚨 {s['Estrategia']}\n⚽ {s['Jogo']}\n⚠️ Placar: {s.get('Placar_Sinal','?')}{prob}")
        time.sleep(0.5)

# --- 6. CORE ---
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'multiplas_enviadas' not in st.session_state: st.session_state['multiplas_enviadas'] = set()
if 'controle_stats' not in st.session_state: st.session_state['controle_stats'] = {} 
if 'api_usage' not in st.session_state: st.session_state['api_usage'] = {'used': 0, 'limit': 75000}
if 'data_api_usage' not in st.session_state: st.session_state['data_api_usage'] = datetime.now(pytz.utc).date()
if 'alvos_do_dia' not in st.session_state: st.session_state['alvos_do_dia'] = {}

carregar_tudo()

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
            enviar_relatorio_bi(TG_TOKEN, TG_CHAT); st.toast("Relatório Enviado!")
            
    # Variável de controle do robô ligada ao session_state para não resetar
    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)
    
    with st.expander("📶 Consumo API", expanded=False):
        u = st.session_state['api_usage']
        perc = min(u['used'] / u['limit'], 1.0) if u['limit'] > 0 else 0
        st.progress(perc)
        st.caption(f"Utilizado: **{u['used']}** / {u['limit']}")

# --- 8. DASHBOARD ---
if st.session_state.ROBO_LIGADO:
    carregar_tudo()
    
    # Listas de referência normalizadas
    ids_black = [normalizar_id(x) for x in st.session_state['df_black']['id'].values]
    ids_safe = [normalizar_id(x) for x in st.session_state['df_safe']['id'].values]
    ids_obs = [normalizar_id(x) for x in st.session_state['df_vip']['id'].values]

    hoje_real = get_time_br().strftime('%Y-%m-%d')
    st.session_state['historico_sinais'] = [s for s in st.session_state['historico_sinais'] if s['Data'] == hoje_real]

    api_error = False
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        resp = requests.get(url, headers={"x-apisports-key": API_KEY}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)
        update_api_usage(resp.headers); res = resp.json()
        jogos_live = res.get('response', []) if not res.get('errors') else []; api_error = bool(res.get('errors'))
    except: jogos_live = []; api_error = True

    if not api_error: 
        check_green_red_hibrido(jogos_live, TG_TOKEN, TG_CHAT, API_KEY)
        verificar_alerta_matinal(TG_TOKEN, TG_CHAT, API_KEY) 

    radar = []; alvos = st.session_state.get('alvos_do_dia', {})
    candidatos_multipla = []; ids_no_radar = []

    for j in jogos_live:
        lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']
        if lid in ids_black: continue
        
        nome_liga_show = j['league']['name']
        if lid in ids_safe: nome_liga_show += " 🛡️"
        elif lid in ids_obs: nome_liga_show += " ⚠️"
            
        ids_no_radar.append(fid)
        tempo = j['fixture']['status']['elapsed'] or 0
        st_short = j['fixture']['status']['short']
        home = j['teams']['home']['name']; away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"
        gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
        stats = []; status_vis = "👁️"
        
        rank_h = None; rank_a = None
        if j['league']['id'] in LIGAS_TABELA:
            rk = buscar_ranking(API_KEY, j['league']['id'], j['league']['season'])
            rank_h = rk.get(home); rank_a = rk.get(away)

        t_esp = 60 if (69<=tempo<=76) else (90 if tempo<=15 else 180)
        ult_chk = st.session_state['controle_stats'].get(fid, datetime.min)
        if deve_buscar_stats(tempo, gh, ga, st_short):
            if (datetime.now() - ult_chk).total_seconds() > t_esp:
                try:
                    r_st = requests.get("https://v3.football.api-sports.io/fixtures/statistics", headers={"x-apisports-key": API_KEY}, params={"fixture": fid}, timeout=5)
                    update_api_usage(r_st.headers); stats = r_st.json().get('response', [])
                    if stats: st.session_state['controle_stats'][fid] = datetime.now(); st.session_state[f"st_{fid}"] = stats
                except: stats = []
            else: stats = st.session_state.get(f"st_{fid}", [])
        
        lista_sinais = []
        if stats:
            lista_sinais = processar(j, stats, tempo, placar, rank_h, rank_a)
            salvar_safe_league(lid, j['league']['country'], j['league']['name'], True, (rank_h is not None))
            if st_short == 'HT' and gh == 0 and ga == 0:
                try:
                    s1 = stats[0]['statistics']; s2 = stats[1]['statistics']
                    v1 = next((x['value'] for x in s1 if x['type']=='Total Shots'), 0) or 0
                    v2 = next((x['value'] for x in s2 if x['type']=='Total Shots'), 0) or 0
                    sg1 = next((x['value'] for x in s1 if x['type']=='Shots on Goal'), 0) or 0
                    sg2 = next((x['value'] for x in s2 if x['type']=='Shots on Goal'), 0) or 0
                    if (v1+v2) > 12 and (sg1+sg2) > 6:
                        candidatos_multipla.append({'fid': fid, 'jogo': f"{home} x {away}", 'stats': f"{v1+v2} Chutes", 'indica': "Over 0.5 FT (Apostar que sai gol no 2º tempo)"})
                except: pass
        else: status_vis = "💤"

        if not lista_sinais and not stats and tempo >= 45 and st_short != 'HT': gerenciar_strikes(lid, j['league']['country'], j['league']['name'])
        
        if lista_sinais:
            status_vis = f"✅ {len(lista_sinais)} Sinais"
            for s in lista_sinais:
                uid = f"{fid}_{s['tag']}"
                if uid not in st.session_state['alertas_enviados']:
                    item = {
                        "FID": fid, "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'),
                        "Liga": j['league']['name'], "Jogo": f"{home} x {away}", "Placar_Sinal": placar, "Estrategia": s['tag'], "Resultado": "Pendente",
                        "HomeID": str(j['teams']['home']['id']) if lid in ids_safe else "", "AwayID": str(j['teams']['away']['id']) if lid in ids_safe else ""
                    }
                    if adicionar_historico(item):
                        prob = buscar_inteligencia(s['tag'], j['league']['name'], f"{home} x {away}")
                        headshot = (home in alvos and alvos[home]==s['tag']) or (away in alvos and alvos[away]==s['tag'])
                        
                        tit = "🎯 HEADSHOT | PREVISÃO CONFIRMADA 🎯" if headshot else ("💎 SINAL DE OURO 💎" if "GOLDEN" in s['tag'] else "🚨 SINAL ENCONTRADO 🚨")
                        extra = f"\n🔥 {s['tag'].upper()} *(Validado)*" if headshot else f"\n🔥 {s['tag'].upper()}"
                        
                        msg = f"<b>{tit}</b>\n\n🏆 <b>{j['league']['name']}</b>\n⚽ {home} 🆚 {away}\n⏰ <b>{tempo}' minutos</b> (Placar: {placar})\n{extra}\n⚠️ <b>AÇÃO:</b> {s['ordem']}\n\n📊 <i>Dados: {s['stats']}</i>{prob}"
                        enviar_telegram(TG_TOKEN, TG_CHAT, msg); st.session_state['alertas_enviados'].add(uid); st.toast(f"Sinal: {s['tag']}")
        
        radar.append({"Liga": nome_liga_show, "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})

    if candidatos_multipla:
        novos = [c for c in candidatos_multipla if c['fid'] not in st.session_state['multiplas_enviadas']]
        if novos:
            msg = "<b>🚀 OPORTUNIDADE DE MÚLTIPLA (HT) 🚀</b>\n" + "".join([f"\n⚽ {c['jogo']} ({c['stats']})\n⚠️ AÇÃO: {c['indica']}" for c in novos])
            for c in novos: st.session_state['multiplas_enviadas'].add(c['fid'])
            enviar_telegram(TG_TOKEN, TG_CHAT, msg)

    agenda = []
    if not api_error:
        prox = buscar_agenda_cached(API_KEY, hoje_real)
        agora = get_time_br()
        for p in prox:
            try:
                if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] in ['NS', 'TBD'] and p['fixture']['id'] not in ids_no_radar:
                    if datetime.fromisoformat(p['fixture']['date']) > agora:
                        l_id = normalizar_id(p['league']['id']); l_nm = p['league']['name']
                        if l_id in ids_safe: l_nm += " 🛡️"
                        elif l_id in ids_obs: l_nm += " ⚠️"
                        agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": l_nm, "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})
            except: pass

    dashboard_placeholder = st.empty()
    with dashboard_placeholder.container():
        if api_error: st.markdown('<div class="status-error">🚨 API LIMITADA - AGUARDE</div>', unsafe_allow_html=True)
        else: st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        hist_hj = pd.DataFrame(st.session_state['historico_sinais'])
        t, g, r, w = calcular_stats(hist_hj)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t}</div><div class="metric-sub">{g} Green | {r} Red</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(radar)}</div><div class="metric-sub">Monitorando</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-title">Ligas Seguras</div><div class="metric-value">{len(ids_safe)}</div><div class="metric-sub">Validadas</div></div>', unsafe_allow_html=True)
        
        st.write("")
        abas = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", f"📜 Histórico ({len(hist_hj)})", "📈 BI & Analytics", f"🚫 Blacklist ({len(ids_black)})", f"🛡️ Seguras ({len(ids_safe)})", f"⚠️ Obs ({len(ids_obs)})"])
        
        with abas[0]: 
            if radar: st.dataframe(pd.DataFrame(radar)[['Liga', 'Jogo', 'Tempo', 'Status']].astype(str), use_container_width=True, hide_index=True)
            else: st.info("Buscando jogos...")
        with abas[1]: 
            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos futuros hoje.")
        with abas[2]: 
            if not hist_hj.empty: st.dataframe(hist_hj.astype(str), use_container_width=True, hide_index=True)
            else: st.caption("Vazio.")
        
        with abas[3]: 
            st.markdown("### 📊 Inteligência de Mercado")
            df_bi = st.session_state.get('historico_full', pd.DataFrame())
            if df_bi.empty: st.warning("Sem dados históricos.")
            else:
                try:
                    df_bi['Data_Str'] = df_bi['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
                    df_bi['Data_DT'] = pd.to_datetime(df_bi['Data_Str'], errors='coerce')
                    df_bi = df_bi.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
                except: pass
                
                dias = st.selectbox("📅 Período", ["Tudo", "Hoje", "7 Dias", "30 Dias"])
                h_bi = pd.to_datetime(get_time_br().date())
                
                if 'Data_DT' in df_bi.columns:
                    if dias == "Hoje": df_show = df_bi[df_bi['Data_DT'] == h_bi]
                    elif dias == "7 Dias": df_show = df_bi[df_bi['Data_DT'] >= (h_bi - timedelta(days=7))]
                    elif dias == "30 Dias": df_show = df_bi[df_bi['Data_DT'] >= (h_bi - timedelta(days=30))]
                    else: df_show = df_bi
                else: df_show = df_bi
                
                if not df_show.empty:
                    gr = df_show['Resultado'].str.contains('GREEN').sum(); rd = df_show['Resultado'].str.contains('RED').sum()
                    tt = gr+rd; ww = (gr/tt*100) if tt>0 else 0
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Sinais", tt); m2.metric("Greens", gr); m3.metric("Reds", rd); m4.metric("Assertividade", f"{ww:.1f}%")
                    st.divider()
                    
                    st_s = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                    if not st_s.empty:
                        cts = st_s.groupby(['Estrategia', 'Resultado']).size().reset_index(name='Qtd')
                        fig = px.bar(cts, x='Estrategia', y='Qtd', color='Resultado', color_discrete_map={'✅ GREEN': '#00FF00', '❌ RED': '#FF0000'}, title="Performance por Estratégia", text='Qtd')
                        fig.update_layout(template="plotly_dark"); st.plotly_chart(fig, use_container_width=True)

        with abas[4]: st.dataframe(st.session_state['df_black'], use_container_width=True, hide_index=True)
        with abas[5]: st.dataframe(st.session_state['df_safe'], use_container_width=True, hide_index=True)
        with abas[6]: st.dataframe(st.session_state.get('df_vip', pd.DataFrame()), use_container_width=True, hide_index=True)

    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True); time.sleep(1)
    st.rerun()
else:
    st.title("❄️ Neves Analytics")
    st.info("💡 Robô em espera. Configure na lateral.")
