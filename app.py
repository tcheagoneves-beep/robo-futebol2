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
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")

# INICIALIZAÇÃO DE VARIÁVEIS
if 'ROBO_LIGADO' not in st.session_state: st.session_state.ROBO_LIGADO = False
if 'last_db_update' not in st.session_state: st.session_state['last_db_update'] = 0
if 'bi_enviado_data' not in st.session_state: st.session_state['bi_enviado_data'] = ""
if 'confirmar_reset' not in st.session_state: st.session_state['confirmar_reset'] = False
DB_CACHE_TIME = 60

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
        width: 100%; height: 50px !important; font-size: 16px !important; font-weight: bold !important;
        background-color: #262730; border: 1px solid #4e4e4e; color: white;
    }
    .stButton button:hover { border-color: #00FF00; color: #00FF00; }
    
    /* BOTÃO PERIGO */
    div[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"] {
        background-color: #4A0E0E !important; border: 1px solid #FF4B4B !important; color: #FF4B4B !important;
    }
    div[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"]:hover {
        background-color: #FF0000 !important; color: white !important;
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

# --- 1. CONEXÃO ---
conn = st.connection("gsheets", type=GSheetsConnection)

COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID']
COLS_SAFE = ['id', 'País', 'Liga', 'Motivo', 'Strikes', 'Jogos_Erro']
COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes', 'Jogos_Erro']
COLS_BLACK = ['id', 'País', 'Liga', 'Motivo']

LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]

# --- 2. UTILITÁRIOS ---
def get_time_br(): return datetime.now(pytz.timezone('America/Sao_Paulo'))
def clean_fid(x): 
    try: return str(int(float(x))) 
    except: return '0'

def normalizar_id(val):
    try:
        s_val = str(val).strip()
        if not s_val or s_val.lower() == 'nan': return ""
        return str(int(float(s_val)))
    except: return str(val).strip()

# --- 3. BANCO DE DADOS ---
def carregar_aba(nome_aba, colunas_esperadas):
    try:
        df = conn.read(worksheet=nome_aba, ttl=0)
        if not df.empty:
            for col in colunas_esperadas:
                if col not in df.columns:
                    if col == 'Strikes': df[col] = '0'
                    elif col == 'Jogos_Erro': df[col] = ''
                    else: df[col] = ""
        if df.empty or len(df.columns) < len(colunas_esperadas): 
            return pd.DataFrame(columns=colunas_esperadas)
        return df.fillna("").astype(str)
    except: return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df_para_salvar):
    try: conn.update(worksheet=nome_aba, data=df_para_salvar); return True
    except: return False

def carregar_tudo(force=False):
    now = time.time()
    if not force:
        if (now - st.session_state['last_db_update']) < DB_CACHE_TIME:
            if 'df_black' in st.session_state and 'df_safe' in st.session_state and 'historico_full' in st.session_state: return

    df = carregar_aba("Blacklist", COLS_BLACK)
    if not df.empty: df['id'] = df['id'].apply(normalizar_id)
    st.session_state['df_black'] = df

    df = carregar_aba("Seguras", COLS_SAFE)
    if not df.empty: df['id'] = df['id'].apply(normalizar_id)
    st.session_state['df_safe'] = df

    df = carregar_aba("Obs", COLS_OBS)
    if not df.empty: df['id'] = df['id'].apply(normalizar_id)
    st.session_state['df_vip'] = df
    
    df = carregar_aba("Historico", COLS_HIST)
    if not df.empty and 'Data' in df.columns:
        df['FID'] = df['FID'].apply(clean_fid)
        st.session_state['historico_full'] = df
        hoje = get_time_br().strftime('%Y-%m-%d')
        st.session_state['historico_sinais'] = df[df['Data'] == hoje].to_dict('records')[::-1]
        
        if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
        df_hoje = df[df['Data'] == hoje]
        for _, row in df_hoje.iterrows():
            st.session_state['alertas_enviados'].add(f"{row['FID']}_{row['Estrategia']}")
    else:
        st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
        st.session_state['historico_sinais'] = []

    st.session_state['last_db_update'] = now

def adicionar_historico(item):
    if 'historico_full' not in st.session_state: st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)
    df_novo = pd.DataFrame([item])
    st.session_state['historico_full'] = pd.concat([df_novo, st.session_state['historico_full']], ignore_index=True)
    st.session_state['historico_sinais'].insert(0, item)
    return salvar_aba("Historico", st.session_state['historico_full'])

def atualizar_historico_ram_disk(lista_atualizada):
    df_hoje = pd.DataFrame(lista_atualizada)
    df_disk = st.session_state['historico_full']
    hoje = get_time_br().strftime('%Y-%m-%d')
    if not df_disk.empty and 'Data' in df_disk.columns:
         df_disk['Data'] = df_disk['Data'].astype(str).str.replace(' 00:00:00', '', regex=False)
         df_disk = df_disk[df_disk['Data'] != hoje]
    df_final = pd.concat([df_hoje, df_disk], ignore_index=True)
    df_final = df_final.drop_duplicates(subset=['FID', 'Estrategia'], keep='first')
    st.session_state['historico_full'] = df_final 
    salvar_aba("Historico", df_final)

def salvar_blacklist(id_liga, pais, nome_liga, motivo_ban):
    df = st.session_state['df_black']
    id_norm = normalizar_id(id_liga)
    
    if id_norm not in df['id'].values:
        novo = pd.DataFrame([{
            'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga),
            'Motivo': str(motivo_ban) # Motivo Específico
        }])
        final = pd.concat([df, novo], ignore_index=True)
        st.session_state['df_black'] = final
        salvar_aba("Blacklist", final)
        
        # Limpa Obs
        df_vip = st.session_state.get('df_vip', pd.DataFrame())
        if not df_vip.empty and id_norm in df_vip['id'].values:
            df_vip_limpo = df_vip[df_vip['id'] != id_norm]
            st.session_state['df_vip'] = df_vip_limpo
            salvar_aba("Obs", df_vip_limpo)

def salvar_safe_league_basic(id_liga, pais, nome_liga):
    id_norm = normalizar_id(id_liga)
    df = st.session_state['df_safe']
    if id_norm not in df['id'].values:
        novo = pd.DataFrame([{
            'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 
            'Motivo': 'Validada', 'Strikes': '0', 'Jogos_Erro': ''
        }])
        final = pd.concat([df, novo], ignore_index=True)
        if salvar_aba("Seguras", final): st.session_state['df_safe'] = final

def resetar_erros(id_liga):
    id_norm = normalizar_id(id_liga)
    df_vip = st.session_state.get('df_vip', pd.DataFrame())
    if not df_vip.empty and id_norm in df_vip['id'].values:
        df_new_vip = df_vip[df_vip['id'] != id_norm]
        if salvar_aba("Obs", df_new_vip): st.session_state['df_vip'] = df_new_vip
    
    df_safe = st.session_state.get('df_safe', pd.DataFrame())
    if not df_safe.empty and id_norm in df_safe['id'].values:
        idx = df_safe[df_safe['id'] == id_norm].index[0]
        if str(df_safe.at[idx, 'Strikes']) != '0':
            df_safe.at[idx, 'Strikes'] = '0'; df_safe.at[idx, 'Jogos_Erro'] = ''
            if salvar_aba("Seguras", df_safe): st.session_state['df_safe'] = df_safe

def gerenciar_erros(id_liga, pais, nome_liga, fid_jogo):
    id_norm = normalizar_id(id_liga)
    fid_str = str(fid_jogo)
    
    # 1. LIGA SEGURA
    df_safe = st.session_state.get('df_safe', pd.DataFrame())
    if not df_safe.empty and id_norm in df_safe['id'].values:
        idx = df_safe[df_safe['id'] == id_norm].index[0]
        jogos_erro = str(df_safe.at[idx, 'Jogos_Erro']).split(',') if str(df_safe.at[idx, 'Jogos_Erro']).strip() else []
        if fid_str in jogos_erro: return 
        jogos_erro.append(fid_str)
        strikes = len(jogos_erro)
        
        if strikes >= 10:
            df_safe = df_safe.drop(idx)
            salvar_aba("Seguras", df_safe); st.session_state['df_safe'] = df_safe
            
            df_vip = st.session_state.get('df_vip', pd.DataFrame())
            novo_obs = pd.DataFrame([{
                'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 
                'Data_Erro': get_time_br().strftime('%Y-%m-%d'), 'Strikes': '1', 'Jogos_Erro': fid_str
            }])
            final_vip = pd.concat([df_vip, novo_obs], ignore_index=True)
            salvar_aba("Obs", final_vip); st.session_state['df_vip'] = final_vip
        else:
            df_safe.at[idx, 'Strikes'] = str(strikes); df_safe.at[idx, 'Jogos_Erro'] = ",".join(jogos_erro)
            salvar_aba("Seguras", df_safe); st.session_state['df_safe'] = df_safe
        return

    # 2. LIGA OBS
    df_vip = st.session_state.get('df_vip', pd.DataFrame())
    strikes = 0; jogos_erro = []
    if not df_vip.empty and id_norm in df_vip['id'].values:
        row = df_vip[df_vip['id'] == id_norm].iloc[0]
        val_jogos = str(row.get('Jogos_Erro', '')).strip()
        if val_jogos: jogos_erro = val_jogos.split(',')
    
    if fid_str in jogos_erro: return
    jogos_erro.append(fid_str)
    strikes = len(jogos_erro)
    
    if strikes >= 10:
        # MOTIVO ESPECÍFICO AQUI
        motivo = f"Banida ({strikes} Jogos s/ Dados)"
        salvar_blacklist(id_liga, pais, nome_liga, motivo)
        st.toast(f"🚫 {nome_liga} Banida!")
    else:
        if id_norm in df_vip['id'].values:
            idx = df_vip[df_vip['id'] == id_norm].index[0]
            df_vip.at[idx, 'Strikes'] = str(strikes); df_vip.at[idx, 'Jogos_Erro'] = ",".join(jogos_erro)
            df_vip.at[idx, 'Data_Erro'] = get_time_br().strftime('%Y-%m-%d')
            salvar_aba("Obs", df_vip); st.session_state['df_vip'] = df_vip
        else:
            novo = pd.DataFrame([{
                'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 
                'Data_Erro': get_time_br().strftime('%Y-%m-%d'), 'Strikes': '1', 'Jogos_Erro': fid_str
            }])
            final = pd.concat([df_vip, novo], ignore_index=True)
            salvar_aba("Obs", final); st.session_state['df_vip'] = final

def calcular_stats(df_raw):
    if df_raw.empty: return 0, 0, 0, 0
    df_raw = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])
    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])
    total = len(df_raw)
    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0
    return total, greens, reds, winrate

def verificar_reset_diario():
    hoje_utc = datetime.now(pytz.utc).date()
    if 'data_api_usage' not in st.session_state: st.session_state['data_api_usage'] = hoje_utc
    if st.session_state['data_api_usage'] != hoje_utc:
        st.session_state['api_usage']['used'] = 0; st.session_state['data_api_usage'] = hoje_utc
        st.session_state['alvos_do_dia'] = {}
        return True
    return False

def update_api_usage(headers):
    if not headers: return
    try:
        limit = int(headers.get('x-ratelimit-requests-limit', 75000))
        remaining = int(headers.get('x-ratelimit-requests-remaining', 0))
        used = limit - remaining
        st.session_state['api_usage'] = {'used': used, 'limit': limit}
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

def _worker_telegram(token, chat_id, msg):
    try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=5)
    except: pass

def _worker_telegram_photo(token, chat_id, photo_buffer, caption):
    try:
        url = f"https://api.telegram.org/bot{token}/sendPhoto"
        files = {'photo': photo_buffer}
        data = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'HTML'}
        requests.post(url, files=files, data=data, timeout=10)
    except: pass

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        t = threading.Thread(target=_worker_telegram, args=(token, cid, msg))
        t.daemon = True; t.start()

def verificar_automacao_bi(token, chat_ids):
    agora = get_time_br()
    if agora.hour == 23 and agora.minute >= 30:
        hoje_str = agora.strftime('%Y-%m-%d')
        if st.session_state.get('bi_enviado_data') != hoje_str:
            enviar_relatorio_bi(token, chat_ids)
            st.session_state['bi_enviado_data'] = hoje_str
            st.toast("Relatório Automático Enviado!")

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
            id_h = str(row.get('HomeID', '')).strip(); id_a = str(row.get('AwayID', '')).strip()
            nomes = row['Jogo'].split(' x '); nome_h = nomes[0].split('(')[0].strip(); nome_a = nomes[1].split('(')[0].strip()
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
        df = df.copy()
        df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()
        df['Data_DT'] = pd.to_datetime(df['Data_Str'], errors='coerce')
        df = df.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')
    except: return
    
    hoje = pd.to_datetime(get_time_br().date())
    mask_dia = df['Data_DT'] == hoje
    mask_sem = df['Data_DT'] >= (hoje - timedelta(days=7))
    mask_mes = df['Data_DT'] >= (hoje - timedelta(days=30))
    
    def cm(d):
        g = d['Resultado'].str.contains('GREEN').sum(); r = d['Resultado'].str.contains('RED').sum()
        tot = g+r; wr = (g/tot*100) if tot>0 else 0
        return tot, g, r, wr

    t_d, g_d, r_d, w_d = cm(df[mask_dia])
    t_s, g_s, r_s, w_s = cm(df[mask_sem])
    t_m, g_m, r_m, w_m = cm(df[mask_mes])
    t_a, g_a, r_a, w_a = cm(df)

    if token and chat_ids:
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(7, 4))
        stats = df[mask_mes][df[mask_mes]['Resultado'].isin(['✅ GREEN', '❌ RED'])]
        if not stats.empty:
            c = stats.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)
            c.plot(kind='bar', stacked=True, color=['#00FF00', '#FF0000'], ax=ax, width=0.6)
            ax.set_title(f'PERFORMANCE 30 DIAS (WR: {w_m:.1f}%)', color='white', fontsize=12)
            ax.legend(title='', frameon=False)
            plt.tight_layout()
            buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=100, facecolor='#0E1117'); buf.seek(0)
            msg = f"📊 <b>RELATÓRIO BI</b>\n\n📆 <b>HOJE:</b> {t_d} (WR: {w_d:.1f}%)\n📅 <b>7 DIAS:</b> {t_s} (WR: {w_s:.1f}%)\n🗓️ <b>30 DIAS:</b> {t_m} (WR: {w_m:.1f}%)\n♾️ <b>TOTAL:</b> {t_a} (WR: {w_a:.1f}%)"
            ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
            for cid in ids:
                buf.seek(0); _worker_telegram_photo(token, cid, buf, msg)
            plt.close(fig)

def processar_resultado(sinal, jogo_api, token, chats):
    gh = jogo_api['goals']['home'] or 0
    ga = jogo_api['goals']['away'] or 0
    st_short = jogo_api['fixture']['status']['short']
    STRATS_HT_ONLY = ["Gol Relâmpago", "Massacre", "Choque Líderes", "Briga de Rua"]
    if "Múltipla" in sinal['Estrategia']: return False
    try: ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
    except: return False

    if (gh+ga) > (ph+pa):
        if "Morno" in sinal['Estrategia']: 
            sinal['Resultado'] = '❌ RED'
            enviar_telegram(token, chats, f"❌ <b>RED | GOL SAIU</b>\n⚽ {sinal['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {sinal['Estrategia']}")
            return True
        else:
            sinal['Resultado'] = '✅ GREEN'
            enviar_telegram(token, chats, f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\n📈 Placar: <b>{gh}x{ga}</b>\n🎯 {sinal['Estrategia']}")
            return True

    eh_ht_strat = any(x in sinal['Estrategia'] for x in STRATS_HT_ONLY)
    if eh_ht_strat and st_short in ['HT', '2H', 'FT', 'AET', 'PEN', 'ABD']:
        sinal['Resultado'] = '❌ RED'
        enviar_telegram(token, chats, f"❌ <b>RED | INTERVALO (HT)</b>\n⚽ {sinal['Jogo']}\n📉 Placar HT: {gh}x{ga}\n🎯 {sinal['Estrategia']} (Não bateu no 1º Tempo)")
        return True

    if st_short in ['FT', 'AET', 'PEN', 'ABD']:
        if "Morno" in sinal['Estrategia'] and (gh+ga) <= 1:
             sinal['Resultado'] = '✅ GREEN'
             enviar_telegram(token, chats, f"✅ <b>GREEN | UNDER BATIDO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}")
             return True
        sinal['Resultado'] = '❌ RED'
        enviar_telegram(token, chats, f"❌ <b>RED | ENCERRADO</b>\n⚽ {sinal['Jogo']}\n📉 Placar Final: {gh}x{ga}\n🎯 {sinal['Estrategia']}")
        return True
    return False

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    atualizou = False
    hist = st.session_state['historico_sinais']
    pendentes = [s for s in hist if s['Resultado'] == 'Pendente']
    if not pendentes: return
    hoje_str = get_time_br().strftime('%Y-%m-%d')
    ids_live = [j['fixture']['id'] for j in jogos_live]
    for s in pendentes:
        if s.get('Data') != hoje_str: continue
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

def verificar_var_rollback(jogos_live, token, chats):
    hist = st.session_state['historico_sinais']
    greens = [s for s in hist if 'GREEN' in str(s['Resultado'])]
    if not greens: return
    atualizou = False
    for s in greens:
        if "Morno" in s['Estrategia']: continue
        fid = int(clean_fid(s.get('FID', 0)))
        jogo_api = next((j for j in jogos_live if j['fixture']['id'] == fid), None)
        if jogo_api:
            gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0
            try:
                ph, pa = map(int, s['Placar_Sinal'].split('x'))
                if (gh + ga) <= (ph + pa):
                    s['Resultado'] = 'Pendente'; s['Placar_Sinal'] = f"{gh}x{ga}"; atualizou = True
                    msg = (f"⚠️ <b>VAR ACIONADO | GOL ANULADO</b>\n\n⚽ {s['Jogo']}\n📉 Placar voltou para: <b>{gh}x{ga}</b>\n🔄 Status revertido para <b>PENDENTE</b>.")
                    enviar_telegram(token, chats, msg)
            except: pass
    if atualizou: atualizar_historico_ram_disk(hist)

def reenviar_sinais(token, chats):
    hist = st.session_state['historico_sinais']
    if not hist: return st.toast("Sem sinais.")
    st.toast("Reenviando...")
    for s in reversed(hist):
        prob = buscar_inteligencia(s['Estrategia'], s['Liga'], s['Jogo'])
        enviar_telegram(token, chats, f"🔄 <b>REENVIO</b>\n\n🚨 {s['Estrategia']}\n⚽ {s['Jogo']}\n⚠️ Placar: {s.get('Placar_Sinal','?')}{prob}")
        time.sleep(0.5)

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
    if 70 <= tempo <= 85 and abs(gh - ga) <= 1: return True
    if tempo <= 60 and abs(gh - ga) <= 1: return True
    if status == 'HT' and gh == 0 and ga == 0: return True
    return False

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0
        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        tot_chutes = sh_h + sh_a; tot_gol = sog_h + sog_a
        txt_stats = f"{tot_chutes} Chutes (🎯 {tot_gol} no Gol)"
    except: return []
    fid = j['fixture']['id']; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0
    rh, ra = momentum(fid, sog_h, sog_a)
    SINAIS = []
    
    if tempo <= 30 and (gh+ga) >= 2: SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": "🔥 Over Gols (Tendência de Goleada)", "stats": f"Placar: {gh}x{ga}"})
    if (gh + ga) == 0:
        if (tempo <= 2 and (sog_h + sog_a) >= 1) or (tempo <= 10 and (sh_h + sh_a) >= 2):
            SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": "Over
