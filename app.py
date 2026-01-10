import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 0. CONFIGURAÇÃO VISUAL (LAYOUT COMPACTO) ---
st.set_page_config(page_title="Neves Analytics PRO", layout="centered", page_icon="❄️")
st.cache_data.clear()

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    
    /* Cards de Métricas mais bonitos e compactos */
    .metric-box {
        background-color: #1A1C24; 
        border: 1px solid #333; 
        border-radius: 10px; 
        padding: 15px; 
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    .metric-title {font-size: 13px; color: #aaaaaa; text-transform: uppercase; letter-spacing: 1px;}
    .metric-value {font-size: 28px; font-weight: bold; color: #00FF00;}
    
    /* Status Ativo */
    .status-active {
        background-color: #1F4025; color: #00FF00; 
        border: 1px solid #00FF00; padding: 12px; 
        text-align: center; border-radius: 8px; font-weight: bold;
        margin-bottom: 20px;
    }
    
    /* Timer Fixo no Rodapé (Estilo Netflix/Moderno) */
    .footer-timer {
        position: fixed; left: 0; bottom: 0; width: 100%;
        background-color: #0E1117; color: #FFD700;
        text-align: center; padding: 12px; font-size: 16px;
        font-weight: bold; border-top: 1px solid #333;
        z-index: 9999;
        box-shadow: 0 -4px 10px rgba(0,0,0,0.5);
    }
    
    /* Tabelas mais limpas */
    .stDataFrame { border-radius: 10px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# --- 1. ARQUIVOS E CONSTANTES ---
FILES = {
    'black': 'neves_blacklist.txt',
    'vip': 'neves_strikes_vip.txt',
    'hist': 'neves_historico_sinais.csv',
    'report': 'neves_status_relatorio.txt'
}

# Lista de IDs Importantes (Para prioridade visual, não apenas imunidade)
LIGAS_VIP = [
    39, 78, 135, 140, 61, 2, 3, 9, 45, 48, # Europa
    71, 72, 13, 11, # Brasil/Latam
    474, 475, 476, 477, 478, 479, # Estaduais Principais
    606, 610, 628, 55, 143 # Outros Estaduais
]

# --- 2. FUNÇÕES DE DADOS SEGURAS ---
def safe_read_csv(filepath, columns):
    if not os.path.exists(filepath): return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(filepath)
        # Se colunas não baterem, recria
        if not set(columns).issubset(df.columns): return pd.DataFrame(columns=columns)
        return df.fillna("").astype(str)
    except: return pd.DataFrame(columns=columns)

def carregar_dados_iniciais():
    if 'df_black' not in st.session_state:
        st.session_state['df_black'] = safe_read_csv(FILES['black'], ['id', 'País', 'Liga'])
    if 'df_strikes' not in st.session_state:
        st.session_state['df_strikes'] = safe_read_csv(FILES['vip'], ['id', 'País', 'Liga', 'Data_Erro', 'Strikes'])
    if 'historico_sinais' not in st.session_state:
        df = safe_read_csv(FILES['hist'], ['Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado'])
        hoje = datetime.now().strftime('%Y-%m-%d')
        if not df.empty and 'Data' in df.columns:
            df = df[df['Data'] == hoje]
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

def salvar_historico(item):
    df_novo = pd.DataFrame([item])
    df_novo.to_csv(FILES['hist'], mode='a', header=not os.path.exists(FILES['hist']), index=False)

def salvar_strike(id_liga, pais, nome_liga, strikes):
    df = st.session_state['df_strikes']
    hoje = datetime.now().strftime('%Y-%m-%d')
    id_str = str(id_liga)
    
    # Remove entrada antiga se existir para atualizar
    if id_str in df['id'].values:
        df = df[df['id'] != id_str]
    
    novo = pd.DataFrame([{
        'id': id_str, 'País': str(pais), 'Liga': str(nome_liga), 
        'Data_Erro': hoje, 'Strikes': str(strikes)
    }])
    
    df_final = pd.concat([df, novo], ignore_index=True)
    df_final.to_csv(FILES['vip'], index=False)
    st.session_state['df_strikes'] = df_final

# --- 3. LÓGICA DE STRIKES (2 RODADAS) ---
def gerenciar_strikes(id_liga, pais, nome_liga):
    """
    Só bane se houver erro em 2 DIAS DIFERENTES (2 Rodadas).
    """
    df = st.session_state['df_strikes']
    hoje = datetime.now().strftime('%Y-%m-%d')
    id_str = str(id_liga)
    
    strikes_atuais = 0
    ultima_data = ""
    
    if id_str in df['id'].values:
        row = df[df['id'] == id_str].iloc[0]
        strikes_atuais = int(row['Strikes'])
        ultima_data = row['Data_Erro']
    
    # Se o erro é HOJE e já foi anotado hoje, não faz nada (espera próxima rodada)
    if ultima_data == hoje:
        return # Já tomou strike hoje
        
    # Se é um erro em DATA NOVA (Nova Rodada)
    novo_strike = strikes_atuais + 1
    
    if novo_strike >= 2:
        salvar_blacklist(id_liga, pais, nome_liga)
        st.toast(f"🚫 {nome_liga} Banida (Falha em 2 Rodadas)", icon="❌")
    else:
        salvar_strike(id_liga, pais, nome_liga, novo_strike)
        st.toast(f"⚠️ {nome_liga}: Sem dados (Strike 1/2)", icon="⚠️")

# --- 4. TELEGRAM ---
def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "Markdown"}, timeout=3)
        except: pass

def reenviar_sinais_hoje(token, chat_ids):
    hist = st.session_state['historico_sinais']
    if not hist:
        st.toast("Sem sinais hoje para reenviar.")
        return
    
    st.toast(f"Reenviando {len(hist)} sinais...")
    for sinal in reversed(hist): # Envia do mais antigo pro mais novo ou vice versa
        msg = f"🔄 *REENVIO MANUAL*\n\n🚨 *{sinal['Estrategia']}*\n⚽ {sinal['Jogo']}\n🏆 {sinal['Liga']}\nPlacar Sinal: {sinal['Placar_Sinal']}"
        enviar_telegram(token, chat_ids, msg)
        time.sleep(1) # Pausa para não bloquear

def verificar_greens_reds(jogos_live, token, chat_ids):
    """Atualiza Green/Red"""
    atualizou = False
    historico = st.session_state['historico_sinais']
    
    for sinal in historico:
        if sinal['Resultado'] == 'Pendente':
            jogo = next((j for j in jogos_live if j['teams']['home']['name'] in sinal['Jogo']), None)
            if jogo:
                gh = jogo['goals']['home'] or 0
                ga = jogo['goals']['away'] or 0
                try:
                    ph, pa = map(int, sinal['Placar_Sinal'].split('x'))
                except: continue

                if (gh + ga) > (ph + pa):
                    sinal['Resultado'] = '✅ GREEN'
                    msg = f"✅ *GREEN CONFIRMADO!* \n\n⚽ {sinal['Jogo']}\n📈 Placar Atual: {gh}x{ga}\n🎯 {sinal['Estrategia']}"
                    enviar_telegram(token, chat_ids, msg)
                    atualizou = True
                elif jogo['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
                    sinal['Resultado'] = '❌ RED'
                    msg = f"❌ *RED* \n\n⚽ {sinal['Jogo']}\n📉 {sinal['Estrategia']} não bateu."
                    enviar_telegram(token, chat_ids, msg)
                    atualizou = True
    
    if atualizou:
        # Salva atualização no CSV
        pd.DataFrame(historico).to_csv(FILES['hist'], index=False)

def enviar_relatorio_final(token, chat_ids):
    hoje = datetime.now().strftime('%Y-%m-%d')
    hist = [h for h in st.session_state['historico_sinais'] if h['Data'] == hoje]
    
    if not hist: return
    
    greens = len([h for h in hist if 'GREEN' in h['Resultado']])
    reds = len([h for h in hist if 'RED' in h['Resultado']])
    pendentes = len(hist) - (greens + reds)
    total = greens + reds
    assertividade = (greens / total * 100) if total > 0 else 0
    
    msg = f"📊 *RELATÓRIO DE SINAIS ({hoje})*\n\n"
    msg += f"🚀 Total: {len(hist)}\n✅ Greens: {greens}\n❌ Reds: {reds}\n⏳ Pendentes: {pendentes}\n\n"
    msg += f"🎯 Assertividade: {assertividade:.1f}%"
    
    enviar_telegram(token, chat_ids, msg)
    with open(FILES['report'], 'w') as f: f.write(hoje)

# --- 5. LÓGICA DO JOGO ---
if 'ligas_imunes' not in st.session_state: st.session_state['ligas_imunes'] = {}
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
carregar_dados_iniciais()

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
    
    # Estratégias
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

# --- 6. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics PRO")
    
    with st.expander("⚙️ Configurações", expanded=True):
        API_KEY = st.text_input("Chave API:", type="password")
        TG_TOKEN = st.text_input("Token Telegram:", type="password")
        TG_CHAT = st.text_input("Chat IDs:")
        INTERVALO = st.slider("Ciclo (s):", 30, 300, 60)
        
        c1, c2 = st.columns(2)
        if c1.button("🔄 Reenviar Sinais"): reenviar_sinais_hoje(TG_TOKEN, TG_CHAT)
        if c2.button("🗑️ Limpar Blacklist"):
            if os.path.exists(FILES['black']): os.remove(FILES['black'])
            st.session_state['df_black'] = pd.DataFrame(columns=['id', 'País', 'Liga'])
            st.toast("Blacklist Limpa!")
            time.sleep(1)
            st.rerun()

    with st.expander("📘 Estratégias", expanded=False):
        st.markdown("**🟣 Porteira:** 2+ gols <30' -> Over Gols.")
        st.markdown("**🟢 Blitz:** Pressão -> Gol/Over.")
        st.markdown("**💰 Janela:** 70-75' intenso -> Over Asiático.")
        st.markdown("**⚡ Relâmpago:** 5-15' elétrico -> Over 0.5 HT.")

    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 7. DASHBOARD PRINCIPAL ---
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
        
        if tempo > 80 or tempo < 2: continue
        
        # Busca Stats
        try:
            url_s = "https://v3.football.api-sports.io/fixtures/statistics"
            stats = requests.get(url_s, headers={"x-apisports-key": API_KEY}, params={"fixture": fid}).json().get('response', [])
        except: stats = []
        
        sinal = processar_jogo(j, stats, tempo, placar)
        
        # --- AQUI ESTÁ A LÓGICA DE STRIKES (2 RODADAS) ---
        if not sinal and not stats and tempo >= 45:
            # Chama a função que gerencia Strikes e só bane se for dia diferente
            gerenciar_strikes(lid, j['league']['country'], j['league']['name'])
            # Não 'continue' aqui para podermos ver o jogo na tabela, mas sem status
        
        if stats: # Se tem dados, é segura
            st.session_state['ligas_imunes'][lid] = {'País': j['league']['country'], 'Liga': j['league']['name']}

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
                salvar_historico(item)
                st.toast(f"Sinal: {sinal['tag']}")

        radar.append({
            "Liga": j['league']['name'], "Jogo": f"{home} {placar} {away}",
            "Tempo": f"{tempo}'", "Status": vis_status
        })

    # Agenda e Relatório
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

    if not radar and not agenda:
        if not os.path.exists(FILES['report']) or open(FILES['report']).read() != datetime.now().strftime('%Y-%m-%d'):
            enviar_relatorio_final(TG_TOKEN, TG_CHAT)

    # --- RENDERIZAÇÃO ---
    with main.container():
        st.markdown('<div class="status-active">🟢 SISTEMA DE MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        hoje = datetime.now().strftime('%Y-%m-%d')
        hist_hoje = [x for x in st.session_state['historico_sinais'] if x['Data'] == hoje]
        
        # MÉTRICAS PEDIDAS: Sinais Hoje | Jogos Live | Ligas Seguras (Não Blacklist)
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-box"><div class="metric-value">{len(hist_hoje)}</div><div class="metric-title">Sinais Hoje</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-box"><div class="metric-value">{len(radar)}</div><div class="metric-title">Jogos Live</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-box"><div class="metric-value">{len(st.session_state["ligas_imunes"])}</div><div class="metric-title">Ligas Seguras</div></div>', unsafe_allow_html=True)
        
        st.write("")

        # Abas
        t1, t2, t3, t4, t5 = st.tabs([
            f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", 
            f"📜 Histórico ({len(hist_hoje)})", f"🚫 Blacklist ({len(st.session_state['df_black'])})", 
            f"⚠️ Observação ({len(st.session_state['df_strikes'])})"
        ])
        
        with t1: st.dataframe(pd.DataFrame(radar).astype(str), use_container_width=True, hide_index=True) if radar else st.info("Buscando jogos...")
        with t2: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True) if agenda else st.caption("Sem jogos.")
        with t3: st.dataframe(pd.DataFrame(hist_hoje).astype(str), use_container_width=True, hide_index=True) if hist_hoje else st.caption("Nenhum sinal.")
        with t4: st.dataframe(st.session_state['df_black'].astype(str), use_container_width=True, hide_index=True)
        with t5: st.dataframe(st.session_state['df_strikes'].astype(str), use_container_width=True, hide_index=True)

    relogio = st.empty()
    for i in range(INTERVALO, 0, -1):
        relogio.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
        time.sleep(1)
    st.rerun()

else:
    with main.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure na lateral.")
