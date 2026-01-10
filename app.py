import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 1. CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")

st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .metric-card {background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; text-align: center;}
    .metric-value {font-size: 24px; font-weight: bold; color: #00FF00;}
    .metric-label {font-size: 14px; color: #ccc;}
    .strategy-card { background-color: #1e1e1e; padding: 15px; border-radius: 8px; margin-bottom: 10px; border-left: 4px solid #00FF00; }
    .strategy-title { color: #00FF00; font-weight: bold; font-size: 16px; margin-bottom: 5px; }
    .strategy-desc { font-size: 13px; color: #cccccc; line-height: 1.6; }
</style>
""", unsafe_allow_html=True)

# --- 2. GESTÃO DE DADOS E MEMÓRIA ---
DB_FILE = 'neves_dados.txt'
BLACK_FILE = 'neves_blacklist.txt'
STRIKES_FILE = 'neves_strikes_vip.txt'
HIST_FILE = 'neves_historico_sinais.csv'
RELATORIO_FILE = 'neves_status_relatorio.txt'

# --- 🛡️ LISTA VIP (Incluindo Estaduais BR) ---
LIGAS_VIP = [
    39, 78, 135, 140, 61, 2, 3, 9, 45, 48, # Europa
    71, 72, 13, 11, # Brasil A/B/Liberta
    474, 475, 476, 477, 478, 479, # Estaduais Principais (Gaúcho=477)
    606, 610, 628, 55, 143 # Outros (Paranaense, Goiano, Brasiliense)
]

# --- INICIALIZAÇÃO DE MEMÓRIA ---
if 'ligas_imunes' in st.session_state:
    if st.session_state['ligas_imunes'] and isinstance(list(st.session_state['ligas_imunes'].values())[0], str):
        st.session_state['ligas_imunes'] = {} 

if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'erros_vip' not in st.session_state: st.session_state['erros_vip'] = {}
if 'ligas_imunes' not in st.session_state: st.session_state['ligas_imunes'] = {} 
if 'erros_por_liga' not in st.session_state: st.session_state['erros_por_liga'] = {}

# --- FUNÇÕES DE ARQUIVO (PERSISTÊNCIA) ---
def carregar_historico():
    if not os.path.exists(HIST_FILE): return []
    try:
        df = pd.read_csv(HIST_FILE)
        hoje = datetime.now().strftime('%Y-%m-%d')
        df = df[df['Data'] == hoje]
        return df.to_dict('records')
    except: return []

def salvar_sinal_historico(sinal_dict):
    df_novo = pd.DataFrame([sinal_dict])
    if not os.path.exists(HIST_FILE):
        df_novo.to_csv(HIST_FILE, index=False)
    else:
        df_novo.to_csv(HIST_FILE, mode='a', header=False, index=False)

def verificar_relatorio_enviado():
    if not os.path.exists(RELATORIO_FILE): return False
    try:
        with open(RELATORIO_FILE, 'r') as f:
            return f.read().strip() == datetime.now().strftime('%Y-%m-%d')
    except: return False

def marcar_relatorio_como_enviado():
    with open(RELATORIO_FILE, 'w') as f:
        f.write(datetime.now().strftime('%Y-%m-%d'))

if 'historico_sinais' not in st.session_state:
    st.session_state['historico_sinais'] = carregar_historico()

# --- FUNÇÕES ---
def carregar_blacklist():
    if not os.path.exists(BLACK_FILE): return pd.DataFrame(columns=['id', 'País', 'Liga'])
    try: return pd.read_csv(BLACK_FILE)
    except: return pd.DataFrame(columns=['id', 'País', 'Liga'])

def salvar_na_blacklist(id_liga, pais, nome_liga):
    df = carregar_blacklist()
    if str(id_liga) not in df['id'].astype(str).values:
        novo = pd.DataFrame([{'id': str(id_liga), 'País': pais, 'Liga': nome_liga}])
        pd.concat([df, novo], ignore_index=True).to_csv(BLACK_FILE, index=False)

def carregar_strikes_vip():
    cols = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes']
    if not os.path.exists(STRIKES_FILE): return pd.DataFrame(columns=cols)
    try: 
        df = pd.read_csv(STRIKES_FILE)
        if 'Liga' not in df.columns: return pd.DataFrame(columns=cols)
        return df
    except: return pd.DataFrame(columns=cols)

def registrar_erro_vip(id_liga, pais, nome_liga):
    df = carregar_strikes_vip()
    hoje = datetime.now().strftime('%Y-%m-%d')
    id_liga = str(id_liga)
    
    if id_liga in df['id'].astype(str).values:
        idx = df.index[df['id'].astype(str) == id_liga].tolist()[0]
        ultima_data = df.at[idx, 'Data_Erro']
        strikes_atuais = df.at[idx, 'Strikes']
        
        if ultima_data != hoje:
            strikes_atuais += 1
            df.at[idx, 'Data_Erro'] = hoje
            df.at[idx, 'Strikes'] = strikes_atuais
            df.at[idx, 'Liga'] = nome_liga
            df.at[idx, 'País'] = pais
            df.to_csv(STRIKES_FILE, index=False)
            if strikes_atuais >= 2:
                salvar_na_blacklist(id_liga, pais, nome_liga)
                st.toast(f"🚫 VIP Banida: {nome_liga}")
    else:
        novo = pd.DataFrame([{
            'id': id_liga, 'País': pais, 'Liga': nome_liga, 
            'Data_Erro': hoje, 'Strikes': 1
        }])
        pd.concat([df, novo], ignore_index=True).to_csv(STRIKES_FILE, index=False)
        st.toast(f"⚠️ VIP Alertada: {nome_liga}")

def limpar_erro_vip(id_liga):
    if not os.path.exists(STRIKES_FILE): return
    df = pd.read_csv(STRIKES_FILE)
    if str(id_liga) in df['id'].astype(str).values:
        df = df[df['id'].astype(str) != str(id_liga)]
        df.to_csv(STRIKES_FILE, index=False)
        st.toast(f"✅ VIP Recuperada: {id_liga}")

def verificar_qualidade_dados(stats):
    if not stats: return False
    try:
        for time_stats in stats:
            for item in time_stats.get('statistics', []):
                if item['type'] in ['Shots on Goal', 'Total Shots']:
                    if item['value'] is not None: return True
        return False
    except: return False

def enviar_telegram_real(token, chat_ids, mensagem):
    if token and chat_ids:
        for cid in chat_ids.split(','):
            try:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                              data={"chat_id": cid.strip(), "text": mensagem, "parse_mode": "Markdown"}, timeout=5)
            except: pass

def enviar_relatorio_diario(token, chat_ids):
    historico = carregar_historico()
    msg = "📊 *RELATÓRIO DIÁRIO NEVES PRO* 📊\n\n"
    msg += f"📅 Data: {datetime.now().strftime('%d/%m/%Y')}\n"
    
    if not historico:
        msg += "💤 Nenhum sinal gerado hoje."
    else:
        msg += f"🚀 Sinais Enviados: {len(historico)}\n\n"
        for item in historico:
            msg += f"⏰ {item['Hora']} | {item['Jogo']}\n"
            msg += f"🎯 {item['Estrategia']} ({item['Liga']})\n"
            msg += "--------------------\n"
    
    msg += "✅ *Fim do monitoramento.*"
    enviar_telegram_real(token, chat_ids, msg)
    marcar_relatorio_como_enviado()
    st.toast("Relatório Enviado!")

def agora_brasil():
    return datetime.utcnow() - timedelta(hours=3)

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics PRO")
    
    with st.expander("✅ Status do Sistema", expanded=True):
        st.caption("Estratégias Ativas:")
        st.markdown("🟣 **A** - Porteira Aberta")
        st.markdown("🟢 **B** - Reação / Blitz")
        st.markdown("💰 **C** - Janela de Ouro")
        st.markdown("⚡ **D** - Gol Relâmpago")
    
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs:")
        
        st.markdown("---")
        if st.button("📤 Forçar Relatório"):
            enviar_relatorio_diario(tg_token, tg_chat_ids)

        INTERVALO = st.slider("Ciclo (seg):", 30, 300, 60)
        MODO_DEMO = st.checkbox("🛠️ Modo Simulação", value=False)
        
        st.markdown("---")
        col_res1, col_res2 = st.columns(2)
        with col_res1:
            if st.button("♻️ Reset"):
                st.session_state['alertas_enviados'] = set() 
                st.session_state['memoria_pressao'] = {}
                st.session_state['erros_vip'] = {}
                st.session_state['ligas_imunes'] = {}
                st.session_state['historico_sinais'] = []
                st.toast("Reiniciado!")
                time.sleep(1)
                st.rerun()
        
        with col_res2:
            if st.button("🗑️ Limpar DB"):
                for f in [BLACK_FILE, STRIKES_FILE, HIST_FILE, RELATORIO_FILE]:
                    if os.path.exists(f): os.remove(f)
                st.toast("Arquivos limpos!")
                time.sleep(1)
                st.rerun()

    st.markdown("---")
    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 4. API ---
@st.cache_data(ttl=3600)
def buscar_proximos(key):
    if not key and not MODO_DEMO: return []
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        data_hoje = agora_brasil().strftime('%Y-%m-%d')
        params = {"date": data_hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": key}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_dados(endpoint, params=None):
    if MODO_DEMO:
        return [
            {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 47}}, "league": {"id": 1, "name": "Liga Acréscimo", "country": "BR"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Fav (47')"}, "away": {"name": "Zebra"}}},
            {"fixture": {"id": 2, "status": {"short": "HT", "elapsed": 45}}, "league": {"id": 2, "name": "Liga Intervalo", "country": "BR"}, "goals": {"home": 0, "away": 0}, "teams": {"home": {"name": "Time A"}, "away": {"name": "Time B"}}}
        ]
    if not API_KEY: return []
    try:
        res = requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_stats(fid, demo_stage=0):
    if MODO_DEMO:
        if fid == 1: 
            return [{"team": {"name": "Home"}, "statistics": [{"type": "Total Shots", "value": 12}, {"type": "Shots on Goal", "value": 6}]}, {"team": {"name": "Away"}, "statistics": [{"type": "Total Shots", "value": 2}, {"type": "Shots on Goal", "value": 0}]}]
        return []
    return buscar_dados("statistics", {"fixture": fid})

# --- 5. MOMENTUM ---
def atualizar_momentum(fid, sog_h_atual, sog_a_atual):
    agora = datetime.now()
    janela_tempo = timedelta(minutes=7)
    
    if fid not in st.session_state['memoria_pressao']:
        st.session_state['memoria_pressao'][fid] = {'sog_h_total': sog_h_atual, 'sog_a_total': sog_a_atual, 'sog_h_timestamps': [], 'sog_a_timestamps': []}
        return 0, 0 

    memoria = st.session_state['memoria_pressao'][fid]
    
    delta_h = max(0, sog_h_atual - memoria['sog_h_total'])
    delta_a = max(0, sog_a_atual - memoria['sog_a_total'])
    
    for _ in range(delta_h): memoria['sog_h_timestamps'].append(agora)
    for _ in range(delta_a): memoria['sog_a_timestamps'].append(agora)
    
    memoria['sog_h_total'] = sog_h_atual
    memoria['sog_a_total'] = sog_a_atual
    
    memoria['sog_h_timestamps'] = [t for t in memoria['sog_h_timestamps'] if agora - t <= janela_tempo]
    memoria['sog_a_timestamps'] = [t for t in memoria['sog_a_timestamps'] if agora - t <= janela_tempo]
    
    st.session_state['memoria_pressao'][fid] = memoria
    return len(memoria['sog_h_timestamps']), len(memoria['sog_a_timestamps'])

# --- 6. PROCESSADOR ---
def processar_jogo(j, stats):
    f_id = j['fixture']['id']
    tempo = j['fixture']['status'].get('elapsed', 0)
    home = j['teams']['home']['name']
    away = j['teams']['away']['name']
    gh = j['goals']['home'] or 0
    ga = j['goals']['away'] or 0
    total_gols = gh + ga
    
    try:
        def get_val(team_idx, type_name):
            if not stats or len(stats) <= team_idx: return 0
            try:
                data = next((item for item in stats[team_idx]['statistics'] if item["type"] == type_name), None)
                return data['value'] if data and data['value'] else 0
            except: return 0

        sh_h = get_val(0, "Total Shots")
        sog_h = get_val(0, "Shots on Goal")
        sh_a = get_val(1, "Total Shots")
        sog_a = get_val(1, "Shots on Goal")
        total_chutes = sh_h + sh_a
        
        recentes_h, recentes_a = atualizar_momentum(f_id, sog_h, sog_a)
        
        if tempo <= 30 and total_gols >= 2:
            return {"tag": "🟣 Porteira Aberta", "ordem": "Adicionar em Múltipla Over Gols", "motivo": f"Jogo frenético ({gh}x{ga}).", "stats": f"{gh}x{ga}"}

        if 5 <= tempo <= 15:
            if (sog_h >= 1 or sog_a >= 1):
                return {"tag": "⚡ Gol Relâmpago", "ordem": "Apostar em Over 0.5 HT", "motivo": "Início elétrico.", "stats": f"Chutes Alvo: {sog_h + sog_a}"}

        if tempo <= 60:
            if (gh <= ga) and (recentes_h >= 2 or sh_h >= 6):
                acao = "⚠️ Jogo Aberto" if (recentes_a >= 1) else "✅ Apostar no Mandante"
                return {"tag": "🟢 Reação/Blitz", "ordem": acao, "motivo": f"{home} amassando!", "stats": f"Blitz: {recentes_h}"}
            if (ga <= gh) and (recentes_a >= 2 or sh_a >= 6):
                acao = "⚠️ Jogo Aberto" if (recentes_h >= 1) else "✅ Apostar no Visitante"
                return {"tag": "🟢 Reação/Blitz", "ordem": acao, "motivo": f"{away} amassando!", "stats": f"Blitz: {recentes_a}"}

        if 70 <= tempo <= 75:
            if total_chutes >= 18 and abs(gh - ga) <= 1:
                return {"tag": "💰 Janela de Ouro", "ordem": "Entrar em Mais 1.0 Gol (Asiático)", "motivo": "Pressão final.", "stats": f"Total Chutes: {total_chutes}"}

    except: return None
    return None

# --- 7. EXECUÇÃO ---
main_placeholder = st.empty()

if ROBO_LIGADO:
    df_black = carregar_blacklist()
    ids_bloqueados = df_black['id'].astype(str).values
    
    jogos_live = buscar_dados("fixtures", {"live": "all"})
    radar = []
    
    for j in jogos_live:
        l_id = str(j['league']['id'])
        
        if l_id in ids_bloqueados: continue
        
        f_id = j['fixture']['id']
        tempo = j['fixture']['status'].get('elapsed', 0)
        home = j['teams']['home']['name']
        away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"
        
        eh_intervalo = (j['fixture']['status']['short'] in ['HT', 'BT']) or (48 <= tempo <= 52)
        eh_aquecimento = (tempo < 5)
        eh_fim = (tempo > 80)
        dentro_janela = not (eh_intervalo or eh_aquecimento or eh_fim)
        
        sinal = None
        icone_visual = "👁️"
        
        if eh_aquecimento: icone_visual = "⏳"
        elif eh_intervalo: icone_visual = "💤"
        elif eh_fim: icone_visual = "🏁"
        
        if dentro_janela:
            stats = buscar_stats(f_id)
            stats_validos = verificar_qualidade_dados(stats)
            
            # LÓGICA DE INTEGRIDADE
            if not stats_validos and not MODO_DEMO:
                if int(l_id) in LIGAS_VIP:
                    if l_id not in st.session_state['ligas_imunes']:
                        registrar_erro_vip(l_id, j['league']['country'], j['league']['name'])
                elif l_id in st.session_state['ligas_imunes']: pass
                else:
                    # Regra dos 45 min para não VIPs
                    if tempo >= 45:
                        salvar_na_blacklist(l_id, j['league']['country'], j['league']['name'])
                        st.toast(f"🚫 Banida (45min sem dados): {j['league']['name']}")
            
            if stats_validos:
                st.session_state['ligas_imunes'][l_id] = {'País': j['league']['country'], 'Liga': j['league']['name']}
                if int(l_id) in LIGAS_VIP: limpar_erro_vip(l_id)

            sinal = processar_jogo(j, stats)
            
            if sinal:
                icone_visual = "✅"
                if f_id not in st.session_state['alertas_enviados']:
                    msg = (
                        f"🚨 *NEVES ANALYTICS PRO* 🚨\n\n"
                        f"⚽ *{home}* {placar} *{away}*\n"
                        f"🏆 {j['league']['name']}\n"
                        f"⏰ {tempo}'\n\n"
                        f"🧩 *Estratégia:* {sinal['tag']}\n"
                        f"⚠️ *ORDEM:* {sinal['ordem']}\n"
                        f"📈 *Dados:* {sinal['stats']}"
                    )
                    enviar_telegram_real(tg_token, tg_chat_ids, msg)
                    st.session_state['alertas_enviados'].add(f_id)
                    st.toast(f"Sinal Enviado: {sinal['tag']}")
                    
                    item_historico = {
                        "Data": agora_brasil().strftime('%Y-%m-%d'),
                        "Hora": agora_brasil().strftime('%H:%M'),
                        "Liga": j['league']['name'],
                        "Jogo": f"{home} x {away}",
                        "Placar": placar,
                        "Estrategia": sinal['tag'],
                        "Resultado": "Pendente"
                    }
                    salvar_sinal_historico(item_historico)
                    st.session_state['historico_sinais'].append(item_historico)

        mem = st.session_state['memoria_pressao'].get(f_id, {})
        rec_h = len(mem.get('sog_h_timestamps', [])) if mem else 0
        rec_a = len(mem.get('sog_a_timestamps', [])) if mem else 0
        info_mom = f" | ⚡ {rec_h}x{rec_a}" if (rec_h + rec_a) > 0 else ""

        radar.append({
            "Liga": j['league']['name'],
            "Jogo": f"{home} {placar} {away}",
            "Tempo": f"{tempo}'",
            "Status": f"{icone_visual} {sinal['tag'] if sinal else ''}{info_mom}"
        })

    prox_raw = buscar_proximos(API_KEY)
    prox_filtrado = []
    limite = (agora_brasil() - timedelta(minutes=15)).strftime('%H:%M')
    
    for p in prox_raw:
        lid = str(p['league']['id'])
        if lid in ids_bloqueados: continue
        if p['fixture']['status']['short'] != 'NS': continue
        if p['fixture']['date'][11:16] < limite: continue 
        prox_filtrado.append({
            "Hora": p['fixture']['date'][11:16], 
            "Liga": p['league']['name'], 
            "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"
        })

    # --- AUTOMAÇÃO DO RELATÓRIO ---
    if not radar and not prox_filtrado:
        if not verificar_relatorio_enviado():
            enviar_relatorio_diario(tg_token, tg_chat_ids)

    # --- EXIBIÇÃO SEGURA (SEM CRASH) ---
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.markdown('<div class="status-box status-active">🟢 SISTEMA DE MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        # Histórico recarregado
        historico_real = carregar_historico()
        
        # Métricas
        c1, c2, c3 = st.columns(3)
        c1.markdown(f'<div class="metric-card"><div class="metric-value">{len(historico_real)}</div><div class="metric-label">Sinais Hoje</div></div>', unsafe_allow_html=True)
        c2.markdown(f'<div class="metric-card"><div class="metric-value">{len(radar)}</div><div class="metric-label">Jogos Live</div></div>', unsafe_allow_html=True)
        c3.markdown(f'<div class="metric-card"><div class="metric-value">{len(st.session_state["ligas_imunes"])}</div><div class="metric-label">Ligas Seguras</div></div>', unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)

        # Dataframes Seguros (Convertidos para String para evitar erro)
        df_radar = pd.DataFrame(radar).astype(str)
        df_hist = pd.DataFrame(historico_real).astype(str)
        df_agenda = pd.DataFrame(prox_filtrado).astype(str)
        
        lista_imunes = list(st.session_state['ligas_imunes'].values())
        df_imunes = pd.DataFrame(lista_imunes).astype(str) if lista_imunes else pd.DataFrame(columns=['País', 'Liga'])
        
        df_obs = carregar_strikes_vip()
        if not df_obs.empty and 'País' in df_obs.columns: 
            df_obs = df_obs[['País', 'Liga', 'Data_Erro', 'Strikes']].astype(str)

        # Tabs
        t1, t2, t3, t4, t5, t6 = st.tabs([
            f"📡 Radar ({len(radar)})", 
            f"📜 Histórico ({len(historico_real)})",
            f"📅 Agenda ({len(prox_filtrado)})", 
            f"🚫 Blacklist ({len(df_black)})",
            f"🛡️ Seguras ({len(df_imunes)})",
            f"⚠️ Observação ({len(df_obs)})"
        ])
        
        with t1:
            if not df_radar.empty: st.dataframe(df_radar, use_container_width=True, hide_index=True)
            else: st.info("Monitorando...")
        with t2:
            if not df_hist.empty: st.dataframe(df_hist, use_container_width=True, hide_index=True)
            else: st.caption("Nenhum sinal hoje.")
        with t3:
            if not df_agenda.empty: st.dataframe(df_agenda.sort_values("Hora"), use_container_width=True, hide_index=True)
            else: st.caption("Sem jogos.")
        with t4: st.table(df_black.sort_values(['País', 'Liga']).astype(str)) if not df_black.empty else st.caption("Limpo.")
        with t5: st.table(df_imunes.sort_values(['País', 'Liga'])) if not df_imunes.empty else st.caption("Vazio.")
        with t6: st.table(df_obs) if not df_obs.empty else st.caption("Tudo ok.")

        relogio = st.empty()
        for i in range(INTERVALO, 0, -1):
            relogio.markdown(f'<div class="timer-text">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
    
    st.rerun()

else:
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure e ligue na lateral.")
