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

# --- 2. CONSTANTES E ARQUIVOS ---
DB_FILE = 'neves_dados.txt'
BLACK_FILE = 'neves_blacklist.txt'
STRIKES_FILE = 'neves_strikes_vip.txt'
HIST_FILE = 'neves_historico_sinais.csv'
RELATORIO_FILE = 'neves_status_relatorio.txt'

# --- 🛡️ LISTA VIP ---
LIGAS_VIP = [
    39, 78, 135, 140, 61, 2, 3, 9, 45, 48, 
    71, 72, 13, 11, 
    474, 475, 476, 477, 478, 479, 
    606, 610, 628, 55, 143 
]

# --- 3. FUNÇÕES DE ARQUIVO BLINDADAS (CORREÇÃO DE ERRO) ---
def carregar_blacklist():
    cols_esperadas = ['id', 'País', 'Liga']
    if not os.path.exists(BLACK_FILE): return pd.DataFrame(columns=cols_esperadas)
    
    try:
        df = pd.read_csv(BLACK_FILE)
        # Se o arquivo for velho e não tiver a coluna País, recria do zero para não travar
        if 'País' not in df.columns:
            return pd.DataFrame(columns=cols_esperadas)
        return df
    except:
        return pd.DataFrame(columns=cols_esperadas)

def carregar_strikes_vip():
    cols_esperadas = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes']
    if not os.path.exists(STRIKES_FILE): return pd.DataFrame(columns=cols_esperadas)
    
    try:
        df = pd.read_csv(STRIKES_FILE)
        if 'Liga' not in df.columns or 'País' not in df.columns:
            return pd.DataFrame(columns=cols_esperadas)
        return df
    except:
        return pd.DataFrame(columns=cols_esperadas)

def carregar_historico():
    if not os.path.exists(HIST_FILE): return []
    try:
        df = pd.read_csv(HIST_FILE)
        hoje = datetime.now().strftime('%Y-%m-%d')
        # Filtra e converte tudo para string para evitar erro visual
        df = df[df['Data'] == hoje].astype(str)
        return df.to_dict('records')
    except: return []

# --- INICIALIZAÇÃO DE MEMÓRIA ---
if 'ligas_imunes' in st.session_state:
    # Correção de tipo se a memória estiver suja
    if st.session_state['ligas_imunes'] and isinstance(list(st.session_state['ligas_imunes'].values())[0], str):
        st.session_state['ligas_imunes'] = {} 

if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'erros_vip' not in st.session_state: st.session_state['erros_vip'] = {}
if 'ligas_imunes' not in st.session_state: st.session_state['ligas_imunes'] = {} 
if 'erros_por_liga' not in st.session_state: st.session_state['erros_por_liga'] = {}
if 'historico_sinais' not in st.session_state: st.session_state['historico_sinais'] = carregar_historico()

# --- FUNÇÕES LÓGICAS ---
def salvar_na_blacklist(id_liga, pais, nome_liga):
    df = carregar_blacklist()
    if str(id_liga) not in df['id'].astype(str).values:
        novo = pd.DataFrame([{'id': str(id_liga), 'País': str(pais), 'Liga': str(nome_liga)}])
        pd.concat([df, novo], ignore_index=True).to_csv(BLACK_FILE, index=False)

def registrar_erro_vip(id_liga, pais, nome_liga):
    df = carregar_strikes_vip()
    hoje = datetime.now().strftime('%Y-%m-%d')
    id_liga = str(id_liga)
    
    if id_liga in df['id'].astype(str).values:
        idx = df.index[df['id'].astype(str) == id_liga].tolist()[0]
        ultima_data = df.at[idx, 'Data_Erro']
        strikes_atuais = int(df.at[idx, 'Strikes'])
        
        if ultima_data != hoje:
            strikes_atuais += 1
            df.at[idx, 'Data_Erro'] = hoje
            df.at[idx, 'Strikes'] = strikes_atuais
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

def salvar_sinal_historico(sinal_dict):
    df_novo = pd.DataFrame([sinal_dict])
    if not os.path.exists(HIST_FILE):
        df_novo.to_csv(HIST_FILE, index=False)
    else:
        df_novo.to_csv(HIST_FILE, mode='a', header=False, index=False)

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
    # Correção: Divide a string de IDs por vírgula para enviar a todos
    if token and chat_ids:
        lista_ids = [id.strip() for id in chat_ids.split(',') if id.strip()]
        for cid in lista_ids:
            try:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", 
                              data={"chat_id": cid, "text": mensagem, "parse_mode": "Markdown"}, timeout=5)
            except: pass

def verificar_relatorio_enviado():
    if not os.path.exists(RELATORIO_FILE): return False
    try:
        with open(RELATORIO_FILE, 'r') as f:
            return f.read().strip() == datetime.now().strftime('%Y-%m-%d')
    except: return False

def marcar_relatorio_como_enviado():
    with open(RELATORIO_FILE, 'w') as f:
        f.write(datetime.now().strftime('%Y-%m-%d'))

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

# --- 4. API ---
@st.cache_data(ttl=3600)
def buscar_proximos(key):
    if not key and not st.session_state.get('MODO_DEMO', False): return []
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        data_hoje = agora_brasil().strftime('%Y-%m-%d')
        params = {"date": data_hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": key}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_dados(endpoint, params=None, api_key=None):
    if st.session_state.get('MODO_DEMO', False):
        return [
            {"fixture": {"id": 1, "status": {"short": "1H", "elapsed": 47}}, "league": {"id": 1, "name": "Liga Teste", "country": "BR"}, "goals": {"home": 0, "away": 1}, "teams": {"home": {"name": "Fav"}, "away": {"name": "Zebra"}}},
        ]
    if not api_key: return []
    try:
        res = requests.get(f"https://v3.football.api-sports.io/{endpoint}", headers={"x-apisports-key": api_key}, params=params, timeout=10).json()
        return res.get('response', [])
    except: return []

def buscar_stats(fid, api_key=None):
    if st.session_state.get('MODO_DEMO', False): return []
    return buscar_dados("statistics", {"fixture": fid}, api_key)

# --- 5. LÓGICA ESTRATÉGICA ---
def atualizar_momentum(fid, sog_h, sog_a):
    agora = datetime.now()
    if fid not in st.session_state['memoria_pressao']:
        st.session_state['memoria_pressao'][fid] = {'sog_h': sog_h, 'sog_a': sog_a, 'h_times': [], 'a_times': []}
        return 0, 0
    
    mem = st.session_state['memoria_pressao'][fid]
    delta_h = max(0, sog_h - mem['sog_h'])
    delta_a = max(0, sog_a - mem['sog_a'])
    
    for _ in range(delta_h): mem['h_times'].append(agora)
    for _ in range(delta_a): mem['a_times'].append(agora)
    
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a
    mem['h_times'] = [t for t in mem['h_times'] if agora - t <= timedelta(minutes=7)]
    mem['a_times'] = [t for t in mem['a_times'] if agora - t <= timedelta(minutes=7)]
    
    st.session_state['memoria_pressao'][fid] = mem
    return len(mem['h_times']), len(mem['a_times'])

def processar_jogo(j, stats):
    f_id = j['fixture']['id']
    tempo = j['fixture']['status'].get('elapsed', 0)
    home = j['teams']['home']['name']
    away = j['teams']['away']['name']
    gh = j['goals']['home'] or 0
    ga = j['goals']['away'] or 0
    
    try:
        # Extração de dados segura
        def get_val(idx, nome):
            if not stats or len(stats) <= idx: return 0
            for i in stats[idx].get('statistics', []):
                if i['type'] == nome: return i['value'] or 0
            return 0

        sh_h = get_val(0, "Total Shots")
        sog_h = get_val(0, "Shots on Goal")
        sh_a = get_val(1, "Total Shots")
        sog_a = get_val(1, "Shots on Goal")
        
        recentes_h, recentes_a = atualizar_momentum(f_id, sog_h, sog_a)
        
        # Estratégias
        if tempo <= 30 and (gh + ga) >= 2:
            return {"tag": "🟣 Porteira Aberta", "ordem": "🔥 ENTRADA SECA: Over Gols Limite (Asiático)", "motivo": f"Jogo frenético ({gh}x{ga}).", "stats": f"{gh}x{ga}"}

        if 5 <= tempo <= 15 and (sog_h + sog_a) >= 1:
            return {"tag": "⚡ Gol Relâmpago", "ordem": "Apostar em Over 0.5 HT", "motivo": "Início elétrico.", "stats": f"Chutes Alvo: {sog_h + sog_a}"}

        if tempo <= 60:
            if (gh <= ga) and (recentes_h >= 2 or sh_h >= 6):
                acao = "⚠️ Jogo Aberto" if (recentes_a >= 1) else "✅ Apostar no Mandante"
                return {"tag": "🟢 Reação/Blitz", "ordem": acao, "motivo": f"{home} amassando!", "stats": f"Blitz: {recentes_h}"}
            if (ga <= gh) and (recentes_a >= 2 or sh_a >= 6):
                acao = "⚠️ Jogo Aberto" if (recentes_h >= 1) else "✅ Apostar no Visitante"
                return {"tag": "🟢 Reação/Blitz", "ordem": acao, "motivo": f"{away} amassando!", "stats": f"Blitz: {recentes_a}"}

        if 70 <= tempo <= 75 and (sh_h + sh_a) >= 18 and abs(gh - ga) <= 1:
            return {"tag": "💰 Janela de Ouro", "ordem": "Entrar em Mais 1.0 Gol (Asiático)", "motivo": "Pressão final.", "stats": f"Total: {sh_h + sh_a}"}

    except: return None
    return None

# --- 6. INTERFACE SIDEBAR ---
with st.sidebar:
    st.title("❄️ Neves Analytics PRO")
    
    with st.expander("✅ Status do Sistema", expanded=True):
        st.markdown("🟣 **A** - Porteira Aberta")
        st.markdown("🟢 **B** - Reação / Blitz")
        st.markdown("💰 **C** - Janela de Ouro")
        st.markdown("⚡ **D** - Gol Relâmpago")
    
    with st.expander("⚙️ Configurações", expanded=False):
        API_KEY = st.text_input("Chave API-SPORTS:", type="password")
        tg_token = st.text_input("Telegram Token:", type="password")
        tg_chat_ids = st.text_input("Chat IDs (Separar por vírgula):")
        
        st.markdown("---")
        if st.button("📤 Forçar Relatório"):
            enviar_relatorio_diario(tg_token, tg_chat_ids)

        INTERVALO = st.slider("Ciclo (seg):", 30, 300, 60)
        st.session_state['MODO_DEMO'] = st.checkbox("🛠️ Modo Simulação", value=False)
        
        st.markdown("---")
        c1, c2 = st.columns(2)
        if c1.button("♻️ Reset RAM"):
            st.session_state['alertas_enviados'] = set()
            st.rerun()
        if c2.button("🗑️ Reset Total"):
            for f in [BLACK_FILE, STRIKES_FILE, HIST_FILE, RELATORIO_FILE]:
                if os.path.exists(f): os.remove(f)
            st.session_state['historico_sinais'] = []
            st.rerun()

    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 7. LOOP PRINCIPAL ---
main_placeholder = st.empty()

if ROBO_LIGADO:
    # 1. Carrega dados persistentes (com correção automática)
    df_black = carregar_blacklist()
    ids_bloqueados = df_black['id'].astype(str).values
    
    # 2. Busca jogos
    jogos_live = buscar_dados("fixtures", {"live": "all"}, API_KEY)
    radar = []
    
    for j in jogos_live:
        l_id = str(j['league']['id'])
        if l_id in ids_bloqueados: continue
        
        f_id = j['fixture']['id']
        tempo = j['fixture']['status'].get('elapsed', 0)
        home = j['teams']['home']['name']
        away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"
        
        # Filtros de Tempo
        eh_intervalo = (j['fixture']['status']['short'] in ['HT', 'BT']) or (48 <= tempo <= 52)
        eh_aquecimento = (tempo < 5)
        eh_fim = (tempo > 80)
        dentro_janela = not (eh_intervalo or eh_aquecimento or eh_fim)
        
        sinal = None
        icone = "👁️"
        
        if eh_aquecimento: icone = "⏳"
        elif eh_intervalo: icone = "💤"
        elif eh_fim: icone = "🏁"
        
        if dentro_janela:
            stats = buscar_stats(f_id, API_KEY)
            stats_validos = verificar_qualidade_dados(stats)
            
            # Validação de Integridade e Banimento
            if not stats_validos and not st.session_state['MODO_DEMO']:
                if int(l_id) in LIGAS_VIP:
                    if l_id not in st.session_state['ligas_imunes']:
                        registrar_erro_vip(l_id, j['league']['country'], j['league']['name'])
                elif l_id in st.session_state['ligas_imunes']: pass
                else:
                    if tempo >= 45:
                        salvar_na_blacklist(l_id, j['league']['country'], j['league']['name'])
            
            if stats_validos:
                st.session_state['ligas_imunes'][l_id] = {'País': j['league']['country'], 'Liga': j['league']['name']}
                if int(l_id) in LIGAS_VIP: limpar_erro_vip(l_id)

            # Processamento de Estratégia
            sinal = processar_jogo(j, stats)
            
            if sinal:
                icone = "✅"
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
                    
                    # Salva
                    item = {
                        "Data": agora_brasil().strftime('%Y-%m-%d'),
                        "Hora": agora_brasil().strftime('%H:%M'),
                        "Liga": j['league']['name'],
                        "Jogo": f"{home} x {away}",
                        "Placar": placar,
                        "Estrategia": sinal['tag'],
                        "Resultado": "Pendente"
                    }
                    salvar_sinal_historico(item)
                    st.session_state['historico_sinais'].append(item)

        # Monta linha do radar
        mem = st.session_state['memoria_pressao'].get(f_id, {})
        mom = f" | ⚡ {len(mem.get('h_times', []))}x{len(mem.get('a_times', []))}" if mem else ""
        
        radar.append({
            "Liga": j['league']['name'],
            "Jogo": f"{home} {placar} {away}",
            "Tempo": f"{tempo}'",
            "Status": f"{icone} {sinal['tag'] if sinal else ''}{mom}"
        })

    # Agenda
    prox_raw = buscar_proximos(API_KEY)
    agenda = []
    limite = (agora_brasil() - timedelta(minutes=15)).strftime('%H:%M')
    for p in prox_raw:
        if str(p['league']['id']) not in ids_bloqueados:
            if p['fixture']['status']['short'] == 'NS' and p['fixture']['date'][11:16] >= limite:
                agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": p['league']['name'], "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})

    # Relatório Automático
    if not radar and not agenda:
        if not verificar_relatorio_enviado():
            enviar_relatorio_diario(tg_token, tg_chat_ids)

    # --- RENDERIZAÇÃO FINAL (LAYOUT) ---
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.markdown('<div class="status-box status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        # Painel Minimizável
        with st.expander("📊 Painel de Controle (Métricas)", expanded=False):
            hist_real = carregar_historico()
            c1, c2, c3 = st.columns(3)
            c1.markdown(f'<div class="metric-card"><div class="metric-value">{len(hist_real)}</div><div class="metric-label">Sinais Hoje</div></div>', unsafe_allow_html=True)
            c2.markdown(f'<div class="metric-card"><div class="metric-value">{len(radar)}</div><div class="metric-label">Jogos Live</div></div>', unsafe_allow_html=True)
            c3.markdown(f'<div class="metric-card"><div class="metric-value">{len(st.session_state["ligas_imunes"])}</div><div class="metric-label">Seguras</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Preparação de Tabelas Seguras (String)
        df_radar = pd.DataFrame(radar).astype(str)
        df_hist = pd.DataFrame(hist_real).astype(str)
        df_agenda = pd.DataFrame(agenda).astype(str)
        df_black = df_black.astype(str)
        
        # Abas
        t1, t2, t3, t4, t5 = st.tabs([
            f"📡 Radar ({len(radar)})", 
            f"📜 Histórico ({len(hist_real)})",
            f"📅 Agenda ({len(agenda)})", 
            f"🚫 Blacklist ({len(df_black)})",
            "⚙️ Dados"
        ])
        
        with t1: st.dataframe(df_radar, use_container_width=True, hide_index=True) if not df_radar.empty else st.info("Aguardando jogos...")
        with t2: st.dataframe(df_hist, use_container_width=True, hide_index=True) if not df_hist.empty else st.caption("Sem sinais hoje.")
        with t3: st.dataframe(df_agenda.sort_values("Hora"), use_container_width=True, hide_index=True) if not df_agenda.empty else st.caption("Agenda vazia.")
        with t4: st.table(df_black.sort_values(['País', 'Liga'])) if not df_black.empty else st.caption("Limpa.")
        with t5:
            st.caption("Ligas Seguras (Whitelist)")
            st.json(st.session_state['ligas_imunes'])
            st.caption("Erros VIP (Observação)")
            st.table(carregar_strikes_vip().astype(str))

        relogio = st.empty()
        for i in range(INTERVALO, 0, -1):
            relogio.markdown(f'<div class="timer-text">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
    
    st.rerun()

else:
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure e ligue na lateral.")
