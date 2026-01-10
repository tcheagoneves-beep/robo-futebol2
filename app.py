import streamlit as st
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- 0. CONFIGURAÇÃO E LIMPEZA ---
st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")
st.cache_data.clear()

# Estilos idênticos ao original que você gostava
st.markdown("""
<style>
    .stApp {background-color: #0E1117; color: white;}
    .status-box {padding: 15px; border-radius: 10px; text-align: center; margin-bottom: 20px; font-weight: bold;}
    .status-active {background-color: #1F4025; color: #00FF00; border: 1px solid #00FF00;}
    .timer-text { font-size: 18px; color: #FFD700; text-align: center; font-weight: bold; margin-top: 30px; padding: 15px; border-top: 1px solid #333;}
    .strategy-card { background-color: #1e1e1e; padding: 15px; border-radius: 8px; margin-bottom: 10px; border-left: 4px solid #00FF00; }
    .strategy-title { color: #00FF00; font-weight: bold; font-size: 16px; margin-bottom: 5px; }
    .strategy-desc { font-size: 13px; color: #cccccc; line-height: 1.6; }
</style>
""", unsafe_allow_html=True)

# --- 1. ARQUIVOS E CONSTANTES ---
FILES = {
    'black': 'neves_blacklist.txt',
    'vip': 'neves_strikes_vip.txt',
    'hist': 'neves_historico_sinais.csv',
    'report': 'neves_status_relatorio.txt'
}

# Lista VIP Completa (Europa + Estaduais + Brasileirão)
LIGAS_VIP = [
    39, 78, 135, 140, 61, 2, 3, 9, 45, 48, # Europa
    71, 72, 13, 11, # Brasil/Latam
    474, 475, 476, 477, 478, 479, # Estaduais Principais
    606, 610, 628, 55, 143 # Outros Estaduais
]

# --- 2. FUNÇÕES DE ARQUIVO BLINDADAS (Para acabar com os erros) ---
def safe_load_csv(filepath, columns):
    """Lê o arquivo. Se estiver corrompido ou formato velho, reseta."""
    if not os.path.exists(filepath): return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(filepath)
        # Se faltar coluna, o arquivo é velho -> Reseta
        if not set(columns).issubset(df.columns):
            return pd.DataFrame(columns=columns)
        return df.fillna("").astype(str)
    except:
        return pd.DataFrame(columns=columns)

def carregar_dados():
    st.session_state['df_black'] = safe_load_csv(FILES['black'], ['id', 'País', 'Liga'])
    st.session_state['df_vip'] = safe_load_csv(FILES['vip'], ['id', 'País', 'Liga', 'Data_Erro', 'Strikes'])
    
    # Histórico: Carrega e converte para lista de dicionários
    df_h = safe_load_csv(FILES['hist'], ['Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado'])
    st.session_state['historico_sinais'] = df_h.to_dict('records')

def salvar_blacklist(id_liga, pais, nome_liga):
    novo = pd.DataFrame([{'id': str(id_liga), 'País': str(pais), 'Liga': str(nome_liga)}])
    try:
        df = safe_load_csv(FILES['black'], ['id', 'País', 'Liga'])
        if str(id_liga) not in df['id'].values:
            pd.concat([df, novo], ignore_index=True).to_csv(FILES['black'], index=False)
            st.session_state['df_black'] = safe_load_csv(FILES['black'], ['id', 'País', 'Liga']) # Recarrega
    except: pass

def salvar_historico_arquivo():
    if 'historico_sinais' in st.session_state and st.session_state['historico_sinais']:
        df = pd.DataFrame(st.session_state['historico_sinais'])
        df.to_csv(FILES['hist'], index=False)

# --- 3. INICIALIZAÇÃO ---
if 'ligas_imunes' not in st.session_state: st.session_state['ligas_imunes'] = {}
if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()
if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}
if 'historico_sinais' not in st.session_state: carregar_dados() # Carrega na primeira vez

# --- 4. TELEGRAM E RELATÓRIOS ---
def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for cid in ids:
        try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": cid, "text": msg, "parse_mode": "Markdown"}, timeout=3)
        except: pass

def verificar_greens_e_reds(jogos_live, token, chat_ids):
    """Verifica se algum sinal pendente bateu (Green) ou falhou (Red)"""
    atualizou = False
    for sinal in st.session_state['historico_sinais']:
        if sinal['Resultado'] == 'Pendente':
            # Procura o jogo na lista ao vivo ou finalizados
            jogo_live = next((j for j in jogos_live if j['teams']['home']['name'] in sinal['Jogo']), None)
            
            if jogo_live:
                # Placar atual
                gh = jogo_live['goals']['home'] or 0
                ga = jogo_live['goals']['away'] or 0
                placar_atual_total = gh + ga
                
                # Placar do sinal (ex: "1x0")
                try:
                    ph_sinal, pa_sinal = map(int, sinal['Placar_Sinal'].lower().split('x'))
                    total_sinal = ph_sinal + pa_sinal
                except: continue

                # Lógica Green: Saiu gol depois do sinal?
                if placar_atual_total > total_sinal:
                    sinal['Resultado'] = '✅ GREEN'
                    msg = f"✅ *GREEN CONFIRMADO!* ✅\n\n⚽ {sinal['Jogo']}\n📈 Saiu gol! ({gh}x{ga})\n🎯 {sinal['Estrategia']}"
                    enviar_telegram(token, chat_ids, msg)
                    atualizou = True
                
                # Lógica Red: Jogo acabou e não saiu gol
                elif jogo_live['fixture']['status']['short'] in ['FT', 'AET', 'PEN']:
                    sinal['Resultado'] = '❌ RED'
                    msg = f"❌ *RED* \n\n⚽ {sinal['Jogo']}\n📉 Não bateu.\n🎯 {sinal['Estrategia']}"
                    enviar_telegram(token, chat_ids, msg)
                    atualizou = True
    
    if atualizou:
        salvar_historico_arquivo()

def enviar_relatorio_final(token, chat_ids):
    hoje = datetime.now().strftime('%Y-%m-%d')
    sinais_hj = [s for s in st.session_state['historico_sinais'] if s['Data'] == hoje]
    
    if not sinais_hj: return 
    
    total = len(sinais_hj)
    greens = len([s for s in sinais_hj if 'GREEN' in s['Resultado']])
    reds = len([s for s in sinais_hj if 'RED' in s['Resultado']])
    pendentes = total - (greens + reds)
    
    winrate = (greens / (greens + reds)) * 100 if (greens + reds) > 0 else 0
    
    msg = f"📊 *FECHAMENTO DO MERCADO* 📊\nData: {hoje}\n\n"
    msg += f"🚀 Total Sinais: {total}\n"
    msg += f"✅ Greens: {greens}\n"
    msg += f"❌ Reds: {reds}\n"
    msg += f"⏳ Pendentes: {pendentes}\n"
    msg += f"📈 Aproveitamento: {winrate:.1f}%"
    
    enviar_telegram(token, chat_ids, msg)
    with open(FILES['report'], 'w') as f: f.write(hoje)

# --- 5. LÓGICA DE JOGO ---
def atualizar_momentum(fid, sog_h, sog_a):
    if fid not in st.session_state['memoria_pressao']:
        st.session_state['memoria_pressao'][fid] = {'sog_h': sog_h, 'sog_a': sog_a, 'h_times': [], 'a_times': []}
        return 0, 0
    mem = st.session_state['memoria_pressao'][fid]
    now = datetime.now()
    if sog_h > mem['sog_h']: mem['h_times'].extend([now] * (sog_h - mem['sog_h']))
    if sog_a > mem['sog_a']: mem['a_times'].extend([now] * (sog_a - mem['sog_a']))
    mem['h_times'] = [t for t in mem['h_times'] if now - t <= timedelta(minutes=7)]
    mem['a_times'] = [t for t in mem['a_times'] if now - t <= timedelta(minutes=7)]
    mem['sog_h'], mem['sog_a'] = sog_h, sog_a
    return len(mem['h_times']), len(mem['a_times'])

def processar_jogo(j, stats, tempo, placar):
    if not stats: return None
    
    # 1. Verifica Qualidade dos Dados (Segurança)
    tem_chutes = False
    sh_h, sog_h, sh_a, sog_a = 0, 0, 0, 0
    for idx, t in enumerate(stats):
        for s in t.get('statistics', []):
            if s['type'] == 'Total Shots' and s['value'] is not None:
                tem_chutes = True
                if idx == 0: sh_h = s['value']
                else: sh_a = s['value']
            if s['type'] == 'Shots on Goal' and s['value'] is not None:
                tem_chutes = True
                if idx == 0: sog_h = s['value']
                else: sog_a = s['value']
    
    if not tem_chutes: return None

    # 2. Dados do Jogo
    fid = j['fixture']['id']
    gh = j['goals']['home'] or 0
    ga = j['goals']['away'] or 0
    rec_h, rec_a = atualizar_momentum(fid, sog_h, sog_a)
    
    # 3. Estratégias
    if tempo <= 30 and (gh+ga) >= 2:
        return {"tag": "🟣 Porteira Aberta", "ordem": "🔥 ENTRADA SECA: Over Gols Limite", "stats": f"{gh}x{ga}"}
    
    if 5 <= tempo <= 15 and (sog_h+sog_a) >= 1:
        return {"tag": "⚡ Gol Relâmpago", "ordem": "Apostar Over 0.5 HT", "stats": f"Chutes Alvo: {sog_h+sog_a}"}
    
    if 70 <= tempo <= 75 and (sh_h+sh_a) >= 18 and abs(gh-ga) <= 1:
        return {"tag": "💰 Janela de Ouro", "ordem": "Over Gols Asiático", "stats": f"Total Chutes: {sh_h+sh_a}"}
    
    if tempo <= 60:
        if gh <= ga and (rec_h >= 2 or sh_h >= 8):
            return {"tag": "🟢 Blitz Casa", "ordem": "Gol Mandante ou Over", "stats": f"Pressão (7'): {rec_h}"}
        if ga <= gh and (rec_a >= 2 or sh_a >= 8):
            return {"tag": "🟢 Blitz Visitante", "ordem": "Gol Visitante ou Over", "stats": f"Pressão (7'): {rec_a}"}
            
    return None

# --- 6. SIDEBAR ---
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
        TG_TOKEN = st.text_input("Telegram Token:", type="password")
        TG_CHAT = st.text_input("Chat IDs (separar por vírgula):")
        INTERVALO = st.slider("Ciclo (seg):", 30, 300, 60)
        
        st.markdown("---")
        if st.button("📤 Forçar Relatório"):
            enviar_relatorio_final(TG_TOKEN, TG_CHAT)
            st.toast("Enviado!")
            
        st.markdown("---")
        if st.button("🗑️ Resetar Tudo (Correção)"):
            for f in FILES.values(): 
                if os.path.exists(f): os.remove(f)
            st.session_state['alertas_enviados'] = set()
            st.rerun()

    ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=False)

# --- 7. EXECUÇÃO ---
main_placeholder = st.empty()

if ROBO_LIGADO:
    carregar_dados() # Garante dados atualizados
    df_black = st.session_state['df_black']
    ids_black = df_black['id'].values if not df_black.empty else []
    
    # 1. API
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"live": "all", "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params=params, timeout=10).json()
        jogos = res.get('response', [])
    except: jogos = []

    # 2. Verifica Greens
    verificar_greens_e_reds(jogos, TG_TOKEN, TG_CHAT)

    radar = []
    
    for j in jogos:
        lid = str(j['league']['id'])
        if lid in ids_black: continue
        
        fid = j['fixture']['id']
        tempo = j['fixture']['status']['elapsed']
        home = j['teams']['home']['name']
        away = j['teams']['away']['name']
        placar = f"{j['goals']['home']}x{j['goals']['away']}"
        
        # Regra 45 Min (Banimento por falta de dados)
        # Se não tiver stats ainda, o próximo bloco vai falhar e cair no 'else'
        
        # Filtro de tempo
        if tempo > 80 or tempo < 5: continue
        
        # Busca Estatísticas
        try:
            url_s = "https://v3.football.api-sports.io/fixtures/statistics"
            res_s = requests.get(url_s, headers={"x-apisports-key": API_KEY}, params={"fixture": fid}).json()
            stats = res_s.get('response', [])
        except: stats = []
        
        sinal = processar_jogo(j, stats, tempo, placar)
        
        # Validação de Dados (45 Min)
        if not sinal and not stats and tempo >= 45 and int(lid) not in LIGAS_VIP:
            salvar_blacklist(lid, j['league']['country'], j['league']['name'])
            st.toast(f"🚫 Liga Banida (Sem dados): {j['league']['name']}")
            continue
        
        if stats: # Se tem dados, marca como segura
            st.session_state['ligas_imunes'][lid] = {'País': j['league']['country'], 'Liga': j['league']['name']}

        # Envio de Sinal
        status_visual = "👁️"
        if sinal:
            status_visual = "✅ " + sinal['tag']
            if fid not in st.session_state['alertas_enviados']:
                msg = f"🚨 *{sinal['tag']}*\n⚽ {home} {placar} {away}\n🏆 {j['league']['name']}\n⚠️ {sinal['ordem']}\n📈 {sinal['stats']}"
                enviar_telegram(TG_TOKEN, TG_CHAT, msg)
                st.session_state['alertas_enviados'].add(fid)
                
                # Salva Histórico
                novo_hist = {
                    "Data": datetime.now().strftime('%Y-%m-%d'),
                    "Hora": datetime.now().strftime('%H:%M'),
                    "Liga": j['league']['name'],
                    "Jogo": f"{home} x {away}",
                    "Placar_Sinal": placar,
                    "Estrategia": sinal['tag'],
                    "Resultado": "Pendente"
                }
                st.session_state['historico_sinais'].insert(0, novo_hist)
                salvar_historico_arquivo()
                st.toast(f"Sinal Enviado: {sinal['tag']}")

        radar.append({
            "Liga": j['league']['name'],
            "Jogo": f"{home} {placar} {away}",
            "Tempo": f"{tempo}'",
            "Status": status_visual
        })

    # Agenda
    agenda = []
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": datetime.now().strftime('%Y-%m-%d'), "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": API_KEY}, params=params).json()
        for p in res.get('response', []):
            if p['fixture']['status']['short'] == 'NS':
                agenda.append({
                    "Hora": p['fixture']['date'][11:16],
                    "Liga": p['league']['name'],
                    "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"
                })
    except: pass

    # Envio Automático de Relatório (Se tudo acabou)
    if not radar and not agenda:
        if not os.path.exists(FILES['report']) or open(FILES['report']).read() != datetime.now().strftime('%Y-%m-%d'):
            enviar_relatorio_final(TG_TOKEN, TG_CHAT)

    # --- RENDERIZAÇÃO ---
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.markdown('<div class="status-box status-active">🟢 SISTEMA DE MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)
        
        # Tabelas (com conversão para string para evitar erros)
        df_radar = pd.DataFrame(radar).astype(str)
        df_hist = pd.DataFrame(st.session_state['historico_sinais']).astype(str)
        df_agenda = pd.DataFrame(agenda).astype(str)
        df_black = st.session_state['df_black'].astype(str)
        df_imunes = pd.DataFrame(list(st.session_state['ligas_imunes'].values())).astype(str)
        df_obs = st.session_state['df_vip'].astype(str)

        # Tabs Originais
        t1, t2, t3, t4, t5, t6 = st.tabs([
            f"📡 Radar ({len(radar)})", 
            f"📅 Agenda ({len(agenda)})", 
            f"📜 Histórico ({len(st.session_state['historico_sinais'])})",
            f"🚫 Blacklist ({len(df_black)})",
            f"🛡️ Seguras ({len(df_imunes)})",
            f"⚠️ Observação ({len(df_obs)})"
        ])
        
        with t1: st.dataframe(df_radar, use_container_width=True, hide_index=True) if not df_radar.empty else st.info("Buscando jogos...")
        with t2: st.dataframe(df_agenda.sort_values('Hora'), use_container_width=True, hide_index=True) if not df_agenda.empty else st.caption("Sem jogos.")
        with t3: st.dataframe(df_hist, use_container_width=True, hide_index=True) if not df_hist.empty else st.caption("Nenhum sinal hoje.")
        with t4: st.table(df_black.sort_values(['País', 'Liga'])) if not df_black.empty else st.caption("Limpo.")
        with t5: st.table(df_imunes) if not df_imunes.empty else st.caption("Vazio.")
        with t6: st.table(df_obs) if not df_obs.empty else st.caption("Tudo ok.")

        # Manual Restaurado
        with st.expander("📘 Manual de Inteligência (Detalhes Técnicos)", expanded=False):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("""
                <div class="strategy-card"><div class="strategy-title">🟣 A - Porteira Aberta</div><div class="strategy-desc">Jogo frenético < 30'.<br>Ação: Múltipla Over Gols.</div></div>
                <div class="strategy-card"><div class="strategy-title">🟢 B - Reação / Blitz</div><div class="strategy-desc">Fav perdendo e amassando.<br>Ação: Apostar no Gol ou Mais 1 Gol.</div></div>
                """, unsafe_allow_html=True)
            with c2:
                st.markdown("""
                <div class="strategy-card"><div class="strategy-title">💰 C - Janela de Ouro</div><div class="strategy-desc">Reta final (70-75') com pressão.<br>Ação: Over Limite (Gol Asiático).</div></div>
                <div class="strategy-card"><div class="strategy-title">⚡ D - Gol Relâmpago</div><div class="strategy-desc">Início elétrico (5-15').<br>Ação: Over 0.5 HT.</div></div>
                """, unsafe_allow_html=True)

        # Timer Restaurado no Final
        relogio = st.empty()
        for i in range(INTERVALO, 0, -1):
            relogio.markdown(f'<div class="timer-text">Próxima varredura em {i}s</div>', unsafe_allow_html=True)
            time.sleep(1)
    
    st.rerun()

else:
    with main_placeholder.container():
        st.title("❄️ Neves Analytics PRO")
        st.info("💡 Robô em espera. Configure e ligue na lateral.")
