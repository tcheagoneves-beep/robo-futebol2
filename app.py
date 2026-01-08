import streamlit as st
import pandas as pd
import requests
from datetime import datetime

# --- CONFIGURAÇÃO INICIAL ---
st.set_page_config(page_title="Caçador de Gols - Bot", layout="wide", page_icon="⚽")

# Barra lateral
with st.sidebar:
    st.header("⚙️ Configurações")
    API_KEY = st.text_input("Sua API Key (RapidAPI):", type="password")
    st.markdown("[Pegar chave grátis aqui](https://rapidapi.com/api-sports/api/api-football)")
    
    # Ligas Focadas (Ex: Emirados, Brasil, Inglaterra, etc.)
    LIGAS_ALVO = [201, 71, 39, 140, 61, 78, 135, 307] 

# --- FUNÇÕES DE API (CONEXÃO) ---

def buscar_jogos_do_dia():
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"date": datetime.today().strftime('%Y-%m-%d')}
    headers = {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try:
        return requests.get(url, headers=headers, params=querystring).json().get('response', [])
    except: return []

def buscar_classificacao(league_id, season):
    url = "https://api-football-v1.p.rapidapi.com/v3/standings"
    querystring = {"league": league_id, "season": season}
    headers = {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try:
        data = requests.get(url, headers=headers, params=querystring).json()
        return data['response'][0]['league']['standings'][0] if data['response'] else []
    except: return []

def buscar_stats_live(fixture_id):
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    headers = {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"}
    try:
        return requests.get(url, headers=headers, params={"fixture": fixture_id}).json().get('response', [])
    except: return []

# --- CÉREBRO DO ROBÔ (ESTRATÉGIA DE GOLS) ---

def analisar_pre_jogo(rank_casa, rank_fora, total_times):
    # Procuramos jogos "Desequilibrados" ou "Desesperados" -> Tendência de Gols
    motivo = "Neutro"
    
    # 1. Favorito Claro (Ex: 1º contra 15º) -> Chance de Goleada
    if (rank_casa <= 3 and rank_fora >= 10) or (rank_fora <= 3 and rank_casa >= 10):
        motivo = "🔥 ALTA: Favorito x Zebra (Chance de Goleada)"
    # 2. Jogo de "Vida ou Morte" (Z4) -> Jogo aberto/nervoso
    elif rank_casa >= (total_times - 3) and rank_fora >= (total_times - 3):
        motivo = "⚠️ TENSÃO: Duelo Z4 (Defesas Fracas)"
    # 3. Disputa de Topo -> Ambos marcam
    elif rank_casa <= 5 and rank_fora <= 5:
        motivo = "⚔️ CLÁSSICO: Topo da Tabela (Qualidade Alta)"
        
    return motivo

def analisar_live_gols(tempo, placar, stats_casa, stats_fora):
    # Extrair dados numéricos
    def get_val(stats, key):
        val = stats.get(key, 0)
        if isinstance(val, str): return int(val.replace('%', ''))
        return val or 0

    posse_casa = get_val(stats_casa, 'Ball Possession')
    atq_p_casa = get_val(stats_casa, 'Dangerous Attacks')
    chutes_casa = get_val(stats_casa, 'Total Shots')
    
    atq_p_fora = get_val(stats_fora, 'Dangerous Attacks')
    chutes_fora = get_val(stats_fora, 'Total Shots')
    
    soma_chutes = chutes_casa + chutes_fora
    recomendacao = "Observar"
    cor_alerta = "blue"

    # --- LÓGICA DE TEMPOS (1º ou 2º Tempo) ---
    
    # CENÁRIO 1: GOL NO PRIMEIRO TEMPO (HT)
    # Entre 15 e 40 minutos + Jogo 0x0 + Pressão
    if 15 <= tempo <= 40 and ("0-0" in placar or "0-1" in placar or "1-0" in placar):
        
        # Regra de Pressão: Zebra amassando ou Jogo muito aberto
        if (atq_p_casa > 10 or atq_p_fora > 10) and soma_chutes >= 5:
            recomendacao = "💰 APOSTAR: GOL NO 1º TEMPO (Over 0.5 HT)"
            cor_alerta = "green"
            
            # Refinamento: Zebra Perigosa
            if (posse_casa < 40 and atq_p_casa > atq_p_fora):
                recomendacao += " (Zebra Perigosa!)"

    # CENÁRIO 2: GOL NO FINAL (FT)
    # Entre 70 e 85 minutos + Jogo Empatado ou diferença de 1 gol
    elif 70 <= tempo <= 88:
        # Se o jogo continua intenso (Muitos ataques perigosos acumulados)
        if (atq_p_casa + atq_p_fora) > 60: # Jogo movimentado
             recomendacao = "💰 APOSTAR: GOL NO FINAL (Over Limite)"
             cor_alerta = "red"
        
    return recomendacao, cor_alerta

# --- INTERFACE VISUAL ---

st.title("⚽ Robô Trader: Especialista em Gols")
tab1, tab2 = st.tabs(["1. Filtro do Dia (Pré)", "2. Radar Ao Vivo (Live)"])

# --- ABA 1: FILTRAR JOGOS ---
with tab1:
    if st.button("Buscar Jogos com Potencial de Gols"):
        if not API_KEY: st.error("Coloque a API Key na lateral!")
        else:
            with st.spinner("Analisando potenciais de goleada..."):
                todos = buscar_jogos_do_dia()
                alvos = []
                bar = st.progress(0)
                
                # Focamos nos primeiros 30 jogos compatíveis para economizar API no teste
                count = 0
                for jogo in todos:
                    if jogo['fixture']['status']['short'] not in ['FT', 'AET', 'PEN']:
                        lid = jogo['league']['id']
                        if lid in LIGAS_ALVO:
                            tabela = buscar_classificacao(lid, jogo['league']['season'])
                            if tabela:
                                id_c = jogo['teams']['home']['id']
                                id_f = jogo['teams']['away']['id']
                                r_c = next((x['rank'] for x in tabela if x['team']['id']==id_c), 10)
                                r_f = next((x['rank'] for x in tabela if x['team']['id']==id_f), 10)
                                
                                motivo = analisar_pre_jogo(r_c, r_f, len(tabela))
                                if "Neutro" not in motivo:
                                    alvos.append({
                                        'id': jogo['fixture']['id'],
                                        'jogo': f"{jogo['teams']['home']['name']} x {jogo['teams']['away']['name']}",
                                        'horario': jogo['fixture']['date'][11:16],
                                        'motivo': motivo
                                    })
                            count += 1
                            bar.progress(min(count/30, 1.0))
                            if count >= 30: break
                
                st.session_state['lista_gols'] = alvos
                st.success(f"{len(alvos)} Jogos selecionados para monitorar gols!")

    if 'lista_gols' in st.session_state:
        st.dataframe(pd.DataFrame(st.session_state['lista_gols']))

# --- ABA 2: MONITORAR AO VIVO ---
with tab2:
    if st.button("📡 Rastrear Gols Agora"):
        if 'lista_gols' not in st.session_state: st.warning("Rode a Aba 1 primeiro!")
        else:
            st.write("Monitorando pressão nos jogos selecionados...")
            for alvo in st.session_state['lista_gols']:
                stats_raw = buscar_stats_live(alvo['id'])
                if stats_raw:
                    # Tentar pegar o placar atual e tempo (Isso requereria outra chamada na API completa, 
                    # mas vamos assumir que o usuário vê na Bet365 e aqui ele vê a estatística)
                    # *Na versão paga da API, isso vem direto. Na free, as vezes tem delay.
                    
                    stats_casa = {i['type']: i['value'] for i in stats_raw[0]['statistics']}
                    stats_fora = {i['type']: i['value'] for i in stats_raw[1]['statistics']}
                    
                    # Vamos SIMULAR o tempo baseado na hora do jogo para o exemplo funcionar
                    # Na real: você usaria jogo['fixture']['status']['elapsed']
                    # Mas como estamos chamando stats direto, vamos mostrar os dados para decisão:
                    
                    st.markdown(f"### {alvo['jogo']}")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Ataques Perigosos (Casa)", stats_casa.get('Dangerous Attacks', 0))
                    c2.metric("Ataques Perigosos (Fora)", stats_fora.get('Dangerous Attacks', 0))
                    c3.metric("Total Chutes", (stats_casa.get('Total Shots',0) or 0) + (stats_fora.get('Total Shots',0) or 0))
                    
                    # Alerta Visual
                    atq_casa = stats_casa.get('Dangerous Attacks', 0) or 0
                    atq_fora = stats_fora.get('Dangerous Attacks', 0) or 0
                    
                    if (atq_casa + atq_fora) > 30: 
                        st.error("🔥 JOGO QUENTE! Muitos ataques perigosos. Alta chance de Gol.")
                    else:
                        st.info("Jogo morno. Aguardar aquecer.")
                    
                    st.divider()
                else:
                    st.caption(f"Sem dados ao vivo ainda para: {alvo['jogo']}")