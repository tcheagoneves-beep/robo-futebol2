import streamlit as st

import pandas as pd

import requests

import time

import os

import threading

import random

from concurrent.futures import ThreadPoolExecutor, as_completed

import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt

import plotly.express as px

import plotly.graph_objects as go

import io

from datetime import datetime, timedelta

import pytz

from streamlit_gsheets import GSheetsConnection

import google.generativeai as genai

import json

import re

import firebase_admin

from firebase_admin import credentials, firestore

import math

try:
    import betfairlightweight
    from betfairlightweight import filters as bf_filters
    BF_AVAILABLE = True
except ImportError:
    BF_AVAILABLE = False

# ==============================================================================

# 1. CONFIGURAÇÃO INICIAL E CSS

# ==============================================================================

st.set_page_config(page_title="Neves Analytics PRO", layout="wide", page_icon="❄️")

placeholder_root = st.empty()

st.markdown("""

<style>

    .stApp {background-color: #0E1117; color: white;}

    .main .block-container { max-width: 100%; padding: 1rem 1rem 80px 1rem; }

    .metric-box { background-color: #1A1C24; border: 1px solid #333; border-radius: 8px; padding: 10px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.2); margin-bottom: 10px; }

    .metric-title {font-size: 10px; color: #aaaaaa; text-transform: uppercase; margin-bottom: 2px;}

    .metric-value {font-size: 20px; font-weight: bold; color: #00FF00;}

    .metric-sub {font-size: 10px; color: #cccccc;}

    .status-active { background-color: #1F4025; color: #00FF00; border: 1px solid #00FF00; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 5px; font-weight: bold; font-size: 14px;}

    .status-error { background-color: #3B1010; color: #FF4B4B; border: 1px solid #FF4B4B; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 5px; font-weight: bold; font-size: 14px;}

    .status-warning { background-color: #3B3B10; color: #FFFF00; border: 1px solid #FFFF00; padding: 8px; border-radius: 6px; text-align: center; margin-bottom: 5px; font-weight: bold; font-size: 14px;}

    .stButton button { width: 100%; height: 55px !important; font-size: 18px !important; font-weight: bold !important; background-color: #262730; border: 1px solid #4e4e4e; color: white; border-radius: 8px; }

    .stButton button:hover { border-color: #00FF00; color: #00FF00; }

    .footer-timer { position: fixed; left: 0; bottom: 0; width: 100%; background-color: #0E1117; color: #FFD700; text-align: center; padding: 10px; font-size: 12px; border-top: 1px solid #333; z-index: 99999; box-shadow: 0 -2px 10px rgba(0,0,0,0.5); }

    .stDataFrame { font-size: 12px; }

    #MainMenu {visibility: hidden;}

    footer {visibility: hidden;}

</style>

""", unsafe_allow_html=True)

# ==============================================================================

# 2. INICIALIZAÇÃO DE VARIÁVEIS

# ==============================================================================

if 'TG_TOKEN' not in st.session_state: st.session_state['TG_TOKEN'] = ""

if 'TG_CHAT' not in st.session_state: st.session_state['TG_CHAT'] = ""

if 'API_KEY' not in st.session_state: st.session_state['API_KEY'] = ""

if 'ROBO_LIGADO' not in st.session_state: st.session_state.ROBO_LIGADO = False

if 'last_db_update' not in st.session_state: st.session_state['last_db_update'] = 0

if 'last_static_update' not in st.session_state: st.session_state['last_static_update'] = 0

if 'stake_padrao' not in st.session_state: st.session_state['stake_padrao'] = 10.0

if 'banca_inicial' not in st.session_state: st.session_state['banca_inicial'] = 100.0
if 'banca_atual' not in st.session_state: st.session_state['banca_atual'] = st.session_state.get('banca_inicial', 100.0)
if 'regras_aprendidas' not in st.session_state: st.session_state['regras_aprendidas'] = {}

if 'api_usage' not in st.session_state: st.session_state['api_usage'] = {'used': 0, 'limit': 75000}

if 'data_api_usage' not in st.session_state: st.session_state['data_api_usage'] = datetime.now(pytz.utc).date()

if 'gemini_usage' not in st.session_state: st.session_state['gemini_usage'] = {'used': 0, 'limit': 10000}

if 'alvos_do_dia' not in st.session_state: st.session_state['alvos_do_dia'] = {}
if 'kelly_modo' not in st.session_state: st.session_state['kelly_modo'] = 'fracionario'
if 'ia_profunda_ativada' not in st.session_state: st.session_state['ia_profunda_ativada'] = True  # Default ON
if 'odds_history' not in st.session_state: st.session_state['odds_history'] = {}
if 'banca_updates' not in st.session_state: st.session_state['banca_updates'] = set()
if 'tier2_calls_hoje' not in st.session_state: st.session_state['tier2_calls_hoje'] = 0
if 'sniper_tarde_enviado' not in st.session_state: st.session_state['sniper_tarde_enviado'] = False
if 'sniper_noite_enviado' not in st.session_state: st.session_state['sniper_noite_enviado'] = False
if 'tier2_max_dia' not in st.session_state: st.session_state['tier2_max_dia'] = 500

if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()

if 'var_avisado_cache' not in st.session_state: st.session_state['var_avisado_cache'] = set()

if 'multiplas_enviadas' not in st.session_state: st.session_state['multiplas_enviadas'] = set()

if 'memoria_pressao' not in st.session_state: st.session_state['memoria_pressao'] = {}

if 'controle_stats' not in st.session_state: st.session_state['controle_stats'] = {}

if 'jogos_salvos_bigdata' not in st.session_state: st.session_state['jogos_salvos_bigdata'] = set()

if 'jogos_salvos_bigdata_carregados' not in st.session_state: st.session_state['jogos_salvos_bigdata_carregados'] = False

if 'ia_bloqueada_ate' not in st.session_state: st.session_state['ia_bloqueada_ate'] = None

if 'last_check_date' not in st.session_state: st.session_state['last_check_date'] = ""

if 'bi_enviado' not in st.session_state: st.session_state['bi_enviado'] = False

if 'ia_enviada' not in st.session_state: st.session_state['ia_enviada'] = False

if 'financeiro_enviado' not in st.session_state: st.session_state['financeiro_enviado'] = False

if 'bigdata_enviado' not in st.session_state: st.session_state['bigdata_enviado'] = False

if 'matinal_enviado' not in st.session_state: st.session_state['matinal_enviado'] = False

if 'precisa_salvar' not in st.session_state: st.session_state['precisa_salvar'] = False

if 'BLOQUEAR_SALVAMENTO' not in st.session_state: st.session_state['BLOQUEAR_SALVAMENTO'] = False

if 'total_bigdata_count' not in st.session_state: st.session_state['total_bigdata_count'] = 0

# --- VARIÁVEIS PARA MÚLTIPLAS, TRADING E ALAVANCAGEM ---

if 'multipla_matinal_enviada' not in st.session_state: st.session_state['multipla_matinal_enviada'] = False

if 'multiplas_live_cache' not in st.session_state: st.session_state['multiplas_live_cache'] = {}

if 'multiplas_pendentes' not in st.session_state: st.session_state['multiplas_pendentes'] = []

if 'alternativos_enviado' not in st.session_state: st.session_state['alternativos_enviado'] = False

if 'alavancagem_enviada' not in st.session_state: st.session_state['alavancagem_enviada'] = False

# [NOVO] Variáveis de controle do Trading (Drop Odds)

if 'drop_enviado_12' not in st.session_state: st.session_state['drop_enviado_12'] = False

if 'drop_enviado_16' not in st.session_state: st.session_state['drop_enviado_16'] = False

# --- BETFAIR EXCHANGE ---
if 'bf_ativo' not in st.session_state: st.session_state['bf_ativo'] = False
if 'bf_client' not in st.session_state: st.session_state['bf_client'] = None
if 'bf_session_token' not in st.session_state: st.session_state['bf_session_token'] = None
if 'bf_auto_bet' not in st.session_state: st.session_state['bf_auto_bet'] = False
if 'bf_max_stake' not in st.session_state: st.session_state['bf_max_stake'] = 10.0
if 'bf_dry_run' not in st.session_state: st.session_state['bf_dry_run'] = True
if 'bf_bets_hoje' not in st.session_state: st.session_state['bf_bets_hoje'] = []
if 'bf_so_aprovados' not in st.session_state: st.session_state['bf_so_aprovados'] = True
if 'bf_total_apostado' not in st.session_state: st.session_state['bf_total_apostado'] = 0.0
if 'bf_limit_dia' not in st.session_state: st.session_state['bf_limit_dia'] = 100.0
if 'bf_saldo' not in st.session_state: st.session_state['bf_saldo'] = 0.0
if 'bf_max_pct' not in st.session_state: st.session_state['bf_max_pct'] = 5
if 'bf_username_input' not in st.session_state: st.session_state['bf_username_input'] = ''
if 'bf_password_input' not in st.session_state: st.session_state['bf_password_input'] = ''
if 'bf_app_key_input' not in st.session_state: st.session_state['bf_app_key_input'] = ''
if 'bf_erro' not in st.session_state: st.session_state['bf_erro'] = ''

# -------------------------------------------------

db_firestore = None

if "FIREBASE_CONFIG" in st.secrets:

    try:

        if not firebase_admin._apps:

            fb_creds = json.loads(st.secrets["FIREBASE_CONFIG"])

            cred = credentials.Certificate(fb_creds)

            firebase_admin.initialize_app(cred)

        db_firestore = firestore.client()

    except Exception as e: st.error(f"Erro Firebase: {e}")

IA_ATIVADA = False

try:

    if "GEMINI_KEY" in st.secrets:

        genai.configure(api_key=st.secrets["GEMINI_KEY"])

        model_ia = genai.GenerativeModel('gemini-2.0-flash')

        # [NOVO] Wrapper com retry para 429 (Resource Exhausted)
        def gemini_safe_call(prompt, generation_config=None, max_retries=3):
            """Chama Gemini com retry automático para erro 429."""
            for attempt in range(max_retries):
                try:
                    if generation_config:
                        resp = model_ia.generate_content(prompt, generation_config=generation_config)
                    else:
                        resp = model_ia.generate_content(prompt)
                    st.session_state['gemini_usage']['used'] += 1
                    return resp
                except Exception as e:
                    if '429' in str(e) or 'Resource' in str(e):
                        wait = (attempt + 1) * 10  # 10s, 20s, 30s
                        print(f"[GEMINI] ⚠️ 429 Rate Limit. Aguardando {wait}s... (tentativa {attempt+1}/{max_retries})")
                        time.sleep(wait)
                    else:
                        raise e
            print("[GEMINI] ❌ 429 persistente após 3 tentativas")
            return None

        IA_ATIVADA = True

except: IA_ATIVADA = False

# ==============================================================================
# BETFAIR EXCHANGE — MÓDULO DE INTEGRAÇÃO
# ==============================================================================

BF_CONECTADO = False

def _bf_secret(key, default=""):
    """Busca secret da Betfair (suporta [BETFAIR] section ou BF_ flat keys)."""
    # 1. Tenta seção [BETFAIR] do TOML
    try:
        val = st.secrets["BETFAIR"][key]
        if val: return str(val)
    except: pass
    # 2. Tenta flat key BF_KEY
    try:
        val = st.secrets[f"BF_{key}"]
        if val: return str(val)
    except: pass
    # 3. Fallback: input manual do UI
    input_key = f"bf_{key.lower()}_input"
    return st.session_state.get(input_key, default)

def _bf_has_secrets():
    """Verifica se credenciais Betfair estão configuradas (secrets ou UI)."""
    try:
        _ = st.secrets["BETFAIR"]["USERNAME"]
        return True
    except: pass
    try:
        _ = st.secrets["BF_USERNAME"]
        return True
    except: pass
    return bool(st.session_state.get('bf_username_input', ''))

def _fix_pem(pem_text):
    """Corrige PEM cujo header/footer está colado no body (sem newline)."""
    if not pem_text: return ""
    pem_text = pem_text.strip()
    import re
    # Detecta header colado: "-----BEGIN XXX-----MIIB..." (sem \n entre header e body)
    # Separa header do body
    pem_text = re.sub(r'(-----BEGIN [^-]+-----)\s*([A-Za-z0-9+/])', r'\1\n\2', pem_text)
    # Separa body do footer
    pem_text = re.sub(r'([A-Za-z0-9+/=])\s*(-----END [^-]+-----)', r'\1\n\2', pem_text)
    # Agora extrai partes
    m = re.match(r'(-----BEGIN [^-]+-----)\n(.*?)\n(-----END [^-]+-----)', pem_text, re.DOTALL)
    if not m: return pem_text
    header = m.group(1)
    body = m.group(2).replace('\n', '').replace('\r', '').replace(' ', '')
    footer = m.group(3)
    # Quebra body em linhas de 64 chars (padrão PEM)
    body_lines = [body[i:i+64] for i in range(0, len(body), 64)]
    return header + '\n' + '\n'.join(body_lines) + '\n' + footer + '\n'

def bf_login():
    """Login na Betfair via API (cert-based ou REST interativo)."""
    global BF_CONECTADO
    erros = []  # Coleta todos os erros pra mostrar na UI
    if not BF_AVAILABLE:
        st.session_state['bf_erro'] = "betfairlightweight não instalado"
        return False
    try:
        username = _bf_secret("USERNAME")
        password = _bf_secret("PASSWORD")
        app_key = _bf_secret("APP_KEY")
        if not all([username, password, app_key]):
            faltam = []
            if not username: faltam.append("USERNAME")
            if not password: faltam.append("PASSWORD")
            if not app_key: faltam.append("APP_KEY")
            st.session_state['bf_erro'] = f"Faltam: {', '.join(faltam)}"
            return False
        st.session_state['bf_erro'] = ""
        print(f"[BETFAIR] 🔄 Login: {username[:5]}*** | AppKey: {app_key[:6]}***")

        client = None

        # ═══ TENTATIVA 1: Login via CERTIFICADO ═══
        cert_crt = _bf_secret("CERT_CRT")
        cert_key_pem = _bf_secret("CERT_KEY")
        if cert_crt and cert_key_pem and "BEGIN" in str(cert_crt):
            try:
                import tempfile
                cert_dir = tempfile.mkdtemp()
                crt_path = os.path.join(cert_dir, "client-2048.crt")
                key_path = os.path.join(cert_dir, "client-2048.key")
                crt_fixed = _fix_pem(str(cert_crt))
                key_fixed = _fix_pem(str(cert_key_pem))
                with open(crt_path, 'w') as f: f.write(crt_fixed)
                with open(key_path, 'w') as f: f.write(key_fixed)
                print(f"[BETFAIR] 🔐 CRT: {len(crt_fixed)}b, KEY: {len(key_fixed)}b")

                resp = requests.post(
                    "https://identitysso-cert.betfair.com/api/certlogin",
                    headers={"X-Application": app_key, "Content-Type": "application/x-www-form-urlencoded"},
                    data={"username": username, "password": password},
                    cert=(crt_path, key_path),
                    timeout=20
                )
                data = resp.json()
                login_status = data.get("loginStatus", "NO_STATUS")
                print(f"[BETFAIR] 🔐 Cert: {login_status}")
                if login_status == "SUCCESS":
                    client = betfairlightweight.APIClient(
                        username=username, password=password, app_key=app_key,
                        certs=cert_dir, lightweight=True
                    )
                    client.session_token = data["sessionToken"]
                    print("[BETFAIR] ✅ Cert login OK!")
                else:
                    erros.append(f"Cert: {login_status}")
            except Exception as e_cert:
                erros.append(f"Cert: {str(e_cert)[:80]}")
                print(f"[BETFAIR] ⚠️ Cert: {e_cert}")
        else:
            erros.append("Sem certificados configurados")

        # ═══ TENTATIVA 2: Login REST (sem cert) ═══
        if client is None:
            print("[BETFAIR] 🔄 Tentando REST...")
            for url in ["https://identitysso.betfair.com/api/login", "https://identitysso.betfair.com.br/api/login"]:
                try:
                    resp = requests.post(
                        url,
                        headers={"X-Application": app_key, "Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
                        data={"username": username, "password": password},
                        timeout=15
                    )
                    data = resp.json()
                    status = data.get("status", data.get("loginStatus", ""))
                    error = data.get("error", data.get("loginStatus", ""))
                    dominio = url.split('/')[2]
                    print(f"[BETFAIR] 📡 {dominio}: status={status}, error={error}")
                    if status == "SUCCESS":
                        client = betfairlightweight.APIClient(
                            username=username, password=password, app_key=app_key,
                            lightweight=True
                        )
                        client.session_token = data.get("token", data.get("sessionToken"))
                        print(f"[BETFAIR] ✅ REST login OK via {dominio}!")
                        break
                    else:
                        erros.append(f"REST {dominio}: {error}")
                except Exception as e_url:
                    erros.append(f"REST: {str(e_url)[:60]}")

        if client is None:
            st.session_state['bf_erro'] = " | ".join(erros) if erros else "Falhou sem detalhes"
            return False

        # ═══ CONECTADO! ═══
        st.session_state['bf_client'] = client
        st.session_state['bf_session_token'] = getattr(client, 'session_token', '')
        st.session_state['bf_ativo'] = True
        st.session_state['bf_erro'] = ""
        BF_CONECTADO = True
        try:
            funds = client.account.get_account_funds()
            saldo = float(funds.get("availableToBetBalance", 0) if isinstance(funds, dict) else getattr(funds, 'available_to_bet_balance', 0))
            st.session_state['bf_saldo'] = saldo
            print(f"[BETFAIR] 💰 Saldo: R$ {saldo:.2f}")
        except Exception as e:
            print(f"[BETFAIR] ⚠️ Saldo: {e}")
            st.session_state['bf_saldo'] = 0.0
        return True
    except Exception as e:
        st.session_state['bf_erro'] = f"Erro: {str(e)[:120]}"
        print(f"[BETFAIR] ❌ {e}")
        import traceback; traceback.print_exc()
        BF_CONECTADO = False
        return False

def bf_keep_alive():
    """Mantém sessão Betfair ativa e atualiza saldo."""
    client = st.session_state.get('bf_client')
    if not client: return False
    try:
        client.keep_alive()
        # Atualiza saldo
        try:
            funds = client.account.get_account_funds()
            saldo = float(funds.get("availableToBetBalance", 0) if isinstance(funds, dict) else getattr(funds, 'available_to_bet_balance', 0))
            st.session_state['bf_saldo'] = saldo
        except: pass
        return True
    except:
        return bf_login()

def bf_buscar_evento(home_name, away_name, data_jogo=None):
    """Busca evento na Betfair pelo nome dos times."""
    client = st.session_state.get('bf_client')
    if not client: return None
    try:
        from difflib import SequenceMatcher
        filtro_tempo = {}
        if data_jogo:
            inicio = f"{data_jogo}T00:00:00Z"
            fim = f"{data_jogo}T23:59:59Z"
            filtro_tempo = {"from": inicio, "to": fim}

        market_filter = {"eventTypeIds": ["1"], "textQuery": home_name}
        if filtro_tempo:
            market_filter["marketStartTime"] = filtro_tempo

        eventos = client.betting.list_events(filter=market_filter)
        if not eventos: return None

        melhor_score = 0
        melhor_evento = None
        busca = f"{home_name} v {away_name}".lower()

        for ev in eventos:
            nome_ev = ev.get("event", {}).get("name", "") if isinstance(ev, dict) else getattr(ev, 'event', None)
            if nome_ev and hasattr(nome_ev, 'name'):
                nome_ev = nome_ev.name
            elif isinstance(nome_ev, dict):
                nome_ev = nome_ev.get("name", "")
            nome_lower = str(nome_ev).lower()
            score = SequenceMatcher(None, busca, nome_lower).ratio()
            # Bonus se ambos os times aparecem
            if home_name.lower().split()[0] in nome_lower: score += 0.2
            if away_name.lower().split()[0] in nome_lower: score += 0.2
            if score > melhor_score:
                melhor_score = score
                melhor_evento = ev
        if melhor_score >= 0.45:
            ev_id = None
            if isinstance(melhor_evento, dict):
                ev_id = melhor_evento.get("event", {}).get("id")
            else:
                ev_id = getattr(getattr(melhor_evento, 'event', None), 'id', None)
            print(f"[BETFAIR] 🔍 Match: {home_name} vs {away_name} → EventID {ev_id} (score: {melhor_score:.2f})")
            return ev_id
        return None
    except Exception as e:
        print(f"[BETFAIR] ❌ Erro buscar evento: {e}")
        return None

# Mapa: tipo de estratégia → market type da Betfair
BF_MARKET_MAP = {
    "OVER": "OVER_UNDER_25",     # Over 2.5 (default, ajustado por linha)
    "UNDER": "OVER_UNDER_25",
    "RESULTADO": "MATCH_ODDS",
    "FAVORITO": "MATCH_ODDS",
    "ESCANTEIOS": "CORNER_MATCH_BETS",
    "GOLS": "OVER_UNDER_15",     # Over 1.5 (mais seguro)
}

def bf_mapear_market_type(estrategia, palpite=""):
    """Determina o market type Betfair baseado na estratégia."""
    palpite_lower = palpite.lower()
    # Detecta linha de gols no palpite
    if "2.5" in palpite: return "OVER_UNDER_25"
    if "1.5" in palpite: return "OVER_UNDER_15"
    if "3.5" in palpite: return "OVER_UNDER_35"
    if "0.5" in palpite and "gol" in palpite_lower: return "OVER_UNDER_05"
    if "escanteio" in palpite_lower or "corner" in palpite_lower: return "CORNER_MATCH_BETS"
    if "vitória" in palpite_lower or "vencedor" in palpite_lower or "favorito" in palpite_lower: return "MATCH_ODDS"
    if "under" in palpite_lower or "menos" in palpite_lower: return "OVER_UNDER_25"
    # Fallback pelo tipo da estratégia
    tipo = classificar_tipo_estrategia(estrategia)
    return BF_MARKET_MAP.get(tipo, "OVER_UNDER_15")

def bf_buscar_mercado(event_id, market_type="OVER_UNDER_25"):
    """Busca mercado específico de um evento na Betfair."""
    client = st.session_state.get('bf_client')
    if not client or not event_id: return None, None
    try:
        market_filter = {"eventIds": [str(event_id)], "marketTypeCodes": [market_type]}
        catalogos = client.betting.list_market_catalogue(
            filter=market_filter,
            market_projection=["RUNNER_DESCRIPTION"],
            max_results=5
        )
        if not catalogos: return None, None
        mercado = catalogos[0] if isinstance(catalogos, list) else catalogos
        market_id = mercado.get("marketId") if isinstance(mercado, dict) else getattr(mercado, 'market_id', None)
        runners = mercado.get("runners", []) if isinstance(mercado, dict) else getattr(mercado, 'runners', [])
        print(f"[BETFAIR] 📊 Mercado: {market_type} → {market_id} ({len(runners)} runners)")
        return market_id, runners
    except Exception as e:
        print(f"[BETFAIR] ❌ Erro buscar mercado: {e}")
        return None, None

def bf_selecionar_runner(runners, tipo_aposta, home_name="", away_name=""):
    """Seleciona o runner correto para a aposta."""
    if not runners: return None
    try:
        for r in runners:
            if isinstance(r, dict):
                nome = r.get("runnerName", "").lower()
                sel_id = r.get("selectionId")
            else:
                nome = getattr(r, 'runner_name', "").lower()
                sel_id = getattr(r, 'selection_id', None)
            if tipo_aposta in ["OVER", "GOLS"]:
                if "over" in nome: return sel_id
            elif tipo_aposta == "UNDER":
                if "under" in nome: return sel_id
            elif tipo_aposta in ["RESULTADO", "FAVORITO"]:
                if home_name and home_name.lower().split()[0] in nome: return sel_id
        # Fallback: primeiro runner
        if isinstance(runners[0], dict):
            return runners[0].get("selectionId")
        return getattr(runners[0], 'selection_id', None)
    except: return None

def bf_obter_melhor_odd(market_id, selection_id):
    """Obtém a melhor odd disponível para back."""
    client = st.session_state.get('bf_client')
    if not client or not market_id: return 0.0
    try:
        price_filter = {"priceProjection": {"priceData": ["EX_BEST_OFFERS"]}}
        books = client.betting.list_market_book(
            market_ids=[market_id],
            price_projection={"priceData": ["EX_BEST_OFFERS"]}
        )
        if not books: return 0.0
        book = books[0] if isinstance(books, list) else books
        runners_book = book.get("runners", []) if isinstance(book, dict) else getattr(book, 'runners', [])
        for r in runners_book:
            r_id = r.get("selectionId") if isinstance(r, dict) else getattr(r, 'selection_id', None)
            if str(r_id) == str(selection_id):
                backs = r.get("ex", {}).get("availableToBack", []) if isinstance(r, dict) else getattr(getattr(r, 'ex', None), 'available_to_back', [])
                if backs:
                    best = backs[0]
                    price = best.get("price", 0) if isinstance(best, dict) else getattr(best, 'price', 0)
                    return float(price)
        return 0.0
    except Exception as e:
        print(f"[BETFAIR] ❌ Erro obter odd: {e}")
        return 0.0

def bf_apostar(market_id, selection_id, stake, odd_limite=1.01):
    """Coloca aposta BACK na Betfair Exchange."""
    client = st.session_state.get('bf_client')
    if not client or not market_id or not selection_id: return None
    # Verificações de segurança
    if st.session_state.get('bf_dry_run', True):
        resultado = {"status": "DRY_RUN", "market_id": market_id, "selection_id": selection_id, "stake": stake, "odd": odd_limite}
        print(f"[BETFAIR] 🧪 DRY RUN: Apostaria R${stake:.2f} @{odd_limite:.2f} no mercado {market_id}")
        st.session_state['bf_total_apostado'] = st.session_state.get('bf_total_apostado', 0) + stake
        st.session_state['bf_bets_hoje'].append({
            "hora": get_time_br().strftime('%H:%M'),
            "market": market_id,
            "stake": stake,
            "odd": odd_limite,
            "status": "DRY_RUN"
        })
        return resultado
    if st.session_state.get('bf_total_apostado', 0) + stake > st.session_state.get('bf_limit_dia', 100.0):
        print(f"[BETFAIR] ⚠️ Limite diário atingido (R${st.session_state['bf_limit_dia']:.2f})")
        return None
    try:
        order = client.betting.place_orders(
            market_id=market_id,
            instructions=[{
                "selectionId": int(selection_id),
                "handicap": "0",
                "side": "BACK",
                "orderType": "LIMIT",
                "limitOrder": {
                    "size": round(stake, 2),
                    "price": round(odd_limite, 2),
                    "persistenceType": "LAPSE"
                }
            }]
        )
        if isinstance(order, dict):
            status = order.get("status", "UNKNOWN")
        else:
            status = getattr(order, 'status', 'UNKNOWN')
        st.session_state['bf_total_apostado'] += stake
        st.session_state['bf_bets_hoje'].append({
            "hora": get_time_br().strftime('%H:%M'),
            "market": market_id,
            "stake": stake,
            "odd": odd_limite,
            "status": str(status)
        })
        print(f"[BETFAIR] {'✅' if 'SUCCESS' in str(status) else '⚠️'} Aposta: R${stake:.2f} @{odd_limite:.2f} → {status}")
        return order
    except Exception as e:
        print(f"[BETFAIR] ❌ Erro ao apostar: {e}")
        return None

def bf_executar_aposta_sinal(home, away, estrategia, ordem, odd_val, kelly_stake, opiniao_db, token, chat_ids):
    """
    Pipeline completo: valida jogo → busca mercado → calcula stake → aposta.
    Stake: Arriscado=1% saldo BF | Aprovado=Kelly (max bf_max_pct% saldo BF)
    """
    if not st.session_state.get('bf_ativo') or not st.session_state.get('bf_auto_bet'):
        return False
    # Filtro: se bf_so_aprovados está ligado, só aposta em Aprovados
    if st.session_state.get('bf_so_aprovados') and opiniao_db != "Aprovado":
        print(f"[BETFAIR] ⏭️ Sinal {opiniao_db} (não aprovado), pulando.")
        return False
    try:
        hoje = get_time_br().strftime('%Y-%m-%d')
        # ═══ 1. BUSCA E VALIDAÇÃO DO JOGO ═══
        # Busca pelo nome do mandante na Betfair
        event_id = bf_buscar_evento(home, away, hoje)
        if not event_id:
            # Tenta variações (remove acentos, abreviações)
            home_clean = home.replace("FC", "").replace("SC", "").strip()
            event_id = bf_buscar_evento(home_clean, away, hoje)
        if not event_id:
            msg_nf = f"🅱️ ⚠️ <b>Jogo não encontrado na Betfair</b>\n⚽ {home} vs {away}\n🎯 {estrategia}\n<i>Aposta não executada (match não validado)</i>"
            enviar_telegram(token, chat_ids, msg_nf)
            return False

        # ═══ 2. DETERMINA MERCADO ═══
        tipo = classificar_tipo_estrategia(estrategia)
        market_type = bf_mapear_market_type(estrategia, ordem)
        market_id, runners = bf_buscar_mercado(event_id, market_type)
        # Fallback progressivo
        if not market_id and market_type != "OVER_UNDER_15":
            market_id, runners = bf_buscar_mercado(event_id, "OVER_UNDER_15")
        if not market_id and market_type != "OVER_UNDER_25":
            market_id, runners = bf_buscar_mercado(event_id, "OVER_UNDER_25")
        if not market_id:
            print(f"[BETFAIR] ❌ Nenhum mercado encontrado para {home} vs {away}")
            return False

        # ═══ 3. SELECIONA RUNNER ═══
        selection_id = bf_selecionar_runner(runners, tipo, home, away)
        if not selection_id:
            print(f"[BETFAIR] ❌ Runner não encontrado")
            return False

        # ═══ 4. ODD REAL DA BETFAIR ═══
        bf_odd = bf_obter_melhor_odd(market_id, selection_id)
        if bf_odd < 1.01:
            print(f"[BETFAIR] ❌ Odd indisponível (mercado seco)")
            return False

        # ═══ 5. CÁLCULO DE STAKE (REGRA DO TIAGO) ═══
        # Saldo da Betfair (atualizado no login/keep_alive)
        saldo_bf = st.session_state.get('bf_saldo', 0)
        if saldo_bf <= 0:
            # Tenta atualizar saldo
            try:
                client = st.session_state.get('bf_client')
                if client:
                    funds = client.account.get_account_funds()
                    saldo_bf = float(funds.get("availableToBetBalance", 0) if isinstance(funds, dict) else getattr(funds, 'available_to_bet_balance', 0))
                    st.session_state['bf_saldo'] = saldo_bf
            except: pass
        if saldo_bf <= 0:
            saldo_bf = float(st.session_state.get('banca_atual', 100.0))  # Fallback: banca do robô

        max_pct = st.session_state.get('bf_max_pct', 5) / 100.0  # Ex: 5 → 0.05

        if opiniao_db == "Aprovado":
            # Kelly stake (limitado a max_pct% do saldo)
            stake_final = min(kelly_stake, saldo_bf * max_pct) if kelly_stake > 0 else saldo_bf * 0.02
        else:
            # Arriscado/Neutro: 1% fixo do saldo
            stake_final = saldo_bf * 0.01

        stake_final = round(max(stake_final, 2.0), 2)  # Mínimo Betfair = R$2

        # Verifica limite diário
        if st.session_state.get('bf_total_apostado', 0) + stake_final > st.session_state.get('bf_limit_dia', 100.0):
            print(f"[BETFAIR] ⚠️ Limite diário atingido")
            return False

        # ═══ 6. APOSTA! ═══
        resultado = bf_apostar(market_id, selection_id, stake_final, bf_odd)

        # ═══ 7. NOTIFICA TELEGRAM ═══
        dry_tag = "🧪 TESTE" if st.session_state.get('bf_dry_run') else "💰 REAL"
        pct_usado = (stake_final / saldo_bf * 100) if saldo_bf > 0 else 0
        if resultado:
            status_txt = resultado.get('status', 'OK') if isinstance(resultado, dict) else str(getattr(resultado, 'status', 'OK'))
            msg_bf = (
                f"🅱️ <b>BETFAIR {dry_tag}</b>\n"
                f"⚽ {home} vs {away}\n"
                f"🎯 {estrategia} | {market_type}\n"
                f"🤖 IA: {opiniao_db}\n"
                f"💰 Stake: R$ {stake_final:.2f} ({pct_usado:.1f}% saldo) @ {bf_odd:.2f}\n"
                f"📊 Status: {status_txt}\n"
                f"🏦 Saldo BF: R$ {saldo_bf:.2f} | Dia: R$ {st.session_state.get('bf_total_apostado', 0):.2f}"
            )
            enviar_telegram(token, chat_ids, msg_bf)
            return True
        return False
    except Exception as e:
        print(f"[BETFAIR] ❌ Pipeline erro: {e}")
        return False

# Tenta login automático se secrets estão configuradas
if BF_AVAILABLE:
    try:
        if _bf_has_secrets() and not st.session_state.get('bf_ativo'):
            bf_login()
            BF_CONECTADO = st.session_state.get('bf_ativo', False)
    except: pass

conn = st.connection("gsheets", type=GSheetsConnection)

COLS_HIST = ['FID', 'Data', 'Hora', 'Liga', 'Jogo', 'Placar_Sinal', 'Estrategia', 'Resultado', 'HomeID', 'AwayID', 'Odd', 'Odd_Atualizada', 'Opiniao_IA', 'Probabilidade', 'Stake_Recomendado_RS']

COLS_SAFE = ['id', 'País', 'Liga', 'Motivo', 'Strikes', 'Jogos_Erro']

COLS_OBS = ['id', 'País', 'Liga', 'Data_Erro', 'Strikes', 'Jogos_Erro']

COLS_BLACK = ['id', 'País', 'Liga', 'Motivo']

LIGAS_TABELA = [71, 72, 39, 140, 141, 135, 78, 79, 94]

DB_CACHE_TIME = 60

STATIC_CACHE_TIME = 600

# Mapa para referência

MAPA_LOGICA_ESTRATEGIAS = {

    "🟣 Porteira Aberta": "Over Gols",

    "⚡ Gol Relâmpago": "Over HT",

    "💰 Janela de Ouro": "Over Limite",

    "🟢 Blitz Casa": "Over Gols",

    "🟢 Blitz Visitante": "Over Gols",

    "🔥 Massacre": "Over HT",

    "⚔️ Choque Líderes": "Over HT",

    "🥊 Briga de Rua": "Over HT",

    "❄️ Jogo Morno": "Under HT/FT",

    "💎 GOLDEN BET": "Over Limite",

    "🏹 Tiroteio Elite": "Over Gols",

    "⚡ Contra-Ataque Letal": "Back Zebra",

    "💎 Sniper Final": "Over Limite",

    "🦁 Back Favorito (Nettuno)": "Back Vencedor",

    "🔫 Lay Goleada": "Over Limite",

    "👴 Estratégia do Vovô": "Back Favorito (Segurança)",

    "🟨 Sniper de Cartões": "Over Cartões",

    "🧤 Muralha (Defesas)": "Over Defesas",

    "Alavancagem": "Bet Builder",

    "Drop Odds Cashout": "Trading", # [NOVO] Adicionado para o relatório

    "⚔️ Contra-Ataque Fulminante": "Over Gols",

    "🏴 Escanteio Tático": "Over Escanteios"

}

MAPA_ODDS_TEORICAS = {

    "🟣 Porteira Aberta": {"min": 1.50, "max": 1.80},

    "⚡ Gol Relâmpago": {"min": 1.30, "max": 1.45},

    "💰 Janela de Ouro": {"min": 1.70, "max": 2.10},

    "🟢 Blitz Casa": {"min": 1.50, "max": 1.70},

    "🟢 Blitz Visitante": {"min": 1.50, "max": 1.70},

    "🔥 Massacre": {"min": 1.25, "max": 1.40},

    "⚔️ Choque Líderes": {"min": 1.40, "max": 1.60},

    "🥊 Briga de Rua": {"min": 1.40, "max": 1.60},

    "❄️ Jogo Morno": {"min": 1.20, "max": 1.35},

    "💎 GOLDEN BET": {"min": 1.80, "max": 2.40},

    "🏹 Tiroteio Elite": {"min": 1.40, "max": 1.60},

    "⚡ Contra-Ataque Letal": {"min": 1.60, "max": 2.20},

    "💎 Sniper Final": {"min": 1.80, "max": 2.50},

    "🔫 Lay Goleada": {"min": 1.60, "max": 2.20},

    "👴 Estratégia do Vovô": {"min": 1.05, "max": 1.25},

    "🟨 Sniper de Cartões": {"min": 1.50, "max": 1.90},

    "🧤 Muralha (Defesas)": {"min": 1.60, "max": 2.10},

    "⚔️ Contra-Ataque Fulminante": {"min": 1.60, "max": 2.20},

    "🏴 Escanteio Tático": {"min": 1.75, "max": 2.50}

}

# ==============================================================================

# 2. FUNÇÕES AUXILIARES, DADOS E API

# ==============================================================================

# ==============================================================================
# MÓDULOS DE INTELIGÊNCIA HÍBRIDA (Injetados do V17)
# ==============================================================================

def classificar_tipo_estrategia(estrategia: str) -> str:
    '''
    [PATCH V5.2] Classifica CORRETAMENTE se a estratégia é OVER, UNDER ou RESULTADO.

    OVER: Aposta que SAI GOL (GREEN quando sai gol)
    UNDER: Aposta que NÃO SAI GOL (GREEN quando não sai gol)
    RESULTADO: Aposta em vitória/empate (GREEN quando time ganha/mantém)
    '''
    estrategia_lower = str(estrategia or '').lower()

    # ─────────────────────────────────────────────────────────────────────
    # ESTRATÉGIAS DE OVER (Aposta que SAI GOL)
    # ─────────────────────────────────────────────────────────────────────
    over_keywords = [
        'porteira aberta',      # 🟣 Porteira Aberta
        'golden bet',           # 💎 Golden Bet
        'gol relâmpago',        # ⚡ Gol Relâmpago
        'gol relampago',        # (sem acento)
        'blitz casa',           # 🟢 Blitz Casa
        'blitz visitante',      # 🟢 Blitz Visitante
        'blitz',                # (genérico)
        'massacre',             # 🔥 Massacre
        'choque',               # ⚔️ Choque Líderes
        'briga de rua',         # 🥊 Briga de Rua
        'escanteios',           # 🏴 Escanteios
        'escanteio',            # (singular)
        'corner',               # Corner (inglês)
        'janela de ouro',       # 💰 Janela de Ouro
        'janela ouro',          # (sem "de")
        'tiroteio elite',       # 🏹 Tiroteio Elite
        'sniper final',         # 💎 Sniper Final
        'sniper matinal',       # Sniper Matinal
        'lay goleada',          # 🔫 Lay Goleada
        'gigante dormindo',     # Gigante Dormindo (se existir)
        'reação do gigante',    # Reação do Gigante (se existir)
        'over',                 # Genérico OVER
        'btts',                 # Both Teams to Score
        'ambas marcam',         # Ambas Marcam
        'gol',                  # (genérico se contém "gol")
    ]

    # ─────────────────────────────────────────────────────────────────────
    # ESTRATÉGIAS DE UNDER (Aposta que NÃO SAI GOL)
    # ─────────────────────────────────────────────────────────────────────
    under_keywords = [
        'jogo morno',           # ❄️ Jogo Morno
        'arame liso',           # 🧊 Arame Liso
        'under',                # Genérico UNDER
        'sem gols',             # Sem Gols
        'morno',                # (simplificado)
        'arame',                # (simplificado)
    ]

    # ─────────────────────────────────────────────────────────────────────
    # ESTRATÉGIAS DE RESULTADO (Aposta em vitória/empate)
    # ─────────────────────────────────────────────────────────────────────
    resultado_keywords = [
        'estratégia do vovô',   # 👴 Estratégia do Vovô
        'estrategia do vovo',   # (sem acento)
        'vovô',                 # (simplificado)
        'vovo',                 # (sem acento)
        'contra-ataque',        # ⚡ Contra-Ataque Letal
        'contra ataque',        # (sem hífen)
        'back',                 # Back (apostar no favorito)
        'segurar',              # Segurar resultado
        'manter',               # Manter resultado
        'vitória',              # Vitória
        'vitoria',              # (sem acento)
        'empate',               # Empate
    ]

    # ─────────────────────────────────────────────────────────────────────
    # ORDEM DE CHECAGEM (IMPORTANTE: Mais específico primeiro)
    # ─────────────────────────────────────────────────────────────────────

    # 1. Checa RESULTADO primeiro (para evitar conflito com "estratégia do vovô")
    for keyword in resultado_keywords:
        if keyword in estrategia_lower:
            return 'RESULTADO'

    # 2. Checa UNDER
    for keyword in under_keywords:
        if keyword in estrategia_lower:
            return 'UNDER'

    # 3. Checa OVER
    for keyword in over_keywords:
        if keyword in estrategia_lower:
            return 'OVER'

    # 4. Se não matchou nada, assume NEUTRO (não deveria acontecer)
    return 'NEUTRO'

def obter_descricao_aposta(estrategia: str) -> dict:
    '''
    [NOVO V5.2] Retorna descrição da aposta para cada estratégia.

    Retorna dict com:
        'tipo': OVER/UNDER/RESULTADO
        'aposta': Descrição da aposta
        'ordem': Texto para Telegram (o que fazer)
        'ganha_se': Quando GREEN
        'perde_se': Quando RED
    '''
    tipo = classificar_tipo_estrategia(estrategia)
    estrategia_lower = str(estrategia or '').lower()

    # ─────────────────────────────────────────────────────────────────────
    # MAPEAMENTO ESTRATÉGIA → DESCRIÇÃO
    # ─────────────────────────────────────────────────────────────────────

    if 'golden bet' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }

    elif 'janela' in estrategia_lower and 'ouro' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }

    elif 'gol relâmpago' in estrategia_lower or 'gol relampago' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols no 1º Tempo',
            'ordem': '👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT',
            'ganha_se': 'Sai GOL no 1º Tempo',
            'perde_se': 'Não sai GOL no 1º Tempo'
        }

    elif 'blitz' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }

    elif 'porteira' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }

    elif 'tiroteio' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }

    elif 'sniper' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE nos ACRÉSCIMOS\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL nos acréscimos',
            'perde_se': 'Não sai GOL'
        }

    elif 'lay goleada' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols (Asiático)',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE (gol da honra)\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }

    elif 'massacre' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols no 1º Tempo',
            'ordem': '👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT',
            'ganha_se': 'Sai GOL no 1º Tempo',
            'perde_se': 'Não sai GOL no 1º Tempo'
        }

    elif 'choque' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols no 1º Tempo',
            'ordem': '👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT',
            'ganha_se': 'Sai GOL no 1º Tempo',
            'perde_se': 'Não sai GOL no 1º Tempo'
        }

    elif 'briga de rua' in estrategia_lower:
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols no 1º Tempo',
            'ordem': '👉 FAZER: Entrar em GOLS 1º TEMPO\n✅ Aposta: Mais de 0.5 Gols HT',
            'ganha_se': 'Sai GOL no 1º Tempo',
            'perde_se': 'Não sai GOL no 1º Tempo'
        }

    elif 'contra-ataque' in estrategia_lower or 'contra ataque' in estrategia_lower:
        return {
            'tipo': 'RESULTADO',
            'aposta': 'Back Empate ou Zebra',
            'ordem': '👉 FAZER: Back no time que está PERDENDO\n⚡ Aposta: Recuperação ou Empate',
            'ganha_se': 'Time EMPATA ou VIRA',
            'perde_se': 'Time que está ganhando MANTÉM'
        }

    # ─── UNDER STRATEGIES ───

    elif 'jogo morno' in estrategia_lower or 'morno' in estrategia_lower:
        return {
            'tipo': 'UNDER',
            'aposta': 'Menos de 0.5 Gols (Under)',
            'ordem': '👉 FAZER: Entrar em UNDER GOLS\n❄️ Aposta: NÃO SAI mais gol',
            'ganha_se': 'NÃO sai GOL',
            'perde_se': 'Sai GOL'
        }

    elif 'arame liso' in estrategia_lower or 'arame' in estrategia_lower:
        return {
            'tipo': 'UNDER',
            'aposta': 'Menos de 0.5 Gols (Under)',
            'ordem': '👉 FAZER: Entrar em UNDER GOLS\n🧊 Aposta: Falsa pressão, NÃO SAI gol',
            'ganha_se': 'NÃO sai GOL (falsa pressão confirmada)',
            'perde_se': 'Sai GOL'
        }

    # ─── RESULTADO STRATEGIES ───

    elif 'vovô' in estrategia_lower or 'vovo' in estrategia_lower:
        return {
            'tipo': 'RESULTADO',
            'aposta': 'Vitória do time que está ganhando',
            'ordem': '👉 FAZER: Back no time que está GANHANDO\n👴 Aposta: Time manterá a vitória',
            'ganha_se': 'Time MANTÉM ou AUMENTA vantagem',
            'perde_se': 'Time EMPATA ou PERDE'
        }

    # ─── FALLBACK GENÉRICO ───

    elif tipo == 'OVER':
        return {
            'tipo': 'OVER',
            'aposta': 'Mais de 0.5 Gols',
            'ordem': '👉 FAZER: Entrar em GOL LIMITE\n✅ Aposta: Mais de 0.5 Gols',
            'ganha_se': 'Sai GOL',
            'perde_se': 'Não sai GOL'
        }

    elif tipo == 'UNDER':
        return {
            'tipo': 'UNDER',
            'aposta': 'Menos de 0.5 Gols',
            'ordem': '👉 FAZER: Entrar em UNDER GOLS\n❄️ Aposta: NÃO SAI mais gol',
            'ganha_se': 'NÃO sai GOL',
            'perde_se': 'Sai GOL'
        }

    else:  # RESULTADO ou NEUTRO
        return {
            'tipo': 'RESULTADO',
            'aposta': 'Resultado Final',
            'ordem': '👉 FAZER: Apostar no resultado indicado',
            'ganha_se': 'Resultado se confirma',
            'perde_se': 'Resultado não se confirma'
        }

def calcular_gols_atuais(placar_str: str) -> int:
    '''Calcula gols atuais (ex: 2x1 -> 3).'''
    try:
        gh, ga = map(int, str(placar_str).lower().replace(' ', '').split('x'))
        return int(gh) + int(ga)
    except:
        return 0

def calcular_threshold_dinamico(estrategia: str, odd_atual: float) -> int:
    '''Threshold dinâmico por estratégia + odd (50-80).'''
    estr = str(estrategia or '')
    tipo = classificar_tipo_estrategia(estr)
    if tipo == 'UNDER':
        thr = 65
    elif 'golden' in estr.lower() or 'diamante' in estr.lower():
        thr = 75
    else:
        thr = 50

    try:
        odd = float(odd_atual)
        if odd >= 2.00:
            thr -= 5
        elif odd <= 1.30:
            thr += 10
    except:
        pass

    return int(max(50, min(thr, 80)))

def rastrear_movimento_odd(fid, estrategia, odd_atual, janela_min=5):
    '''Rastreia movimento de odd em memória (últimos X min).'''
    try:
        fid = str(fid)
        odd_atual = float(odd_atual)
    except:
        return 'DESCONHECIDO', 0.0

    if 'odd_history' not in st.session_state:
        st.session_state['odd_history'] = {}

    hist = st.session_state['odd_history'].get(fid, [])
    agora = get_time_br()
    hist.append({'t': agora, 'odd': odd_atual, 'estrategia': str(estrategia)})

    limite = agora - timedelta(minutes=int(janela_min))
    hist = [x for x in hist if x.get('t') and x['t'] >= limite]
    st.session_state['odd_history'][fid] = hist

    if len(hist) < 2:
        return 'ESTÁVEL', 0.0

    odd_ini = hist[0]['odd']
    if odd_ini <= 0:
        return 'ESTÁVEL', 0.0

    variacao = ((odd_atual - odd_ini) / odd_ini) * 100.0

    if variacao <= -7:
        return 'CAINDO FORTE', variacao
    if variacao <= -3:
        return 'CAINDO', variacao
    if variacao >= 7:
        return 'SUBINDO FORTE', variacao
    if variacao >= 3:
        return 'SUBINDO', variacao
    return 'ESTÁVEL', variacao

def calcular_kelly_criterion(probabilidade, odd, modo='fracionario'):
    '''Kelly % ideal (prob 0-100). Half-Kelly com teto 5%.'''
    try:
        prob_decimal = float(probabilidade) / 100.0
        odd = float(odd)
        if odd <= 1.01: return 0.0

        # Kelly puro
        kelly = (prob_decimal * odd - 1) / (odd - 1)
        if kelly <= 0: return 0.0

        # Half-Kelly (fracionário = padrão de segurança)
        kelly *= 0.5

        # Converte para %
        pct = round(kelly * 100.0, 2)

        # Teto por modo
        if modo == 'conservador':
            pct = min(pct, 2.0)   # Max 2%
        elif modo == 'fracionario':
            pct = min(pct, 5.0)   # Max 5%
        else:  # completo
            pct = min(pct, 10.0)  # Max 10%

        # Piso mínimo
        if 0 < pct < 0.5: pct = 0.5

        return pct
    except:
        return 1.0

def calcular_stake_recomendado(banca_atual, probabilidade, odd, modo='fracionario', opiniao_ia='Aprovado'):
    '''
    Stake inteligente baseado em Kelly + opinião da IA.

    ✅ Aprovado: Kelly (min 1%, max 5%) → aposta mais quando confia
    ⚠️ Arriscado: Fixo 1% da banca → protege capital
    '''
    try:
        banca_atual = float(banca_atual)
        if banca_atual <= 0: banca_atual = 100.0

        kelly_pct = calcular_kelly_criterion(probabilidade, odd, modo)

        if opiniao_ia == 'Aprovado':
            # IA confia: Kelly determina (mín 1%, máx 3%)
            pct = max(kelly_pct, 1.0)
            pct = min(pct, 3.0)  # [FIX] Max 3% (era 5% - muito agressivo)
        else:
            # IA não confia: stake mínimo (1%)
            pct = 1.0

        valor = round((banca_atual * pct) / 100.0, 2)
        valor = max(valor, 1.0)  # Mínimo R$ 1.00

        return {'porcentagem': pct, 'valor': valor, 'modo': modo, 'kelly_puro': kelly_pct}
    except:
        return {'porcentagem': 1.0, 'valor': max(round(float(banca_atual or 100) * 0.01, 2), 1.0), 'modo': 'erro', 'kelly_puro': 0}

# ==============================================================================
# [CORREÇÃO CRÍTICA] ODDS MÍNIMAS POR ESTRATÉGIA
# ==============================================================================
ODD_MINIMA_POR_ESTRATEGIA = {
    "estratégia do vovô": 1.20,
    "vovô": 1.20,
    "jogo morno": 1.35,
    "morno": 1.35,
    "porteira aberta": 1.50,
    "porteira": 1.50,
    "golden bet": 1.80,
    "golden": 1.80,
    "gol relâmpago": 1.60,
    "relâmpago": 1.60,
    "blitz": 1.60,
    "massacre": 1.70,
    "alavancagem": 3.50,
    "sniper": 1.80,
    "arame liso": 1.35,
    "under": 1.40,
}

def obter_odd_minima(estrategia):
    """Retorna a odd mínima aceitável para a estratégia. Padrão 1.50 se não encontrar."""
    try:
        estrategia_lower = str(estrategia or '').lower()
        for chave, odd_min in ODD_MINIMA_POR_ESTRATEGIA.items():
            if chave in estrategia_lower:
                return float(odd_min)
        return 1.50
    except:
        return 1.50

# --- [MELHORIA] NOVA FUNÇÃO DE BUSCA DE ODD PRÉ-MATCH (ROBUSTA) ---
@st.cache_data(ttl=3600)
def buscar_odd_pre_match(api_key, fid):
    try:
        url = "https://v3.football.api-sports.io/odds"
        params = {"fixture": fid, "bookmaker": "8"} # ID 8 = Bet365
        r = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()

        if not r.get('response'): return 0.0, "Sem Bet365"

        bookmakers = r['response'][0]['bookmakers']
        if not bookmakers: return 0.0, "Sem Bet365"

        bet365 = bookmakers[0]

        if bet365:
            # [FIX] Prioriza Over 1.5 (mais seguro e alinhado com palpites)
            mercado_gols = next((m for m in bet365['bets'] if m['id'] == 5), None)
            if mercado_gols:
                # Tenta Over 1.5 primeiro (default seguro)
                odd_obj = next((v for v in mercado_gols['values'] if v['value'] == "Over 1.5"), None)
                # Fallback Over 2.5 se não tiver 1.5
                if not odd_obj:
                     odd_obj = next((v for v in mercado_gols['values'] if v['value'] == "Over 2.5"), None)

                if odd_obj:
                    return float(odd_obj['odd']), f"{odd_obj['value']} (Bet365)"

        return 0.0, "N/A"
    except: return 0.0, "N/A"

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

def formatar_inteiro_visual(val):

    try:

        if str(val) == 'nan' or str(val) == '': return "0"

        return str(int(float(str(val))))

    except: return str(val)

def gerar_chave_universal(fid, estrategia, tipo_sinal="SINAL"):

    try: fid_clean = str(int(float(str(fid).strip())))

    except: fid_clean = str(fid).strip()

    strat_clean = str(estrategia).strip().upper().replace(" ", "_")

    chave = f"{fid_clean}_{strat_clean}"

    if tipo_sinal == "SINAL": return chave

    elif tipo_sinal == "GREEN": return f"RES_GREEN_{chave}"

    elif tipo_sinal == "RED": return f"RES_RED_{chave}"

    return chave

def update_api_usage(headers):

    if not headers: return

    try:

        limit = int(headers.get('x-ratelimit-requests-limit', 75000))

        remaining = int(headers.get('x-ratelimit-requests-remaining', 0))

        used = limit - remaining

        st.session_state['api_usage'] = {'used': used, 'limit': limit}

    except: pass

def verificar_reset_diario():

    hoje_utc = datetime.now(pytz.utc).date()

    if st.session_state['data_api_usage'] != hoje_utc:

        st.session_state['api_usage']['used'] = 0; st.session_state['data_api_usage'] = hoje_utc

        st.session_state['gemini_usage']['used'] = 0

        st.session_state['alvos_do_dia'] = {}
        st.session_state['tier2_calls_hoje'] = 0
        st.session_state['sniper_tarde_enviado'] = False
        st.session_state['sniper_noite_enviado'] = False

        st.session_state['matinal_enviado'] = False

        st.session_state['multipla_matinal_enviada'] = False

        st.session_state['alternativos_enviado'] = False

        st.session_state['alavancagem_enviada'] = False

        st.session_state['drop_enviado_12'] = False

        st.session_state['drop_enviado_16'] = False

        return True

    return False

def testar_conexao_telegram(token):

    if not token: return False, "Token Vazio"

    try:

        res = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=5)

        if res.status_code == 200:

            return True, res.json()['result']['first_name']

        return False, f"Erro {res.status_code}"

    except:

        return False, "Sem Conexão"

def calcular_stats(df_raw):

    if df_raw.empty: return 0, 0, 0, 0

    df_raw = df_raw.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')

    greens = len(df_raw[df_raw['Resultado'].str.contains('GREEN', na=False)])

    reds = len(df_raw[df_raw['Resultado'].str.contains('RED', na=False)])

    total = len(df_raw)

    winrate = (greens / (greens + reds) * 100) if (greens + reds) > 0 else 0.0

    return total, greens, reds, winrate

# [NOVO MÓDULO] ESTRATÉGIA CASHOUT / DROP ODDS (PRÉ-LIVE)

def buscar_odds_comparativas(api_key, fixture_id):

    url = "https://v3.football.api-sports.io/odds"

    try:

        params_b365 = {"fixture": fixture_id, "bookmaker": "8"}

        params_pin = {"fixture": fixture_id, "bookmaker": "4"}

        r365 = requests.get(url, headers={"x-apisports-key": api_key}, params=params_b365).json()

        rpin = requests.get(url, headers={"x-apisports-key": api_key}, params=params_pin).json()

        odd_365 = 0; odd_pin = 0; time_alvo = ""

        if r365.get('response'):

            mkts = r365['response'][0]['bookmakers'][0]['bets']

            vencedor = next((m for m in mkts if m['id'] == 1), None)

            if vencedor:

                v_casa = float(next((v['odd'] for v in vencedor['values'] if v['value'] == 'Home'), 0))

                v_fora = float(next((v['odd'] for v in vencedor['values'] if v['value'] == 'Away'), 0))

                if rpin.get('response'):

                    mkts_pin = rpin['response'][0]['bookmakers'][0]['bets']

                    vencedor_pin = next((m for m in mkts_pin if m['id'] == 1), None)

                    if vencedor_pin:

                        p_casa = float(next((v['odd'] for v in vencedor_pin['values'] if v['value'] == 'Home'), 0))

                        p_fora = float(next((v['odd'] for v in vencedor_pin['values'] if v['value'] == 'Away'), 0))

                        margem = 1.10

                        if v_casa > (p_casa * margem): return v_casa, p_casa, "Casa"

                        elif v_fora > (p_fora * margem): return v_fora, p_fora, "Visitante"

        return 0, 0, None

    except: return 0, 0, None

def scanner_drop_odds_pre_live(api_key):

    agora = datetime.now(pytz.timezone('America/Sao_Paulo'))

    hoje_str = agora.strftime('%Y-%m-%d')

    try:

        url = "https://v3.football.api-sports.io/fixtures"

        params = {"date": hoje_str, "timezone": "America/Sao_Paulo"}

        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()

        jogos = res.get('response', [])

        LIGAS_PERMITIDAS = [39, 140, 78, 135, 61, 2, 3]

        oportunidades = []

        for j in jogos:

            lid = j['league']['id']

            fid = j['fixture']['id']

            if lid not in LIGAS_PERMITIDAS: continue

            dt_jogo = j['fixture']['date']

            try:

                hora_jogo = datetime.fromisoformat(dt_jogo.replace('Z', '+00:00'))

            except:

                 hora_jogo = datetime.strptime(dt_jogo[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.utc)

            agora_utc = datetime.now(pytz.utc)

            diff = (hora_jogo - agora_utc).total_seconds() / 3600

            if j['fixture']['status']['short'] != 'NS': continue

            if not (3 <= diff <= 8): continue

            odd_b365, odd_pin, lado = buscar_odds_comparativas(api_key, fid)

            if odd_b365 > 0 and lado:

                diferenca = ((odd_b365 - odd_pin) / odd_pin) * 100

                oportunidades.append({

                    "fid": fid,

                    "jogo": f"{j['teams']['home']['name']} x {j['teams']['away']['name']}",

                    "liga": j['league']['name'],

                    "hora": j['fixture']['date'][11:16],

                    "lado": lado,

                    "odd_b365": odd_b365,

                    "odd_pinnacle": odd_pin,

                    "valor": diferenca

                })

        return oportunidades

    except Exception as e: return []

# --- GERENCIAMENTO DE PLANILHAS E DADOS ---

def carregar_aba(nome_aba, colunas_esperadas):

    chave_memoria = ""

    if nome_aba == "Historico": chave_memoria = 'historico_full'

    elif nome_aba == "Seguras": chave_memoria = 'df_safe'

    elif nome_aba == "Obs": chave_memoria = 'df_vip'

    elif nome_aba == "Blacklist": chave_memoria = 'df_black'

    try:

        df = conn.read(worksheet=nome_aba, ttl=10)

        if df.empty and chave_memoria in st.session_state:

            df_ram = st.session_state[chave_memoria]

            if not df_ram.empty:

                st.toast(f"⚠️ Erro leitura {nome_aba}. Usando Cache.", icon="🛡️")

                return df_ram

        if not df.empty:

            for col in colunas_esperadas:

                if col not in df.columns:

                    if col == 'Probabilidade': df[col] = "0"

                    else: df[col] = "1.20" if col == 'Odd' else ""

            return df.fillna("").astype(str)

        return pd.DataFrame(columns=colunas_esperadas)

    except Exception as e:

        if chave_memoria and chave_memoria in st.session_state:

            df_ram = st.session_state[chave_memoria]

            if not df_ram.empty: return df_ram

        st.session_state['BLOQUEAR_SALVAMENTO'] = True

        return pd.DataFrame(columns=colunas_esperadas)

def salvar_aba(nome_aba, df_para_salvar):

    if nome_aba in ["Historico", "Seguras", "Obs"] and df_para_salvar.empty: return False

    if st.session_state.get('BLOQUEAR_SALVAMENTO', False):

        st.session_state['precisa_salvar'] = True

        return False

    try:

        conn.update(worksheet=nome_aba, data=df_para_salvar)

        if nome_aba == "Historico": st.session_state['precisa_salvar'] = False

        return True

    except:

        st.session_state['precisa_salvar'] = True

        return False

def salvar_blacklist(id_liga, pais, nome_liga, motivo_ban):

    df = st.session_state['df_black']

    id_norm = normalizar_id(id_liga)

    if id_norm in df['id'].values:

        idx = df[df['id'] == id_norm].index[0]

        df.at[idx, 'Motivo'] = str(motivo_ban)

    else:

        novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': str(motivo_ban)}])

        df = pd.concat([df, novo], ignore_index=True)

    st.session_state['df_black'] = df

    salvar_aba("Blacklist", df)

    sanitizar_conflitos()

def sanitizar_conflitos():

    df_black = st.session_state.get('df_black', pd.DataFrame())

    df_vip = st.session_state.get('df_vip', pd.DataFrame())

    df_safe = st.session_state.get('df_safe', pd.DataFrame())

    if df_black.empty or df_vip.empty or df_safe.empty: return

    alterou_black, alterou_vip, alterou_safe = False, False, False

    for idx, row in df_black.iterrows():

        id_b = normalizar_id(row['id'])

        motivo_atual = str(row['Motivo'])

        df_vip['id_norm'] = df_vip['id'].apply(normalizar_id)

        mask_vip = df_vip['id_norm'] == id_b

        if mask_vip.any():

            strikes = formatar_inteiro_visual(df_vip.loc[mask_vip, 'Strikes'].values[0])

            novo_motivo = f"Banida ({strikes} Jogos Sem Dados)"

            if motivo_atual != novo_motivo:

                df_black.at[idx, 'Motivo'] = novo_motivo

                alterou_black = True

            df_vip = df_vip[~mask_vip]

            alterou_vip = True

        df_safe['id_norm'] = df_safe['id'].apply(normalizar_id)

        mask_safe = df_safe['id_norm'] == id_b

        if mask_safe.any():

            df_safe = df_safe[~mask_safe]

            alterou_safe = True

    if 'id_norm' in df_vip.columns: df_vip = df_vip.drop(columns=['id_norm'])

    if 'id_norm' in df_safe.columns: df_safe = df_safe.drop(columns=['id_norm'])

    if alterou_black: st.session_state['df_black'] = df_black; salvar_aba("Blacklist", df_black)

    if alterou_vip: st.session_state['df_vip'] = df_vip; salvar_aba("Obs", df_vip)

    if alterou_safe: st.session_state['df_safe'] = df_safe; salvar_aba("Seguras", df_safe)

def salvar_safe_league_basic(id_liga, pais, nome_liga, tem_tabela=False):

    id_norm = normalizar_id(id_liga)

    df = st.session_state['df_safe']

    txt_motivo = "Validada (Chutes + Tabela)" if tem_tabela else "Validada (Chutes)"

    if id_norm not in df['id'].values:

        novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Motivo': txt_motivo, 'Strikes': '0', 'Jogos_Erro': ''}])

        final = pd.concat([df, novo], ignore_index=True)

        if salvar_aba("Seguras", final): st.session_state['df_safe'] = final; sanitizar_conflitos()

    else:

        idx = df[df['id'] == id_norm].index[0]

        if df.at[idx, 'Motivo'] != txt_motivo:

            df.at[idx, 'Motivo'] = txt_motivo

            if salvar_aba("Seguras", df): st.session_state['df_safe'] = df

def resetar_erros(id_liga):

    id_norm = normalizar_id(id_liga)

    df_safe = st.session_state.get('df_safe', pd.DataFrame())

    if not df_safe.empty and id_norm in df_safe['id'].values:

        idx = df_safe[df_safe['id'] == id_norm].index[0]

        if str(df_safe.at[idx, 'Strikes']) != '0':

            df_safe.at[idx, 'Strikes'] = '0'; df_safe.at[idx, 'Jogos_Erro'] = ''

            if salvar_aba("Seguras", df_safe): st.session_state['df_safe'] = df_safe

def gerenciar_erros(id_liga, pais, nome_liga, fid_jogo):

    id_norm = normalizar_id(id_liga)

    fid_str = str(fid_jogo)

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

            novo_obs = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Data_Erro': get_time_br().strftime('%Y-%m-%d'), 'Strikes': '1', 'Jogos_Erro': fid_str}])

            final_vip = pd.concat([df_vip, novo_obs], ignore_index=True)

            salvar_aba("Obs", final_vip); st.session_state['df_vip'] = final_vip

        else:

            df_safe.at[idx, 'Strikes'] = str(strikes); df_safe.at[idx, 'Jogos_Erro'] = ",".join(jogos_erro)

            salvar_aba("Seguras", df_safe); st.session_state['df_safe'] = df_safe

        return

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

        salvar_blacklist(id_liga, pais, nome_liga, f"Banida ({formatar_inteiro_visual(strikes)} Jogos Sem Dados)")

    else:

        if id_norm in df_vip['id'].values:

            idx = df_vip[df_vip['id'] == id_norm].index[0]

            df_vip.at[idx, 'Strikes'] = str(strikes); df_vip.at[idx, 'Jogos_Erro'] = ",".join(jogos_erro)

            df_vip.at[idx, 'Data_Erro'] = get_time_br().strftime('%Y-%m-%d')

            salvar_aba("Obs", df_vip); st.session_state['df_vip'] = df_vip

        else:

            novo = pd.DataFrame([{'id': id_norm, 'País': str(pais), 'Liga': str(nome_liga), 'Data_Erro': get_time_br().strftime('%Y-%m-%d'), 'Strikes': '1', 'Jogos_Erro': fid_str}])

            final = pd.concat([df_vip, novo], ignore_index=True)

            salvar_aba("Obs", final); st.session_state['df_vip'] = final

def carregar_tudo(force=False):

    now = time.time()

    if force or (now - st.session_state['last_static_update']) > STATIC_CACHE_TIME or 'df_black' not in st.session_state:

        st.session_state['df_black'] = carregar_aba("Blacklist", COLS_BLACK)

        st.session_state['df_safe'] = carregar_aba("Seguras", COLS_SAFE)

        st.session_state['df_vip'] = carregar_aba("Obs", COLS_OBS)

        if not st.session_state['df_black'].empty: st.session_state['df_black']['id'] = st.session_state['df_black']['id'].apply(normalizar_id)

        if not st.session_state['df_safe'].empty: st.session_state['df_safe']['id'] = st.session_state['df_safe']['id'].apply(normalizar_id)

        if not st.session_state['df_vip'].empty: st.session_state['df_vip']['id'] = st.session_state['df_vip']['id'].apply(normalizar_id)

        sanitizar_conflitos()

        st.session_state['last_static_update'] = now

    if 'historico_full' not in st.session_state or force:

        df = carregar_aba("Historico", COLS_HIST)

        if not df.empty and 'Probabilidade' in df.columns:

            def normalizar_legado_prob(val):

                s_val = str(val).strip().replace(',', '.')

                if '%' in s_val: return s_val

                if s_val == 'nan' or s_val == '': return '0%'

                try:

                    float_val = float(s_val)

                    if float_val <= 1.0: float_val *= 100

                    return f"{int(float_val)}%"

                except: return s_val

            df['Probabilidade'] = df['Probabilidade'].apply(normalizar_legado_prob)

        if df.empty and 'historico_full' in st.session_state and not st.session_state['historico_full'].empty:

            df = st.session_state['historico_full']

        if not df.empty and 'Data' in df.columns:

            df['FID'] = df['FID'].apply(clean_fid)

            try:

                df['Data_Temp'] = pd.to_datetime(df['Data'], errors='coerce')

                df['Data'] = df['Data_Temp'].dt.strftime('%Y-%m-%d').fillna(df['Data'])

                df = df.drop(columns=['Data_Temp'])

            except: pass

            st.session_state['historico_full'] = df

            hoje = get_time_br().strftime('%Y-%m-%d')

            st.session_state['historico_sinais'] = df[df['Data'] == hoje].to_dict('records')[::-1]

            if 'alertas_enviados' not in st.session_state: st.session_state['alertas_enviados'] = set()

            for item in st.session_state['historico_sinais']:

                st.session_state['alertas_enviados'].add(gerar_chave_universal(item['FID'], item['Estrategia'], "SINAL"))

                if 'GREEN' in str(item['Resultado']): st.session_state['alertas_enviados'].add(gerar_chave_universal(item['FID'], item['Estrategia'], "GREEN"))

                if 'RED' in str(item['Resultado']): st.session_state['alertas_enviados'].add(gerar_chave_universal(item['FID'], item['Estrategia'], "RED"))

        else:

            if 'historico_full' not in st.session_state:

                st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)

                st.session_state['historico_sinais'] = []

    if 'jogos_salvos_bigdata_carregados' not in st.session_state or not st.session_state['jogos_salvos_bigdata_carregados'] or force:

        st.session_state['jogos_salvos_bigdata_carregados'] = True

    st.session_state['last_db_update'] = now

def adicionar_historico(item):

    if 'historico_full' not in st.session_state: st.session_state['historico_full'] = carregar_aba("Historico", COLS_HIST)

    df_memoria = st.session_state['historico_full']

    df_novo = pd.DataFrame([item])

    df_final = pd.concat([df_novo, df_memoria], ignore_index=True)

    st.session_state['historico_full'] = df_final

    st.session_state['historico_sinais'].insert(0, item)

    st.session_state['precisa_salvar'] = True

    return True

def atualizar_historico_ram(lista_atualizada_hoje):

    if 'historico_full' not in st.session_state: return

    df_memoria = st.session_state['historico_full']

    df_hoje_updates = pd.DataFrame(lista_atualizada_hoje)

    if df_hoje_updates.empty or df_memoria.empty: return

    mapa_atualizacao = {}

    for _, row in df_hoje_updates.iterrows():

        chave = f"{row['FID']}_{row['Estrategia']}"

        mapa_atualizacao[chave] = row

    def atualizar_linha(row):

        chave = f"{row['FID']}_{row['Estrategia']}"

        if chave in mapa_atualizacao:

            nova_linha = mapa_atualizacao[chave]

            if str(row['Resultado']) != str(nova_linha['Resultado']): st.session_state['precisa_salvar'] = True

            return nova_linha

        return row

    df_final = df_memoria.apply(atualizar_linha, axis=1)

    st.session_state['historico_full'] = df_final

def consultar_bigdata_cenario_completo(home_id, away_id):

    """

    RAIO-X 360º: Analisa Gols, Cantos, Cartões, Chutes e Agressividade.

    """

    if not db_firestore: return "Big Data Offline"

    try:

        docs_h = db_firestore.collection("BigData_Futebol").where("home_id", "==", str(home_id)).limit(20).stream()

        docs_a = db_firestore.collection("BigData_Futebol").where("away_id", "==", str(away_id)).limit(20).stream()

        def safe_get(stats_dict, key):

            try: return float(stats_dict.get(key, 0))

            except: return 0.0

        h_data = {'qtd': 0, 'gols_pro': 0, 'cantos': 0, 'cards': 0, 'sog': 0, 'faltas': 0, 'imp': 0}

        for d in docs_h:

            dd = d.to_dict(); h_data['qtd'] += 1; st = dd.get('estatisticas', {})

            try: h_data['gols_pro'] += int(dd['placar_final'].split('x')[0])

            except: pass

            h_data['cantos'] += safe_get(st, 'escanteios_casa')

            h_data['cards'] += safe_get(st, 'cartoes_amarelos') + safe_get(st, 'cartoes_vermelhos')

            h_data['sog'] += safe_get(st, 'chutes_gol')

        a_data = {'qtd': 0, 'gols_pro': 0, 'cantos': 0, 'cards': 0, 'sog': 0, 'faltas': 0, 'imp': 0}

        for d in docs_a:

            dd = d.to_dict(); a_data['qtd'] += 1; st = dd.get('estatisticas', {})

            try: a_data['gols_pro'] += int(dd['placar_final'].split('x')[1])

            except: pass

            a_data['cantos'] += safe_get(st, 'escanteios_fora')

            a_data['cards'] += safe_get(st, 'cartoes_amarelos') + safe_get(st, 'cartoes_vermelhos')

            a_data['sog'] += safe_get(st, 'chutes_gol')

        if h_data['qtd'] == 0 and a_data['qtd'] == 0: return "Sem dados suficientes."

        txt_h = "N/D"

        if h_data['qtd'] > 0: q = h_data['qtd']; txt_h = (f"MANDANTE (Casa, {q}j): Gols {h_data['gols_pro']/q:.1f} | Cantos {h_data['cantos']/q:.1f} | ChutesGol {h_data['sog']/q:.1f}")

        txt_a = "N/D"

        if a_data['qtd'] > 0: q = a_data['qtd']; txt_a = (f"VISITANTE (Fora, {q}j): Gols {a_data['gols_pro']/q:.1f} | Cantos {a_data['cantos']/q:.1f} | ChutesGol {a_data['sog']/q:.1f}")

        return f"{txt_h} || {txt_a}"

    except Exception as e: return f"Erro BD: {str(e)}"

def salvar_bigdata(jogo_api, stats):

    if not db_firestore: return

    try:

        fid = str(jogo_api['fixture']['id'])

        if fid in st.session_state['jogos_salvos_bigdata']: return

        s1 = stats[0]['statistics']; s2 = stats[1]['statistics']

        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0

        def sanitize(val): return str(val) if val is not None else "0"

        rate_h = 0; rate_a = 0

        if 'API_KEY' in st.session_state:

            try:

                url_stats = "https://v3.football.api-sports.io/fixtures/players"

                p_res = requests.get(url_stats, headers={"x-apisports-key": st.session_state['API_KEY']}, params={"fixture": fid}).json()

                if p_res.get('response'):

                    for t in p_res['response']:

                        is_h = (t['team']['id'] == jogo_api['teams']['home']['id'])

                        notas = []

                        for p in t['players']:

                            try:

                                rating = float(p['statistics'][0]['games']['rating'])

                                if rating > 0: notas.append(rating)

                            except: pass

                        if notas:

                            media = sum(notas)/len(notas)

                            if is_h: rate_h = media

                            else: rate_a = media

            except: pass

        item_bigdata = {

            'fid': fid, 'data_hora': get_time_br().strftime('%Y-%m-%d %H:%M'),

            'liga': sanitize(jogo_api['league']['name']), 'home_id': str(jogo_api['teams']['home']['id']), 'away_id': str(jogo_api['teams']['away']['id']),

            'jogo': f"{sanitize(jogo_api['teams']['home']['name'])} x {sanitize(jogo_api['teams']['away']['name'])}",

            'placar_final': f"{jogo_api['goals']['home']}x{jogo_api['goals']['away']}",

            'rating_home': str(rate_h), 'rating_away': str(rate_a),

            'estatisticas': {

                'chutes_total': gv(s1, 'Total Shots') + gv(s2, 'Total Shots'),

                'chutes_gol': gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal'),

                'chutes_area': gv(s1, 'Shots insidebox') + gv(s2, 'Shots insidebox'),

                'escanteios_total': gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks'),

                'escanteios_casa': gv(s1, 'Corner Kicks'), 'escanteios_fora': gv(s2, 'Corner Kicks'),

                'faltas_total': gv(s1, 'Fouls') + gv(s2, 'Fouls'),

                'cartoes_amarelos': gv(s1, 'Yellow Cards') + gv(s2, 'Yellow Cards'),

                'cartoes_vermelhos': gv(s1, 'Red Cards') + gv(s2, 'Red Cards'),

                'posse_casa': str(gv(s1, 'Ball Possession')),

                'ataques_perigosos': gv(s1, 'Dangerous Attacks') + gv(s2, 'Dangerous Attacks'),

                'impedimentos': gv(s1, 'Offsides') + gv(s2, 'Offsides'),

                'passes_pct_casa': str(gv(s1, 'Passes %')).replace('%',''), 'passes_pct_fora': str(gv(s2, 'Passes %')).replace('%','')

            }

        }

        db_firestore.collection("BigData_Futebol").document(fid).set(item_bigdata)

        st.session_state['jogos_salvos_bigdata'].add(fid)

    except: pass

def extrair_dados_completos(stats_api):

    if not stats_api: return "Dados indisponíveis."

    try:

        s1 = stats_api[0]['statistics']; s2 = stats_api[1]['statistics']

        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0

        return f"📊 STATS: Posse {gv(s1,'Ball Possession')}x{gv(s2,'Ball Possession')} | Chutes {gv(s1,'Total Shots')}x{gv(s2,'Total Shots')} | Cantos {gv(s1,'Corner Kicks')}x{gv(s2,'Corner Kicks')}"

    except: return "Erro stats."

@st.cache_data(ttl=3600)

def buscar_media_gols_ultimos_jogos(api_key, home_id, away_id):

    try:

        def get_avg_goals(team_id, location_filter):

            url = "https://v3.football.api-sports.io/fixtures"

            params = {"team": team_id, "last": "20", "status": "FT"}

            res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()

            jogos = res.get('response', [])

            gols_marcados = 0; jogos_contados = 0

            for j in jogos:

                is_home_match = (j['teams']['home']['id'] == team_id)

                if location_filter == 'home' and is_home_match:

                    gols_marcados += (j['goals']['home'] or 0); jogos_contados += 1

                elif location_filter == 'away' and not is_home_match:

                    gols_marcados += (j['goals']['away'] or 0); jogos_contados += 1

                if jogos_contados >= 10: break

            if jogos_contados == 0: return "0.00"

            return "{:.2f}".format(gols_marcados / jogos_contados)

        return {'home': get_avg_goals(home_id, 'home'), 'away': get_avg_goals(away_id, 'away')}

    except: return {'home': '?', 'away': '?'}

@st.cache_data(ttl=86400)

def analisar_tendencia_50_jogos(api_key, home_id, away_id):

    try:

        def get_stats_50(team_id):

            url = "https://v3.football.api-sports.io/fixtures"

            params = {"team": team_id, "last": "50", "status": "FT"}

            res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()

            jogos = res.get('response', [])

            # --- A CORREÇÃO ESTÁ NESTA LINHA ABAIXO ---

            # Antes faltava o "qtd": 0. Agora adicionamos para não dar erro de chave inexistente.

            if not jogos: return {"qtd": 0, "over05_ht": 0, "over15_ft": 0, "ambas_marcam": 0, "avg_cards": 0, "avg_shots_goal": 0, "avg_saves_conceded": 0}

            stats = {"qtd": len(jogos), "over05_ht": 0, "over15_ft": 0, "over25_ft": 0, "ambas_marcam": 0}

            for j in jogos:

                gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0

                g_ht_h = j['score']['halftime']['home'] or 0; g_ht_a = j['score']['halftime']['away'] or 0

                if (g_ht_h + g_ht_a) > 0: stats["over05_ht"] += 1

                if (gh + ga) >= 2: stats["over15_ft"] += 1

                if (gh + ga) >= 3: stats["over25_ft"] += 1

                if gh > 0 and ga > 0: stats["ambas_marcam"] += 1

            return {k: int((v / stats["qtd"]) * 100) if k not in ["qtd"] else v for k, v in stats.items()}

        return {"home": get_stats_50(home_id), "away": get_stats_50(away_id)}

    except: return None

@st.cache_data(ttl=21600)  # Cache 6h

def analisar_tendencia_macro_micro(api_key, home_id, away_id):
    try:
        def get_team_stats_unified(team_id):
            url = "https://v3.football.api-sports.io/fixtures"
            params = {"team": team_id, "last": "10", "status": "FT"}
            res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
            jogos = res.get('response', [])

            if not jogos: return "Sem dados.", 0, 0, 0

            resumo_txt = ""
            over_gols_count = 0
            sequencia = []
            total_gols_marcados = 0

            for j in jogos[:5]:
                goals_home = j['goals']['home'] or 0
                goals_away = j['goals']['away'] or 0

                if j['teams']['home']['id'] == team_id:
                    gols_time = goals_home; gols_adv = goals_away
                else:
                    gols_time = goals_away; gols_adv = goals_home

                if gols_time > gols_adv: sequencia.append('V')
                elif gols_time == gols_adv: sequencia.append('E')
                else: sequencia.append('D')

                total_gols_marcados += gols_time
                if (goals_home + goals_away) >= 2: over_gols_count += 1

            seq_str = ' '.join(sequencia)
            media_gols = total_gols_marcados / 5 if len(jogos) >= 5 else 0
            resumo_txt = f"{seq_str} | Média {media_gols:.1f} gols/jogo"
            pct_over_recent = int((over_gols_count / min(len(jogos), 5)) * 100)

            # [OTIMIZADO] Cartões via BigData Firebase (0 API calls) ao invés de 10 calls individuais
            media_cards = 0
            total_reds = 0
            try:
                if db_firestore:
                    docs = db_firestore.collection("BigData_Futebol").where("home_id", "==", str(team_id)).limit(10).stream()
                    docs2 = db_firestore.collection("BigData_Futebol").where("away_id", "==", str(team_id)).limit(10).stream()
                    total_cards = 0; card_games = 0
                    for d in list(docs) + list(docs2):
                        dd = d.to_dict()
                        st_d = dd.get('estatisticas', {})
                        yc = float(st_d.get('cartoes_amarelos', 0) or 0)
                        rc = float(st_d.get('cartoes_vermelhos', 0) or 0)
                        total_cards += yc + rc; total_reds += int(rc); card_games += 1
                    if card_games > 0:
                        media_cards = total_cards / card_games
            except: pass

            return resumo_txt, pct_over_recent, media_cards, total_reds

        # 2 API calls total (1 per team) instead of 22!
        h_txt, h_pct, h_med_cards, h_reds = get_team_stats_unified(home_id)
        a_txt, a_pct, a_med_cards, a_reds = get_team_stats_unified(away_id)

        return {
            "home": {"resumo": h_txt, "micro": h_pct, "avg_cards": h_med_cards, "reds": h_reds},
            "away": {"resumo": a_txt, "micro": a_pct, "avg_cards": a_med_cards, "reds": a_reds}
        }
    except Exception as e:
        print(f"Erro MacroMicro: {e}")
        return None

@st.cache_data(ttl=600)  # Cache 10min (era 2min = muitas calls)

def buscar_agenda_cached(api_key, date_str):

    try:

        url = "https://v3.football.api-sports.io/fixtures"

        return requests.get(url, headers={"x-apisports-key": api_key}, params={"date": date_str, "timezone": "America/Sao_Paulo"}).json().get('response', [])

    except: return []

# ==============================================================================

# [NOVO] FUNÇÕES DE INTELIGÊNCIA HÍBRIDA (MÚLTIPLAS + NOVOS MERCADOS)

# ==============================================================================

def carregar_contexto_global_firebase():

    if not db_firestore: return "Firebase Offline."

    try:

        docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(20000).stream()

        stats_gerais = {"total": 0, "over05": 0, "gols_total": 0}

        for d in docs:

            dd = d.to_dict()

            stats_gerais["total"] += 1

            placar = dd.get('placar_final', '0x0')

            try:

                gh, ga = map(int, placar.split('x'))

                if (gh + ga) > 0: stats_gerais["over05"] += 1

                stats_gerais["gols_total"] += (gh + ga)

            except: pass

        if stats_gerais["total"] == 0: return "Sem dados no Firebase."

        media_gols = stats_gerais["gols_total"] / stats_gerais["total"]

        pct_over05 = (stats_gerais["over05"] / stats_gerais["total"]) * 100

        return f"BIG DATA (Base Massiva {stats_gerais['total']} jogos): Média de Gols {media_gols:.2f} | Taxa Over 0.5 Global: {pct_over05:.1f}%."

    except Exception as e: return f"Erro Firebase: {e}"

def filtrar_multiplas_nao_correlacionadas(itens, janela_min=90):
    """Remove itens correlacionados (mesma liga ou kickoff dentro da janela_min).
    Funciona com lista de dicts contendo (fid, league_id, kickoff) ou (fid) usando st.session_state['multipla_meta'].
    Mantém a ordem original (prioriza o que a IA escolheu primeiro).
    """
    try:
        meta_global = st.session_state.get('multipla_meta', {}) or {}
        selecionados = []
        for it in (itens or []):
            fid = str(it.get('fid', ''))
            lid = it.get('league_id', None)
            ko  = it.get('kickoff', None)
            if (lid is None or ko is None) and fid in meta_global:
                mg = meta_global.get(fid, {})
                lid = lid if lid is not None else mg.get('league_id')
                ko  = ko  if ko  is not None else mg.get('kickoff')
            correlacionado = False
            for s in selecionados:
                fid_s = str(s.get('fid', ''))
                lid_s = s.get('league_id', None)
                ko_s  = s.get('kickoff', None)
                if (lid_s is None or ko_s is None) and fid_s in meta_global:
                    ms = meta_global.get(fid_s, {})
                    lid_s = lid_s if lid_s is not None else ms.get('league_id')
                    ko_s  = ko_s  if ko_s  is not None else ms.get('kickoff')
                if lid is not None and lid_s is not None and str(lid) == str(lid_s):
                    correlacionado = True; break
                try:
                    if ko and ko_s and abs((ko - ko_s).total_seconds()) <= janela_min * 60:
                        correlacionado = True; break
                except:
                    pass
            if not correlacionado:
                selecionados.append(it)
        return selecionados
    except:
        return itens

def gerar_multipla_matinal_ia(api_key):

    if not IA_ATIVADA: return None, []

    hoje = get_time_br().strftime('%Y-%m-%d')

    try:

        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])

        jogos_candidatos = [j for j in jogos if j['fixture']['status']['short'] == 'NS']

        if len(jogos_candidatos) < 2: return None, []

        lista_jogos_txt = ""
        mapa_jogos = {}
        count_validos = 0

        for j in jogos_candidatos:
            if count_validos >= 25: break  # [OTIMIZADO] 40→25

            fid = j['fixture']['id']
            home = j['teams']['home']['name']
            away = j['teams']['away']['name']
            home_id = j['teams']['home']['id']
            away_id = j['teams']['away']['id']
            liga_nome = j['league']['name']

            try:
                stats_h = analisar_tendencia_50_jogos(api_key, home_id, away_id)
                if not stats_h or stats_h['home']['qtd'] == 0: continue

                mapa_jogos[fid] = f"{home} x {away}"

                # ═══ [NOVO] Dados extras para Múltipla ═══
                h = stats_h['home']; a = stats_h['away']

                # BigData
                bd_txt = ""
                try:
                    bd = consultar_bigdata_cenario_completo(home_id, away_id)
                    if bd and "Sem dados" not in str(bd): bd_txt = f"| BigData: {str(bd)[:80]}"
                except: pass

                # Winrate liga
                wr_txt = ""
                try:
                    df = st.session_state.get('historico_full', pd.DataFrame())
                    if not df.empty:
                        df_l = df[(df['Liga'] == liga_nome) & (df['Resultado'].isin(['✅ GREEN', '❌ RED']))]
                        if len(df_l) >= 3:
                            wr = (len(df_l[df_l['Resultado'].str.contains('GREEN')]) / len(df_l)) * 100
                            wr_txt = f"| WR Liga: {wr:.0f}%"
                            if wr < 40: wr_txt += " ⚠️ TÓXICA"
                except: pass

                # Liga bloqueada?
                regra_txt = ""
                try:
                    regras = st.session_state.get('regras_aprendidas', {})
                    for strat, ligas_ruins in regras.get('ligas_bloqueadas', {}).items():
                        if liga_nome in ligas_ruins:
                            regra_txt = "| 🚫 BLOQUEADA"
                            break
                except: pass

                lista_jogos_txt += f"- ID {fid}: {home} x {away} ({liga_nome}) | Over1.5: Casa {h['over15_ft']}%/Fora {a['over15_ft']}% | Over2.5: Casa {h.get('over25_ft',0)}%/Fora {a.get('over25_ft',0)}% | BTTS: {h['ambas_marcam']}%/{a['ambas_marcam']}% {bd_txt} {wr_txt} {regra_txt}\n"
                count_validos += 1
            except: pass

        if not lista_jogos_txt: return None, []

        contexto_firebase = carregar_contexto_global_firebase()
        df_sheets = st.session_state.get('historico_full', pd.DataFrame())
        winrate_sheets = "N/A"
        if not df_sheets.empty:
            greens = len(df_sheets[df_sheets['Resultado'].str.contains('GREEN', na=False)])
            total = len(df_sheets[df_sheets['Resultado'].isin(['✅ GREEN', '❌ RED'])])
            if total > 0: winrate_sheets = f"{(greens/total)*100:.1f}%"

        prompt = f"""
        Atue como GESTOR DE RISCO E ESTRATÉGIA.
        OBJETIVO: Criar uma "Múltipla de Segurança" (Bingo Matinal) com 2 ou 3 jogos para HOJE.
        DADOS GLOBAIS: Winrate Pessoal: {winrate_sheets}. {contexto_firebase}.

        🚨 REGRAS OBRIGATÓRIAS:
        - Se "⚠️ TÓXICA" ou "🚫 BLOQUEADA" → NÃO use esse jogo.
        - Escolha APENAS jogos com Over 1.5 FT > 75% em AMBOS os times.
        - PREFIRA Over 1.5 (mais seguro) ao invés de Over 2.5.
        - Só use Over 2.5 se AMBOS os times > 65% em Over 2.5.

        LISTA DE CANDIDATOS:
        {lista_jogos_txt}

        TAREFA:
        Escolha os 2 ou 3 jogos estatisticamente MAIS SEGUROS para Over 0.5 Gols ou Over 1.5 Gols.
        Não tenha preconceito com ligas menores, foque nos NÚMEROS.

        FORMATO JSON: {{ "jogos": [ {{"fid": 123, "jogo": "A x B", "motivo": "Over1.5 Casa 85% Fora 78%"}} ], "probabilidade_combinada": "90" }}
        """

        response = gemini_safe_call(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        if not response: return None, []
        return json.loads(response.text), mapa_jogos

    except Exception as e: return None, []

def formatar_sniper_para_telegram(texto_gemini):
    """
    Formata o texto do Gemini para ficar bonito no Telegram com HTML.
    """
    try:
        # Converte markdown para HTML
        texto = texto_gemini

        # Remove o preâmbulo do Gemini
        texto = re.sub(r'^Ok,.*?ativado\.\s*', '', texto, flags=re.DOTALL)

        # Substitui **texto** por <b>texto</b>
        texto = re.sub(r'\*\*([^\*]+)\*\*', r'<b>\1</b>', texto)

        # Substitui __texto__ por <i>texto</i>
        texto = re.sub(r'__([^_]+)__', r'<i>\1</i>', texto)

        # Melhora as linhas divisórias
        texto = re.sub(r'^\*\*.*?\*\*$', lambda m: f"━━━━━━━━━━━━━━━━━━━━━━━━━\n{m.group(0)}\n━━━━━━━━━━━━━━━━━━━━━━━━━", texto, flags=re.MULTILINE)

        # Corrige bullets
        texto = re.sub(r'^\s*\*\s+<b>', '• <b>', texto, flags=re.MULTILINE)
        texto = re.sub(r'^\s*\*\s+', '• ', texto, flags=re.MULTILINE)

        # Adiciona quebras de linha extras para melhor legibilidade
        texto = re.sub(r'(<b>[^<]+</b>)\s*\n', r'\1\n\n', texto)

        return texto
    except Exception as e:
        print(f"Erro ao formatar Sniper: {e}")
        return texto_gemini

def validar_snipers_pos_gemini(texto_ia, mapa_jogos, api_key):
    """
    FILTRO INTELIGENTE PÓS-GEMINI
    Cruza CADA palpite do Gemini contra dados REAIS:
    - Firebase BigData (1100+ jogos)
    - Histórico pessoal (Sheets)
    - Estatísticas de 50 jogos (já cached)
    - Regras de aprendizado (ligas tóxicas)
    Se o palpite contradiz os dados → remove ou rebaixa
    """
    import re
    if not texto_ia or len(texto_ia) < 100:
        return texto_ia

    # Zonas e seus tipos
    zonas = {
        'ZONA DE GOLS': 'GOLS',
        'ZONA DE TRINCHEIRA': 'UNDER',
        'ZONA DE MATCH': 'MATCH',
        'ZONA DE CART': 'CARTOES',
        'ZONA DE ESCAN': 'ESCANTEIOS',
        'ZONA DE DEFESA': 'DEFESAS',
    }

    # Encontra posições de cada zona
    zonas_pos = []
    for key in zonas:
        m = re.search(key, texto_ia, re.IGNORECASE)
        if m:
            zonas_pos.append((m.start(), key))
    zonas_pos.sort(key=lambda x: x[0])

    blocos_validados = []
    total_orig = 0
    total_validado = 0
    removidos = []

    for idx, (pos, zona_key) in enumerate(zonas_pos):
        fim = zonas_pos[idx + 1][0] if idx + 1 < len(zonas_pos) else len(texto_ia)
        bloco = texto_ia[pos:fim]
        tipo = zonas[zona_key]

        # Extrai jogos deste bloco
        jogos_bloco = re.findall(r'(?:Jogo|⚽)[:\s]*(.+?)(?:\n|$)', bloco)
        total_orig += len(jogos_bloco)

        # Valida cada jogo contra dados reais
        linhas_bloco = bloco.split('\n')
        bloco_limpo = []
        jogo_atual_linhas = []
        jogo_atual_nome = ""

        for linha in linhas_bloco:
            # Detecta início de novo jogo
            if re.search(r'(?:Jogo|⚽)', linha):
                # Valida jogo anterior se existir
                if jogo_atual_linhas and jogo_atual_nome:
                    score, motivo = _validar_palpite_sniper(jogo_atual_nome, tipo, mapa_jogos, api_key)
                    if score >= 60:
                        # Adiciona score de confiança
                        jogo_atual_linhas.append(f"📊 <i>Confiança: {score}% ({motivo})</i>")
                        bloco_limpo.extend(jogo_atual_linhas)
                        total_validado += 1
                    else:
                        removidos.append(f"{jogo_atual_nome} ({tipo}: {score}% - {motivo})")

                jogo_atual_linhas = [linha]
                # Extrai nome do jogo
                match = re.search(r'(?:Jogo|⚽)[:\s]*(.+)', linha)
                jogo_atual_nome = match.group(1).strip().replace('<b>', '').replace('</b>', '') if match else ""
            elif jogo_atual_linhas:
                jogo_atual_linhas.append(linha)
            else:
                bloco_limpo.append(linha)  # Cabeçalho da zona

        # Valida último jogo do bloco
        if jogo_atual_linhas and jogo_atual_nome:
            score, motivo = _validar_palpite_sniper(jogo_atual_nome, tipo, mapa_jogos, api_key)
            if score >= 60:
                jogo_atual_linhas.append(f"📊 <i>Confiança: {score}% ({motivo})</i>")
                bloco_limpo.extend(jogo_atual_linhas)
                total_validado += 1
            else:
                removidos.append(f"{jogo_atual_nome} ({tipo}: {score}% - {motivo})")

        if not bloco_limpo:
            # Zona ficou vazia → adiciona aviso
            header_match = re.match(r'.*?(?:ZONA\s+DE\s+\w+)', bloco)
            header = header_match.group(0) if header_match else bloco.split('\n')[0]
            bloco_limpo = [header, "Nenhum jogo passou na validação de dados."]

        blocos_validados.append('\n'.join(bloco_limpo))

    # Reconstrói texto
    if blocos_validados:
        # Pega texto antes da primeira zona
        primeira_zona = zonas_pos[0][0] if zonas_pos else len(texto_ia)
        header = texto_ia[:primeira_zona]
        texto_final = header + '\n'.join(blocos_validados)

        # Adiciona resumo da validação
        if removidos:
            texto_final += f"\n\n🔍 <i>Validação: {total_validado}/{total_orig} passaram ({len(removidos)} removidos por dados fracos)</i>"

        print(f"[SNIPER VALIDAÇÃO] ✅ {total_validado}/{total_orig} aprovados | Removidos: {', '.join(removidos[:5])}")
        return texto_final

    return texto_ia

def _validar_palpite_sniper(jogo_nome, tipo_zona, mapa_jogos, api_key):
    """
    Valida UM palpite contra dados reais. Retorna (score 0-100, motivo).
    Cruza: Firebase BigData + Histórico Sheets + Stats 50j + Learning Rules
    """
    score = 50  # Base neutra
    motivos = []

    # 1. Tenta encontrar o jogo no mapa
    jogo_info = None
    jogo_clean = jogo_nome.replace('<b>', '').replace('</b>', '').replace('**', '').strip()
    for key, val in mapa_jogos.items():
        key_clean = str(key).replace('<b>', '').replace('</b>', '')
        if isinstance(val, dict):
            parts = key_clean.split(' x ')
            if len(parts) >= 2:
                if parts[0].strip().lower() in jogo_clean.lower() or parts[1].strip().lower() in jogo_clean.lower():
                    jogo_info = val
                    break

    if not jogo_info:
        return 50, "sem_dados"

    home_id = jogo_info.get('home_id', 0)
    away_id = jogo_info.get('away_id', 0)

    # ═══ FONTE 1: BigData Firebase (gols, chutes, cantos) ═══
    try:
        bd = consultar_bigdata_cenario_completo(home_id, away_id)
        if bd and "Sem dados" not in str(bd) and "Offline" not in str(bd):
            import re as re2
            gols_matches = re2.findall(r'Gols\s*([\d.]+)', str(bd))
            cantos_matches = re2.findall(r'Cantos\s*([\d.]+)', str(bd))
            sog_matches = re2.findall(r'ChutesGol\s*([\d.]+)', str(bd))

            if gols_matches:
                avg_gols = sum(float(g) for g in gols_matches) / len(gols_matches)
                if tipo_zona == 'GOLS':
                    if avg_gols >= 1.5: score += 15; motivos.append(f"BD:{avg_gols:.1f}g")
                    elif avg_gols >= 1.0: score += 5
                    else: score -= 15; motivos.append(f"BD:baixo({avg_gols:.1f}g)")
                elif tipo_zona == 'UNDER':
                    if avg_gols < 1.0: score += 15; motivos.append(f"BD:{avg_gols:.1f}g")
                    elif avg_gols > 1.8: score -= 15; motivos.append(f"BD:alto({avg_gols:.1f}g)")

            if cantos_matches and tipo_zona == 'ESCANTEIOS':
                avg_cantos = sum(float(c) for c in cantos_matches) / len(cantos_matches)
                if avg_cantos >= 5.0: score += 15; motivos.append(f"BD:{avg_cantos:.0f}c")
                else: score -= 10

            if sog_matches and tipo_zona == 'DEFESAS':
                avg_sog = sum(float(s) for s in sog_matches) / len(sog_matches)
                if avg_sog >= 4.0: score += 15; motivos.append(f"BD:{avg_sog:.0f}ch")
                else: score -= 10
    except: pass

    # ═══ FONTE 2: Stats 50 jogos (cached) ═══
    try:
        macro = analisar_tendencia_50_jogos(api_key, home_id, away_id)
        if macro:
            h = macro['home']; a = macro['away']
            if tipo_zona == 'GOLS':
                over15_avg = (h.get('over15_ft', 0) + a.get('over15_ft', 0)) / 2
                over25_avg = (h.get('over25_ft', 0) + a.get('over25_ft', 0)) / 2
                if over15_avg >= 75: score += 15; motivos.append(f"50j:{over15_avg:.0f}%o15")
                elif over15_avg >= 60: score += 5
                else: score -= 15; motivos.append(f"50j:fraco({over15_avg:.0f}%)")
            elif tipo_zona == 'UNDER':
                over15_avg = (h.get('over15_ft', 0) + a.get('over15_ft', 0)) / 2
                if over15_avg < 50: score += 15; motivos.append(f"50j:{over15_avg:.0f}%o15")
                else: score -= 10
            elif tipo_zona == 'MATCH':
                btts_avg = (h.get('ambas_marcam', 0) + a.get('ambas_marcam', 0)) / 2
                score += 5  # Match odds sempre tem algum fundamento
                motivos.append("50j:ok")
    except: pass

    # ═══ FONTE 3: Histórico pessoal (Sheets) ═══
    try:
        df = st.session_state.get('historico_full', pd.DataFrame())
        if not df.empty:
            # Winrate do jogo (times envolvidos)
            for team_part in jogo_clean.split(' x '):
                team = team_part.strip()
                if not team: continue
                f_t = df[df['Jogo'].str.contains(team, na=False, case=False)]
                f_done = f_t[f_t['Resultado'].isin(['✅ GREEN', '❌ RED'])]
                if len(f_done) >= 3:
                    wr = (len(f_done[f_done['Resultado'].str.contains('GREEN')]) / len(f_done)) * 100
                    if wr >= 70: score += 10; motivos.append(f"{team[:8]}:{wr:.0f}%")
                    elif wr < 40: score -= 15; motivos.append(f"{team[:8]}:ruim({wr:.0f}%)")
    except: pass

    # ═══ FONTE 4: Liga no aprendizado (bloqueada?) ═══
    try:
        liga_nome = jogo_info.get('liga', '')
        regras = st.session_state.get('regras_aprendidas', {})
        ligas_block = regras.get('ligas_bloqueadas', {})
        for strat, ligas_ruins in ligas_block.items():
            if liga_nome in ligas_ruins:
                score -= 25
                motivos.append(f"🚫{liga_nome[:12]}")
                break
    except: pass

    # ═══ FONTE 5: Winrate da liga no Sheets ═══
    try:
        df = st.session_state.get('historico_full', pd.DataFrame())
        liga_nome = jogo_info.get('liga', '')
        if not df.empty and liga_nome:
            df_liga = df[df['Liga'] == liga_nome]
            df_liga_done = df_liga[df_liga['Resultado'].isin(['✅ GREEN', '❌ RED'])]
            if len(df_liga_done) >= 5:
                wr_liga = (len(df_liga_done[df_liga_done['Resultado'].str.contains('GREEN')]) / len(df_liga_done)) * 100
                if wr_liga >= 65: score += 10; motivos.append(f"Liga:{wr_liga:.0f}%")
                elif wr_liga < 40: score -= 20; motivos.append(f"Liga:tóxica({wr_liga:.0f}%)")
    except: pass

    score = max(0, min(100, score))
    motivo_str = "+".join(motivos) if motivos else "base"
    return score, motivo_str

def gerar_insights_matinais_ia(api_key):
    """V4 SNIPER: 6 zonas + dados ricos. Retorna STRING (compatível com envio simples)."""
    if not IA_ATIVADA: return "IA Offline."
    hoje = get_time_br().strftime('%Y-%m-%d')
    try:
        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])
        jogos_candidatos = [j for j in jogos if j['fixture']['status']['short'] == 'NS']
        if not jogos_candidatos: return "Sem jogos para analisar hoje."

        lista_para_ia = ""
        count = 0
        random.shuffle(jogos_candidatos)

        for j in jogos_candidatos:
            if count >= 15: break

            fid = j['fixture']['id']
            home_id = j['teams']['home']['id']
            away_id = j['teams']['away']['id']
            home = j['teams']['home']['name']
            away = j['teams']['away']['name']
            liga = j['league']['name']

            # WINRATE PESSOAL
            txt_pessoal = ''
            try:
                df_sheets = st.session_state.get('historico_full', pd.DataFrame())
                if df_sheets is not None and not df_sheets.empty:
                    parts = []
                    for team_name in [home, away]:
                        f_t = df_sheets[df_sheets['Jogo'].str.contains(team_name, na=False, case=False)]
                        if len(f_t) >= 3:
                            wr_t = (f_t['Resultado'].str.contains('GREEN', na=False).sum() / len(f_t)) * 100
                            parts.append(f"{team_name}: {wr_t:.0f}% ({len(f_t)}j)")
                    if parts:
                        txt_pessoal = "WINRATE PESSOAL: " + " | ".join(parts)
            except:
                txt_pessoal = ''

            # DADOS MACRO (50 JOGOS - cached)
            macro = analisar_tendencia_50_jogos(api_key, home_id, away_id)
            if not macro: continue

            # DADOS MICRO (10 JOGOS - cached)
            micro = None
            try: micro = analisar_tendencia_macro_micro(api_key, home_id, away_id)
            except: pass

            # BIG DATA FIREBASE
            txt_bigdata = ""
            try:
                bd = consultar_bigdata_cenario_completo(home_id, away_id)
                if bd and "Sem dados" not in str(bd) and "Offline" not in str(bd):
                    txt_bigdata = f"\n            \U0001f5c4\ufe0f BIG DATA ({bd})"
            except: pass

            # WINRATE POR LIGA
            txt_wr_liga = ""
            try:
                df_sheets = st.session_state.get('historico_full', pd.DataFrame())
                if df_sheets is not None and not df_sheets.empty:
                    df_liga = df_sheets[df_sheets['Liga'] == liga]
                    df_liga_done = df_liga[df_liga['Resultado'].isin(['\u2705 GREEN', '\u274c RED'])]
                    if len(df_liga_done) >= 3:
                        g_liga = len(df_liga_done[df_liga_done['Resultado'].str.contains('GREEN', na=False)])
                        wr_liga = (g_liga / len(df_liga_done)) * 100
                        txt_wr_liga = f"\n            \U0001f4ca WINRATE LIGA ({liga}): {wr_liga:.0f}% em {len(df_liga_done)} jogos"
                        if wr_liga < 40:
                            txt_wr_liga += " \u26a0\ufe0f LIGA T\u00d3XICA"
            except: pass

            # APRENDIZADO
            txt_regra = ""
            try:
                regras = st.session_state.get('regras_aprendidas', {})
                ligas_block_all = regras.get('ligas_bloqueadas', {})
                for strat, ligas_ruins in ligas_block_all.items():
                    if liga in ligas_ruins:
                        txt_regra = f"\n            \U0001f6ab ALERTA APRENDIZADO: Liga bloqueada para {strat}"
                        break
            except: pass

            if macro:
                h_50 = macro['home']; a_50 = macro['away']
                odd_txt = "N/D"

                micro_txt = ""
                try:
                    if micro:
                        media_cards_soma = micro['home']['avg_cards'] + micro['away']['avg_cards']
                        micro_txt = f"""
            \U0001f525 FASE ATUAL (10 Jogos):
            - {txt_pessoal}
            - Casa: {micro['home']['resumo']} | Cart\u00f5es: {micro['home']['avg_cards']:.1f}
            - Fora: {micro['away']['resumo']} | Cart\u00f5es: {micro['away']['avg_cards']:.1f}
            - Soma Cart\u00f5es: {media_cards_soma:.1f}"""
                except: micro_txt = ""

                lista_para_ia += f"""
            ---
            \u26bd Jogo: {home} x {away} ({liga})
            \U0001f4b0 Ref: {odd_txt}
            \U0001f4c5 LONGO PRAZO (50 Jogos):
            - Casa: Over 1.5 FT: {h_50.get('over15_ft', 0)}% | Over 2.5 FT: {h_50.get('over25_ft', 0)}% | Over 0.5 HT: {h_50.get('over05_ht', 0)}% | BTTS: {h_50.get('ambas_marcam', 0)}% | ({h_50.get('qtd', 0)} jogos)
            - Fora: Over 1.5 FT: {a_50.get('over15_ft', 0)}% | Over 2.5 FT: {a_50.get('over25_ft', 0)}% | Over 0.5 HT: {a_50.get('over05_ht', 0)}% | BTTS: {a_50.get('ambas_marcam', 0)}% | ({a_50.get('qtd', 0)} jogos)
            {micro_txt}{txt_bigdata}{txt_wr_liga}{txt_regra}
            """
                count += 1
                print(f"[SNIPER] Jogo {count}: {home} x {away} \u2705")

        if not lista_para_ia: return "Sem jogos com tendencia clara."

        prompt = f"""
        ATUE COMO UM CIENTISTA DE DADOS E TRADER ESPORTIVO (PERFIL SNIPER).

        Analise a lista de jogos. Voc\u00ea tem dados de **50 JOGOS** (Hist\u00f3rico) e **10 JOGOS** (Momento).
        Cruze essas informa\u00e7\u00f5es para encontrar valor real.

        DADOS DOS JOGOS:
        {lista_para_ia}

        ---------------------------------------------------------------------
        \U0001f6ab FILTRO DE ELITE (OBRIGAT\u00d3RIO - SEJA RIGOROSO):
        1. "Falso Favorito": Se o time ganhou os \u00faltimos 2 jogos, mas nos 50 jogos tem menos de 40% de vit\u00f3rias -> N\u00c3O indique Vit\u00f3ria.
        2. "Vit\u00f3ria Magra": Se o favorito costuma ganhar de 1x0 -> N\u00c3O indique Over Gols. Indique Vencedor ou Under.
        3. "Arame Liso": Se os times empatam muito (0x0, 1x1) -> OBRIGAT\u00d3RIO sugerir UNDER.
        4. "Instabilidade": Se o hist\u00f3rico mostra V-D-V-D -> Jogo imprevis\u00edvel -> DESCARTE.
        ---------------------------------------------------------------------

        \U0001f9e0 INTELIG\u00caNCIA DE SELE\u00c7\u00c3O:

        \U0001f6a8 REGRAS OBRIGAT\u00d3RIAS:
        - Se um jogo tem "\u26a0\ufe0f LIGA T\u00d3XICA" ou "\U0001f6ab ALERTA APRENDIZADO" \u2192 N\u00c3O indique esse jogo.
        - Use os dados do BIG DATA para cruzar com os 50 jogos.
        - Se WINRATE LIGA for abaixo de 40% \u2192 DESCARTE o jogo desta liga.

        SUA MISS\u00c3O: Preencher as 6 zonas abaixo. Retorne NO M\u00cdNIMO 5 jogos DIFERENTES.

        SA\u00cdDA OBRIGAT\u00d3RIA:

        \U0001f525 **ZONA DE GOLS (OVER)**
        \u26bd Jogo: [Time Casa] x [Time Fora]
        \U0001f3c6 Liga: [Nome]
        \U0001f3af Palpite: **MAIS de 1.5 Gols**
        \U0001f4b0 Ref (Over 1.5): @[odd]
        \U0001f4dd Motivo: [Cite dados de 50 jogos]

        \u2744\ufe0f **ZONA DE TRINCHEIRA (UNDER)**
        \u26bd Jogo: [Time Casa] x [Time Fora]
        \U0001f3c6 Liga: [Nome]
        \U0001f3af Palpite: **MENOS de 2.5 Gols**
        \U0001f4b0 Ref (Under 2.5): @[odd]
        \U0001f4dd Motivo: [Cite dados]

        \U0001f3c6 **ZONA DE MATCH ODDS**
        \u26bd Jogo: [Time Casa] x [Time Fora]
        \U0001f3c6 Liga: [Nome]
        \U0001f3af Palpite: **Vit\u00f3ria do [Time]**
        \U0001f4b0 Ref (Casa/Fora): @[odd]
        \U0001f4dd Motivo: [Cite dados]

        \U0001f7e8 **ZONA DE CART\u00d5ES**
        \u26bd Jogo: [Time Casa] x [Time Fora]
        \U0001f3c6 Liga: [Nome]
        \U0001f3af Palpite: **Mais de 3.5 Cart\u00f5es**
        \U0001f4dd Motivo: [Cite m\u00e9dia de cart\u00f5es]

        \U0001f3f4 **ZONA DE ESCANTEIOS**
        \u26bd Jogo: [Time Casa] x [Time Fora]
        \U0001f3c6 Liga: [Nome]
        \U0001f3af Palpite: **Mais de 8.5 Escanteios**
        \U0001f4dd Motivo: [Cite dados]

        \U0001f9e4 **ZONA DE DEFESAS DO GOLEIRO**
        \u26bd Jogo: [Time Casa] x [Time Fora]
        \U0001f3c6 Liga: [Nome]
        \U0001f3af Palpite: **Goleiro [Time]: Mais de 3.5 Defesas**
        \U0001f4dd Motivo: [Cite dados]

        IMPORTANTE:
        - Retorne NO M\u00cdNIMO 5 jogos diferentes
        - TODAS as 6 zonas devem ter pelo menos 1 indica\u00e7\u00e3o (se n\u00e3o houver dados, escreva "Nenhuma oportunidade encontrada nesta zona")
        """

        response = model_ia.generate_content(prompt, generation_config=genai.types.GenerationConfig(temperature=0.0))
        st.session_state['gemini_usage']['used'] += 1
        texto_ia = response.text

        import re
        # Remove preambulo
        texto_ia = re.sub(r'^Ok,.*?ativado\.?\s*', '', texto_ia, flags=re.DOTALL)
        # Converte markdown para HTML
        texto_ia = re.sub(r'\*\*([^\*]+)\*\*', r'<b>\1</b>', texto_ia)
        texto_ia = re.sub(r'__([^_]+)__', r'<i>\1</i>', texto_ia)
        # Fix bullets
        texto_ia = re.sub(r'^\s*\*\s+', '├─ ', texto_ia, flags=re.MULTILINE)
        # Safety: close open HTML tags
        abertos_b = texto_ia.count('<b>') - texto_ia.count('</b>')
        abertos_i = texto_ia.count('<i>') - texto_ia.count('</i>')
        if abertos_b > 0: texto_ia += '</b>' * abertos_b
        if abertos_i > 0: texto_ia += '</i>' * abertos_i

        return texto_ia

    except Exception as e: return f"Erro na analise: {str(e)}"

def gerar_analise_mercados_alternativos_ia(api_key):

    if not IA_ATIVADA: return []

    hoje = get_time_br().strftime('%Y-%m-%d')

    try:

        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])

        jogos_candidatos = [
            j for j in jogos
            if j['fixture'].get('referee') and j['fixture']['status']['short'] == 'NS'
        ]

        if not jogos_candidatos: return []

        # ═══ [NOVO] COLETA DE DADOS REAIS POR JOGO ═══
        amostra = jogos_candidatos[:30]  # Max 30 (economiza API)
        dados_analise = ""
        mapa_alt = {}

        for j in amostra:
            fid = j['fixture']['id']
            home = j['teams']['home']['name']
            away = j['teams']['away']['name']
            home_id = j['teams']['home']['id']
            away_id = j['teams']['away']['id']
            referee = j['fixture']['referee']
            liga_nome = j['league']['name']

            # 1. Stats micro (10 jogos) → cartões reais
            cards_txt = ""
            try:
                micro = analisar_tendencia_macro_micro(api_key, home_id, away_id)
                if micro and isinstance(micro, dict):
                    h_cards = micro.get('home', {}).get('avg_cards', 0)
                    a_cards = micro.get('away', {}).get('avg_cards', 0)
                    soma_cards = h_cards + a_cards
                    cards_txt = f"Cartões(10j): Casa {h_cards:.1f} + Fora {a_cards:.1f} = {soma_cards:.1f}/jogo"
            except: pass

            # 2. BigData Firebase (gols, chutes, cantos)
            bd_txt = ""
            try:
                bd = consultar_bigdata_cenario_completo(home_id, away_id)
                if bd and "Sem dados" not in str(bd) and "Offline" not in str(bd):
                    bd_txt = f"BigData: {bd}"
            except: pass

            # 3. Stats 50j (chutes, gols para defesas)
            stats_txt = ""
            try:
                macro = analisar_tendencia_50_jogos(api_key, home_id, away_id)
                if macro:
                    h = macro['home']; a = macro['away']
                    stats_txt = f"50j: Casa Over1.5={h.get('over15_ft',0)}% BTTS={h.get('ambas_marcam',0)}% | Fora Over1.5={a.get('over15_ft',0)}% BTTS={a.get('ambas_marcam',0)}%"
            except: pass

            # 4. Winrate liga (Sheets)
            wr_liga_txt = ""
            try:
                df = st.session_state.get('historico_full', pd.DataFrame())
                if not df.empty:
                    df_liga = df[(df['Liga'] == liga_nome) & (df['Resultado'].isin(['✅ GREEN', '❌ RED']))]
                    if len(df_liga) >= 3:
                        wr = (len(df_liga[df_liga['Resultado'].str.contains('GREEN')]) / len(df_liga)) * 100
                        wr_liga_txt = f"WR Liga: {wr:.0f}% ({len(df_liga)}j)"
                        if wr < 40: wr_liga_txt += " ⚠️ TÓXICA"
            except: pass

            # 5. Regras de aprendizado
            regra_txt = ""
            try:
                regras = st.session_state.get('regras_aprendidas', {})
                ligas_block = regras.get('ligas_bloqueadas', {})
                for strat, ligas_ruins in ligas_block.items():
                    if liga_nome in ligas_ruins:
                        regra_txt = f"🚫 BLOQUEADA para {strat}"
                        break
            except: pass

            mapa_alt[str(fid)] = {'home': home, 'away': away, 'liga': liga_nome, 'home_id': home_id, 'away_id': away_id}

            dados_analise += f"""- Jogo: {home} x {away} | Liga: {liga_nome} | Árbitro: {referee} | ID: {fid}
  {cards_txt}
  {stats_txt}
  {bd_txt}
  {wr_liga_txt}
  {regra_txt}
"""

        prompt = f"""
        ATUE COMO UM ESPECIALISTA EM MERCADOS ALTERNATIVOS (CARTÕES + DEFESAS DE GOLEIRO).

        LISTA DE JOGOS COM DADOS REAIS:
        {dados_analise}

        🚨 REGRAS OBRIGATÓRIAS:
        - Se "⚠️ TÓXICA" ou "🚫 BLOQUEADA" → NÃO indique esse jogo.
        - CARTÕES: Só indique se Soma Cartões (10j) >= 4.0 E árbitro for rigoroso.
        - DEFESAS: Só indique se houver desnível técnico claro nos stats (Over1.5 alto de um lado = muitos chutes).
        - Use os NÚMEROS dos dados, não invente.

        CRITÉRIOS:
        1. CARTÕES: Soma Cartões >= 4.5 → "Mais de 3.5 Cartões". Soma >= 5.5 → "Mais de 4.5 Cartões".
        2. DEFESAS: Time forte (>60% Over) vs fraco → Goleiro do fraco vai trabalhar → "Mais de 3.5 Defesas".

        SAÍDA OBRIGATÓRIA (JSON):
        {{
            "sinais": [
                {{
                    "fid": "12345",
                    "tipo": "CARTAO" ou "GOLEIRO",
                    "titulo": "🟨 SNIPER DE CARTÕES" ou "🧤 MURALHA (DEFESAS)",
                    "jogo": "Time A x Time B",
                    "destaque": "Soma cartões 5.2/jogo + Árbitro rigoroso",
                    "indicacao": "Mais de 3.5 Cartões"
                }}
            ]
        }}
        """

        response = gemini_safe_call(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
        if not response: return []
        return json.loads(response.text).get('sinais', [])

    except: return []

# [MÓDULO ATUALIZADO] ESTRATÉGIA ALAVANCAGEM SNIPER (TOP 3)

def gerar_bet_builder_alavancagem(api_key):

    if not IA_ATIVADA: return []

    hoje = get_time_br().strftime('%Y-%m-%d')

    try:

        url = "https://v3.football.api-sports.io/fixtures"
        params = {"date": hoje, "timezone": "America/Sao_Paulo"}
        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()
        jogos = res.get('response', [])

        LIGAS_ALAVANCAGEM = [39, 140, 78, 135, 61, 71, 72, 2, 3]
        candidatos = [j for j in jogos if j['league']['id'] in LIGAS_ALAVANCAGEM and j['fixture']['status']['short'] == 'NS']

        if not candidatos: return []

        lista_provaveis = []
        df_historico = st.session_state.get('historico_full', pd.DataFrame())

        for j in candidatos:
            try:
                home_nm = j['teams']['home']['name']
                away_nm = j['teams']['away']['name']
                home_id = j['teams']['home']['id']
                away_id = j['teams']['away']['id']
                liga_nome = j['league']['name']

                # ═══ [NOVO] Verifica liga bloqueada ═══
                liga_bloqueada = False
                try:
                    regras = st.session_state.get('regras_aprendidas', {})
                    for strat, ligas_ruins in regras.get('ligas_bloqueadas', {}).items():
                        if liga_nome in ligas_ruins:
                            liga_bloqueada = True
                            break
                except: pass
                if liga_bloqueada: continue

                dados_bd = consultar_bigdata_cenario_completo(home_id, away_id)

                # ═══ [NOVO] Stats 50 jogos ═══
                stats_50j = ""
                cards_txt = ""
                try:
                    macro = analisar_tendencia_50_jogos(api_key, home_id, away_id)
                    if macro:
                        h = macro['home']; a = macro['away']
                        stats_50j = f"50j: Over1.5 Casa={h.get('over15_ft',0)}% Fora={a.get('over15_ft',0)}% | Over2.5 Casa={h.get('over25_ft',0)}% Fora={a.get('over25_ft',0)}% | BTTS={h.get('ambas_marcam',0)}%/{a.get('ambas_marcam',0)}%"
                except: pass

                try:
                    micro = analisar_tendencia_macro_micro(api_key, home_id, away_id)
                    if micro and isinstance(micro, dict):
                        h_c = micro.get('home', {}).get('avg_cards', 0)
                        a_c = micro.get('away', {}).get('avg_cards', 0)
                        cards_txt = f"Cartões(10j): {h_c:.1f}+{a_c:.1f}={h_c+a_c:.1f}"
                except: pass

                txt_historico_pessoal = "Sem histórico recente."
                wr_h = 0; wr_a = 0
                if not df_historico.empty:
                    f_h = df_historico[df_historico['Jogo'].str.contains(home_nm, na=False, case=False)]
                    f_a = df_historico[df_historico['Jogo'].str.contains(away_nm, na=False, case=False)]
                    if len(f_h) > 0: wr_h = len(f_h[f_h['Resultado'].str.contains('GREEN', na=False)]) / len(f_h) * 100
                    if len(f_a) > 0: wr_a = len(f_a[f_a['Resultado'].str.contains('GREEN', na=False)]) / len(f_a) * 100
                    if len(f_h) > 0 or len(f_a) > 0:
                        txt_historico_pessoal = f"Histórico: {home_nm} ({wr_h:.0f}%) | {away_nm} ({wr_a:.0f}%)."

                # ═══ [MELHORADO] Score baseado em dados reais ═══
                score = 0
                if macro:
                    h = macro['home']; a = macro['away']
                    if h.get('over15_ft', 0) >= 70 and a.get('over15_ft', 0) >= 70: score += 3
                    if h.get('ambas_marcam', 0) >= 50 and a.get('ambas_marcam', 0) >= 50: score += 2
                if dados_bd and "Sem dados" not in str(dados_bd): score += 1
                if (len(f_h) > 2 and wr_h < 40) or (len(f_a) > 2 and wr_a < 40): score -= 5

                if score >= 2:
                    lista_provaveis.append({
                        "jogo": j,
                        "bigdata": dados_bd,
                        "stats_50j": stats_50j,
                        "cards": cards_txt,
                        "historico": txt_historico_pessoal,
                        "referee": j['fixture'].get('referee', 'Desconhecido'),
                        "score": score
                    })
            except: pass

        if not lista_provaveis: return []

        lista_provaveis.sort(key=lambda x: x['score'], reverse=True)
        top_picks = lista_provaveis[:3]

        resultados_finais = []
        for pick in top_picks:
            j = pick['jogo']
            home = j['teams']['home']['name']
            away = j['teams']['away']['name']
            fid = j['fixture']['id']

            prompt = f"""
            ATUE COMO ANALISTA SÊNIOR DE ALAVANCAGEM.
            MONTE UM BET BUILDER PARA ESTE JOGO ESPECÍFICO.

            DADOS:
            1. JOGO: {home} x {away} ({j['league']['name']})
            2. BIG DATA: {pick['bigdata']}
            3. {pick['stats_50j']}
            4. {pick['cards']}
            5. HISTÓRICO USER: {pick['historico']}
            6. JUIZ: {pick['referee']}

            REGRA OBRIGATÓRIA: As seleções devem ser ESPECÍFICAS com valores exatos.
            NÃO use reticências ou termos vagos como "Vencedor..." ou "Gols...".
            Use frases completas como "Vitória do {home}", "Mais de 1.5 Gols", "Mais de 3.5 Cartões".
            Baseie CADA seleção nos DADOS acima. Se Over 2.5 < 55%, use Over 1.5.

            SAÍDA JSON:
            {{
                "titulo": "🚀 ALAVANCAGEM {home} vs {away}",
                "selecoes": ["Vitória do [Time Específico]", "Mais de 1.5 Gols", "Mais de 3.5 Cartões"],
                "analise_ia": "Explicação técnica rápida usando os dados fornecidos.",
                "confianca": "Alta"
            }}
            """

            try:
                time.sleep(1)
                response = gemini_safe_call(prompt, generation_config=genai.types.GenerationConfig(response_mime_type="application/json"))
                if not response: continue
                r_json = json.loads(response.text)
                r_json['fid'] = fid
                r_json['jogo'] = f"{home} x {away}"
                resultados_finais.append(r_json)
            except: pass

        return resultados_finais

    except Exception as e: return []

# ==============================================================================

# 4. INTELIGÊNCIA ARTIFICIAL, CÁLCULOS E ESTRATÉGIAS (O CÉREBRO)

# ==============================================================================

def buscar_rating_inteligente(api_key, team_id):

    if db_firestore:

        try:

            docs_h = db_firestore.collection("BigData_Futebol").where("home_id", "==", str(team_id)).limit(20).stream()

            docs_a = db_firestore.collection("BigData_Futebol").where("away_id", "==", str(team_id)).limit(20).stream()

            notas = []

            for d in docs_h:

                dados = d.to_dict()

                if 'rating_home' in dados and float(dados['rating_home']) > 0: notas.append(float(dados['rating_home']))

            for d in docs_a:

                dados = d.to_dict()

                if 'rating_away' in dados and float(dados['rating_away']) > 0: notas.append(float(dados['rating_away']))

            if len(notas) >= 3:

                return f"{(sum(notas)/len(notas)):.2f} (Média {len(notas)}j)"

        except: pass

    try:

        url = "https://v3.football.api-sports.io/fixtures"

        params = {"team": team_id, "last": "1", "status": "FT"}

        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()

        if not res.get('response'): return "N/A"

        last_fid = res['response'][0]['fixture']['id']

        url_stats = "https://v3.football.api-sports.io/fixtures/players"

        p_res = requests.get(url_stats, headers={"x-apisports-key": api_key}, params={"fixture": last_fid}).json()

        if not p_res.get('response'): return "N/A"

        for t in p_res['response']:

            if t['team']['id'] == team_id:

                notas = []

                for p in t['players']:

                    try:

                        rating = float(p['statistics'][0]['games']['rating'])

                        if rating > 0: notas.append(rating)

                    except: pass

                if notas: return f"{(sum(notas)/len(notas)):.2f}"

        return "N/A"

    except: return "N/A"

def estimar_odd_teorica(estrategia, tempo_jogo):

    import random

    limites = MAPA_ODDS_TEORICAS.get(estrategia, {"min": 1.40, "max": 1.60})

    odd_base_min = limites['min']

    odd_base_max = limites['max']

    fator_tempo = 0.0

    try:

        t = int(str(tempo_jogo).replace("'", ""))

        if t > 80: fator_tempo = 0.20

        elif t > 70: fator_tempo = 0.10

    except: pass

    odd_simulada = random.uniform(odd_base_min, odd_base_max) + fator_tempo

    return "{:.2f}".format(odd_simulada)

def get_live_odds(fixture_id, api_key, strategy_name, total_gols_atual=0, tempo_jogo=0):

    try:

        url = "https://v3.football.api-sports.io/odds/live"

        params = {"fixture": fixture_id}

        res = requests.get(url, headers={"x-apisports-key": api_key}, params=params).json()

        target_markets = []

        target_line = 0.0

        if "Relâmpago" in strategy_name and total_gols_atual == 0:

            target_markets = ["1st half", "first half"]; target_line = 0.5

        elif "Golden" in strategy_name and total_gols_atual == 1:

            target_markets = ["match goals", "goals over/under"]; target_line = 1.5

        else:

            ht_strategies = ["Relâmpago", "Massacre", "Choque", "Briga", "Morno"]

            is_ht = any(x in strategy_name for x in ht_strategies)

            target_markets = ["1st half", "first half"] if is_ht else ["match goals", "goals over/under"]

            target_line = total_gols_atual + 0.5

        if res.get('response'):

            markets = res['response'][0]['odds']

            for m in markets:

                m_name = m['name'].lower()

                if any(tm in m_name for tm in target_markets) and "over" in m_name:

                    for v in m['values']:

                        try:

                            line_raw = str(v['value']).lower().replace("over", "").strip()

                            line_val = float(''.join(c for c in line_raw if c.isdigit() or c == '.'))

                            if abs(line_val - target_line) < 0.1:

                                raw_odd = float(v['odd'])

                                if raw_odd > 50: raw_odd = raw_odd / 1000

                                return "{:.2f}".format(raw_odd)

                        except: pass

        return estimar_odd_teorica(strategy_name, tempo_jogo)

    except: return estimar_odd_teorica(strategy_name, tempo_jogo)

def buscar_inteligencia(estrategia, liga, jogo):

    df = st.session_state.get('historico_full', pd.DataFrame())

    if df.empty: return "\n🔮 <b>Prob: Sem Histórico</b>"

    try:

        times = jogo.split(' x ')

        if len(times) < 2: return "\n🔮 <b>Prob: Nomes Irregulares</b>"

        time_casa = times[0].split('(')[0].strip()

        time_visitante = times[1].split('(')[0].strip()

    except: return "\n🔮 <b>Prob: Erro Nome</b>"

    numerador = 0; denominador = 0; fontes = []

    try:

        f_casa = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_casa, na=False))]

        f_vis = df[(df['Estrategia'] == estrategia) & (df['Jogo'].str.contains(time_visitante, na=False))]

        if len(f_casa) >= 3 or len(f_vis) >= 3:

            wr_c = (f_casa['Resultado'].str.contains('GREEN').sum()/len(f_casa)*100) if len(f_casa)>=3 else 0

            wr_v = (f_vis['Resultado'].str.contains('GREEN').sum()/len(f_vis)*100) if len(f_vis)>=3 else 0

            div = 2 if (len(f_casa)>=3 and len(f_vis)>=3) else 1

            numerador += ((wr_c + wr_v)/div) * 5; denominador += 5; fontes.append("Time")

    except: pass

    try:

        f_liga = df[(df['Estrategia'] == estrategia) & (df['Liga'] == liga)]

        if len(f_liga) >= 3:

            wr_l = (f_liga['Resultado'].str.contains('GREEN').sum()/len(f_liga)*100)

            numerador += wr_l * 3; denominador += 3; fontes.append("Liga")

    except: pass

    if denominador == 0: return "\n🔮 <b>Prob: Calculando...</b>"

    prob_final = numerador / denominador

    str_fontes = "+".join(fontes) if fontes else "Geral"

    return f"\n{'🔥' if prob_final >= 80 else '🔮' if prob_final > 40 else '⚠️'} <b>Prob: {prob_final:.0f}% ({str_fontes})</b>"

def obter_odd_final_para_calculo(odd_registro, estrategia):

    try:

        if pd.isna(odd_registro) or str(odd_registro).strip() == "" or str(odd_registro).lower() == "nan":

            limites = MAPA_ODDS_TEORICAS.get(estrategia, {"min": 1.40, "max": 1.60})

            return (limites['min'] + limites['max']) / 2

        valor = float(odd_registro)

        if valor <= 1.01:

            limites = MAPA_ODDS_TEORICAS.get(estrategia, {"min": 1.40, "max": 1.60})

            return (limites['min'] + limites['max']) / 2

        return valor

    except: return 1.50

def calcular_veredicto_datadriven(estrategia, liga, odd_val=0, stats_live=None, momentum_h=0, momentum_a=0, bigdata_txt=""):
    """
    Veredicto COMPLETO: Histórico Sheets + BigData Firebase + Dados ao Vivo.
    Retorna: (veredicto: str, probabilidade: int, fontes: str)
    """
    scores = []
    pesos = []
    fontes = []

    # ═══ FONTE 1: HISTÓRICO PESSOAL (Sheets) — peso 3 ═══
    df = st.session_state.get('historico_full', pd.DataFrame())
    if not df.empty:
        df_final = df[df['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
        if len(df_final) >= 10:
            # 1a. Winrate da estratégia
            df_strat = df_final[df_final['Estrategia'] == estrategia]
            if len(df_strat) >= 5:
                g_s = len(df_strat[df_strat['Resultado'].str.contains('GREEN')])
                wr_strat = (g_s / len(df_strat)) * 100
                scores.append(wr_strat)
                pesos.append(3)
                fontes.append(f"Strat:{wr_strat:.0f}%({len(df_strat)}j)")

            # 1b. Winrate estratégia + liga (mais forte)
            df_sl = df_final[(df_final['Estrategia'] == estrategia) & (df_final['Liga'] == liga)]
            if len(df_sl) >= 3:
                g_sl = len(df_sl[df_sl['Resultado'].str.contains('GREEN')])
                wr_sl = (g_sl / len(df_sl)) * 100
                scores.append(wr_sl)
                pesos.append(5)
                fontes.append(f"Liga:{wr_sl:.0f}%({len(df_sl)}j)")

            # 1c. Winrate por faixa de odd
            if odd_val > 1.0:
                try:
                    df_final['Odd_Num'] = df_final['Odd'].apply(
                        lambda x: float(str(x).replace(',','.')) if str(x).replace(',','.').replace('.','',1).isdigit() else 0
                    )
                    if odd_val < 1.30: fmin, fmax = 1.01, 1.35
                    elif odd_val < 1.50: fmin, fmax = 1.30, 1.55
                    elif odd_val < 1.80: fmin, fmax = 1.50, 1.85
                    else: fmin, fmax = 1.80, 5.0
                    df_f = df_final[df_final['Odd_Num'].between(fmin, fmax)]
                    if len(df_f) >= 5:
                        g_f = len(df_f[df_f['Resultado'].str.contains('GREEN')])
                        wr_f = (g_f / len(df_f)) * 100
                        scores.append(wr_f)
                        pesos.append(2)
                        fontes.append(f"Odd:{wr_f:.0f}%")
                except: pass

    # ═══ FONTE 2: BIG DATA FIREBASE (1100+ jogos) — peso 4 ═══
    if bigdata_txt and "Sem dados" not in bigdata_txt and "Offline" not in bigdata_txt and "Erro" not in bigdata_txt:
        try:
            import re as re2
            # Extrai médias de gols do BigData
            gols_matches = re2.findall(r'Gols\s*([\d.]+)', bigdata_txt)
            cantos_matches = re2.findall(r'Cantos\s*([\d.]+)', bigdata_txt)
            sog_matches = re2.findall(r'ChutesGol\s*([\d.]+)', bigdata_txt)

            # Score BigData baseado na estratégia
            bd_score = 50  # Base neutra

            if gols_matches:
                avg_gols = sum(float(g) for g in gols_matches) / len(gols_matches)
                if "Over" in estrategia or "Gol" in estrategia or "Blitz" in estrategia:
                    if avg_gols >= 2.0: bd_score = 80
                    elif avg_gols >= 1.5: bd_score = 70
                    elif avg_gols >= 1.0: bd_score = 60
                    else: bd_score = 35
                elif "Under" in estrategia or "Trincheira" in estrategia:
                    if avg_gols < 0.8: bd_score = 80
                    elif avg_gols < 1.2: bd_score = 70
                    else: bd_score = 40

            if sog_matches:
                avg_sog = sum(float(s) for s in sog_matches) / len(sog_matches)
                if "Over" in estrategia and avg_sog >= 4.0: bd_score += 10
                if "Under" in estrategia and avg_sog < 2.5: bd_score += 10

            bd_score = min(bd_score, 95)
            scores.append(bd_score)
            pesos.append(4)
            fontes.append(f"BD:{bd_score:.0f}%")
        except: pass

    # ═══ FONTE 3: DADOS AO VIVO (chutes, posse, momentum) — peso 3 ═══
    if stats_live:
        try:
            s1 = stats_live[0].get('statistics', []) if isinstance(stats_live[0], dict) else []
            s2 = stats_live[1].get('statistics', []) if len(stats_live) > 1 and isinstance(stats_live[1], dict) else []

            def gv(lst, tipo):
                return next((x['value'] for x in lst if x['type'] == tipo), 0) or 0

            chutes_gol = gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal')
            chutes_total = gv(s1, 'Total Shots') + gv(s2, 'Total Shots')

            live_score = 50
            if "Over" in estrategia or "Gol" in estrategia or "Blitz" in estrategia:
                if chutes_gol >= 8: live_score = 85
                elif chutes_gol >= 5: live_score = 75
                elif chutes_gol >= 3: live_score = 65
                elif chutes_gol < 2: live_score = 30

                # Momentum boost
                if momentum_h + momentum_a >= 8: live_score += 5
            elif "Under" in estrategia:
                if chutes_total < 5: live_score = 80
                elif chutes_total < 10: live_score = 65
                else: live_score = 35

            live_score = min(live_score, 95)
            scores.append(live_score)
            pesos.append(3)
            fontes.append(f"Live:{live_score:.0f}%")
        except: pass

    # ═══ WINRATE GERAL do robô (peso 1 - baseline) ═══
    if not df.empty:
        df_final2 = df[df['Resultado'].isin(['✅ GREEN', '❌ RED'])]
        if len(df_final2) > 0:
            g_total = len(df_final2[df_final2['Resultado'].str.contains('GREEN')])
            wr_total = (g_total / len(df_final2)) * 100
            scores.append(wr_total)
            pesos.append(1)

    # ═══ SEM DADOS SUFICIENTES ═══
    if not scores:
        return "Arriscado", 50, "Sem dados"  # [FIX] Sem histórico = conservador

    # ═══ CÁLCULO FINAL (média ponderada) ═══
    prob_final = sum(s * p for s, p in zip(scores, pesos)) / sum(pesos)

    # Liga bloqueada pelo aprendizado?
    regras = st.session_state.get('regras_aprendidas', {})
    ligas_block = regras.get('ligas_bloqueadas', {}).get(estrategia, [])
    if liga in ligas_block:
        return "Arriscado", int(prob_final), "+".join(fontes) + "|🚫Liga"

    # Veredicto
    if prob_final >= 72:
        return "Aprovado", int(prob_final), "+".join(fontes)
    else:
        return "Arriscado", int(prob_final), "+".join(fontes)

def consultar_ia_gemini(dados_jogo, estrategia, stats_raw, rh, ra, extra_context="", time_favoravel="", liga_nome=""):

    if not IA_ATIVADA: return "", "N/A"

    try:

        s1 = stats_raw[0]['statistics']; s2 = stats_raw[1]['statistics']

        def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0

        chutes_totais = gv(s1, 'Total Shots') + gv(s2, 'Total Shots')

        chutes_gol = gv(s1, 'Shots on Goal') + gv(s2, 'Shots on Goal')

        escanteios = gv(s1, 'Corner Kicks') + gv(s2, 'Corner Kicks')

        tempo_str = str(dados_jogo.get('tempo', '0')).replace("'", "")

        tempo = int(tempo_str) if tempo_str.isdigit() else 0

        # Filtro Hardcode: Jogo morto

        if "Under" not in estrategia and "Morno" not in estrategia:

            if tempo > 20 and chutes_totais < 2:

                return "\n🤖 <b>IA:</b> ⚠️ <b>Reprovado</b> - Jogo sem volume (Morto).", "10%"

        # PROMPT "LIBERADO" PARA TEXTO RICO

        prompt = f"""

        ATUE COMO UM TRADER ESPORTIVO SÊNIOR (TEXTO ANALÍTICO).

        CENÁRIO:

        - Jogo: {dados_jogo['jogo']} ({dados_jogo['placar']}) aos {tempo} min.

        - Estratégia: {estrategia}

        DADOS TÉCNICOS:

        - Chutes no Gol: {chutes_gol} | Total: {chutes_totais}

        - Escanteios: {escanteios}

        - Pressão (Momentum): Casa {rh} x {ra} Fora

        CONTEXTO:

        {extra_context}

        SUA MISSÃO:

        1. Calcule a probabilidade (0-100%).

        2. Dê um Veredicto (Aprovado/Arriscado/Reprovado).

        3. ESCREVA UMA ANÁLISE TÁTICA DE 2 LINHAS explicando o momento do jogo. Fale sobre quem está pressionando, se há perigo real ou se o jogo está travado.

           - Regra de Ouro: No mercado de Gols, jogo lá e cá (trocação) é BOM.

        FORMATO DE SAÍDA (Use exatamente este padrão):

        VEREDICTO: [Seu Veredicto]

        PROB: [Número]%

        ANÁLISE: [Sua explicação rica e detalhada aqui. Não use caracteres especiais como chaves ou aspas de código.]

        """

        response = gemini_safe_call(prompt, generation_config=genai.types.GenerationConfig(temperature=0.4)) # Aumentei a temperatura para ela ser mais criativa

        texto_raw = response.text

        # --- LIMPEZA DE CÓDIGO (VACINA ANTI-JSON) ---

        for lixo in ['```json', '```', '{', '}', '"PROB"', '"VEREDICTO"', '"ANÁLISE"']:

            texto_raw = texto_raw.replace(lixo, "")

        # 1. Extração da Probabilidade

        prob_val = 0

        prob_str = "N/A"

        match = re.search(r'(?:PROB|Probabilidade)[:\s]*(\d+)', texto_raw, re.IGNORECASE)

        if match:

            prob_val = int(match.group(1))

        else:

            match_b = re.search(r'(\d{2})%', texto_raw)

            if match_b: prob_val = int(match_b.group(1))

        if prob_val > 0: prob_str = f"{prob_val}%"

        # 2. Extração do Veredicto

        veredicto = "Neutro"

        texto_lower = texto_raw.lower()

        if "aprovado" in texto_lower and "reprovado" not in texto_lower: veredicto = "Aprovado"

        elif "arriscado" in texto_lower: veredicto = "Arriscado"

        elif "reprovado" in texto_lower: veredicto = "Reprovado"

        # 3. EXTRAÇÃO DO TEXTO RICO (O SEGREDO)

        # Vamos pegar tudo que vem depois de "ANÁLISE:"

        motivo = "Análise técnica indisponível."

        # Tenta achar a tag explícita primeiro

        partes = re.split(r'ANÁLISE:|Análise:|MOTIVO:|Motivo:', texto_raw, flags=re.IGNORECASE)

        if len(partes) > 1:

            motivo = partes[-1].strip() # Pega a última parte (o texto livre)

        else:

            # Se não achar a tag, tenta limpar as linhas técnicas e pega o resto

            linhas = texto_raw.split('\n')

            linhas_texto = []

            for l in linhas:

                l_up = l.upper()

                if "VEREDICTO" not in l_up and "PROB" not in l_up and len(l) > 10:

                    linhas_texto.append(l.strip())

            if linhas_texto:

                motivo = " ".join(linhas_texto)

        # Limpeza final de pontuação estranha no início da frase

        motivo = motivo.lstrip(' :-,."\'')

        # 4. [DATA-DRIVEN] Veredicto vem dos DADOS REAIS, não do Gemini
        try:
            odd_ctx = 0
            try:
                odd_m = re.search(r'@(\d+\.?\d*)', str(extra_context))
                if odd_m: odd_ctx = float(odd_m.group(1))
            except: pass
            veredicto_data, prob_data, fontes_data = calcular_veredicto_datadriven(
                estrategia, liga_nome, odd_ctx,
                stats_live=stats_raw,
                momentum_h=rh, momentum_a=ra,
                bigdata_txt=str(extra_context)
            )
            if fontes_data not in ("Sem dados", "Poucos dados"):
                veredicto = veredicto_data
                prob_str = f"{prob_data}%"
                motivo = f"{motivo}\n📊 <i>Base: {fontes_data}</i>"
            else:
                if veredicto == "Aprovado" and prob_val < 60: veredicto = "Arriscado"
                if prob_str == "N/A" and veredicto == "Aprovado": veredicto = "Arriscado"
        except:
            if veredicto == "Aprovado" and prob_val < 60: veredicto = "Arriscado"

        emoji = "✅" if veredicto == "Aprovado" else "⚠️"

        return f"\n🤖 <b>ANÁLISE TÉCNICA:</b>\n{emoji} <b>{veredicto.upper()} ({prob_str})</b>\n📝 <i>{motivo}</i>", prob_str
    except: return "", "N/A"

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

# ==============================================================================
# MÓDULOS IA AVANÇADOS (9 Módulos - Injetados do V17)
# ==============================================================================

def ia_memory_analise(estrategia, liga, time_casa, time_fora):
    """
    Analisa histórico pessoal do usuário para esta estratégia/liga/time.
    Retorna confiança ajustada baseada em performance real.
    """
    try:
        df = st.session_state.get('historico_full', pd.DataFrame())
        if df.empty:
            return {
                'tem_historico': False,
                'winrate': 0,
                'total_apostas': 0,
                'confianca': 'neutro',
                'mensagem': 'Sem histórico pessoal ainda.'
            }

        # Filtro 1: Estratégia específica
        f_estrategia = df[df['Estrategia'] == estrategia]

        # Filtro 2: Liga específica
        f_liga = df[df['Liga'] == liga]

        # Filtro 3: Times específicos
        f_times = df[
            (df['Jogo'].str.contains(time_casa, na=False, case=False)) |
            (df['Jogo'].str.contains(time_fora, na=False, case=False))
        ]

        # Prioridade: Estratégia + Liga > Estratégia > Liga
        if len(f_estrategia[f_estrategia['Liga'] == liga]) >= 3:
            amostra = f_estrategia[f_estrategia['Liga'] == liga]
            contexto = f"{estrategia} + {liga}"
        elif len(f_estrategia) >= 5:
            amostra = f_estrategia
            contexto = f"{estrategia} (geral)"
        elif len(f_liga) >= 5:
            amostra = f_liga
            contexto = f"{liga} (geral)"
        elif len(f_times) >= 3:
            amostra = f_times
            contexto = f"Times ({time_casa}/{time_fora})"
        else:
            return {
                'tem_historico': False,
                'winrate': 0,
                'total_apostas': 0,
                'confianca': 'neutro',
                'mensagem': f'Pouco histórico ({len(df)} apostas totais).'
            }

        # Calcula winrate
        finalizados = amostra[amostra['Resultado'].isin(['✅ GREEN', '❌ RED'])]
        if len(finalizados) == 0:
            return {
                'tem_historico': False,
                'winrate': 0,
                'total_apostas': len(amostra),
                'confianca': 'neutro',
                'mensagem': 'Sinais ainda pendentes.'
            }

        greens = len(finalizados[finalizados['Resultado'].str.contains('GREEN', na=False)])
        total = len(finalizados)
        winrate = (greens / total) * 100

        # Classifica confiança
        if winrate >= 75:
            confianca = 'muito_alta'
            emoji = '🔥'
        elif winrate >= 65:
            confianca = 'alta'
            emoji = '💎'
        elif winrate >= 55:
            confianca = 'media'
            emoji = '⚖️'
        elif winrate >= 45:
            confianca = 'baixa'
            emoji = '⚠️'
        else:
            confianca = 'muito_baixa'
            emoji = '🚨'

        mensagem = f"{emoji} {winrate:.0f}% em {contexto} ({greens}G/{total-greens}R)"

        return {
            'tem_historico': True,
            'winrate': winrate,
            'total_apostas': total,
            'confianca': confianca,
            'mensagem': mensagem,
            'contexto': contexto,
            'greens': greens,
            'reds': total - greens
        }

    except Exception as e:
        return {
            'tem_historico': False,
            'winrate': 0,
            'total_apostas': 0,
            'confianca': 'neutro',
            'mensagem': f'Erro: {str(e)}'
        }

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 2: 💰 IA STAKE MANAGER - GESTÃO DINÂMICA (KELLY CRITERION)
# ═══════════════════════════════════════════════════════════════════════════

def ia_stake_manager(probabilidade_ia, odd_atual, winrate_historico=None):
    """
    Calcula stake ideal usando Kelly Criterion modificado.
    Considera probabilidade IA + histórico pessoal + odd.
    """
    try:
        banca_atual = float(st.session_state.get('banca_atual',
                           st.session_state.get('banca_inicial', 1000.0)))

        # Ajusta probabilidade se tiver histórico
        prob_final = probabilidade_ia / 100
        if winrate_historico and winrate_historico > 0:
            # Média ponderada: 60% histórico, 40% IA
            prob_final = (winrate_historico/100 * 0.6) + (probabilidade_ia/100 * 0.4)

        # Kelly Criterion: f = (bp - q) / b
        # f = fração da banca
        # b = odd - 1 (decimal)
        # p = probabilidade de win
        # q = probabilidade de loss (1-p)

        b = odd_atual - 1
        p = prob_final
        q = 1 - p

        kelly = (b * p - q) / b

        # Limites de segurança
        kelly = max(0, kelly)  # Nunca negativo
        kelly = min(kelly, 0.15)  # Máximo 15% da banca

        # Fracional Kelly (mais conservador)
        kelly_fracional = kelly * 0.5  # 50% do Kelly

        # 3 Níveis de gestão
        conservador_pct = max(2, kelly_fracional * 100)  # Min 2%
        moderado_pct = max(5, kelly * 100 * 0.75)  # Min 5%
        agressivo_pct = max(8, kelly * 100)  # Min 8%

        # Limita máximos
        conservador_pct = min(conservador_pct, 5)
        moderado_pct = min(moderado_pct, 10)
        agressivo_pct = min(agressivo_pct, 15)

        return {
            'conservador': {
                'porcentagem': conservador_pct,
                'valor': (conservador_pct / 100) * banca_atual
            },
            'moderado': {
                'porcentagem': moderado_pct,
                'valor': (moderado_pct / 100) * banca_atual
            },
            'agressivo': {
                'porcentagem': agressivo_pct,
                'valor': (agressivo_pct / 100) * banca_atual
            },
            'kelly_puro': kelly * 100,
            'recomendado': 'moderado'  # Padrão
        }

    except Exception as e:
        # Fallback seguro
        banca = st.session_state.get('banca_inicial', 1000.0)
        return {
            'conservador': {'porcentagem': 2, 'valor': banca * 0.02},
            'moderado': {'porcentagem': 5, 'valor': banca * 0.05},
            'agressivo': {'porcentagem': 10, 'valor': banca * 0.10},
            'kelly_puro': 0,
            'recomendado': 'moderado'
        }

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 3: 📈 IA ODD TRACKER - TIMING DE ENTRADA
# ═══════════════════════════════════════════════════════════════════════════

def ia_odd_tracker_analise(odd_atual, estrategia, tempo_jogo):
    """
    Analisa se a odd atual é boa ou se deve aguardar valorização.
    Usa histórico de movimentação + padrões.
    """
    try:
        # Odds mínimas aceitáveis por estratégia
        odds_minimas = {
            "Golden Bet": 1.70,
            "Sniper Final": 1.80,
            "Janela de Ouro": 1.65,
            "Blitz Casa": 1.50,
            "Blitz Visitante": 1.50,
            "Porteira Aberta": 1.55,
            "Tiroteio Elite": 1.45,
            "Gol Relâmpago": 1.35,
            "Lay Goleada": 1.60
        }

        odd_minima = odds_minimas.get(estrategia, 1.50)

        # Análise de timing
        if odd_atual >= (odd_minima * 1.15):  # 15% acima do mínimo
            status = "excelente"
            acao = "ENTRE AGORA"
            emoji = "💎"
            mensagem = f"Odd ótima! {odd_atual:.2f} está acima da média histórica."
        elif odd_atual >= odd_minima:
            status = "boa"
            acao = "PODE ENTRAR"
            emoji = "✅"
            mensagem = f"Odd aceitável (@{odd_atual:.2f})."
        elif odd_atual >= (odd_minima * 0.90):  # Até 10% abaixo
            status = "aguardar"
            acao = "AGUARDE 2-3 MIN"
            emoji = "⏰"
            mensagem = f"Odd baixa (@{odd_atual:.2f}). Pode valorizar."
        else:
            status = "ruim"
            acao = "NÃO RECOMENDADO"
            emoji = "⛔"
            mensagem = f"Odd muito baixa (@{odd_atual:.2f})."

        # Projeção de valorização (baseado em tempo de jogo)
        if tempo_jogo and tempo_jogo < 75 and status == "aguardar":
            projecao_odd = odd_atual + (0.10 * (75 - tempo_jogo) / 30)  # Aproximação
            tempo_espera = "2-4 minutos"
        else:
            projecao_odd = odd_atual
            tempo_espera = "0"

        return {
            'status': status,
            'acao': acao,
            'emoji': emoji,
            'mensagem': mensagem,
            'odd_atual': odd_atual,
            'odd_minima': odd_minima,
            'projecao': projecao_odd,
            'tempo_espera': tempo_espera
        }

    except:
        return {
            'status': 'desconhecido',
            'acao': 'ANALISE MANUAL',
            'emoji': '❓',
            'mensagem': 'Sem dados suficientes.',
            'odd_atual': odd_atual,
            'odd_minima': 1.50,
            'projecao': odd_atual,
            'tempo_espera': '0'
        }

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 4: 🚪 IA EXIT STRATEGY - CASHOUT INTELIGENTE
# ═══════════════════════════════════════════════════════════════════════════

def ia_exit_strategy(tempo_atual, placar_atual, odd_entrada, estrategia):
    """
    Sugere quando fazer cashout ou segurar até o final.
    Analisa risco vs recompensa baseado no tempo restante.
    """
    try:
        tempo_restante = 90 - tempo_atual

        # Estratégias que pedem cashout precoce (alta volatilidade)
        estrategias_volateis = [
            "Golden Bet", "Sniper Final", "Lay Goleada",
            "Porteira Aberta", "Blitz Casa", "Blitz Visitante"
        ]

        # Estratégias para segurar (baixa volatilidade)
        estrategias_seguras = [
            "Jogo Morno", "Estratégia do Vovô", "Arame Liso"
        ]

        # Análise de risco
        if tempo_restante <= 5:
            # Últimos 5 minutos
            recomendacao = "segurar"
            emoji = "🔒"
            mensagem = "Faltam poucos minutos. Segure!"
            cashout_pct = 0

        elif tempo_restante <= 15 and estrategia in estrategias_seguras:
            # Estratégias seguras: segurar
            recomendacao = "segurar"
            emoji = "🔒"
            mensagem = "Estratégia segura. Aguarde o final."
            cashout_pct = 0

        elif tempo_restante >= 20 and estrategia in estrategias_volateis:
            # Estratégias voláteis com muito tempo: cashout parcial
            recomendacao = "cashout_parcial"
            emoji = "🚪"
            mensagem = f"Ainda {tempo_restante} min. Jogo pode virar."
            cashout_pct = 60  # Segura 60%, deixa 40% correr

        elif tempo_restante <= 10:
            # Reta final: decisão por placar
            if "x" in placar_atual:
                try:
                    gh, ga = map(int, placar_atual.split('x'))
                    diferenca = abs(gh - ga)

                    if diferenca >= 2:
                        # Placar tranquilo
                        recomendacao = "segurar"
                        emoji = "🔒"
                        mensagem = "Placar seguro. Mantenha!"
                        cashout_pct = 0
                    else:
                        # Placar apertado
                        recomendacao = "cashout_parcial"
                        emoji = "⚠️"
                        mensagem = "Placar apertado. Proteja lucro."
                        cashout_pct = 70
                except:
                    recomendacao = "segurar"
                    emoji = "🔒"
                    mensagem = "Aguarde o final."
                    cashout_pct = 0
            else:
                recomendacao = "segurar"
                emoji = "🔒"
                mensagem = "Aguarde o final."
                cashout_pct = 0
        else:
            # Caso padrão: segurar
            recomendacao = "segurar"
            emoji = "🔒"
            mensagem = "Mantenha a posição."
            cashout_pct = 0

        return {
            'recomendacao': recomendacao,
            'emoji': emoji,
            'mensagem': mensagem,
            'cashout_pct': cashout_pct,
            'tempo_restante': tempo_restante
        }

    except:
        return {
            'recomendacao': 'segurar',
            'emoji': '🔒',
            'mensagem': 'Sem análise. Decisão manual.',
            'cashout_pct': 0,
            'tempo_restante': 0
        }

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 5: 🧬 IA MULTI-AGENTE - VOTAÇÃO DE ESPECIALISTAS
# ═══════════════════════════════════════════════════════════════════════════

def ia_multi_agente_votacao(
    probabilidade_ia,
    winrate_historico,
    stats_jogo,
    big_data_info,
    estrategia
):
    """
    3 agentes IA analisam o sinal e votam.
    Mostra consenso ou divergência para o usuário decidir.
    """
    try:
        votos = []

        # ─────────────────────────────────────────────────────────────────
        # AGENTE 1: 🛡️ CONSERVADOR (Foca em histórico pessoal)
        # ─────────────────────────────────────────────────────────────────
        if winrate_historico and winrate_historico >= 70:
            voto_conservador = "aprovar"
            conf_conservador = "alta"
            razao_conservador = f"Seu histórico: {winrate_historico:.0f}%"
        elif winrate_historico and winrate_historico >= 55:
            voto_conservador = "neutro"
            conf_conservador = "media"
            razao_conservador = f"Histórico OK: {winrate_historico:.0f}%"
        elif winrate_historico and winrate_historico < 55:
            voto_conservador = "rejeitar"
            conf_conservador = "baixa"
            razao_conservador = f"Histórico fraco: {winrate_historico:.0f}%"
        else:
            voto_conservador = "neutro"
            conf_conservador = "media"
            razao_conservador = "Sem histórico suficiente"

        votos.append({
            'agente': '🛡️ Conservador',
            'voto': voto_conservador,
            'confianca': conf_conservador,
            'razao': razao_conservador
        })

        # ─────────────────────────────────────────────────────────────────
        # AGENTE 2: ⚖️ MODERADO (Foca em stats do jogo atual)
        # ─────────────────────────────────────────────────────────────────
        if probabilidade_ia >= 75:
            voto_moderado = "aprovar"
            conf_moderado = "alta"
            razao_moderado = f"Stats fortes ({probabilidade_ia}%)"
        elif probabilidade_ia >= 60:
            voto_moderado = "aprovar"
            conf_moderado = "media"
            razao_moderado = f"Stats boas ({probabilidade_ia}%)"
        else:
            voto_moderado = "rejeitar"
            conf_moderado = "baixa"
            razao_moderado = f"Stats fracas ({probabilidade_ia}%)"

        votos.append({
            'agente': '⚖️ Moderado',
            'voto': voto_moderado,
            'confianca': conf_moderado,
            'razao': razao_moderado
        })

        # ─────────────────────────────────────────────────────────────────
        # AGENTE 3: 🚀 AGRESSIVO (Foca em Big Data macro)
        # ─────────────────────────────────────────────────────────────────
        # Analisa se o padrão do Big Data confirma
        if "8" in str(big_data_info) or "9" in str(big_data_info):
            # Média de gols alta no histórico
            voto_agressivo = "aprovar"
            conf_agressivo = "alta"
            razao_agressivo = "Padrão macro confirma"
        elif "MANDANTE" in str(big_data_info) and "Casa" in estrategia:
            voto_agressivo = "aprovar"
            conf_agressivo = "alta"
            razao_agressivo = "Big Data favorece casa"
        elif "VISITANTE" in str(big_data_info) and "Visitante" in estrategia:
            voto_agressivo = "aprovar"
            conf_agressivo = "alta"
            razao_agressivo = "Big Data favorece visitante"
        else:
            voto_agressivo = "neutro"
            conf_agressivo = "media"
            razao_agressivo = "Sem padrão macro claro"

        votos.append({
            'agente': '🚀 Agressivo',
            'voto': voto_agressivo,
            'confianca': conf_agressivo,
            'razao': razao_agressivo
        })

        # ─────────────────────────────────────────────────────────────────
        # ANÁLISE DE CONSENSO
        # ─────────────────────────────────────────────────────────────────
        aprovar_count = sum(1 for v in votos if v['voto'] == 'aprovar')
        rejeitar_count = sum(1 for v in votos if v['voto'] == 'rejeitar')
        neutro_count = sum(1 for v in votos if v['voto'] == 'neutro')

        if aprovar_count >= 2:
            consenso = "favoravel"
            emoji_consenso = "✅"
            mensagem_consenso = f"{aprovar_count}/3 Favorável"
        elif rejeitar_count >= 2:
            consenso = "desfavoravel"
            emoji_consenso = "⛔"
            mensagem_consenso = f"{rejeitar_count}/3 Contra"
        else:
            consenso = "dividido"
            emoji_consenso = "⚖️"
            mensagem_consenso = "Sem consenso (decisão sua)"

        return {
            'votos': votos,
            'consenso': consenso,
            'emoji': emoji_consenso,
            'mensagem': mensagem_consenso,
            'aprovar': aprovar_count,
            'rejeitar': rejeitar_count,
            'neutro': neutro_count
        }

    except Exception as e:
        return {
            'votos': [],
            'consenso': 'erro',
            'emoji': '❓',
            'mensagem': f'Erro: {str(e)}',
            'aprovar': 0,
            'rejeitar': 0,
            'neutro': 0
        }

# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÃO ORQUESTRADORA: 🎯 IA COMPLETA (CHAMA TODOS OS 5 MÓDULOS)
# ═══════════════════════════════════════════════════════════════════════════

def ia_analise_completa(
    estrategia,
    liga,
    time_casa,
    time_fora,
    placar,
    tempo_jogo,
    odd_atual,
    probabilidade_ia,
    stats_jogo,
    big_data_info
):
    """
    Orquestra os 5 módulos de IA e retorna análise completa.
    Esta função é chamada para CADA sinal (ao vivo ou pré-jogo).
    """
    try:
        # Módulo 1: Memory
        memory = ia_memory_analise(estrategia, liga, time_casa, time_fora)

        # Módulo 2: Stake Manager
        winrate_hist = memory['winrate'] if memory['tem_historico'] else None
        stake = ia_stake_manager(probabilidade_ia, odd_atual, winrate_hist)

        # Módulo 3: Odd Tracker
        odd_tracker = ia_odd_tracker_analise(odd_atual, estrategia, tempo_jogo)

        # Módulo 4: Exit Strategy (só para sinais ao vivo)
        if tempo_jogo:
            exit_strategy = ia_exit_strategy(tempo_jogo, placar, odd_atual, estrategia)
        else:
            exit_strategy = None

        # Módulo 5: Multi-Agente
        multi_agente = ia_multi_agente_votacao(
            probabilidade_ia,
            winrate_hist,
            stats_jogo,
            big_data_info,
            estrategia
        )

        return {
            'memory': memory,
            'stake': stake,
            'odd_tracker': odd_tracker,
            'exit_strategy': exit_strategy,
            'multi_agente': multi_agente
        }

    except Exception as e:
        # Fallback seguro
        return {
            'memory': {'tem_historico': False, 'mensagem': 'Erro'},
            'stake': {
                'conservador': {'porcentagem': 2, 'valor': 20},
                'moderado': {'porcentagem': 5, 'valor': 50},
                'agressivo': {'porcentagem': 10, 'valor': 100}
            },
            'odd_tracker': {'status': 'desconhecido', 'acao': 'ANALISE MANUAL'},
            'exit_strategy': None,
            'multi_agente': {'consenso': 'erro', 'mensagem': 'Erro análise'}
        }

# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÃO AUXILIAR: FORMATAR MENSAGEM TELEGRAM COM IA COMPLETA
# ═══════════════════════════════════════════════════════════════════════════

def formatar_mensagem_ia_completa(analise_ia, tipo_sinal="AO_VIVO"):
    """
    Formata a mensagem do Telegram com TODAS as análises da IA.

    Args:
        analise_ia: Resultado de ia_analise_completa()
        tipo_sinal: "AO_VIVO" ou "PRE_JOGO"

    Returns:
        String formatada para Telegram (HTML)
    """
    try:
        msg = "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "🧠 <b>ANÁLISE IA COMPLETA</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        # ─────────────────────────────────────────────────────────────────
        # 1. HISTÓRICO PESSOAL
        # ─────────────────────────────────────────────────────────────────
        memory = analise_ia['memory']
        msg += f"\n📚 <b>SEU HISTÓRICO:</b>\n"
        if memory['tem_historico']:
            msg += f"├─ {memory['mensagem']}\n"
            msg += f"└─ Confiança: <b>{memory['confianca'].upper().replace('_', ' ')}</b>\n"
        else:
            msg += f"└─ {memory['mensagem']}\n"

        # ─────────────────────────────────────────────────────────────────
        # 2. VOTAÇÃO DOS ESPECIALISTAS
        # ─────────────────────────────────────────────────────────────────
        multi = analise_ia['multi_agente']
        msg += f"\n🗳️ <b>VOTAÇÃO ESPECIALISTAS:</b>\n"

        for voto in multi['votos']:
            if voto['voto'] == 'aprovar':
                emoji_voto = "✅"
            elif voto['voto'] == 'rejeitar':
                emoji_voto = "⛔"
            else:
                emoji_voto = "⚖️"

            msg += f"├─ {voto['agente']}: {emoji_voto}\n"
            msg += f"│  └─ {voto['razao']}\n"

        msg += f"└─ <b>Consenso: {multi['emoji']} {multi['mensagem']}</b>\n"

        # ─────────────────────────────────────────────────────────────────
        # 3. TIMING DE ENTRADA (ODD TRACKER)
        # ─────────────────────────────────────────────────────────────────
        odd_t = analise_ia['odd_tracker']
        msg += f"\n📈 <b>TIMING DE ENTRADA:</b>\n"
        msg += f"├─ Odd Atual: @{odd_t['odd_atual']:.2f}\n"
        msg += f"├─ Status: {odd_t['emoji']} <b>{odd_t['status'].upper()}</b>\n"
        msg += f"└─ Ação: <b>{odd_t['acao']}</b>\n"

        if odd_t['status'] == 'aguardar':
            msg += f"   └─ Projeção: @{odd_t['projecao']:.2f} em {odd_t['tempo_espera']}\n"

        # ─────────────────────────────────────────────────────────────────
        # 4. GESTÃO DE STAKE (KELLY CRITERION)
        # ─────────────────────────────────────────────────────────────────
        stake = analise_ia['stake']
        msg += f"\n💰 <b>GESTÃO DE STAKE:</b>\n"
        msg += f"├─ 🛡️ Conservador: R$ {stake['conservador']['valor']:.2f} ({stake['conservador']['porcentagem']:.1f}%)\n"
        msg += f"├─ ⚖️ Moderado: R$ {stake['moderado']['valor']:.2f} ({stake['moderado']['porcentagem']:.1f}%) ⭐\n"
        msg += f"└─ 🚀 Agressivo: R$ {stake['agressivo']['valor']:.2f} ({stake['agressivo']['porcentagem']:.1f}%)\n"

        # ─────────────────────────────────────────────────────────────────
        # 5. ESTRATÉGIA DE SAÍDA (Só para sinais ao vivo)
        # ─────────────────────────────────────────────────────────────────
        if tipo_sinal == "AO_VIVO" and analise_ia['exit_strategy']:
            exit_s = analise_ia['exit_strategy']
            msg += f"\n🚪 <b>PLANO DE SAÍDA:</b>\n"
            msg += f"├─ {exit_s['emoji']} {exit_s['mensagem']}\n"

            if exit_s['cashout_pct'] > 0:
                msg += f"└─ <b>Cashout sugerido: {exit_s['cashout_pct']}%</b>\n"
                msg += f"   └─ (Segura {exit_s['cashout_pct']}%, deixa {100-exit_s['cashout_pct']}% correr)\n"
            else:
                msg += f"└─ <b>Segurar até o final</b>\n"

        msg += "\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        return msg

    except Exception as e:
        return f"\n\n⚠️ Erro na análise IA: {str(e)}\n"

# ═══════════════════════════════════════════════════════════════════════════
# 🎯 4 NOVOS MÓDULOS DE IA ESPECÍFICOS PARA PRÉ-JOGO
# ═══════════════════════════════════════════════════════════════════════════
#
# INSERIR após os 5 módulos existentes (depois da linha ~2810)
#
# CONFIGURAÇÃO HÍBRIDA:
# - PRÉ-JOGO: Memory, Stake, Multi-Agente + 4 NOVOS
# - AO VIVO: Memory, Stake, Odd Tracker, Exit Strategy, Multi-Agente
#
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 6: 🎯 IA ODDS MOVEMENT - Movimento de Linha (24h)
# ═══════════════════════════════════════════════════════════════════════════

def ia_odds_movement_analise(fid_jogo, odd_atual, estrategia):
    """
    Monitora movimento da odd nas últimas 24h.
    Detecta sharp money vs public money.
    Identifica se você está early ou late.
    """
    try:
        # Tenta buscar histórico de odd do cache/Firebase
        if 'odds_history' not in st.session_state:
            st.session_state['odds_history'] = {}

        history = st.session_state['odds_history'].get(str(fid_jogo), [])

        if len(history) >= 2:
            # Tem histórico
            odd_abertura = history[0]['odd']
            odd_atual_hist = history[-1]['odd']
            tempo_decorrido = len(history)  # Aproximação em horas

            # Calcula variação
            variacao_pct = ((odd_atual - odd_abertura) / odd_abertura) * 100

            # Classifica movimento
            if abs(variacao_pct) < 3:
                movimento = "ESTÁVEL"
                tendencia = "⚖️"
            elif variacao_pct <= -10:
                movimento = "CAINDO FORTE"
                tendencia = "📉"
            elif variacao_pct <= -5:
                movimento = "CAINDO"
                tendencia = "📉"
            elif variacao_pct >= 10:
                movimento = "SUBINDO FORTE"
                tendencia = "📈"
            elif variacao_pct >= 5:
                movimento = "SUBINDO"
                tendencia = "📈"
            else:
                movimento = "ESTÁVEL"
                tendencia = "⚖️"

            # Detecta tipo de dinheiro
            if variacao_pct <= -10:
                tipo_dinheiro = "🦈 SHARP MONEY (Profissionais)"
                interpretacao = "Você está LATE. Odd já caiu muito."
            elif variacao_pct >= 10:
                tipo_dinheiro = "📢 PUBLIC MONEY (Recreativos)"
                interpretacao = "Odd inflada. Oportunidade de VALUE!"
            else:
                tipo_dinheiro = "⚖️ BALANCEADO"
                interpretacao = "Mercado equilibrado."

            # Recomendação
            if variacao_pct <= -15:
                recomendacao = "EVITAR (odd caiu demais)"
                emoji_rec = "⛔"
            elif variacao_pct >= 15:
                recomendacao = "EXCELENTE (value bet)"
                emoji_rec = "💎"
            elif abs(variacao_pct) < 5:
                recomendacao = "BOA (odd estável)"
                emoji_rec = "✅"
            else:
                recomendacao = "OK (movimento moderado)"
                emoji_rec = "⚖️"

            return {
                'tem_dados': True,
                'odd_abertura': odd_abertura,
                'odd_atual': odd_atual,
                'variacao_pct': variacao_pct,
                'movimento': movimento,
                'tendencia': tendencia,
                'tipo_dinheiro': tipo_dinheiro,
                'interpretacao': interpretacao,
                'recomendacao': recomendacao,
                'emoji': emoji_rec,
                'tempo_monitorado': tempo_decorrido
            }
        else:
            # Sem histórico suficiente
            return {
                'tem_dados': False,
                'odd_atual': odd_atual,
                'mensagem': 'Sem histórico de 24h (jogo muito recente)'
            }

    except Exception as e:
        return {
            'tem_dados': False,
            'odd_atual': odd_atual,
            'mensagem': f'Erro: {str(e)}'
        }

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 7: 📊 IA H2H ANALYZER - Análise de Confronto Direto
# ═══════════════════════════════════════════════════════════════════════════

def ia_h2h_analise(time_casa, time_fora, home_id, away_id, api_key):
    """
    Análise profunda dos últimos confrontos diretos.
    Identifica padrões específicos deste duelo.
    """
    try:
        # Busca H2H da API
        url = f"https://v3.football.api-sports.io/fixtures/headtohead"
        params = {"h2h": f"{home_id}-{away_id}", "last": 10}
        headers = {"x-apisports-key": api_key}

        response = requests.get(url, headers=headers, params=params, timeout=5)

        if response.status_code != 200:
            return {
                'tem_dados': False,
                'mensagem': 'API indisponível'
            }

        data = response.json()
        jogos_h2h = data.get('response', [])

        if len(jogos_h2h) < 3:
            return {
                'tem_dados': False,
                'mensagem': 'Poucos jogos H2H (mín. 3)'
            }

        # Analisa jogos
        vitorias_casa = 0
        vitorias_fora = 0
        empates = 0
        over_25 = 0
        btts = 0
        total_gols = 0

        for jogo in jogos_h2h:
            goals_home = jogo['goals']['home']
            goals_away = jogo['goals']['away']

            if goals_home is None or goals_away is None:
                continue

            total_gols += (goals_home + goals_away)

            # Vitórias (considerando mando)
            fixture_home_id = jogo['teams']['home']['id']
            if fixture_home_id == home_id:
                # Jogo em casa
                if goals_home > goals_away:
                    vitorias_casa += 1
                elif goals_away > goals_home:
                    vitorias_fora += 1
                else:
                    empates += 1
            else:
                # Jogo invertido (casa era visitante)
                if goals_away > goals_home:
                    vitorias_casa += 1
                elif goals_home > goals_away:
                    vitorias_fora += 1
                else:
                    empates += 1

            # Over 2.5
            if (goals_home + goals_away) > 2:
                over_25 += 1

            # BTTS
            if goals_home > 0 and goals_away > 0:
                btts += 1

        total_jogos = len(jogos_h2h)
        media_gols = total_gols / total_jogos if total_jogos > 0 else 0

        # Percentuais
        pct_vit_casa = (vitorias_casa / total_jogos) * 100
        pct_over = (over_25 / total_jogos) * 100
        pct_btts = (btts / total_jogos) * 100

        # Identifica padrões
        padroes = []
        if pct_btts >= 75:
            padroes.append("💎 BTTS fortíssimo (75%+)")
        if pct_over >= 70:
            padroes.append("🔥 Over 2.5 consistente (70%+)")
        if media_gols >= 3.5:
            padroes.append("⚡ Jogos sempre movimentados (3.5+ gols)")
        if pct_vit_casa >= 70:
            padroes.append("🏠 Casa domina este confronto (70%+)")

        if not padroes:
            padroes.append("⚖️ Sem padrões claros (histórico irregular)")

        # Recomendações baseadas em H2H
        recomendacoes = []
        if pct_btts >= 75:
            recomendacoes.append("BTTS (Sim) - 75%+ histórico")
        if pct_over >= 70:
            recomendacoes.append("Over 2.5 - 70%+ histórico")
        if pct_vit_casa >= 60:
            recomendacoes.append(f"Casa (1X2) - {pct_vit_casa:.0f}% histórico")

        return {
            'tem_dados': True,
            'total_jogos': total_jogos,
            'vitorias_casa': vitorias_casa,
            'vitorias_fora': vitorias_fora,
            'empates': empates,
            'pct_vit_casa': pct_vit_casa,
            'over_25': over_25,
            'pct_over': pct_over,
            'btts': btts,
            'pct_btts': pct_btts,
            'media_gols': media_gols,
            'padroes': padroes,
            'recomendacoes': recomendacoes
        }

    except Exception as e:
        return {
            'tem_dados': False,
            'mensagem': f'Erro API: {str(e)}'
        }

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 8: 🌡️ IA MOMENTUM DETECTOR - Forma Recente dos Times
# ═══════════════════════════════════════════════════════════════════════════

def ia_momentum_analise(time_casa, time_fora, home_id, away_id, api_key):
    """
    Analisa últimos 5 jogos de cada time.
    Detecta momentum (melhorando, estável, caindo).
    Identifica tendências recentes.
    """
    try:
        # Função auxiliar para analisar sequência
        def analisar_sequencia(team_id, location='all'):
            url = f"https://v3.football.api-sports.io/fixtures"
            params = {
                "team": team_id,
                "last": 5 if location == 'all' else 5,
                "status": "FT"
            }

            if location == 'home':
                params['venue'] = 'home'
            elif location == 'away':
                params['venue'] = 'away'

            headers = {"x-apisports-key": api_key}

            response = requests.get(url, headers=headers, params=params, timeout=5)

            if response.status_code != 200:
                return None

            data = response.json()
            jogos = data.get('response', [])

            if len(jogos) < 3:
                return None

            vitorias = 0
            empates = 0
            derrotas = 0
            gols_marcados = 0
            gols_sofridos = 0

            for jogo in jogos[:5]:
                home_id_fixture = jogo['teams']['home']['id']
                goals_home = jogo['goals']['home']
                goals_away = jogo['goals']['away']

                if goals_home is None or goals_away is None:
                    continue

                # Determina se o time jogou em casa ou fora
                if home_id_fixture == team_id:
                    # Jogou em casa
                    gols_marcados += goals_home
                    gols_sofridos += goals_away

                    if goals_home > goals_away:
                        vitorias += 1
                    elif goals_home == goals_away:
                        empates += 1
                    else:
                        derrotas += 1
                else:
                    # Jogou fora
                    gols_marcados += goals_away
                    gols_sofridos += goals_home

                    if goals_away > goals_home:
                        vitorias += 1
                    elif goals_away == goals_home:
                        empates += 1
                    else:
                        derrotas += 1

            total = vitorias + empates + derrotas
            if total == 0:
                return None

            media_gols_marcados = gols_marcados / total
            media_gols_sofridos = gols_sofridos / total

            # Classifica momentum
            pontos = (vitorias * 3) + (empates * 1)
            pct_pontos = (pontos / (total * 3)) * 100

            if pct_pontos >= 70:
                momentum = "🚀 CRESCENTE"
            elif pct_pontos >= 50:
                momentum = "⚖️ ESTÁVEL"
            else:
                momentum = "📉 DECRESCENTE"

            return {
                'vitorias': vitorias,
                'empates': empates,
                'derrotas': derrotas,
                'sequencia': f"{vitorias}V {empates}E {derrotas}D",
                'gols_marcados': gols_marcados,
                'gols_sofridos': gols_sofridos,
                'media_gols_marcados': media_gols_marcados,
                'media_gols_sofridos': media_gols_sofridos,
                'momentum': momentum,
                'pct_pontos': pct_pontos
            }

        # Analisa casa (em casa)
        forma_casa = analisar_sequencia(home_id, 'home')

        # Analisa fora (fora)
        forma_fora = analisar_sequencia(away_id, 'away')

        if not forma_casa or not forma_fora:
            return {
                'tem_dados': False,
                'mensagem': 'Dados insuficientes de forma'
            }

        # Compara momentum
        diff_momentum = forma_casa['pct_pontos'] - forma_fora['pct_pontos']

        if diff_momentum >= 30:
            comparacao = "🔥 Casa MUITO superior"
        elif diff_momentum >= 15:
            comparacao = "✅ Casa superior"
        elif diff_momentum <= -30:
            comparacao = "⚡ Fora MUITO superior"
        elif diff_momentum <= -15:
            comparacao = "⚠️ Fora superior"
        else:
            comparacao = "⚖️ Equilíbrio"

        # Impacto na aposta
        if abs(diff_momentum) >= 30:
            impacto = "ALTO - Diferença extrema de forma"
        elif abs(diff_momentum) >= 15:
            impacto = "MÉDIO - Diferença significativa"
        else:
            impacto = "BAIXO - Formas similares"

        return {
            'tem_dados': True,
            'casa': forma_casa,
            'fora': forma_fora,
            'comparacao': comparacao,
            'diff_momentum': diff_momentum,
            'impacto': impacto
        }

    except Exception as e:
        return {
            'tem_dados': False,
            'mensagem': f'Erro: {str(e)}'
        }

# ═══════════════════════════════════════════════════════════════════════════
# MÓDULO 9: 🔮 IA SMART ENTRY - Momento Ideal de Entrada
# ═══════════════════════════════════════════════════════════════════════════

def ia_smart_entry_decisao(
    odds_movement,
    h2h,
    momentum,
    memory,
    stake,
    multi_agente,
    estrategia
):
    """
    Combina TODOS os dados e calcula o momento ideal de entrada.
    Retorna: ENTRAR AGORA / ESPERAR 2-4H / ESPERAR LIVE / NÃO ENTRAR
    """
    try:
        opcoes = []

        # ─────────────────────────────────────────────────────────────────
        # OPÇÃO 1: ENTRAR AGORA
        # ─────────────────────────────────────────────────────────────────

        pontos_agora = 0
        razoes_agora = []

        # Consenso favorável?
        if multi_agente['consenso'] == 'favoravel':
            pontos_agora += 30
            razoes_agora.append("✅ Consenso 3/3 favorável")
        elif multi_agente['consenso'] == 'dividido':
            pontos_agora += 15
            razoes_agora.append("⚖️ Consenso 2/3")

        # H2H forte?
        if h2h['tem_dados'] and len(h2h.get('padroes', [])) > 0:
            if "💎" in str(h2h['padroes']) or "🔥" in str(h2h['padroes']):
                pontos_agora += 25
                razoes_agora.append("💎 Padrão H2H fortíssimo")

        # Momentum favorável?
        if momentum['tem_dados']:
            if "superior" in momentum.get('comparacao', '').lower():
                pontos_agora += 20
                razoes_agora.append("🚀 Momentum favorável")

        # Histórico pessoal bom?
        if memory['tem_historico'] and memory['winrate'] >= 65:
            pontos_agora += 15
            razoes_agora.append(f"📚 Seu histórico: {memory['winrate']:.0f}%")

        # Odd movement OK?
        if odds_movement['tem_dados']:
            if odds_movement.get('variacao_pct', 0) >= -5:
                pontos_agora += 10
                razoes_agora.append("📈 Odd ainda boa")

        opcoes.append({
            'opcao': 'ENTRAR AGORA',
            'pontos': pontos_agora,
            'emoji': '⭐',
            'razoes': razoes_agora,
            'stake_sugerido': stake['moderado']['porcentagem'],
            'stake_valor': stake['moderado']['valor']
        })

        # ─────────────────────────────────────────────────────────────────
        # OPÇÃO 2: ESPERAR LIVE
        # ─────────────────────────────────────────────────────────────────

        pontos_live = 0
        razoes_live = []

        # Odd caiu muito?
        if odds_movement['tem_dados']:
            if odds_movement.get('variacao_pct', 0) <= -10:
                pontos_live += 30
                razoes_live.append("📉 Odd caiu muito (-10%+)")

        # Consenso dividido?
        if multi_agente['consenso'] == 'dividido':
            pontos_live += 20
            razoes_live.append("⚖️ Consenso dividido (aguardar confirmação)")

        # Momentum equilibrado?
        if momentum['tem_dados']:
            if "Equilíbrio" in momentum.get('comparacao', ''):
                pontos_live += 15
                razoes_live.append("⚖️ Jogo pode ir para qualquer lado")

        opcoes.append({
            'opcao': 'ESPERAR LIVE',
            'pontos': pontos_live,
            'emoji': '⏰',
            'razoes': razoes_live,
            'aguardar': '15-20 min de jogo',
            'objetivo': 'Odd melhor no live'
        })

        # ─────────────────────────────────────────────────────────────────
        # OPÇÃO 3: NÃO ENTRAR
        # ─────────────────────────────────────────────────────────────────

        pontos_nao = 0
        razoes_nao = []

        # Consenso desfavorável?
        if multi_agente['consenso'] == 'desfavoravel':
            pontos_nao += 40
            razoes_nao.append("⛔ Consenso contra (0/3 ou 1/3)")

        # Odd caiu MUITO?
        if odds_movement['tem_dados']:
            if odds_movement.get('variacao_pct', 0) <= -15:
                pontos_nao += 30
                razoes_nao.append("⛔ Odd caiu demais (-15%+)")

        # Histórico pessoal ruim?
        if memory['tem_historico'] and memory['winrate'] < 45:
            pontos_nao += 20
            razoes_nao.append(f"⚠️ Histórico fraco: {memory['winrate']:.0f}%")

        # Momentum contra?
        if momentum['tem_dados']:
            if "Fora MUITO superior" in momentum.get('comparacao', ''):
                pontos_nao += 15
                razoes_nao.append("📉 Momentum desfavorável")

        opcoes.append({
            'opcao': 'NÃO ENTRAR',
            'pontos': pontos_nao,
            'emoji': '⛔',
            'razoes': razoes_nao,
            'alternativa': 'Esperar próximo jogo'
        })

        # ─────────────────────────────────────────────────────────────────
        # DECISÃO FINAL
        # ─────────────────────────────────────────────────────────────────

        # Ordena por pontos
        opcoes_sorted = sorted(opcoes, key=lambda x: x['pontos'], reverse=True)

        # Escolhe a melhor
        melhor_opcao = opcoes_sorted[0]

        # Se empate, prioriza ENTRAR AGORA
        if len(opcoes_sorted) >= 2 and opcoes_sorted[0]['pontos'] == opcoes_sorted[1]['pontos']:
            for op in opcoes_sorted:
                if op['opcao'] == 'ENTRAR AGORA':
                    melhor_opcao = op
                    break

        return {
            'decisao': melhor_opcao['opcao'],
            'confianca': melhor_opcao['pontos'],
            'todas_opcoes': opcoes_sorted,
            'recomendacao': melhor_opcao
        }

    except Exception as e:
        return {
            'decisao': 'ERRO',
            'confianca': 0,
            'mensagem': f'Erro: {str(e)}'
        }

# ═══════════════════════════════════════════════════════════════════════════
# 🎯 ORQUESTRADOR E FORMATADOR - PRÉ-JOGO (7 MÓDULOS)
# ═══════════════════════════════════════════════════════════════════════════
#
# INSERIR após os 4 novos módulos de pré-jogo
#
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÃO ORQUESTRADORA: 🎯 IA COMPLETA PRÉ-JOGO (7 MÓDULOS)
# ═══════════════════════════════════════════════════════════════════════════

def ia_analise_completa_pre_jogo(
    estrategia,
    liga,
    time_casa,
    time_fora,
    home_id,
    away_id,
    fid_jogo,
    odd_atual,
    probabilidade_ia,
    api_key
):
    """
    Orquestra os 7 módulos de IA para PRÉ-JOGO.

    MÓDULOS:
    1. Memory (histórico pessoal)
    2. Stake Manager (gestão Kelly)
    3. Multi-Agente (3 especialistas)
    4. Odds Movement (movimento 24h) - NOVO
    5. H2H Analyzer (confronto direto) - NOVO
    6. Momentum Detector (forma dos times) - NOVO
    7. Smart Entry (momento ideal) - NOVO
    """
    try:
        # ─────────────────────────────────────────────────────────────────
        # MÓDULOS MANTIDOS (funcionam bem no pré-jogo)
        # ─────────────────────────────────────────────────────────────────

        # 1. Memory
        memory = ia_memory_analise(estrategia, liga, time_casa, time_fora)

        # 2. Stake Manager
        winrate_hist = memory['winrate'] if memory['tem_historico'] else None
        stake = ia_stake_manager(probabilidade_ia, odd_atual, winrate_hist)

        # 3. Multi-Agente
        multi_agente = ia_multi_agente_votacao(
            probabilidade_ia,
            winrate_hist,
            "Análise Pré-Jogo",
            "",
            estrategia
        )

        # ─────────────────────────────────────────────────────────────────
        # MÓDULOS NOVOS (específicos pré-jogo)
        # ─────────────────────────────────────────────────────────────────

        # 4. Odds Movement
        odds_movement = ia_odds_movement_analise(fid_jogo, odd_atual, estrategia)

        # 5. H2H Analyzer
        h2h = ia_h2h_analise(time_casa, time_fora, home_id, away_id, api_key)

        # 6. Momentum Detector
        momentum = ia_momentum_analise(time_casa, time_fora, home_id, away_id, api_key)

        # 7. Smart Entry (combina tudo)
        smart_entry = ia_smart_entry_decisao(
            odds_movement,
            h2h,
            momentum,
            memory,
            stake,
            multi_agente,
            estrategia
        )

        return {
            'memory': memory,
            'stake': stake,
            'multi_agente': multi_agente,
            'odds_movement': odds_movement,
            'h2h': h2h,
            'momentum': momentum,
            'smart_entry': smart_entry
        }

    except Exception as e:
        # Fallback seguro
        return {
            'memory': {'tem_historico': False, 'mensagem': 'Erro'},
            'stake': {
                'conservador': {'porcentagem': 2, 'valor': 20},
                'moderado': {'porcentagem': 5, 'valor': 50},
                'agressivo': {'porcentagem': 10, 'valor': 100}
            },
            'multi_agente': {'consenso': 'erro', 'mensagem': 'Erro'},
            'odds_movement': {'tem_dados': False, 'mensagem': 'Erro'},
            'h2h': {'tem_dados': False, 'mensagem': 'Erro'},
            'momentum': {'tem_dados': False, 'mensagem': 'Erro'},
            'smart_entry': {'decisao': 'ERRO', 'confianca': 0}
        }

# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÃO FORMATADOR: 📝 MENSAGEM TELEGRAM PRÉ-JOGO (7 MÓDULOS)
# ═══════════════════════════════════════════════════════════════════════════

def formatar_mensagem_ia_pre_jogo(analise_ia):
    """
    Formata a mensagem do Telegram com os 7 módulos de pré-jogo.

    Args:
        analise_ia: Resultado de ia_analise_completa_pre_jogo()

    Returns:
        String formatada para Telegram (HTML)
    """
    try:
        msg = "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "🧠 <b>ANÁLISE IA PRÉ-JOGO</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        # ─────────────────────────────────────────────────────────────────
        # 1. HISTÓRICO PESSOAL
        # ─────────────────────────────────────────────────────────────────
        memory = analise_ia['memory']
        msg += f"\n📚 <b>SEU HISTÓRICO:</b>\n"
        if memory['tem_historico']:
            msg += f"├─ {memory['mensagem']}\n"
            msg += f"└─ Confiança: <b>{memory['confianca'].upper().replace('_', ' ')}</b>\n"
        else:
            msg += f"└─ {memory['mensagem']}\n"

        # ─────────────────────────────────────────────────────────────────
        # 2. CONFRONTO DIRETO (H2H)
        # ─────────────────────────────────────────────────────────────────
        h2h = analise_ia['h2h']
        msg += f"\n📊 <b>HISTÓRICO H2H:</b>\n"
        if h2h['tem_dados']:
            msg += f"├─ Últimos {h2h['total_jogos']} jogos\n"
            msg += f"├─ Casa: {h2h['vitorias_casa']}V | Empates: {h2h['empates']} | Fora: {h2h['vitorias_fora']}V\n"
            msg += f"├─ Over 2.5: {h2h['pct_over']:.0f}% | BTTS: {h2h['pct_btts']:.0f}%\n"
            msg += f"├─ Média gols: {h2h['media_gols']:.1f}/jogo\n"

            if h2h['padroes']:
                msg += f"└─ <b>Padrões:</b>\n"
                for padrao in h2h['padroes'][:2]:  # Top 2
                    msg += f"   └─ {padrao}\n"
        else:
            msg += f"└─ {h2h['mensagem']}\n"

        # ─────────────────────────────────────────────────────────────────
        # 3. FORMA RECENTE (MOMENTUM)
        # ─────────────────────────────────────────────────────────────────
        momentum = analise_ia['momentum']
        msg += f"\n🌡️ <b>FORMA RECENTE:</b>\n"
        if momentum['tem_dados']:
            casa = momentum['casa']
            fora = momentum['fora']

            msg += f"├─ Casa: {casa['sequencia']} | {casa['momentum']}\n"
            msg += f"│  └─ {casa['media_gols_marcados']:.1f} gols/jogo\n"
            msg += f"├─ Fora: {fora['sequencia']} | {fora['momentum']}\n"
            msg += f"│  └─ {fora['media_gols_marcados']:.1f} gols/jogo\n"
            msg += f"└─ <b>{momentum['comparacao']}</b>\n"
        else:
            msg += f"└─ {momentum['mensagem']}\n"

        # ─────────────────────────────────────────────────────────────────
        # 4. MOVIMENTO DE ODD (24H)
        # ─────────────────────────────────────────────────────────────────
        odds_mv = analise_ia['odds_movement']
        msg += f"\n🎯 <b>MOVIMENTO DE ODD:</b>\n"
        if odds_mv['tem_dados']:
            msg += f"├─ Abertura: @{odds_mv['odd_abertura']:.2f}\n"
            msg += f"├─ Atual: @{odds_mv['odd_atual']:.2f}\n"
            msg += f"├─ Variação: {odds_mv['variacao_pct']:+.1f}% {odds_mv['tendencia']}\n"
            msg += f"├─ {odds_mv['tipo_dinheiro']}\n"
            msg += f"└─ {odds_mv['emoji']} <b>{odds_mv['recomendacao']}</b>\n"
        else:
            msg += f"└─ {odds_mv['mensagem']}\n"

        # ─────────────────────────────────────────────────────────────────
        # 5. VOTAÇÃO DOS ESPECIALISTAS
        # ─────────────────────────────────────────────────────────────────
        multi = analise_ia['multi_agente']
        msg += f"\n🗳️ <b>VOTAÇÃO ESPECIALISTAS:</b>\n"

        for voto in multi['votos']:
            if voto['voto'] == 'aprovar':
                emoji_voto = "✅"
            elif voto['voto'] == 'rejeitar':
                emoji_voto = "⛔"
            else:
                emoji_voto = "⚖️"

            msg += f"├─ {voto['agente']}: {emoji_voto}\n"
            msg += f"│  └─ {voto['razao']}\n"

        msg += f"└─ <b>Consenso: {multi['emoji']} {multi['mensagem']}</b>\n"

        # ─────────────────────────────────────────────────────────────────
        # 6. GESTÃO DE STAKE
        # ─────────────────────────────────────────────────────────────────
        stake = analise_ia['stake']
        msg += f"\n💰 <b>GESTÃO DE STAKE:</b>\n"
        msg += f"├─ 🛡️ Conservador: R$ {stake['conservador']['valor']:.2f} ({stake['conservador']['porcentagem']:.1f}%)\n"
        msg += f"├─ ⚖️ Moderado: R$ {stake['moderado']['valor']:.2f} ({stake['moderado']['porcentagem']:.1f}%) ⭐\n"
        msg += f"└─ 🚀 Agressivo: R$ {stake['agressivo']['valor']:.2f} ({stake['agressivo']['porcentagem']:.1f}%)\n"

        # ─────────────────────────────────────────────────────────────────
        # 7. MOMENTO IDEAL DE ENTRADA (SMART ENTRY)
        # ─────────────────────────────────────────────────────────────────
        smart = analise_ia['smart_entry']
        msg += f"\n🔮 <b>MOMENTO IDEAL:</b>\n"

        if smart['decisao'] != 'ERRO':
            rec = smart['recomendacao']
            msg += f"├─ <b>Recomendação: {rec['emoji']} {rec['opcao']}</b>\n"
            msg += f"├─ Confiança: {smart['confianca']} pontos\n"

            if rec['razoes']:
                msg += f"└─ Motivos:\n"
                for razao in rec['razoes'][:3]:  # Top 3
                    msg += f"   └─ {razao}\n"

            # Se tem stake sugerido
            if 'stake_sugerido' in rec:
                msg += f"\n💡 <b>Stake sugerido: {rec['stake_sugerido']:.1f}%</b> (R$ {rec['stake_valor']:.2f})\n"

            # Se é esperar live
            if rec['opcao'] == 'ESPERAR LIVE':
                msg += f"⏰ Aguardar: {rec.get('aguardar', 'Início do jogo')}\n"
        else:
            msg += f"└─ {smart.get('mensagem', 'Erro na análise')}\n"

        msg += "\n━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        return msg

    except Exception as e:
        return f"\n\n⚠️ Erro na análise IA: {str(e)}\n"

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):
    if not stats: return []
    try:
        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']
        def get_v(l, t): v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0

        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')
        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')
        ck_h = get_v(stats_h, 'Corner Kicks'); ck_a = get_v(stats_a, 'Corner Kicks')
        blk_h = get_v(stats_h, 'Blocked Shots'); blk_a = get_v(stats_a, 'Blocked Shots')
        faltas_h = get_v(stats_h, 'Fouls'); faltas_a = get_v(stats_a, 'Fouls')
        cards_h = get_v(stats_h, 'Yellow Cards') + get_v(stats_h, 'Red Cards')
        cards_a = get_v(stats_a, 'Yellow Cards') + get_v(stats_a, 'Red Cards')

        total_chutes = sh_h + sh_a
        total_chutes_gol = sog_h + sog_a
        total_bloqueados = blk_h + blk_a
        chutes_fora_h = max(0, sh_h - sog_h - blk_h)
        chutes_fora_a = max(0, sh_a - sog_a - blk_a)
        total_fora = chutes_fora_h + chutes_fora_a

        posse_h = 50
        try: posse_h = int(str(next((x['value'] for x in stats_h if x['type']=='Ball Possession'), "50%")).replace('%', ''))
        except: pass
        posse_a = 100 - posse_h

        rh, ra = momentum(j['fixture']['id'], sog_h, sog_a)
        gh = j['goals']['home']; ga = j['goals']['away']; total_gols = gh + ga

        def gerar_ordem_gol(gols_atuais, tipo="Over"):
            linha = gols_atuais + 0.5
            if tipo == "Over": return f"👉 <b>FAZER:</b> Entrar em GOLS (Over)\n✅ Aposta: <b>Mais de {linha} Gols</b>"
            elif tipo == "HT": return f"👉 <b>FAZER:</b> Entrar em GOLS 1º TEMPO\n✅ Aposta: <b>Mais de 0.5 Gols HT</b>"
            elif tipo == "Limite": return f"👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de {gols_atuais + 1.0} Gols</b> (Asiático)"
            return "Apostar em Gols."

        SINAIS = []
        golden_bet_ativada = False

        # 1. GOLDEN BET (sog>=3, bloq>=3, chutes>=14, tempo 60-78)
        if 60 <= tempo <= 78:
            pressao_real_h = (rh >= 2 and sog_h >= 3)
            pressao_real_a = (ra >= 2 and sog_a >= 3)
            if (pressao_real_h or pressao_real_a) and (total_gols >= 1 or total_chutes >= 14):
                if total_bloqueados >= 3:
                    SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"🛡️ {total_bloqueados} Bloqueios (Pressão Real)", "rh": rh, "ra": ra, "favorito": "GOLS"})
                    golden_bet_ativada = True

        # 2. JANELA DE OURO (65-78 min, chutes_gol>=3)
        if not golden_bet_ativada and (65 <= tempo <= 78) and abs(gh - ga) <= 1:
            if total_chutes_gol >= 3:
                SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"🔥 {total_chutes_gol} Chutes no Gol", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 3. JOGO MORNO (domínio 65, sog>4)
        dominio_claro = (posse_h > 65 or posse_a > 65) or (sog_h > 4 or sog_a > 4)
        if 50 <= tempo <= 78 and total_chutes <= 12 and (sog_h + sog_a) <= 3 and gh == ga and not dominio_claro:
            SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": f"👉 <b>FAZER:</b> Under Gols\n✅ Aposta: <b>Menos de {total_gols + 0.5} Gols</b>", "stats": "Jogo Travado", "rh": rh, "ra": ra, "favorito": "UNDER"})

        # 4. ARAME LISO (mantido)
        if 55 <= tempo <= 80 and total_chutes >= 10 and (sog_h + sog_a) <= 3 and total_gols <= 1:
            SINAIS.append({"tag": "🧊 Arame Liso", "ordem": f"👉 <b>FAZER:</b> Under Gols\n⚠️ <i>Muita finalização pra fora.</i>\n✅ Aposta: <b>Menos de {total_gols + 1.5} Gols</b>", "stats": f"{total_chutes} Chutes (Só {sog_h+sog_a} no gol)", "rh": 0, "ra": 0, "favorito": "UNDER"})

        # 5. VOVÔ - RESTRITA (78-88, posse>=50, sog>=2, chutes 5-16)
        if 78 <= tempo <= 88 and total_chutes >= 5 and total_chutes <= 16:
            diff = gh - ga
            if diff == 1 and ra < 1 and posse_h >= 50 and sog_h >= 2:
                SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito (Segurar)\n✅ Aposta: <b>Vitória Seca</b>", "stats": "Controle Total", "rh": rh, "ra": ra, "favorito": "FAVORITO"})
            elif diff == -1 and rh < 1 and posse_a >= 50 and sog_a >= 2:
                SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito (Segurar)\n✅ Aposta: <b>Vitória Seca</b>", "stats": "Controle Total", "rh": rh, "ra": ra, "favorito": "FAVORITO"})

        # 6. PORTEIRA ABERTA (tempo<=35, + caminho alternativo)
        if tempo <= 35:
            if total_gols >= 2 and (sog_h >= 1 and sog_a >= 1):
                SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": gerar_ordem_gol(total_gols), "stats": "Jogo Aberto", "rh": rh, "ra": ra, "favorito": "GOLS"})
            elif total_gols >= 1 and total_chutes >= 6 and total_chutes_gol >= 3:
                SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": gerar_ordem_gol(total_gols), "stats": "Jogo Ofensivo", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 7. GOL RELÂMPAGO (tempo<=15, chutes>=3)
        if total_gols == 0 and (tempo <= 15 and total_chutes >= 3):
            SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": gerar_ordem_gol(0, "HT"), "stats": "Início Intenso", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 8. BLITZ (sh>=7, rh>=2, sog>=2)
        if tempo <= 65:
            blitz_casa = (gh <= ga) and (rh >= 2 or sh_h >= 7) and sog_h >= 2
            blitz_fora = (ga <= gh) and (ra >= 2 or sh_a >= 7) and sog_a >= 2
            if blitz_casa:
                SINAIS.append({"tag": "🟢 Blitz Casa", "ordem": gerar_ordem_gol(total_gols), "stats": "Pressão Limpa", "rh": rh, "ra": ra, "favorito": "GOLS"})
            elif blitz_fora:
                SINAIS.append({"tag": "🟢 Blitz Visitante", "ordem": gerar_ordem_gol(total_gols), "stats": "Pressão Limpa", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 9. TIROTEIO ELITE (12-30 min, chutes>=6, sog>=3)
        if 12 <= tempo <= 30 and total_chutes >= 6 and (sog_h + sog_a) >= 3:
            SINAIS.append({"tag": "🏹 Tiroteio Elite", "ordem": gerar_ordem_gol(total_gols), "stats": "Muitos Chutes", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 10. LAY GOLEADA (55-88, diff>=2, chutes>=12)
        if 55 <= tempo <= 88 and abs(gh - ga) >= 2 and total_chutes >= 12:
            time_perdendo_chuta = (gh < ga and sog_h >= 1) or (ga < gh and sog_a >= 1)
            if time_perdendo_chuta:
                SINAIS.append({"tag": "🔫 Lay Goleada", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": "Goleada Viva", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 11. SNIPER FINAL (tempo>=78, rh>=3, sog>=4)
        if tempo >= 78 and abs(gh - ga) <= 1:
            if total_fora <= 8 and ((rh >= 3) or (total_chutes_gol >= 4) or (ra >= 3)):
                SINAIS.append({"tag": "💎 Sniper Final", "ordem": "👉 <b>FAZER:</b> Over Gol Limite\n✅ Busque o Gol no Final", "stats": "Pontaria Ajustada", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 12. MASSACRE (20-45 min, domínio absoluto)
        if 20 <= tempo <= 45 and total_gols >= 1:
            if (sh_h >= 8 and sog_h >= 4 and posse_h >= 60) or (sh_a >= 8 and sog_a >= 4 and posse_a >= 60):
                SINAIS.append({"tag": "🔥 Massacre", "ordem": gerar_ordem_gol(total_gols, "HT"), "stats": "Domínio Absoluto", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 13. CHOQUE DE LÍDERES (15-45 min, jogo equilibrado)
        if 15 <= tempo <= 45 and abs(posse_h - posse_a) <= 10:
            if sog_h >= 2 and sog_a >= 2 and total_chutes >= 8:
                SINAIS.append({"tag": "⚔️ Choque Líderes", "ordem": gerar_ordem_gol(total_gols, "HT"), "stats": "Equilíbrio Ofensivo", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 14. BRIGA DE RUA (20-45 min, jogo agressivo)
        if 20 <= tempo <= 45:
            total_faltas = faltas_h + faltas_a
            if total_faltas >= 12 and total_chutes >= 6 and (sog_h + sog_a) >= 2:
                SINAIS.append({"tag": "🥊 Briga de Rua", "ordem": gerar_ordem_gol(total_gols, "HT"), "stats": f"🔥 {total_faltas} Faltas", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # 15. CONTRA-ATAQUE LETAL (30-70 min, time perdendo perigoso)
        if 30 <= tempo <= 70:
            if gh < ga and sog_h >= 3 and sh_h >= 5:
                SINAIS.append({"tag": "⚡ Contra-Ataque Letal", "ordem": "👉 <b>FAZER:</b> Back Empate ou Zebra\n✅ Aposta: <b>Mandante (Recuperação)</b>", "stats": "Pressão do Perdedor", "rh": rh, "ra": ra, "favorito": "ZEBRA"})
            elif ga < gh and sog_a >= 3 and sh_a >= 5:
                SINAIS.append({"tag": "⚡ Contra-Ataque Letal", "ordem": "👉 <b>FAZER:</b> Back Empate ou Zebra\n✅ Aposta: <b>Visitante (Recuperação)</b>", "stats": "Pressão do Perdedor", "rh": rh, "ra": ra, "favorito": "ZEBRA"})

        return SINAIS
    except:
        return []

# [REMOVIDO] Funções duplicadas movidas para seção única abaixo

def _worker_telegram(token, chat_id, msg):
    try:
        resp = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=10)
        if resp.status_code != 200:
            if resp.status_code == 400 and "parse entities" in resp.text:
                msg_clean = re.sub(r'<[^>]+>', '', msg)
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat_id, "text": msg_clean}, timeout=10)
            else:
                print(f"[TG] ❌ Erro {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"[TG] ❌ Exceção: {e}")

def enviar_telegram(token, chat_ids, msg):
    if not token or not chat_ids: return
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]

    msgs_para_enviar = []
    if len(msg) <= 4090:
        msgs_para_enviar.append(msg)
    else:
        buffer = ""
        linhas = msg.split('\n')
        for linha in linhas:
            if len(buffer) + len(linha) + 1 > 4000:
                msgs_para_enviar.append(buffer)
                buffer = linha + "\n"
            else:
                buffer += linha + "\n"
        if buffer: msgs_para_enviar.append(buffer)

    for cid in ids:
        for m in msgs_para_enviar:
            t = threading.Thread(target=_worker_telegram, args=(token, cid, m))
            t.daemon = True; t.start()
            time.sleep(0.3)

def salvar_snipers_do_texto(texto_ia):
    if not texto_ia or "Sem jogos" in texto_ia: return
    try:
        padrao_jogo = re.findall(r'⚽ Jogo: (.*?)(?:\n|$)', texto_ia)
        for i, jogo_nome in enumerate(padrao_jogo):
            item_sniper = {
                "FID": f"SNIPER_{random.randint(10000, 99999)}",
                "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": "08:00",
                "Liga": "Sniper Matinal", "Jogo": jogo_nome.strip(), "Placar_Sinal": "0x0",
                "Estrategia": "Sniper Matinal", "Resultado": "Pendente",
                "Opiniao_IA": "Sniper", "Probabilidade": "Alta"
            }
            adicionar_historico(item_sniper)
    except: pass

# ==============================================================================
# [MELHORIA V2] Sniper Matinal - salvar com filtro de qualidade (não remove o original)
# ==============================================================================
def salvar_snipers_do_texto_v2(texto_ia):
    if not texto_ia or 'Sem jogos' in str(texto_ia):
        return

    zonas = ['ZONA DE GOLS', 'ZONA DE TRINCHEIRA', 'ZONA DE MATCH ODDS', 'ZONA DE CARTÕES', 'ZONA DE ESCANTEIOS', 'ZONA DE DEFESAS']
    texto = str(texto_ia)
    if not any(z in texto for z in zonas):
        return

    try:
        jogos_encontrados = []
        for zona in zonas:
            if zona not in texto:
                continue
            bloco = texto.split(zona, 1)[1]
            for z2 in zonas:
                if z2 != zona and z2 in bloco:
                    bloco = bloco.split(z2, 1)[0]
            for linha in bloco.splitlines():
                if 'Jogo:' in linha:
                    nome = linha.replace('⚽', '').strip()
                    if nome and nome not in jogos_encontrados:
                        jogos_encontrados.append(nome)
                if len(jogos_encontrados) >= 5:
                    break
            if len(jogos_encontrados) >= 5:
                break

        for jogo_nome in jogos_encontrados[:5]:
            item_sniper = {
                'FID': f"SNIPER_{random.randint(10000, 99999)}",
                'Data': get_time_br().strftime('%Y-%m-%d'),
                'Hora': '08:00',
                'Liga': 'Sniper Matinal',
                'Jogo': jogo_nome.strip(),
                'Placar_Sinal': '0x0',
                'Estrategia': 'Sniper Matinal',
                'Resultado': 'Pendente',
                'Opiniao_IA': 'Sniper',
                'Probabilidade': 'Alta',
                'Tipo_Sinal': 'MATINAL',
                'Confidence_Score': '85'
            }
            adicionar_historico(item_sniper)
    except:
        pass

# Ativa versão v2 (mantém a antiga intacta)
salvar_snipers_do_texto = salvar_snipers_do_texto_v2

def salvar_snipers_completo(texto_ia, mapa_jogos, turno="Matinal"):
    """
    Salva snipers com FID REAL, zona, palpite e tipo para apuração automática.
    mapa_jogos: {"Team A x Team B": {"fid": "12345", "home_id": 1, "away_id": 2}}
    """
    if not texto_ia or 'Sem jogos' in str(texto_ia): return

    import re
    texto = str(texto_ia)

    # Mapeamento zona → tipo de apuração
    zona_map = {
        'ZONA DE GOLS': {'tipo': 'OVER', 'estrategia': f'Sniper Gols ({turno})'},
        'ZONA DE TRINCHEIRA': {'tipo': 'UNDER', 'estrategia': f'Sniper Under ({turno})'},
        'ZONA DE MATCH': {'tipo': 'RESULTADO', 'estrategia': f'Sniper Match ({turno})'},
        'ZONA DE CART': {'tipo': 'CARTOES', 'estrategia': f'Sniper Cartões ({turno})'},
        'ZONA DE ESCAN': {'tipo': 'ESCANTEIOS', 'estrategia': f'Sniper Escanteios ({turno})'},
        'ZONA DE DEFESA': {'tipo': 'DEFESAS', 'estrategia': f'Sniper Defesas ({turno})'},
    }

    # Encontra posições de cada zona
    zonas_pos = []
    for key in zona_map:
        m = re.search(key, texto, re.IGNORECASE)
        if m:
            zonas_pos.append((m.start(), key))
    zonas_pos.sort(key=lambda x: x[0])

    salvos = 0
    for idx, (pos, zona_key) in enumerate(zonas_pos):
        # Extrai bloco desta zona
        fim = zonas_pos[idx + 1][0] if idx + 1 < len(zonas_pos) else len(texto)
        bloco = texto[pos:fim]

        # Extrai jogos e palpites do bloco
        jogos_no_bloco = re.findall(r'(?:Jogo|⚽)[:\s]*(.+?)(?:\n|$)', bloco)
        palpites_no_bloco = re.findall(r'(?:Palpite|🎯)[:\s]*(.+?)(?:\n|$)', bloco)
        ligas_no_bloco = re.findall(r'(?:Liga|🏆)[:\s]*(.+?)(?:\n|$)', bloco)

        info = zona_map[zona_key]

        for i, jogo_raw in enumerate(jogos_no_bloco):
            jogo_nome = jogo_raw.strip().replace('<b>', '').replace('</b>', '').replace('**', '')
            palpite = palpites_no_bloco[i].strip().replace('<b>', '').replace('</b>', '').replace('**', '') if i < len(palpites_no_bloco) else "N/D"
            liga = ligas_no_bloco[i].strip().replace('<b>', '').replace('</b>', '') if i < len(ligas_no_bloco) else "Sniper"

            # Tenta encontrar FID real no mapa_jogos
            fid_real = f"SNIPER_{random.randint(10000, 99999)}"
            for key_mapa, val_mapa in mapa_jogos.items():
                # Match parcial no nome do jogo
                parts_mapa = key_mapa.split(' x ')
                if len(parts_mapa) >= 2:
                    if parts_mapa[0].strip().lower() in jogo_nome.lower() or parts_mapa[1].strip().lower() in jogo_nome.lower():
                        fid_real = str(val_mapa.get('fid', fid_real)) if isinstance(val_mapa, dict) else str(val_mapa)
                        break

            item = {
                'FID': fid_real,
                'Data': get_time_br().strftime('%Y-%m-%d'),
                'Hora': get_time_br().strftime('%H:%M'),
                'Liga': liga,
                'Jogo': jogo_nome,
                'Placar_Sinal': '0x0',
                'Estrategia': info['estrategia'],
                'Resultado': 'Pendente',
                'Opiniao_IA': 'Sniper',
                'Probabilidade': 'Alta',
                'Odd': '',
                'Palpite_Original': palpite,
                'Tipo_Apuracao': info['tipo'],
            }
            adicionar_historico(item)
            salvos += 1

    print(f"[SNIPER SAVE] ✅ {salvos} sinais salvos ({turno}) com FIDs e palpites")

def processar(j, stats, tempo, placar, rank_home=None, rank_away=None):

    if not stats: return []

    try:

        stats_h = stats[0]['statistics']; stats_a = stats[1]['statistics']

        def get_v(l, t): v = next((x['value'] for x in l if x['type']==t), 0); return v if v is not None else 0

        sh_h = get_v(stats_h, 'Total Shots'); sog_h = get_v(stats_h, 'Shots on Goal')

        sh_a = get_v(stats_a, 'Total Shots'); sog_a = get_v(stats_a, 'Shots on Goal')

        ck_h = get_v(stats_h, 'Corner Kicks'); ck_a = get_v(stats_a, 'Corner Kicks')

        blk_h = get_v(stats_h, 'Blocked Shots'); blk_a = get_v(stats_a, 'Blocked Shots')

        post_h = get_v(stats_h, 'Shots against goalbar')

        total_chutes = sh_h + sh_a; total_chutes_gol = sog_h + sog_a; total_bloqueados = blk_h + blk_a

        chutes_fora_h = max(0, sh_h - sog_h - blk_h); chutes_fora_a = max(0, sh_a - sog_a - blk_a)

        total_fora = chutes_fora_h + chutes_fora_a

        posse_h = 50

        try: posse_h = int(str(next((x['value'] for x in stats_h if x['type']=='Ball Possession'), "50%")).replace('%', ''))

        except: pass

        posse_a = 100 - posse_h

        rh, ra = momentum(j['fixture']['id'], sog_h, sog_a)

        gh = j['goals']['home']; ga = j['goals']['away']; total_gols = gh + ga

        def gerar_ordem_gol(gols_atuais, tipo="Over"):

            linha = gols_atuais + 0.5

            if tipo == "Over": return f"👉 <b>FAZER:</b> Entrar em GOLS (Over)\n✅ Aposta: <b>Mais de {linha} Gols</b>"

            elif tipo == "HT": return f"👉 <b>FAZER:</b> Entrar em GOLS 1º TEMPO\n✅ Aposta: <b>Mais de 0.5 Gols HT</b>"

            elif tipo == "Limite": return f"👉 <b>FAZER:</b> Entrar em GOL LIMITE\n✅ Aposta: <b>Mais de {gols_atuais + 1.0} Gols</b> (Asiático)"

            return "Apostar em Gols."

        SINAIS = []

        golden_bet_ativada = False

        # --- ESTRATÉGIA 1: GOLDEN BET (Corrigida: Exige Domínio Claro) ---

        if 65 <= tempo <= 75:

            # Filtro IA: Só entra se houver pressão REAL (Mínimo 4 chutes no gol de um lado E pressão alta)

            pressao_real_h = (rh >= 3 and sog_h >= 5) # Aumentei de 4 para 5

            pressao_real_a = (ra >= 3 and sog_a >= 5)

            if (pressao_real_h or pressao_real_a) and (total_gols >= 1 or total_chutes >= 18):

                if total_bloqueados >= 5:

                    SINAIS.append({"tag": "💎 GOLDEN BET", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"🛡️ {total_bloqueados} Bloqueios (Pressão Real)", "rh": rh, "ra": ra, "favorito": "GOLS"})

                    golden_bet_ativada = True

        # --- ESTRATÉGIA 2: JANELA DE OURO (Corrigida: Timing) ---

        if not golden_bet_ativada and (70 <= tempo <= 75) and abs(gh - ga) <= 1:

            # Filtro IA: Evita se o jogo estiver muito "fechado" (poucos chutes)

            if total_chutes_gol >= 5: # Aumentei régua de 4 para 5

                SINAIS.append({"tag": "💰 Janela de Ouro", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": f"🔥 {total_chutes_gol} Chutes no Gol", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # --- ESTRATÉGIA 3: JOGO MORNO (Corrigida: Domínio) ---

        # Filtro IA: Se um time tem mais de 60% de posse ou >3 chutes no gol, NÃO É MORNO.

        dominio_claro = (posse_h > 60 or posse_a > 60) or (sog_h > 3 or sog_a > 3)

        if 55 <= tempo <= 75 and total_chutes <= 10 and (sog_h + sog_a) <= 2 and gh == ga and not dominio_claro:

            SINAIS.append({"tag": "❄️ Jogo Morno", "ordem": f"👉 <b>FAZER:</b> Under Gols\n✅ Aposta: <b>Menos de {total_gols + 0.5} Gols</b>", "stats": "Jogo Travado", "rh": rh, "ra": ra, "favorito": "UNDER"})

        # --- ESTRATÉGIA 4: VOVÔ (Corrigida: Solidez Defensiva) ---

        if 75 <= tempo <= 85 and total_chutes < 18: # Ajustei tempo para 75-85 (mais seguro)

            diff = gh - ga

            # Filtro IA: Só entra se o time ganhando NÃO estiver sofrendo pressão (ra < 2)

            if (diff == 1 and ra < 1 and posse_h >= 45) or (diff == -1 and rh < 1 and posse_a >= 45):

                 SINAIS.append({"tag": "👴 Estratégia do Vovô", "ordem": "👉 <b>FAZER:</b> Back Favorito (Segurar)\n✅ Aposta: <b>Vitória Seca</b>", "stats": "Controle Total", "rh": rh, "ra": ra, "favorito": "FAVORITO"})

        # --- ESTRATÉGIA 5: PORTEIRA ABERTA (Corrigida: Equilíbrio) ---

        if tempo <= 30 and total_gols >= 2:

            # Filtro IA: Jogo tem que estar aberto (chutes dos dois lados)

            if sog_h >= 1 and sog_a >= 1:

                SINAIS.append({"tag": "🟣 Porteira Aberta", "ordem": gerar_ordem_gol(total_gols), "stats": "Jogo Aberto", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # --- ESTRATÉGIA 6: GOL RELÂMPAGO ---

        if total_gols == 0 and (tempo <= 12 and total_chutes >= 4): # Aumentei tempo 10->12 e chutes 3->4

            SINAIS.append({"tag": "⚡ Gol Relâmpago", "ordem": gerar_ordem_gol(0, "HT"), "stats": "Início Intenso", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # --- ESTRATÉGIA 7: BLITZ ---

        if tempo <= 60:

            if (gh <= ga and (rh >= 3 or sh_h >= 10)) or (ga <= gh and (ra >= 3 or sh_a >= 10)): # Aumentei sh 8->10

                if post_h == 0:

                    tag_blitz = "🟢 Blitz Casa" if gh <= ga else "🟢 Blitz Visitante"

                    SINAIS.append({"tag": tag_blitz, "ordem": gerar_ordem_gol(total_gols), "stats": "Pressão Limpa", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # --- ESTRATÉGIA 8: TIROTEIO ELITE ---

        if 15 <= tempo <= 25 and total_chutes >= 8 and (sog_h + sog_a) >= 4: # Aumentei chutes 6->8 e gol 3->4

             SINAIS.append({"tag": "🏹 Tiroteio Elite", "ordem": gerar_ordem_gol(total_gols), "stats": "Muitos Chutes", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # --- ESTRATÉGIA 9: LAY GOLEADA (Corrigida: Defesa Instável) ---

        if 60 <= tempo <= 88 and abs(gh - ga) >= 3 and (total_chutes >= 16): # Aumentei chutes 14->16

             # Filtro: Só entra se o time perdendo ainda estiver chutando (tentando honra)

             time_perdendo_chuta = (gh < ga and sog_h >= 2) or (ga < gh and sog_a >= 2)

             if time_perdendo_chuta:

                 SINAIS.append({"tag": "🔫 Lay Goleada", "ordem": gerar_ordem_gol(total_gols, "Limite"), "stats": "Goleada Viva", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # --- ESTRATÉGIA 10: SNIPER FINAL ---

        if tempo >= 80 and abs(gh - ga) <= 1:

            # Filtro: Exige pressão absurda no final

            if total_fora <= 6 and ((rh >= 5) or (total_chutes_gol >= 6) or (ra >= 5)):

                SINAIS.append({"tag": "💎 Sniper Final", "ordem": "👉 <b>FAZER:</b> Over Gol Limite\n✅ Busque o Gol no Final", "stats": "Pontaria Ajustada", "rh": rh, "ra": ra, "favorito": "GOLS"})

        # ═══ [NOVA] CONTRA-ATAQUE FULMINANTE ═══
        # Time perdendo domina posse + chutes → gol iminente
        if 60 <= tempo <= 75 and abs(gh - ga) == 1:
            # Identifica quem está perdendo
            if gh < ga:  # Casa perdendo
                perdedor_posse = posse_h
                perdedor_sog = sog_h
                ganhador_sog = sog_a
            else:  # Fora perdendo
                perdedor_posse = posse_a
                perdedor_sog = sog_a
                ganhador_sog = sog_h

            if perdedor_posse >= 55 and perdedor_sog >= (ganhador_sog * 2) and perdedor_sog >= 4:
                SINAIS.append({
                    "tag": "⚔️ Contra-Ataque Fulminante",
                    "ordem": f"👉 <b>FAZER:</b> Over 0.5 Gols (Restante)\n✅ Aposta: <b>Próximo Gol</b>",
                    "stats": f"Perdedor: {perdedor_posse}% posse, {perdedor_sog} chutes no gol vs {ganhador_sog}",
                    "rh": rh, "ra": ra, "favorito": "GOLS"
                })

        # ═══ [NOVA] ESCANTEIO TÁTICO ═══
        # Jogo 1x1 com escanteios baixos mas um time atacando muito → explosão de corners
        total_corners = ck_h + ck_a
        if 45 <= tempo <= 65 and gh == 1 and ga == 1 and total_corners < 7:
            diff_corners = abs(ck_h - ck_a)
            # Time com mais escanteios tem ataque perigoso
            if ck_h > ck_a:
                atacante_sog = sog_h
                atacante_shots = sh_h
            else:
                atacante_sog = sog_a
                atacante_shots = sh_a
            precisao = (atacante_sog / atacante_shots * 100) if atacante_shots > 0 else 0

            if diff_corners >= 3 and precisao >= 55:
                SINAIS.append({
                    "tag": "🏴 Escanteio Tático",
                    "ordem": f"👉 <b>FAZER:</b> Over 9.5 Escanteios\n✅ Aposta: <b>Mais de 9.5 Escanteios</b>",
                    "stats": f"Escanteios: {ck_h}x{ck_a} | Precisão atacante: {precisao:.0f}%",
                    "rh": rh, "ra": ra, "favorito": "ESCANTEIOS"
                })

        return SINAIS

    except: return []

# --- FIM PARTE 3 ---

# ==============================================================================

# 5. FUNÇÕES DE SUPORTE, AUTOMAÇÃO E INTERFACE (O CORPO)

# ==============================================================================

def deve_buscar_stats(tempo, gh, ga, status):

    if status == 'HT': return True

    if 0 <= tempo <= 95: return True

    return False

def fetch_stats_single(fid, api_key):

    try:

        url = "https://v3.football.api-sports.io/fixtures/statistics"

        r = requests.get(url, headers={"x-apisports-key": api_key}, params={"fixture": fid}, timeout=3)

        return fid, r.json().get('response', []), r.headers

    except: return fid, [], None

def atualizar_stats_em_paralelo(jogos_alvo, api_key):

    resultados = {}

    with ThreadPoolExecutor(max_workers=5) as executor:

        futures = {executor.submit(fetch_stats_single, j['fixture']['id'], api_key): j for j in jogos_alvo}

        time.sleep(0.1)

        for future in as_completed(futures):

            fid, stats, headers = future.result()

            if stats:

                resultados[fid] = stats

                update_api_usage(headers)

    return resultados

# ==============================================================================

# FUNÇÕES DE IA AUXILIARES (BI, FINANCEIRO E ESTRATÉGIA)

# ==============================================================================

def analisar_bi_com_ia():

    if not IA_ATIVADA: return "IA Offline."

    df = st.session_state.get('historico_full', pd.DataFrame())

    if df.empty: return "Sem dados suficientes para análise de BI."

    try:

        # Preparando dados para a IA ler

        df = df.copy()

        # Limpeza básica para garantir que a IA entenda (pega os últimos 30 registros)

        resumo_csv = df.tail(30).to_string(index=False)

        prompt = f"""

        ATUE COMO UM CONSULTOR DE DATA SCIENCE E TRADING ESPORTIVO.

        Analise os últimos resultados do robô abaixo (CSV):

        {resumo_csv}

        SUA MISSÃO:

        1. Identifique qual Estratégia está dando mais Green.

        2. Identifique se há algum padrão nos Reds (ex: alguma liga específica ou horário).

        3. Dê uma nota de 0 a 10 para o desempenho recente.

        SAÍDA (Seja direto, máximo 4 linhas):

        "Insight: [Sua análise aqui]"

        """

        response = gemini_safe_call(prompt, generation_config=genai.types.GenerationConfig(temperature=0.5))

        return response.text.strip()

    except Exception as e:

        return "Não foi possível gerar o insight da IA no momento."

def analisar_financeiro_com_ia(stake_padrao, banca_inicial):
    """Relatório financeiro REAL — usa odds e stakes reais do histórico."""
    try:
        df = st.session_state.get('historico_full', pd.DataFrame())
        hoje = get_time_br().strftime('%Y-%m-%d')

        df_hoje = df[df['Data'] == hoje].copy()
        if df_hoje.empty: return "Sem operações hoje."

        # Separa Aprovados vs Arriscados
        if 'Opiniao_IA' in df_hoje.columns:
            df_aprov = df_hoje[df_hoje['Opiniao_IA'] == 'Aprovado'].drop_duplicates(subset=['FID'])
            df_arris = df_hoje[df_hoje['Opiniao_IA'].isin(['Arriscado', 'Neutro'])].drop_duplicates(subset=['FID'])
        else:
            df_aprov = df_hoje.drop_duplicates(subset=['FID'])
            df_arris = pd.DataFrame()

        banca_atual = float(st.session_state.get('banca_atual', banca_inicial))
        kelly_modo = st.session_state.get('kelly_modo', 'fracionario')

        lucro_total = 0
        detalhes = []

        for _, row in df_aprov.iterrows():
            resultado = str(row.get('Resultado', ''))
            if 'GREEN' not in resultado and 'RED' not in resultado:
                continue

            # Odd REAL
            try:
                odd_raw = str(row.get('Odd', '1.50')).replace(',', '.')
                odd_real = float(odd_raw) if odd_raw and odd_raw.lower() != 'nan' else 1.50
                if math.isnan(odd_real) or odd_real <= 1.0: odd_real = 1.50
            except: odd_real = 1.50

            # Stake REAL (Kelly)
            try:
                prob_raw = str(row.get('Probabilidade', '60')).replace('%', '').replace('...', '0')
                prob = float(prob_raw) if prob_raw and prob_raw.lower() != 'nan' else 60.0
                if math.isnan(prob): prob = 60.0
                kelly_res = calcular_stake_recomendado(banca_atual, prob, odd_real, kelly_modo, 'Aprovado')
                stake_real = kelly_res['valor'] if kelly_res and kelly_res['valor'] > 0 else float(stake_padrao)
                if math.isnan(stake_real) or math.isinf(stake_real): stake_real = float(stake_padrao)
            except: stake_real = float(stake_padrao)

            jogo = str(row.get('Jogo', ''))[:25]

            if 'GREEN' in resultado:
                lucro = round(stake_real * (odd_real - 1), 2)
                if math.isnan(lucro): lucro = 0
                lucro_total += lucro
                detalhes.append(f"✅ {jogo} | +R$ {lucro:.2f} (@{odd_real:.2f})")
            elif 'RED' in resultado:
                lucro_total -= stake_real
                detalhes.append(f"❌ {jogo} | -R$ {stake_real:.2f}")

        greens = len([d for d in detalhes if d.startswith("✅")])
        reds = len([d for d in detalhes if d.startswith("❌")])
        total_entradas = greens + reds
        winrate = (greens / total_entradas * 100) if total_entradas > 0 else 0

        # Arriscados (info)
        arr_g = len(df_arris[df_arris['Resultado'].str.contains('GREEN', na=False)]) if not df_arris.empty else 0
        arr_r = len(df_arris[df_arris['Resultado'].str.contains('RED', na=False)]) if not df_arris.empty else 0

        if math.isnan(lucro_total): lucro_total = 0
        roi = (lucro_total / (total_entradas * float(stake_padrao)) * 100) if total_entradas > 0 and float(stake_padrao) > 0 else 0
        if math.isnan(roi): roi = 0

        emoji = "🤑" if lucro_total > 0 else "🔻" if lucro_total < 0 else "➖"
        banca_final = round(banca_atual, 2)

        detalhes_txt = ""
        for d in detalhes[:8]:
            detalhes_txt += f"  {d}\n"
        if len(detalhes) > 8:
            detalhes_txt += f"  <i>... +{len(detalhes)-8} sinais</i>\n"

        texto = f"""💰 <b>FECHAMENTO DO DIA</b>

📊 <b>SINAIS APROVADOS:</b>
✅ {greens} Greens | ❌ {reds} Reds
🎯 Winrate: {winrate:.0f}%

{detalhes_txt}
▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬
{emoji} <b>Resultado:</b> R$ {lucro_total:+.2f}
📈 <b>ROI:</b> {roi:+.1f}%
🏦 <b>Banca:</b> R$ {banca_final:,.2f}
▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬

📋 <b>SINAIS IGNORADOS (Arriscados):</b>
🟢 {arr_g} teriam sido GREEN | 🔴 {arr_r} teriam sido RED
"""
        return texto
    except Exception as e: return f"Erro: {e}"

# ==============================================================================

def atualizar_aprendizado():
    """
    Roda 1x por dia (junto com reset diário).
    Analisa histórico e gera REGRAS AUTOMÁTICAS que o robô aplica nos sinais.

    Grava em st.session_state['regras_aprendidas'] um dict:
    {
        'ligas_bloqueadas': {'Lay Goleada': ['Süper Lig', 'Saudi League'], ...},
        'odd_minima_ajustada': {'Janela de Ouro': 1.40, ...},
        'estrategias_desativadas': ['Múltiplas'],
        'estrategias_top': ['Estratégia do Vovô', 'Blitz Casa'],
        'ligas_top': {'Estratégia do Vovô': ['Premier League', 'La Liga']},
        'ultima_atualizacao': '2026-02-15',
        'total_jogos_analisados': 138,
    }
    """
    try:
        df = st.session_state.get('historico_full', pd.DataFrame())
        if df.empty: return

        df_final = df[df['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
        if len(df_final) < 20: return  # Precisa de massa crítica

        regras = {
            'ligas_bloqueadas': {},
            'odd_minima_ajustada': {},
            'estrategias_desativadas': [],
            'estrategias_top': [],
            'ligas_top': {},
            'horarios_fortes': [],
            'ultima_atualizacao': get_time_br().strftime('%Y-%m-%d'),
            'total_jogos_analisados': len(df_final),
        }

        # ═══════════════════════════════════════════════
        # 1. LIGAS TÓXICAS por estratégia (auto-block)
        # ═══════════════════════════════════════════════
        for strat in df_final['Estrategia'].unique():
            df_s = df_final[df_final['Estrategia'] == strat]
            ligas_block = []
            ligas_boas = []

            for liga in df_s['Liga'].unique():
                df_sl = df_s[df_s['Liga'] == liga]
                if len(df_sl) >= 3:  # Mínimo 3 jogos para julgar
                    g = len(df_sl[df_sl['Resultado'].str.contains('GREEN')])
                    wr = (g / len(df_sl)) * 100
                    if wr < 35:  # Menos de 35% = tóxica
                        ligas_block.append(liga)
                    elif wr >= 75 and len(df_sl) >= 3:  # 75%+ = forte
                        ligas_boas.append(liga)

            if ligas_block:
                regras['ligas_bloqueadas'][strat] = ligas_block
            if ligas_boas:
                regras['ligas_top'][strat] = ligas_boas

        # ═══════════════════════════════════════════════
        # 2. ODD MÍNIMA por estratégia (auto-ajuste)
        # ═══════════════════════════════════════════════
        for strat in df_final['Estrategia'].unique():
            df_s = df_final[df_final['Estrategia'] == strat]
            if len(df_s) < 5: continue

            try:
                df_s['Odd_Num'] = df_s['Odd'].apply(lambda x: float(str(x).replace(',','.')) if str(x).replace(',','.').replace('.','').isdigit() else 0)

                # Odds baixas (<1.35) winrate
                df_low = df_s[df_s['Odd_Num'].between(1.01, 1.35)]
                if len(df_low) >= 3:
                    g_low = len(df_low[df_low['Resultado'].str.contains('GREEN')])
                    wr_low = (g_low / len(df_low)) * 100
                    if wr_low < 45:  # Odds baixas dando muito RED
                        regras['odd_minima_ajustada'][strat] = 1.40

                # Odds médias (1.35-1.80) winrate
                df_mid = df_s[df_s['Odd_Num'].between(1.35, 1.80)]
                if len(df_mid) >= 3:
                    g_mid = len(df_mid[df_mid['Resultado'].str.contains('GREEN')])
                    wr_mid = (g_mid / len(df_mid)) * 100
                    if wr_mid < 45:
                        regras['odd_minima_ajustada'][strat] = 1.80
            except: pass

        # ═══════════════════════════════════════════════
        # 3. ESTRATÉGIAS para desativar (winrate < 40% com 10+ jogos)
        # ═══════════════════════════════════════════════
        for strat in df_final['Estrategia'].unique():
            df_s = df_final[df_final['Estrategia'] == strat]
            if len(df_s) >= 10:
                g = len(df_s[df_s['Resultado'].str.contains('GREEN')])
                wr = (g / len(df_s)) * 100
                if wr < 40:
                    regras['estrategias_desativadas'].append(strat)
                elif wr >= 70:
                    regras['estrategias_top'].append(strat)

        # ═══════════════════════════════════════════════
        # 4. HORÁRIOS fortes
        # ═══════════════════════════════════════════════
        try:
            df_final['Hora_Num'] = df_final['Hora'].apply(lambda x: int(str(x).split(':')[0]) if ':' in str(x) else 0)
            for nome, h_min, h_max in [("manha", 6, 12), ("tarde", 12, 17), ("noite", 17, 23)]:
                df_p = df_final[(df_final['Hora_Num'] >= h_min) & (df_final['Hora_Num'] < h_max)]
                if len(df_p) >= 5:
                    g_p = len(df_p[df_p['Resultado'].str.contains('GREEN')])
                    wr_p = (g_p / len(df_p)) * 100
                    if wr_p >= 65:
                        regras['horarios_fortes'].append(nome)
        except: pass

        # Salva
        st.session_state['regras_aprendidas'] = regras

        # Log
        n_bloq = sum(len(v) for v in regras['ligas_bloqueadas'].values())
        n_odd = len(regras['odd_minima_ajustada'])
        n_desat = len(regras['estrategias_desativadas'])
        n_top = len(regras['estrategias_top'])
        print(f"[APRENDIZADO] ✅ {len(df_final)} jogos | {n_bloq} ligas bloqueadas | {n_odd} odds ajustadas | {n_desat} strats desativadas | {n_top} strats top")

    except Exception as e:
        print(f"[APRENDIZADO] ❌ Erro: {e}")

def aplicar_filtro_aprendizado(estrategia, liga, odd, opiniao_ia):
    """
    Aplica as regras aprendidas ANTES de enviar sinal.
    Retorna: (deve_enviar: bool, motivo: str)
    """
    regras = st.session_state.get('regras_aprendidas', {})
    if not regras: return True, ""

    # 1. Estratégia desativada?
    if estrategia in regras.get('estrategias_desativadas', []):
        return False, f"[APRENDIZADO] {estrategia} desativada (WR < 40%)"

    # 2. Liga bloqueada para essa estratégia?
    ligas_block = regras.get('ligas_bloqueadas', {}).get(estrategia, [])
    if liga in ligas_block:
        return False, f"[APRENDIZADO] {liga} bloqueada para {estrategia}"

    # 3. Odd mínima ajustada?
    try:
        odd_num = float(str(odd).replace(',', '.'))
        odd_min = regras.get('odd_minima_ajustada', {}).get(estrategia, 0)
        if odd_min > 0 and odd_num < odd_min:
            return False, f"[APRENDIZADO] Odd {odd_num:.2f} < mínima {odd_min:.2f} para {estrategia}"
    except: pass

    return True, ""

def formatar_resumo_aprendizado():
    """Gera texto resumo do aprendizado para o BI."""
    regras = st.session_state.get('regras_aprendidas', {})
    if not regras: return "🧠 Sem dados suficientes para aprendizado."

    txt = f"🧠 <b>APRENDIZADO ({regras.get('total_jogos_analisados', 0)} jogos)</b>\n"

    # Bloqueios
    bloqs = regras.get('ligas_bloqueadas', {})
    if bloqs:
        txt += "🚫 <b>Bloqueios automáticos:</b>\n"
        for strat, ligas in bloqs.items():
            txt += f"  {strat}: {', '.join(ligas[:3])}\n"

    # Odds ajustadas
    odds_aj = regras.get('odd_minima_ajustada', {})
    if odds_aj:
        txt += "📊 <b>Odds ajustadas:</b>\n"
        for strat, odd in odds_aj.items():
            txt += f"  {strat}: mín @{odd:.2f}\n"

    # Top
    tops = regras.get('estrategias_top', [])
    if tops:
        txt += f"⭐ <b>Top:</b> {', '.join(tops[:3])}\n"

    # Desativadas
    desat = regras.get('estrategias_desativadas', [])
    if desat:
        txt += f"⛔ <b>Desativadas:</b> {', '.join(desat)}\n"

    return txt

def diagnostico_real_estrategias():
    """
    Diagnóstico REAL baseado em dados do histórico.
    Python calcula TUDO → Gemini só interpreta.
    """
    if not IA_ATIVADA: return "IA Offline."

    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados suficientes."

    try:
        df_final = df[df['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
        if len(df_final) < 15: return "Preciso de pelo menos 15 jogos finalizados."

        total = len(df_final)
        greens_total = len(df_final[df_final['Resultado'].str.contains('GREEN')])
        reds_total = total - greens_total
        wr_global = (greens_total / total) * 100

        # ══════════════════════════════════════════════════
        # 1. BREAKDOWN POR ESTRATÉGIA
        # ══════════════════════════════════════════════════
        stats_por_estrategia = []
        for strat in df_final['Estrategia'].unique():
            df_s = df_final[df_final['Estrategia'] == strat]
            g = len(df_s[df_s['Resultado'].str.contains('GREEN')])
            r = len(df_s) - g
            wr = (g / len(df_s)) * 100 if len(df_s) > 0 else 0

            # Breakdown por liga dentro da estratégia
            ligas_reds = []
            ligas_greens = []
            for liga in df_s['Liga'].unique():
                df_sl = df_s[df_s['Liga'] == liga]
                g_l = len(df_sl[df_sl['Resultado'].str.contains('GREEN')])
                r_l = len(df_sl) - g_l
                wr_l = (g_l / len(df_sl)) * 100 if len(df_sl) > 0 else 0
                if len(df_sl) >= 2:
                    if wr_l < 40:
                        ligas_reds.append(f"{liga}: {g_l}G/{r_l}R ({wr_l:.0f}%)")
                    elif wr_l >= 75:
                        ligas_greens.append(f"{liga}: {g_l}G/{r_l}R ({wr_l:.0f}%)")

            # Odds dos REDs vs GREENs
            odd_media_red = 0
            odd_media_green = 0
            try:
                df_reds = df_s[df_s['Resultado'].str.contains('RED')]
                df_greens = df_s[df_s['Resultado'].str.contains('GREEN')]
                if len(df_reds) > 0:
                    odd_media_red = df_reds['Odd'].apply(lambda x: float(str(x).replace(',','.')) if str(x).replace(',','.').replace('.','').isdigit() else 0).mean()
                if len(df_greens) > 0:
                    odd_media_green = df_greens['Odd'].apply(lambda x: float(str(x).replace(',','.')) if str(x).replace(',','.').replace('.','').isdigit() else 0).mean()
            except: pass

            # IA opinion breakdown
            ia_aprovado_wr = 0
            ia_arriscado_wr = 0
            try:
                df_aprov = df_s[df_s['Opiniao_IA'] == 'Aprovado']
                df_arris = df_s[df_s['Opiniao_IA'].isin(['Arriscado', 'Neutro'])]
                if len(df_aprov) >= 2:
                    ia_aprovado_wr = (len(df_aprov[df_aprov['Resultado'].str.contains('GREEN')]) / len(df_aprov)) * 100
                if len(df_arris) >= 2:
                    ia_arriscado_wr = (len(df_arris[df_arris['Resultado'].str.contains('GREEN')]) / len(df_arris)) * 100
            except: pass

            stats_por_estrategia.append({
                'nome': strat,
                'total': len(df_s),
                'greens': g,
                'reds': r,
                'winrate': wr,
                'ligas_ruins': ligas_reds[:5],
                'ligas_boas': ligas_greens[:5],
                'odd_media_red': odd_media_red,
                'odd_media_green': odd_media_green,
                'ia_aprovado_wr': ia_aprovado_wr,
                'ia_arriscado_wr': ia_arriscado_wr,
            })

        # Ordena por mais REDs
        stats_por_estrategia.sort(key=lambda x: x['reds'], reverse=True)

        # ══════════════════════════════════════════════════
        # 2. BREAKDOWN POR LIGA (GERAL)
        # ══════════════════════════════════════════════════
        stats_por_liga = []
        for liga in df_final['Liga'].unique():
            df_l = df_final[df_final['Liga'] == liga]
            if len(df_l) >= 3:
                g_l = len(df_l[df_l['Resultado'].str.contains('GREEN')])
                wr_l = (g_l / len(df_l)) * 100
                stats_por_liga.append({
                    'liga': liga,
                    'total': len(df_l),
                    'greens': g_l,
                    'reds': len(df_l) - g_l,
                    'winrate': wr_l
                })
        stats_por_liga.sort(key=lambda x: x['winrate'])

        # ══════════════════════════════════════════════════
        # 3. BREAKDOWN POR FAIXA DE ODD
        # ══════════════════════════════════════════════════
        faixas_odd = []
        try:
            df_final['Odd_Num'] = df_final['Odd'].apply(lambda x: float(str(x).replace(',','.')) if str(x).replace(',','.').replace('.','').isdigit() else 0)
            for faixa_nome, faixa_min, faixa_max in [
                ("1.01-1.30", 1.01, 1.30), ("1.31-1.50", 1.31, 1.50),
                ("1.51-1.80", 1.51, 1.80), ("1.81-2.20", 1.81, 2.20),
                ("2.21-3.00", 2.21, 3.00), ("3.01+", 3.01, 99)
            ]:
                df_f = df_final[(df_final['Odd_Num'] >= faixa_min) & (df_final['Odd_Num'] < faixa_max)]
                if len(df_f) >= 2:
                    g_f = len(df_f[df_f['Resultado'].str.contains('GREEN')])
                    faixas_odd.append(f"{faixa_nome}: {g_f}G/{len(df_f)-g_f}R ({(g_f/len(df_f))*100:.0f}%) [{len(df_f)} jogos]")
        except: pass

        # ══════════════════════════════════════════════════
        # 4. PADRÕES TEMPORAIS
        # ══════════════════════════════════════════════════
        padrao_temporal = ""
        try:
            df_final['Hora_Num'] = df_final['Hora'].apply(lambda x: int(str(x).split(':')[0]) if ':' in str(x) else 0)
            for periodo_nome, h_min, h_max in [
                ("Manhã (6-12h)", 6, 12), ("Tarde (12-17h)", 12, 17),
                ("Noite (17-23h)", 17, 23)
            ]:
                df_p = df_final[(df_final['Hora_Num'] >= h_min) & (df_final['Hora_Num'] < h_max)]
                if len(df_p) >= 3:
                    g_p = len(df_p[df_p['Resultado'].str.contains('GREEN')])
                    padrao_temporal += f"  {periodo_nome}: {g_p}G/{len(df_p)-g_p}R ({(g_p/len(df_p))*100:.0f}%)\n"
        except: pass

        # ══════════════════════════════════════════════════
        # 5. MONTA RELATÓRIO PRÉ-CALCULADO
        # ══════════════════════════════════════════════════
        relatorio = f"""DADOS PRÉ-CALCULADOS (Python — números REAIS do histórico):

📊 GERAL: {total} jogos | {greens_total}G / {reds_total}R | Winrate: {wr_global:.1f}%

📋 POR ESTRATÉGIA (ordenado por mais REDs):
"""
        for s in stats_por_estrategia:
            relatorio += f"""
  {s['nome']}: {s['total']} jogos | {s['greens']}G/{s['reds']}R | WR: {s['winrate']:.0f}%
    Odd média GREEN: {s['odd_media_green']:.2f} | Odd média RED: {s['odd_media_red']:.2f}
    IA Aprovado WR: {s['ia_aprovado_wr']:.0f}% | IA Arriscado WR: {s['ia_arriscado_wr']:.0f}%
    ❌ Ligas ruins: {', '.join(s['ligas_ruins']) if s['ligas_ruins'] else 'Nenhuma com 2+ jogos'}
    ✅ Ligas boas: {', '.join(s['ligas_boas']) if s['ligas_boas'] else 'Nenhuma com 2+ jogos'}
"""

        relatorio += f"""
🏆 RANKING DE LIGAS (pior → melhor):
"""
        for l in stats_por_liga[:5]:
            relatorio += f"  ❌ {l['liga']}: {l['greens']}G/{l['reds']}R ({l['winrate']:.0f}%) [{l['total']} jogos]\n"
        relatorio += "  ...\n"
        for l in stats_por_liga[-5:]:
            relatorio += f"  ✅ {l['liga']}: {l['greens']}G/{l['reds']}R ({l['winrate']:.0f}%) [{l['total']} jogos]\n"

        relatorio += f"""
💰 POR FAIXA DE ODD:
"""
        for f in faixas_odd:
            relatorio += f"  {f}\n"

        if padrao_temporal:
            relatorio += f"""
⏰ POR HORÁRIO:
{padrao_temporal}"""

        # ══════════════════════════════════════════════════
        # 6. GEMINI INTERPRETA (não calcula!)
        # ══════════════════════════════════════════════════
        prompt = f"""ATUE COMO CONSULTOR DE DADOS ESPORTIVOS.

Abaixo estão estatísticas REAIS já calculadas do histórico de apostas do usuário.
NÃO invente números. Use APENAS os dados abaixo.

{relatorio}

TAREFA:
1. DIAGNÓSTICO: Identifique as 3 maiores fraquezas (estratégia + liga + odd que mais dão RED)
2. AÇÕES CONCRETAS: Para cada fraqueza, dê uma ação ESPECÍFICA:
   - "Bloquear liga X na estratégia Y" (se liga tem <40% winrate)
   - "Aumentar odd mínima para Z na estratégia W" (se odds baixas dão mais RED)
   - "Desativar estratégia K" (se winrate < 45% com 10+ jogos)
3. PONTOS FORTES: As 2 melhores combinações (estratégia + liga + odd)
4. PROJEÇÃO: Se aplicar as ações, quantos REDs seriam eliminados?

FORMATO: Use emojis, seja direto, sem enrolação. Cite os NÚMEROS do relatório."""

        response = gemini_safe_call(prompt, generation_config=genai.types.GenerationConfig(temperature=0.2))

        # Combina relatório numérico + interpretação IA
        resultado_final = f"""📊 **DADOS REAIS ({total} jogos)**
🏆 Winrate: {wr_global:.1f}% ({greens_total}G / {reds_total}R)

"""
        # Top 3 estratégias por volume
        for s in stats_por_estrategia[:5]:
            emoji = "✅" if s['winrate'] >= 65 else "⚠️" if s['winrate'] >= 50 else "❌"
            resultado_final += f"{emoji} **{s['nome']}**: {s['winrate']:.0f}% ({s['greens']}G/{s['reds']}R)\n"

        resultado_final += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{response.text.strip()}"

        return resultado_final

    except Exception as e:
        import traceback; traceback.print_exc()
        return f"Erro no diagnóstico: {str(e)}"

def criar_estrategia_nova_ia():
    """
    Sugestão de NOVAS estratégias baseada nos dados reais.
    Analisa onde o robô tem edge e sugere variações.
    """
    if not IA_ATIVADA: return "IA Offline."

    df = st.session_state.get('historico_full', pd.DataFrame())
    if df.empty: return "Sem dados suficientes."

    try:
        df_final = df[df['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()
        if len(df_final) < 15: return "Preciso de pelo menos 15 jogos."

        total = len(df_final)
        greens = len(df_final[df_final['Resultado'].str.contains('GREEN')])
        wr = (greens / total) * 100

        # Estatísticas por estratégia
        resumo_strats = ""
        for strat in df_final['Estrategia'].unique():
            df_s = df_final[df_final['Estrategia'] == strat]
            g = len(df_s[df_s['Resultado'].str.contains('GREEN')])
            wr_s = (g / len(df_s)) * 100 if len(df_s) > 0 else 0
            ligas_top = []
            for liga in df_s['Liga'].unique():
                df_sl = df_s[df_s['Liga'] == liga]
                if len(df_sl) >= 2:
                    g_l = len(df_sl[df_sl['Resultado'].str.contains('GREEN')])
                    wr_l = (g_l / len(df_sl)) * 100
                    if wr_l >= 70:
                        ligas_top.append(f"{liga}({wr_l:.0f}%)")
            resumo_strats += f"  {strat}: {len(df_s)} jogos, WR {wr_s:.0f}%, Ligas fortes: {', '.join(ligas_top[:3]) if ligas_top else 'N/A'}\n"

        # Mercados mais usados e seus resultados
        # Check for Placar_Sinal patterns
        placares_info = ""
        try:
            for placar_pattern in ['0x0', '0x1', '1x0', '1x1', '2x0', '2x1']:
                df_p = df_final[df_final['Placar_Sinal'].str.contains(placar_pattern, na=False)]
                if len(df_p) >= 3:
                    g_p = len(df_p[df_p['Resultado'].str.contains('GREEN')])
                    placares_info += f"  Placar {placar_pattern}: {g_p}G/{len(df_p)-g_p}R ({(g_p/len(df_p))*100:.0f}%)\n"
        except: pass

        prompt = f"""ATUE COMO ENGENHEIRO DE ESTRATÉGIAS DE APOSTAS ESPORTIVAS.

DADOS REAIS DO ROBÔ:
- {total} jogos | Winrate: {wr:.1f}%
- Estratégias atuais:
{resumo_strats}
{f'- Padrões por placar de entrada:{chr(10)}{placares_info}' if placares_info else ''}

O robô monitora jogos AO VIVO e entra em mercados como:
Over Gols, Under, BTTS, Match Odds, Cartões, Escanteios, Lay Goleada

TAREFA: Sugira 2 NOVAS estratégias que o robô ainda NÃO usa, baseadas em:
1. PADRÕES que os dados mostram (ex: se Over tem alto winrate em ligas X, criar variação)
2. COMBINAÇÕES inexploradas (ex: "Over 0.5 HT + BTTS" quando ambos times marcam >60%)
3. FILTROS ESPECÍFICOS que melhorariam a entrada

Para cada estratégia:
- Nome criativo
- Regra de entrada (condições específicas com números)
- Em que ligas/horários funcionaria melhor (baseado nos dados)
- Odd mínima/máxima sugerida
- Projeção de winrate esperado

Seja ESPECÍFICO e USE os números do histórico. Nada genérico."""

        response = gemini_safe_call(prompt, generation_config=genai.types.GenerationConfig(temperature=0.4))
        return response.text.strip()

    except Exception as e: return f"Erro: {str(e)}"

def otimizar_estrategias_existentes_ia():
    """Wrapper: chama o diagnóstico real."""
    return diagnostico_real_estrategias()

def enviar_analise_estrategia(token, chat_ids):

    sugestao = criar_estrategia_nova_ia()

    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]

    msg = f"🧪 <b>LABORATÓRIO DE ESTRATÉGIAS (IA)</b>\n\n{sugestao}"

    for cid in ids: enviar_telegram(token, cid, msg)

def enviar_relatorio_financeiro(token, chat_ids, cenario, lucro, roi, entradas):

    msg = f"💰 <b>RELATÓRIO FINANCEIRO</b>\n\n📊 <b>Cenário:</b> {cenario}\n💵 <b>Lucro Líquido:</b> R$ {lucro:.2f}\n📈 <b>ROI:</b> {roi:.1f}%\n🎟️ <b>Entradas:</b> {entradas}\n\n<i>Cálculo baseado na gestão configurada.</i>"

    enviar_telegram(token, chat_ids, msg)

def enviar_relatorio_bi(token, chat_ids):

    df = st.session_state.get('historico_full', pd.DataFrame())

    if df.empty: return

    try:

        df = df.copy()

        df['Data_Str'] = df['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()

        df['Data_DT'] = pd.to_datetime(df['Data_Str'], errors='coerce')

        df = df.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')

        agora = get_time_br().date()

        hoje = pd.to_datetime(agora)

        d_hoje = df[df['Data_DT'] == hoje]

        d_semana = df[df['Data_DT'] >= (hoje - timedelta(days=7))]

        d_mes = df[df['Data_DT'] >= (hoje - timedelta(days=30))]

        def get_placar_str(d_slice):

            if d_slice.empty: return "Sem dados"

            finalizados = d_slice[d_slice['Resultado'].isin(['✅ GREEN', '❌ RED'])]

            g = finalizados['Resultado'].str.contains('GREEN').sum()

            r = finalizados['Resultado'].str.contains('RED').sum()

            t = g + r

            wr = (g/t*100) if t > 0 else 0

            return f"<b>{g}G - {r}R</b> ({wr:.1f}%)"

        def get_ia_stats(d_slice):

            if 'Opiniao_IA' not in d_slice.columns: return "N/A"

            aprovadas = d_slice[d_slice['Opiniao_IA'] == 'Aprovado']

            return get_placar_str(aprovadas)

        top_strats_txt = ""

        try:

            df_closed = df[df['Resultado'].isin(['✅ GREEN', '❌ RED'])]

            if not df_closed.empty:

                ranking = df_closed.groupby('Estrategia')['Resultado'].apply(lambda x: (x.str.contains('GREEN').sum() / len(x) * 100)).sort_values(ascending=False).head(5)

                lista_top = []

                for strat, wr in ranking.items():

                    qtd = len(df_closed[df_closed['Estrategia'] == strat])

                    lista_top.append(f"▪️ {strat}: {wr:.0f}% ({qtd}j)")

                top_strats_txt = "\n".join(lista_top)

        except: top_strats_txt = "Dados insuficientes"

        insight_text = analisar_bi_com_ia()

        # Banca atual
        banca_bi = st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 100.0))
        banca_ini = st.session_state.get('banca_inicial', 100.0)
        variacao = ((banca_bi - banca_ini) / banca_ini * 100) if banca_ini > 0 else 0
        emoji_banca = "📈" if variacao >= 0 else "📉"

        msg = f"""📊 <b>RELATÓRIO BI DIÁRIO</b>

📆 <b>HOJE:</b>
├─ Geral: {get_placar_str(d_hoje)}
├─ 🤖 IA Aprovados: {get_ia_stats(d_hoje)}

🗓 <b>SEMANA (7d):</b>
├─ Geral: {get_placar_str(d_semana)}
├─ 🤖 IA Aprovados: {get_ia_stats(d_semana)}

📅 <b>MÊS (30d):</b>
├─ Geral: {get_placar_str(d_mes)}

🏦 <b>BANCA:</b>
├─ Inicial: R$ {banca_ini:,.2f}
├─ Atual: R$ {banca_bi:,.2f}
├─ {emoji_banca} Variação: {variacao:+.1f}%

🏆 <b>TOP ESTRATÉGIAS:</b>
{top_strats_txt}

🧠 <b>INSIGHT IA:</b>
{insight_text}

{formatar_resumo_aprendizado()}
"""

        enviar_telegram(token, chat_ids, msg)

    except Exception as e:
        print(f"[BI] ❌ ERRO enviar_relatorio_bi: {e}")
        import traceback; traceback.print_exc()
        try:
            enviar_telegram(token, chat_ids, f"📈 <b>RELATÓRIO BI</b>\n\n⚠️ Relatório simplificado (erro no completo):\n\n{analisar_bi_com_ia()}")
        except:
            enviar_telegram(token, chat_ids, f"📈 <b>RELATÓRIO BI</b>\n\n⚠️ Erro ao gerar relatório: {str(e)[:100]}")

# [FIX] Duplicata removida - usa a versão COM splitting (definida acima)
# enviar_telegram com split de mensagens longas já está definida na linha ~4940

def salvar_snipers_do_texto(texto_ia):

    if not texto_ia or "Sem jogos" in texto_ia: return

    try:

        padrao_jogo = re.findall(r'⚽ Jogo: (.*?)(?:\n|$)', texto_ia)

        for i, jogo_nome in enumerate(padrao_jogo):

            item_sniper = {

                "FID": f"SNIPER_{random.randint(10000, 99999)}",

                "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": "08:00",

                "Liga": "Sniper Matinal", "Jogo": jogo_nome.strip(), "Placar_Sinal": "0x0",

                "Estrategia": "Sniper Matinal", "Resultado": "Pendente",

                "Opiniao_IA": "Sniper", "Probabilidade": "Alta"

            }

            adicionar_historico(item_sniper)

    except: pass

def enviar_multipla_matinal(token, chat_ids, api_key):

    if st.session_state.get('multipla_matinal_enviada'): return

    dados_json, mapa_nomes = gerar_multipla_matinal_ia(api_key)

    if not dados_json or "jogos" not in dados_json: return

    jogos = dados_json['jogos']

    prob = dados_json.get('probabilidade_combinada', '90')

    msg = "🚀 <b>MÚLTIPLA DE SEGURANÇA (IA)</b>\n"

    ids_compostos = []; nomes_compostos = []

    for idx, j in enumerate(jogos):

        icone = ["1️⃣", "2️⃣", "3️⃣"][idx] if idx < 3 else "👉"

        msg += f"\n{icone} <b>Jogo: {j['jogo']}</b>\n🎯 Seleção: Over 0.5 Gols\n📝 Motivo: {j['motivo']}\n"

        ids_compostos.append(str(j['fid'])); nomes_compostos.append(j['jogo'])

    msg += f"\n⚠️ <b>Conclusão:</b> Probabilidade combinada de {prob}%."

    enviar_telegram(token, chat_ids, msg)

    multipla_obj = {"id_unico": f"MULT_{'_'.join(ids_compostos)}", "tipo": "MATINAL", "fids": ids_compostos, "nomes": nomes_compostos, "status": "Pendente", "data": get_time_br().strftime('%Y-%m-%d')}

    if 'multiplas_pendentes' not in st.session_state: st.session_state['multiplas_pendentes'] = []

    st.session_state['multiplas_pendentes'].append(multipla_obj)

    st.session_state['multipla_matinal_enviada'] = True

def enviar_alerta_alternativos(token, chat_ids, api_key):

    if st.session_state.get('alternativos_enviado'): return

    sinais = gerar_analise_mercados_alternativos_ia(api_key)

    if not sinais: return

    for s in sinais:

        msg = f"<b>{s['titulo']}</b>\n\n⚽ <b>{s['jogo']}</b>\n\n🔎 <b>Análise:</b>\n{s['destaque']}\n\n🎯 <b>INDICAÇÃO:</b> {s['indicacao']}"

        if s['tipo'] == 'GOLEIRO': msg += "\n⚠️ <i>Regra: Aposte no 'Goleiro do Time', não no nome do jogador.</i>"

        enviar_telegram(token, chat_ids, msg)

        linha_alvo = "0"

        try: linha_alvo = re.findall(r"[-+]?\d*\.\d+|\d+", s['indicacao'])[0]

        except: pass

        item_alt = {

            "FID": f"ALT_{s['fid']}", "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": "08:05",

            "Liga": "Mercado Alternativo", "Jogo": s['jogo'], "Placar_Sinal": f"Meta: {linha_alvo}",

            "Estrategia": s['titulo'], "Resultado": "Pendente", "Opiniao_IA": "Aprovado", "Probabilidade": "Alta"

        }

        adicionar_historico(item_alt)

        time.sleep(2)

    st.session_state['alternativos_enviado'] = True

def enviar_alavancagem(token, chat_ids, api_key):

    if st.session_state.get('alavancagem_enviada'): return

    lista_dados = gerar_bet_builder_alavancagem(api_key)

    if not lista_dados:

        st.session_state['alavancagem_enviada'] = True; return

    for dados in lista_dados:

        msg = f"💎 <b>{dados['titulo']}</b>\n"

        msg += f"⚽ <b>{dados['jogo']}</b>\n\n"

        msg += "🛠️ <b>CRIAR APOSTA (Combinação):</b>\n"

        for sel in dados['selecoes']: msg += f"✅ {sel}\n"

        msg += f"\n🧠 <b>Motivo IA:</b> {dados['analise_ia']}\n"

        msg += "⚠️ <i>Gestão: Use apenas 'Gordura' (Stake Baixa). Alvo: Odd @3.50+</i>"

        enviar_telegram(token, chat_ids, msg)

        item_alavancagem = {

            "FID": str(dados['fid']), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": "10:00",

            "Liga": "Bet Builder Elite", "Jogo": dados['jogo'], "Placar_Sinal": "Combo Alavancagem",

            "Estrategia": "Alavancagem", "Resultado": "Pendente", "Opiniao_IA": "Aprovado", "Probabilidade": "Alta (Top 3)"

        }

        adicionar_historico(item_alavancagem)

        time.sleep(3)

    st.session_state['alavancagem_enviada'] = True

def verificar_multipla_quebra_empate(jogos_live, token, chat_ids):

    candidatos = []

    for j in jogos_live:

        fid = j['fixture']['id']; stats = st.session_state.get(f"st_{fid}", [])

        if not stats: continue

        tempo = j['fixture']['status']['elapsed'] or 0; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0

        if not (30 <= tempo <= 80) or gh != ga: continue

        try:

            s1 = stats[0]['statistics']; s2 = stats[1]['statistics']

            def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0

            chutes_total = gv(s1, 'Total Shots') + gv(s2, 'Total Shots')

            if chutes_total >= (14 if (gh+ga)==0 else 18):

                candidatos.append({'fid': str(fid), 'jogo': f"{j['teams']['home']['name']} x {j['teams']['away']['name']}", 'placar': f"{gh}x{ga}", 'stats': f"{chutes_total} Chutes", 'tempo': tempo, 'total_gols_ref': (gh+ga)})

        except: pass

    if len(candidatos) >= 2:

        dupla = candidatos[:2]

        id_dupla = f"LIVE_{dupla[0]['fid']}_{dupla[1]['fid']}"

        if id_dupla in st.session_state['multiplas_live_cache']: return

        msg = "🚀 <b>ALERTA: DUPLA QUEBRA-EMPATE</b>\nJogos empatados com alta pressão.\n"

        ids_save = []; nomes_save = []; gols_ref_save = {}

        for d in dupla:

            msg += f"\n⚽ <b>{d['jogo']} ({d['placar']})</b>\n⏰ {d['tempo']}' min | 🔥 {d['stats']}"

            ids_save.append(d['fid']); nomes_save.append(d['jogo']); gols_ref_save[d['fid']] = d['total_gols_ref']

        msg += "\n\n🎯 <b>Indicação:</b> Múltipla Over +0.5 Gols na partida"

        enviar_telegram(token, chat_ids, msg)

        st.session_state['multiplas_live_cache'][id_dupla] = True

        multipla_obj = {"id_unico": id_dupla, "tipo": "LIVE", "fids": ids_save, "nomes": nomes_save, "gols_ref": gols_ref_save, "status": "Pendente", "data": get_time_br().strftime('%Y-%m-%d')}

        if 'multiplas_pendentes' not in st.session_state: st.session_state['multiplas_pendentes'] = []

        st.session_state['multiplas_pendentes'].append(multipla_obj)

def enviar_sniper_por_zonas(token, chat_ids, texto_completo, titulo="🌅 <b>SNIPER MATINAL (IA + DADOS)</b>"):
    """Envia o Sniper dividido por ZONAS (não por chars) para evitar HTML quebrado."""
    import re

    # Headers completos das 6 zonas (Gemini usa estes padrões)
    zone_patterns = [
        r'🔥\s*\*?\*?ZONA DE GOLS',
        r'❄️\s*\*?\*?ZONA DE TRINCHEIRA',
        r'🏆\s*\*?\*?ZONA DE MATCH',
        r'🟨\s*\*?\*?ZONA DE CART',
        r'🏴\s*\*?\*?ZONA DE ESCAN',
        r'🧤\s*\*?\*?ZONA DE DEFESA',
        r'ZONA DE GOLS',
        r'ZONA DE TRINCHEIRA',
        r'ZONA DE MATCH',
        r'ZONA DE CART',
        r'ZONA DE ESCAN',
        r'ZONA DE DEFESA',
    ]

    # Encontra posições dos headers de zona
    posicoes = []
    for pattern in zone_patterns:
        for m in re.finditer(pattern, texto_completo, re.IGNORECASE):
            posicoes.append(m.start())

    partes = []
    if len(posicoes) >= 2:
        posicoes = sorted(set(posicoes))
        # Merge posições muito próximas (< 5 chars = mesmo header)
        merged = [posicoes[0]]
        for p in posicoes[1:]:
            if p - merged[-1] > 10:
                merged.append(p)
        posicoes = merged

        for i in range(len(posicoes)):
            inicio = posicoes[i]
            fim = posicoes[i+1] if i+1 < len(posicoes) else len(texto_completo)
            parte = texto_completo[inicio:fim].strip()
            if len(parte) > 30:
                partes.append(parte)

    # Fallback: divide por linhas duplas se não encontrou zonas
    if not partes:
        blocos = texto_completo.split('\n\n')
        buffer = ""
        for bloco in blocos:
            if len(buffer) + len(bloco) > 3500:
                if buffer.strip(): partes.append(buffer.strip())
                buffer = bloco + "\n\n"
            else:
                buffer += bloco + "\n\n"
        if buffer.strip(): partes.append(buffer.strip())

    # Último fallback: manda tudo
    if not partes:
        partes = [texto_completo]

    # Converte markdown e sanitiza HTML em cada parte
    ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
    for i, parte in enumerate(partes):
        # Converte **bold** para <b>bold</b>
        parte = re.sub(r'\*\*([^\*]+)\*\*', r'<b>\1</b>', parte)
        # Fecha tags HTML abertas
        abertos_b = parte.count('<b>') - parte.count('</b>')
        abertos_i = parte.count('<i>') - parte.count('</i>')
        if abertos_b > 0: parte += '</b>' * abertos_b
        if abertos_i > 0: parte += '</i>' * abertos_i
        if parte.startswith('</b>'): parte = parte[4:]
        if parte.startswith('</i>'): parte = parte[4:]

        header = f"{titulo}\n\n" if i == 0 else ""
        msg = f"{header}{parte}"

        for cid in ids:
            enviar_telegram(token, cid, msg)
        time.sleep(1)

    return len(partes)

def verificar_alerta_matinal(token, chat_ids, api_key):

    agora = get_time_br()

    # 1. Sniper Matinal

    if 6 <= agora.hour < 12:

        if not st.session_state['matinal_enviado']:

            insights = gerar_insights_matinais_ia(api_key)

            if insights and "Sem jogos" not in insights:

                ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]

                msg_final = f"🌅 <b>SNIPER MATINAL (IA + DADOS)</b>\n\n{insights}"

                for cid in ids: enviar_telegram(token, cid, msg_final)

                salvar_snipers_do_texto(insights)

                st.session_state['matinal_enviado'] = True

            else:

                st.session_state['matinal_enviado'] = True

        if st.session_state['matinal_enviado'] and not st.session_state.get('multipla_matinal_enviada', False):

            time.sleep(5); enviar_multipla_matinal(token, chat_ids, api_key)

        if st.session_state['matinal_enviado'] and st.session_state['multipla_matinal_enviada'] and not st.session_state.get('alternativos_enviado', False):

            time.sleep(5); enviar_alerta_alternativos(token, chat_ids, api_key)

        if agora.hour >= 10 and not st.session_state.get('alavancagem_enviada', False):

            time.sleep(5); enviar_alavancagem(token, chat_ids, api_key)

    # 2. SNIPER DA TARDE (12h-14h) — Mesma inteligência, novos jogos
    if 12 <= agora.hour < 15:
        if not st.session_state.get('sniper_tarde_enviado', False):
            insights_tarde = gerar_insights_matinais_ia(api_key)
            if insights_tarde and "Sem jogos" not in insights_tarde:
                ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
                msg_tarde = f"☀️ <b>SNIPER DA TARDE (IA + DADOS)</b>\n\n{insights_tarde}"
                for cid in ids: enviar_telegram(token, cid, msg_tarde)
                salvar_snipers_do_texto(insights_tarde)
                print(f"[SNIPER TARDE] ✅ Enviado!")
            st.session_state['sniper_tarde_enviado'] = True

    # 3. SNIPER DA NOITE (17h-18h) — Jogos noturnos
    if 17 <= agora.hour < 18:
        if not st.session_state.get('sniper_noite_enviado', False):
            insights_noite = gerar_insights_matinais_ia(api_key)
            if insights_noite and "Sem jogos" not in insights_noite:
                ids = [x.strip() for x in str(chat_ids).replace(';', ',').split(',') if x.strip()]
                msg_noite = f"🌙 <b>SNIPER DA NOITE (IA + DADOS)</b>\n\n{insights_noite}"
                for cid in ids: enviar_telegram(token, cid, msg_noite)
                salvar_snipers_do_texto(insights_noite)
                print(f"[SNIPER NOITE] ✅ Enviado!")
            st.session_state['sniper_noite_enviado'] = True

    # 5. [NOVO] TRADING PRÉ-LIVE (DROP ODDS) - COM JANELA ESTENDIDA ATÉ 13:30

    # Se for meio-dia ou se for 13h (até 30min) E ainda não enviou...

    faixa_12h = (agora.hour == 12 or (agora.hour == 13 and agora.minute <= 30))

    # Se for 16h (até 30min) E ainda não enviou...

    faixa_16h = (agora.hour == 16 and agora.minute <= 30)

    # Verifica o trigger da faixa das 12h

    if faixa_12h and not st.session_state.get('drop_enviado_12', False):

        drops = scanner_drop_odds_pre_live(api_key)

        if drops:

            for d in drops:

                msg = f"💰 <b>ESTRATÉGIA CASHOUT (DROP ODDS)</b>\n\n⚽ <b>{d['jogo']}</b>\n🏆 {d['liga']} | ⏰ {d['hora']}\n\n📉 <b>DESAJUSTE:</b>\n• Bet365: <b>@{d['odd_b365']:.2f}</b>\n• Pinnacle: <b>@{d['odd_pinnacle']:.2f}</b>\n• Drop: <b>{d['valor']:.1f}%</b>\n\n⚙️ <b>AÇÃO:</b>\n1️⃣ Compre vitória do <b>{d['lado']}</b>\n2️⃣ <b>+ BANKER:</b> Adicione 'Under 7.5 Gols'\n3️⃣ <b>SAÍDA:</b> Cashout ao igualar Pinnacle."

                enviar_telegram(token, chat_ids, msg)

                item_drop = {"FID": f"DROP_{d['fid']}", "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": "Trading Pré-Live", "Jogo": d['jogo'], "Placar_Sinal": f"Entrada: @{d['odd_b365']}", "Estrategia": "Drop Odds Cashout", "Resultado": "Pendente", "Opiniao_IA": "Aprovado", "Probabilidade": "Técnica"}

                adicionar_historico(item_drop)

        st.session_state['drop_enviado_12'] = True # Marca como enviado para não repetir

    # Verifica o trigger da faixa das 16h

    if faixa_16h and not st.session_state.get('drop_enviado_16', False):

        drops = scanner_drop_odds_pre_live(api_key)

        if drops:

            for d in drops:

                msg = f"💰 <b>ESTRATÉGIA CASHOUT (DROP ODDS)</b>\n\n⚽ <b>{d['jogo']}</b>\n🏆 {d['liga']} | ⏰ {d['hora']}\n\n📉 <b>DESAJUSTE:</b>\n• Bet365: <b>@{d['odd_b365']:.2f}</b>\n• Pinnacle: <b>@{d['odd_pinnacle']:.2f}</b>\n• Drop: <b>{d['valor']:.1f}%</b>\n\n⚙️ <b>AÇÃO:</b>\n1️⃣ Compre vitória do <b>{d['lado']}</b>\n2️⃣ <b>+ BANKER:</b> Adicione 'Under 7.5 Gols'\n3️⃣ <b>SAÍDA:</b> Cashout ao igualar Pinnacle."

                enviar_telegram(token, chat_ids, msg)

                item_drop = {"FID": f"DROP_{d['fid']}", "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": "Trading Pré-Live", "Jogo": d['jogo'], "Placar_Sinal": f"Entrada: @{d['odd_b365']}", "Estrategia": "Drop Odds Cashout", "Resultado": "Pendente", "Opiniao_IA": "Aprovado", "Probabilidade": "Técnica"}

                adicionar_historico(item_drop)

        st.session_state['drop_enviado_16'] = True

    hoje_str = agora.strftime('%Y-%m-%d')

    if st.session_state.get('last_check_date') != hoje_str:

        st.session_state['matinal_enviado'] = False; st.session_state['multipla_matinal_enviada'] = False

        st.session_state['alternativos_enviado'] = False; st.session_state['alavancagem_enviada'] = False

        st.session_state['drop_enviado_12'] = False; st.session_state['drop_enviado_16'] = False

        # Reset Betfair diário
        st.session_state['bf_bets_hoje'] = []; st.session_state['bf_total_apostado'] = 0.0

        st.session_state['last_check_date'] = hoje_str

def check_green_red_hibrido(jogos_live, token, chats, api_key):
    hist = st.session_state['historico_sinais']
    pendentes = [s for s in hist if (s.get('Resultado') or '') == 'Pendente']
    if not pendentes: return

    hoje_str = get_time_br().strftime('%Y-%m-%d')
    updates_buffer = []
    mapa_live = {j['fixture']['id']: j for j in jogos_live}

    for s in pendentes:
        if s.get('Data') != hoje_str: continue
        if "Sniper" in (s.get('Estrategia') or '') or "Alavancagem" in (s.get('Estrategia') or '') or "Drop" in (s.get('Estrategia') or ''): continue
        if "Mercado Alternativo" in (s.get('Liga') or ''): continue

        fid = int(clean_fid(s.get('FID', 0)))
        strat = s.get('Estrategia') or ''

        jogo_api = mapa_live.get(fid)
        if not jogo_api:
             try:
                 res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                 if res['response']:
                     jogo_api = res['response'][0]
                     fb_status = jogo_api['fixture']['status']['short']
                     fb_elapsed = jogo_api['fixture']['status'].get('elapsed', '?')
                     print(f"[CHECK] ⚠️ FID {fid} não estava no live feed. API fallback: status={fb_status}, elapsed={fb_elapsed}'")
             except: pass

        if jogo_api:
            gh = jogo_api['goals']['home'] or 0
            ga = jogo_api['goals']['away'] or 0
            st_short = jogo_api['fixture']['status']['short']
            elapsed = jogo_api['fixture']['status'].get('elapsed') or 0

            # ═══ TRAVA DE SEGURANÇA: Não confirmar resultado se jogo ainda rola ═══
            # Se status diz FT mas elapsed < 85 → API bugou, ignora
            if st_short in ['FT', 'AET', 'PEN'] and elapsed and int(elapsed) < 85:
                print(f"[CHECK] ⚠️ FID {fid}: API diz {st_short} mas elapsed={elapsed}' → Ignorando (bug API)")
                continue
            # Se jogo está em andamento → só permite GREEN antecipado, NUNCA RED
            jogo_em_andamento = st_short in ['1H', '2H', 'HT', 'ET', 'BT', 'P']

            try: ph, pa = map(int, (s.get('Placar_Sinal') or '0x0').split('x'))
            except: ph, pa = 0, 0

            key_sinal = gerar_chave_universal(fid, strat, "SINAL")
            key_green = gerar_chave_universal(fid, strat, "GREEN")
            key_red = gerar_chave_universal(fid, strat, "RED")
            deve_enviar = (key_sinal in st.session_state.get('alertas_enviados', set()))

            res_final = None

            # --- LÓGICA DE APURAÇÃO CORRIGIDA --- [PATCH V5.3]
            tipo_estrategia = classificar_tipo_estrategia(strat)
            res_final = None
            jogo_finalizado = st_short in ['FT', 'AET', 'PEN', 'ABD']

            if tipo_estrategia == 'RESULTADO':
                if jogo_finalizado and not jogo_em_andamento:
                    if ph > pa:
                        res_final = '✅ GREEN' if gh > ga else '❌ RED'
                    elif pa > ph:
                        res_final = '✅ GREEN' if ga > gh else '❌ RED'
                    else:
                        res_final = '✅ GREEN' if gh == ga else '❌ RED'

            elif tipo_estrategia == 'UNDER':
                gols_no_sinal = ph + pa
                gols_atuais = gh + ga
                if gols_atuais > gols_no_sinal:
                    res_final = '❌ RED'  # Under quebrou → RED imediato (correto)
                elif jogo_finalizado and not jogo_em_andamento:
                    res_final = '✅ GREEN' if gols_atuais == gols_no_sinal else '❌ RED'

            elif tipo_estrategia == 'OVER':
                gols_no_sinal = ph + pa
                gols_atuais = gh + ga
                if gols_atuais > gols_no_sinal:
                    res_final = '✅ GREEN'  # Saiu gol → GREEN imediato (correto)
                elif jogo_finalizado and not jogo_em_andamento:
                    res_final = '❌ RED'  # Acabou sem gol → RED só no FT real

            else:
                if (gh + ga) > (ph + pa):
                    res_final = '✅ GREEN'
                elif jogo_finalizado and not jogo_em_andamento:
                    res_final = '❌ RED'

            # --- ENVIO E SALVAMENTO ---
            if res_final:

                # [MELHORIA] Atualiza banca automaticamente após GREEN/RED (anti-duplicação)
                try:
                    if 'banca_updates' not in st.session_state:
                        st.session_state['banca_updates'] = set()
                    key_apura = gerar_chave_universal(fid, strat, "GREEN" if "GREEN" in res_final else "RED")
                    if key_apura not in st.session_state['banca_updates']:
                        st.session_state['banca_updates'].add(key_apura)

                        if 'banca_atual' not in st.session_state:
                            st.session_state['banca_atual'] = float(st.session_state.get('banca_inicial', 1000.0))
                        saldo = float(st.session_state.get('banca_atual', 1000.0))

                        # [FIX] Banca usa Kelly com winrate REAL (histórico), não probabilidade IA
                        stake_val = max(float(st.session_state.get('banca_atual', 100.0)) * 0.01, 1.0)  # Default 1%

                        odd_local = 1.50
                        try:
                            odd_raw_l = str(s.get('Odd','1.50')).replace(',', '.')
                            odd_local = float(odd_raw_l) if odd_raw_l and odd_raw_l.lower() != 'nan' else 1.50
                            if math.isnan(odd_local) or odd_local <= 1.0: odd_local = 1.50
                        except:
                            odd_local = 1.50

                        try:
                            # Usa winrate REAL da estratégia no histórico (não a probabilidade da IA)
                            df_wr = st.session_state.get('historico_full', pd.DataFrame())
                            prob_real = 60.0  # Default conservador
                            if not df_wr.empty:
                                df_strat = df_wr[(df_wr['Estrategia'] == strat) & (df_wr['Resultado'].isin(['✅ GREEN', '❌ RED']))]
                                if len(df_strat) >= 5:
                                    prob_real = (len(df_strat[df_strat['Resultado'].str.contains('GREEN')]) / len(df_strat)) * 100

                            opiniao_banca = str(s.get('Opiniao_IA', 'Neutro'))
                            kelly_modo_b = st.session_state.get('kelly_modo', 'fracionario')
                            banca_para_calc = float(st.session_state.get('banca_atual', 100.0))
                            kelly_b = calcular_stake_recomendado(banca_para_calc, prob_real, odd_local, kelly_modo_b, opiniao_banca)
                            if kelly_b and kelly_b['valor'] > 0:
                                stake_val = kelly_b['valor']
                        except: pass

                        if 'GREEN' in res_final:
                            saldo += stake_val * (odd_local - 1)
                        else:
                            saldo -= stake_val

                        import math
                        if math.isnan(saldo) or math.isinf(saldo):
                            saldo = float(st.session_state.get('banca_inicial', 100.0))
                        st.session_state['banca_atual'] = float(saldo)

                        if 'historico_banca' not in st.session_state:
                            st.session_state['historico_banca'] = []
                        st.session_state['historico_banca'].append({
                            'data': get_time_br().strftime('%Y-%m-%d %H:%M'),
                            'saldo': float(saldo),
                            'resultado': res_final,
                            'stake': float(stake_val),
                            'odd': float(odd_local),
                            'jogo': s.get('Jogo',''),
                            'estrategia': strat,
                            'fid': str(fid)
                        })
                except:
                    pass
                s['Resultado'] = res_final; updates_buffer.append(s)

                if deve_enviar:
                    tipo_msg = "GREEN" if "GREEN" in res_final else "RED"

                    if tipo_msg == "GREEN" and key_green not in st.session_state['alertas_enviados']:
                         try:
                            # GREEN usa Kelly inteligente
                            stake_g = max(float(st.session_state.get('banca_atual', 100.0)) * 0.01, 1.0)
                            try:
                                prob_g = float(str(s.get('Probabilidade', '50')).replace('%','').replace('...','0'))
                                opiniao_g = str(s.get('Opiniao_IA', 'Neutro'))
                                kelly_g = calcular_stake_recomendado(float(st.session_state.get('banca_atual', 100.0)), prob_g, odd_g, st.session_state.get('kelly_modo', 'fracionario'), opiniao_g)
                                if kelly_g and kelly_g['valor'] > 0: stake_g = kelly_g['valor']
                            except: pass
                            odd_g = 1.50
                            try:
                                odd_g_raw = str(s.get('Odd','1.50')).replace(',','.')
                                odd_g = float(odd_g_raw) if odd_g_raw and odd_g_raw.lower() != 'nan' else 1.50
                                if math.isnan(odd_g) or odd_g <= 1.0: odd_g = 1.50
                            except: pass
                            lucro_g = round(stake_g * (odd_g - 1), 2)
                            banca_g = round(float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 100.0))), 2)
                            import math
                            if math.isnan(banca_g) or math.isinf(banca_g): banca_g = round(float(st.session_state.get('banca_inicial', 100.0)), 2)
                            msg_green = f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {s['Jogo']}\n📈 Placar: {gh}x{ga}\n🎯 {strat}\n💰 Lucro: +R$ {lucro_g:.2f} | Banca: R$ {banca_g:,.2f}"
                         except: msg_green = f"✅ <b>GREEN CONFIRMADO!</b>\n⚽ {s['Jogo']}\n📈 Placar: {gh}x{ga}\n🎯 {strat}"
                         enviar_telegram(token, chats, msg_green)
                         st.session_state['alertas_enviados'].add(key_green)

                    elif tipo_msg == "RED" and key_red not in st.session_state['alertas_enviados']:
                         try:
                            # RED usa Kelly inteligente
                            odd_r_local = 1.50
                            try:
                                odd_r_raw = str(s.get('Odd','1.50')).replace(',','.')
                                odd_r_local = float(odd_r_raw) if odd_r_raw and odd_r_raw.lower() != 'nan' else 1.50
                                if math.isnan(odd_r_local) or odd_r_local <= 1.0: odd_r_local = 1.50
                            except: pass
                            stake_r = max(float(st.session_state.get('banca_atual', 100.0)) * 0.01, 1.0)
                            try:
                                prob_r = float(str(s.get('Probabilidade', '50')).replace('%','').replace('...','0'))
                                opiniao_r = str(s.get('Opiniao_IA', 'Neutro'))
                                kelly_r_calc = calcular_stake_recomendado(float(st.session_state.get('banca_atual', 100.0)), prob_r, odd_r_local, st.session_state.get('kelly_modo', 'fracionario'), opiniao_r)
                                if kelly_r_calc and kelly_r_calc['valor'] > 0: stake_r = kelly_r_calc['valor']
                            except: pass
                            banca_r = round(float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 100.0))), 2)
                            import math
                            if math.isnan(banca_r) or math.isinf(banca_r): banca_r = round(float(st.session_state.get('banca_inicial', 100.0)), 2)
                            msg_red = f"❌ <b>RED CONFIRMADO</b>\n⚽ {s['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {strat}\n💰 Perda: -R$ {stake_r:.2f} | Banca: R$ {banca_r:,.2f}"
                         except: msg_red = f"❌ <b>RED CONFIRMADO</b>\n⚽ {s['Jogo']}\n📉 Placar: {gh}x{ga}\n🎯 {strat}"
                         enviar_telegram(token, chats, msg_red)
                         st.session_state['alertas_enviados'].add(key_red)

    if updates_buffer: atualizar_historico_ram(updates_buffer)

def conferir_resultados_sniper(jogos_live, api_key):
    """Apura GREEN/RED dos Snipers (Matinal/Tarde/Noite/Cartões) com base no palpite."""
    hist = st.session_state.get('historico_sinais', [])
    snipers = [s for s in hist if "Sniper" in (s.get('Estrategia') or '') and s.get('Resultado') == "Pendente"]
    if not snipers: return

    updates = []
    ids_live = {str(j['fixture']['id']): j for j in jogos_live}

    for s in snipers:
        fid = str(s.get('FID', ''))

        # Pula snipers sem FID real (ainda com SNIPER_XXXXX do sistema antigo)
        if fid.startswith('SNIPER_') and not fid.replace('SNIPER_', '').isdigit():
            continue
        # Tenta converter FID numérico mesmo que comece com SNIPER_
        if fid.startswith('SNIPER_'):
            fid_num = fid.replace('SNIPER_', '')
            if len(fid_num) <= 5:  # Random ID (10000-99999), não é FID real
                continue
            fid = fid_num

        # Busca jogo
        jogo = ids_live.get(fid)
        if not jogo:
            try:
                r = requests.get("https://v3.football.api-sports.io/fixtures",
                    headers={"x-apisports-key": api_key}, params={"id": fid}).json()
                if r.get('response'): jogo = r['response'][0]
            except: pass

        if not jogo: continue

        status = jogo['fixture']['status']['short']
        if status not in ['FT', 'AET', 'PEN']: continue

        gh = jogo['goals']['home'] or 0
        ga = jogo['goals']['away'] or 0
        total_gols = gh + ga

        tipo = (s.get('Tipo_Apuracao') or '')
        palpite = (s.get('Palpite_Original') or '').lower()
        res_final = None

        # ═══ APURAÇÃO POR TIPO ═══
        if tipo == 'OVER' or 'gol' in (s.get('Estrategia') or '').lower():
            # Extrai linha (Over 2.5, Over 1.5, etc.)
            import re
            linha_match = re.search(r'(\d+\.?\d*)\s*gol', palpite)
            if linha_match:
                linha = float(linha_match.group(1))
                res_final = '✅ GREEN' if total_gols > linha else '❌ RED'
            elif 'ambas' in palpite or 'btts' in palpite:
                res_final = '✅ GREEN' if (gh > 0 and ga > 0) else '❌ RED'
            else:
                # Fallback: Over 0.5 (pelo menos 1 gol)
                res_final = '✅ GREEN' if total_gols > 0 else '❌ RED'

        elif tipo == 'UNDER' or 'under' in (s.get('Estrategia') or '').lower():
            linha_match = re.search(r'(\d+\.?\d*)\s*gol', palpite)
            if linha_match:
                linha = float(linha_match.group(1))
                res_final = '✅ GREEN' if total_gols < linha else '❌ RED'
            else:
                res_final = '✅ GREEN' if total_gols <= 2 else '❌ RED'

        elif tipo == 'RESULTADO' or 'match' in (s.get('Estrategia') or '').lower():
            if 'empate' in palpite:
                res_final = '✅ GREEN' if gh == ga else '❌ RED'
            elif 'casa' in palpite or 'mandante' in palpite or 'home' in palpite:
                res_final = '✅ GREEN' if gh > ga else '❌ RED'
            elif 'fora' in palpite or 'visitante' in palpite or 'away' in palpite:
                res_final = '✅ GREEN' if ga > gh else '❌ RED'
            else:
                # Tenta achar nome do time no palpite
                jogo_nome = s.get('Jogo', '')
                times = jogo_nome.split(' x ')
                if len(times) >= 2:
                    home_name = times[0].strip().lower()
                    away_name = times[1].strip().lower()
                    if home_name in palpite:
                        res_final = '✅ GREEN' if gh > ga else '❌ RED'
                    elif away_name in palpite:
                        res_final = '✅ GREEN' if ga > gh else '❌ RED'

        elif tipo == 'CARTOES':
            # Tenta buscar stats de cartões
            try:
                stats = st.session_state.get(f"st_{fid}", [])
                if stats:
                    def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
                    total_cards = sum(gv(s_t.get('statistics', []), 'Yellow Cards') + gv(s_t.get('statistics', []), 'Red Cards') for s_t in stats)
                    linha_match = re.search(r'(\d+\.?\d*)\s*cart', palpite)
                    if linha_match:
                        linha = float(linha_match.group(1))
                        if 'menos' in palpite or 'under' in palpite:
                            res_final = '✅ GREEN' if total_cards < linha else '❌ RED'
                        else:
                            res_final = '✅ GREEN' if total_cards > linha else '❌ RED'
            except: pass

        elif tipo == 'ESCANTEIOS':
            try:
                stats = st.session_state.get(f"st_{fid}", [])
                if stats:
                    def gv(l, t): return next((x['value'] for x in l if x['type']==t), 0) or 0
                    total_corners = sum(gv(s_t.get('statistics', []), 'Corner Kicks') for s_t in stats)
                    linha_match = re.search(r'(\d+\.?\d*)\s*(?:escan|corner)', palpite)
                    if linha_match:
                        linha = float(linha_match.group(1))
                        if 'menos' in palpite:
                            res_final = '✅ GREEN' if total_corners < linha else '❌ RED'
                        else:
                            res_final = '✅ GREEN' if total_corners > linha else '❌ RED'
            except: pass

        # Fallback genérico (se não conseguiu apurar por tipo)
        if not res_final and not tipo:
            res_final = '✅ GREEN' if total_gols > 0 else '❌ RED'

        if res_final:
            s['Resultado'] = res_final
            updates.append(s)
            print(f"[SNIPER CHECK] {s.get('Jogo','')} | {s.get('Estrategia','')} | {res_final}")

    if updates: atualizar_historico_ram(updates)

def verificar_var_rollback(jogos_live, token, chats):

    if 'var_avisado_cache' not in st.session_state: st.session_state['var_avisado_cache'] = set()

    hist = st.session_state['historico_sinais']

    greens = [s for s in hist if 'GREEN' in str(s.get('Resultado', ''))]

    if not greens: return

    updates = []

    for s in greens:

        if "Morno" in (s.get('Estrategia') or ''): continue

        fid = int(clean_fid(s.get('FID', 0)))

        jogo_api = next((j for j in jogos_live if j['fixture']['id'] == fid), None)

        if jogo_api:

            gh = jogo_api['goals']['home'] or 0; ga = jogo_api['goals']['away'] or 0

            try:

                ph, pa = map(int, (s.get('Placar_Sinal') or '0x0').split('x'))

                if (gh + ga) <= (ph + pa):

                    assinatura_var = f"{fid}_{s.get('Estrategia','')}_{gh}x{ga}"

                    if assinatura_var in st.session_state['var_avisado_cache']: continue

                    s['Resultado'] = 'Pendente'; updates.append(s)

                    key_green = gerar_chave_universal(fid, s.get('Estrategia',''), "GREEN")

                    st.session_state['alertas_enviados'].discard(key_green)

                    st.session_state['var_avisado_cache'].add(assinatura_var)

                    enviar_telegram(token, chats, f"⚠️ <b>VAR ACIONADO | GOL ANULADO</b>\n⚽ {s.get('Jogo','')}\n📉 Placar voltou: <b>{gh}x{ga}</b>")

            except: pass

    if updates: atualizar_historico_ram(updates)

def verificar_automacao_bi(token, chat_ids, stake_padrao):

    agora = get_time_br()
    hoje_str = agora.strftime('%Y-%m-%d')

    # Reset diário
    if st.session_state.get('last_check_date') != hoje_str:
        print(f"[BI] Reset diário: {hoje_str}")
        st.session_state['bi_enviado'] = False; st.session_state['ia_enviada'] = False
        st.session_state['financeiro_enviado'] = False; st.session_state['bigdata_enviado'] = False
        st.session_state['last_check_date'] = hoje_str
        # [APRENDIZADO] Atualiza regras com base no histórico
        try: atualizar_aprendizado()
        except Exception as e: print(f"[APRENDIZADO] Erro no reset: {e}")

    # Debug: mostra estado atual
    if agora.hour == 23:
        print(f"[BI] {agora.strftime('%H:%M')} | bi={st.session_state.get('bi_enviado')} | fin={st.session_state.get('financeiro_enviado')} | bd={st.session_state.get('bigdata_enviado')}")

    # 23:00+ → BI Report
    if agora.hour >= 23 and not st.session_state.get('bi_enviado', False):
        try:
            print(f"[BI] Enviando Relatório BI...")
            enviar_relatorio_bi(token, chat_ids)
            st.session_state['bi_enviado'] = True
            print(f"[BI] ✅ Relatório BI enviado!")
        except Exception as e:
            print(f"[BI] ❌ ERRO BI: {e}")
            import traceback; traceback.print_exc()
            st.session_state['bi_enviado'] = True  # Não tenta de novo

    # 23:10+ → Financeiro
    if agora.hour >= 23 and agora.minute >= 10 and not st.session_state.get('financeiro_enviado', False):
        try:
            print(f"[BI] Enviando Financeiro...")
            analise_fin = analisar_financeiro_com_ia(stake_padrao, st.session_state.get('banca_inicial', 100))
            msg_fin = f"💰 <b>CONSULTORIA FINANCEIRA</b>\n\n{analise_fin}"
            enviar_telegram(token, chat_ids, msg_fin)
            st.session_state['financeiro_enviado'] = True
            print(f"[BI] ✅ Financeiro enviado!")
        except Exception as e:
            print(f"[BI] ❌ ERRO Financeiro: {e}")
            import traceback; traceback.print_exc()
            st.session_state['financeiro_enviado'] = True

    # 23:20+ → Diagnóstico/Estratégias
    if agora.hour >= 23 and agora.minute >= 20 and not st.session_state.get('bigdata_enviado', False):
        try:
            print(f"[BI] Enviando Diagnóstico...")
            enviar_analise_estrategia(token, chat_ids)
            st.session_state['bigdata_enviado'] = True
            print(f"[BI] ✅ Diagnóstico enviado!")
        except Exception as e:
            print(f"[BI] ❌ ERRO Diagnóstico: {e}")
            import traceback; traceback.print_exc()
            st.session_state['bigdata_enviado'] = True

# --- BARRA LATERAL (CONFIGURAÇÕES E BOTÕES MANUAIS) ---

with st.sidebar:

    st.title("❄️ Neves Analytics")

    with st.expander("⚙️ Configurações", expanded=True):

        st.session_state['API_KEY'] = st.text_input("Chave API:", value=st.session_state['API_KEY'], type="password")

        st.session_state['TG_TOKEN'] = st.text_input("Token Telegram:", value=st.session_state['TG_TOKEN'], type="password")

        st.session_state['TG_CHAT'] = st.text_input("Chat IDs:", value=st.session_state['TG_CHAT'])

        INTERVALO = st.slider("Ciclo (s):", 60, 300, 60)

        if st.button("🧹 Limpar Cache"):

            st.cache_data.clear(); carregar_tudo(force=True); st.session_state['last_db_update'] = 0; st.toast("Cache Limpo!")

        # --- BETFAIR EXCHANGE ---
        if BF_AVAILABLE:
            st.markdown("---")
            st.markdown("**🅱️ Betfair Exchange**")
            # Mostra campos de input se secrets não estão configurados
            if not _bf_has_secrets():
                st.session_state['bf_username_input'] = st.text_input("BF Usuário:", value=st.session_state.get('bf_username_input', ''), type="password")
                st.session_state['bf_password_input'] = st.text_input("BF Senha:", value=st.session_state.get('bf_password_input', ''), type="password")
                st.session_state['bf_app_key_input'] = st.text_input("BF App Key:", value=st.session_state.get('bf_app_key_input', ''), type="password")
            st.session_state['bf_auto_bet'] = st.checkbox("🤖 Auto-Bet Betfair", value=st.session_state.get('bf_auto_bet', False))
            if st.session_state['bf_auto_bet']:
                st.session_state['bf_dry_run'] = st.checkbox("🧪 Modo Teste (Dry Run)", value=st.session_state.get('bf_dry_run', True))
                st.session_state['bf_so_aprovados'] = st.checkbox("✅ Só IA Aprovados", value=st.session_state.get('bf_so_aprovados', True))
                st.session_state['bf_max_pct'] = st.slider("💰 % Máx da Banca (Aprovado):", 1, 10, st.session_state.get('bf_max_pct', 5))
                st.session_state['bf_limit_dia'] = st.number_input("📊 Limite Dia (R$):", min_value=10.0, max_value=5000.0, value=st.session_state.get('bf_limit_dia', 100.0), step=10.0)
                bets_hoje = len(st.session_state.get('bf_bets_hoje', []))
                total_dia = st.session_state.get('bf_total_apostado', 0)
                st.caption(f"📈 Hoje: {bets_hoje} apostas | R$ {total_dia:.2f} apostado")
                if not st.session_state.get('bf_dry_run'):
                    st.warning("⚠️ MODO REAL — Apostas serão executadas!")
            if not st.session_state.get('bf_ativo'):
                if st.button("🔗 Conectar Betfair"):
                    with st.spinner("Conectando à Betfair..."):
                        ok = bf_login()
                        if ok:
                            st.success(f"✅ Betfair conectada! Saldo: R$ {st.session_state.get('bf_saldo', 0):.2f}")
                            st.rerun()
                        else:
                            erro = st.session_state.get('bf_erro', 'Erro desconhecido')
                            st.error(f"❌ {erro}")
                if st.session_state.get('bf_erro'):
                    st.caption(f"⚠️ {st.session_state['bf_erro']}")
            else:
                saldo_txt = f"R$ {st.session_state.get('bf_saldo', 0):.2f}" if st.session_state.get('bf_saldo', 0) > 0 else "?"
                st.markdown(f'<span style="color:lime">✅ Betfair Conectada | Saldo: {saldo_txt}</span>', unsafe_allow_html=True)
                if st.button("🔄 Reconectar"):
                    st.session_state['bf_ativo'] = False
                    bf_login()
                    st.rerun()

    with st.expander("🛠️ Ferramentas Manuais", expanded=False):

        if st.button("🌅 Testar Múltipla + Alternativos"):

            with st.spinner("Gerando alertas..."):

                verificar_alerta_matinal(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], st.session_state['API_KEY'])

                st.success("Alertas Matinais Disparados (Se houver)!")

        if st.button("🧠 Pedir Análise do BI"):

            if IA_ATIVADA:

                with st.spinner("🤖 O Consultor Neves está analisando seus dados..."):

                    analise = analisar_bi_com_ia(); st.markdown("### 📝 Relatório do Consultor"); st.info(analise)

            else: st.error("IA Desconectada.")

        if st.button("🧪 Sugerir Novas Estratégias (Dados Reais)"):

            if IA_ATIVADA:

                with st.spinner("🤖 Analisando padrões globais no Big Data..."):

                    sugestao = criar_estrategia_nova_ia(); st.markdown("### 💡 Sugestão da IA"); st.success(sugestao)
                    try:
                        ids_tg = [x.strip() for x in str(st.session_state.get('TG_CHAT','')).replace(';',',').split(',') if x.strip()]
                        msg_sug = f"🧪 <b>NOVAS ESTRATÉGIAS (Dados Reais)</b>\n\n{sugestao}"
                        if len(msg_sug) > 4000: msg_sug = msg_sug[:3990] + "\n\n<i>(...truncado)</i>"
                        for cid in ids_tg: enviar_telegram(st.session_state.get('TG_TOKEN',''), cid, msg_sug)
                        st.toast("📨 Sugestão enviada no Telegram!")
                    except: pass

            else: st.error("IA Desconectada.")

        if st.button("🔧 Diagnóstico Real (Dados)"):

            if IA_ATIVADA:

                with st.spinner("🤖 Cruzando performance real com Big Data..."):

                    sugestao_otimizacao = otimizar_estrategias_existentes_ia()

                    st.markdown("### 🔍 Diagnóstico Real"); st.info(sugestao_otimizacao)
                    # Envia pro Telegram
                    try:
                        ids_tg = [x.strip() for x in str(st.session_state.get('TG_CHAT','')).replace(';',',').split(',') if x.strip()]
                        msg_diag = f"🔍 <b>DIAGNÓSTICO REAL (Dados)</b>\n\n{sugestao_otimizacao}"
                        # Telegram limit 4096 chars
                        if len(msg_diag) > 4000: msg_diag = msg_diag[:3990] + "\n\n<i>(...truncado)</i>"
                        for cid in ids_tg: enviar_telegram(st.session_state.get('TG_TOKEN',''), cid, msg_diag)
                        st.toast("📨 Diagnóstico enviado no Telegram!")
                    except: pass

            else: st.error("IA Desconectada.")

        if st.button("🚀 Gerar Alavancagem (Jogo Único)"):

            if IA_ATIVADA:

                with st.spinner("🤖 Triangulando API + Big Data + Histórico Pessoal..."):

                    enviar_alavancagem(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], st.session_state['API_KEY'])

                    st.success("Análise de Alavancagem Realizada e Salva!")

            else: st.error("IA Desconectada.")

        # [BOTÃO MANUAL DE TRADING]

        st.markdown("---")

        if st.button("📉 Escanear Drop Odds (Estratégia Vídeo)"):

            if IA_ATIVADA:

                with st.spinner("Comparando Bet365 vs Pinnacle..."):

                    drops = scanner_drop_odds_pre_live(st.session_state['API_KEY'])

                    if drops:

                        st.success(f"Encontradas {len(drops)} oportunidades!")

                        for d in drops:

                            st.write(f"⚽ {d['jogo']} | Bet365: {d['odd_b365']} vs Pin: {d['odd_pinnacle']}")

                            msg = f"💰 <b>ESTRATÉGIA CASHOUT (DROP ODDS)</b>\n\n⚽ <b>{d['jogo']}</b>\n📉 <b>DESAJUSTE:</b>\n• Bet365: @{d['odd_b365']}\n• Pinnacle: @{d['odd_pinnacle']}\n• Drop: {d['valor']:.1f}%"

                            enviar_telegram(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], msg)

                    else: st.warning("Nenhum desajuste de odd encontrado nas Ligas Top agora.")

            else: st.error("IA/API necessária.")

        if st.button("🔄 Forçar Backfill (Salvar Jogos Perdidos)"):

            with st.spinner("Buscando na API todos os jogos finalizados hoje..."):

                hoje_real = get_time_br().strftime('%Y-%m-%d')

                todos_jogos_hoje = buscar_agenda_cached(st.session_state['API_KEY'], hoje_real)

                ft_pendentes = [j for j in todos_jogos_hoje if j['fixture']['status']['short'] in ['FT', 'AET', 'PEN'] and str(j['fixture']['id']) not in st.session_state['jogos_salvos_bigdata']]

                if ft_pendentes:

                    st.info(f"Processando {len(ft_pendentes)} jogos...")

                    stats_recuperadas = atualizar_stats_em_paralelo(ft_pendentes, st.session_state['API_KEY'])

                    count_salvos = 0

                    for fid, stats in stats_recuperadas.items():

                        j_obj = next((x for x in ft_pendentes if str(x['fixture']['id']) == str(fid)), None)

                        if j_obj: salvar_bigdata(j_obj, stats)

                        count_salvos += 1

                    st.success(f"✅ Recuperados e Salvos: {count_salvos} jogos!")

                else: st.warning("Nenhum jogo finalizado pendente.")

        if st.button("📊 Enviar Relatório BI"): enviar_relatorio_bi(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT']); st.toast("Relatório Enviado!")

        if st.button("💰 Enviar Relatório Financeiro"):

            if 'last_fin_stats' in st.session_state:

                s = st.session_state['last_fin_stats']

                enviar_relatorio_financeiro(st.session_state['TG_TOKEN'], st.session_state['TG_CHAT'], s['cenario'], s['lucro'], s['roi'], s['entradas'])

                st.toast("Relatório Financeiro Enviado!")

            else: st.error("Abra a aba Financeiro primeiro.")

    with st.expander("💰 Gestão de Banca", expanded=False):

        stake_padrao = st.number_input("Valor da Aposta (R$)", value=st.session_state.get('stake_padrao', 10.0), step=5.0)

        banca_inicial = st.number_input("Banca Inicial (R$)", value=st.session_state.get('banca_inicial', 100.0), step=50.0)

        st.session_state['stake_padrao'] = stake_padrao; st.session_state['banca_inicial'] = banca_inicial

        # Mostra banca atual (tracking)
        banca_real = st.session_state.get('banca_atual', banca_inicial)
        import math
        if math.isnan(banca_real) or math.isinf(banca_real):
            banca_real = banca_inicial
            st.session_state['banca_atual'] = banca_inicial
        cor_banca = "🟢" if banca_real >= banca_inicial else "🔴"
        st.caption(f"{cor_banca} **Banca Atual:** R$ {banca_real:,.2f}")
        if st.button("🔄 Resetar Banca", help="Volta a banca ao valor inicial"):
            st.session_state['banca_atual'] = float(banca_inicial)
            st.session_state['banca_updates'] = set()
            st.session_state['historico_banca'] = []
            st.toast(f"Banca resetada para R$ {banca_inicial:.2f}")

        st.session_state['kelly_modo'] = st.selectbox("💎 Kelly Criterion:", ["conservador", "fracionario", "completo"], index=1, help="Modo de cálculo do stake recomendado")
        st.session_state['ia_profunda_ativada'] = st.checkbox("🧠 IA Tier 2 (H2H/Momentum)", value=st.session_state.get('ia_profunda_ativada', True), help="Só ativa em Ligas Seguras 🛡️. ~5 calls extras/sinal")

    with st.expander("📊 Consumo (API + IA + Tier 2)", expanded=False):
        verificar_reset_diario()
        st.caption("**📶 API-Football**")
        u = st.session_state['api_usage']; perc = min(u['used'] / u['limit'], 1.0) if u['limit'] > 0 else 0
        st.progress(perc); st.caption(f"Utilizado: **{u['used']:,}** / {u['limit']:,} ({perc*100:.1f}%)")
        st.caption("**🤖 Gemini IA**")
        u_ia = st.session_state['gemini_usage']; u_ia['limit'] = 10000
        perc_ia = min(u_ia['used'] / u_ia['limit'], 1.0)
        st.progress(perc_ia); st.caption(f"Requisições: **{u_ia['used']}** / {u_ia['limit']}")
        st.caption("**🧠 IA Tier 2 (H2H/Momentum)**")
        t2_used = st.session_state.get('tier2_calls_hoje', 0)
        st.caption(f"Calls hoje: **{t2_used}** (só ligas 🛡️ — sem limite)")

    st.write("---")

    tg_ok, tg_nome = testar_conexao_telegram(st.session_state['TG_TOKEN'])

    if tg_ok: st.markdown(f'<div class="status-active">✈️ TELEGRAM: CONECTADO ({tg_nome})</div>', unsafe_allow_html=True)

    else: st.markdown(f'<div class="status-error">❌ TELEGRAM: ERRO ({tg_nome})</div>', unsafe_allow_html=True)

    if IA_ATIVADA: st.markdown('<div class="status-active">🤖 IA GEMINI ATIVA</div>', unsafe_allow_html=True)

    else: st.markdown('<div class="status-error">❌ IA DESCONECTADA</div>', unsafe_allow_html=True)

    if db_firestore: st.markdown('<div class="status-active">🔥 FIREBASE CONECTADO</div>', unsafe_allow_html=True)

    else: st.markdown('<div class="status-warning">⚠️ FIREBASE OFFLINE</div>', unsafe_allow_html=True)

    if st.session_state.get('bf_ativo'):
        bf_mode = "🧪 DRY RUN" if st.session_state.get('bf_dry_run') else "💰 REAL"
        bf_saldo_txt = f" | R$ {st.session_state.get('bf_saldo', 0):.2f}" if st.session_state.get('bf_saldo', 0) > 0 else ""
        st.markdown(f'<div class="status-active">🅱️ BETFAIR ({bf_mode}{bf_saldo_txt})</div>', unsafe_allow_html=True)
    elif BF_AVAILABLE:
        st.markdown('<div class="status-warning">⚠️ BETFAIR DESCONECTADA</div>', unsafe_allow_html=True)

    st.write("---")

    st.session_state.ROBO_LIGADO = st.checkbox("🚀 LIGAR ROBÔ", value=st.session_state.ROBO_LIGADO)

    st.markdown("---")

    st.markdown("### ⚠️ Zona de Perigo")

    if st.button("☢️ ZERAR ROBÔ", type="primary", use_container_width=True): st.session_state['confirmar_reset'] = True

    if st.session_state.get('confirmar_reset'):

        st.error("Tem certeza? Isso apaga TODO o histórico.")

        c1, c2 = st.columns(2)

        if c1.button("✅ SIM"):

            st.cache_data.clear()

            st.session_state['historico_full'] = pd.DataFrame(columns=COLS_HIST)

            salvar_aba("Historico", st.session_state['historico_full'])

            st.session_state['confirmar_reset'] = False; st.rerun()

        if c2.button("❌ NÃO"): st.session_state['confirmar_reset'] = False; st.rerun()

# --- FUNÇÃO QUE ESTAVA FALTANDO ---

def validar_multiplas_pendentes(jogos_live, api_key, token, chat_ids):

    if 'multiplas_pendentes' not in st.session_state or not st.session_state['multiplas_pendentes']: return

    pendentes = st.session_state['multiplas_pendentes']

    mapa_live = {str(j['fixture']['id']): j for j in jogos_live}

    for m in pendentes:

        if m['status'] != 'Pendente': continue

        # Verifica se é de hoje

        if m['data'] != get_time_br().strftime('%Y-%m-%d'): continue

        resultados_jogos = []

        placar_final_str = []

        for fid in m['fids']:

            jogo = mapa_live.get(fid)

            # Se não estiver no Live, tenta buscar na API (pode ter acabado)

            if not jogo:

                try:

                    res = requests.get("https://v3.football.api-sports.io/fixtures", headers={"x-apisports-key": api_key}, params={"id": fid}).json()

                    if res.get('response'): jogo = res['response'][0]

                except: pass

            if not jogo:

                resultados_jogos.append("PENDENTE")

                continue

            status_short = jogo['fixture']['status']['short']

            gh = jogo['goals']['home'] or 0

            ga = jogo['goals']['away'] or 0

            total_agora = gh + ga

            # Regra de Green (Matinal vs Quebra-Empate)

            if m['tipo'] == "MATINAL":

                condicao_green = (total_agora >= 1) # Over 0.5

            else:

                # Pega a referência de gols salva (ex: estava 1x1, precisa de mais 1)

                gols_ref = m.get('gols_ref', {}).get(fid, 0)

                condicao_green = (total_agora > gols_ref)

            if condicao_green: resultados_jogos.append("GREEN")

            elif status_short in ['FT', 'AET', 'PEN', 'INT']: resultados_jogos.append("RED")

            else: resultados_jogos.append("PENDENTE")

            placar_final_str.append(f"{gh}x{ga}")

        # Avaliação Final da Múltipla

        if "RED" in resultados_jogos:

            msg = f"❌ <b>RED MÚLTIPLA FINALIZADA</b>\nUma das seleções não bateu.\n📉 Placar Final: {' / '.join(placar_final_str)}"

            enviar_telegram(token, chat_ids, msg)

            m['status'] = "RED"

            # Salva no histórico

            item_save = {"FID": m['id_unico'], "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": "Múltiplas", "Jogo": " + ".join(m['nomes']), "Placar_Sinal": " / ".join(placar_final_str), "Estrategia": f"Múltipla {m['tipo']}", "Resultado": "❌ RED", "HomeID": "", "AwayID": "", "Odd": "", "Odd_Atualizada": "", "Opiniao_IA": "Aprovado", "Probabilidade": "Alta"}

            adicionar_historico(item_save)

        elif "PENDENTE" not in resultados_jogos and all(x == "GREEN" for x in resultados_jogos):

            msg = f"✅ <b>GREEN MÚLTIPLA CONFIRMADO!</b>\nTodas as seleções bateram!\n📈 Placares: {' / '.join(placar_final_str)}"

            enviar_telegram(token, chat_ids, msg)

            m['status'] = "GREEN"

            # Salva no histórico

            item_save = {"FID": m['id_unico'], "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'), "Liga": "Múltiplas", "Jogo": " + ".join(m['nomes']), "Placar_Sinal": " / ".join(placar_final_str), "Estrategia": f"Múltipla {m['tipo']}", "Resultado": "✅ GREEN", "HomeID": "", "AwayID": "", "Odd": "", "Odd_Atualizada": "", "Opiniao_IA": "Aprovado", "Probabilidade": "Alta"}

            adicionar_historico(item_save)

# ----------------------------------

# --- FUNÇÃO DE MERCADOS ALTERNATIVOS (Recuperada) ---

def verificar_mercados_alternativos(api_key):

    """

    Função Auto-Auditável: Confere se os sinais de Cartões e Goleiros bateram.

    """

    hist = st.session_state.get('historico_sinais', [])

    pendentes = [s for s in hist if (s.get('Liga') or '') == 'Mercado Alternativo' and (s.get('Resultado') or '') == 'Pendente']

    if not pendentes: return

    updates_buffer = []

    for s in pendentes:

        try:

            fid_real = str(s['FID']).replace("ALT_", "")

            meta = 0.0

            try: meta = float(str(s['Placar_Sinal']).split(':')[1].strip())

            except: continue

            url = "https://v3.football.api-sports.io/fixtures"

            r = requests.get(url, headers={"x-apisports-key": api_key}, params={"id": fid_real}).json()

            if not r.get('response'): continue

            jogo = r['response'][0]

            status = jogo['fixture']['status']['short']

            if status not in ['FT', 'AET', 'PEN']: continue

            url_stats = "https://v3.football.api-sports.io/fixtures/statistics"

            r_stats = requests.get(url_stats, headers={"x-apisports-key": api_key}, params={"fixture": fid_real}).json()

            if not r_stats.get('response'): continue

            stats_home = r_stats['response'][0]['statistics']

            stats_away = r_stats['response'][1]['statistics']

            def gv(lista, tipo): return next((x['value'] or 0 for x in lista if x['type'] == tipo), 0)

            resultado_final = "❌ RED"

            if "CARTÕES" in (s.get('Estrategia') or '') or "SNIPER" in (s.get('Estrategia') or ''):

                cards_h = gv(stats_home, "Yellow Cards") + gv(stats_home, "Red Cards")

                cards_a = gv(stats_away, "Yellow Cards") + gv(stats_away, "Red Cards")

                total_cards = cards_h + cards_a

                if total_cards > meta: resultado_final = "✅ GREEN"

                s['Placar_Sinal'] = f"Meta: {meta} | Saiu: {total_cards}"

            elif "DEFESAS" in (s.get('Estrategia') or '') or "MURALHA" in (s.get('Estrategia') or ''):

                saves_h = gv(stats_home, "Goalkeeper Saves")

                saves_a = gv(stats_away, "Goalkeeper Saves")

                max_saves = max(saves_h, saves_a)

                if max_saves >= meta: resultado_final = "✅ GREEN"

                s['Placar_Sinal'] = f"Meta: {meta} | Defesas: {max_saves}"

            s['Resultado'] = resultado_final

            updates_buffer.append(s)

        except: pass

    if updates_buffer: atualizar_historico_ram(updates_buffer)

# ----------------------------------------------------

# --- LOOP PRINCIPAL DO ROBÔ ---

if st.session_state.ROBO_LIGADO:

    with placeholder_root.container():

        carregar_tudo()

        s_padrao = st.session_state.get('stake_padrao', 10.0)

        b_inicial = st.session_state.get('banca_inicial', 100.0)

        safe_token = st.session_state.get('TG_TOKEN', '')

        safe_chat = st.session_state.get('TG_CHAT', '')

        safe_api = st.session_state.get('API_KEY', '')

        # Chamada das Automações

        verificar_automacao_bi(safe_token, safe_chat, s_padrao)

        verificar_alerta_matinal(safe_token, safe_chat, safe_api)

        # Betfair keep-alive
        if st.session_state.get('bf_ativo'):
            try: bf_keep_alive()
            except: pass

        ids_black = [normalizar_id(x) for x in st.session_state['df_black']['id'].values]

        df_obs = st.session_state.get('df_vip', pd.DataFrame()); count_obs = len(df_obs)

        df_safe_show = st.session_state.get('df_safe', pd.DataFrame()); count_safe = len(df_safe_show)

        ids_safe = [normalizar_id(x) for x in df_safe_show['id'].values]

        hoje_real = get_time_br().strftime('%Y-%m-%d')

        if 'historico_full' in st.session_state and not st.session_state['historico_full'].empty:

             df_full = st.session_state['historico_full']

             st.session_state['historico_sinais'] = df_full[df_full['Data'] == hoje_real].to_dict('records')[::-1]

        api_error = False; jogos_live = []

        try:

            url = "https://v3.football.api-sports.io/fixtures"

            resp = requests.get(url, headers={"x-apisports-key": safe_api}, params={"live": "all", "timezone": "America/Sao_Paulo"}, timeout=10)

            update_api_usage(resp.headers); res = resp.json()

            raw_live = res.get('response', []) if not res.get('errors') else []

            dict_clean = {j['fixture']['id']: j for j in raw_live}

            jogos_live = list(dict_clean.values())

            api_error = bool(res.get('errors'))

            if api_error and "errors" in res: st.error(f"Detalhe do Erro: {res['errors']}")

        except Exception as e: jogos_live = []; api_error = True; st.error(f"Erro de Conexão: {e}")

        if not api_error:

            # 1. Rotinas Padrão (blindadas para não travar UI)

            try: check_green_red_hibrido(jogos_live, safe_token, safe_chat, safe_api)
            except Exception as e: print(f"[ERRO] check_green_red: {e}")

            try: conferir_resultados_sniper(jogos_live, safe_api)
            except Exception as e: print(f"[ERRO] conferir_sniper: {e}")

            try: verificar_var_rollback(jogos_live, safe_token, safe_chat)
            except Exception as e: print(f"[ERRO] var_rollback: {e}")

            # 2. NOVAS ROTINAS (Múltiplas e Mercados Especiais)

            try: verificar_multipla_quebra_empate(jogos_live, safe_token, safe_chat)
            except Exception as e: print(f"[ERRO] multipla_quebra: {e}")

            try: validar_multiplas_pendentes(jogos_live, safe_api, safe_token, safe_chat)
            except Exception as e: print(f"[ERRO] validar_multiplas: {e}")

            try: verificar_mercados_alternativos(safe_api)
            except Exception as e: print(f"[ERRO] mercados_alt: {e}")

        radar = []; agenda = []; candidatos_multipla = []; ids_no_radar = []

        if not api_error:

            jogos_para_atualizar = []

            agora_dt = datetime.now()

            # Loop de Análise dos Jogos ao Vivo

            for j in jogos_live:

                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']

                if lid in ids_black: continue

                status_short = j['fixture']['status']['short']

                elapsed = j['fixture']['status']['elapsed']

                if status_short not in ['1H', '2H', 'HT', 'ET']: continue

                ult_chk = st.session_state['controle_stats'].get(fid, datetime.min)

                gh = j['goals']['home']; ga = j['goals']['away']

                tempo_jogo = elapsed or 0

                t_esp = 180

                eh_importante = (tempo_jogo <= 20) or (tempo_jogo >= 70 and abs(gh-ga)<=1) or (status_short == 'HT')

                memoria = st.session_state['memoria_pressao'].get(fid, {})

                pressao_recente = (len(memoria.get('h_t', [])) + len(memoria.get('a_t', []))) >= 4

                if eh_importante or pressao_recente: t_esp = 60

                if deve_buscar_stats(tempo_jogo, gh, ga, status_short):

                    if (agora_dt - ult_chk).total_seconds() > t_esp:

                        jogos_para_atualizar.append(j)

            if jogos_para_atualizar:

                novas_stats = atualizar_stats_em_paralelo(jogos_para_atualizar, safe_api)

                for fid_up, stats_up in novas_stats.items():

                        st.session_state['controle_stats'][fid_up] = datetime.now()

                        st.session_state[f"st_{fid_up}"] = stats_up

            # Busca de Agenda e Backfill

            prox = buscar_agenda_cached(safe_api, hoje_real); agora = get_time_br()

            ft_para_salvar = []

            for p in prox:

                try:

                    if p['fixture']['status']['short'] in ['FT', 'AET', 'PEN'] and str(p['fixture']['id']) not in st.session_state['jogos_salvos_bigdata']:

                        ft_para_salvar.append(p)

                except: pass

            if ft_para_salvar:

                lote = random.sample(ft_para_salvar, min(len(ft_para_salvar), 3))

                stats_ft = atualizar_stats_em_paralelo(lote, safe_api)

                for fid, s in stats_ft.items():

                    j_obj = next((x for x in lote if x['fixture']['id'] == fid), None)

                    if j_obj: salvar_bigdata(j_obj, s)

            # Processamento de Estratégias

            STATUS_BOLA_ROLANDO = ['1H', '2H', 'HT', 'ET', 'P', 'BT']

            for j in jogos_live:

                lid = normalizar_id(j['league']['id']); fid = j['fixture']['id']

                if lid in ids_black: continue

                status_short = j['fixture']['status']['short']

                elapsed = j['fixture']['status']['elapsed']

                if status_short not in STATUS_BOLA_ROLANDO: continue

                if (elapsed is None or elapsed == 0) and status_short != 'HT': continue

                nome_liga_show = j['league']['name']

                if lid in ids_safe: nome_liga_show += " 🛡️"

                elif lid in df_obs['id'].values: nome_liga_show += " ⚠️"

                else: nome_liga_show += " ❓"

                ids_no_radar.append(fid)

                tempo = j['fixture']['status']['elapsed'] or 0; st_short = j['fixture']['status']['short']

                home = j['teams']['home']['name']; away = j['teams']['away']['name']

                placar = f"{j['goals']['home']}x{j['goals']['away']}"; gh = j['goals']['home'] or 0; ga = j['goals']['away'] or 0

                if st_short == 'FT': continue

                stats = st.session_state.get(f"st_{fid}", [])

                status_vis = "👁️" if stats else "💤"

                rank_h = None; rank_a = None

                lista_sinais = []

                if stats:

                    lista_sinais = processar(j, stats, tempo, placar, rank_h, rank_a)

                    salvar_safe_league_basic(lid, j['league']['country'], j['league']['name'], tem_tabela=(rank_h is not None))

                    resetar_erros(lid)

                    if st_short == 'HT' and gh == 0 and ga == 0:

                        try:

                            s1 = stats[0]['statistics']; s2 = stats[1]['statistics']

                            v1 = next((x['value'] for x in s1 if x['type']=='Total Shots'), 0) or 0

                            v2 = next((x['value'] for x in s2 if x['type']=='Total Shots'), 0) or 0

                            sg1 = next((x['value'] for x in s1 if x['type']=='Shots on Goal'), 0) or 0

                            sg2 = next((x['value'] for x in s2 if x['type']=='Shots on Goal'), 0) or 0

                            if (v1+v2) > 12 and (sg1+sg2) > 6: candidatos_multipla.append({'fid': fid, 'jogo': f"{home} x {away}", 'stats': f"{v1+v2} Chutes", 'indica': "Over 0.5 FT"})

                        except: pass

                else: gerenciar_erros(lid, j['league']['country'], j['league']['name'], fid)

                # --- BLOCO DE ENVIO ATUALIZADO (LAYOUT EXECUTIVO + WINRATE CORRIGIDO) ---

                if lista_sinais:

                    status_vis = f"✅ {len(lista_sinais)} Sinais"

                    medias_gols = buscar_media_gols_ultimos_jogos(safe_api, j['teams']['home']['id'], j['teams']['away']['id'])

                    dados_50 = analisar_tendencia_50_jogos(safe_api, j['teams']['home']['id'], j['teams']['away']['id'])

                    nota_home = buscar_rating_inteligente(safe_api, j['teams']['home']['id'])

                    nota_away = buscar_rating_inteligente(safe_api, j['teams']['away']['id'])

                    txt_bigdata = consultar_bigdata_cenario_completo(j['teams']['home']['id'], j['teams']['away']['id'])

                    df_sheets = st.session_state.get('historico_full', pd.DataFrame())

                    txt_pessoal = "Neutro"

                    if not df_sheets.empty:

                        f_h = df_sheets[df_sheets['Jogo'].str.contains(home, na=False, case=False)]

                        if len(f_h) > 2:

                            greens = len(f_h[f_h['Resultado'].str.contains('GREEN', na=False)])

                            wr = (greens/len(f_h))*100

                            txt_pessoal = f"Winrate Pessoal com {home}: {wr:.0f}%"

                    txt_history = ""

                    if dados_50:

                        txt_history = (f"API (50j): Casa(Over1.5: {dados_50['home']['over15_ft']}%) | Fora(Over1.5: {dados_50['away']['over15_ft']}%)")

                    extra_ctx = f"""

                    FONTE 1 (API/SofaScore): {txt_history} | Rating: {nota_home}x{nota_away}

                    FONTE 2 (BIG DATA): {txt_bigdata}

                    FONTE 3 (HISTÓRICO PESSOAL): {txt_pessoal}

                    """

                    for s in lista_sinais:

                        prob = "..."

                        liga_safe = j['league']['name'].replace("<", "").replace(">", "").replace("&", "e")

                        home_safe = home.replace("<", "").replace(">", "").replace("&", "e")

                        away_safe = away.replace("<", "").replace(">", "").replace("&", "e")

                        rh = s.get('rh', 0); ra = s.get('ra', 0)

                        uid_normal = gerar_chave_universal(fid, s['tag'], "SINAL")

                        uid_super = f"SUPER_{uid_normal}"

                        ja_enviado_total = False

                        if uid_normal in st.session_state['alertas_enviados']: ja_enviado_total = True

                        if not ja_enviado_total:

                            for item_hist in st.session_state['historico_sinais']:

                                key_hist = gerar_chave_universal(item_hist['FID'], item_hist['Estrategia'], "SINAL")

                                if key_hist == uid_normal:

                                    ja_enviado_total = True; st.session_state['alertas_enviados'].add(uid_normal); break

                        if ja_enviado_total: continue

                        st.session_state['alertas_enviados'].add(uid_normal)

                        odd_atual_str = get_live_odds(fid, safe_api, s['tag'], gh+ga, tempo)

                        try: odd_val = float(odd_atual_str)

                        except: odd_val = 0.0

                        destaque_odd = ""

                        if odd_val >= 1.80:

                            destaque_odd = "\n💎 <b>SUPER ODD DETECTADA! (EV+)</b>"

                            st.session_state['alertas_enviados'].add(uid_super)

                        opiniao_txt = ""; prob_txt = "..."; opiniao_db = "Neutro"

                        if IA_ATIVADA:

                            try:

                                time.sleep(0.2)

                                dados_ia = {'jogo': f"{home} x {away}", 'placar': placar, 'tempo': f"{tempo}'"}

                                time_fav_ia = s.get('favorito', '')

                                opiniao_txt, prob_txt = consultar_ia_gemini(dados_ia, s['tag'], stats, rh, ra, extra_context=extra_ctx, time_favoravel=time_fav_ia, liga_nome=j['league']['name'])

                                if "aprovado" in opiniao_txt.lower(): opiniao_db = "Aprovado"

                                elif "arriscado" in opiniao_txt.lower(): opiniao_db = "Arriscado"

                                else: opiniao_db = "Neutro"

                            except: pass

                        # ═══ IA TIER 2: H2H + MOMENTUM (só ligas seguras + toggle ativo) ═══
                        tier2_h2h = None
                        tier2_momentum = None
                        tier2_votacao = None
                        tier2_smart = None
                        tier2_ativo = False

                        try:
                            tier2_ligado = st.session_state.get('ia_profunda_ativada', False)
                            tier2_budget_ok = True  # Sem limite - consome livremente em ligas seguras
                            liga_segura = str(lid) in ids_safe

                            if tier2_ligado and tier2_budget_ok and liga_segura and IA_ATIVADA:
                                tier2_ativo = True
                                home_id_t2 = j['teams']['home']['id']
                                away_id_t2 = j['teams']['away']['id']

                                # H2H (confrontos diretos) — +1 API call
                                try:
                                    tier2_h2h = ia_h2h_analise(home, away, home_id_t2, away_id_t2, safe_api)
                                    st.session_state['tier2_calls_hoje'] += 1
                                except: tier2_h2h = None

                                # Momentum (forma recente) — +2 API calls
                                try:
                                    tier2_momentum = ia_momentum_analise(home, away, home_id_t2, away_id_t2, safe_api)
                                    st.session_state['tier2_calls_hoje'] += 2
                                except: tier2_momentum = None

                                # Multi-agente votação (3 especialistas) — sem API extra
                                try:
                                    prob_num_t2 = float(str(prob_txt).replace('%','').replace('...','0'))
                                    stats_jogo_t2 = {'chutes_casa': rh, 'chutes_fora': ra, 'tempo': tempo, 'placar': placar}
                                    tier2_votacao = ia_multi_agente_votacao(prob_num_t2, 0, stats_jogo_t2, txt_bigdata, s['tag'])
                                except: tier2_votacao = None

                                # Smart Entry (decisão de timing) — sem API extra
                                try:
                                    mov_t2, var_t2 = rastrear_movimento_odd(fid, s['tag'], odd_val)
                                    mem_t2 = ia_memory_analise(s['tag'], j['league']['name'], home, away)
                                    kelly_t2 = calcular_stake_recomendado(
                                        float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 1000.0))),
                                        prob_num_t2, odd_val, st.session_state.get('kelly_modo', 'fracionario')
                                    )
                                    tier2_smart = ia_smart_entry_decisao(
                                        {'tendencia': mov_t2, 'variacao': var_t2},
                                        tier2_h2h or {},
                                        tier2_momentum or {},
                                        mem_t2 or {},
                                        kelly_t2 or {},
                                    )
                                except: tier2_smart = None
                        except: pass

                        # Calcular Kelly stake ANTES de salvar no histórico
                        kelly_stake_salvar = ""
                        try:
                            # [FIX] Usa winrate REAL da estratégia, não prob_txt da IA
                            df_wr_k = st.session_state.get('historico_full', pd.DataFrame())
                            prob_k = 60.0  # Default conservador
                            if not df_wr_k.empty:
                                df_sk = df_wr_k[(df_wr_k['Estrategia'] == s['tag']) & (df_wr_k['Resultado'].isin(['✅ GREEN', '❌ RED']))]
                                if len(df_sk) >= 5:
                                    prob_k = (len(df_sk[df_sk['Resultado'].str.contains('GREEN')]) / len(df_sk)) * 100
                            if prob_k > 0 and odd_val > 1.0:
                                banca_k = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 100.0)))
                                kelly_m = st.session_state.get('kelly_modo', 'fracionario')
                                kelly_r = calcular_stake_recomendado(banca_k, prob_k, odd_val, kelly_m, opiniao_db)
                                if kelly_r and kelly_r['valor'] > 0:
                                    kelly_stake_salvar = f"R$ {kelly_r['valor']:.2f}"
                        except: pass

                        item = {
                            "FID": str(fid), "Data": get_time_br().strftime('%Y-%m-%d'), "Hora": get_time_br().strftime('%H:%M'),
                            "Liga": j['league']['name'], "Jogo": f"{home} x {away} ({placar})", "Placar_Sinal": placar,
                            "Estrategia": s['tag'], "Resultado": "Pendente",
                            "HomeID": str(j['teams']['home']['id']) if lid in ids_safe else "", "AwayID": str(j['teams']['away']['id']) if lid in ids_safe else "",
                            "Odd": odd_atual_str, "Odd_Atualizada": "", "Opiniao_IA": opiniao_db, "Probabilidade": prob_txt,
                            "Stake_Recomendado_RS": kelly_stake_salvar
                        }

                        if adicionar_historico(item):

                            try:

                                txt_winrate_historico = ""

                                if txt_pessoal != "Neutro": txt_winrate_historico = f" | 👤 {txt_pessoal}"

                                # 1. Cabeçalho com Winrate

                                header_winrate = ""

                                df_h = st.session_state.get('historico_full', pd.DataFrame())

                                if not df_h.empty:

                                    strat_f = df_h[df_h['Estrategia'] == s['tag']]

                                    if len(strat_f) >= 3:

                                        greens_s = len(strat_f[strat_f['Resultado'].str.contains('GREEN', na=False)])

                                        wr_s = (greens_s / len(strat_f)) * 100

                                        header_winrate = f" | 🟢 <b>Strat: {wr_s:.0f}%</b>"

                                if not header_winrate and "Winrate Pessoal" in txt_pessoal:

                                    wr_val = txt_pessoal.split(':')[-1].strip()

                                    header_winrate = f" | 👤 <b>Time: {wr_val}</b>"

                                # Sempre mostra API quando disponível
                                api_pct_header = ""
                                if dados_50:
                                    api_pct_header = f" | 📊 API: {dados_50['home']['over15_ft']}%"
                                if not header_winrate:
                                    header_winrate = api_pct_header
                                elif api_pct_header and "API" not in header_winrate:
                                    header_winrate += api_pct_header

                                # --- DADOS DE MOMENTO E TEXTOS EXTRAS (AQUI ESTÁ A CORREÇÃO) ---

                                texto_momento = "Morno 🧊"

                                if rh > ra: texto_momento = "Pressão Casa 🔥"

                                elif ra > rh: texto_momento = "Pressão Visitante 🔥"

                                elif rh > 2 or ra > 2: texto_momento = "Jogo Aberto ⚡"

                                linha_bd = ""

                                if "MANDANTE" in txt_bigdata: linha_bd = f"• 💾 <b>Big Data:</b> Tendência confirmada.\n"

                                txt_stats_extras = ""

                                try:

                                    # Recuperando visual: Dados + Médias + Raio-X

                                    txt_stats_extras += f"\n📊 <b>Dados:</b> <i>{texto_momento}</i>"

                                    if medias_gols:

                                        txt_stats_extras += f"\n⚽ <b>Médias (10j):</b> Casa {medias_gols['home']} | Fora {medias_gols['away']}"

                                    if dados_50:

                                        txt_stats_extras += f"\n🔎 <b>Raio-X (50 Jogos):</b>\nFreq. Over 1.5: Casa <b>{dados_50['home']['over15_ft']}%</b> | Fora <b>{dados_50['away']['over15_ft']}%</b>"

                                except: pass

                                # ------------------------------------------------

                                # ═══ MONTAGEM DA MENSAGEM (FORMATO COMPACTO MOCKUP) ═══

                                # --- Classificação e Inteligência ---
                                tipo_aposta = classificar_tipo_estrategia(s['tag'])

                                # Semáforo de odd
                                odd_min = obter_odd_minima(s['tag'])
                                if odd_val >= 1.80:
                                    semaforo = "🔥 <b>ODD DE VALOR:</b>"
                                elif odd_val >= odd_min:
                                    semaforo = "✅ <b>ODD BOA:</b>"
                                else:
                                    semaforo = "⚠️ <b>ALERTA: ODD BAIXA</b>"

                                # Movimento de odd
                                mov_status, mov_var = rastrear_movimento_odd(fid, s['tag'], odd_val)
                                mov_emoji = "📈" if "SUBINDO" in mov_status else "📉" if "CAINDO" in mov_status else "➡️"

                                # Kelly stake
                                kelly_info = ""
                                stake_sinal = 0
                                try:
                                    prob_num_kelly = float(str(prob_txt).replace('%','').replace('...','0'))
                                    banca_k = float(st.session_state.get('banca_atual', st.session_state.get('banca_inicial', 100.0)))
                                    kelly_modo = st.session_state.get('kelly_modo', 'fracionario')
                                    kelly_res = calcular_stake_recomendado(banca_k, prob_num_kelly, odd_val, kelly_modo, opiniao_db)
                                    if kelly_res and kelly_res['valor'] > 0:
                                        stake_sinal = kelly_res['valor']
                                        retorno_pot = round(stake_sinal * (odd_val - 1), 2) if odd_val > 1 else 0
                                        kelly_info = f"├─ 💰 <b>Apostar:</b> R$ {stake_sinal:.2f} ({kelly_res['porcentagem']:.1f}%) → Retorno: +R$ {retorno_pot:.2f}\n"
                                except: pass
                                if not kelly_info:
                                    sp = max(float(st.session_state.get('banca_atual', 100.0)) * 0.01, 1.0)
                                    ret = round(sp * (odd_val - 1), 2) if odd_val > 1 else 0
                                    kelly_info = f"├─ 💰 <b>Apostar:</b> R$ {sp:.2f} (1%) → Retorno: +R$ {ret:.2f}\n"

                                # Classificação visual
                                if tipo_aposta == "OVER":
                                    tipo_linha = "⚽ <b>OVER:</b> Sai gol"
                                    emoji_sinal = "✅"
                                elif tipo_aposta == "UNDER":
                                    tipo_linha = "❄️ <b>UNDER:</b> NÃO sai gol"
                                    emoji_sinal = "✅"
                                elif tipo_aposta == "RESULTADO":
                                    tipo_linha = "👴 <b>RESULTADO:</b> Manter vitória"
                                    emoji_sinal = "✅"
                                else:
                                    tipo_linha = "📊 <b>SINAL:</b> Confira a indicação"
                                    emoji_sinal = "✅"

                                # Liga segura?
                                liga_badge = " 🛡️" if str(lid) in ids_safe else ""

                                # Winrate pessoal compacto
                                wr_pessoal = ""
                                try:
                                    df_hp = st.session_state.get('historico_full', pd.DataFrame())
                                    if not df_hp.empty:
                                        sf = df_hp[df_hp['Estrategia'] == s['tag']]
                                        if len(sf) >= 3:
                                            gp = len(sf[sf['Resultado'].str.contains('GREEN', na=False)])
                                            rp = len(sf[sf['Resultado'].str.contains('RED', na=False)])
                                            wp = (gp / len(sf)) * 100 if len(sf) > 0 else 0
                                            wr_pessoal = f" | 📚 {wp:.0f}% ({gp}G/{rp}R)"
                                except: pass

                                # IA compacta
                                ia_emoji = "✅" if opiniao_db == "Aprovado" else "⚠️" if opiniao_db == "Arriscado" else "📊"
                                ia_compacta = f"🤖 <b>IA:</b> {ia_emoji} <b>{opiniao_db.upper()}</b> ({prob_txt})"

                                # Rating
                                rating_line = ""
                                try:
                                    if nota_home and nota_away:
                                        nh = float(nota_home) if nota_home else 0
                                        na_r = float(nota_away) if nota_away else 0
                                        if nh > 0 or na_r > 0:
                                            rating_line = f"⭐ <b>Rating:</b> Casa {nh:.1f} | Fora {na_r:.1f}\n"
                                except: pass

                                # Gols recentes
                                gols_recentes = ""
                                try:
                                    if dados_50:
                                        gols_recentes = f"📈 <b>Gols (50j):</b> Casa {dados_50['home']['over15_ft']}% | Fora {dados_50['away']['over15_ft']}% Over\n"
                                except: pass

                                # Médias
                                medias_line = ""
                                try:
                                    if medias_gols:
                                        medias_line = f"⚽ <b>Médias (10j):</b> Casa {medias_gols['home']} | Fora {medias_gols['away']}\n"
                                except: pass

                                # Intensidade (chutes no gol)
                                intensidade = ""
                                try:
                                    if rh + ra > 0:
                                        ratio = round((rh + ra) / max(tempo, 1) * 45, 1)
                                        fogo = "🔥" if ratio > 1.0 else "🧊" if ratio < 0.5 else "⚡"
                                        intensidade = f" | Intensidade {ratio} {fogo}"
                                except: pass

                                # ═══ MENSAGEM COMPACTA (~18 linhas) ═══
                                msg = f"{emoji_sinal} <b>SINAL {s['tag'].upper()}</b>{header_winrate}\n"
                                msg += f"🏆 {liga_safe}{liga_badge}\n"
                                msg += f"⚽ <b>{home_safe} 🆚 {away_safe}</b>\n"
                                msg += f"⏰ {tempo}' min | 🥅 Placar: {placar}\n\n"
                                msg += f"{tipo_linha}\n"
                                msg += f"{semaforo} @{odd_atual_str} {mov_emoji}\n"
                                msg += "────────────────\n"
                                msg += f"{s['ordem']}\n\n"
                                msg += f"📊 {texto_momento}\n"
                                msg += rating_line
                                msg += gols_recentes
                                msg += medias_line
                                msg += linha_bd
                                # IA COMPACTA + TIER 2 (se disponível)
                                msg += f"\n{ia_compacta}{intensidade}\n"

                                # H2H compacto
                                h2h_line = ""
                                if tier2_h2h and tier2_h2h.get('tem_dados'):
                                    h2h_line = f"├─ 📊 <b>H2H:</b> {tier2_h2h.get('total_jogos',0)}j | Over {tier2_h2h.get('pct_over',0):.0f}% | BTTS {tier2_h2h.get('pct_btts',0):.0f}%\n"
                                    msg += h2h_line

                                # Momentum compacto
                                if tier2_momentum and tier2_momentum.get('tem_dados', {}) != False:
                                    try:
                                        mom_casa = tier2_momentum.get('casa', {})
                                        mom_fora = tier2_momentum.get('fora', {})
                                        if isinstance(mom_casa, dict) and isinstance(mom_fora, dict):
                                            seq_c = mom_casa.get('sequencia', '?')
                                            seq_f = mom_fora.get('sequencia', '?')
                                            mom_c = mom_casa.get('momentum', '?')
                                            mom_f = mom_fora.get('momentum', '?')
                                            emoji_mc = "🚀" if mom_c == "subindo" else "📉" if mom_c == "caindo" else "⚖️"
                                            emoji_mf = "🚀" if mom_f == "subindo" else "📉" if mom_f == "caindo" else "⚖️"
                                            msg += f"├─ 🌡️ <b>Casa:</b> {seq_c} ({emoji_mc}) | <b>Fora:</b> {seq_f} ({emoji_mf})\n"
                                    except: pass

                                # Votação multi-agente
                                if tier2_votacao and tier2_votacao.get('consenso') != 'erro':
                                    voto_emoji = tier2_votacao.get('emoji', '❓')
                                    voto_count = f"{tier2_votacao.get('aprovar',0)}/3"
                                    msg += f"├─ 🗳️ <b>Consenso:</b> {voto_emoji} {voto_count} Favorável{wr_pessoal}\n"
                                else:
                                    msg += f"├─ 🗳️ <b>Consenso:</b> {ia_emoji}{wr_pessoal}\n"

                                msg += kelly_info

                                # Smart Entry (decisão de timing)
                                if tier2_smart and tier2_smart.get('decisao') not in [None, 'ERRO']:
                                    dec = tier2_smart['decisao']
                                    conf = tier2_smart.get('confianca', 0)
                                    dec_emoji = "⭐" if "ENTRAR" in str(dec).upper() else "⏳" if "AGUARD" in str(dec).upper() else "🔒"
                                    msg += f"└─ 🔮 <b>Decisão:</b> {dec_emoji} {dec} ({conf} pts)\n"
                                else:
                                    msg += f"└─ 🚪 <b>Saída:</b> 🔒 Segurar até o final\n"

                                # Alerta odd baixa
                                if odd_val < odd_min:
                                    msg += f"\n⚠️ <i>Odd abaixo do mínimo @{odd_min:.2f}. Cautela.</i>\n"

                                # [APRENDIZADO] Verifica regras aprendidas
                                try:
                                    deve_ap, motivo_ap = aplicar_filtro_aprendizado(s['tag'], liga_safe, odd_val, opiniao_db)
                                    if not deve_ap:
                                        msg += f"\n🧠 <i>Aprendizado: {motivo_ap}</i>\n"
                                        print(f"[APRENDIZADO] ⚠️ {motivo_ap}")
                                except: pass

                                # [REGRA DE OURO] SEMPRE ENVIA O SINAL (IA nunca veta)
                                if opiniao_db == "Arriscado":
                                    msg += "\n👀 <i>Obs: Risco moderado detectado pela IA.</i>"
                                enviar_telegram(safe_token, safe_chat, msg)
                                sent_status = True

                                # ═══ BETFAIR AUTO-BET ═══
                                try:
                                    if st.session_state.get('bf_ativo') and st.session_state.get('bf_auto_bet'):
                                        bf_executar_aposta_sinal(
                                            home, away, s['tag'], s.get('ordem', ''),
                                            odd_val, stake_sinal, opiniao_db,
                                            safe_token, safe_chat
                                        )
                                except Exception as e_bf:
                                    print(f"[BETFAIR] ❌ Erro no auto-bet: {e_bf}")
                                if opiniao_db == "Aprovado":
                                    st.toast(f"✅ Sinal Enviado: {s['tag']} (IA Favorável)")
                                elif opiniao_db == "Arriscado":
                                    st.toast(f"⚠️ Sinal Enviado: {s['tag']} (IA Dividida)")
                                else:
                                    st.toast(f"📤 Sinal Enviado: {s['tag']}")

                            except Exception as e: print(f"Erro ao enviar sinal: {e}")

                        elif uid_super not in st.session_state['alertas_enviados'] and odd_val >= 1.80:

                             st.session_state['alertas_enviados'].add(uid_super)

                             msg_super = (f"💎 <b>OPORTUNIDADE DE VALOR!</b>\n\n⚽ {home} 🆚 {away}\n📈 <b>A Odd subiu!</b> Entrada valorizada.\n🔥 <b>Estratégia:</b> {s['tag']}\n💰 <b>Nova Odd: @{odd_atual_str}</b>")

                             enviar_telegram(safe_token, safe_chat, msg_super)

                radar.append({"Liga": nome_liga_show, "Jogo": f"{home} {placar} {away}", "Tempo": f"{tempo}'", "Status": status_vis})

            if candidatos_multipla:

                novos = [c for c in candidatos_multipla if c['fid'] not in st.session_state['multiplas_enviadas']]

                if novos:

                    msg = "<b>🚀 OPORTUNIDADE DE MÚLTIPLA (HT) 🚀</b>\n" + "".join([f"\n⚽ {c['jogo']} ({c['stats']})\n⚠️ AÇÃO: {c['indica']}" for c in novos])

                    for c in novos: st.session_state['multiplas_enviadas'].add(c['fid'])

                    enviar_telegram(safe_token, safe_chat, msg)

            for p in prox:

                try:

                    if str(p['league']['id']) not in ids_black and p['fixture']['status']['short'] in ['NS', 'TBD'] and p['fixture']['id'] not in ids_no_radar:

                        if datetime.fromisoformat(p['fixture']['date']) > agora:

                            l_id = normalizar_id(p['league']['id']); l_nm = p['league']['name']

                            if l_id in ids_safe: l_nm += " 🛡️"

                            elif l_id in df_obs['id'].values: l_nm += " ⚠️"

                            agenda.append({"Hora": p['fixture']['date'][11:16], "Liga": l_nm, "Jogo": f"{p['teams']['home']['name']} vs {p['teams']['away']['name']}"})

                except: pass

        if st.session_state.get('precisa_salvar'):

            if 'historico_full' in st.session_state and not st.session_state['historico_full'].empty:

                st.caption("⏳ Sincronizando dados pendentes...")

                salvar_aba("Historico", st.session_state['historico_full'])

        if api_error: st.markdown('<div class="status-error">🚨 API LIMITADA - AGUARDE</div>', unsafe_allow_html=True)

        else: st.markdown('<div class="status-active">🟢 MONITORAMENTO ATIVO</div>', unsafe_allow_html=True)

        hist_hj = pd.DataFrame(st.session_state['historico_sinais'])

        t, g, r, w = calcular_stats(hist_hj)

        c1, c2, c3 = st.columns(3)

        c1.markdown(f'<div class="metric-box"><div class="metric-title">Sinais Hoje</div><div class="metric-value">{t}</div><div class="metric-sub">{g} Green | {r} Red</div></div>', unsafe_allow_html=True)

        c2.markdown(f'<div class="metric-box"><div class="metric-title">Jogos Live</div><div class="metric-value">{len(radar)}</div><div class="metric-sub">Monitorando</div></div>', unsafe_allow_html=True)

        cor_winrate = "#00FF00" if w >= 50 else "#FFFF00"

        c3.markdown(f'<div class="metric-box"><div class="metric-title">Assertividade Dia</div><div class="metric-value" style="color:{cor_winrate};">{w:.1f}%</div><div class="metric-sub">Winrate Diário</div></div>', unsafe_allow_html=True)

        st.write("")

        abas = st.tabs([f"📡 Radar ({len(radar)})", f"📅 Agenda ({len(agenda)})", f"💰 Financeiro", f"📜 Histórico ({len(hist_hj)})", "📈 BI & Analytics", f"🚫 Blacklist ({len(st.session_state['df_black'])})", f"🛡️ Seguras ({count_safe})", f"⚠️ Obs ({count_obs})", "💾 Big Data", "💬 Chat IA", "📉 Trading"])

        with abas[0]:

            if radar: st.dataframe(pd.DataFrame(radar)[['Liga', 'Jogo', 'Tempo', 'Status']].astype(str), use_container_width=True, hide_index=True)

            else: st.info("Buscando jogos...")

        with abas[1]:

            if agenda: st.dataframe(pd.DataFrame(agenda).sort_values('Hora').astype(str), use_container_width=True, hide_index=True)

            else: st.caption("Sem jogos futuros hoje.")

        with abas[2]:

            st.markdown("### 💰 Evolução Financeira")

            c_fin1, c_fin2 = st.columns(2)

            stake_padrao = c_fin1.number_input("Valor da Aposta (Stake):", value=st.session_state.get('stake_padrao', 10.0), step=5.0)

            banca_inicial = c_fin2.number_input("Banca Inicial:", value=st.session_state.get('banca_inicial', 100.0), step=50.0)

            st.session_state['stake_padrao'] = stake_padrao; st.session_state['banca_inicial'] = banca_inicial

            modo_simulacao = st.radio("Cenário de Entrada:", ["Todos os sinais", "Apenas 1 sinal por jogo", "Até 2 sinais por jogo"], horizontal=True)

            filtrar_ia = st.checkbox("🤖 Somente Sinais APROVADOS pela IA", value=True, help="Recomendado: mostra apenas os sinais que você realmente entrou")

            df_fin = st.session_state.get('historico_full', pd.DataFrame())

            if not df_fin.empty:

                df_fin = df_fin.copy()

                df_fin['Odd_Calc'] = df_fin.apply(lambda row: obter_odd_final_para_calculo(row['Odd'], row['Estrategia']), axis=1)

                df_fin = df_fin[df_fin['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()

                df_fin = df_fin.sort_values(by=['FID', 'Hora'], ascending=[True, True])

                if filtrar_ia and 'Opiniao_IA' in df_fin.columns: df_fin = df_fin[df_fin['Opiniao_IA'] == 'Aprovado']

                if modo_simulacao == "Apenas 1 sinal por jogo": df_fin = df_fin.groupby('FID').head(1)

                elif modo_simulacao == "Até 2 sinais por jogo": df_fin = df_fin.groupby('FID').head(2)

                if not df_fin.empty:

                    lucros = []; saldo_atual = banca_inicial; historico_saldo = [banca_inicial]; qtd_greens = 0; qtd_reds = 0

                    for idx, row in df_fin.iterrows():

                        res = row['Resultado']; odd = row['Odd_Calc']

                        if 'GREEN' in res: lucro = (stake_padrao * odd) - stake_padrao; qtd_greens += 1

                        else: lucro = -stake_padrao; qtd_reds += 1

                        saldo_atual += lucro; lucros.append(lucro); historico_saldo.append(saldo_atual)

                    df_fin['Lucro'] = lucros; total_lucro = sum(lucros)

                    roi = (total_lucro / (len(df_fin) * stake_padrao)) * 100

                    st.session_state['last_fin_stats'] = {'cenario': modo_simulacao, 'lucro': total_lucro, 'roi': roi, 'entradas': len(df_fin)}

                    m1, m2, m3, m4 = st.columns(4)

                    cor_delta = "normal" if total_lucro >= 0 else "inverse"

                    m1.metric("Banca Atual", f"R$ {saldo_atual:.2f}")

                    m2.metric("Lucro Líquido", f"R$ {total_lucro:.2f}", delta=f"{roi:.1f}%", delta_color=cor_delta)

                    m3.metric("Entradas", len(df_fin))

                    m4.metric("Winrate", f"{(qtd_greens/len(df_fin)*100):.1f}%")

                    fig_fin = px.line(y=historico_saldo, x=range(len(historico_saldo)), title="Crescimento da Banca (Realista)")

                    fig_fin.update_layout(xaxis_title="Entradas", yaxis_title="Saldo (R$)", template="plotly_dark")

                    fig_fin.add_hline(y=banca_inicial, line_dash="dot", annotation_text="Início", line_color="gray")

                    st.plotly_chart(fig_fin, use_container_width=True)

                else: st.info("Aguardando fechamento de sinais.")

            else: st.info("Sem dados históricos.")

        with abas[3]:

            if not hist_hj.empty:

                df_show = hist_hj.copy()

                if 'Jogo' in df_show.columns and 'Placar_Sinal' in df_show.columns: df_show['Jogo'] = df_show['Jogo'] + " (" + df_show['Placar_Sinal'].astype(str) + ")"

                colunas_esconder = ['FID', 'HomeID', 'AwayID', 'Data_Str', 'Data_DT', 'Odd_Atualizada', 'Placar_Sinal', 'Is_Green', 'Is_Red']

                cols_view = [c for c in df_show.columns if c not in colunas_esconder]

                st.dataframe(df_show[cols_view], use_container_width=True, hide_index=True)

            else: st.caption("Vazio.")

        with abas[4]:

            st.markdown("### 📊 Inteligência de Mercado (V4 - Drill Down)")

            df_bi = st.session_state.get('historico_full', pd.DataFrame())

            if df_bi.empty: st.warning("Sem dados históricos.")

            else:

                try:

                    df_bi = df_bi.copy()

                    df_bi['Data_Str'] = df_bi['Data'].astype(str).str.replace(' 00:00:00', '', regex=False).str.strip()

                    df_bi['Data_DT'] = pd.to_datetime(df_bi['Data_Str'], errors='coerce')

                    df_bi = df_bi.drop_duplicates(subset=['FID', 'Estrategia'], keep='last')

                    hoje = pd.to_datetime(get_time_br().date())

                    d_hoje = df_bi[df_bi['Data_DT'] == hoje]

                    d_7d = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=7))]

                    d_30d = df_bi[df_bi['Data_DT'] >= (hoje - timedelta(days=30))]

                    filtro = st.selectbox("📅 Período", ["Tudo", "Hoje", "7 Dias", "30 Dias"], key="bi_select")

                    if filtro == "Hoje": df_show = d_hoje

                    elif filtro == "7 Dias": df_show = d_7d

                    elif filtro == "30 Dias": df_show = d_30d

                    else: df_show = df_bi

                    if not df_show.empty:

                        if 'Probabilidade' in df_show.columns:

                            prob_min = st.slider("🎯 Filtrar Probabilidade Mínima IA (%)", 0, 100, 0)

                            if prob_min > 0:

                                def limpar_prob(x):

                                    try: return int(str(x).replace('%', ''))

                                    except: return 0

                                df_show = df_show[df_show['Probabilidade'].apply(limpar_prob) >= prob_min]

                        gr = df_show['Resultado'].str.contains('GREEN').sum(); rd = df_show['Resultado'].str.contains('RED').sum(); tt = len(df_show); ww = (gr/tt*100) if tt>0 else 0

                        m1, m2, m3, m4 = st.columns(4)

                        m1.metric("Sinais", tt); m2.metric("Greens", gr); m3.metric("Reds", rd); m4.metric("Assertividade", f"{ww:.1f}%")

                        st.divider()

                        st.markdown("### 🏆 Melhores e Piores Ligas (Com Detalhe de Estratégia)")

                        df_finished = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]

                        if not df_finished.empty:

                            stats_ligas = df_finished.groupby('Liga')['Resultado'].apply(lambda x: pd.Series({'Winrate': (x.str.contains('GREEN').sum() / len(x) * 100), 'Total': len(x), 'Reds': x.str.contains('RED').sum(), 'Greens': x.str.contains('GREEN').sum()})).unstack()

                            stats_ligas = stats_ligas[stats_ligas['Total'] >= 2]

                            def get_top_strat(liga):

                                d_l = df_finished[df_finished['Liga'] == liga]

                                if d_l.empty: return "-"

                                s = d_l.groupby('Estrategia')['Resultado'].apply(lambda x: x.str.contains('GREEN').sum()/len(x)).sort_values(ascending=False)

                                return f"{s.index[0]} ({s.iloc[0]*100:.0f}%)"

                            def get_worst_strat(liga):

                                d_l = df_finished[(df_finished['Liga'] == liga) & (df_finished['Resultado'].str.contains('RED'))]

                                if d_l.empty: return "Nenhuma"

                                s = d_l['Estrategia'].value_counts()

                                return f"{s.index[0]} ({s.iloc[0]} Reds)"

                            col_top, col_worst = st.columns(2)

                            with col_top:

                                st.caption("🌟 Top Ligas (Mais Lucrativas)")

                                top_ligas = stats_ligas.sort_values(by=['Winrate', 'Total'], ascending=[False, False]).head(10)

                                top_ligas['Top Estratégia'] = top_ligas.index.map(get_top_strat)

                                st.dataframe(top_ligas[['Winrate', 'Total', 'Top Estratégia']].style.format({'Winrate': '{:.0f}%', 'Total': '{:.0f}'}), use_container_width=True)

                            with col_worst:

                                st.caption("💀 Ligas Críticas (Mais Reds)")

                                worst_ligas = stats_ligas.sort_values(by=['Reds'], ascending=False).head(10)

                                worst_ligas['Pior Estratégia'] = worst_ligas.index.map(get_worst_strat)

                                st.dataframe(worst_ligas[['Reds', 'Total', 'Pior Estratégia']].style.format({'Reds': '{:.0f}', 'Total': '{:.0f}'}), use_container_width=True)

                        st.divider()

                        st.markdown("### 🧠 Auditoria da IA")

                        if 'Opiniao_IA' in df_show.columns:

                            df_audit = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])].copy()

                            df_audit = df_audit[df_audit['Opiniao_IA'].isin(['Aprovado', 'Arriscado', 'Sniper'])]

                            if not df_audit.empty:

                                pivot = pd.crosstab(df_audit['Opiniao_IA'], df_audit['Resultado'], margins=False)

                                if '✅ GREEN' not in pivot.columns: pivot['✅ GREEN'] = 0

                                if '❌ RED' not in pivot.columns: pivot['❌ RED'] = 0

                                pivot['Total'] = pivot['✅ GREEN'] + pivot['❌ RED']

                                pivot['Winrate %'] = (pivot['✅ GREEN'] / pivot['Total'] * 100)

                                st.dataframe(pivot.style.format({'Winrate %': '{:.2f}%'}).highlight_max(axis=0, color='#1F4025'), use_container_width=True)

                        st.markdown("### 📈 Estratégias")

                        st_s = df_show[df_show['Resultado'].isin(['✅ GREEN', '❌ RED'])]

                        if not st_s.empty:

                            resumo_strat = st_s.groupby(['Estrategia', 'Resultado']).size().unstack(fill_value=0)

                            if '✅ GREEN' in resumo_strat.columns:

                                resumo_strat['Winrate'] = (resumo_strat['✅ GREEN'] / (resumo_strat.get('✅ GREEN',0) + resumo_strat.get('❌ RED',0)) * 100)

                                st.dataframe(resumo_strat.sort_values('Winrate', ascending=False).style.format({'Winrate': '{:.2f}%'}), use_container_width=True)

                except Exception as e: st.error(f"Erro BI: {e}")

        with abas[5]: st.dataframe(st.session_state['df_black'][['País', 'Liga', 'Motivo']], use_container_width=True, hide_index=True)

        with abas[6]:

            df_safe_show = st.session_state.get('df_safe', pd.DataFrame()).copy()

            if not df_safe_show.empty:

                df_safe_show['Status Risco'] = df_safe_show['Strikes'].apply(lambda x: "🟢 100% Estável" if str(x)=='0' else f"⚠️ Atenção ({x}/10)")

                st.dataframe(df_safe_show[['País', 'Liga', 'Motivo', 'Status Risco']], use_container_width=True, hide_index=True)

            else: st.info("Nenhuma liga segura ainda.")

        with abas[7]:

            df_vip_show = st.session_state.get('df_vip', pd.DataFrame()).copy()

            if not df_vip_show.empty:

                df_vip_show['Strikes'] = df_vip_show['Strikes'].apply(formatar_inteiro_visual)

                st.dataframe(df_vip_show[['País', 'Liga', 'Data_Erro', 'Strikes']], use_container_width=True, hide_index=True)

            else: st.info("Nenhuma observação no momento.")

        with abas[8]:

            st.markdown(f"### 💾 Banco de Dados de Partidas (Firebase)")

            st.caption("A IA usa esses dados para criar novas estratégias. Os dados são salvos na nuvem.")

            if db_firestore:

                col_fb1, col_fb2 = st.columns([1, 3])

                if col_fb1.button("🔄 Carregar/Atualizar Tabela"):

                    try:

                        with st.spinner("Baixando dados do Firebase..."):

                            try:

                                count_query = db_firestore.collection("BigData_Futebol").count(); res_count = count_query.get()

                                st.session_state['total_bigdata_count'] = res_count[0][0].value

                            except: pass

                            docs = db_firestore.collection("BigData_Futebol").order_by("data_hora", direction=firestore.Query.DESCENDING).limit(50).stream()

                            data = [d.to_dict() for d in docs]

                            st.session_state['cache_firebase_view'] = data

                            st.toast(f"Dados atualizados!")

                    except Exception as e: st.error(f"Erro ao ler Firebase: {e}")

                if st.session_state.get('total_bigdata_count', 0) > 0: st.metric("Total de Jogos Armazenados", st.session_state['total_bigdata_count'])

                if 'cache_firebase_view' in st.session_state and st.session_state['cache_firebase_view']:

                    st.success(f"📂 Visualizando {len(st.session_state['cache_firebase_view'])} registros (Cache Local)")

                    st.dataframe(pd.DataFrame(st.session_state['cache_firebase_view']), use_container_width=True)

                else: st.info("ℹ️ Clique no botão acima para visualizar os dados salvos.")

            else: st.warning("⚠️ Firebase não conectado.")

        with abas[9]:

            st.markdown("### 💬 Chat Intelligence (Auditor de Algoritmo)")

            if "messages" not in st.session_state:

                st.session_state["messages"] = [{"role": "assistant", "content": "Olá! Estou pronto para auditar seu código. Se houver Reds, me avise que eu gero a correção."}]

            if len(st.session_state["messages"]) > 6:

                st.session_state["messages"] = st.session_state["messages"][-6:]

            for msg in st.session_state.messages:

                st.chat_message(msg["role"]).write(msg["content"])

            if prompt := st.chat_input("Ex: Crie um filtro para evitar o Red de hoje."):

                if not IA_ATIVADA: st.error("IA Desconectada. Verifique a API Key.")

                else:

                    st.session_state.messages.append({"role": "user", "content": prompt})

                    st.chat_message("user").write(prompt)

                    txt_radar = "RADAR VAZIO."

                    if radar: txt_radar = pd.DataFrame(radar).to_string(index=False)

                    txt_hoje = "SEM DADOS HOJE."

                    if 'historico_sinais' in st.session_state and st.session_state['historico_sinais']:

                        df_hj = pd.DataFrame(st.session_state['historico_sinais'])

                        cols = ['Liga', 'Jogo', 'Estrategia', 'Resultado', 'Placar_Sinal', 'Opiniao_IA']

                        cols_exist = [c for c in cols if c in df_hj.columns]

                        txt_hoje = df_hj[cols_exist].head(20).to_string(index=False)

                    txt_bigdata = "BIG DATA OFFLINE."

                    total_bd = st.session_state.get('total_bigdata_count', 0)

                    dados_bd = st.session_state.get('cache_firebase_view', [])

                    if dados_bd or total_bd > 0:

                         txt_bigdata = f"TOTAL REAL DE JOGOS NO BANCO: {total_bd} JOGOS.\n"

                         txt_bigdata += "Amostra visual (últimos 50): \n"

                         try:

                             df_bd = pd.DataFrame(dados_bd)

                             if 'estatisticas' in df_bd.columns:

                                 txt_bigdata += f"Exemplo de Estrutura de Dados: {df_bd.iloc[0]['estatisticas']}"

                         except: pass

                    contexto_chat = f"""

                    ATUE COMO: Engenheiro Sênior Python e Cientista de Dados do "Neves Analytics".

                    IMPORTANTE:

                    - O usuário possui um Big Data com {total_bd} jogos armazenados.

                    - Não diga que a amostra é pequena se o total for alto.

                    CONTEXTO ATUAL:

                    1. PERFORMANCE HOJE (Google Sheets):

                    {txt_hoje}

                    2. BIG DATA (Firebase):

                    {txt_bigdata}

                    3. RADAR (Ao Vivo):

                    {txt_radar}

                    USUÁRIO PERGUNTOU: "{prompt}"

                    """

                    try:

                        with st.spinner("Gerando solução de código..."):

                            response = gemini_safe_call(contexto_chat)

                            msg_ia = response.text

                        st.session_state.messages.append({"role": "assistant", "content": msg_ia})

                        st.chat_message("assistant").write(msg_ia)

                        if len(st.session_state["messages"]) > 6:

                            time.sleep(0.5); st.rerun()

                    except Exception as e: st.error(f"Erro na IA: {e}")

        # --- NOVA ABA DE TRADING ---

        with abas[10]:

            st.markdown("### 📈 Trading Pré-Live (Drop Odds)")

            st.caption("Apostas baseadas em variação de preço antes do jogo começar (Cashout Bet365).")

            c_trade1, c_trade2 = st.columns(2)

            if c_trade1.button("🔍 Escanear Mercado Agora (Manual)"):

                if IA_ATIVADA:

                    with st.spinner("Comparando Bet365 vs Pinnacle... Isso pode demorar."):

                        drops = scanner_drop_odds_pre_live(st.session_state['API_KEY'])

                        if drops:

                            st.success(f"Encontradas {len(drops)} oportunidades!")

                            for d in drops:

                                st.markdown(f"""

                                ---

                                ⚽ **{d['jogo']}** ({d['liga']}) | ⏰ {d['hora']}

                                📉 **Drop:** {d['valor']:.1f}%

                                • Bet365: **@{d['odd_b365']}**

                                • Pinnacle: **@{d['odd_pinnacle']}**

                                👉 *Entrar no {d['lado']} + Banker*

                                """)

                        else:

                            st.warning("Nenhum desajuste de odd encontrado nas Ligas Top agora.")

                else: st.error("IA/API necessária.")

        for i in range(INTERVALO, 0, -1):

            st.markdown(f'<div class="footer-timer">Próxima varredura em {i}s</div>', unsafe_allow_html=True)

            time.sleep(1)

        st.rerun()

else:

    with placeholder_root.container():

        st.title("❄️ Neves Analytics")

        st.info("💡 Robô em espera. Configure na lateral.")
