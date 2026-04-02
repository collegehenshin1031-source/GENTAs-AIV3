"""
HAGETAKA SCOPE - M&A候補検知ツール
- ログイン画面のスタイリッシュ化
- ハゲタカ診断エンジン（AI判定・チャート）の完全統合
- 戦略室のタブ移動＆スマホ対応カレンダー
- 金融庁コンプライアンス対応（文言校正）
- カート操作の即時反映＆ジャンプボタン
- PC版の銘柄コード入力欄の余白最適化
- フィルターのデフォルト設定（すべて・要監視OFF）
- M&A候補の並び順（LEVEL昇順、需給スコア降順）
- 【完全修正】expand_more等のアイコン文字化けバグ解消
- ダークモード/ライトモード完全自動対応（CSS変数化）
- ダークモード時のロゴ自動最適化（白パネル追加）
- ハゲタカ診断結果の独立カード（枠線）化
- 【改善】フローティングボタン（カート）を青系に変更し視認性向上
- 【修正】英字コード（151Aなど）の完全対応と全角/改行コピペ対応
- 【修正】yfinanceのデータ欠損（None）による診断エラーを完全解消
- 【追加修正】エラー（データ取得失敗）時にキャッシュさせない仕様に変更
- 【追加修正】データ取得エラー時のメッセージに「アクセス集中」の旨を追記
- 【究極防壁】ブラウザ偽装のランダム化と人間らしいヘッダー付与で長期間ブロックを極限回避
"""

import hashlib
import json
import re
import ast
import smtplib
import io
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
import time
import unicodedata
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional
from pathlib import Path
import streamlit as st
from datetime import datetime
import pytz
import base64
import pandas as pd
import numpy as np
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Google Sheets連携
from streamlit_gsheets import GSheetsConnection
import gspread
from google.oauth2.service_account import Credentials

# 暗号化
from cryptography.fernet import Fernet

# KABU+ データ取得
import kabuplus_client as kp

# ==========================================
# 定数
# ==========================================
JST = pytz.timezone("Asia/Tokyo")
MARKET_CAP_MIN = 300
MARKET_CAP_MAX = 2000

FLOW_SCORE_HIGH = 70
FLOW_SCORE_MEDIUM = 40

LEVEL_COLORS = {4: "#C41E3A", 3: "#FF9800", 2: "#FFC107", 1: "#5C6BC0", 0: "#9E9E9E"}

MASTER_PASSWORD = "88888"
DISCLAIMER_TEXT = "本ツールは市場データの可視化を目的とした補助ツールです。<br>銘柄推奨・売買助言ではありません。最終判断は利用者ご自身で行ってください。"

# ==========================================
# 【最強の通信セッション構築】
# ==========================================
# 複数のブラウザ（Chrome, Safari, Firefox等）の身分証を用意
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15"
]

def get_yf_session():
    """Bot弾きを回避し、弾かれても自動リトライするセッションを作成"""
    session = requests.Session()
    # 429(Too Many Requests)や50xエラー時に、最大5回まで間隔を空けて自動リトライ
    retries = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    # 一般的なブラウザからのアクセスにランダムで偽装し、人間らしいヘッダーを付与
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    })
    return session

yf_session = get_yf_session()

# ==========================================
# 【KABU+ 一括データ】診断用の銘柄情報キャッシュ
# ==========================================
@st.cache_data(ttl=900, show_spinner=False)
def _load_kabuplus_info() -> dict:
    """KABU+ から全銘柄の指標データを一括取得し info 辞書を構築"""
    try:
        uid, pwd = kp.get_credentials()
        if not uid or not pwd:
            return {}
        merged = kp.fetch_merged_data(uid, pwd)
        if merged.empty:
            return {}
        return kp.build_info_lookup(merged)
    except Exception:
        return {}


def _get_kabuplus_info(ticker: str) -> dict:
    """特定銘柄の KABU+ info を取得（キャッシュ済みルックアップ）"""
    return _load_kabuplus_info().get(ticker, {})

# ==========================================
# カート操作のコールバック関数（即時反映用）
# ==========================================
def clear_cart():
    st.session_state["cart"] = []

def add_to_cart(ticker):
    if ticker not in st.session_state.get("cart", []):
        st.session_state["cart"].append(ticker)

def remove_from_cart(ticker):
    if ticker in st.session_state.get("cart", []):
        st.session_state["cart"].remove(ticker)

# ==========================================
# UI設定・CSS
# ==========================================
st.set_page_config(page_title="源太AI🤖ハゲタカSCOPE", page_icon="🦅", layout="wide", initial_sidebar_state="collapsed")

# ページトップへのジャンプ用アンカー（目印）
st.markdown('<div id="top-of-page"></div>', unsafe_allow_html=True)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');

/* ヘッダー完全消去 */
header { visibility: hidden !important; display: none !important; }
#MainMenu, footer, .stDeployButton { display: none !important; }
[data-testid="stToolbar"] { display: none !important; }

/* 🌟 全体のベースデザイン（Streamlitのネイティブテーマ変数を活用） */
/* spanやdivへの強制上書きを解除し、アイコンの文字化けを防止しました */
.stApp { font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important; }

/* 背景のほんのりグラデーション（ダーク/ライト両対応の透明度） */
div[data-testid="stAppViewContainer"]{
  background-image: radial-gradient(1200px 600px at 10% 0%, rgba(92,107,192,0.06), transparent 60%),
                    radial-gradient(900px 450px at 95% 10%, rgba(196,30,58,0.06), transparent 55%) !important;
}
.main .block-container{ max-width: 1080px !important; padding: 2.0rem 1.2rem 3.2rem 1.2rem !important; }
h1{ text-align:center !important; font-size: 1.55rem !important; font-weight: 800 !important; margin-bottom: .2rem !important; }
.subtitle{ text-align:center; color: var(--text-color); opacity: 0.7; font-size:.85rem; margin-bottom: 1.1rem; }

/* 🌟 ライトモード時のロゴ（背景透過） */
.logo-img { mix-blend-mode: multiply; transition: all 0.3s ease; }

/* =======================================
   Tabs (スマホ対応の安定化・スワイプ対応)
   ======================================= */
.stTabs [data-baseweb="tab-list"] {
  display: flex !important;
  flex-wrap: nowrap !important;
  overflow-x: auto !important;
  -webkit-overflow-scrolling: touch !important;
  justify-content: flex-start !important;
  background: var(--secondary-background-color) !important;
  padding: 0.35rem !important;
  border-radius: 14px !important;
  border: 1px solid rgba(128,128,128,0.15) !important;
  box-shadow: 0 4px 15px rgba(0,0,0,0.05) !important;
  margin-bottom: 1.0rem !important;
  gap: 0.3rem !important;
}
.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar { display: none !important; }

.stTabs [data-baseweb="tab"] {
  flex: 1 1 0 !important;
  min-width: max-content !important;
  padding: 0.6rem 0.8rem !important;
  border-radius: 10px !important;
  font-weight: 700 !important;
  justify-content: center !important;
}
.stTabs [data-baseweb="tab"] p {
  white-space: nowrap !important;
  margin: 0 !important;
  color: var(--text-color);
  opacity: 0.7;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] { 
    background: linear-gradient(135deg, #0F172A 0%, #334155 100%) !important; 
}
.stTabs [data-baseweb="tab"][aria-selected="true"] p { color: #FFFFFF !important; opacity: 1.0; }

/* =======================================
   Cards (スマホベース＆テーマ対応)
   ======================================= */
.spike-card{
  position: relative; 
  background-color: var(--secondary-background-color) !important; 
  border-radius: 16px; padding: 1rem; margin-bottom: .75rem; 
  border: 1px solid rgba(128,128,128,0.2) !important;
  box-shadow: 0 10px 30px rgba(0,0,0,0.05);
}
.spike-card::before{
  content:""; position:absolute; left:0; top:10px; bottom:10px; width:4px;
  border-radius: 999px; background: rgba(128,128,128,0.3);
}
.spike-card.high{ border-color: rgba(196,30,58,0.4) !important; box-shadow: 0 10px 30px rgba(196,30,58,0.15); }
.spike-card.high::before{ background: linear-gradient(180deg, #C41E3A 0%, #E63946 100%); }
.spike-card.medium{ border-color: rgba(255,152,0,0.4) !important; box-shadow: 0 10px 30px rgba(255,152,0,0.15); }
.spike-card.medium::before{ background: linear-gradient(180deg, #FF9800 0%, #FFC107 100%); }

.card-header{ display:flex; justify-content:space-between; align-items:center; gap:.7rem; margin-bottom: .55rem; }
.ticker-name a{ font-weight: 800; color: var(--text-color) !important; text-decoration:none; font-size: 1.1rem; }
.ticker-name a:hover{ text-decoration: underline; }
.ticker-jp-name { font-size: 0.75rem; color: var(--text-color); opacity: 0.6; margin-left: 6px; }

.ratio-badge{
  min-width: 70px; text-align:center; padding: .2rem .6rem; border-radius: 8px; font-weight: 800;
  border: 1px solid rgba(128,128,128,0.2); background-color: var(--background-color); cursor: help;
}
.ratio-badge.high{ color:#FFFFFF !important; border-color: rgba(196,30,58,0.4); background: linear-gradient(135deg, #C41E3A 0%, #E63946 100%); }
.ratio-badge.medium{ color: var(--text-color) !important; border-color: rgba(255,152,0,0.4); background: rgba(255,152,0,0.15); }
.score-val{ font-size: 1.0rem; line-height: 1.0; }
.score-label{ font-size: 0.55rem; line-height: 1.0; display: block; margin-bottom: 2px; opacity: 0.8;}

.level-badge { padding: 3px 10px; border-radius: 12px; font-size: 0.75rem; font-weight: 700; color: white !important; }

.card-body{ display:grid; grid-template-columns: repeat(4, 1fr); gap: .8rem; margin-top: .2rem; }
.info-label{ font-size: .72rem; color: var(--text-color); opacity: 0.7; font-weight: 700; letter-spacing: .02em; }
.info-value{ font-size: .93rem; color: var(--text-color); font-weight: 700; }
.price-val { color: #ff4b4b !important; font-weight: 800; }

.tag-container { padding: 0 0.8rem 0.5rem; font-size: 0.7rem; }
.tag-watch { background: rgba(92,107,192,0.15); color: #5C6BC0 !important; padding: 2px 8px; border-radius: 999px; margin-right: 6px; font-weight: 700; display: inline-block; margin-bottom: 4px; }
.tag-normal { background: var(--background-color); color: var(--text-color); border: 1px solid rgba(128,128,128,0.3); padding: 2px 8px; border-radius: 999px; margin-right: 6px; display: inline-block; margin-bottom: 4px; }

/* 💻 PC版の銘柄カード最適化 */
@media (min-width: 768px) {
    .spike-card { padding: 1.5rem 2.0rem 0.5rem !important; margin-bottom: 1.0rem !important; }
    .ticker-name a { font-size: 1.6rem !important; }
    .ticker-jp-name { font-size: 1.1rem !important; margin-left: 12px !important; }
    .card-body { display: flex !important; gap: 5rem !important; margin-top: 1.0rem !important; }
    .info-label { font-size: 0.9rem !important; }
    .info-value { font-size: 1.3rem !important; }
    .price-val { font-size: 1.4rem !important; }
    .level-badge { font-size: 1.0rem !important; padding: 5px 14px !important; }
    .score-label { font-size: 0.75rem !important; }
    .score-val { font-size: 1.4rem !important; }
    .ratio-badge { padding: 0.4rem 1.0rem !important; }
    .tag-container { padding: 1.0rem 0 0.5rem !important; }
    .tag-watch, .tag-normal { font-size: 0.85rem !important; padding: 4px 12px !important; margin-right: 8px !important; }
}

/* =======================================
   🌟 ハゲタカ診断 個別カード化スタイル (多重枠線バグ修正版)
   ======================================= */
/* 該当コンテナのみをピンポイントで枠線表示する安全なCSS */
div[data-testid="stVerticalBlock"]:has(> div:nth-child(1) .diagnosis-card-marker) {
    background-color: var(--secondary-background-color) !important;
    border: 2px solid rgba(128, 128, 128, 0.2) !important;
    border-radius: 16px !important;
    padding: 1.5rem !important;
    margin-bottom: 2.5rem !important;
    margin-top: 1.0rem !important;
    box-shadow: 0 8px 30px rgba(0, 0, 0, 0.08) !important;
}

/* =======================================
   ボタンとその他のコンポーネント
   ======================================= */
div.stButton > button{ border-radius: 12px !important; font-weight: 800 !important; padding: .55rem .9rem !important; }

div.stButton > button[data-testid="baseButton-primary"] {
    background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%) !important;
    color: white !important;
    border: none !important;
    box-shadow: 0 4px 10px rgba(59, 130, 246, 0.3) !important;
}

.reset-btn-container { margin-bottom: -0.5rem; }
.reset-btn-container button {
    background: transparent !important;
    color: #E11D48 !important;
    border: 1px solid rgba(225, 29, 72, 0.4) !important;
    font-weight: 800 !important;
    border-radius: 12px !important;
    transition: all 0.2s ease !important;
}
.reset-btn-container button:hover {
    background: rgba(225, 29, 72, 0.1) !important;
}

.filter-btn-container button { border-radius: 12px !important; font-weight: 800 !important; }

/* 免責事項ボックスのスタイル */
.disclaimer-box {
    background-color: rgba(245, 158, 11, 0.1); 
    border-left: 4px solid #F59E0B;
    border-radius: 8px;
    padding: 0.8rem 1rem; margin: 1.5rem 0 1rem 0; font-size: 0.75rem; 
    color: var(--text-color) !important; line-height: 1.5;
}

/* =======================================
   🌟 ダークモード時の自動最適化設定
   ======================================= */
@media (prefers-color-scheme: dark) {
    /* ダークモード時はロゴの背景透過をやめ、白い角丸パネルを敷いてクッキリ見せる */
    .logo-img {
        mix-blend-mode: normal !important;
        background-color: rgba(255, 255, 255, 0.95) !important;
        padding: 10px 15px !important;
        border-radius: 16px !important;
        box-shadow: 0 4px 15px rgba(0,0,0,0.5) !important;
    }
    
    /* 診断カードの枠線をダークモード用に少し明るく調整 */
    div[data-testid="stVerticalBlock"]:has(> div:nth-child(1) .diagnosis-card-marker) {
        border: 2px solid rgba(255, 255, 255, 0.15) !important;
        box-shadow: 0 10px 25px rgba(0,0,0,0.5) !important;
    }
}
</style>
""", unsafe_allow_html=True)

# ==========================================
# 表記ゆれ吸収・共通ヘルパー
# ==========================================
STATE_HELP = {
    "要監視": "変化が強めです。優先して確認します。",
    "観測中": "変化の兆しがあります。数日単位で見守ります。",
    "沈静": "今は大きな変化が見えません。記録だけ残します。",
}

def _norm_label(s) -> str:
    if s is None: return ""
    t = str(s).strip()
    return re.sub(r'^[\s○●◎◯・\-–—★☆▶▷→⇒✓✔✅☑︎【\[\(（]+', '', t).strip()

def _norm_tag(t) -> str:
    t = _norm_label(t)
    if not t: return ""
    if "要監視" in t: return "要監視"
    return t

def _tags_list(x) -> List[str]:
    if x is None: return []
    if isinstance(x, list): return [str(v) for v in x]
    return [str(x)]

def _normalize_item(it: Dict) -> Dict:
    d = dict(it) if isinstance(it, dict) else {}
    raw_state = d.get("display_state", d.get("state", ""))
    tags_raw = _tags_list(d.get("tags"))
    tags_norm = []
    has_watch = ("要監視" in _norm_label(raw_state))
    for tg in tags_raw:
        nt = _norm_tag(tg)
        if not nt: continue
        if nt == "要監視": has_watch = True; continue
        if nt in ["下側ゾーン", "上側ゾーン"]: continue
        tags_norm.append(nt)
    state = "要監視" if has_watch else (_norm_label(raw_state) or "観測中")
    d["display_state"] = state
    uniq = []
    seen = set()
    for t in tags_norm:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    d["tags"] = uniq
    return d

def _is_watch(item: dict) -> bool:
    state = _norm_label(item.get("display_state", item.get("state", "")))
    if "要監視" in state: return True
    for tg in _tags_list(item.get("tags")):
        if "要監視" in _norm_label(tg): return True
    return False

def get_logo_base64():
    try:
        with open("logo.png", "rb") as f:
            return base64.b64encode(f.read()).decode()
    except: return None

@st.cache_data(ttl=900)
def load_data() -> Dict:
    """ratios.json を読む。名前はバッチ済みデータ＋辞書のみで解決し、Yahoo へは繋がない（全銘柄時の重さ対策）。"""
    p = Path("data/ratios.json")
    if not p.exists():
        return {}

    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)

    for bucket in ("data", "all_data"):
        bucket_data = data.get(bucket, {}) or {}
        if isinstance(bucket_data, dict):
            for ticker, item in bucket_data.items():
                if isinstance(item, dict):
                    item["name"] = get_display_japanese_name(
                        ticker, item.get("name"), allow_yahoo_fallback=False
                    )

    return data

def get_fernet() -> Fernet: return Fernet(st.secrets["encryption"]["key"].encode())
def encrypt_password(pw: str) -> str: return get_fernet().encrypt(pw.encode()).decode() if pw else ""
def decrypt_password(pw: str) -> str: 
    try: return get_fernet().decrypt(pw.encode()).decode() if pw else ""
    except: return ""

def get_gsheets_connection(): return st.connection("gsheets", type=GSheetsConnection)

def load_settings_by_email(email: str) -> Optional[Dict]:
    if not email: return None
    try:
        df = get_gsheets_connection().read(worksheet="settings", usecols=[0, 1], ttl=0)
        if df is None or df.empty: return None
        df.columns = ["email", "encrypted_password"]
        row = df[df["email"].str.lower().str.strip() == email.lower().strip()]
        if row.empty: return None
        return {"email": row.iloc[0]["email"], "encrypted_password": row.iloc[0]["encrypted_password"]}
    except:
        st.cache_data.clear()
        return None

def get_gspread_client():
    try:
        cd = dict(st.secrets["connections"]["gsheets"])
        cd.pop("spreadsheet", None); cd.pop("worksheet", None)
        creds = Credentials.from_service_account_info(cd, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds)
    except: return None

def save_settings_to_sheet(email: str, app_password: str) -> bool:
    if not email: return False
    email = email.lower().strip()
    try:
        client = get_gspread_client()
        if not client: return False
        url = st.secrets["connections"]["gsheets"].get("spreadsheet")
        ws = client.open_by_url(url).worksheet("settings")
        enc_pw = encrypt_password(app_password)
        try: all_emails = ws.col_values(1)
        except: all_emails = []
        row_index = next((i + 1 for i, ce in enumerate(all_emails) if ce and ce.lower().strip() == email), -1)
        if row_index > 1: ws.update_cell(row_index, 2, enc_pw)
        else: ws.append_row([email, enc_pw])
        st.cache_data.clear()
        return True
    except: return False

def delete_settings_from_sheet(email: str) -> bool:
    if not email: return False
    email = email.lower().strip()
    try:
        client = get_gspread_client()
        if not client: return False
        url = st.secrets["connections"]["gsheets"].get("spreadsheet")
        ws = client.open_by_url(url).worksheet("settings")
        try: all_emails = ws.col_values(1)
        except: all_emails = []
        row_index = next((i + 1 for i, ce in enumerate(all_emails) if ce and ce.lower().strip() == email), -1)
        if row_index > 1:
            ws.delete_rows(row_index)
            st.cache_data.clear()
            return True
        return False
    except: return False

def send_test_email(email: str, app_password: str) -> tuple[bool, str]:
    try:
        msg = MIMEMultipart()
        msg["From"] = msg["To"] = email
        msg["Subject"] = "🦅 ハゲタカSCOPE - テスト通知"
        msg.attach(MIMEText("メール設定が正常に完了しました！", "plain", "utf-8"))
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(email, app_password)
            server.send_message(msg)
        return True, "テストメール送信成功！"
    except Exception as e: return False, f"送信エラー: {str(e)}"

# ==========================================
# ハゲタカ診断エンジン用ヘルパー関数
# ==========================================
@st.cache_data(ttl=86400)
def get_jpx_data():
    try:
        html_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(html_url, headers=headers, timeout=10)
        response.raise_for_status()

        match = re.search(r'href="([^"]+data_j\.(?:xls|xlsx|csv))"', response.text, flags=re.IGNORECASE)
        if not match:
            return {}, []

        file_url = "https://www.jpx.co.jp" + match.group(1)
        file_response = requests.get(file_url, headers=headers, timeout=15)
        file_response.raise_for_status()

        if file_url.lower().endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_response.content), encoding="utf-8")
        else:
            df = pd.read_excel(io.BytesIO(file_response.content))

        if df is None or df.empty or len(df.columns) < 3:
            return {}, []

        df = df.copy()
        market_col = df.columns[3] if len(df.columns) >= 4 else None

        if market_col is not None:
            market_series = df[market_col].astype(str)
            market_mask = market_series.str.contains("プライム|スタンダード|グロース", na=False)
            df_tickers = df[market_mask].copy()
            if df_tickers.empty:
                df_tickers = df.copy()
        else:
            df_tickers = df.copy()

        def safe_code(x):
            if pd.isnull(x):
                return ""
            s = str(x).strip().upper()
            if s.endswith(".0"):
                s = s[:-2]
            return s

        codes = df_tickers.iloc[:, 1].apply(safe_code)
        names = df_tickers.iloc[:, 2].astype(str).str.strip()

        name_map = {}
        for code, name in zip(codes, names):
            if code and name and name.lower() != "nan":
                name_map[code] = name

        return name_map, list(name_map.keys())
    except Exception:
        return {}, []


@st.cache_data(ttl=86400)
def load_local_ticker_name_master():
    """fetch_data.py の固定辞書を安全に読み込んで表示側でも使う"""
    try:
        fetch_data_path = Path(__file__).resolve().parent / "fetch_data.py"
        src = fetch_data_path.read_text(encoding="utf-8")
        tree = ast.parse(src)

        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "TICKER_NAMES":
                        value = ast.literal_eval(node.value)
                        if isinstance(value, dict):
                            return {str(k).strip(): str(v).strip() for k, v in value.items()}
        return {}
    except Exception:
        return {}


jpx_names, jpx_codes = get_jpx_data()
LOCAL_TICKER_MASTER = load_local_ticker_name_master()

TICKER_NAMES_JP = {
    "3923.T": "ラクス", "4443.T": "Sansan", "4478.T": "フリー", "3994.T": "マネーフォワード",
    "4165.T": "プレイド", "4169.T": "ENECHANGE", "4449.T": "ギフティ", "4475.T": "HENNGE",
    "4431.T": "スマレジ", "4057.T": "インターファクトリー", "3697.T": "SHIFT", "4194.T": "ビジョナル",
    "4180.T": "Appier", "3655.T": "ブレインパッド", "4751.T": "サイバーエージェント",
    "3681.T": "ブイキューブ", "6035.T": "IRジャパン", "4384.T": "ラクスル", "9558.T": "ジャパニアス",
    "4441.T": "トビラシステムズ", "6315.T": "TOWA", "6323.T": "ローツェ", "6890.T": "フェローテック",
    "7735.T": "SCREENホールディングス", "6146.T": "ディスコ", "6266.T": "タツモ",
    "3132.T": "マクニカホールディングス", "6920.T": "レーザーテック", "4565.T": "そーせいグループ",
    "4587.T": "ペプチドリーム", "4582.T": "シンバイオ製薬", "4583.T": "カイオム・バイオ",
    "4563.T": "アンジェス", "2370.T": "メディネット", "4593.T": "ヘリオス", "3064.T": "MonotaRO",
    "3092.T": "ZOZO", "3769.T": "GMOペイメント", "4385.T": "メルカリ", "7342.T": "ウェルスナビ",
    "4480.T": "メドレー", "6560.T": "LTS", "3182.T": "オイシックス", "9166.T": "GENDA",
    "3765.T": "ガンホー", "3659.T": "ネクソン", "3656.T": "KLab", "3932.T": "アカツキ",
    "4071.T": "プラスアルファ", "4485.T": "JTOWER", "7095.T": "Macbee Planet",
    "4054.T": "日本情報クリエイト", "6095.T": "メドピア", "4436.T": "ミンカブ", "4477.T": "BASE",
}


def get_display_japanese_name(
    ticker: str,
    fallback_name: str | None = None,
    info: dict | None = None,
    *,
    allow_yahoo_fallback: bool = True,
) -> str:
    """allow_yahoo_fallback=False のとき Yahoo!ファイナンス日本へアクセスしない（一覧・load_data 用で軽量化）"""
    code_only = str(ticker or "").replace(".T", "").strip().upper()
    ticker_key = f"{code_only}.T" if code_only else str(ticker or "").strip().upper()
    fallback_name = (fallback_name or "").strip()
    info = info or {}

    allowed_brand_names = {
        "SHIFT", "TOWA", "ZOZO", "HENNGE", "GENDA", "MonotaRO", "Appier",
        "BASE", "JTOWER", "Sansan", "Macbee Planet", "KLab", "LTS", "PR TIMES",
        "THECOO", "WACUL", "CRI・ミドルウェア", "eBASE", "NOK", "NTN", "THK",
        "TPR", "IHI", "SUBARU", "KYB"
    }

    candidates = [
        jpx_names.get(code_only),
        LOCAL_TICKER_MASTER.get(ticker_key),
        TICKER_NAMES_JP.get(ticker_key),
        fallback_name,
        info.get("shortName"),
        info.get("longName"),
    ]

    for cand in candidates:
        cand = (cand or "").strip()
        if not cand:
            continue
        if re.search(r"[ぁ-んァ-ヶ一-龠々ー]", cand):
            return cand
        if cand in allowed_brand_names:
            return cand

    if allow_yahoo_fallback:
        try:
            url_yfjp = f"https://finance.yahoo.co.jp/quote/{code_only}.T"
            res_yfjp = yf_session.get(url_yfjp, timeout=3)
            match = re.search(r"<title>(.+?)(?:\(株\))?【", res_yfjp.text)
            if match:
                title_name = match.group(1).strip()
                if title_name:
                    return title_name
        except Exception:
            pass

    for cand in candidates:
        cand = (cand or "").strip()
        if cand:
            return cand

    return code_only or str(ticker or "")

# 🌟 全角半角・スペース・改行・大文字小文字をすべて吸収してコードを抽出する関数
def normalize_input(input_text):
    if not input_text: return []
    # 全角を半角に、小文字を大文字に統一（151a -> 151A）
    text = unicodedata.normalize('NFKC', input_text).upper()
    # スペース、改行、カンマなどをすべて半角スペースに変換
    text = re.sub(r'[\s,、\n\r]+', ' ', text)
    # 分割して空白を除去
    codes = [c.strip() for c in text.split(' ') if c.strip()]
    # 重複を削除して返す（順序は維持）
    return list(dict.fromkeys(codes))

def check_dna(hist):
    try:
        window = 60
        if len(hist) < window: return False
        pct_change = hist['Close'].pct_change(periods=60)
        max_spike = pct_change.max()
        return max_spike >= 0.8
    except:
        return False

def format_market_cap(oku_val):
    oku_val = int(oku_val)
    if oku_val >= 10000:
        cho = oku_val // 10000
        oku = oku_val % 10000
        return f"{cho}兆円" if oku == 0 else f"{cho}兆{oku}億円"
    return f"{oku_val}億円"

# ==========================================
# 案5: バッチ保存済みOHLCVキャッシュ（64シャード + レガシー1ファイル）
# fetch_data.HISTORY_SHARD_COUNT と同一であること
# ==========================================
HISTORY_SHARD_COUNT = 64


def _history_shard_id(ticker: str) -> int:
    return int(hashlib.md5(ticker.encode("utf-8")).hexdigest(), 16) % HISTORY_SHARD_COUNT


@st.cache_data(ttl=3600, max_entries=128, show_spinner=False)
def _load_history_shard(shard_id: int) -> dict:
    p = Path(f"data/history/shard_{shard_id:02d}.json")
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


@st.cache_data(ttl=3600, show_spinner=False)
def _load_stock_history_legacy_flat() -> dict:
    """後方互換: 単一の stock_history.json（updated_at 等を除く）"""
    p = Path("data/stock_history.json")
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        for k in ("updated_at", "format"):
            data.pop(k, None)
        return data
    except Exception:
        return {}


def load_ticker_history_row(ticker: str) -> dict | None:
    """診断用: 銘柄1件分のキャッシュ（シャード優先、無ければレガシー）"""
    row = _load_history_shard(_history_shard_id(ticker)).get(ticker)
    if row and row.get("dates"):
        return row
    legacy = _load_stock_history_legacy_flat().get(ticker)
    if legacy and legacy.get("dates"):
        return legacy
    return None


def _build_hist_from_cache(ticker: str, history_data: dict):
    """stock_history.jsonのデータからpandas DataFrameを復元"""
    data = history_data.get(ticker)
    if not data or not data.get("dates"):
        return None
    try:
        hist = pd.DataFrame({
            "Open":   data["O"],
            "High":   data["H"],
            "Low":    data["L"],
            "Close":  data["C"],
            "Volume": data["V"],
        }, index=pd.to_datetime(data["dates"]))
        hist.index.name = "Date"
        return hist
    except Exception:
        return None


def _fetch_stooq_hist_jp(ticker: str) -> pd.DataFrame | None:
    """yfinance が空になる東証英字銘柄（151A 等）向け。Stooq 日足（例: 151a.jp）。"""
    code = str(ticker or "").replace(".T", "").strip()
    if not code:
        return None
    sym = f"{code.lower()}.jp"
    url = f"https://stooq.com/q/d/l/?s={sym}&i=d"
    try:
        r = requests.get(url, timeout=25)
        r.raise_for_status()
        raw = r.text.strip()
        if not raw or raw.lower().startswith("no data"):
            return None
        df = pd.read_csv(io.StringIO(raw))
    except Exception:
        return None
    if df is None or df.empty:
        return None
    colmap = {str(c).strip(): str(c).strip() for c in df.columns}
    df = df.rename(columns=colmap)
    lower = {c.lower(): c for c in df.columns}
    def col(name: str) -> str | None:
        return lower.get(name.lower())
    dcol, ocol, hcol, lcol, ccol, vcol = (
        col("date"), col("open"), col("high"), col("low"), col("close"), col("volume")
    )
    if not all([dcol, ocol, hcol, lcol, ccol]):
        return None
    vol_series = (
        pd.to_numeric(df[vcol], errors="coerce").fillna(0)
        if vcol
        else pd.Series(0.0, index=df.index)
    )
    out = pd.DataFrame({
        "Open": pd.to_numeric(df[ocol], errors="coerce"),
        "High": pd.to_numeric(df[hcol], errors="coerce"),
        "Low": pd.to_numeric(df[lcol], errors="coerce"),
        "Close": pd.to_numeric(df[ccol], errors="coerce"),
        "Volume": vol_series,
    })
    out.index = pd.to_datetime(df[dcol], errors="coerce")
    out = out[~out.index.isna()].dropna(subset=["Close"], how="any").sort_index()
    out = out.tail(520)
    if len(out) < 5:
        return None
    out["Volume"] = out["Volume"].fillna(0)
    return out


def _fetch_yahoo_chart_api(ticker: str) -> pd.DataFrame | None:
    """
    Yahoo Finance Chart API を直接呼び出してOHLCVを取得。
    yfinanceライブラリを経由しないため、英字コード（151A等）や
    yfinanceで取れない銘柄でも取得できることがある。
    """
    code = str(ticker or "").replace(".T", "").strip()
    if not code:
        return None
    symbol = f"{code}.T"
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": "1y", "interval": "1d", "includePrePost": "false"}
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return None

        timestamps = result[0].get("timestamp", [])
        quote = result[0].get("indicators", {}).get("quote", [{}])[0]
        if not timestamps or not quote:
            return None

        df = pd.DataFrame({
            "Open": quote.get("open", []),
            "High": quote.get("high", []),
            "Low": quote.get("low", []),
            "Close": quote.get("close", []),
            "Volume": quote.get("volume", []),
        }, index=pd.to_datetime(timestamps, unit="s", utc=True))
        df.index = df.index.tz_convert("Asia/Tokyo").tz_localize(None)
        df.index.name = "Date"
        df = df.dropna(subset=["Close"])
        if len(df) < 5:
            return None
        df["Volume"] = df["Volume"].fillna(0)
        return df
    except Exception:
        return None


def _fetch_kabuoji3(ticker: str) -> pd.DataFrame | None:
    """
    kabuoji3.com から日本株のOHLCVを取得。
    日本株専用サイトのため、yfinance/Stooqで取れない銘柄でも取得可能。
    ※ pd.read_html は lxml 依存のため、正規表現で手動パースする。
    """
    code = str(ticker or "").replace(".T", "").strip()
    if not code:
        return None
    url = f"https://kabuoji3.com/stock/{code}/"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None

        # テーブル行を正規表現で抽出: <tr><td>日付</td><td>始値</td>...
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", resp.text, re.DOTALL)
        records = []
        for row_html in rows:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.DOTALL)
            if len(cells) >= 6:
                # 日付, 始値, 高値, 安値, 終値, 出来高
                date_str = cells[0].strip()
                if not re.match(r"\d{4}-\d{2}-\d{2}", date_str):
                    continue
                try:
                    o = float(cells[1].replace(",", ""))
                    h = float(cells[2].replace(",", ""))
                    l = float(cells[3].replace(",", ""))
                    c = float(cells[4].replace(",", ""))
                    v = float(cells[5].replace(",", ""))
                    records.append({"Date": date_str, "Open": o, "High": h, "Low": l, "Close": c, "Volume": v})
                except ValueError:
                    continue

        if len(records) < 5:
            return None

        df = pd.DataFrame(records)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        df["Volume"] = df["Volume"].fillna(0)
        return df if len(df) >= 5 else None
    except Exception:
        return None


def _fetch_yf_data_with_retry(ticker: str, max_retries: int = 2, base_delay: float = 3.0):
    """
    OHLCV履歴のみを取得（高速化版）。
    ★ info は KABU+ から取得するため、ここでは取らない。
    ★ 4段階フォールバック: yf.download() → Yahoo Chart API直接 → Stooq → kabuoji3
    """
    last_error = None

    # 1) yf.download() で一括取得（stock.history()より高速）
    for attempt in range(max_retries):
        try:
            session = get_yf_session()
            hist = yf.download(
                tickers=[ticker], period="2y", interval="1d",
                auto_adjust=True, progress=False, threads=False,
                session=session,
            )
            if hist is not None and not hist.empty and len(hist) >= 5:
                # MultiIndex対策
                if isinstance(hist.columns, pd.MultiIndex):
                    hist.columns = hist.columns.get_level_values(0)
                hist = hist[["Open", "High", "Low", "Close", "Volume"]].dropna()
                if len(hist) >= 5:
                    return hist
            raise ValueError("データが空または不十分です")
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2 ** attempt))

    # 2) Yahoo Finance Chart API 直接呼び出し（yfinanceライブラリを迂回）
    hist_yc = _fetch_yahoo_chart_api(ticker)
    if hist_yc is not None and len(hist_yc) >= 5:
        return hist_yc

    # 3) Stooq フォールバック（英字コード 151A 等に有効）
    hist_sq = _fetch_stooq_hist_jp(ticker)
    if hist_sq is not None and len(hist_sq) >= 5:
        return hist_sq

    # 4) kabuoji3 フォールバック（日本株専用・最終手段）
    hist_kj = _fetch_kabuoji3(ticker)
    if hist_kj is not None and len(hist_kj) >= 5:
        return hist_kj

    raise last_error if last_error else ValueError("データ取得に失敗しました")


# 🚨 【エラー回避＆キャッシュ対策】の内部関数（データ取得失敗時は例外を投げてキャッシュさせない）
@st.cache_data(ttl=900, show_spinner=False)
def _evaluate_stock_cached(ticker):
    # ★ Step 1: info は KABU+ から一括取得済みデータを優先使用
    info = _get_kabuplus_info(ticker)

    # ★ Step 2: OHLCV はバッチキャッシュ → yf.download() → Stooq の順
    row = load_ticker_history_row(ticker)
    hist = _build_hist_from_cache(ticker, {ticker: row}) if row else None

    if hist is not None and len(hist) >= 5:
        # キャッシュヒット: KABU+ info がなければキャッシュの info を使う
        if not info:
            info = (row.get("info") or {}) if row else {}
    else:
        # キャッシュミス: OHLCV のみ取得（info は KABU+ で済んでいる）
        hist = _fetch_yf_data_with_retry(ticker)

    # 取得失敗時は例外を出してキャッシュ化を回避する
    if hist is None or hist.empty or len(hist) < 5:
        raise ValueError("Insufficient data or fetch limit")

    current_price = hist['Close'].iloc[-1]
    current_vol = hist['Volume'].iloc[-1]

    # 少ない日数でも計算できるように修正
    avg_vol_100 = hist['Volume'][-100:].mean() if len(hist) >= 100 else hist['Volume'].mean()
    
    # yfinanceから欠損値(None)が返ってきた場合に確実に 0 に変換する
    market_cap = info.get('marketCap') or 0
    shares = info.get('sharesOutstanding') or 0
    if market_cap == 0: market_cap = current_price * shares
    market_cap_oku = market_cap / 100000000
    
    formatted_mcap = format_market_cap(market_cap_oku)

    turnover_rate = (current_vol / shares) * 100 if shares > 0 and current_vol > 0 else 0
        
    if turnover_rate >= 10.0: turnover_str = f"🔥🔥🔥 {turnover_rate:.2f}% (超異常値・大相場警戒)"
    elif turnover_rate >= 5.0: turnover_str = f"🔥🔥 {turnover_rate:.2f}% (異常値・大口介入期待高)"
    elif turnover_rate >= 2.0: turnover_str = f"🔥 {turnover_rate:.2f}% (動意づき)"
    elif turnover_rate > 0: turnover_str = f"💤 {turnover_rate:.2f}% (平常運転)"
    else: turnover_str = "算出不可"

    dividend_rate = info.get('dividendRate') or info.get('trailingAnnualDividendRate') or 0
    payout_ratio = info.get('payoutRatio') or 0
    div_yield = info.get('dividendYield') or info.get('trailingAnnualDividendYield') or 0

    if dividend_rate > 0:
        payout_str = f"{payout_ratio * 100:.1f}%" if payout_ratio > 0 else "-"
        yield_str = f"{div_yield * 100:.2f}%" if div_yield > 0 else "-"
        dividend_text = f"{dividend_rate}円 （利回り: {yield_str} / 配当性向: {payout_str}）"
    else:
        dividend_text = "無配"

    code_only = ticker.replace(".T", "")
    jp_name = get_display_japanese_name(ticker, info=info)

    if market_cap_oku >= 5000:
        cap_category = "large"
        intervention_name = "🏢 機関投資家・大口流入期待度"
    elif market_cap_oku >= 50:
        cap_category = "target"
        intervention_name = "🦅 大口資金・介入期待度 (目安)"
    else:
        cap_category = "small"
        intervention_name = "⚠️ 短期資金・過熱度 (超小型)"

    hist_6mo = hist.tail(125)
    
    # 価格が全く動いていない銘柄で pd.cut がエラーを起こすのを防ぐ
    if hist_6mo['Close'].nunique() > 1:
        price_bins = pd.cut(hist_6mo['Close'], bins=15)
        vol_profile = hist_6mo.groupby(price_bins, observed=False)['Volume'].sum()
        try:
            max_vol_price = vol_profile.idxmax().mid
        except Exception:
            max_vol_price = current_price
    else:
        max_vol_price = current_price
        
    recent_20_low = hist['Low'][-20:].min() if len(hist) >= 20 else hist['Low'].min()

    upside_potential = 0
    is_blue_sky = False
    
    if current_price >= max_vol_price:
        is_blue_sky = True
    else:
        upside_potential = ((max_vol_price - current_price) / current_price) * 100

    deviation = ((current_price - max_vol_price) / max_vol_price) * 100

    if is_blue_sky: pot_level = 4
    elif upside_potential >= 30: pot_level = 3
    elif upside_potential >= 15: pot_level = 2
    elif upside_potential >= 5: pot_level = 1
    else: pot_level = 0

    max_stars = 5 if deviation <= 10.0 else 4 if deviation <= 20.0 else 3
    raw_stars = pot_level + 1
    final_stars = min(raw_stars, max_stars)
    star_rating = "★" * final_stars + "☆" * (5 - final_stars)

    if raw_stars > final_stars:
        if final_stars == 4:
            patterns = [
                ("【上昇トレンド・高値警戒】", "上値の壁は薄いものの、直近底値からの上昇が続いており、新規参入は短期目線での対応が無難な水準です。"),
                ("【モメンタム継続・押し目待ち】", "強い勢いを保っていますが、やや過熱感が出てきました。リスクを抑えるなら押し目を待つのが一案です。"),
                ("【高値圏の順張り局面】", "上値余地はありますが、すでに一定の上昇を遂げています。利益確定売りに警戒しつつの判断が求められます。")
            ]
        else:
            patterns = [
                ("【高値圏のモメンタム相場】", "上値を抑える壁はなく強いトレンドですが、乖離率が高く高値掴みのリスクがあります。短期戦と割り切った対応が求められる水準です。"),
                ("【急騰後・リスクリワード低下】", "勢いは非常に強いものの、今からの新規エントリーはリスクとリターンのバランスが取りにくくなっています。慎重な判断が必要です。"),
                ("【過熱気味の上昇波】", "上値余地を残しつつも、テクニカル的には過熱感が漂います。無理に深追いせず、冷静に状況を見極めたい局面です。")
            ]
    else:
        if pot_level == 4:
            patterns = [
                ("【青天井モード】", "上値に目立った需給の壁（抵抗線）がなく、売り手が不在の真空地帯に突入しています。"),
                ("【上値抵抗クリア】", "過去の重いしこり玉（含み損）エリアを突破しており、需給が好転している局面です。"),
                ("【真空地帯への突入】", "目立った戻り売り圧力が少なく、トレンドに逆らわない順張りが有効な水準です。"),
                ("【売り手不在の快晴】", "上値での迷いが生じにくく、資金流入がストレートに株価に反映されやすい帯域にいます。"),
                ("【需給良好・上値追い】", "過去の取引の壁を抜けました。ただし、急ピッチな上昇時は利食いにも留意してください。"),
                ("【視界良好チャート】", "上値を抑えつける強固な壁が見当たりません。資金の逃げ足にだけ注意して波に乗りたい位置です。")
            ]
        elif pot_level == 3:
            patterns = [
                ("【大幅な上値余地】", "強固な抵抗線まで距離があり、大きな値幅取りが狙えるポテンシャルを秘めています。"),
                ("【上値余地：特大】", "最大の壁まで十分な空間が開いており、大口の仕掛けが入りやすいエリアです。"),
                ("【リバウンド妙味】", "上値の重い水準まで距離があるため、反発トレンドに乗れた際のリターンが大きくなりやすい形状です。"),
                ("【ターゲット遠方】", "主要なヤレヤレ売りが降ってくる水準まで、軽快な足取りが期待できます。"),
                ("【絶好の上昇空間】", "次の大きな節目まで邪魔する壁がなく、買い圧力が素直に効きやすいチャートです。"),
                ("【値幅取り期待ゾーン】", "出来高の壁まで距離的余裕があり、トレンド発生時の爆発力に期待が持てる位置取りです。")
            ]
        elif pot_level == 2:
            patterns = [
                ("【堅実な上値余地】", "次の抵抗帯まで適度な距離があり、セオリー通りの着実な上昇が見込めます。"),
                ("【上値余地：中】", "極端な遠さではありませんが、壁に到達するまで十分に利益を狙える水準にあります。"),
                ("【標準的なターゲット】", "最も分厚い出来高の壁に向けて、じわじわと水準を切り上げる展開が期待されます。"),
                ("【ステップアップ局面】", "まずは直上の壁を目標に、資金の流入に伴って堅調に推移しやすい位置です。"),
                ("【適度な空間】", "壁までの距離感として「ちょうど狙いやすい」位置取り。押し目があれば拾いたい形状です。"),
                ("【トレンド追従向きの局面】", "上値抵抗までの道のりは見えており、無理のない範囲で波に乗るのが有効な局面です。")
            ]
        elif pot_level == 1:
            patterns = [
                ("【抵抗帯接近】", "すぐ上に出来高の壁が迫っています。ここを突破できるかが目先の最大の焦点となります。"),
                ("【激戦区への突入】", "過去の取引が密集するエリアが間近です。売り買いが交錯しやすく、乱高下に注意が必要です。"),
                ("【上値の壁テスト】", "分厚い壁へのアタック局面。跳ね返されるリスクも考慮し、打診買いから入りたい水準です。"),
                ("【ブレイク前夜警戒】", "すぐ上の抵抗線を明確に上抜ければ景色が一変しますが、現状はまだ重い壁の下に位置しています。"),
                ("【上値余地：小】", "ターゲットまでの距離が短く、ここから新規で大きな値幅を狙うにはややリスクが伴う位置です。"),
                ("【壁打ち反落リスク】", "壁にぶつかって反落する「壁打ち」になりやすい位置。突破を確認してからの参戦でも遅くありません。")
            ]
        else:
            patterns = [
                ("【頭打ち警戒】", "現在値のすぐ上に強烈なしこり玉が大量待機しており、上値が極めて重い状態です。"),
                ("【岩盤到達・上値重し】", "過去最大の出来高を記録した価格帯に突入しています。大量の戻り売りを消化する莫大なパワーが必要です。"),
                ("【ヤレヤレ売り集中エリア】", "「買値に戻ったら売ろう」と待っていた投資家の売りが降り注ぐ、最も苦しい価格帯です。"),
                ("【上値抵抗MAX】", "需給面での障壁が一番高いエリアです。好材料などの強力なエンジンがない限り、突破は困難です。"),
                ("【ブレイクアウト待ちが一案】", "この分厚い壁の中での勝負は分が悪いです。明確に上抜けて真空地帯に入るのを待つのが賢明です。"),
                ("【撤退ラインの徹底が重要】", "壁に跳ね返されて急落するリスクが高い水準です。保有している場合は利益確定も視野に入る位置と言えます。")
            ]

    selected_pattern = random.choice(patterns)
    star_desc = selected_pattern[0]
    base_logic = selected_pattern[1]

    flavor_logic = ""
    if cap_category == "large": flavor_logic = "時価総額が巨大なため値動きは重めですが、機関投資家や外国人投資家の資金流入をエンジンとした、強力で重厚なトレンドが期待できます。"
    elif cap_category == "target": flavor_logic = "中小型株として大口資金が最も好む規模感であり、資金が投下されれば一気に株価が動意づく（または壁を突破する）ポテンシャルを秘めています。"
    else: flavor_logic = "※ただし時価総額が小さすぎるため、プロは資金を入れづらい銘柄です。主に個人投資家による短期的な値幅取りの対象（乱高下）になりやすいため、リスク管理を徹底してください。"

    star_logic = base_logic + "<br><br>" + flavor_logic

    past_1y = hist[-250:]
    year_high = past_1y['High'].max()
    year_low = past_1y['Low'].min()
    position_score = 0.5
    if year_high != year_low:
        position_score = (current_price - year_low) / (year_high - year_low)
        
    has_dna = check_dna(hist)
    vol_ratio = current_vol / avg_vol_100 if avg_vol_100 > 0 else 0
    
    is_platinum = 500 <= market_cap_oku <= 2000
    is_magma = vol_ratio >= 1.5

    safe_judgment, safe_explain = "", ""
    if deviation <= -5.0:
        safe_judgment = "📉 割安：底値仕込みが適切とされるゾーン（任意）"
        safe_explain = "現在値が需給の壁より下に位置する「割安圏」です。直近底値（青の点線）を割ったら撤退というルールで、安値で仕込めるチャンスと言えます。"
    elif deviation <= 0.0:
        safe_judgment = "⚔️ 激戦：ブレイク前夜期待"
        safe_explain = "分厚い需給の壁へのアタック目前です。ここを明確に上抜ければ一気に青空が広がる激戦区と位置付けられます。"
    elif deviation <= 10.0:
        safe_judgment = "🚀 安全圏：トレンド初動かも！？"
        safe_explain = "需給の壁を突破したばかりで、最も素直に上昇の波に乗りやすいベストタイミングになりやすい水準です。"
    elif deviation <= 20.0:
        safe_judgment = "⚠️ 警戒：短期過熱気味警戒レベル"
        safe_explain = "壁の突破から一定の上昇をしており少し離れすぎました。壁付近までの「押し目（下落）」を待つのが無難です。"
    else:
        safe_judgment = "💀 高度な警戒：高値掴みリスク大"
        safe_explain = "壁から完全に乖離した超高値圏です。今から飛び乗るのは極めて危険で、上級者でも難しいゾーンのため注意必須です。"

    intervention_score = 0
    if is_platinum: intervention_score += 35
    elif 100 <= market_cap_oku <= 5000: intervention_score += 15
    if vol_ratio >= 3.0: intervention_score += 40
    elif vol_ratio >= 1.5: intervention_score += 25
    if position_score <= 0.2: intervention_score += 15
    if has_dna: intervention_score += 10
    
    intervention_score = int(round(min(intervention_score, 100) / 10.0)) * 10
    intervention_score = max(10, min(intervention_score, 90))
    
    intervention_comment = ""
    if intervention_score >= 80: intervention_comment = "🚨 【極めて濃厚】大規模な資金流入のシグナルが点灯しています。"
    elif intervention_score >= 50: intervention_comment = "👀 【予兆あり】平常時とは異なる資金の動きが観測されています。"
    else: intervention_comment = "💤 【静観】現在は目立った資金流入の動きは検出されていません。"

    base_rank = "D"
    if intervention_score >= 80 and (is_blue_sky or upside_potential >= 30): base_rank = "S"
    elif intervention_score >= 70: base_rank = "A"
    elif intervention_score >= 50 or (is_platinum and position_score <= 0.5): base_rank = "B"
    else: base_rank = "C"

    warning_text = "【注意】※安全性を要確認" if deviation > 20 else ""

    icons_list = []
    if has_dna: icons_list.append("🧬")
    if is_platinum: icons_list.append("💎")
    if is_magma: icons_list.append("🦅")
    icons_str = " ".join(icons_list)

    return {
        "コード": code_only, "銘柄名": jp_name, "現在値": int(current_price),
        "時価総額": market_cap_oku, "時価総額_表示": formatted_mcap, "dividend_text": dividend_text,
        "turnover_str": turnover_str, "ランク": base_rank, "警告": warning_text,
        "乖離率": deviation, "hist": hist, "max_vol_price": max_vol_price,
        "recent_20_low": recent_20_low, "star_rating": star_rating, "star_desc": star_desc,
        "star_logic": star_logic, "intervention_name": intervention_name,
        "intervention_score": intervention_score, "intervention_comment": intervention_comment,
        "safe_judgment": safe_judgment, "safe_explain": safe_explain, "icons_str": icons_str
    }

# 🚨 【呼び出し元関数】エラー時はキャッシュせずに例外を受け流す
def evaluate_stock(ticker):
    try:
        return _evaluate_stock_cached(ticker)
    except Exception:
        return None

def draw_chart(row, chart_key: str | None = None):
    hist_data = row['hist'].tail(150)
    max_vol_price = row['max_vol_price']
    recent_20_low = row['recent_20_low']
    
    bins = 15
    hist_data_copy = hist_data.copy()
    hist_data_copy['price_bins'] = pd.cut(hist_data_copy['Close'], bins=bins)
    vol_profile = hist_data_copy.groupby('price_bins', observed=False)['Volume'].sum()
    bin_centers = [b.mid for b in vol_profile.index]
    bin_volumes = vol_profile.values
    
    fig = make_subplots(rows=1, cols=2, shared_yaxes=True, column_widths=[0.85, 0.15], horizontal_spacing=0)
    fig.add_trace(go.Candlestick(x=hist_data.index, open=hist_data['Open'], high=hist_data['High'], low=hist_data['Low'], close=hist_data['Close'], name="株価", showlegend=False), row=1, col=1)
    fig.add_trace(go.Bar(x=bin_volumes, y=bin_centers, orientation='h', marker_color='rgba(255, 165, 0, 0.6)', name="出来高ボリューム", showlegend=False, hoverinfo='y'), row=1, col=2)
    
    fig.add_hline(y=max_vol_price, line_width=2, line_dash="dash", line_color="orange", 
                  annotation_text=f" {int(max_vol_price)}円 🚧 需給の壁 ", 
                  annotation_position="top left", annotation_font_color="orange", row=1, col=1)
    fig.add_hline(y=max_vol_price, line_width=2, line_dash="dash", line_color="orange", row=1, col=2)
    
    fig.add_hline(y=recent_20_low, line_width=1.5, line_dash="dot", line_color="cyan", 
                  annotation_text=f" 直近底値(1ヶ月) 🔵 {int(recent_20_low)}円 ", 
                  annotation_position="bottom right", annotation_font_color="cyan", row=1, col=1)
    fig.add_hline(y=recent_20_low, line_width=1.5, line_dash="dot", line_color="cyan", row=1, col=2)

    fig.update_layout(
        title=f"{row['銘柄名']} 日足 ＆ 価格帯別出来高", 
        xaxis_rangeslider_visible=False, height=350, margin=dict(l=0, r=0, t=30, b=0), dragmode=False,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
    )
    fig.update_xaxes(fixedrange=True); fig.update_yaxes(fixedrange=True)
    fig.update_xaxes(showticklabels=False, row=1, col=2)
    
    if chart_key:
        try:
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False}, key=chart_key)
        except TypeError:
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    else:
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

def render_card(ticker: str, d: Dict):
    flow_score = d.get("flow_score", 0)
    level = int(d.get("level", 0))
    state = d.get("display_state", d.get("state", "観測中"))
    
    _state_clean = _norm_label(state) or str(state).strip()
    _tip = STATE_HELP.get(_state_clean, "状態の目安です。").replace('"', "&quot;")
    state_html = f'<span title="{_tip}" style="color:#5C6BC0;font-weight:800;">{_state_clean}</span>' if _state_clean == "要監視" else f'<span title="{_tip}">{_state_clean}</span>'

    tags = d.get("tags", [])
    if flow_score >= FLOW_SCORE_HIGH: card_class, score_class = "high", "high"
    elif flow_score >= FLOW_SCORE_MEDIUM: card_class, score_class = "medium", "medium"
    else: card_class, score_class = "", "normal"

    level_color = LEVEL_COLORS.get(level, "#9E9E9E")
    
    code_only = ticker.replace(".T", "")
    url = f"https://finance.yahoo.co.jp/quote/{code_only}.T"
    name_jp = get_display_japanese_name(ticker, d.get("name"), allow_yahoo_fallback=False)

    tags_html = ""
    for tag in tags[:4]:
        if tag == "要監視":
            tags_html += f'<span class="tag-watch">要監視</span>'
        else:
            tags_html += f'<span class="tag-normal">{tag}</span>'

    score_text = f"{flow_score}"
    level_text = f"LEVEL {level}" if level > 0 else "LEVEL -"

    st.markdown(f"""
    <div class="spike-card {card_class}">
        <div class="card-header">
            <div class="ticker-name">
                <a href="{url}" target="_blank">{code_only}</a>
                <span class="ticker-jp-name">{str(name_jp)[:12]}</span>
            </div>
            <div style="display:flex;align-items:center;gap:8px;">
                <span class="level-badge" style="background:{level_color};">{level_text}</span>
                <div class="ratio-badge {score_class}" title="※直近の出来高変化等を示す独自スコア">
                    <span class="score-label">需給スコア</span>
                    <span class="score-val">{score_text}</span>
                </div>
            </div>
        </div>
        <div class="card-body">
            <div><span class="info-label">現在値</span><br><span class="info-value price-val">¥{d.get('price',0):,.0f}</span></div>
            <div><span class="info-label">状態</span><br><span class="info-value">{state_html}</span></div>
            <div><span class="info-label">時価総額</span><br><span class="info-value">{d.get('market_cap_oku',0):,}億円</span></div>
            <div><span class="info-label">出来高</span><br><span class="info-value">{d.get('vol_ratio', 0)}x</span></div>
        </div>
        <div class="tag-container">{tags_html}</div>
    </div>
    """, unsafe_allow_html=True)

    cart_list = st.session_state.get("cart", [])
    cart_len = len(cart_list)
    
    if ticker in cart_list:
        btn_text = f"🗑️ 診断カートから外す ({cart_len}/5)"
        st.button(btn_text, key=f"cart_rm_{ticker}", use_container_width=True, on_click=remove_from_cart, args=(ticker,))
    else:
        is_full = cart_len >= 5
        btn_text = "🛒 カートの上限に達しました (5/5)" if is_full else f"🛒 診断カートに入れる ({cart_len}/5)"
        st.button(btn_text, key=f"cart_add_{ticker}", use_container_width=True, disabled=is_full, type="primary", on_click=add_to_cart, args=(ticker,))

# ==========================================
# 画面遷移
# ==========================================
def show_login_page():
    logo_base64 = get_logo_base64()
    st.markdown("<div style='height: 40px;'></div>", unsafe_allow_html=True)
    
    _, col2, _ = st.columns([1, 2, 1])
    with col2:
        if logo_base64:
            st.markdown(f'<div style="text-align: center; margin-bottom: 1.5rem;"><img src="data:image/png;base64,{logo_base64}" class="logo-img" style="max-width: 280px; width: 90%;"></div>', unsafe_allow_html=True)
        else:
            st.markdown("<h1 style='text-align:center;'>🦅 源太AI ハゲタカSCOPE</h1>", unsafe_allow_html=True)
        
        if st.session_state.get("login_error"):
            st.error("❌ 認証に失敗しました。入力内容をご確認ください。")
            
        tab1, tab2 = st.tabs(["🔑 アプリを利用する", "⚙️ 通知設定の呼び出し"])
        
        with tab1:
            st.markdown("<div style='text-align:center; padding: 1rem 0 0 0;'><p style='opacity:0.7; font-size:0.85rem; margin-bottom: 0.5rem;'>共通パスワードを入力して候補一覧を閲覧します</p></div>", unsafe_allow_html=True)
            pw_input = st.text_input("パスワード", placeholder="共通パスワードを入力", type="password", key="login_pw")
            st.markdown(f'<div class="disclaimer-box"><strong>⚠️ 免責事項</strong><br>{DISCLAIMER_TEXT}</div><div style="text-align:center; margin-bottom: 10px;"><span style="font-size: 0.8rem; font-weight: bold; color: #DC2626;">※ログインすることで上記に同意したものとみなします。</span></div>', unsafe_allow_html=True)
            if st.button("ログインして利用開始", use_container_width=True, type="primary"):
                if pw_input == MASTER_PASSWORD:
                    st.cache_data.clear()
                    st.session_state.update({"logged_in": True, "login_error": False, "login_type": "master"})
                    st.rerun()
                else:
                    st.session_state["login_error"] = True
                    st.rerun()
                    
        with tab2:
            st.markdown("<div style='text-align:center; padding: 1rem 0;'><p style='opacity:0.7; font-size:0.85rem; margin-bottom: 1rem;'>登録済みのメールアドレスを入力して、<br>通知先や設定を変更・停止します</p></div>", unsafe_allow_html=True)
            email_input = st.text_input("登録済みメールアドレス", placeholder="example@gmail.com", key="login_email")
            st.markdown(f'<div class="disclaimer-box" style="margin-top:0.5rem;"><strong>⚠️ 免責事項</strong><br>{DISCLAIMER_TEXT}</div>', unsafe_allow_html=True)
            if st.button("設定を呼び出す（同意して進む）", use_container_width=True):
                try:
                    settings = load_settings_by_email(email_input)
                    if settings:
                        st.cache_data.clear()
                        st.session_state.update({"logged_in": True, "login_error": False, "login_type": "email", "email_address": settings["email"], "app_password": decrypt_password(settings["encrypted_password"])})
                        st.rerun()
                    else:
                        st.session_state["login_error"] = True
                        st.rerun()
                except:
                    st.session_state["login_error"] = True
                    st.rerun()

def show_main_page():
    if "flt_level_select" not in st.session_state:
        st.session_state["flt_level_select"] = "すべて"
    if "flt_watch_only" not in st.session_state:
        st.session_state["flt_watch_only"] = False

    logo_base64 = get_logo_base64()
    if logo_base64:
        st.markdown(f'<div style="text-align: center; margin-bottom: 0.5rem;"><img src="data:image/png;base64,{logo_base64}" class="logo-img" style="max-width: 320px; width: 80%;"></div>', unsafe_allow_html=True)
    else:
        st.title("🦅 HAGETAKA SCOPE")
    st.markdown(f'<p class="subtitle">M&A候補の早期検知ツール（時価総額{MARKET_CAP_MIN}億〜{MARKET_CAP_MAX}億円）</p>', unsafe_allow_html=True)
    
    data = load_data()
    
    tab1, tab2, tab3 = st.tabs(["📊 M&A候補", "🦅 ハゲタカ診断", "🔔 通知設定"])
    
    # ==========================================
    # タブ1: M&A候補
    # ==========================================
    with tab1:
        with st.expander("💡 LEVELと需給スコアの見方", expanded=False):
            st.markdown("""
            **■ LEVEL（0〜4）**
            時価総額やPBRなどの「財務的特徴」と、決算や出来高などの「市場データ変化」に基づく独自基準の合算評価です。
            * **LEVEL 4 (赤)** : 複数の抽出条件を同時に満たす、データ上の特異性が高い状態。
            * **LEVEL 3 (橙)** : 一定の出来高変化と、特徴的な財務指標が観測される状態。
            * **LEVEL 2 (黄)** : 平常時とは異なる、何らかのデータ変化が観測された状態。
            
            **■ 需給スコア（0〜100）**
            直近の「出来高の急増」や「価格変動の幅」などから、市場における取引の活発化度合いを数値化した独自の指標です。
            * **70以上 (赤)** : 過去の平均と比較して、非常に強い出来高変化のシグナルが点灯している状態。
            * **40以上 (橙)** : 平常時よりも商いが膨らみ、市場の関心を集めていると推測される状態。
            """)

        if data:
            updated_at = data.get("updated_at", "不明")
            st.caption(f"📡 最終更新: {updated_at}")
            
            show_all = st.checkbox("中型株以外も表示", value=False)
            display_data = data.get("all_data", {}) if show_all else data.get("data", {})
            display_data = {tk: _normalize_item(it) for tk, it in (display_data or {}).items()}

            lvl4 = len([v for v in display_data.values() if int(v.get("level", 0)) == 4])
            lvl3p = len([v for v in display_data.values() if int(v.get("level", 0)) >= 3])
            
            st.markdown(f"""
            <div style="display: flex; justify-content: space-around; align-items: center; background-color: var(--secondary-background-color); 
                        border: 1px solid rgba(128,128,128,0.2); border-radius: 12px; padding: 0.8rem; margin-bottom: 0.5rem; 
                        box-shadow: 0 4px 15px rgba(0,0,0,0.05);">
                <div style="text-align: center;">
                    <div style="opacity: 0.7; font-size: 0.75rem; font-weight: 700;">LEVEL 4</div>
                    <div style="color: #C41E3A; font-size: 1.4rem; font-weight: 900;">{lvl4}<span style="font-size: 0.8rem; opacity: 0.6; font-weight: 600; margin-left: 2px;">件</span></div>
                </div>
                <div style="width: 1px; height: 40px; background: rgba(128,128,128,0.2);"></div>
                <div style="text-align: center;">
                    <div style="opacity: 0.7; font-size: 0.75rem; font-weight: 700;">LEVEL 3+</div>
                    <div style="color: #FF9800; font-size: 1.4rem; font-weight: 900;">{lvl3p}<span style="font-size: 0.8rem; opacity: 0.6; font-weight: 600; margin-left: 2px;">件</span></div>
                </div>
                <div style="width: 1px; height: 40px; background: rgba(128,128,128,0.2);"></div>
                <div style="text-align: center;">
                    <div style="opacity: 0.7; font-size: 0.75rem; font-weight: 700;">総検出数</div>
                    <div style="font-size: 1.4rem; font-weight: 900;">{len(display_data)}<span style="font-size: 0.8rem; opacity: 0.6; font-weight: 600; margin-left: 2px;">件</span></div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            _, center_col, _ = st.columns([1, 1.8, 1])
            with center_col:
                st.markdown('<div class="reset-btn-container">', unsafe_allow_html=True)
                cart_len = len(st.session_state.get("cart", []))
                st.button(f"🗑️ カートを空にする ({cart_len}/5)", use_container_width=True, on_click=clear_cart)
                st.markdown('</div>', unsafe_allow_html=True)
                
                with st.popover("🔎 フィルターを開く", use_container_width=True):
                    level_options = ["すべて", "LEVEL 4 のみ", "LEVEL 3 以上", "LEVEL 2 以上", "LEVEL 1 以上"]
                    current_idx = level_options.index(st.session_state["flt_level_select"]) if st.session_state["flt_level_select"] in level_options else 0
                    
                    st.session_state["flt_query"] = st.text_input("検索", value=st.session_state.get("flt_query", ""))
                    level_opt = st.selectbox("LEVEL絞り込み", options=level_options, index=current_idx)
                    watch_opt = st.toggle("要監視のみ", value=st.session_state.get("flt_watch_only", False))
                    
                    st.session_state["flt_level_select"] = level_opt
                    st.session_state["flt_watch_only"] = watch_opt
                    
                    if st.button("🔄 デフォルトに戻す", use_container_width=True):
                        st.session_state.update({"flt_level_select": "すべて", "flt_watch_only": False, "flt_query": ""})
                        st.rerun()

                chips = []
                lvl_sel = st.session_state.get("flt_level_select", "すべて")
                if lvl_sel != "すべて": chips.append(lvl_sel)
                if st.session_state.get("flt_watch_only", False): chips.append("要監視のみ")
                if st.session_state.get("flt_query", ""): chips.append(f"検索: {st.session_state['flt_query']}")
                
                if chips: 
                    st.markdown(f"<div style='text-align:center; font-size:0.8rem; opacity:0.7; margin-top:8px;'>✅ 適用中: {' / '.join(chips)}</div>", unsafe_allow_html=True)

            st.markdown("---")
            
            q = (st.session_state.get("flt_query") or "").strip().lower()
            w_only = bool(st.session_state.get("flt_watch_only", False))
            lvl_sel = st.session_state.get("flt_level_select", "すべて")
            
            filtered_data = {}
            for tk, it in display_data.items():
                lv = int(it.get("level", 0))
                if lvl_sel == "LEVEL 4 のみ" and lv < 4: continue
                elif lvl_sel == "LEVEL 3 以上" and lv < 3: continue
                elif lvl_sel == "LEVEL 2 以上" and lv < 2: continue
                elif lvl_sel == "LEVEL 1 以上" and lv < 1: continue
                if w_only and not _is_watch(it): continue
                if q and q not in f"{tk} {(it.get('name') or '')}".lower(): continue
                filtered_data[tk] = it

            if filtered_data:
                # LEVEL 1から昇順に並べる（同じLEVELなら需給スコア降順）
                sorted_items = sorted(filtered_data.items(), key=lambda x: (int(x[1].get('level',0)), -float(x[1].get('flow_score',0))))
                for ticker, d in sorted_items:
                    render_card(ticker, d)
            else:
                st.info("該当する銘柄がありません")
                
            current_cart_len = len(st.session_state.get('cart', []))
            
            # 🌟 フローティングボタンの色（文字色は絶対に白）
            if current_cart_len >= 5:
                btn_text = "🚨 カート満杯！上に戻って【診断】へ"
                btn_bg = "linear-gradient(135deg, #C41E3A 0%, #E63946 100%)"
                btn_shadow = "0 10px 30px rgba(196, 30, 58, 0.5)"
            else:
                btn_text = f"🛒 カート: {current_cart_len}/5件 🔼 上に戻る"
                btn_bg = "linear-gradient(135deg, #3B82F6 0%, #2563EB 100%)"
                btn_shadow = "0 10px 25px rgba(59, 130, 246, 0.4)"

            st.markdown(f"""
            <style>
            .floating-jump-btn {{
                position: fixed;
                bottom: 25px;
                right: 30px;
                background: {btn_bg};
                color: #FFFFFF !important; /* 絶対に白文字 */
                padding: 14px 24px;
                border-radius: 50px;
                font-weight: 800;
                font-size: 1.05rem;
                text-decoration: none;
                box-shadow: {btn_shadow};
                z-index: 999999;
                border: 2px solid rgba(255,255,255,0.3);
                transition: transform 0.2s ease, box-shadow 0.2s ease;
                display: flex;
                align-items: center;
                justify-content: center;
                gap: 8px;
            }}
            .floating-jump-btn:hover {{
                transform: translateY(-5px);
                box-shadow: 0 15px 35px rgba(0,0,0,0.5);
            }}
            @media (max-width: 768px) {{
                .floating-jump-btn {{
                    bottom: 20px;
                    right: 50%;
                    transform: translateX(50%);
                    width: 90%;
                    font-size: 1rem;
                    padding: 12px 20px;
                }}
                .floating-jump-btn:hover {{
                    transform: translate(50%, -5px);
                }}
            }}
            </style>
            <a href="#top-of-page" target="_self" class="floating-jump-btn">{btn_text}</a>
            """, unsafe_allow_html=True)
        else:
            st.info("データがありません。GitHub Actionsを実行してください。")

    # ==========================================
    # タブ2: ハゲタカ診断（＋戦略室）
    # ==========================================
    with tab2:
        st.markdown("### 🦅 ハゲタカAI 診断室")
        
        with st.expander("🦅 ハゲタカ戦略室（記号の解説 ＆ 相場カレンダー）を開く", expanded=False):
            strat_tab1, strat_tab2 = st.tabs(["🦅 記号の解説", "📅 2026年 戦略カレンダー"])
            
            with strat_tab1:
                st.markdown("""
                <ul style='font-size: 0.9rem; line-height: 1.6; margin-top: 10px; opacity: 0.9;'>
                    <li style='margin-bottom: 8px;'><b>💎 プラチナ (Platinum)</b><br>時価総額 <b>500億～2000億円</b><br><span style='opacity: 0.7;'>大口資金が最も仕掛けやすいとされる規模感。</span></li>
                    <li style='margin-bottom: 8px;'><b>🦅 大口資金参入？</b><br>出来高急増（平常時の1.5倍以上）<br><span style='opacity: 0.7;'>水面下での「仕込み」が疑われる状態。</span></li>
                    <li><b>🧬 DNA（習性）</b><br>過去に短期間で急騰した実績あり。<br><span style='opacity: 0.7;'>値動きを主導する特定の資金が存在する可能性あり。</span></li>
                </ul>
                """, unsafe_allow_html=True)
                
            with strat_tab2:
                current_month = datetime.now(JST).month
                strategy_text = {
                    1: "⚠️ **1月：資金温存**\n外国人買いが入りますが、3月の調整に備えて現金比率を高めましょう。",
                    2: "⚠️ **2月：様子見**\n無理に動く時期ではありません。監視銘柄の選定に集中。",
                    3: "📉 **3月：換金売り警戒＆仕込み**\n中旬の調整は「優良株」を拾う最大のチャンス！",
                    4: "🔥 **4月：ニューマネー流入**\n新年度予算で中小型株が動意づきます。3月の仕込みを利益に。",
                    5: "🔥 **5月：セルインメイの裏をかく**\n決算後の「材料出尽くし」による下落は、資金集めの好機です。",
                    6: "💰 **6月：ボーナス・配当再投資**\n資金潤沢. 大型株へシフトする時期。",
                    7: "💰 **7月：サマーラリー**\n夏枯れ前の最後のひと稼ぎ。",
                    8: "🌊 **8月：夏枯れ・真空地帯**\n市場参加者不在. AIによるフラッシュクラッシュ（急落）のみ警戒。",
                    9: "📉 **9月：彼岸底**\n10月の大底に向けた調整。",
                    10: "🔥 **10月：年内最後の大底**\nここから年末ラリーへ. 全力買いの急所。",
                    11: "🍂 **11月：節税売り（タックスロス）**\n手仕舞い売りされた銘柄を拾う。",
                    12: "🎉 **12月：掉尾の一振**\n年末ラリーで全てを利益に変えて逃げ切る。"
                }
                st.info(f"**今月の戦略 ({current_month}月)：**\n{strategy_text.get(current_month, '戦略待機中')}")
                
                with st.expander("年間カレンダーをすべて見る"):
                    for m, text in strategy_text.items():
                        if m == current_month:
                            st.markdown(f"**👉 {text}**")
                        else:
                            st.markdown(text)

        with st.expander("🔰 【源太AI・各項目の見方と算出ロジック】"):
            st.markdown("""
            #### ① 🦅 大口介入期待度（％メーター）
            **「市場の関心がこの銘柄に向かっている可能性」**をデータから推測します。(時価総額の規模、異常出来高、値動きの煮詰まり、過去のボラティリティ等から算出)。
            
            #### ② 🌟 上値の需給の壁までの余地（★マーク）
            **「過去の取引が多く行われた価格帯（需給の壁）までの距離」**を示します。星が多いほど、直近で戻り売り（ヤレヤレ売り）が出やすい価格帯までの余地があることを意味します。
            
            #### ③ 🚧 安全性（壁からの乖離と撤退ライン）
            **「現在値が『最大の需給の壁』から何%離れているか」**を示します。マイナス圏は壁の下にある状態であり、直近底値（青の点線）をリスク管理の目安（撤退ライン）として活用できるポイントです。
            
            #### ④ 📊 チャート ＆ 価格帯別出来高（右側の横棒）
            チャートの右側は、**過去半年間で「どの価格帯でどれだけ取引されたか」**を表します。一番棒が長いオレンジの点線が**『最も取引が密集した価格帯（需給の壁）』**です。
            <span style='color: #ffaa00; font-weight: bold;'>⚠️注意: オレンジの線を下回っている場合は、含み損を抱えた投資家の戻り売り圧力が警戒されるため、直近底値などの『下値支持線』を意識したリスク管理が重要です。</span>
            """, unsafe_allow_html=True)
            
        cart_codes = [code.replace(".T", "") for code in st.session_state.get("cart", [])]
        default_input = " ".join(cart_codes)
        
        st.markdown("##### 🔍 気になる銘柄を入力")
        with st.form(key='search_form'):
            # 🌟 入力欄をコピペしやすくしつつ、高さを最小限に抑えて見た目を維持
            input_code = st.text_area("銘柄コード", value=default_input, placeholder="例: 7011 7203 151A\n改行やスペース区切りで複数入力できます", label_visibility="collapsed", height=68)
            search_btn = st.form_submit_button("🦅 ハゲタカAIで診断する")
            
        if search_btn and input_code:
            codes = normalize_input(input_code)
            if not codes: 
                st.error("銘柄コードを入力してください")
            elif len(codes) > 5:
                st.error("⚠️ サーバー負荷軽減のため、一度に診断できるのは最大5銘柄までです。銘柄数を減らして再度お試しください。")
            else:
                with st.spinner(f'🦅 {len(codes)}銘柄を精密検査中...'):
                    for code in codes:
                        diag_data = evaluate_stock(f"{code}.T")
                        if diag_data:
                            # 💡 診断結果をカードで囲んで区切りを明確に
                            with st.container():
                                st.markdown('<div class="diagnosis-card-marker" style="display:none;"></div>', unsafe_allow_html=True)
                                
                                c1, c2 = st.columns([1, 2])
                                with c1:
                                    st.markdown(f"<h2 style='margin-bottom: 0px;'>{diag_data['icons_str']} {diag_data['コード']} {diag_data['銘柄名']}</h2>", unsafe_allow_html=True)
                                    
                                    base_rank = diag_data['ランク']
                                    warning = diag_data['警告']
                                    rank_color = "red" if base_rank == "S" else "orange" if base_rank == "A" else "#3B82F6"
                                    
                                    if warning:
                                        rank_html = f"<h3 style='color:{rank_color}; margin-top: 5px;'>総合判定: {base_rank} <span style='color:#ff4b4b; font-size:0.8em;'>{warning}</span></h3>"
                                    else:
                                        rank_html = f"<h3 style='color:{rank_color}; margin-top: 5px;'>総合判定: {base_rank}</h3>"
                                    
                                    st.markdown(rank_html, unsafe_allow_html=True)
                                    
                                    with st.expander("💡 総合判定の基準を見る", key=f"diag_exp_rank_{code}"):
                                        st.markdown("""
                                        * **【Sランク】** 大口介入期待度80%以上 ＋ 上昇期待値(上値余地)30%以上
                                        * **【Aランク】** 大口介入期待度70%以上（強い資金流入シグナル）
                                        * **【Bランク】** 大口介入期待度50%以上、または プラチナサイズ(500〜2000億) ＋ 底値圏での煮詰まり
                                        * **【Cランク】** 上記以外の標準的な状態
                                        * **【注意】** 需給の壁から20%以上乖離している場合、過熱感のアラートが表示されます
                                        """)

                                    st.write(f"現在値: **{diag_data['現在値']}** 円")
                                    st.write(f"時価総額: **{diag_data['時価総額_表示']}**")
                                    st.write(f"配当情報: **{diag_data['dividend_text']}**")
                                    st.write(f"商い熱量: **{diag_data['turnover_str']}**")
                                    
                                    with st.expander("💡 商い熱量（株式回転率）とは？", key=f"diag_exp_turnover_{code}"):
                                        st.markdown("""
                                        **商い熱量 ＝ 出来高が総発行株数の何％にあたるか（株式回転率）**
                                        この数値は、株価が動く「エネルギーの大きさ」を見極めるための重要なテクニカル指標です。
                                        
                                        * **① 資金流入の規模感の把握**
                                          前日比で出来高が増えていても、発行済株数に対してごくわずかであれば限定的な動きです。しかし、1日で「5%」や「10%」が取引されていたら、明確な資金介入と株主構成の変化を伴う大きなトレンドの初動（または終焉）の可能性を示唆します。
                                        * **② 流動性（浮動株）の消化具合**
                                          発行済株数の中には、市場に出回らない「固定株」があります。発行済株数の5%の出来高があったということは、実際に市場に出回っている株（浮動株）の10%〜20%が1日で入れ替わった計算になり、極めて活発な商いと言えます。
                                        * **③ 需給の壁（戻り売り）の突破力**
                                          上値に過去の取引が密集する壁（戻り売り圧力）があったとしても、この商い熱量が異常に高ければ、その売り圧力を吸収して上昇するだけのエネルギーが市場に存在することの裏付けとなります。
                                        """)
                                    
                                    st.markdown("---")
                                    st.markdown(f"### {diag_data['intervention_name']}: {diag_data['intervention_score']}%")
                                    try:
                                        st.progress(diag_data['intervention_score'] / 100.0, key=f"diag_prog_iv_{code}")
                                    except TypeError:
                                        st.progress(diag_data['intervention_score'] / 100.0)
                                    st.markdown(f"**{diag_data['intervention_comment']}**")
                                    
                                with c2:
                                    st.markdown("##### 📋 AI診断カルテ")
                                    st.markdown(f"#### {diag_data['star_rating']} {diag_data['star_desc']}")
                                    
                                    st.markdown(f"""
                                    <div style="background-color: rgba(59, 130, 246, 0.1); padding: 15px; border-left: 5px solid #3B82F6; border-radius: 5px; margin-bottom: 15px; font-size: 0.95rem; line-height: 1.6;">
                                    {diag_data['star_logic']}
                                    </div>
                                    """, unsafe_allow_html=True)
                                    
                                    st.markdown("---")
                                    st.markdown(f"<h3 style='font-size: 1.2rem; font-weight: bold;'>🛡️ 安全性（需給の壁からの乖離率）: {diag_data['乖離率']:.1f}%</h3>", unsafe_allow_html=True)
                                    st.markdown(f"<div style='color: {'#ff4b4b' if diag_data['乖離率'] > 10 else '#3B82F6'}; background-color: rgba(128, 128, 128, 0.08); padding: 10px; border-radius: 5px;'><strong>💡 AI解説:</strong> {diag_data['safe_explain']}</div>", unsafe_allow_html=True)
                                    st.markdown(f"**（判定: {diag_data['safe_judgment']}）**")
                                    
                                    with st.expander("💡 安全性（壁からの乖離と撤退ライン）の見方を見る", key=f"diag_exp_safe_{code}"):
                                        safe_explain_html = f"""
                                        <div style='font-size: 0.95rem; line-height: 1.6;'>
                                        当ツールでは、安全性を<strong>「最大の需給の壁（オレンジの点線）」からの乖離率（％）</strong>で判定します。<br>
                                        マイナス圏（壁より下）は過去のしこり玉を恐れて一般投資家が手を出せない「割安圏」であり、大口資金が水面下で仕込むポイントになりやすいです。<br><br>
                                        <span style='color: #3B82F6; font-weight: bold;'>【🛡️プロのリスク管理】マイナス圏で仕込む場合は、直近の底値（青の点線）を下回ったら「シナリオ崩れ」として撤退（損切り）を検討することで、リスク管理の目安としてお使いください。</span><br><br>
                                        <strong>【AIの判定基準一覧】</strong><br>
                                        ・<strong>-5.0%以下 【📉 割安】</strong> 底値仕込みが適切とされるゾーン（任意）<br>
                                        ・<strong>0.0%以下 【⚔️ 激戦】</strong> ブレイク前夜期待<br>
                                        ・<strong>+10.0%以内 【🚀 安全圏】</strong> トレンド初動かも！？<br>
                                        ・<strong>+20.0%以内 【⚠️ 警戒】</strong> 短期過熱気味警戒レベル<br>
                                        ・<strong>+20.1%以上 【💀 高度な警戒】</strong> 高値掴みリスク大
                                        </div>
                                        """
                                        st.markdown(safe_explain_html, unsafe_allow_html=True)

                                draw_chart(diag_data, chart_key=f"hagetaka_chart_{code}")
                        else: 
                            # 🚨 ここが確実に表示されるように修正
                            st.error(f"❌ 【 {code} 】 : データが取得できませんでした。\n\n※存在しない銘柄、または**アクセス集中による一時的な通信制限**の可能性があります。しばらく時間を空けてから再度お試しください。")

    # ==========================================
    # タブ3: 通知設定
    # ==========================================
    with tab3:
        st.markdown("### 🔔 メール通知設定")
        st.info("※設定したメールアドレス宛に、毎日の分析結果（該当銘柄がある場合のみ）が自動送信されます。")
        current_email = st.session_state.get("email_address", "")
        if current_email: st.success(f"現在、**{current_email}** の設定を呼び出し中です。")
            
        email = st.text_input("Gmailアドレス", value=current_email, placeholder="example@gmail.com")
        app_password = st.text_input("アプリパスワード（16桁）", value=st.session_state.get("app_password", ""), type="password", placeholder="xxxx xxxx xxxx xxxx")
        
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("💾 新規登録・更新", use_container_width=True):
                if email and app_password:
                    with st.spinner("保存中..."):
                        if save_settings_to_sheet(email, app_password):
                            st.session_state["email_address"] = email.lower().strip()
                            st.session_state["app_password"] = app_password
                            st.success("✅ 設定を保存しました！")
                        else: st.error("❌ 保存に失敗しました")
                else: st.warning("入力してください")
        with c2:
            if st.button("🧪 テスト送信", use_container_width=True):
                if email and app_password:
                    with st.spinner("送信中..."):
                        ok, msg = send_test_email(email, app_password)
                        if ok: st.success(f"✅ {msg}")
                        else: st.error(f"❌ {msg}")
                else: st.warning("入力してください")
        with c3:
            if st.button("🗑️ 通知を停止（削除）", use_container_width=True):
                if email:
                    with st.spinner("削除中..."):
                        if delete_settings_from_sheet(email):
                            st.session_state.update({"email_address": "", "app_password": ""})
                            st.success("✅ 登録情報を削除し、通知を停止しました。")
                        else: st.error("❌ 削除に失敗したか、登録されていません。")
                else: st.warning("メールアドレスを入力してください")
                
        st.markdown("---")
        if st.button("🚪 ログアウトしてトップへ", type="primary"):
            st.cache_data.clear()
            st.session_state.update({"logged_in": False, "login_type": None, "email_address": "", "app_password": "", "cart": []})
            st.rerun()

# ==========================================
# メイン処理
# ==========================================
if "logged_in" not in st.session_state: st.session_state["logged_in"] = False
if "cart" not in st.session_state: st.session_state["cart"] = []
if st.session_state.get("logged_in"): show_main_page()
else: show_login_page()
