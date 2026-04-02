"""
源太AI🤖ハゲタカSCOPE v3
─────────────────────────
・KABU+ 一括データ取得（数秒で全銘柄）
・ニュースはキャッシュ表示（GitHub Actions でバックグラウンド取得）
・Streamlit Secrets で認証管理
"""

import re
import unicodedata
import time
from datetime import datetime
import pandas as pd
import numpy as np
import streamlit as st

import kabuplus_client as kp
import hagetaka_scanner as scanner
import ma_detector as ma
import notifier

# ==========================================
# 🔑 パスワード（Secrets 優先）
# ==========================================
def _get_password(key: str, default: str) -> str:
    try:
        return st.secrets["app"][key]
    except Exception:
        return default

LOGIN_PASSWORD = _get_password("login_password", "88888")
ADMIN_CODE = _get_password("admin_code", "888888")

# ==========================================
# UI 設定
# ==========================================
st.set_page_config(
    page_title="源太AI🤖ハゲタカSCOPE",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ==========================================
# 🎨 CSS（既存デザイン維持）
# ==========================================
st.markdown("""
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
.stDeployButton {display:none;}

div[data-testid="stAppViewContainer"] {
    background: linear-gradient(180deg, #FAFAFA 0%, #FFF5F5 100%) !important;
}
.main .block-container {
    max-width: 1200px !important;
    padding: 1rem 2rem 3rem 2rem !important;
    margin: 0 auto !important;
}
h1 {
    text-align: center !important;
    font-size: 2rem !important;
    font-weight: 800 !important;
    background: linear-gradient(135deg, #C41E3A 0%, #E85A71 100%) !important;
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
    background-clip: text !important;
    margin-bottom: 0.5rem !important;
}
.subtitle {
    text-align: center; color: #666;
    font-size: 0.95rem; margin-bottom: 1.5rem;
}
.stTabs [data-baseweb="tab-list"] {
    justify-content: center !important; gap: 0 !important;
    background-color: #FFF !important; padding: 0.4rem !important;
    border-radius: 16px !important;
    box-shadow: 0 2px 12px rgba(196,30,58,0.1) !important;
    margin-bottom: 1.5rem !important;
}
.stTabs [data-baseweb="tab"] {
    padding: 0.75rem 2rem !important; border-radius: 12px !important;
    font-weight: 600 !important; font-size: 1rem !important;
    color: #666 !important; transition: all 0.3s ease !important;
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #C41E3A 0%, #E85A71 100%) !important;
    color: white !important;
    box-shadow: 0 4px 12px rgba(196,30,58,0.3) !important;
}
.stTabs [data-baseweb="tab-highlight"],
.stTabs [data-baseweb="tab-border"] { display: none !important; }
div.stButton { text-align: center !important; }
div.stButton > button:first-child {
    background: linear-gradient(135deg, #C41E3A 0%, #E85A71 100%) !important;
    color: white !important; font-weight: 700 !important;
    border-radius: 12px !important; border: none !important;
    padding: 0.85rem 2.5rem !important;
    box-shadow: 0 4px 15px rgba(196,30,58,0.25) !important;
    transition: all 0.3s ease !important; font-size: 1.05rem !important;
}
div.stButton > button:hover {
    transform: translateY(-3px) !important;
    box-shadow: 0 8px 25px rgba(196,30,58,0.35) !important;
}
.stock-card {
    background: white; border-radius: 16px; padding: 1.25rem;
    margin: 0.75rem 0; box-shadow: 0 2px 12px rgba(0,0,0,0.06);
    border-left: 5px solid #C41E3A; transition: all 0.2s ease;
}
.stock-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 20px rgba(196,30,58,0.15);
}
.stock-card.lockon { border-left-color: #C41E3A; background: linear-gradient(135deg,#FFF 0%,#FFF5F5 100%); }
.stock-card.high   { border-left-color: #F97316; background: linear-gradient(135deg,#FFF 0%,#FFF7ED 100%); }
.stock-card.medium { border-left-color: #EAB308; background: linear-gradient(135deg,#FFF 0%,#FEFCE8 100%); }
.score-badge {
    display: inline-block; padding: 0.35rem 0.85rem;
    border-radius: 20px; font-weight: 700; font-size: 0.9rem;
}
.score-badge.lockon { background: linear-gradient(135deg,#C41E3A 0%,#E85A71 100%); color: white; }
.score-badge.high   { background: linear-gradient(135deg,#F97316 0%,#FB923C 100%); color: white; }
.score-badge.medium { background: linear-gradient(135deg,#EAB308 0%,#FACC15 100%); color: #1a1a1a; }
.signal-tag {
    display: inline-block; background: #F3F4F6;
    padding: 0.25rem 0.6rem; border-radius: 6px;
    font-size: 0.8rem; margin: 0.15rem; color: #374151;
}
[data-testid="stMetric"] {
    background: white !important; padding: 1rem !important;
    border-radius: 12px !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04) !important;
}
[data-testid="stMetricValue"] {
    font-size: 1.5rem !important; font-weight: 700 !important;
    color: #C41E3A !important;
}
.stExpander {
    background-color: #FFF !important; border: none !important;
    border-radius: 12px !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04) !important;
}
.stTextArea textarea, .stTextInput input {
    border-radius: 10px !important; border: 2px solid #E8E8E8 !important;
    font-size: 16px !important;
}
.stTextArea textarea:focus, .stTextInput input:focus {
    border-color: #C41E3A !important;
}
.hero-section {
    text-align: center; padding: 2rem 1rem;
    background: linear-gradient(135deg,#C41E3A 0%,#E85A71 100%);
    border-radius: 20px; color: white; margin-bottom: 1.5rem;
}
.hero-section h2 { color: white !important; font-size: 1.3rem; margin-bottom: 0.5rem; }
.hero-section p { opacity: 0.9; font-size: 0.95rem; }
@media (max-width: 768px) {
    .main .block-container { padding: 0.75rem 1rem 2rem 1rem !important; }
    h1 { font-size: 1.5rem !important; }
    .stTabs [data-baseweb="tab-list"] { flex-wrap: nowrap !important; overflow-x: auto !important; }
    .stTabs [data-baseweb="tab"] { padding: 0.6rem 1rem !important; font-size: 0.9rem !important; white-space: nowrap !important; }
    div.stButton > button:first-child { width: 100% !important; }
    .hero-section { padding: 1.5rem 1rem; }
}
</style>
""", unsafe_allow_html=True)


# ==========================================
# 認証
# ==========================================
def check_password():
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False
    if not st.session_state["logged_in"]:
        st.markdown("## 🔒 ACCESS RESTRICTED")
        pw = st.text_input("パスワードを入力してください", type="password")
        if st.button("ログイン"):
            inp = unicodedata.normalize("NFKC", pw).upper().strip()
            sec = unicodedata.normalize("NFKC", LOGIN_PASSWORD).upper().strip()
            if inp == sec:
                st.session_state["logged_in"] = True
                st.rerun()
            else:
                st.error("パスワードが違います 🙅")
        st.stop()

check_password()


# ==========================================
# ヘルパー
# ==========================================
def fmt_price(x):
    if x is None or pd.isna(x): return "—"
    try: return f"¥{float(x):,.0f}"
    except: return "—"

def fmt_pct(x):
    if x is None or pd.isna(x): return "—"
    try:
        v = float(x)
        return f"{'+' if v >= 0 else ''}{v:.2f}%"
    except: return "—"

def fmt_market_cap(x):
    """x は円単位"""
    if x is None or pd.isna(x) or x == 0: return "—"
    try:
        oku = float(x) / 1e8
        if oku >= 10000: return f"{oku/10000:.1f}兆円"
        return f"{oku:.0f}億円"
    except: return "—"

def get_card_class(level):
    if level == scanner.SignalLevel.LOCKON: return "lockon"
    if level == scanner.SignalLevel.HIGH: return "high"
    return "medium"


def render_stock_card(sig: scanner.HagetakaSignal):
    cls = get_card_class(sig.signal_level)
    st.markdown(f"""
    <div class="stock-card {cls}">
        <div style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:0.5rem;">
            <div>
                <span class="score-badge {cls}">{sig.signal_level.value}</span>
                <span style="font-size:1.3rem; font-weight:700; margin-left:0.5rem;">{sig.name}</span>
                <span style="color:#666; margin-left:0.5rem;">({sig.code})</span>
            </div>
            <div style="text-align:right;">
                <div style="font-size:1.4rem; font-weight:700;">{fmt_price(sig.price)}</div>
                <div style="color:{'#22C55E' if sig.change_pct >= 0 else '#EF4444'}; font-weight:600;">
                    {fmt_pct(sig.change_pct)}
                </div>
            </div>
        </div>
        <div style="margin-top:0.75rem; display:flex; gap:1.5rem; flex-wrap:wrap; font-size:0.9rem; color:#666;">
            <span>📊 スコア: <strong style="color:#C41E3A;">{sig.total_score}点</strong></span>
            <span>🌪️ 回転率: <strong>{sig.turnover_pct:.1f}%</strong></span>
            <span>💰 時価総額: <strong>{fmt_market_cap(sig.market_cap)}</strong></span>
            {"<span>📈 出来高倍率: <strong>" + f"{sig.volume_ratio:.1f}倍" + "</strong></span>" if sig.volume_ratio > 0 else ""}
        </div>
        <div style="margin-top:0.75rem;">
            {''.join([f'<span class="signal-tag">{s}</span>' for s in sig.signals[:5]])}
        </div>
    </div>
    """, unsafe_allow_html=True)


# ==========================================
# セッション初期化
# ==========================================
def init_session():
    defaults = {
        "notification_config": notifier.load_notification_config(),
        "watchlist": notifier.load_watchlist(),
        "scan_results": [],
        "last_scan_time": None,
        "kabuplus_data": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session()


# ==========================================
# KABU+ 接続チェック
# ==========================================
def load_kabuplus_data():
    """KABU+ データを取得してセッションにキャッシュ"""
    uid, pwd = kp.get_credentials()
    if not uid or not pwd:
        st.error("⚠️ KABU+ の認証情報が設定されていません。`.streamlit/secrets.toml` を確認してください。")
        st.code("""
# .streamlit/secrets.toml に以下を追加:
[kabuplus]
id = "your_id"
password = "your_password"
        """)
        st.stop()

    with st.spinner("📡 KABU+ からデータ取得中...（全銘柄一括 / 数秒）"):
        df = kp.fetch_stock_prices(uid, pwd)

    if df.empty:
        st.error("❌ KABU+ からデータを取得できませんでした（休日の可能性があります）")
        st.stop()

    return df


# ==========================================
# メイン画面
# ==========================================
st.title("🦅 源太AI ハゲタカSCOPE")
st.markdown('<p class="subtitle">プロの投資戦略をのぞき見る「カンニング級の裏・攻略本」</p>', unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["🎯 ロックオン銘柄", "📊 ハゲタカ監視", "🔔 通知設定"])


# ==========================================
# タブ1: ロックオン銘柄
# ==========================================
with tab1:
    st.markdown("""
    <div class="hero-section">
        <h2>🎯 AIが検知した「今日の標的」</h2>
        <p>KABU+ から全銘柄データを一括取得 → 数秒でスキャン完了</p>
    </div>
    """, unsafe_allow_html=True)

    # ステータス
    col1, _, col3 = st.columns(3)
    with col1:
        last_scan = st.session_state.get("last_scan_time")
        if last_scan:
            st.markdown(f"✅ 最終スキャン: **{last_scan.strftime('%H:%M')}**")
    with col3:
        lockons = [s for s in st.session_state.get("scan_results", []) if s.total_score >= 50]
        st.metric("ロックオン銘柄", f"{len(lockons)}件")

    st.divider()

    # スキャン設定
    st.markdown("### 🔍 スキャン設定")

    scan_mode_options = {
        "⚡ クイックスキャン（推奨）": scanner.ScanMode.QUICK,
        "🌱 グロース市場（約500銘柄）": scanner.ScanMode.GROWTH,
        "🏬 スタンダード市場（約1,400銘柄）": scanner.ScanMode.STANDARD,
        "🏢 プライム市場（約1,800銘柄）": scanner.ScanMode.PRIME,
        "🌐 全銘柄スキャン（約3,800銘柄）": scanner.ScanMode.ALL,
        "✏️ 銘柄コードを直接入力": scanner.ScanMode.CUSTOM,
    }

    selected_label = st.selectbox(
        "スキャン対象を選択", options=list(scan_mode_options.keys()), index=0,
    )
    selected_mode = scan_mode_options[selected_label]
    opt = scanner.SCAN_OPTIONS[selected_mode]

    st.markdown(f"""
    <div style="background:#F8F9FA; padding:0.75rem 1rem; border-radius:8px; font-size:0.9rem;">
        📋 <strong>{opt.description}</strong><br>
        <span style="color:#666;">対象: 約{opt.estimated_count}銘柄 / 所要時間: {opt.estimated_time}</span>
    </div>
    """, unsafe_allow_html=True)

    custom_codes = []
    if selected_mode == scanner.ScanMode.CUSTOM:
        custom_input = st.text_input(
            "銘柄コードを入力（スペース区切り）",
            placeholder="例: 7203 9984 6758 8306",
        )
        if custom_input:
            custom_codes = [c.strip() for c in custom_input.split() if c.strip()]
            st.info(f"📝 {len(custom_codes)}銘柄を入力済み")

    st.markdown("")
    scan_btn = st.button("🚀 スキャン開始", type="primary", use_container_width=True)

    if scan_btn:
        if selected_mode == scanner.ScanMode.CUSTOM and not custom_codes:
            st.error("銘柄コードを入力してください")
            st.stop()

        # KABU+ データ取得
        df = load_kabuplus_data()

        # フィルタリング
        target_df = scanner.filter_scan_targets(df, selected_mode, custom_codes)

        if target_df.empty:
            st.error("スキャン対象の銘柄が見つかりませんでした")
            st.stop()

        st.info(f"🎯 {len(target_df)}銘柄をスキャンします")

        # スキャン実行（数秒で完了）
        with st.spinner("🔍 ハゲタカの足跡を探索中..."):
            vol_history = scanner.load_volume_history()
            results = scanner.scan_dataframe(target_df, vol_history)
            st.session_state["scan_results"] = results
            st.session_state["last_scan_time"] = datetime.now()

        # サマリー
        if results:
            loc = [s for s in results if s.signal_level == scanner.SignalLevel.LOCKON]
            hig = [s for s in results if s.signal_level == scanner.SignalLevel.HIGH]
            med = [s for s in results if s.signal_level == scanner.SignalLevel.MEDIUM]
            st.success(f"""
            ✅ スキャン完了！
            - 🔴 ロックオン: {len(loc)}件
            - 🟠 高警戒: {len(hig)}件
            - 🟡 監視中: {len(med)}件
            """)
        else:
            st.warning("スキャン結果が0件でした。")

        st.rerun()

    # 結果表示
    results = st.session_state.get("scan_results", [])
    if results:
        st.divider()
        col1, col2 = st.columns([1, 1])
        with col1:
            min_score_filter = st.slider("最低スコア", 0, 100, 0, key="filter_score")
        with col2:
            sort_opt = st.selectbox("並び順", ["スコア順", "回転率順"])

        filtered = [s for s in results if s.total_score >= min_score_filter]
        if sort_opt == "回転率順":
            filtered.sort(key=lambda x: x.turnover_pct, reverse=True)
        else:
            filtered.sort(key=lambda x: x.total_score, reverse=True)

        st.markdown(f"### 📋 厳選・監視リスト（{len(filtered)}件）")

        if not filtered:
            st.info("条件に合致する銘柄がありません。フィルターを調整してください。")
        else:
            for sig in filtered[:20]:
                render_stock_card(sig)
                with st.expander(f"📊 {sig.code} の詳細分析"):
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("🥷 ステルス集積", f"{sig.stealth_score}/35点")
                    c2.metric("🧱 板の違和感", f"{sig.board_score}/35点")
                    c3.metric("🔥 出来高臨界点", f"{sig.volume_score}/30点")
                    c4.metric("🌟 ボーナス", f"+{sig.bonus_score}点")
                    st.markdown("**検知シグナル:**")
                    for s in sig.signals:
                        st.markdown(f"- {s}")
    elif st.session_state.get("last_scan_time"):
        st.warning("スキャン結果が0件でした。")
    else:
        st.info("👆 「スキャン開始」ボタンを押して、ハゲタカの足跡を探索してください。")

    # 説明セクション
    st.divider()
    with st.expander("📚 ハゲタカスコープの仕組み"):
        st.markdown("""
        ### 🦅 3つの検知ロジック

        #### 1. 🥷 ステルス集積（最大35点）
        目立たないように株を買い集めている動きを検知します。
        - 出来高の異常増加（過去平均との比較）
        - 価格変動が小さいのに出来高が増えているか
        - 時価総額が買収適正サイズか

        #### 2. 🧱 板の違和感（最大35点）
        年初来高値・安値との位置関係やVWAPとの乖離を検知します。
        - 年初来安値圏での推移（底値買い狙い）
        - 年初来高値からの乖離率
        - VWAPとの乖離

        #### 3. 🔥 出来高の臨界点（最大30点）
        爆発直前に見られる取引量の異常な変化を検知します。
        - 出来高倍率（過去平均比）
        - 浮動株回転率
        - 売買代金 / 時価総額比率

        ---

        | レベル | スコア | 意味 |
        |--------|--------|------|
        | 🔴 ロックオン | 60点以上 | 複数の兆候が重なった最注目銘柄 |
        | 🟠 高警戒 | 45〜59点 | 要注目、監視リスト入り推奨 |
        | 🟡 監視中 | 30〜44点 | 一部兆候あり、継続監視 |
        | 🟢 平常 | 29点以下 | 現時点で特に異常なし |
        """)


# ==========================================
# タブ2: ハゲタカ監視（M&A予兆）
# ==========================================
with tab2:
    st.markdown("""
    <div class="hero-section">
        <h2>📊 M&A予兆監視システム</h2>
        <p>TOB・完全子会社化・MBOなど、M&Aの可能性が高い銘柄を自動検知</p>
    </div>
    """, unsafe_allow_html=True)

    # 監視リスト管理
    st.markdown("### 📋 監視リスト")
    watchlist = st.session_state.get("watchlist", [])

    col1, col2 = st.columns([3, 1])
    with col1:
        new_codes = st.text_input("銘柄を追加", placeholder="例: 7203 9984 6758", key="add_watch")
    with col2:
        if st.button("➕ 追加", key="add_watch_btn", use_container_width=True):
            if new_codes:
                new_list = [c.strip() for c in new_codes.split() if c.strip()]
                for code in new_list:
                    if code not in watchlist:
                        watchlist.append(code)
                st.session_state["watchlist"] = watchlist
                notifier.save_watchlist(watchlist)
                st.success(f"{len(new_list)}件追加しました")
                st.rerun()

    if watchlist:
        st.markdown(f"**現在の監視銘柄** ({len(watchlist)}件): {', '.join(watchlist)}")

        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("🔍 M&A分析実行", type="primary", key="ma_analyze"):
                # KABU+ データ取得
                uid, pwd = kp.get_credentials()
                if not uid:
                    st.error("KABU+ の認証情報を設定してください")
                else:
                    with st.spinner("🎯 M&A予兆分析中..."):
                        try:
                            merged = kp.fetch_merged_data(uid, pwd)
                            news_cache = ma.load_news_cache()
                            ma_results = ma.analyze_ma_from_dataframe(
                                merged, watchlist, news_cache
                            )
                            st.session_state["ma_results"] = ma_results

                            # 通知
                            config = st.session_state.get(
                                "notification_config", notifier.NotificationConfig()
                            )
                            if config.enabled and config.email_enabled:
                                alerts = [s for s in ma_results
                                          if s.total_score >= config.min_score_threshold]
                                if alerts:
                                    notifier.send_ma_alert(config, alerts)
                                    st.success(f"📧 {len(alerts)}件のアラートを送信しました")
                        except Exception as e:
                            st.error(f"エラー: {e}")

        with col2:
            if st.button("🗑️ リストをクリア", key="clear_watch"):
                st.session_state["watchlist"] = []
                notifier.save_watchlist([])
                st.rerun()

        # ニュースキャッシュ情報
        news_cache = ma.load_news_cache()
        cached_codes = [c for c in watchlist if c in news_cache]
        if cached_codes:
            st.caption(f"📰 ニュースキャッシュ: {len(cached_codes)}/{len(watchlist)}銘柄分あり（GitHub Actions で自動更新）")
        else:
            st.caption("📰 ニュースキャッシュなし（GitHub Actions 実行後に利用可能になります）")

        # M&A 分析結果表示
        if "ma_results" in st.session_state and st.session_state["ma_results"]:
            st.divider()
            st.markdown("### 📊 M&A予兆分析結果")

            for score in st.session_state["ma_results"]:
                if score.signal_level == ma.MASignalLevel.CRITICAL:
                    card_class = "lockon"
                elif score.signal_level == ma.MASignalLevel.HIGH:
                    card_class = "high"
                else:
                    card_class = "medium"

                st.markdown(f"""
                <div class="stock-card {card_class}">
                    <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap;">
                        <div>
                            <span class="score-badge {card_class}">{score.signal_level.value}</span>
                            <span style="font-size:1.2rem; font-weight:700; margin-left:0.5rem;">{score.name}</span>
                            <span style="color:#666;">({score.code})</span>
                        </div>
                        <div style="font-size:1.3rem; font-weight:700; color:#C41E3A;">
                            {score.total_score}点
                        </div>
                    </div>
                    <div style="margin-top:0.5rem;">
                        {''.join([f'<span class="signal-tag">{t}</span>' for t in score.reason_tags[:5]])}
                    </div>
                </div>
                """, unsafe_allow_html=True)

                with st.expander(f"📋 {score.code} 詳細"):
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("ニュース", f"{score.news_score}/40")
                    c2.metric("出来高", f"{score.volume_score}/30")
                    c3.metric("バリュエーション", f"{score.valuation_score}/20")
                    c4.metric("テクニカル", f"{score.technical_score}/10")

                    if score.news_items:
                        st.markdown("**📰 関連ニュース**")
                        for news in score.news_items[:5]:
                            if news.url:
                                st.markdown(f"- [{news.title}]({news.url})")
                            else:
                                st.markdown(f"- {news.title}")
    else:
        st.info("監視銘柄を追加してください。")


# ==========================================
# タブ3: 通知設定
# ==========================================
with tab3:
    st.markdown("""
    <div class="hero-section">
        <h2>🔔 ロックオン通知設定</h2>
        <p>条件合致の「標的」を検知した瞬間、メールに通知が届きます</p>
    </div>
    """, unsafe_allow_html=True)

    config = st.session_state.get("notification_config", notifier.NotificationConfig())

    st.markdown("### ⚙️ 基本設定")
    enabled = st.toggle("通知を有効にする", value=config.enabled, key="notify_enabled")
    min_score = st.slider(
        "通知する最低スコア", 0, 100, config.min_score_threshold,
        key="min_score", help="このスコア以上の銘柄が検知された場合に通知されます",
    )

    st.divider()
    st.markdown("### 📧 メール通知設定")
    st.markdown("""
    **Gmailの場合:**
    1. [Googleアカウント](https://myaccount.google.com/)で2段階認証を有効化
    2. [アプリパスワード](https://myaccount.google.com/apppasswords)を生成
    3. 生成された16桁のパスワードを「SMTPパスワード」に入力
    """)

    email_enabled = st.toggle("メール通知を有効にする", value=config.email_enabled, key="email_enabled")

    if email_enabled:
        c1, c2 = st.columns(2)
        with c1:
            email_address = st.text_input("送信先メールアドレス", value=config.email_address, key="email_address")
            smtp_server = st.text_input("SMTPサーバー", value=config.smtp_server, key="smtp_server")
        with c2:
            smtp_user = st.text_input("SMTPユーザー", value=config.smtp_user, key="smtp_user")
            smtp_password = st.text_input("SMTPパスワード", value=config.smtp_password, type="password", key="smtp_password")
        smtp_port = st.number_input("SMTPポート", value=config.smtp_port, key="smtp_port")

        if email_address and smtp_user and smtp_password:
            if st.button("📧 テスト送信", key="test_email"):
                result = notifier.send_email(
                    to_address=email_address,
                    subject="🎯 ハゲタカSCOPE テスト通知",
                    body="ロックオン通知のテストです。\n設定が正常に機能しています。",
                    smtp_server=smtp_server, smtp_port=int(smtp_port),
                    smtp_user=smtp_user, smtp_password=smtp_password,
                )
                if result.success:
                    st.success("✅ テスト送信成功！")
                else:
                    st.error(f"❌ {result.message}")
    else:
        email_address = config.email_address
        smtp_server = config.smtp_server
        smtp_user = config.smtp_user
        smtp_password = config.smtp_password
        smtp_port = config.smtp_port

    st.divider()
    if st.button("💾 設定を保存", type="primary", key="save_config"):
        new_config = notifier.NotificationConfig(
            enabled=enabled,
            email_enabled=email_enabled,
            email_address=email_address if email_enabled else config.email_address,
            smtp_server=smtp_server if email_enabled else config.smtp_server,
            smtp_port=int(smtp_port) if email_enabled else config.smtp_port,
            smtp_user=smtp_user if email_enabled else config.smtp_user,
            smtp_password=smtp_password if email_enabled else config.smtp_password,
            min_score_threshold=min_score,
        )
        notifier.save_notification_config(new_config)
        st.session_state["notification_config"] = new_config
        st.success("✅ 設定を保存しました！")


# ==========================================
# フッター
# ==========================================
st.divider()
st.markdown("""
<div style="text-align:center; color:#888; font-size:0.85rem; padding:1rem;">
    ⚠️ 投資は自己責任でお願いします。本ツールは情報提供を目的としており、投資助言ではありません。<br>
    © 先乗り株カレッジ - 源太AI ハゲタカSCOPE v3（KABU+ 対応版）
</div>
""", unsafe_allow_html=True)


# ==========================================
# 管理者メニュー
# ==========================================
with st.expander("🔧 管理者メニュー"):
    admin_input = st.text_input("管理者コード", type="password", key="admin_pass")
    if admin_input == ADMIN_CODE:
        st.success("認証OK")
        if st.button("🗑️ キャッシュ削除"):
            st.cache_data.clear()
            st.success("削除完了！")
            time.sleep(1)
            st.rerun()
