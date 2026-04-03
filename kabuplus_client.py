"""
KABU+ データ取得クライアント（v5 gzip圧縮・Session最適化版）
─────────────────────────────────────────────────────────────
参考: https://kabu.plus/doc/sample-program/download
・Accept-Encoding: gzip でCSVを圧縮転送（約1/5〜1/7サイズ）
・requests.Session でTCPコネクション再利用（高速化）
・日付なしURL（最新ファイル）と日付ありURL（履歴）を使い分け
・並列数3 + リクエスト間隔でレート制限を回避
・yfinance 依存を完全撤廃
"""

from __future__ import annotations
import io
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple
from datetime import datetime, timedelta, date as date_t

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

# ==========================================
# URL テンプレート
# ==========================================
# 日付なし（最新ファイル）
PRICES_URL_LATEST    = "https://csvex.com/kabu.plus/csv/japan-all-stock-prices-2/daily/japan-all-stock-prices-2.csv"
INDICATORS_URL_LATEST = "https://csvex.com/kabu.plus/csv/japan-all-stock-data/daily/japan-all-stock-data.csv"
MARGIN_URL_LATEST    = "https://csvex.com/kabu.plus/csv/japan-all-stock-margin-transactions/weekly/japan-all-stock-margin-transactions.csv"
OHLC_URL_LATEST      = "https://csvex.com/kabu.plus/csv/tosho-stock-ohlc/daily/tosho-stock-ohlc.csv"

# 日付あり（履歴）
PRICES_URL    = "https://csvex.com/kabu.plus/csv/japan-all-stock-prices-2/daily/japan-all-stock-prices-2_{date}.csv"
INDICATORS_URL = "https://csvex.com/kabu.plus/csv/japan-all-stock-data/daily/japan-all-stock-data_{date}.csv"
MARGIN_URL    = "https://csvex.com/kabu.plus/csv/japan-all-stock-margin-transactions/weekly/japan-all-stock-margin-transactions_{date}.csv"
OHLC_URL      = "https://csvex.com/kabu.plus/csv/tosho-stock-ohlc/daily/tosho-stock-ohlc_{date}.csv"

# gzip圧縮リクエストヘッダー（--compressed 相当）
_GZIP_HEADERS = {"Accept-Encoding": "gzip, deflate"}

# ==========================================
# カラム名の正規化マッピング
# ==========================================
PRICE_COLUMNS = {
    "SC": "code", "名称": "name", "市場": "market", "業種": "industry",
    "日時": "timestamp", "株価": "price", "前日比": "change",
    "前日比（％）": "change_pct", "前日終値": "prev_close",
    "始値": "open", "高値": "high", "安値": "low", "VWAP": "vwap",
    "出来高": "volume", "出来高率": "turnover_rate",
    "売買代金（千円）": "trading_value_k", "時価総額（百万円）": "market_cap_m",
    "値幅下限": "price_limit_low", "値幅上限": "price_limit_high",
    "高値日付": "ytd_high_date", "年初来高値": "ytd_high",
    "年初来高値乖離率": "ytd_high_deviation",
    "安値日付": "ytd_low_date", "年初来安値": "ytd_low",
    "年初来安値乖離率": "ytd_low_deviation",
}
INDICATOR_COLUMNS = {
    "SC": "code", "名称": "name", "市場": "market", "業種": "industry",
    "配当利回り（予想）": "dividend_yield", "1株配当": "dividend_per_share",
    "PER（予想）": "per", "PBR（実績）": "pbr", "EPS": "eps", "BPS": "bps",
    "最低購入金額": "min_purchase", "単元株数": "unit_shares",
    "発行済株式数": "shares_outstanding",
}
# tosho-stock-ohlc の実際の列（2025年7月実測）
# SC, 日付, 始値, 高値, 安値, 終値, VWAP, 出来高, 売買代金, 前場..., 後場...
OHLC_COLUMNS = {
    "SC":     "code",
    "日付":   "date_str",
    "始値":   "open",
    "高値":   "high",
    "安値":   "low",
    "終値":   "close",
    "VWAP":   "vwap",
    "出来高": "volume",
    "売買代金": "trading_value",
}
MARGIN_COLUMNS = {
    "SC": "code", "名称": "name", "市場": "market",
    "信用買残": "margin_buy", "信用買残前週比": "margin_buy_change",
    "信用売残": "margin_sell", "信用売残前週比": "margin_sell_change",
    "貸借倍率": "margin_ratio",
}


# ==========================================
# 認証情報の取得
# ==========================================
def get_credentials() -> Tuple[Optional[str], Optional[str]]:
    uid = os.environ.get("KABUPLUS_ID")
    pwd = os.environ.get("KABUPLUS_PASSWORD")
    if uid and pwd:
        return uid, pwd
    try:
        import streamlit as st
        uid = st.secrets["kabuplus"]["id"]
        pwd = st.secrets["kabuplus"]["password"]
        return uid, pwd
    except Exception:
        return None, None


# ==========================================
# Session 生成（gzip + コネクション再利用）
# ==========================================
def _make_session(user_id: str, password: str) -> requests.Session:
    """gzip圧縮 + Basic認証付き Session を生成"""
    session = requests.Session()
    session.auth = HTTPBasicAuth(user_id, password)
    session.headers.update(_GZIP_HEADERS)
    return session


# ==========================================
# CSV 取得（共通・最新URLを優先）
# ==========================================
def _fetch_csv_latest(
    url_latest: str,
    url_dated_template: str,
    session: requests.Session,
    col_map: dict,
    max_days_back: int = 7,
    min_rows: int = 100,
) -> pd.DataFrame:
    """最新URL → 日付URLの順でフォールバックしながらCSV取得"""
    urls_to_try = [url_latest]
    for days_back in range(1, max_days_back + 1):
        d = datetime.now() - timedelta(days=days_back)
        urls_to_try.append(url_dated_template.format(date=d.strftime("%Y%m%d")))

    for url in urls_to_try:
        try:
            resp = session.get(url, timeout=60)
            if resp.status_code != 200:
                continue
            text = resp.content.decode("shift-jis", errors="replace")
            df = pd.read_csv(io.StringIO(text))
            if len(df) < min_rows:
                continue
            rename = {k: v for k, v in col_map.items() if k in df.columns}
            df = df.rename(columns=rename)
            if "code" in df.columns:
                df["code"] = df["code"].astype(str).str.strip()
            df = _clean_numeric(df)
            return df
        except Exception:
            continue
    return pd.DataFrame()


def _clean_numeric(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "price", "change", "change_pct", "prev_close", "open", "high", "low",
        "vwap", "volume", "turnover_rate", "trading_value_k", "market_cap_m",
        "ytd_high", "ytd_high_deviation", "ytd_low", "ytd_low_deviation",
        "per", "pbr", "eps", "bps", "dividend_yield", "shares_outstanding",
    ]
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("－", "", regex=False)
                .str.replace("-", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ==========================================
# OHLC 1日分取得
# ==========================================
def _fetch_ohlc_one_day(
    target_date: date_t,
    session: requests.Session,
    timeout: int = 45,
    max_retries: int = 2,
) -> Optional[pd.DataFrame]:
    """tosho-stock-ohlc の1日分を gzip 圧縮付きで取得"""
    date_str = target_date.strftime("%Y%m%d")
    url = OHLC_URL.format(date=date_str)

    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 404:
                return None   # 休日・未公開
            if resp.status_code != 200:
                if attempt < max_retries - 1:
                    time.sleep(2)
                continue

            text = resp.content.decode("shift-jis", errors="replace")
            df = pd.read_csv(io.StringIO(text))
            if len(df) < 10:
                return None

            rename = {k: v for k, v in OHLC_COLUMNS.items() if k in df.columns}
            df = df.rename(columns=rename)

            if "code" not in df.columns or "close" not in df.columns:
                return None

            df["code"] = (
                df["code"].astype(str)
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
            )
            for col in ("open", "high", "low", "close", "volume"):
                if col in df.columns:
                    df[col] = pd.to_numeric(
                        df[col].astype(str).str.replace(",", "", regex=False).str.strip(),
                        errors="coerce",
                    )

            if df["close"].isna().all():
                return None

            df["_date"] = target_date
            return df

        except Exception:
            if attempt < max_retries - 1:
                time.sleep(2)
            continue

    return None


# ==========================================
# OHLC 履歴取得（gzip + Session + 並列数制限）
# ==========================================
def fetch_ohlc_history(
    user_id: str,
    password: str,
    lookback_days: int = 250,
    max_workers: int = 3,
    request_interval: float = 0.3,
    calendar_window: int = 390,
) -> dict[str, pd.DataFrame]:
    """
    過去 lookback_days 営業日分の OHLCV を tosho-stock-ohlc から取得。
    gzip 圧縮 + Session でネットワーク負荷を大幅削減。
    {code: DataFrame(index=DatetimeIndex, cols=[Open,High,Low,Close,Volume])} を返す。
    """
    session = _make_session(user_id, password)
    today = datetime.now().date()
    candidate_dates: list[date_t] = [
        today - timedelta(days=i) for i in range(calendar_window)
    ]

    frames_by_date: dict[date_t, pd.DataFrame] = {}
    lock = threading.Lock()
    request_lock = threading.Lock()
    last_request_time = [0.0]

    def _worker(d: date_t):
        with request_lock:
            elapsed = time.time() - last_request_time[0]
            wait = request_interval - elapsed
            if wait > 0:
                time.sleep(wait)
            last_request_time[0] = time.time()

        result = _fetch_ohlc_one_day(d, session)
        if result is not None:
            with lock:
                frames_by_date[d] = result

    print(
        f"  📡 OHLC取得開始（gzip圧縮有効 / 並列{max_workers} / 間隔{request_interval}s）"
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futs = {executor.submit(_worker, d): d for d in candidate_dates}
        done = 0
        for f in as_completed(futs):
            done += 1
            if done % 50 == 0:
                with lock:
                    got = len(frames_by_date)
                print(f"    進捗: {done}/{len(candidate_dates)} 試行, {got} 営業日取得済み")

    if not frames_by_date:
        print("  ⚠️ OHLC CSVを1件も取得できませんでした")
        return {}

    sorted_dates = sorted(frames_by_date.keys(), reverse=True)[:lookback_days]
    sorted_dates.sort()
    print(f"  ✅ OHLC取得完了: {len(sorted_dates)} 営業日 / {len(frames_by_date)} 取得成功")

    combined = pd.concat(
        [frames_by_date[d] for d in sorted_dates], ignore_index=True
    )
    combined = combined.dropna(subset=["close"])

    ohlc_cache: dict[str, pd.DataFrame] = {}
    for code, grp in combined.groupby("code"):
        code = str(code).strip()
        if not code or code == "nan":
            continue
        grp = grp.sort_values("_date").drop_duplicates("_date")
        idx = pd.to_datetime(grp["_date"])

        df_out = pd.DataFrame(
            {
                "Open":   grp["open"].values   if "open"   in grp.columns else grp["close"].values,
                "High":   grp["high"].values   if "high"   in grp.columns else grp["close"].values,
                "Low":    grp["low"].values    if "low"    in grp.columns else grp["close"].values,
                "Close":  grp["close"].values,
                "Volume": grp["volume"].values  if "volume" in grp.columns else [0.0] * len(grp),
            },
            index=idx,
        )
        df_out.index.name = "Date"
        df_out["Volume"] = pd.to_numeric(df_out["Volume"], errors="coerce").fillna(0)
        df_out = df_out.dropna(subset=["Close"])
        if len(df_out) < 20:
            continue
        ohlc_cache[code] = df_out

    print(f"  📊 OHLC銘柄数: {len(ohlc_cache)} 銘柄")
    return ohlc_cache


# ==========================================
# 公開 API（Session統一版）
# ==========================================
def fetch_stock_prices(user_id: str, password: str) -> pd.DataFrame:
    session = _make_session(user_id, password)
    return _fetch_csv_latest(
        PRICES_URL_LATEST, PRICES_URL, session, PRICE_COLUMNS
    )


def fetch_stock_indicators(user_id: str, password: str) -> pd.DataFrame:
    session = _make_session(user_id, password)
    return _fetch_csv_latest(
        INDICATORS_URL_LATEST, INDICATORS_URL, session, INDICATOR_COLUMNS
    )


def fetch_merged_data(user_id: str, password: str) -> pd.DataFrame:
    session = _make_session(user_id, password)
    prices = _fetch_csv_latest(
        PRICES_URL_LATEST, PRICES_URL, session, PRICE_COLUMNS
    )
    if prices.empty:
        return prices
    indicators = _fetch_csv_latest(
        INDICATORS_URL_LATEST, INDICATORS_URL, session, INDICATOR_COLUMNS
    )
    if indicators.empty:
        return prices
    ind_cols = [c for c in indicators.columns
                if c not in ("name", "market", "industry") or c == "code"]
    return prices.merge(
        indicators[ind_cols], on="code", how="left", suffixes=("", "_ind")
    )


def build_info_lookup(merged_df: pd.DataFrame) -> dict:
    lookup = {}
    if merged_df.empty:
        return lookup
    for _, row in merged_df.iterrows():
        code = str(row.get("code", ""))
        if not code:
            continue
        ticker = f"{code}.T"
        mcap_m   = row.get("market_cap_m", 0) or 0
        shares   = row.get("shares_outstanding", 0) or 0
        pbr_val  = row.get("pbr", None)
        price    = row.get("price", 0) or 0
        name     = str(row.get("name", ""))
        if (not shares or shares <= 0) and mcap_m > 0 and price > 0:
            shares = int(mcap_m * 1_000_000 / price)
        lookup[ticker] = {
            "marketCap":                  int(mcap_m * 1_000_000) if mcap_m else 0,
            "sharesOutstanding":          int(shares) if shares else None,
            "priceToBook":                float(pbr_val) if pbr_val and pbr_val > 0 else None,
            "shortName":                  name,
            "longName":                   name,
            "currentPrice":               float(price) if price else None,
            "dividendRate":               float(row.get("dividend_per_share", 0) or 0),
            "dividendYield":              float(row.get("dividend_yield", 0) or 0) / 100.0
                                          if row.get("dividend_yield") else None,
            "trailingAnnualDividendRate": None,
            "trailingAnnualDividendYield": None,
            "payoutRatio":                None,
        }
    return lookup


def fetch_margin_data(user_id: str, password: str) -> pd.DataFrame:
    session = _make_session(user_id, password)
    return _fetch_csv_latest(
        MARGIN_URL_LATEST, MARGIN_URL, session, MARGIN_COLUMNS, max_days_back=14
    )


def build_margin_lookup(margin_df: pd.DataFrame) -> dict:
    lookup = {}
    if margin_df.empty:
        return lookup
    for _, row in margin_df.iterrows():
        code = str(row.get("code", ""))
        if not code:
            continue
        ticker = f"{code}.T"
        lookup[ticker] = {
            "margin_buy":         int(row.get("margin_buy", 0) or 0),
            "margin_sell":        int(row.get("margin_sell", 0) or 0),
            "margin_buy_change":  int(row.get("margin_buy_change", 0) or 0),
            "margin_sell_change": int(row.get("margin_sell_change", 0) or 0),
            "margin_ratio":       round(float(row.get("margin_ratio", 0) or 0), 2)
                                  if row.get("margin_ratio") else None,
        }
    return lookup
