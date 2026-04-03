"""
KABU+ データ取得クライアント（v1統合版）
─────────────────────────────────────
・全銘柄の株価・指標を1回のHTTPで一括取得
・Basic認証 / Shift-JIS 自動処理
・Streamlit環境（app.py）でもCLI環境（fetch_data.py）でも動作
"""

from __future__ import annotations
import io
import os
from typing import Optional, Tuple
from datetime import datetime, timedelta

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

# ==========================================
# URL テンプレート
# ==========================================
PRICES_URL = (
    "https://csvex.com/kabu.plus/csv/"
    "japan-all-stock-prices-2/daily/"
    "japan-all-stock-prices-2_{date}.csv"
)
INDICATORS_URL = (
    "https://csvex.com/kabu.plus/csv/"
    "japan-all-stock-data/daily/"
    "japan-all-stock-data_{date}.csv"
)

# カラム名の正規化マッピング
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


# ==========================================
# 認証情報の取得
# ==========================================
def get_credentials() -> Tuple[Optional[str], Optional[str]]:
    """環境変数 → Streamlit Secrets の順で認証情報を取得"""
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
# CSV 取得（共通）
# ==========================================
def _fetch_csv(
    url_template: str,
    user_id: str,
    password: str,
    col_map: dict,
    max_days_back: int = 7,
) -> pd.DataFrame:
    auth = HTTPBasicAuth(user_id, password)
    for days_back in range(max_days_back):
        target = datetime.now() - timedelta(days=days_back)
        date_str = target.strftime("%Y%m%d")
        url = url_template.format(date=date_str)
        try:
            resp = requests.get(url, auth=auth, timeout=60)
            if resp.status_code != 200:
                continue
            text = resp.content.decode("shift-jis", errors="replace")
            df = pd.read_csv(io.StringIO(text))
            if len(df) < 100:
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
# 公開 API
# ==========================================
def fetch_stock_prices(user_id: str, password: str) -> pd.DataFrame:
    return _fetch_csv(PRICES_URL, user_id, password, PRICE_COLUMNS)


def fetch_stock_indicators(user_id: str, password: str) -> pd.DataFrame:
    return _fetch_csv(INDICATORS_URL, user_id, password, INDICATOR_COLUMNS)


def fetch_merged_data(user_id: str, password: str) -> pd.DataFrame:
    prices = fetch_stock_prices(user_id, password)
    if prices.empty:
        return prices
    indicators = fetch_stock_indicators(user_id, password)
    if indicators.empty:
        return prices
    ind_cols = [c for c in indicators.columns
                if c not in ("name", "market", "industry") or c == "code"]
    return prices.merge(
        indicators[ind_cols], on="code", how="left", suffixes=("", "_ind")
    )


def build_info_lookup(merged_df: pd.DataFrame) -> dict:
    """
    KABU+ データから {ticker: info_dict} の辞書を構築。
    fetch_data.py で yf.Ticker().info の代替として使う。
    キーは "1234.T" 形式。
    """
    lookup = {}
    if merged_df.empty:
        return lookup

    for _, row in merged_df.iterrows():
        code = str(row.get("code", ""))
        if not code:
            continue
        ticker = f"{code}.T"
        mcap_m = row.get("market_cap_m", 0) or 0
        shares = row.get("shares_outstanding", 0) or 0
        pbr_val = row.get("pbr", None)
        price = row.get("price", 0) or 0
        name = str(row.get("name", ""))

        # sharesOutstanding が KABU+ にない場合、時価総額/株価から推定
        if (not shares or shares <= 0) and mcap_m > 0 and price > 0:
            shares = int(mcap_m * 1_000_000 / price)

        lookup[ticker] = {
            "marketCap": int(mcap_m * 1_000_000) if mcap_m else 0,
            "sharesOutstanding": int(shares) if shares else None,
            "priceToBook": float(pbr_val) if pbr_val and pbr_val > 0 else None,
            "shortName": name,
            "longName": name,
            "currentPrice": float(price) if price else None,
            "dividendRate": float(row.get("dividend_per_share", 0) or 0),
            "dividendYield": float(row.get("dividend_yield", 0) or 0) / 100.0 if row.get("dividend_yield") else None,
            "trailingAnnualDividendRate": None,
            "trailingAnnualDividendYield": None,
            "payoutRatio": None,
        }
    return lookup


# ==========================================
# 信用取引残高データ（週次）
# ==========================================
MARGIN_URL = (
    "https://csvex.com/kabu.plus/csv/"
    "japan-all-stock-margin-transactions/weekly/"
    "japan-all-stock-margin-transactions_{date}.csv"
)

MARGIN_COLUMNS = {
    "SC": "code",
    "名称": "name",
    "市場": "market",
    "信用買残": "margin_buy",
    "信用買残前週比": "margin_buy_change",
    "信用売残": "margin_sell",
    "信用売残前週比": "margin_sell_change",
    "貸借倍率": "margin_ratio",
}


def fetch_margin_data(user_id: str, password: str) -> pd.DataFrame:
    """信用取引残高（週次）を一括取得"""
    return _fetch_csv(MARGIN_URL, user_id, password, MARGIN_COLUMNS, max_days_back=14)


def build_margin_lookup(margin_df: pd.DataFrame) -> dict:
    """
    信用残高データから {ticker: margin_dict} の辞書を構築。
    キーは "1234.T" 形式。
    """
    lookup = {}
    if margin_df.empty:
        return lookup

    for _, row in margin_df.iterrows():
        code = str(row.get("code", ""))
        if not code:
            continue
        ticker = f"{code}.T"

        buy = row.get("margin_buy", 0) or 0
        sell = row.get("margin_sell", 0) or 0
        buy_chg = row.get("margin_buy_change", 0) or 0
        sell_chg = row.get("margin_sell_change", 0) or 0
        ratio = row.get("margin_ratio", 0) or 0

        lookup[ticker] = {
            "margin_buy": int(buy) if buy else 0,
            "margin_sell": int(sell) if sell else 0,
            "margin_buy_change": int(buy_chg) if buy_chg else 0,
            "margin_sell_change": int(sell_chg) if sell_chg else 0,
            "margin_ratio": round(float(ratio), 2) if ratio else None,
        }
    return lookup

