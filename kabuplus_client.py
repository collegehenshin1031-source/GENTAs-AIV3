"""
KABU+ データ取得クライアント
─────────────────────────────
・全銘柄の株価・指標を 1-2 回の HTTP で一括取得
・Basic 認証 / Shift-JIS 自動処理
・Streamlit キャッシュ対応
"""

from __future__ import annotations
import io
import re
from typing import Optional, Tuple
from datetime import datetime, timedelta

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import streamlit as st

# ==========================================
# URL テンプレート（CSVEX 経由）
# ==========================================
# 株価一覧表（詳細）: 株価 + 出来高率 + 年初来高安値 + 時価総額 + 売買代金
PRICES_URL = (
    "https://csvex.com/kabu.plus/csv/"
    "japan-all-stock-prices-2/daily/"
    "japan-all-stock-prices-2_{date}.csv"
)

# 投資指標: PER / PBR / EPS / BPS / 配当利回り / 発行済株式数
INDICATORS_URL = (
    "https://csvex.com/kabu.plus/csv/"
    "japan-all-stock-data/daily/"
    "japan-all-stock-data_{date}.csv"
)

# カラム名の正規化マッピング
PRICE_COLUMNS = {
    "SC": "code",
    "名称": "name",
    "市場": "market",
    "業種": "industry",
    "日時": "timestamp",
    "株価": "price",
    "前日比": "change",
    "前日比（％）": "change_pct",
    "前日終値": "prev_close",
    "始値": "open",
    "高値": "high",
    "安値": "low",
    "VWAP": "vwap",
    "出来高": "volume",
    "出来高率": "turnover_rate",
    "売買代金（千円）": "trading_value_k",
    "時価総額（百万円）": "market_cap_m",
    "値幅下限": "price_limit_low",
    "値幅上限": "price_limit_high",
    "高値日付": "ytd_high_date",
    "年初来高値": "ytd_high",
    "年初来高値乖離率": "ytd_high_deviation",
    "安値日付": "ytd_low_date",
    "年初来安値": "ytd_low",
    "年初来安値乖離率": "ytd_low_deviation",
}

INDICATOR_COLUMNS = {
    "SC": "code",
    "名称": "name",
    "市場": "market",
    "業種": "industry",
    "配当利回り（予想）": "dividend_yield",
    "1株配当": "dividend_per_share",
    "PER（予想）": "per",
    "PBR（実績）": "pbr",
    "EPS": "eps",
    "BPS": "bps",
    "最低購入金額": "min_purchase",
    "単元株数": "unit_shares",
    "発行済株式数": "shares_outstanding",
}


# ==========================================
# 認証情報の取得
# ==========================================
def get_credentials() -> Tuple[Optional[str], Optional[str]]:
    """Streamlit Secrets から KABU+ の認証情報を取得"""
    try:
        uid = st.secrets["kabuplus"]["id"]
        pwd = st.secrets["kabuplus"]["password"]
        return uid, pwd
    except Exception:
        return None, None


def get_credentials_env() -> Tuple[Optional[str], Optional[str]]:
    """環境変数から認証情報を取得（GitHub Actions 用）"""
    import os
    uid = os.environ.get("KABUPLUS_ID")
    pwd = os.environ.get("KABUPLUS_PASSWORD")
    return uid, pwd


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
    """
    KABU+ CSV を取得して DataFrame で返す。
    土日祝はデータがないので直近営業日まで遡る。
    """
    auth = HTTPBasicAuth(user_id, password)

    for days_back in range(max_days_back):
        target = datetime.now() - timedelta(days=days_back)
        date_str = target.strftime("%Y%m%d")
        url = url_template.format(date=date_str)

        try:
            resp = requests.get(url, auth=auth, timeout=60)
            if resp.status_code != 200:
                continue

            # Shift-JIS → UTF-8
            text = resp.content.decode("shift-jis", errors="replace")
            df = pd.read_csv(io.StringIO(text))

            if len(df) < 100:  # サニティチェック
                continue

            # カラム名を正規化
            rename = {k: v for k, v in col_map.items() if k in df.columns}
            df = df.rename(columns=rename)

            # 銘柄コードを文字列に統一
            if "code" in df.columns:
                df["code"] = df["code"].astype(str).str.strip()

            # 数値カラムをクリーニング
            df = _clean_numeric_columns(df)

            return df

        except Exception:
            continue

    return pd.DataFrame()


def _clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """数値カラムのカンマ除去・型変換"""
    numeric_cols = [
        "price", "change", "change_pct", "prev_close",
        "open", "high", "low", "vwap",
        "volume", "turnover_rate",
        "trading_value_k", "market_cap_m",
        "ytd_high", "ytd_high_deviation",
        "ytd_low", "ytd_low_deviation",
        "per", "pbr", "eps", "bps",
        "dividend_yield", "shares_outstanding",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
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
@st.cache_data(ttl=600, show_spinner=False)
def fetch_stock_prices(user_id: str, password: str) -> pd.DataFrame:
    """
    全銘柄の株価一覧（詳細版）を一括取得。
    含まれるカラム: code, name, market, price, change_pct,
                   volume, turnover_rate, trading_value_k,
                   market_cap_m, ytd_high/low, ...
    """
    return _fetch_csv(PRICES_URL, user_id, password, PRICE_COLUMNS)


@st.cache_data(ttl=600, show_spinner=False)
def fetch_stock_indicators(user_id: str, password: str) -> pd.DataFrame:
    """
    全銘柄の投資指標データを一括取得。
    含まれるカラム: code, per, pbr, eps, bps,
                   dividend_yield, shares_outstanding, ...
    """
    return _fetch_csv(INDICATORS_URL, user_id, password, INDICATOR_COLUMNS)


@st.cache_data(ttl=600, show_spinner=False)
def fetch_merged_data(user_id: str, password: str) -> pd.DataFrame:
    """
    株価 + 投資指標をマージした統合データ。
    M&A 分析ではこれを使う。
    """
    prices = fetch_stock_prices(user_id, password)
    if prices.empty:
        return prices

    indicators = fetch_stock_indicators(user_id, password)
    if indicators.empty:
        return prices

    # code で結合（投資指標の name/market/industry は重複するので除外）
    ind_cols = [c for c in indicators.columns
                if c not in ("name", "market", "industry") or c == "code"]
    merged = prices.merge(
        indicators[ind_cols], on="code", how="left", suffixes=("", "_ind")
    )
    return merged


# ==========================================
# ユーティリティ
# ==========================================
def filter_by_market(df: pd.DataFrame, market: str) -> pd.DataFrame:
    """市場区分でフィルタリング"""
    if df.empty or "market" not in df.columns:
        return df
    market_map = {
        "prime": "プライム",
        "standard": "スタンダード",
        "growth": "グロース",
    }
    keyword = market_map.get(market, market)
    return df[df["market"].str.contains(keyword, na=False)].copy()


def filter_active_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """出来高ゼロ・値付かずを除外"""
    if df.empty:
        return df
    mask = (
        df["price"].notna()
        & (df["price"] > 0)
        & df["volume"].notna()
        & (df["volume"] > 0)
    )
    return df[mask].copy()


# ==========================================
# GitHub Actions 用（キャッシュなし版）
# ==========================================
def fetch_stock_prices_nocache(user_id: str, password: str) -> pd.DataFrame:
    """Streamlit 外から呼ぶ用"""
    return _fetch_csv(PRICES_URL, user_id, password, PRICE_COLUMNS)


def fetch_stock_indicators_nocache(user_id: str, password: str) -> pd.DataFrame:
    return _fetch_csv(INDICATORS_URL, user_id, password, INDICATOR_COLUMNS)


def fetch_merged_data_nocache(user_id: str, password: str) -> pd.DataFrame:
    prices = fetch_stock_prices_nocache(user_id, password)
    if prices.empty:
        return prices
    indicators = fetch_stock_indicators_nocache(user_id, password)
    if indicators.empty:
        return prices
    ind_cols = [c for c in indicators.columns
                if c not in ("name", "market", "industry") or c == "code"]
    return prices.merge(
        indicators[ind_cols], on="code", how="left", suffixes=("", "_ind")
    )
