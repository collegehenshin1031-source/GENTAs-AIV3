"""
KABU+ データ取得クライアント（v2 OHLC対応版）
─────────────────────────────────────────────
・全銘柄の株価・指標を1回のHTTPで一括取得
・四本値CSV（tosho-stock-ohlc）を並列取得し過去250営業日のOHLCVを構築
・yfinance 依存を完全撤廃するためのメインデータソース
・Basic認証 / Shift-JIS 自動処理
・Streamlit環境（app.py）でもCLI環境（fetch_data.py）でも動作
"""

from __future__ import annotations
import io
import os
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
OHLC_URL = (
    "https://csvex.com/kabu.plus/csv/"
    "tosho-stock-ohlc/daily/"
    "tosho-stock-ohlc_{date}.csv"
)
MARGIN_URL = (
    "https://csvex.com/kabu.plus/csv/"
    "japan-all-stock-margin-transactions/weekly/"
    "japan-all-stock-margin-transactions_{date}.csv"
)

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
OHLC_COLUMNS = {
    "SC": "code",
    "名称": "name",
    "市場": "market",
    "業種": "industry",
    # 日付列（ファイルに含まれている場合）
    "日時": "dt_str",
    "日付": "dt_str",
    # 四本値
    "始値": "open",
    "高値": "high",
    "安値": "low",
    "終値": "close",
    "前日終値": "prev_close",
    "出来高": "volume",
    "売買代金（千円）": "trading_value_k",
    # 調整後が別名の場合
    "調整後始値": "open",
    "調整後高値": "high",
    "調整後安値": "low",
    "調整後終値": "close",
}
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
        "close",
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
# 四本値OHLC履歴取得（yfinance 代替）
# ==========================================
def _fetch_ohlc_one_day(
    target_date: date_t,
    auth: HTTPBasicAuth,
    timeout: int = 30,
) -> Optional[pd.DataFrame]:
    """1日分の四本値CSVを取得して DataFrame を返す。取得失敗は None。"""
    date_str = target_date.strftime("%Y%m%d")
    url = OHLC_URL.format(date=date_str)
    try:
        resp = requests.get(url, auth=auth, timeout=timeout)
        if resp.status_code != 200:
            return None
        text = resp.content.decode("shift-jis", errors="replace")
        df = pd.read_csv(io.StringIO(text))
        if len(df) < 10:
            return None

        # カラム名を正規化
        rename = {k: v for k, v in OHLC_COLUMNS.items() if k in df.columns}
        df = df.rename(columns=rename)

        # 銘柄コード正規化（"7203.0" → "7203"）
        if "code" not in df.columns:
            return None
        df["code"] = (
            df["code"].astype(str)
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
        )

        # 数値変換
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace("－", "0", regex=False)
                    .str.strip(),
                    errors="coerce",
                )

        # close がない場合は price を代用
        if "close" not in df.columns and "price" in df.columns:
            df["close"] = pd.to_numeric(
                df["price"].astype(str)
                .str.replace(",", "", regex=False)
                .str.strip(),
                errors="coerce",
            )

        # 日付カラムを付与（ファイル由来）
        df["_date"] = target_date

        # close が全 NaN なら休業日とみなす
        if df["close"].isna().all():
            return None

        return df

    except Exception:
        return None


def fetch_ohlc_history(
    user_id: str,
    password: str,
    lookback_days: int = 250,
    max_workers: int = 12,
    calendar_window: int = 390,
) -> dict[str, pd.DataFrame]:
    """
    過去 lookback_days 営業日分の四本値を並列取得し、
    {code: DataFrame(index=DatetimeIndex, columns=[Open,High,Low,Close,Volume])} を返す。
    code は文字列 "7203" 形式（.T なし）。
    """
    auth = HTTPBasicAuth(user_id, password)
    today = datetime.now().date()

    # 取得候補日（過去 calendar_window カレンダー日）
    candidate_dates: list[date_t] = [
        today - timedelta(days=i) for i in range(calendar_window)
    ]

    # ── 並列取得 ──────────────────────────────────────────
    frames_by_date: dict[date_t, pd.DataFrame] = {}
    lock = threading.Lock()
    fetched_count = 0

    def _worker(d: date_t):
        nonlocal fetched_count
        result = _fetch_ohlc_one_day(d, auth)
        if result is not None:
            with lock:
                frames_by_date[d] = result
                fetched_count += 1

    print(f"  📡 OHLC並列取得開始（最大{calendar_window}日候補 / 目標{lookback_days}営業日 / {max_workers}スレッド）")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futs = [executor.submit(_worker, d) for d in candidate_dates]
        for f in as_completed(futs):
            pass  # 結果は _worker 内で格納済み

    if not frames_by_date:
        print("  ⚠️ OHLC CSVを1件も取得できませんでした")
        return {}

    # 最新 lookback_days 営業日分だけに絞る
    sorted_dates = sorted(frames_by_date.keys(), reverse=True)[:lookback_days]
    sorted_dates.sort()  # 昇順に戻す

    print(f"  ✅ OHLC取得完了: {len(sorted_dates)} 営業日分")

    # ── 全日付を結合して銘柄ごとに分割 ─────────────────────
    all_frames = [frames_by_date[d] for d in sorted_dates]
    combined = pd.concat(all_frames, ignore_index=True)

    # 必要カラムが揃っているか確認
    need_cols = {"code", "close"}
    if not need_cols.issubset(combined.columns):
        print(f"  ⚠️ OHLCカラム不足: {combined.columns.tolist()}")
        return {}

    # 欠損close を除外
    combined = combined.dropna(subset=["close"])

    # 銘柄コードごとに DataFrame 構築
    ohlc_cache: dict[str, pd.DataFrame] = {}

    for code, grp in combined.groupby("code"):
        code = str(code).strip()
        if not code or code == "nan":
            continue
        grp = grp.sort_values("_date").drop_duplicates("_date")
        idx = pd.to_datetime(grp["_date"])

        def _col(name: str, fallback_name: str | None = None):
            if name in grp.columns:
                return grp[name].values
            if fallback_name and fallback_name in grp.columns:
                return grp[fallback_name].values
            return grp["close"].values

        df_out = pd.DataFrame(
            {
                "Open":   _col("open",   "close"),
                "High":   _col("high",   "close"),
                "Low":    _col("low",    "close"),
                "Close":  grp["close"].values,
                "Volume": _col("volume"),
            },
            index=idx,
        )
        df_out.index.name = "Date"
        # Volume が数値でない行を 0 に
        df_out["Volume"] = pd.to_numeric(df_out["Volume"], errors="coerce").fillna(0).astype(float)
        ohlc_cache[code] = df_out

    print(f"  📊 OHLC銘柄数: {len(ohlc_cache)} 銘柄")
    return ohlc_cache


# ==========================================
# 公開 API（既存）
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
