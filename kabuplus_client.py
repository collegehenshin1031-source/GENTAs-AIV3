"""
KABU+ データ取得クライアント（v6 安定版）
─────────────────────────────────────────
・指標/信用残 → 実績済みの _fetch_csv（Session不使用）に戻す
・OHLC       → 1スレッド逐次取得 + 詳細エラーログで原因特定
・gzip       → OHLC のみ試行（指標は従来通り）
"""

from __future__ import annotations
import io
import os
import time
import threading
from typing import Optional, Tuple
from datetime import datetime, timedelta, date as date_t

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

# ==========================================
# URL テンプレート
# ==========================================
PRICES_URL    = "https://csvex.com/kabu.plus/csv/japan-all-stock-prices-2/daily/japan-all-stock-prices-2_{date}.csv"
INDICATORS_URL = "https://csvex.com/kabu.plus/csv/japan-all-stock-data/daily/japan-all-stock-data_{date}.csv"
MARGIN_URL    = "https://csvex.com/kabu.plus/csv/japan-all-stock-margin-transactions/weekly/japan-all-stock-margin-transactions_{date}.csv"
OHLC_URL      = "https://csvex.com/kabu.plus/csv/tosho-stock-ohlc/daily/tosho-stock-ohlc_{date}.csv"

# ==========================================
# カラム名マッピング
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
    "SC": "code", "日付": "date_str",
    "始値": "open", "高値": "high", "安値": "low",
    "終値": "close", "VWAP": "vwap", "出来高": "volume",
    "売買代金": "trading_value",
}
MARGIN_COLUMNS = {
    "SC": "code", "名称": "name", "市場": "market",
    "信用買残": "margin_buy", "信用買残前週比": "margin_buy_change",
    "信用売残": "margin_sell", "信用売残前週比": "margin_sell_change",
    "貸借倍率": "margin_ratio",
}


# ==========================================
# 認証情報
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
# 実績済み CSV 取得（Session不使用・従来通り）
# ==========================================
def _fetch_csv(url_template, user_id, password, col_map, max_days_back=7):
    """v1から動作実績あり。Session不使用のまま維持。"""
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


def _clean_numeric(df):
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
# OHLC 1日分取得（詳細エラーログ付き）
# ==========================================
def _fetch_ohlc_one_day(
    target_date: date_t,
    auth: HTTPBasicAuth,
    timeout: int = 45,
    verbose: bool = False,
) -> Optional[pd.DataFrame]:
    """tosho-stock-ohlc 1日分取得。失敗理由をverboseで出力。"""
    date_str = target_date.strftime("%Y%m%d")
    url = OHLC_URL.format(date=date_str)
    try:
        resp = requests.get(url, auth=auth, timeout=timeout,
                            headers={"Accept-Encoding": "gzip, deflate"})
        if resp.status_code == 404:
            if verbose:
                print(f"    [{date_str}] 404 Not Found（休日または未公開）")
            return None
        if resp.status_code == 401:
            print(f"    [{date_str}] ⚠️ 401 Unauthorized → 認証情報を確認してください")
            return None
        if resp.status_code != 200:
            if verbose:
                print(f"    [{date_str}] HTTP {resp.status_code}")
            return None

        text = resp.content.decode("shift-jis", errors="replace")
        df = pd.read_csv(io.StringIO(text))
        if len(df) < 10:
            if verbose:
                print(f"    [{date_str}] 行数不足: {len(df)} 行")
            return None

        rename = {k: v for k, v in OHLC_COLUMNS.items() if k in df.columns}
        df = df.rename(columns=rename)

        if "code" not in df.columns or "close" not in df.columns:
            if verbose:
                print(f"    [{date_str}] 列不足: {df.columns.tolist()[:8]}")
            return None

        df["code"] = (df["code"].astype(str).str.strip()
                      .str.replace(r"\.0$", "", regex=True))
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", "", regex=False).str.strip(),
                    errors="coerce")

        if df["close"].isna().all():
            if verbose:
                print(f"    [{date_str}] close 全NaN（休業日）")
            return None

        df["_date"] = target_date
        return df

    except requests.exceptions.Timeout:
        if verbose:
            print(f"    [{date_str}] Timeout ({timeout}s)")
        return None
    except Exception as e:
        if verbose:
            print(f"    [{date_str}] Exception: {e}")
        return None


# ==========================================
# OHLC 履歴取得（逐次・詳細ログ）
# ==========================================
def fetch_ohlc_history(
    user_id: str,
    password: str,
    lookback_days: int = 250,
    calendar_window: int = 390,
    request_interval: float = 1.0,   # 1秒間隔（過負荷防止）
) -> dict[str, pd.DataFrame]:
    """
    tosho-stock-ohlc を逐次取得（1スレッド）。
    最初の10件はverbose=Trueで失敗理由を表示。
    """
    auth = HTTPBasicAuth(user_id, password)
    today = datetime.now().date()
    frames_by_date: dict[date_t, pd.DataFrame] = {}

    print(f"  📡 OHLC逐次取得開始（目標{lookback_days}営業日 / 間隔{request_interval}s）")

    for i in range(calendar_window):
        target = today - timedelta(days=i)
        verbose = (i < 10)  # 最初の10件だけ詳細ログ

        result = _fetch_ohlc_one_day(target, auth, verbose=verbose)
        if result is not None:
            frames_by_date[target] = result
            if len(frames_by_date) % 20 == 0:
                print(f"    取得済み: {len(frames_by_date)} 営業日 "
                      f"（{target.strftime('%Y-%m-%d')} まで）")

        if len(frames_by_date) >= lookback_days:
            break

        if result is not None:
            time.sleep(request_interval)
        # 404/エラーは間隔なし（高速スキップ）

    if not frames_by_date:
        print("  ⚠️ OHLC CSVを1件も取得できませんでした")
        return {}

    sorted_dates = sorted(frames_by_date.keys())
    print(f"  ✅ OHLC取得完了: {len(sorted_dates)} 営業日 "
          f"（{sorted_dates[0]} 〜 {sorted_dates[-1]}）")

    combined = pd.concat([frames_by_date[d] for d in sorted_dates], ignore_index=True)
    combined = combined.dropna(subset=["close"])

    ohlc_cache: dict[str, pd.DataFrame] = {}
    for code, grp in combined.groupby("code"):
        code = str(code).strip()
        if not code or code == "nan":
            continue
        grp = grp.sort_values("_date").drop_duplicates("_date")
        idx = pd.to_datetime(grp["_date"])
        df_out = pd.DataFrame({
            "Open":   grp["open"].values   if "open"   in grp.columns else grp["close"].values,
            "High":   grp["high"].values   if "high"   in grp.columns else grp["close"].values,
            "Low":    grp["low"].values    if "low"    in grp.columns else grp["close"].values,
            "Close":  grp["close"].values,
            "Volume": grp["volume"].values  if "volume" in grp.columns else [0.0] * len(grp),
        }, index=idx)
        df_out.index.name = "Date"
        df_out["Volume"] = pd.to_numeric(df_out["Volume"], errors="coerce").fillna(0)
        df_out = df_out.dropna(subset=["Close"])
        if len(df_out) < 5:   # 最低5日分あれば保持（あとで60日チェックはfetch_dataで）
            continue
        ohlc_cache[code] = df_out

    print(f"  📊 OHLC銘柄数: {len(ohlc_cache)} 銘柄")
    return ohlc_cache


# ==========================================
# 公開 API（実績済み _fetch_csv を使用）
# ==========================================
def fetch_stock_prices(user_id, password):
    return _fetch_csv(PRICES_URL, user_id, password, PRICE_COLUMNS)

def fetch_stock_indicators(user_id, password):
    return _fetch_csv(INDICATORS_URL, user_id, password, INDICATOR_COLUMNS)

def fetch_merged_data(user_id, password):
    prices = fetch_stock_prices(user_id, password)
    if prices.empty:
        return prices
    indicators = fetch_stock_indicators(user_id, password)
    if indicators.empty:
        return prices
    ind_cols = [c for c in indicators.columns
                if c not in ("name", "market", "industry") or c == "code"]
    return prices.merge(ind_cols if False else
                        indicators[ind_cols],
                        on="code", how="left", suffixes=("", "_ind"))

def _safe_float(v, default=0.0):
    """NaN/None を default に変換して float を返す"""
    import math
    try:
        f = float(v)
        return default if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default

def build_info_lookup(merged_df):
    lookup = {}
    if merged_df.empty:
        return lookup
    for _, row in merged_df.iterrows():
        code = str(row.get("code", ""))
        if not code or code == "nan":
            continue
        ticker = f"{code}.T"
        mcap_m  = _safe_float(row.get("market_cap_m"))
        shares  = _safe_float(row.get("shares_outstanding"))
        pbr_val = _safe_float(row.get("pbr"))
        price   = _safe_float(row.get("price"))
        name    = str(row.get("name", "") or "")

        if shares <= 0 and mcap_m > 0 and price > 0:
            shares = mcap_m * 1_000_000 / price

        lookup[ticker] = {
            "marketCap":                   int(mcap_m * 1_000_000) if mcap_m > 0 else 0,
            "sharesOutstanding":           int(shares) if shares > 0 else None,
            "priceToBook":                 float(pbr_val) if pbr_val > 0 else None,
            "shortName":                   name,
            "longName":                    name,
            "currentPrice":                float(price) if price > 0 else None,
            "dividendRate":                _safe_float(row.get("dividend_per_share")),
            "dividendYield":               _safe_float(row.get("dividend_yield")) / 100.0
                                           if _safe_float(row.get("dividend_yield")) else None,
            "trailingAnnualDividendRate":  None,
            "trailingAnnualDividendYield": None,
            "payoutRatio":                 None,
        }
    return lookup

def fetch_margin_data(user_id, password):
    return _fetch_csv(MARGIN_URL, user_id, password, MARGIN_COLUMNS, max_days_back=14)

def build_margin_lookup(margin_df):
    lookup = {}
    if margin_df.empty:
        return lookup
    for _, row in margin_df.iterrows():
        code = str(row.get("code", ""))
        if not code:
            continue
        ticker = f"{code}.T"
        lookup[ticker] = {
            "margin_buy":         int(_safe_float(row.get("margin_buy"))),
            "margin_sell":        int(_safe_float(row.get("margin_sell"))),
            "margin_buy_change":  int(_safe_float(row.get("margin_buy_change"))),
            "margin_sell_change": int(_safe_float(row.get("margin_sell_change"))),
            "margin_ratio":       round(_safe_float(row.get("margin_ratio")), 2)
                                  if _safe_float(row.get("margin_ratio")) else None,
        }
    return lookup
