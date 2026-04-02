"""
ハゲタカスコープ - 検知エンジン v3
──────────────────────────────────
・KABU+ DataFrame から一括スコアリング（yfinance 完全撤廃）
・二段階スコアリング（ゲート → スコア）
・全銘柄を数秒で処理
"""

from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import json
import os
import pandas as pd
import numpy as np


# ==========================================
# 定数・Enum
# ==========================================
class SignalLevel(Enum):
    LOCKON = "🔴 ロックオン"
    HIGH = "🟠 高警戒"
    MEDIUM = "🟡 監視中"
    LOW = "🟢 平常"


class ScanMode(Enum):
    QUICK = "quick"
    PRIME = "prime"
    STANDARD = "standard"
    GROWTH = "growth"
    ALL = "all"
    CUSTOM = "custom"


@dataclass
class ScanOption:
    mode: ScanMode
    label: str
    description: str
    estimated_count: int
    estimated_time: str
    warning: Optional[str] = None


SCAN_OPTIONS = {
    ScanMode.QUICK: ScanOption(
        ScanMode.QUICK, "⚡ クイックスキャン（推奨）",
        "売買代金上位100銘柄を高速スキャン", 100, "約3秒",
    ),
    ScanMode.PRIME: ScanOption(
        ScanMode.PRIME, "🏢 プライム市場",
        "東証プライム上場銘柄", 1800, "約3秒",
    ),
    ScanMode.STANDARD: ScanOption(
        ScanMode.STANDARD, "🏬 スタンダード市場",
        "東証スタンダード上場銘柄", 1400, "約3秒",
    ),
    ScanMode.GROWTH: ScanOption(
        ScanMode.GROWTH, "🌱 グロース市場",
        "東証グロース上場銘柄", 500, "約3秒",
    ),
    ScanMode.ALL: ScanOption(
        ScanMode.ALL, "🌐 全銘柄スキャン",
        "日本株全銘柄（約4,000社）", 4000, "約5秒",
    ),
    ScanMode.CUSTOM: ScanOption(
        ScanMode.CUSTOM, "✏️ 銘柄コードを直接入力",
        "スキャンしたい銘柄を指定", 0, "入力数による",
    ),
}


# ==========================================
# ゲート条件（入口フィルター）
# ==========================================
GATE = {
    "min_trading_value_k": 100_000,   # 売買代金 1億円（千円単位）
    "min_price": 100,                 # 株価 100円以上
}

LOCKON = {
    "min_score": 60,
    "max_count": 5,
    "high_score": 45,
    "medium_score": 30,
}

VOLUME_HISTORY_PATH = "data/volume_history.json"


# ==========================================
# シグナルデータクラス
# ==========================================
@dataclass
class HagetakaSignal:
    code: str
    name: str
    signal_level: SignalLevel
    total_score: int  # 0-100

    stealth_score: int = 0      # ステルス集積 (0-35)
    board_score: int = 0        # 板の違和感 (0-35)
    volume_score: int = 0       # 出来高臨界点 (0-30)
    bonus_score: int = 0

    signals: List[str] = field(default_factory=list)

    price: float = 0
    change_pct: float = 0
    volume: int = 0
    avg_volume: int = 0
    volume_ratio: float = 0
    turnover_pct: float = 0
    market_cap: float = 0
    trading_value: float = 0

    detected_at: datetime = field(default_factory=datetime.now)


# ==========================================
# 出来高履歴（volume_ratio 算出用）
# ==========================================
def load_volume_history() -> Dict[str, list]:
    """過去の出来高データを読み込み"""
    if os.path.exists(VOLUME_HISTORY_PATH):
        try:
            with open(VOLUME_HISTORY_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_volume_history(history: Dict[str, list]):
    """出来高データを保存"""
    os.makedirs(os.path.dirname(VOLUME_HISTORY_PATH), exist_ok=True)
    with open(VOLUME_HISTORY_PATH, "w") as f:
        json.dump(history, f)


def update_volume_history(df: pd.DataFrame, history: Dict[str, list]) -> Dict[str, list]:
    """
    今日の出来高を履歴に追記（直近20日分を保持）。
    GitHub Actions が毎日実行して蓄積する。
    """
    today = datetime.now().strftime("%Y-%m-%d")

    for _, row in df.iterrows():
        code = str(row.get("code", ""))
        vol = row.get("volume", 0)
        if not code or pd.isna(vol) or vol <= 0:
            continue

        if code not in history:
            history[code] = []

        # 同日データは上書き
        entries = [(d, v) for d, v in history[code] if d != today]
        entries.append((today, int(vol)))
        entries = entries[-20:]  # 直近20日
        history[code] = entries

    return history


def get_volume_ratio(code: str, current_vol: float, history: Dict[str, list]) -> float:
    """過去平均と比較した出来高倍率"""
    entries = history.get(code, [])
    if len(entries) < 3:
        return 0.0  # 履歴不足
    past_vols = [v for _, v in entries[:-1]]  # 当日を除く過去分
    if not past_vols:
        return 0.0
    avg = sum(past_vols) / len(past_vols)
    return current_vol / avg if avg > 0 else 0.0


# ==========================================
# スコアリング
# ==========================================
def _stealth_score(row: pd.Series, vol_ratio: float) -> Tuple[int, List[str]]:
    """ステルス集積スコア（最大35点）"""
    scores, signals = [], []
    change = abs(row.get("change_pct", 0) or 0)
    turnover = row.get("turnover_rate", 0) or 0
    mcap_m = row.get("market_cap_m", 0) or 0

    # 条件1: 出来高倍率または回転率の急増（最大15点）
    if vol_ratio >= 2.5:
        scores.append(15); signals.append("📈 出来高が平均の2.5倍以上")
    elif vol_ratio >= 1.8:
        scores.append(10); signals.append("📈 出来高が平均の1.8倍以上")
    elif turnover >= 5.0:
        scores.append(10); signals.append("📈 回転率5%超（活況）")
    elif vol_ratio >= 1.3:
        scores.append(5); signals.append("📈 出来高やや増加傾向")
    elif turnover >= 2.0:
        scores.append(5); signals.append("📈 回転率2%超")
    else:
        scores.append(0)

    # 条件2: 値動き小 × 出来高増 = ステルス集積（最大12点）
    high_vol = (vol_ratio >= 1.5) or (turnover >= 3.0)
    if change < 2.0 and high_vol:
        scores.append(12); signals.append("🥷 値動き小×出来高増＝ステルス集積の可能性")
    elif change < 3.0 and (vol_ratio >= 1.3 or turnover >= 2.0):
        scores.append(8); signals.append("🥷 目立たない買い集めの兆候")
    elif vol_ratio >= 1.2 or turnover >= 1.5:
        scores.append(4); signals.append("🥷 出来高やや増加")
    else:
        scores.append(0)

    # 条件3: 時価総額が買収適正サイズ（最大10点）
    if mcap_m > 0:
        mcap_oku = mcap_m / 100  # 百万円→億円
        if 300 <= mcap_oku <= 3000:
            scores.append(10); signals.append("🎯 時価総額がハゲタカ好適サイズ")
        elif 100 <= mcap_oku < 300 or 3000 < mcap_oku <= 5000:
            scores.append(6); signals.append("🎯 時価総額が買収対象圏内")
        else:
            scores.append(0)
    else:
        scores.append(0)

    top2 = sum(sorted(scores, reverse=True)[:2])
    active = [s for s, sc in zip(signals, scores) if sc > 0]
    return min(top2, 35), active


def _board_score(row: pd.Series) -> Tuple[int, List[str]]:
    """板の違和感スコア（最大35点）"""
    scores, signals = [], []
    price = row.get("price", 0) or 0
    ytd_high = row.get("ytd_high", 0) or 0
    ytd_low = row.get("ytd_low", 0) or 0
    ytd_high_dev = row.get("ytd_high_deviation", 0) or 0
    ytd_low_dev = row.get("ytd_low_deviation", 0) or 0
    vwap = row.get("vwap", 0) or 0

    if price <= 0:
        return 0, []

    # 条件1: 年初来高値・安値との位置関係（最大15点）
    if ytd_high > 0 and ytd_low > 0 and ytd_high > ytd_low:
        position = (price - ytd_low) / (ytd_high - ytd_low)
        if position <= 0.15:
            scores.append(15); signals.append("📉 年初来安値圏（底値買い狙い）")
        elif position >= 0.95:
            scores.append(12); signals.append("📈 年初来高値ブレイク狙い")
        elif position <= 0.3:
            scores.append(8); signals.append("📉 安値圏で推移")
        else:
            scores.append(0)
    else:
        scores.append(0)

    # 条件2: 年初来高値乖離率（最大12点）
    if ytd_high_dev is not None and ytd_high_dev != 0:
        dev = abs(ytd_high_dev)
        if dev >= 40:
            scores.append(12); signals.append("📊 年初来高値から大幅下落（反発狙い）")
        elif dev >= 25:
            scores.append(8); signals.append("📊 年初来高値から乖離大")
        elif dev >= 15:
            scores.append(5); signals.append("📊 年初来高値から乖離中")
        else:
            scores.append(0)
    else:
        scores.append(0)

    # 条件3: VWAP との乖離（最大10点）
    if vwap > 0 and price > 0:
        vwap_dev = (price - vwap) / vwap * 100
        if vwap_dev >= 3.0:
            scores.append(10); signals.append("📈 VWAP上方乖離（買い圧力強い）")
        elif vwap_dev <= -3.0:
            scores.append(10); signals.append("📉 VWAP下方乖離（売り圧力→反発狙い）")
        elif abs(vwap_dev) >= 1.5:
            scores.append(5); signals.append("📊 VWAPから乖離")
        else:
            scores.append(0)
    else:
        scores.append(0)

    top2 = sum(sorted(scores, reverse=True)[:2])
    active = [s for s, sc in zip(signals, scores) if sc > 0]
    return min(top2, 35), active


def _volume_critical_score(row: pd.Series, vol_ratio: float) -> Tuple[int, List[str]]:
    """出来高臨界点スコア（最大30点）"""
    scores, signals = [], []
    turnover = row.get("turnover_rate", 0) or 0
    tv_k = row.get("trading_value_k", 0) or 0
    mcap_m = row.get("market_cap_m", 0) or 0

    # 条件1: 出来高倍率（最大15点）
    if vol_ratio >= 3.0:
        scores.append(15); signals.append("🔥 出来高3倍超（着火）")
    elif vol_ratio >= 2.0:
        scores.append(12); signals.append("🚀 出来高2倍超（予兆）")
    elif vol_ratio >= 1.5:
        scores.append(8); signals.append("⚡ 出来高1.5倍超")
    elif vol_ratio >= 1.3:
        scores.append(4); signals.append("⚡ 出来高1.3倍超")
    elif turnover >= 5.0:
        # 履歴なしの場合、回転率で代替
        scores.append(8); signals.append("⚡ 回転率5%超")
    elif turnover >= 2.0:
        scores.append(4); signals.append("⚡ 回転率2%超")
    else:
        scores.append(0)

    # 条件2: 浮動株回転率（最大12点）
    if turnover >= 8.0:
        scores.append(12); signals.append("🌪️ 浮動株激動（8%超回転）")
    elif turnover >= 5.0:
        scores.append(9); signals.append("🌪️ 浮動株活況（5%超回転）")
    elif turnover >= 2.0:
        scores.append(5); signals.append("🌪️ 浮動株回転率上昇")
    else:
        scores.append(0)

    # 条件3: 売買代金の絶対値（最大10点）
    tv_oku = tv_k / 100_000  # 千円→億円
    if mcap_m > 0:
        mcap_oku = mcap_m / 100
        tv_ratio = tv_oku / mcap_oku * 100 if mcap_oku > 0 else 0
        if tv_ratio >= 5.0:
            scores.append(10); signals.append("💰 売買代金が時価総額比5%超")
        elif tv_ratio >= 2.0:
            scores.append(7); signals.append("💰 売買代金が時価総額比2%超")
        elif tv_oku >= 50:
            scores.append(4); signals.append("💰 売買代金50億超")
        else:
            scores.append(0)
    elif tv_oku >= 100:
        scores.append(7); signals.append("💰 売買代金100億超")
    else:
        scores.append(0)

    top2 = sum(sorted(scores, reverse=True)[:2])
    active = [s for s, sc in zip(signals, scores) if sc > 0]
    return min(top2, 30), active


def _bonus_score(row: pd.Series, vol_ratio: float) -> Tuple[int, List[str]]:
    """ボーナススコア（最大+15点）"""
    bonus, signals = 0, []
    turnover = row.get("turnover_rate", 0) or 0
    mcap_m = row.get("market_cap_m", 0) or 0

    if vol_ratio >= 2.5:
        bonus += 5; signals.append("🌟 出来高急増ボーナス")
    if turnover >= 8.0:
        bonus += 5; signals.append("🌟 高回転率ボーナス")
    if mcap_m > 0 and mcap_m <= 50000:  # 500億以下
        if vol_ratio >= 1.5 or turnover >= 3.0:
            bonus += 5
            signals.append(f"🌟 小型株急動意ボーナス（{mcap_m / 100:.0f}億円）")

    return min(bonus, 15), signals


# ==========================================
# メインスキャン
# ==========================================
def scan_dataframe(
    df: pd.DataFrame,
    volume_history: Optional[Dict[str, list]] = None,
) -> List[HagetakaSignal]:
    """
    KABU+ DataFrame を受け取り、全銘柄をスコアリング。
    yfinance 時代の 20〜30 分 → 数秒で完了。
    """
    if df.empty:
        return []

    if volume_history is None:
        volume_history = load_volume_history()

    results: List[HagetakaSignal] = []

    for _, row in df.iterrows():
        code = str(row.get("code", ""))
        price = row.get("price", 0) or 0
        tv_k = row.get("trading_value_k", 0) or 0

        # ゲート判定
        if price < GATE["min_price"]:
            continue
        if tv_k < GATE["min_trading_value_k"]:
            continue

        # 出来高倍率
        vol = row.get("volume", 0) or 0
        vol_ratio = get_volume_ratio(code, vol, volume_history)

        # スコアリング
        stealth, s_sig = _stealth_score(row, vol_ratio)
        board, b_sig = _board_score(row)
        volume_crit, v_sig = _volume_critical_score(row, vol_ratio)
        bonus, bonus_sig = _bonus_score(row, vol_ratio)

        total = min(stealth + board + volume_crit + bonus, 100)
        all_signals = s_sig + b_sig + v_sig + bonus_sig

        # シグナルレベル（暫定）
        if total >= LOCKON["min_score"]:
            level = SignalLevel.HIGH  # 後でロックオン昇格
        elif total >= LOCKON["high_score"]:
            level = SignalLevel.HIGH
        elif total >= LOCKON["medium_score"]:
            level = SignalLevel.MEDIUM
        else:
            level = SignalLevel.LOW

        turnover = row.get("turnover_rate", 0) or 0
        mcap_m = row.get("market_cap_m", 0) or 0

        results.append(HagetakaSignal(
            code=code,
            name=str(row.get("name", code)),
            signal_level=level,
            total_score=total,
            stealth_score=stealth,
            board_score=board,
            volume_score=volume_crit,
            bonus_score=bonus,
            signals=all_signals,
            price=price,
            change_pct=row.get("change_pct", 0) or 0,
            volume=int(vol),
            avg_volume=0,
            volume_ratio=vol_ratio,
            turnover_pct=turnover,
            market_cap=mcap_m * 1_000_000,  # 百万円→円
            trading_value=tv_k * 1_000,      # 千円→円
        ))

    # スコア順ソート
    results.sort(key=lambda x: x.total_score, reverse=True)

    # ロックオン昇格（上位N件）
    lockon_count = 0
    for sig in results:
        if sig.total_score >= LOCKON["min_score"] and lockon_count < LOCKON["max_count"]:
            sig.signal_level = SignalLevel.LOCKON
            lockon_count += 1
        elif sig.total_score >= LOCKON["high_score"]:
            sig.signal_level = SignalLevel.HIGH
        elif sig.total_score >= LOCKON["medium_score"]:
            sig.signal_level = SignalLevel.MEDIUM
        else:
            sig.signal_level = SignalLevel.LOW

    return results


# ==========================================
# フィルタリング・ユーティリティ
# ==========================================
def filter_scan_targets(
    df: pd.DataFrame,
    mode: ScanMode,
    custom_codes: Optional[List[str]] = None,
) -> pd.DataFrame:
    """スキャンモードに応じて対象銘柄をフィルタリング"""
    from kabuplus_client import filter_by_market, filter_active_stocks

    df = filter_active_stocks(df)

    if mode == ScanMode.CUSTOM and custom_codes:
        return df[df["code"].isin(custom_codes)].copy()
    elif mode == ScanMode.PRIME:
        return filter_by_market(df, "prime")
    elif mode == ScanMode.STANDARD:
        return filter_by_market(df, "standard")
    elif mode == ScanMode.GROWTH:
        return filter_by_market(df, "growth")
    elif mode == ScanMode.QUICK:
        # 売買代金上位100銘柄
        return df.nlargest(100, "trading_value_k")
    else:  # ALL
        return df


def get_lockons(signals: List[HagetakaSignal]) -> List[HagetakaSignal]:
    return [s for s in signals if s.signal_level == SignalLevel.LOCKON]


def get_watchlist_signals(
    signals: List[HagetakaSignal], min_score: int = 30
) -> List[HagetakaSignal]:
    return [s for s in signals if s.total_score >= min_score]
