"""
M&A 予兆検知エンジン v3
────────────────────────
・ニュース分析 → キャッシュ JSON から読み取り（アプリ側はスクレイピングしない）
・バリュエーション → KABU+ の PBR / 時価総額 を直接使用
・ニューススクレイピング関数は auto_monitor.py（GitHub Actions）から呼ばれる
"""

from __future__ import annotations
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
import os
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd

# ==========================================
# 設定
# ==========================================
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

NEWS_CACHE_PATH = "data/news_cache.json"

MA_KEYWORDS = {
    "critical": [
        "完全子会社化", "TOB", "株式公開買付", "MBO", "株式交換",
        "吸収合併", "経営統合", "買収", "子会社化", "親会社",
        "株式移転", "スクイーズアウト", "少数株主", "上場廃止",
    ],
    "high": [
        "資本提携", "業務提携", "第三者割当", "大株主", "筆頭株主",
        "株式取得", "持株比率", "支配権", "経営権", "事業譲渡",
        "再編", "リストラ", "構造改革", "内製化", "グループ再編",
    ],
    "medium": [
        "シナジー", "相乗効果", "事業統合", "効率化", "コスト削減",
        "収益改善", "黒字化", "増配", "自社株買い", "株主還元",
        "アクティビスト", "物言う株主", "株主提案", "敵対的",
    ],
}

EXCLUSION_KEYWORDS = [
    "自社株買い発表", "大規模自社株買い", "買収防衛策", "ポイズンピル",
]


# ==========================================
# Enum / データクラス
# ==========================================
class MASignalLevel(Enum):
    CRITICAL = "🔴 緊急"
    HIGH = "🟠 高"
    MEDIUM = "🟡 中"
    LOW = "🟢 低"
    NONE = "⚪ なし"


@dataclass
class NewsItem:
    title: str
    url: str
    source: str
    date: Optional[datetime] = None
    matched_keywords: List[str] = field(default_factory=list)
    signal_level: MASignalLevel = MASignalLevel.NONE


@dataclass
class MAScore:
    code: str
    name: str
    total_score: int
    signal_level: MASignalLevel
    news_score: int
    volume_score: int
    valuation_score: int
    technical_score: int
    news_items: List[NewsItem] = field(default_factory=list)
    matched_keywords: List[str] = field(default_factory=list)
    exclusion_flags: List[str] = field(default_factory=list)
    reason_tags: List[str] = field(default_factory=list)


# ==========================================
# ニュースキャッシュの読み書き
# ==========================================
def load_news_cache() -> Dict[str, List[dict]]:
    """GitHub Actions が保存したニュースキャッシュを読み込み"""
    if os.path.exists(NEWS_CACHE_PATH):
        try:
            with open(NEWS_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_news_cache(cache: Dict[str, List[dict]]):
    """ニュースキャッシュを保存（auto_monitor から呼ばれる）"""
    os.makedirs(os.path.dirname(NEWS_CACHE_PATH), exist_ok=True)
    with open(NEWS_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _news_items_from_cache(code: str, cache: Dict[str, List[dict]]) -> List[NewsItem]:
    """キャッシュから NewsItem リストを生成"""
    items = []
    for entry in cache.get(code, []):
        level = MASignalLevel.NONE
        matched = entry.get("matched_keywords", [])
        if any(kw in MA_KEYWORDS["critical"] for kw in matched):
            level = MASignalLevel.CRITICAL
        elif any(kw in MA_KEYWORDS["high"] for kw in matched):
            level = MASignalLevel.HIGH
        elif any(kw in MA_KEYWORDS["medium"] for kw in matched):
            level = MASignalLevel.MEDIUM

        items.append(NewsItem(
            title=entry.get("title", ""),
            url=entry.get("url", ""),
            source=entry.get("source", "cache"),
            matched_keywords=matched,
            signal_level=level,
        ))
    return items


# ==========================================
# ニューススクレイピング（GitHub Actions 専用）
# ==========================================
def scrape_yahoo_news(query: str, max_results: int = 10) -> List[NewsItem]:
    """Yahoo! ニュースから関連ニュースをスクレイピング"""
    news_items = []
    try:
        url = f"https://news.yahoo.co.jp/search?p={requests.utils.quote(query)}&ei=UTF-8"
        time.sleep(random.uniform(1.0, 2.5))
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return news_items

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = soup.select(
            'div.newsFeed_item, article.newsFeed_item, '
            'div[class*="NewsItem"], a[href*="/articles/"]'
        )

        for article in articles[:max_results]:
            try:
                title_elem = article.select_one(
                    'h2, h3, span[class*="title"], div[class*="title"]'
                )
                if not title_elem:
                    title_elem = article
                title = title_elem.get_text(strip=True)
                if not title or len(title) < 5:
                    continue

                link = article.select_one('a[href*="/articles/"]')
                article_url = ""
                if link:
                    article_url = link.get("href", "")
                    if not article_url.startswith("http"):
                        article_url = f"https://news.yahoo.co.jp{article_url}"

                matched, signal = _match_keywords(title)

                news_items.append(NewsItem(
                    title=title, url=article_url,
                    source="Yahoo!ニュース",
                    matched_keywords=matched,
                    signal_level=signal,
                ))
            except Exception:
                continue
    except Exception:
        pass
    return news_items


def _match_keywords(text: str) -> Tuple[List[str], MASignalLevel]:
    """テキストからM&Aキーワードをマッチング"""
    matched = []
    signal = MASignalLevel.NONE
    for kw in MA_KEYWORDS["critical"]:
        if kw in text:
            matched.append(kw)
            signal = MASignalLevel.CRITICAL
    if signal == MASignalLevel.NONE:
        for kw in MA_KEYWORDS["high"]:
            if kw in text:
                matched.append(kw)
                signal = MASignalLevel.HIGH
    if signal == MASignalLevel.NONE:
        for kw in MA_KEYWORDS["medium"]:
            if kw in text:
                matched.append(kw)
                signal = MASignalLevel.MEDIUM
    return matched, signal


def scrape_news_for_stock(name: str, code: str) -> List[dict]:
    """
    1銘柄分のニュースをスクレイピング（GitHub Actions から呼ばれる）。
    クエリを 1 パターンに絞って高速化。
    """
    query = f"{name} M&A TOB 完全子会社化"
    items = scrape_yahoo_news(query, max_results=10)

    result = []
    seen_titles = set()
    for item in items:
        if item.title in seen_titles:
            continue
        seen_titles.add(item.title)
        result.append({
            "title": item.title,
            "url": item.url,
            "source": item.source,
            "matched_keywords": item.matched_keywords,
            "date": datetime.now().strftime("%Y-%m-%d"),
        })
    return result


# ==========================================
# スコアリング
# ==========================================
def _news_score(news_items: List[NewsItem]) -> Tuple[int, List[str]]:
    """ニューススコア（最大40点）"""
    crit = sum(1 for n in news_items if n.signal_level == MASignalLevel.CRITICAL)
    high = sum(1 for n in news_items if n.signal_level == MASignalLevel.HIGH)
    med = sum(1 for n in news_items if n.signal_level == MASignalLevel.MEDIUM)

    score = min(25, crit * 10) + min(10, high * 3) + min(5, med * 1)
    keywords = []
    for n in news_items:
        keywords.extend(n.matched_keywords)
    return min(40, score), list(set(keywords))


def _volume_ma_score(row: pd.Series) -> int:
    """出来高スコア（最大30点）"""
    score = 0
    turnover = row.get("turnover_rate", 0) or 0
    tv_k = row.get("trading_value_k", 0) or 0
    mcap_m = row.get("market_cap_m", 0) or 0

    # 回転率
    if turnover >= 10.0:
        score += 15
    elif turnover >= 5.0:
        score += 10
    elif turnover >= 2.0:
        score += 5

    # 売買代金/時価総額比
    if mcap_m > 0:
        tv_ratio = (tv_k / 1000) / (mcap_m) * 100  # %
        if tv_ratio >= 5.0:
            score += 10
        elif tv_ratio >= 2.0:
            score += 7
        elif tv_ratio >= 1.0:
            score += 3

    # 回転率ボーナス
    if turnover >= 8.0:
        score += 5

    return min(30, score)


def _valuation_score(row: pd.Series) -> int:
    """バリュエーションスコア（最大20点）"""
    score = 0
    pbr = row.get("pbr", None)
    mcap_m = row.get("market_cap_m", 0) or 0

    # PBR
    if pbr is not None and pbr > 0:
        if pbr < 0.5:
            score += 8
        elif pbr < 0.8:
            score += 6
        elif pbr < 1.0:
            score += 4

    # 時価総額（中小型が狙われやすい）
    if mcap_m > 0:
        mc_oku = mcap_m / 100
        if 300 <= mc_oku <= 2000:
            score += 6
        elif 2000 < mc_oku <= 5000:
            score += 3

    # 年初来安値乖離率（割安度の代理指標）
    ytd_low_dev = row.get("ytd_low_deviation", 0) or 0
    if ytd_low_dev > 0 and ytd_low_dev <= 10:
        score += 6  # 年初来安値に近い
    elif ytd_low_dev > 0 and ytd_low_dev <= 20:
        score += 3

    return min(20, score)


def _technical_score(row: pd.Series) -> int:
    """テクニカルスコア（最大10点）"""
    score = 0
    change_pct = row.get("change_pct", 0) or 0
    ytd_high_dev = row.get("ytd_high_deviation", 0) or 0

    # 直近の値動き
    if change_pct >= 5:
        score += 5  # 大幅上昇（何かあった？）
    elif change_pct <= -5:
        score += 3  # 大幅下落（投げ売り→買い集め？）

    # 年初来高値からの下落率が大きい → 買収しやすい
    if ytd_high_dev is not None:
        dev = abs(ytd_high_dev)
        if dev >= 30:
            score += 5
        elif dev >= 20:
            score += 3

    return min(10, score)


def _check_exclusion(news_items: List[NewsItem]) -> Tuple[int, List[str]]:
    """除外要因チェック"""
    penalty, flags = 0, []
    for n in news_items:
        for kw in EXCLUSION_KEYWORDS:
            if kw in n.title:
                flags.append(kw)
                penalty += 15
    return penalty, list(set(flags))


def _reason_tags(
    n_score: int, v_score: int, val_score: int, keywords: List[str]
) -> List[str]:
    tags = []
    if n_score >= 20:
        tags.append("📰 M&Aニュース検知")
    if v_score >= 15:
        tags.append("📈 出来高急増")
    if val_score >= 12:
        tags.append("💰 割安×買収適正サイズ")
    if any(kw in keywords for kw in ["完全子会社化", "TOB", "株式公開買付"]):
        tags.append("🎯 直接シグナル")
    if any(kw in keywords for kw in ["親会社", "グループ再編", "内製化"]):
        tags.append("🏢 親子関係")
    if any(kw in keywords for kw in ["アクティビスト", "物言う株主"]):
        tags.append("🦅 アクティビスト")
    return tags


def get_signal_level(total: int) -> MASignalLevel:
    if total >= 70:
        return MASignalLevel.CRITICAL
    if total >= 50:
        return MASignalLevel.HIGH
    if total >= 30:
        return MASignalLevel.MEDIUM
    if total >= 15:
        return MASignalLevel.LOW
    return MASignalLevel.NONE


# ==========================================
# メイン分析（アプリから呼ばれる）
# ==========================================
def analyze_ma_from_dataframe(
    df: pd.DataFrame,
    codes: List[str],
    news_cache: Optional[Dict[str, List[dict]]] = None,
) -> List[MAScore]:
    """
    KABU+ DataFrame + ニュースキャッシュから M&A 予兆を分析。
    スクレイピングは行わない。
    """
    if news_cache is None:
        news_cache = load_news_cache()

    results = []
    target_df = df[df["code"].isin(codes)] if codes else df

    for _, row in target_df.iterrows():
        code = str(row.get("code", ""))
        name = str(row.get("name", ""))

        # ニュース（キャッシュから）
        news_items = _news_items_from_cache(code, news_cache)
        n_score, keywords = _news_score(news_items)

        # 出来高
        v_score = _volume_ma_score(row)

        # バリュエーション
        val_score = _valuation_score(row)

        # テクニカル
        t_score = _technical_score(row)

        # 除外チェック
        penalty, exclusion = _check_exclusion(news_items)

        total = max(0, n_score + v_score + val_score + t_score - penalty)
        level = get_signal_level(total)
        tags = _reason_tags(n_score, v_score, val_score, keywords)

        results.append(MAScore(
            code=code, name=name,
            total_score=total, signal_level=level,
            news_score=n_score, volume_score=v_score,
            valuation_score=val_score, technical_score=t_score,
            news_items=news_items, matched_keywords=keywords,
            exclusion_flags=exclusion, reason_tags=tags,
        ))

    results.sort(key=lambda x: x.total_score, reverse=True)
    return results
