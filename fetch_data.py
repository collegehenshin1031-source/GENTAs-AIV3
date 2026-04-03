"""
HAGETAKA SCOPE - 日次候補抽出（GitHub Actions用）

- 目的：市場データの“状態”を可視化し、候補を少数に絞る
- 本ツールは補助ツールであり、銘柄推奨・売買助言ではありません

出力：
- data/ratios.json … 候補（data）・参考（all_data）
- data/history/shard_XX.json（64分割）… 診断用OHLCV+info。FULL_UNIVERSE=1 でJPX上場（プライム・スタンダード・グロース）をスキャン

注意（全銘柄スキャン時）:
- 実行は 1〜数時間かかることがある / GitHub Actions の timeout-minutes を十分に取ること
- 一部銘柄は Yahoo 経由で欠損しうる / ジョブ全体が必ず成功するとは限らない
- ローカルで短時間テストするときは FULL_UNIVERSE=0（固定辞書のみ）
"""

import hashlib
import io
import json
import os
import re
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd
import pytz
import requests
# yfinance は KABU+ 四本値データに完全置換済み（yfinance依存撤廃）
# import yfinance as yf


def calculate_volume_profile(df: pd.DataFrame, bins: int = 24) -> pd.DataFrame:
    """価格帯別売買高（簡易）を計算（6か月足で使用）"""
    if df is None or df.empty:
        return pd.DataFrame()

    price_min = float(df['Low'].min())
    price_max = float(df['High'].max())
    if not np.isfinite(price_min) or not np.isfinite(price_max) or price_max <= price_min:
        return pd.DataFrame()

    price_bins = np.linspace(price_min, price_max, int(bins) + 1)

    volume_profile = []
    for i in range(len(price_bins) - 1):
        bin_low = float(price_bins[i])
        bin_high = float(price_bins[i + 1])
        bin_center = (bin_low + bin_high) / 2.0

        total_volume = 0.0
        for _, row in df.iterrows():
            low = float(row['Low'])
            high = float(row['High'])
            vol = float(row['Volume'])
            if low <= bin_high and high >= bin_low:
                overlap_low = max(low, bin_low)
                overlap_high = min(high, bin_high)
                if high > low:
                    ratio = (overlap_high - overlap_low) / (high - low)
                else:
                    ratio = 1.0
                total_volume += vol * ratio

        volume_profile.append({
            'price': bin_center,
            'price_low': bin_low,
            'price_high': bin_high,
            'volume': total_volume
        })

    return pd.DataFrame(volume_profile)


def calculate_volume_profile_with_bins(df: pd.DataFrame, price_bins: np.ndarray) -> pd.DataFrame:
    """同じprice_binsを使って価格帯別売買高を計算（差分用）"""
    if df is None or df.empty or price_bins is None or len(price_bins) < 2:
        return pd.DataFrame()

    volume_profile = []
    for i in range(len(price_bins) - 1):
        bin_low = float(price_bins[i])
        bin_high = float(price_bins[i + 1])
        bin_center = (bin_low + bin_high) / 2.0

        total_volume = 0.0
        for _, row in df.iterrows():
            low = float(row["Low"])
            high = float(row["High"])
            vol = float(row["Volume"])
            if low <= bin_high and high >= bin_low:
                overlap_low = max(low, bin_low)
                overlap_high = min(high, bin_high)
                if high > low:
                    ratio = (overlap_high - overlap_low) / (high - low)
                else:
                    ratio = 1.0
                total_volume += vol * ratio

        volume_profile.append({"price": bin_center, "price_low": bin_low, "price_high": bin_high, "volume": total_volume})

    return pd.DataFrame(volume_profile)


def compute_support_from_recent_growth(
    df: pd.DataFrame,
    bins: int = 24,
    recent_ratio: float = 0.33,
    low_band_ratio: float = 0.35,
):
    """下値ラインを「直近で価格帯別売買高が伸びた帯 × 安値付近」から選ぶ。"""
    if df is None or df.empty or len(df) < 40:
        return None, None

    price_min = float(df["Low"].min())
    price_max = float(df["High"].max())
    if not np.isfinite(price_min) or not np.isfinite(price_max) or price_max <= price_min:
        return None, None

    price_bins = np.linspace(price_min, price_max, int(bins) + 1)

    n = len(df)
    recent_len = max(20, int(n * float(recent_ratio)))
    if n < recent_len * 2:
        return None, None

    recent_df = df.tail(recent_len)
    prev_df = df.iloc[-recent_len * 2 : -recent_len]

    vp_recent = calculate_volume_profile_with_bins(recent_df, price_bins)
    vp_prev = calculate_volume_profile_with_bins(prev_df, price_bins)
    if vp_recent.empty or vp_prev.empty:
        return None, None

    vp = vp_recent.copy()
    vp["prev_volume"] = vp_prev["volume"].values
    vp["growth"] = vp["volume"] - vp["prev_volume"]

    low_limit = price_min + (price_max - price_min) * float(low_band_ratio)
    cand = vp[vp["price_high"] <= low_limit].copy()
    if cand.empty:
        return None, None

    cand = cand.sort_values("growth", ascending=False)
    best = cand.iloc[0]
    if float(best.get("growth", 0.0)) <= 0:
        return None, None

    return float(best["price_low"]), float(best["price_high"])


def compute_support_zone_from_profile(vp: pd.DataFrame, threshold_ratio: float = 0.60):
    """高出来高ゾーン（POC周辺）を抽出し、その下限を下値ラインとして返す。"""
    if vp is None or vp.empty:
        return None, None
    if 'volume' not in vp.columns:
        return None, None

    max_vol = float(vp['volume'].max())
    if max_vol <= 0:
        return None, None

    vp_reset = vp.reset_index(drop=True)
    try:
        poc_pos = int(vp_reset['volume'].idxmax())
    except Exception:
        poc_pos = 0

    thr = max_vol * float(threshold_ratio)

    left = poc_pos
    right = poc_pos
    while left - 1 >= 0 and float(vp_reset.loc[left - 1, 'volume']) >= thr:
        left -= 1
    while right + 1 < len(vp_reset) and float(vp_reset.loc[right + 1, 'volume']) >= thr:
        right += 1

    support = float(vp_reset.loc[left, 'price_low'])
    upper = float(vp_reset.loc[right, 'price_high'])
    return support, upper


def support_position_tag(latest_price: float, support_price: float | None) -> tuple[str | None, float | None]:
    """下値ラインからの位置（％）を計算し、中立的なタグ名を返す。"""
    if support_price is None or support_price <= 0:
        return None, None
    gap_pct = (latest_price / support_price - 1.0) * 100.0

    if gap_pct <= 5.0:
        return "下側ゾーン", float(gap_pct)
    if gap_pct >= 20.0:
        return "上側ゾーン", float(gap_pct)
    return None, float(gap_pct)



# ==========================================
# 設定
# ==========================================
LOOKBACK_DAYS = 252
JST = pytz.timezone("Asia/Tokyo")

# 時価総額フィルター（億円）
MARKET_CAP_MIN = 300
MARKET_CAP_MAX = 2000

# FlowScore閾値
FLOW_SCORE_HIGH = 70.0
FLOW_SCORE_MEDIUM = 40.0

# ==========================================
# 日本語銘柄名辞書（固定リスト）
# ==========================================
TICKER_NAMES = {
    # === 情報・通信・IT ===
    "3655.T": "ブレインパッド",
    "3681.T": "ブイキューブ",
    "3697.T": "SHIFT",
    "3765.T": "ガンホー・オンライン・エンターテイメント",
    "3769.T": "GMOペイメントゲートウェイ",
    "3788.T": "GMOグローバルサイン・ホールディングス",
    "3900.T": "クラウドワークス",
    "3914.T": "JIG-SAW",
    "3915.T": "テラスカイ",
    "3916.T": "デジタル・インフォメーション・テクノロジー",
    "3917.T": "アイリッジ",
    "3918.T": "PCIホールディングス",
    "3919.T": "パイプドHD",
    "3920.T": "アイビーシー",
    "3921.T": "ネオジャパン",
    "3922.T": "PR TIMES",
    "3923.T": "ラクス",
    "3925.T": "ダブルスタンダード",
    "3926.T": "オープンドア",
    "3927.T": "フーバーブレイン",
    "3928.T": "マイネット",
    "3930.T": "はてな",
    "3932.T": "アカツキ",
    "3933.T": "チエル",
    "3934.T": "ベネフィットジャパン",
    "3935.T": "エディア",
    "3936.T": "グローバルウェイ",
    "3937.T": "Ubicomホールディングス",
    "3939.T": "カナミックネットワーク",
    "3962.T": "チェンジホールディングス",
    "3966.T": "ユーザベース",
    "3967.T": "エルテス",
    "3968.T": "セグエグループ",
    "3969.T": "エイトレッド",
    "3970.T": "イノベーション",
    "3978.T": "マクロミル",
    "3979.T": "うるる",
    "3981.T": "ビーグリー",
    "3983.T": "オロ",
    "3984.T": "ユーザーローカル",
    "3985.T": "テモナ",
    "3986.T": "ビーブレイクシステムズ",
    "3987.T": "エコモット",
    "3988.T": "SYSホールディングス",
    "3989.T": "シェアリングテクノロジー",
    "3990.T": "UUUM",
    "3991.T": "ウォンテッドリー",
    "3992.T": "ニーズウェル",
    "3993.T": "PKSHA Technology",
    "3994.T": "マネーフォワード",
    "3995.T": "SKIYAKI",
    "3996.T": "サインポスト",
    "3997.T": "トレードワークス",
    "3998.T": "すららネット",
    "3999.T": "ナレッジスイート",
    "4011.T": "ヘッドウォータース",
    "4054.T": "日本情報クリエイト",
    "4057.T": "インターファクトリー",
    "4165.T": "プレイド",
    "4167.T": "ココペリ",
    "4168.T": "ヤプリ",
    "4169.T": "ENECHANGE",
    "4170.T": "Kaizen Platform",
    "4173.T": "WACUL",
    "4174.T": "アピリッツ",
    "4176.T": "ココナラ",
    "4180.T": "Appier Group",
    "4194.T": "ビジョナル",
    "4255.T": "THECOO",
    "4259.T": "エクサウィザーズ",
    "4261.T": "アジアクエスト",
    "4384.T": "ラクスル",
    "4385.T": "メルカリ",
    "4387.T": "ZUU",
    "4388.T": "エーアイ",
    "4392.T": "FIG",
    "4393.T": "バンク・オブ・イノベーション",
    "4431.T": "スマレジ",
    "4434.T": "サーバーワークス",
    "4435.T": "カオナビ",
    "4436.T": "ミンカブ・ジ・インフォノイド",
    "4441.T": "トビラシステムズ",
    "4442.T": "バルテスホールディングス",
    "4443.T": "Sansan",
    "4444.T": "インフォネット",
    "4446.T": "Link-U",
    "4448.T": "Chatwork",
    "4449.T": "ギフティ",
    "4450.T": "パワーソリューションズ",
    "4475.T": "HENNGE",
    "4476.T": "AI CROSS",
    "4477.T": "BASE",
    "4478.T": "フリー",
    "4480.T": "メドレー",
    "4482.T": "ウィルズ",
    "4483.T": "JMDC",
    "4484.T": "ランサーズ",
    "4485.T": "JTOWER",
    "4486.T": "ユナイトアンドグロウ",
    "4487.T": "スペースマーケット",
    "4488.T": "AI inside",
    "4490.T": "ビザスク",
    "4491.T": "コンピューターマネージメント",
    "4493.T": "サイバーセキュリティクラウド",
    "4494.T": "バリオセキュア",
    "4495.T": "アイキューブドシステムズ",
    "4496.T": "コマースOneホールディングス",
    "4497.T": "ロコガイド",
    "4498.T": "サイバートラスト",
    "4499.T": "Speee",
    "4751.T": "サイバーエージェント",
    "6035.T": "IRジャパンホールディングス",
    "9479.T": "インプレスホールディングス",
    "9558.T": "ジャパニアス",

    # === 半導体・電子部品 ===
    "3132.T": "マクニカホールディングス",
    "6146.T": "ディスコ",
    "6266.T": "タツモ",
    "6315.T": "TOWA",
    "6323.T": "ローツェ",
    "6506.T": "安川電機",
    "6594.T": "ニデック",
    "6645.T": "オムロン",
    "6677.T": "エスケーエレクトロニクス",
    "6723.T": "ルネサスエレクトロニクス",
    "6727.T": "ワコム",
    "6728.T": "アルバック",
    "6750.T": "エレコム",
    "6754.T": "アンリツ",
    "6755.T": "富士通ゼネラル",
    "6758.T": "ソニーグループ",
    "6762.T": "TDK",
    "6769.T": "ザインエレクトロニクス",
    "6770.T": "アルプスアルパイン",
    "6779.T": "日本電波工業",
    "6800.T": "ヨコオ",
    "6804.T": "ホシデン",
    "6806.T": "ヒロセ電機",
    "6807.T": "日本航空電子工業",
    "6814.T": "古野電気",
    "6817.T": "スミダコーポレーション",
    "6841.T": "横河電機",
    "6844.T": "新電元工業",
    "6845.T": "アズビル",
    "6855.T": "日本電子材料",
    "6856.T": "堀場製作所",
    "6857.T": "アドバンテスト",
    "6858.T": "小野測器",
    "6861.T": "キーエンス",
    "6864.T": "エヌエフホールディングス",
    "6866.T": "日置電機",
    "6869.T": "シスメックス",
    "6871.T": "日本マイクロニクス",
    "6877.T": "OBARA GROUP",
    "6879.T": "IMAGICA GROUP",
    "6881.T": "キョウデン",
    "6882.T": "三社電機製作所",
    "6890.T": "フェローテックホールディングス",
    "6902.T": "デンソー",
    "6905.T": "コーセル",
    "6908.T": "イリソ電子工業",
    "6914.T": "オプテックスグループ",
    "6918.T": "アバールデータ",
    "6920.T": "レーザーテック",
    "6923.T": "スタンレー電気",
    "6925.T": "ウシオ電機",
    "6929.T": "日本セラミック",
    "6932.T": "遠藤照明",
    "6937.T": "古河電池",
    "6941.T": "山一電機",
    "6951.T": "日本電子",
    "6952.T": "カシオ計算機",
    "6954.T": "ファナック",
    "6955.T": "FDK",
    "6958.T": "日本シイエムケイ",
    "6961.T": "エンプラス",
    "6962.T": "大真空",
    "6963.T": "ローム",
    "6965.T": "浜松ホトニクス",
    "6966.T": "三井ハイテック",
    "6967.T": "新光電気工業",
    "6971.T": "京セラ",
    "6976.T": "太陽誘電",
    "6981.T": "村田製作所",
    "6986.T": "双葉電子工業",
    "6988.T": "日東電工",
    "6989.T": "北陸電気工業",
    "6995.T": "東海理化電機製作所",
    "6996.T": "ニチコン",
    "6997.T": "日本ケミコン",
    "7735.T": "SCREENホールディングス",
    "7752.T": "リコー",

    # === バイオ・ヘルスケア・医薬品 ===
    "2183.T": "リニカル",
    "2370.T": "メディネット",
    "2372.T": "アイロムグループ",
    "2395.T": "新日本科学",
    "3386.T": "コスモ・バイオ",
    "4540.T": "ツムラ",
    "4543.T": "テルモ",
    "4547.T": "キッセイ薬品工業",
    "4548.T": "生化学工業",
    "4549.T": "栄研化学",
    "4550.T": "日水製薬",
    "4551.T": "鳥居薬品",
    "4552.T": "JCRファーマ",
    "4553.T": "東和薬品",
    "4554.T": "富士製薬工業",
    "4555.T": "沢井製薬",
    "4556.T": "カイノス",
    "4557.T": "医学生物学研究所",
    "4558.T": "中京医薬品",
    "4559.T": "ゼリア新薬工業",
    "4560.T": "日本ケミファ",
    "4563.T": "アンジェス",
    "4565.T": "そーせいグループ",
    "4566.T": "LTTバイオファーマ",
    "4568.T": "第一三共",
    "4569.T": "キョーリン製薬ホールディングス",
    "4570.T": "免疫生物研究所",
    "4571.T": "ナノキャリア",
    "4572.T": "カルナバイオサイエンス",
    "4574.T": "大幸薬品",
    "4575.T": "キャンバス",
    "4577.T": "ダイト",
    "4578.T": "大塚ホールディングス",
    "4579.T": "ラクオリア創薬",
    "4581.T": "大正製薬ホールディングス",
    "4582.T": "シンバイオ製薬",
    "4583.T": "カイオム・バイオサイエンス",
    "4584.T": "キッズウェル・バイオ",
    "4586.T": "メドレックス",
    "4587.T": "ペプチドリーム",
    "4588.T": "オンコリスバイオファーマ",
    "4591.T": "リボミック",
    "4592.T": "サンバイオ",
    "4593.T": "ヘリオス",
    "4595.T": "ミズホメディー",
    "4596.T": "窪田製薬ホールディングス",
    "4597.T": "ソレイジア・ファーマ",
    "4598.T": "Delta-Fly Pharma",

    # === EC・サービス・人材 ===
    "2124.T": "JAC Recruitment",
    "2127.T": "日本M&Aセンターホールディングス",
    "2175.T": "エス・エム・エス",
    "2181.T": "パーソルホールディングス",
    "2193.T": "クックパッド",
    "2371.T": "カカクコム",
    "2379.T": "ディップ",
    "2412.T": "ベネフィット・ワン",
    "2413.T": "エムスリー",
    "2427.T": "アウトソーシング",
    "2432.T": "ディー・エヌ・エー",
    "2440.T": "ぐるなび",
    "2453.T": "ジャパンベストレスキューシステム",
    "2454.T": "オールアバウト",
    "2461.T": "ファンコミュニケーションズ",
    "2462.T": "ライク",
    "2477.T": "手間いらず",
    "2491.T": "バリューコマース",
    "2492.T": "インフォマート",
    "2497.T": "ユナイテッド",
    "3031.T": "ラクーンホールディングス",
    "3046.T": "ジンズホールディングス",
    "3048.T": "ビックカメラ",
    "3050.T": "DCMホールディングス",
    "3064.T": "MonotaRO",
    "3076.T": "あいホールディングス",
    "3088.T": "マツキヨココカラ＆カンパニー",
    "3092.T": "ZOZO",
    "3093.T": "トレジャー・ファクトリー",
    "3134.T": "Hamee",
    "3135.T": "マーケットエンタープライズ",
    "3159.T": "丸善CHIホールディングス",
    "3167.T": "TOKAIホールディングス",
    "3176.T": "三洋貿易",
    "3179.T": "シュッピン",
    "3180.T": "ビューティガレージ",
    "3182.T": "オイシックス・ラ・大地",
    "3183.T": "ウイン・パートナーズ",
    "3186.T": "ネクステージ",
    "3193.T": "鳥貴族ホールディングス",
    "3196.T": "ホットランド",
    "3222.T": "ユナイテッド・スーパーマーケット・ホールディングス",
    "3244.T": "サムティ",
    "3254.T": "プレサンスコーポレーション",
    "3277.T": "サンセイランディック",
    "3284.T": "フージャースホールディングス",
    "3288.T": "オープンハウスグループ",
    "3289.T": "東急不動産ホールディングス",
    "6560.T": "LTS",
    "7342.T": "ウェルスナビ",

    # === ゲーム・エンタメ ===
    "3656.T": "KLab",
    "3659.T": "ネクソン",
    "3662.T": "エイチーム",
    "3668.T": "コロプラ",
    "3672.T": "オルトプラス",
    "3678.T": "メディアドゥ",
    "3679.T": "じげん",
    "3687.T": "フィックスターズ",
    "3689.T": "イグニス",
    "3696.T": "セレス",
    "3698.T": "CRI・ミドルウェア",
    "3739.T": "コムシード",
    "3760.T": "ケイブ",
    "3782.T": "ディー・ディー・エス",
    "3793.T": "ドリコム",
    "3810.T": "サイバーステップ",
    "3825.T": "リミックスポイント",
    "3835.T": "eBASE",
    "3836.T": "アバント",
    "3839.T": "ODKソリューションズ",
    "3841.T": "ジーダット",
    "3843.T": "フリービット",
    "3844.T": "コムチュア",
    "3850.T": "NTTデータイントラマート",
    "3851.T": "日本一ソフトウェア",
    "3852.T": "サイバーコム",
    "3853.T": "アステリア",
    "3854.T": "アイル",
    "3856.T": "Abalance",
    "3857.T": "ラック",
    "3858.T": "ユビキタスAI",
    "9166.T": "GENDA",

    # === 建設・不動産 ===
    "1414.T": "ショーボンドホールディングス",
    "1417.T": "ミライト・ワン",
    "1419.T": "タマホーム",
    "1429.T": "日本アクア",
    "1430.T": "ファーストコーポレーション",
    "1431.T": "Lib Work",
    "1433.T": "ベステラ",
    "1434.T": "JESCOホールディングス",
    "1435.T": "Robot Home",
    "1436.T": "フィット",
    "1438.T": "岐阜造園",
    "1443.T": "技研ホールディングス",
    "1444.T": "ニッソウ",
    "1446.T": "キャンディル",
    "1711.T": "省電舎ホールディングス",
    "1716.T": "第一カッター興業",
    "1717.T": "明豊ファシリティワークス",
    "1718.T": "美樹工業",
    "1719.T": "安藤・間",
    "1720.T": "東急建設",
    "1721.T": "コムシスホールディングス",
    "1722.T": "ミサワホーム",
    "1723.T": "日本電技",
    "1724.T": "シンクレイヤ",
    "1726.T": "ビーアールホールディングス",
    "1758.T": "太洋基礎工業",
    "1766.T": "東建コーポレーション",
    "1768.T": "ソネック",
    "1776.T": "三井住建道路",
    "1777.T": "川崎設備工業",
    "1780.T": "ヤマウラ",
    "1782.T": "常磐開発",
    "1787.T": "ナカボーテック",
    "1788.T": "三東工業社",
    "1801.T": "大成建設",
    "1802.T": "大林組",
    "1803.T": "清水建設",
    "1808.T": "長谷工コーポレーション",
    "1812.T": "鹿島建設",
    "1820.T": "西松建設",
    "1821.T": "三井住友建設",
    "1822.T": "大豊建設",
    "1824.T": "前田建設工業",
    "1827.T": "ナカノフドー建設",
    "1833.T": "奥村組",
    "1835.T": "東鉄工業",
    "1847.T": "イチケン",
    "1848.T": "富士ピー・エス",
    "1850.T": "南海辰村建設",
    "1852.T": "浅沼組",
    "1853.T": "森組",
    "1860.T": "戸田建設",
    "1861.T": "熊谷組",
    "1866.T": "北野建設",
    "1867.T": "植木組",
    "1869.T": "名工建設",
    "1870.T": "矢作建設工業",
    "1871.T": "ピーエス三菱",
    "1878.T": "大東建託",
    "1879.T": "新日本建設",
    "1881.T": "NIPPO",
    "1882.T": "東亜道路工業",
    "1883.T": "前田道路",
    "1884.T": "日本道路",
    "1885.T": "東亜建設工業",
    "1887.T": "日本国土開発",
    "1888.T": "若築建設",
    "1890.T": "東洋建設",
    "1893.T": "五洋建設",
    "1899.T": "福田組",
    "1905.T": "テノックス",
    "1911.T": "住友林業",
    "1914.T": "日本基礎技術",
    "1921.T": "巴コーポレーション",
    "1925.T": "大和ハウス工業",
    "1926.T": "ライト工業",
    "1928.T": "積水ハウス",
    "1929.T": "日特建設",
    "1930.T": "北陸電気工事",
    "1934.T": "ユアテック",
    "1939.T": "四電工",
    "1941.T": "中電工",
    "1942.T": "関電工",
    "1944.T": "きんでん",
    "1945.T": "東京エネシス",
    "1946.T": "トーエネック",
    "1949.T": "住友電設",
    "1950.T": "日本電設工業",
    "1951.T": "エクシオグループ",
    "1952.T": "新日本空調",
    "1954.T": "日本工営",
    "1959.T": "九電工",
    "1961.T": "三機工業",
    "1963.T": "日揮ホールディングス",
    "1965.T": "テクノ菱和",
    "1966.T": "高砂熱学工業",
    "1967.T": "ヤマト",
    "1968.T": "太平電業",
    "1972.T": "三晃金属工業",
    "1975.T": "朝日工業社",
    "1976.T": "明星工業",
    "1979.T": "大気社",
    "1980.T": "ダイダン",
    "1982.T": "日比谷総合設備",
    "1983.T": "東芝プラントシステム",

    # === 食品・飲料 ===
    "2001.T": "ニップン",
    "2002.T": "日清製粉グループ本社",
    "2003.T": "日東富士製粉",
    "2004.T": "昭和産業",
    "2053.T": "中部飼料",
    "2108.T": "日本甜菜製糖",
    "2109.T": "DM三井製糖ホールディングス",
    "2117.T": "ウェルネオシュガー",
    "2201.T": "森永製菓",
    "2206.T": "江崎グリコ",
    "2207.T": "名糖産業",
    "2208.T": "ブルボン",
    "2209.T": "井村屋グループ",
    "2211.T": "不二家",
    "2212.T": "山崎製パン",
    "2217.T": "モロゾフ",
    "2220.T": "亀田製菓",
    "2221.T": "岩塚製菓",
    "2222.T": "寿スピリッツ",
    "2229.T": "カルビー",
    "2264.T": "森永乳業",
    "2267.T": "ヤクルト本社",
    "2270.T": "雪印メグミルク",
    "2281.T": "プリマハム",
    "2282.T": "日本ハム",
    "2284.T": "伊藤ハム米久ホールディングス",
    "2292.T": "S FOODS",
    "2501.T": "サッポロホールディングス",
    "2502.T": "アサヒグループホールディングス",
    "2503.T": "キリンホールディングス",
    "2531.T": "宝ホールディングス",
    "2579.T": "コカ・コーラボトラーズジャパンホールディングス",
    "2587.T": "サントリー食品インターナショナル",
    "2593.T": "伊藤園",
    "2594.T": "キーコーヒー",
    "2599.T": "ジャパンフーズ",
    "2602.T": "日清オイリオグループ",
    "2607.T": "不二製油グループ本社",
    "2612.T": "かどや製油",
    "2613.T": "J-オイルミルズ",
    "2651.T": "ローソン",
    "2670.T": "エービーシー・マート",
    "2695.T": "くら寿司",
    "2702.T": "日本マクドナルドホールディングス",
    "2726.T": "パルグループホールディングス",
    "2730.T": "エディオン",
    "2782.T": "セリア",
    "2791.T": "大黒天物産",
    "2801.T": "キッコーマン",
    "2802.T": "味の素",
    "2809.T": "キユーピー",
    "2810.T": "ハウス食品グループ本社",
    "2811.T": "カゴメ",
    "2815.T": "アリアケジャパン",
    "2819.T": "エバラ食品工業",
    "2871.T": "ニチレイ",
    "2875.T": "東洋水産",
    "2876.T": "ヨコレイ",
    "2897.T": "日清食品ホールディングス",
    "2899.T": "永谷園ホールディングス",
    "2903.T": "シノブフーズ",
    "2904.T": "一正蒲鉾",
    "2907.T": "あじかん",
    "2908.T": "フジッコ",
    "2910.T": "ロック・フィールド",
    "2914.T": "日本たばこ産業",
    "2915.T": "ケンコーマヨネーズ",
    "2918.T": "わらべや日洋ホールディングス",
    "2922.T": "なとり",
    "2923.T": "サトウ食品",
    "2924.T": "イフジ産業",
    "2925.T": "ピックルスホールディングス",
    "2929.T": "ファーマフーズ",
    "2930.T": "北の達人コーポレーション",
    "2931.T": "ユーグレナ",

    # === 機械・輸送機器 ===
    "6103.T": "オークマ",
    "6104.T": "芝浦機械",
    "6113.T": "アマダ",
    "6118.T": "アイダエンジニアリング",
    "6134.T": "FUJI",
    "6135.T": "牧野フライス製作所",
    "6136.T": "OSG",
    "6141.T": "DMG森精機",
    "6143.T": "ソディック",
    "6145.T": "日特エンジニアリング",
    "6201.T": "豊田自動織機",
    "6222.T": "島精機製作所",
    "6238.T": "フリュー",
    "6255.T": "エヌ・ピー・シー",
    "6258.T": "平田機工",
    "6268.T": "ナブテスコ",
    "6269.T": "三井海洋開発",
    "6272.T": "レオン自動機",
    "6273.T": "SMC",
    "6278.T": "ユニオンツール",
    "6282.T": "オイレス工業",
    "6284.T": "日精エー・エス・ビー機械",
    "6287.T": "サトーホールディングス",
    "6289.T": "技研製作所",
    "6291.T": "日本エアーテック",
    "6292.T": "カワタ",
    "6293.T": "日精樹脂工業",
    "6294.T": "オカダアイヨン",
    "6298.T": "ワイエイシイホールディングス",
    "6301.T": "小松製作所",
    "6302.T": "住友重機械工業",
    "6305.T": "日立建機",
    "6310.T": "井関農機",
    "6316.T": "丸山製作所",
    "6317.T": "北川鉄工所",
    "6324.T": "ハーモニック・ドライブ・システムズ",
    "6326.T": "クボタ",
    "6328.T": "荏原実業",
    "6330.T": "東洋エンジニアリング",
    "6331.T": "三菱化工機",
    "6332.T": "月島ホールディングス",
    "6333.T": "帝国電機製作所",
    "6335.T": "東京機械製作所",
    "6339.T": "新東工業",
    "6340.T": "澁谷工業",
    "6345.T": "アイチコーポレーション",
    "6349.T": "小森コーポレーション",
    "6351.T": "鶴見製作所",
    "6358.T": "酒井重工業",
    "6361.T": "荏原製作所",
    "6363.T": "酉島製作所",
    "6364.T": "北越工業",
    "6366.T": "千代田化工建設",
    "6367.T": "ダイキン工業",
    "6368.T": "オルガノ",
    "6369.T": "トーヨーカネツ",
    "6370.T": "栗田工業",
    "6371.T": "椿本チエイン",
    "6376.T": "日機装",
    "6381.T": "アネスト岩田",
    "6383.T": "ダイフク",
    "6384.T": "昭和真空",
    "6387.T": "サムコ",
    "6390.T": "加藤製作所",
    "6395.T": "タダノ",
    "6407.T": "CKD",
    "6408.T": "小倉クラッチ",
    "6409.T": "キトー",
    "6412.T": "平和",
    "6417.T": "SANKYO",
    "6418.T": "日本金銭機械",
    "6420.T": "フクシマガリレイ",
    "6432.T": "竹内製作所",
    "6440.T": "JUKI",
    "6448.T": "ブラザー工業",
    "6457.T": "グローリー",
    "6458.T": "新晃工業",
    "6460.T": "セガサミーホールディングス",
    "6463.T": "TPR",
    "6464.T": "ツバキ・ナカシマ",
    "6465.T": "ホシザキ",
    "6471.T": "日本精工",
    "6472.T": "NTN",
    "6473.T": "ジェイテクト",
    "6474.T": "不二越",
    "6479.T": "ミネベアミツミ",
    "6480.T": "日本トムソン",
    "6481.T": "THK",
    "6482.T": "ユーシン精機",
    "6486.T": "イーグル工業",
    "6489.T": "前澤工業",
    "6490.T": "日本ピラー工業",
    "6498.T": "キッツ",
    "7003.T": "三井E&S",
    "7004.T": "日立造船",
    "7011.T": "三菱重工業",
    "7012.T": "川崎重工業",
    "7013.T": "IHI",
    "7201.T": "日産自動車",
    "7202.T": "いすゞ自動車",
    "7203.T": "トヨタ自動車",
    "7205.T": "日野自動車",
    "7211.T": "三菱自動車",
    "7224.T": "新明和工業",
    "7231.T": "トピー工業",
    "7240.T": "NOK",
    "7241.T": "フタバ産業",
    "7242.T": "KYB",
    "7244.T": "市光工業",
    "7245.T": "大同メタル工業",
    "7246.T": "プレス工業",
    "7247.T": "ミクニ",
    "7250.T": "太平洋工業",
    "7259.T": "アイシン",
    "7261.T": "マツダ",
    "7267.T": "ホンダ",
    "7269.T": "スズキ",
    "7270.T": "SUBARU",
    "7272.T": "ヤマハ発動機",
    "7276.T": "小糸製作所",
    "7278.T": "エクセディ",
    "7282.T": "豊田合成",
    "7296.T": "エフ・シー・シー",
    "7309.T": "シマノ",
    "7313.T": "テイ・エス テック",

    # === 銀行・金融・保険 ===
    "7186.T": "コンコルディア・フィナンシャルグループ",
    "7189.T": "西日本フィナンシャルホールディングス",
    "7192.T": "日本モーゲージサービス",
    "8303.T": "新生銀行",
    "8304.T": "あおぞら銀行",
    "8306.T": "三菱UFJフィナンシャル・グループ",
    "8308.T": "りそなホールディングス",
    "8309.T": "三井住友トラスト・ホールディングス",
    "8316.T": "三井住友フィナンシャルグループ",
    "8331.T": "千葉銀行",
    "8334.T": "群馬銀行",
    "8336.T": "武蔵野銀行",
    "8337.T": "千葉興業銀行",
    "8338.T": "筑波銀行",
    "8341.T": "七十七銀行",
    "8343.T": "秋田銀行",
    "8344.T": "山形銀行",
    "8345.T": "岩手銀行",
    "8346.T": "東邦銀行",
    "8349.T": "東北銀行",
    "8350.T": "みちのく銀行",
    "8354.T": "ふくおかフィナンシャルグループ",
    "8355.T": "静岡銀行",
    "8356.T": "十六フィナンシャルグループ",
    "8358.T": "スルガ銀行",
    "8359.T": "八十二銀行",
    "8360.T": "山梨中央銀行",
    "8361.T": "大垣共立銀行",
    "8362.T": "福井銀行",
    "8363.T": "北國フィナンシャルホールディングス",
    "8364.T": "清水銀行",
    "8366.T": "滋賀銀行",
    "8367.T": "南都銀行",
    "8368.T": "百五銀行",
    "8369.T": "京都銀行",
    "8370.T": "紀陽銀行",
    "8377.T": "ほくほくフィナンシャルグループ",
    "8379.T": "広島銀行",
    "8381.T": "山陰合同銀行",
    "8382.T": "中国銀行",
    "8385.T": "伊予銀行",
    "8386.T": "百十四銀行",
    "8387.T": "四国銀行",
    "8388.T": "阿波銀行",
    "8393.T": "宮崎銀行",
    "8395.T": "佐賀銀行",
    "8397.T": "沖縄銀行",
    "8399.T": "琉球銀行",
    "8410.T": "セブン銀行",
    "8411.T": "みずほフィナンシャルグループ",
    "8418.T": "山口フィナンシャルグループ",
    "8424.T": "芙蓉総合リース",
    "8439.T": "東京センチュリー",
    "8473.T": "SBIホールディングス",
    "8508.T": "Jトラスト",
    "8511.T": "日本証券金融",
    "8515.T": "アイフル",
    "8519.T": "ポケットカード",
    "8521.T": "長野銀行",
    "8522.T": "名古屋銀行",
    "8524.T": "北洋銀行",
    "8527.T": "愛知銀行",
    "8541.T": "愛媛銀行",
    "8542.T": "トマト銀行",
    "8543.T": "みなと銀行",
    "8544.T": "京葉銀行",
    "8545.T": "関西みらいフィナンシャルグループ",
    "8550.T": "栃木銀行",
    "8551.T": "北日本銀行",
    "8558.T": "東和銀行",
    "8563.T": "大東銀行",
    "8566.T": "リコーリース",
    "8570.T": "イオンフィナンシャルサービス",
    "8572.T": "アコム",
    "8584.T": "ジャックス",
    "8585.T": "オリエントコーポレーション",
    "8591.T": "オリックス",
    "8593.T": "三菱HCキャピタル",
    "8595.T": "ジャフコ グループ",
    "8596.T": "九州リースサービス",
    "8601.T": "大和証券グループ本社",
    "8604.T": "野村ホールディングス",
    "8609.T": "岡三証券グループ",
    "8613.T": "丸三証券",
    "8614.T": "東洋証券",
    "8616.T": "東海東京フィナンシャル・ホールディングス",
    "8622.T": "水戸証券",
    "8624.T": "いちよし証券",
    "8628.T": "松井証券",
    "8630.T": "SOMPOホールディングス",
    "8697.T": "日本取引所グループ",
    "8698.T": "マネックスグループ",
    "8707.T": "岩井コスモホールディングス",
    "8708.T": "藍澤證券",
    "8713.T": "フィデアホールディングス",
    "8714.T": "池田泉州ホールディングス",
    "8725.T": "MS&ADインシュアランスグループホールディングス",
    "8750.T": "第一生命ホールディングス",
    "8766.T": "東京海上ホールディングス",
    "8795.T": "T&Dホールディングス",

    # === 小売・卸売 ===
    "8012.T": "長瀬産業",
    "8015.T": "豊田通商",
    "8020.T": "兼松",
    "8031.T": "三井物産",
    "8035.T": "東京エレクトロン",
    "8053.T": "住友商事",
    "8058.T": "三菱商事",
    "8059.T": "第一実業",
    "8060.T": "キヤノンマーケティングジャパン",
    "8068.T": "菱洋エレクトロ",
    "8074.T": "ユアサ商事",
    "8078.T": "阪和興業",
    "8079.T": "正栄食品工業",
    "8086.T": "ニプロ",
    "8088.T": "岩谷産業",
    "8091.T": "ニチモウ",
    "8096.T": "兼松エレクトロニクス",
    "8098.T": "稲畑産業",
    "8103.T": "明和産業",
    "8111.T": "ゴールドウイン",
    "8113.T": "ユニ・チャーム",
    "8117.T": "中央自動車工業",
    "8125.T": "ワキタ",
    "8129.T": "東邦ホールディングス",
    "8130.T": "サンゲツ",
    "8131.T": "ミツウロコグループホールディングス",
    "8132.T": "シナネンホールディングス",
    "8133.T": "伊藤忠エネクス",
    "8136.T": "サンリオ",
    "8137.T": "サンワテクノス",
    "8141.T": "新光商事",
    "8150.T": "三信電気",
    "8151.T": "東陽テクニカ",
    "8154.T": "加賀電子",
    "8157.T": "都築電気",
    "8158.T": "ソーダニッカ",
    "8159.T": "立花エレテック",
    "8160.T": "木曽路",
    "8165.T": "千趣会",
    "8167.T": "リテールパートナーズ",
    "8168.T": "ケーヨー",
    "8173.T": "上新電機",
    "8174.T": "日本瓦斯",
    "8179.T": "ロイヤルホールディングス",
    "8182.T": "いなげや",
    "8185.T": "チヨダ",
    "8194.T": "ライフコーポレーション",
    "8200.T": "リンガーハット",
    "8203.T": "MrMaxホールディングス",
    "8217.T": "オークワ",
    "8218.T": "コメリ",
    "8219.T": "青山商事",
    "8227.T": "しまむら",
    "8228.T": "マルイチ産商",
    "8233.T": "高島屋",
    "8237.T": "松屋",
    "8242.T": "エイチ・ツー・オー リテイリング",
    "8252.T": "丸井グループ",
    "8253.T": "クレディセゾン",
    "8255.T": "アクシアル リテイリング",
    "8267.T": "イオン",
    "8273.T": "イズミ",
    "8276.T": "平和堂",
    "8278.T": "フジ",
    "8279.T": "ヤオコー",
    "8282.T": "ケーズホールディングス",
    "8283.T": "PALTAC",
    "8285.T": "三谷産業",

    # === 不動産・REIT ===
    "8801.T": "三井不動産",
    "8802.T": "三菱地所",
    "8803.T": "平和不動産",
    "8804.T": "東京建物",
    "8806.T": "ダイビル",
    "8830.T": "住友不動産",
    "8841.T": "テーオーシー",
    "8850.T": "スターツコーポレーション",
    "8860.T": "フジ住宅",
    "8864.T": "空港施設",
    "8869.T": "明和地所",
    "8876.T": "リログループ",
    "8881.T": "日神グループホールディングス",
    "8905.T": "イオンモール",
    "8909.T": "シノケングループ",
    "8914.T": "エリアリンク",
    "8917.T": "ファースト住建",
    "8919.T": "カチタス",
    "8920.T": "東祥",
    "8923.T": "トーセイ",
    "8931.T": "和田興産",
    "8934.T": "サンフロンティア不動産",
    "8935.T": "FJネクストホールディングス",
    "8940.T": "インテリックス",

    # === 運輸・倉庫 ===
    "9001.T": "東武鉄道",
    "9003.T": "相鉄ホールディングス",
    "9005.T": "東急",
    "9006.T": "京浜急行電鉄",
    "9007.T": "小田急電鉄",
    "9008.T": "京王電鉄",
    "9009.T": "京成電鉄",
    "9010.T": "富士急行",
    "9020.T": "東日本旅客鉄道",
    "9021.T": "西日本旅客鉄道",
    "9022.T": "東海旅客鉄道",
    "9024.T": "西武ホールディングス",
    "9031.T": "西日本鉄道",
    "9033.T": "広島電鉄",
    "9041.T": "近鉄グループホールディングス",
    "9042.T": "阪急阪神ホールディングス",
    "9044.T": "南海電気鉄道",
    "9045.T": "京阪ホールディングス",
    "9048.T": "名古屋鉄道",
    "9052.T": "山陽電気鉄道",
    "9057.T": "遠州トラック",
    "9058.T": "トランコム",
    "9059.T": "カンダホールディングス",
    "9060.T": "日本ロジテム",
    "9062.T": "日本通運",
    "9064.T": "ヤマトホールディングス",
    "9065.T": "山九",
    "9068.T": "丸全昭和運輸",
    "9069.T": "センコーグループホールディングス",
    "9070.T": "トナミホールディングス",
    "9071.T": "日本石油輸送",
    "9072.T": "ニッコンホールディングス",
    "9076.T": "セイノーホールディングス",
    "9078.T": "エスライングループ本社",
    "9081.T": "神奈川中央交通",
    "9086.T": "日立物流",
    "9101.T": "日本郵船",
    "9104.T": "商船三井",
    "9107.T": "川崎汽船",
    "9110.T": "NSユナイテッド海運",
    "9115.T": "明治海運",
    "9119.T": "飯野海運",
    "9142.T": "九州旅客鉄道",
    "9143.T": "SGホールディングス",
    "9147.T": "NIPPON EXPRESSホールディングス",
    "9201.T": "日本航空",
    "9202.T": "ANAホールディングス",
    "9232.T": "パスコ",
    "9301.T": "三菱倉庫",
    "9302.T": "三井倉庫ホールディングス",
    "9303.T": "住友倉庫",
    "9304.T": "澁澤倉庫",
    "9324.T": "安田倉庫",

    # === 電気・ガス・エネルギー ===
    "9432.T": "日本電信電話",
    "9433.T": "KDDI",
    "9434.T": "ソフトバンク",
    "9435.T": "光通信",
    "9436.T": "沖縄セルラー電話",
    "9438.T": "エムティーアイ",
    "9449.T": "GMOインターネットグループ",
    "9466.T": "アイドママーケティングコミュニケーション",
    "9467.T": "アルファポリス",
    "9468.T": "KADOKAWA",
    "9470.T": "学研ホールディングス",
    "9474.T": "ゼンリン",
    "9501.T": "東京電力ホールディングス",
    "9502.T": "中部電力",
    "9503.T": "関西電力",
    "9504.T": "中国電力",
    "9505.T": "北陸電力",
    "9506.T": "東北電力",
    "9507.T": "四国電力",
    "9508.T": "九州電力",
    "9509.T": "北海道電力",
    "9510.T": "沖縄電力",
    "9513.T": "電源開発",
    "9517.T": "イーレックス",
    "9519.T": "レノバ",
    "9531.T": "東京ガス",
    "9532.T": "大阪ガス",
    "9533.T": "東邦ガス",
    "9534.T": "北海道ガス",
    "9535.T": "広島ガス",
    "9536.T": "西部ガスホールディングス",

    # === サービス・その他 ===
    "9603.T": "エイチ・アイ・エス",
    "9605.T": "東映",
    "9607.T": "AOI TYO Holdings",
    "9613.T": "NTTデータグループ",
    "9616.T": "共立メンテナンス",
    "9619.T": "イチネンホールディングス",
    "9621.T": "建設技術研究所",
    "9622.T": "スペース",
    "9627.T": "アインホールディングス",
    "9629.T": "ピー・シー・エー",
    "9632.T": "スバル興業",
    "9640.T": "セゾンテクノロジー",
    "9644.T": "タナベコンサルティンググループ",
    "9651.T": "日本プロセス",
    "9658.T": "ビジネスブレイン太田昭和",
    "9672.T": "東京都競馬",
    "9678.T": "カナモト",
    "9682.T": "DTS",
    "9684.T": "スクウェア・エニックス・ホールディングス",
    "9687.T": "KSK",
    "9692.T": "シーイーシー",
    "9697.T": "カプコン",
    "9699.T": "西尾レントオール",
    "9702.T": "アイ・エス・ビー",
    "9706.T": "日本空港ビルデング",
    "9715.T": "トランスコスモス",
    "9717.T": "ジャステック",
    "9719.T": "SCSK",
    "9726.T": "KNT-CTホールディングス",
    "9729.T": "トーカイ",
    "9731.T": "白洋舎",
    "9735.T": "セコム",
    "9739.T": "NSW",
    "9740.T": "セントラル警備保障",
    "9742.T": "アイネス",
    "9743.T": "丹青社",
    "9744.T": "メイテックグループホールディングス",
    "9746.T": "TKC",
    "9749.T": "富士ソフト",
    "9755.T": "応用地質",
    "9757.T": "船井総研ホールディングス",
    "9759.T": "NSD",
    "9765.T": "オオバ",
    "9766.T": "コナミグループ",
    "9783.T": "ベネッセホールディングス",
    "9787.T": "イオンディライト",
    "9793.T": "ダイセキ",
    "9795.T": "ステップ",
    "9824.T": "泉州電業",
    "9831.T": "ヤマダホールディングス",
    "9832.T": "オートバックスセブン",
    "9837.T": "モリト",
    "9842.T": "アークランズ",
    "9843.T": "ニトリホールディングス",
    "9850.T": "グルメ杵屋",
    "9856.T": "ケーユーホールディングス",
    "9861.T": "吉野家ホールディングス",
    "9869.T": "加藤産業",
    "9873.T": "日本KFCホールディングス",
    "9880.T": "イノテック",
    "9882.T": "イエローハット",
    "9889.T": "JBCCホールディングス",
    "9902.T": "日伝",
    "9906.T": "藤井産業",
    "9908.T": "日本電計",
    "9913.T": "日邦産業",
    "9919.T": "関西フードマーケット",
    "9928.T": "ミロク情報サービス",
    "9932.T": "杉本商事",
    "9934.T": "因幡電機産業",
    "9936.T": "王将フードサービス",
    "9945.T": "プレナス",
    "9948.T": "アークス",
    "9956.T": "バローホールディングス",
    "9974.T": "ベルク",
    "9983.T": "ファーストリテイリング",
    "9984.T": "ソフトバンクグループ",
    "9987.T": "スズケン",
    "9989.T": "サンドラッグ",
    "9997.T": "ベルーナ",
}

MIDCAP_TICKERS = list(TICKER_NAMES.keys())

# 診断用ローカルキャッシュを分割するシャード数（全銘柄時も1ファイルあたり数十〜百銘柄程度）
HISTORY_SHARD_COUNT = 64
HISTORY_DIR = Path("data/history")


def hash_ticker_shard_id(ticker: str) -> int:
    return int(hashlib.md5(ticker.encode("utf-8")).hexdigest(), 16) % HISTORY_SHARD_COUNT


def get_all_listed_tickers_jpx() -> list[str]:
    """JPX上場一覧（プライム・スタンダード・グロース）から Yahoo 形式ティッカー一覧を返す"""
    d = get_jpx_data()
    if not d:
        return []
    out: list[str] = []
    for code in d.keys():
        code = str(code).strip()
        if not code:
            continue
        out.append(f"{code}.T")
    return sorted(set(out))


def build_universe_tickers() -> list[str]:
    """
    FULL_UNIVERSE=1（デフォルト）: JPX 全銘柄 ∪ 固定辞書の和集合。
    FULL_UNIVERSE=0: 従来どおり MIDCAP_TICKERS のみ（ローカル短時間テスト用）。
    """
    if os.environ.get("FULL_UNIVERSE", "0").strip() not in ("1", "true", "True"):
        return list(MIDCAP_TICKERS)
    jpx = get_all_listed_tickers_jpx()
    if not jpx:
        print("⚠️ JPX一覧の取得に失敗。TICKER_NAMES のみで続行します。")
    merged = sorted(set(jpx) | set(MIDCAP_TICKERS))
    return merged


def write_history_shards(shards: list[dict], updated_at: str) -> None:
    """data/history/shard_XX.json と meta.json を書き出す"""
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    total = 0
    for i, bucket in enumerate(shards):
        total += len(bucket)
        (HISTORY_DIR / f"shard_{i:02d}.json").write_text(
            json.dumps(bucket, ensure_ascii=False), encoding="utf-8"
        )
    meta = {
        "updated_at": updated_at,
        "shard_count": HISTORY_SHARD_COUNT,
        "format": "sharded_v1",
        "ticker_count": total,
    }
    (HISTORY_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"💾 保存: {HISTORY_DIR}/shard_00..shard_{HISTORY_SHARD_COUNT - 1:02d}.json （計 {total} 銘柄）")


def get_jpx_data():
    try:
        html_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = requests.get(html_url, headers=headers, timeout=10)
        response.raise_for_status()
        match = re.search(r'href="([^"]+data_j\.xls)"', response.text)
        if not match:
            return {}

        file_url = "https://www.jpx.co.jp" + match.group(1)
        xls_response = requests.get(file_url, headers=headers, timeout=10)
        xls_response.raise_for_status()

        def _jpx_code_cell(v):
            """Excel が数値化した銘柄（7203.0）と英字銘柄（151A）の両方を文字列で保持"""
            if pd.isna(v):
                return ""
            s = str(v).strip()
            if re.match(r"^\d+\.0$", s):
                return s[:-2]
            return s

        df = pd.read_excel(io.BytesIO(xls_response.content))
        df.iloc[:, 1] = df.iloc[:, 1].map(_jpx_code_cell)
        df_tickers = df[df.iloc[:, 3].isin(["プライム", "スタンダード", "グロース"])]
        codes = df_tickers.iloc[:, 1].astype(str).str.strip()
        codes = codes[codes != ""]
        return dict(zip(codes, df_tickers.iloc[:, 2]))
    except Exception:
        return {}

JPX_NAME_MAP = get_jpx_data()


def fetch_yahoo_japan_name(ticker: str) -> str | None:
    code_only = str(ticker or "").replace(".T", "").strip()
    if not code_only:
        return None

    try:
        url_yfjp = f"https://finance.yahoo.co.jp/quote/{code_only}.T"
        res = requests.get(url_yfjp, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        res.raise_for_status()
        match = re.search(r"<title>(.+?)(?:\(株\))?【", res.text)
        if match:
            return match.group(1).strip()
    except Exception:
        return None

    return None


def get_japanese_name(ticker: str, api_name: str | None = None) -> str:
    code_only = str(ticker or "").replace(".T", "").strip()

    candidates = [
        JPX_NAME_MAP.get(code_only),
        TICKER_NAMES.get(ticker),
        fetch_yahoo_japan_name(ticker),
        api_name,
    ]

    for cand in candidates:
        cand = (cand or "").strip()
        if not cand:
            continue
        if re.search(r"[ぁ-んァ-ヶ一-龠々ー]", cand):
            return cand
        if cand in {"SHIFT", "TOWA", "ZOZO", "HENNGE", "GENDA", "MonotaRO", "Appier", "BASE", "JTOWER", "Sansan", "Macbee Planet", "KLab", "LTS", "PR TIMES", "JIG-SAW"}:
            return cand

    for cand in candidates:
        cand = (cand or "").strip()
        if cand:
            return cand

    return code_only


def calculate_flow_score(df: pd.DataFrame) -> dict:
    """FlowScore（需給変化の強さ）を計算。"""
    if df.empty or len(df) < 20:
        return {
            "flow_score": 0.0,
            "vol_anomaly": 0.0,
            "price_stability": 0.0,
            "absorption": 0.0,
            "range_compression": 0.0,
            "lower_shadow": 0.0,
        }

    recent_5 = df.tail(5)
    recent_60 = df.tail(60) if len(df) >= 60 else df

    try:
        # 1) 出来高異常
        avg_vol_60 = float(recent_60["Volume"].mean())
        avg_vol_5 = float(recent_5["Volume"].mean())
        vol_anomaly = min(100.0, (avg_vol_5 / avg_vol_60 - 1) * 50.0) if avg_vol_60 > 0 else 0.0
        vol_anomaly = max(0.0, vol_anomaly)

        # 2) 価格安定度（5日）
        price_change_5 = abs(float(recent_5["Close"].iloc[-1]) / float(recent_5["Close"].iloc[0]) - 1.0) * 100.0
        price_stability = max(0.0, 100.0 - price_change_5 * 20.0)

        # 3) 吸収度（出来高増加率 ÷ 価格変動）
        vol_ratio = (avg_vol_5 / avg_vol_60) if avg_vol_60 > 0 else 1.0
        price_ratio = price_change_5 + 0.1
        absorption = min(100.0, (vol_ratio / price_ratio) * 30.0)

        # 4) 値幅縮小（ATR近似）
        df_copy = df.copy()
        df_copy["TR"] = np.maximum(
            df_copy["High"] - df_copy["Low"],
            np.maximum(
                (df_copy["High"] - df_copy["Close"].shift(1)).abs(),
                (df_copy["Low"] - df_copy["Close"].shift(1)).abs(),
            ),
        )
        atr_20 = float(df_copy["TR"].tail(20).mean())
        atr_5 = float(df_copy["TR"].tail(5).mean())
        range_compression = max(0.0, min(100.0, (1.0 - atr_5 / atr_20) * 100.0)) if atr_20 > 0 else 50.0

        # 5) 下ヒゲ
        body_range = (recent_5["High"] - recent_5["Low"]).replace(0, np.nan)
        lower_shadow_ratio = ((recent_5["Close"] - recent_5["Low"]) / body_range).fillna(0.5)
        lower_shadow = float(lower_shadow_ratio.mean()) * 100.0

        flow_score = (
            vol_anomaly * 0.30 +
            price_stability * 0.25 +
            absorption * 0.25 +
            range_compression * 0.10 +
            lower_shadow * 0.10
        )
        flow_score = float(min(100.0, max(0.0, flow_score)))

        return {
            "flow_score": round(flow_score, 1),
            "vol_anomaly": round(float(vol_anomaly), 1),
            "price_stability": round(float(price_stability), 1),
            "absorption": round(float(absorption), 1),
            "range_compression": round(float(range_compression), 1),
            "lower_shadow": round(float(lower_shadow), 1),
        }
    except Exception as e:
        print(f"    FlowScore計算エラー: {e}")
        return {
            "flow_score": 0.0,
            "vol_anomaly": 0.0,
            "price_stability": 0.0,
            "absorption": 0.0,
            "range_compression": 0.0,
            "lower_shadow": 0.0,
        }


def load_previous_streaks() -> dict:
    """前回のratios.jsonから、FlowScore70+の連続日数を復元。"""
    try:
        p = Path("data/ratios.json")
        if not p.exists():
            return {}
        prev = json.loads(p.read_text(encoding="utf-8"))
        prev_data = prev.get("data", {}) or {}
        return {t: int(d.get("flow_streak_high", 0)) for t, d in prev_data.items()}
    except Exception:
        return {}


def is_watch_state(flow_details: dict) -> bool:
    """表示ラベル『要監視』判定（取引増×値動き小）。"""
    return (flow_details.get("vol_anomaly", 0) > 50 and flow_details.get("price_stability", 0) > 60)


def calculate_reorg_score(market_cap_oku: float | None, pbr: float | None) -> float:
    """再編素地（0-100）。時価総額帯＋PBRで簡易評価。"""
    score = 50.0
    if market_cap_oku and market_cap_oku > 0:
        center = (MARKET_CAP_MIN + MARKET_CAP_MAX) / 2
        span = (MARKET_CAP_MAX - MARKET_CAP_MIN) / 2
        dist = abs(market_cap_oku - center) / span
        size_component = max(0.0, 1.0 - min(1.0, dist)) * 60.0
        score = 20.0 + size_component

    if pbr is not None and pbr > 0:
        if pbr <= 1.0:
            score += 20.0
        elif pbr <= 2.0:
            score += 10.0
        elif pbr >= 5.0:
            score -= 5.0

    return float(min(100.0, max(0.0, score)))


def determine_level(ma_score: float) -> int:
    if ma_score >= 75:
        return 4
    if ma_score >= 60:
        return 3
    if ma_score >= 45:
        return 2
    if ma_score >= 30:
        return 1
    return 0


def fetch_volume_data(
    tickers: list[str],
    ohlc_cache: dict | None = None,
    kabuplus_info: dict | None = None,
) -> tuple[dict, dict, dict, list]:
    """
    KABU+ 四本値キャッシュ（ohlc_cache）を使って全銘柄を処理する。
    yf.download() は一切使用しない。

    ohlc_cache: {code: DataFrame(index=DatetimeIndex, cols=[Open,High,Low,Close,Volume])}
                code は "7203" 形式（.T なし）
    """
    results: dict = {}
    qualified: dict = {}
    stock_history: dict = {}
    shards: list[dict] = [{} for _ in range(HISTORY_SHARD_COUNT)]
    prev_streaks = load_previous_streaks()
    total = len(tickers)
    if ohlc_cache is None:
        ohlc_cache = {}
    if kabuplus_info is None:
        kabuplus_info = {}

    hit = 0
    miss = 0
    miss_no_ohlc   = 0   # ohlc_cache に存在しない
    miss_short     = 0   # データが60日未満
    skip_no_range  = 0   # 時価総額範囲外（market_cap_oku=0 含む）
    LOG_INTERVAL = 200

    for idx, ticker in enumerate(tickers):
        if idx % LOG_INTERVAL == 0:
            print(f"📊 処理中: {idx + 1} / {total}  (取得済み={hit}, スキップ={miss})")

        try:
            # ── OHLC取得 ─────────────────────────────────────────
            code = ticker.replace(".T", "").strip()
            df = ohlc_cache.get(code)

            if df is None or df.empty:
                miss += 1
                miss_no_ohlc += 1
                continue

            df = df.dropna(subset=["Close"])
            if len(df) < 60:
                miss += 1
                miss_short += 1
                continue

            hit += 1

            # ── FlowScore ───────────────────────────────────────
            flow_details = calculate_flow_score(df)
            flow_score = float(flow_details["flow_score"])

            avg_volume = int(df["Volume"].tail(LOOKBACK_DAYS).mean())
            latest_volume = int(df["Volume"].iloc[-1])
            vol_ratio = round(latest_volume / avg_volume, 2) if avg_volume > 0 else 0
            latest_price = float(df["Close"].iloc[-1])
            price_change_5d = (
                round((df["Close"].iloc[-1] / df["Close"].iloc[-6] - 1) * 100, 2)
                if len(df) >= 6 else 0
            )

            # ── KABU+ info（時価総額・PBR・株数） ──────────────
            market_cap_oku = 0.0
            api_name = None
            pbr = None
            shares_outstanding = None
            shares_outstanding_is_estimated = False
            info: dict = {}

            kp_info = kabuplus_info.get(ticker, {})
            if kp_info:
                info = kp_info
                mc = kp_info.get("marketCap", 0) or 0
                if mc:
                    market_cap_oku = round(float(mc) / 1e8, 0)
                api_name = kp_info.get("shortName") or kp_info.get("longName")
                pbr = kp_info.get("priceToBook")
                so = kp_info.get("sharesOutstanding")
                if so is not None and so > 0:
                    shares_outstanding = int(so)
                else:
                    if mc and latest_price:
                        try:
                            shares_outstanding = int(float(mc) / float(latest_price))
                            shares_outstanding_is_estimated = True
                        except Exception:
                            shares_outstanding = None
            # KABU+ にない場合はスキップ（yfinance 依存撤廃）

            # ── 株式履歴保存 ────────────────────────────────────
            stock_history[ticker] = {
                "dates": [d.strftime("%Y-%m-%d") for d in df.index],
                "O": [round(float(v), 1) for v in df["Open"]],
                "H": [round(float(v), 1) for v in df["High"]],
                "L": [round(float(v), 1) for v in df["Low"]],
                "C": [round(float(v), 1) for v in df["Close"]],
                "V": [int(v) for v in df["Volume"]],
                "info": {
                    "marketCap": info.get("marketCap"),
                    "sharesOutstanding": info.get("sharesOutstanding"),
                    "dividendRate": info.get("dividendRate"),
                    "trailingAnnualDividendRate": info.get("trailingAnnualDividendRate"),
                    "payoutRatio": info.get("payoutRatio"),
                    "dividendYield": info.get("dividendYield"),
                    "trailingAnnualDividendYield": info.get("trailingAnnualDividendYield"),
                    "shortName": info.get("shortName"),
                    "longName": info.get("longName"),
                },
            }
            shards[hash_ticker_shard_id(ticker)][ticker] = stock_history[ticker]

            name = get_japanese_name(ticker, api_name)
            in_range = MARKET_CAP_MIN <= market_cap_oku <= MARKET_CAP_MAX

            # ── 要監視・連続日数 ────────────────────────────────
            watch_flag = is_watch_state(flow_details)
            display_state = "要監視" if watch_flag else "観測中"
            prev_high = int(prev_streaks.get(ticker, 0))
            flow_streak_high = prev_high + 1 if flow_score >= FLOW_SCORE_HIGH else 0

            # ── スコア計算 ──────────────────────────────────────
            reorg_score = calculate_reorg_score(market_cap_oku, pbr)
            event_score: float = 0.0
            event_tags: list[str] = []

            ma_score = float(
                min(100.0, max(0.0,
                    flow_score * 0.45 + reorg_score * 0.40 + event_score * 0.15
                ))
            )
            level = determine_level(ma_score)

            # ── サポートライン ──────────────────────────────────
            support_price = None
            support_upper = None
            support_gap_pct = None
            support_tag = None
            try:
                df_half_year = df.tail(125)
                if len(df_half_year) >= 30:
                    vp = calculate_volume_profile(df_half_year, bins=24)
                    sup_p, sup_u = compute_support_zone_from_profile(vp)
                    if sup_p is not None:
                        support_price = sup_p
                        support_upper = sup_u
                        support_tag, support_gap_pct = support_position_tag(
                            latest_price, support_price
                        )
            except Exception:
                pass

            # ── タグ ────────────────────────────────────────────
            tags: list[str] = []
            if support_tag:
                tags.append(support_tag)
            if watch_flag:
                tags.append("要監視")
            if flow_details.get("vol_anomaly", 0) >= 50:
                tags.append("出来高変化")
            if flow_streak_high >= 2:
                tags.append(f"継続{flow_streak_high}日")
            tags.extend(event_tags)

            # ── 発行株数に対する出来高比率 ───────────────────────
            volume_of_shares_pct = None
            volume_of_shares_pct_is_estimated = False
            if shares_outstanding and shares_outstanding > 0:
                try:
                    volume_of_shares_pct = (float(latest_volume) / float(shares_outstanding)) * 100.0
                    volume_of_shares_pct_is_estimated = bool(shares_outstanding_is_estimated)
                except Exception:
                    volume_of_shares_pct = None

            result = {
                "name": name,
                "price": round(latest_price, 1),
                "volume": latest_volume,
                "avg_volume": avg_volume,
                "vol_ratio": vol_ratio,
                "shares_outstanding": int(shares_outstanding) if shares_outstanding else None,
                "shares_outstanding_is_estimated": bool(shares_outstanding_is_estimated) if shares_outstanding else None,
                "volume_of_shares_pct": round(float(volume_of_shares_pct), 3) if volume_of_shares_pct is not None else None,
                "volume_of_shares_pct_is_estimated": bool(volume_of_shares_pct_is_estimated) if volume_of_shares_pct is not None else None,
                "price_change_5d": price_change_5d,
                "market_cap_oku": int(market_cap_oku) if market_cap_oku else 0,
                "pbr": round(float(pbr), 2) if pbr else None,
                "in_cap_range": in_range,
                "level": int(level),
                "ma_score": round(ma_score, 1),
                "flow_score": round(flow_score, 1),
                "flow_details": flow_details,
                "flow_streak_high": int(flow_streak_high),
                "reorg_score": round(reorg_score, 1),
                "event_score": round(event_score, 1),
                "display_state": display_state,
                "support_price": round(float(support_price), 1) if support_price else None,
                "support_upper": round(float(support_upper), 1) if support_upper else None,
                "support_gap_pct": round(float(support_gap_pct), 1) if support_gap_pct is not None else None,
                "tags": tags,
            }

            results[ticker] = result
            if not in_range:
                skip_no_range += 1
            if in_range and flow_score >= FLOW_SCORE_MEDIUM:
                qualified[ticker] = result

        except Exception as e:
            print(f"  ❌ {ticker}: {str(e)[:80]}")
            continue

    print(f"✅ 全銘柄処理完了: 成功={hit}, スキップ={miss} / 合計={total}")
    print(f"   └ OHLCなし={miss_no_ohlc}, データ不足(<60日)={miss_short}")
    print(f"   └ 成功{hit}件中: 時価総額範囲外={skip_no_range}, 範囲内={hit - skip_no_range}")
    print(f"   └ 候補(範囲内+Flow>={FLOW_SCORE_MEDIUM})={len(qualified)}")
    return results, qualified, stock_history, shards


def main():
    global JPX_NAME_MAP

    now_jst = datetime.now(JST)
    updated_at = now_jst.strftime("%Y-%m-%d %H:%M:%S")

    JPX_NAME_MAP = get_jpx_data()
    universe = build_universe_tickers()

    print("=" * 60)
    print("🦅 HAGETAKA SCOPE - 日次候補抽出")
    print("=" * 60)
    print(f"⏰ 実行時刻: {updated_at} JST")
    print(f"🎯 対象: 時価総額 {MARKET_CAP_MIN}億〜{MARKET_CAP_MAX}億円（候補フィルタ）")
    print(f"📋 スキャン銘柄数: {len(universe)} （FULL_UNIVERSE={os.environ.get('FULL_UNIVERSE', '0')}）")

    # ★ KABU+ 指標データ（時価総額・PBR等）
    kabuplus_info = {}
    try:
        import kabuplus_client as kp
        kp_id, kp_pw = kp.get_credentials()
        if kp_id and kp_pw:
            print("📡 KABU+ からデータ一括取得中...")
            merged = kp.fetch_merged_data(kp_id, kp_pw)
            if not merged.empty:
                kabuplus_info = kp.build_info_lookup(merged)
                print(f"  → KABU+ {len(kabuplus_info)}銘柄の指標データ取得完了")
            else:
                print("  ⚠️ KABU+ 指標データ取得失敗（休日の可能性）")
        else:
            print("  ⚠️ KABU+ 認証情報なし")
    except Exception as e:
        print(f"  ⚠️ KABU+ 指標エラー: {e}")

    # ★ 信用残高データ（週次）
    margin_lookup = {}
    try:
        import kabuplus_client as kp
        kp_id, kp_pw = kp.get_credentials()
        if kp_id and kp_pw:
            print("📡 KABU+ 信用残高データ取得中...")
            margin_df = kp.fetch_margin_data(kp_id, kp_pw)
            if not margin_df.empty:
                margin_lookup = kp.build_margin_lookup(margin_df)
                print(f"  → 信用残高 {len(margin_lookup)}銘柄取得完了")
            else:
                print("  ⚠️ 信用残高データ取得失敗")
    except Exception as e:
        print(f"  ⚠️ 信用残高エラー: {e}")

    # ★ 四本値OHLC履歴（yfinance 完全置換）
    ohlc_cache: dict = {}
    try:
        import kabuplus_client as kp
        kp_id, kp_pw = kp.get_credentials()
        if kp_id and kp_pw:
            print("📡 KABU+ 四本値履歴データ取得中（最大250営業日）...")
            ohlc_cache = kp.fetch_ohlc_history(kp_id, kp_pw, lookback_days=250)
            if not ohlc_cache:
                print("  ⚠️ OHLC履歴データ取得失敗（全銘柄スキップの可能性）")
        else:
            print("  ⚠️ KABU+ 認証情報なし → OHLCデータなし")
    except Exception as e:
        print(f"  ⚠️ OHLC取得エラー: {e}")

    # ══ 診断ログ ══════════════════════════════════════════
    print("\n" + "─" * 60)
    print("🔍 診断情報")
    print(f"  ohlc_cache   : {len(ohlc_cache)} 銘柄")
    print(f"  kabuplus_info: {len(kabuplus_info)} 銘柄")
    if ohlc_cache:
        sample_code = next(iter(ohlc_cache))
        sample_df   = ohlc_cache[sample_code]
        print(f"  OHLCサンプル : {sample_code} → {len(sample_df)} 日分 "
              f"({sample_df.index[0].date()} 〜 {sample_df.index[-1].date()})")
    if kabuplus_info:
        sample_tk = next(iter(kabuplus_info))
        sample_info = kabuplus_info[sample_tk]
        mcap = sample_info.get("marketCap", 0) or 0
        print(f"  KPサンプル   : {sample_tk} 時価総額={mcap/1e8:.0f}億円 "
              f"PBR={sample_info.get('priceToBook')}")
    # universe と ohlc_cache のコード形式確認
    if universe:
        u0 = universe[0]
        code0 = u0.replace(".T", "").strip()
        in_cache = code0 in ohlc_cache
        print(f"  universe[0]  : {u0} → code={code0} → cache内={'✅' if in_cache else '❌'}")
    print("─" * 60 + "\n")
    # ══════════════════════════════════════════════════════

    results, qualified, stock_history, shards = fetch_volume_data(
        universe,
        ohlc_cache=ohlc_cache,
        kabuplus_info=kabuplus_info,
    )

    # ★ 信用残高データを各銘柄の結果に統合
    for ticker, result in results.items():
        mg = margin_lookup.get(ticker, {})
        result["margin_buy"] = mg.get("margin_buy", 0)
        result["margin_sell"] = mg.get("margin_sell", 0)
        result["margin_buy_change"] = mg.get("margin_buy_change", 0)
        result["margin_sell_change"] = mg.get("margin_sell_change", 0)
        result["margin_ratio"] = mg.get("margin_ratio")

    filtered = {k: v for k, v in results.items() if v.get("in_cap_range")}
    # 並び：LEVEL→MAScore→FlowScore
    sorted_qualified = dict(sorted(qualified.items(), key=lambda x: (int(x[1].get("level",0)), float(x[1].get("ma_score",0)), float(x[1].get("flow_score",0))), reverse=True))
    sorted_filtered = dict(sorted(filtered.items(), key=lambda x: (int(x[1].get("level",0)), float(x[1].get("ma_score",0)), float(x[1].get("flow_score",0))), reverse=True))

    level_counts = {}
    for r in sorted_qualified.values():
        lv = int(r.get("level", 0))
        level_counts[lv] = level_counts.get(lv, 0) + 1

    output = {
        "updated_at": updated_at,
        "date": now_jst.strftime("%Y-%m-%d"),
        "market_cap_range": f"{MARKET_CAP_MIN}億〜{MARKET_CAP_MAX}億円",
        "total_count": len(sorted_qualified),
        "all_count": len(results),
        "filtered_count": len(filtered),
        "level_counts": level_counts,
        "data": sorted_qualified,
        "all_data": sorted_filtered,
        "disclaimer": "本ツールは市場データの可視化を目的とした補助ツールです。銘柄推奨・売買助言ではありません。",
    }

    os.makedirs("data", exist_ok=True)
    Path("data/ratios.json").write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print("💾 保存完了: data/ratios.json")
    print(f"🎯 候補: {len(sorted_qualified)} 件 / フィルタ通過: {len(filtered)} 件")

    write_history_shards(shards, updated_at)

    if os.environ.get("WRITE_LEGACY_STOCK_HISTORY", "0").strip() in ("1", "true", "True"):
        history_output = {"updated_at": updated_at, **stock_history}
        Path("data/stock_history.json").write_text(json.dumps(history_output, ensure_ascii=False), encoding="utf-8")
        print(f"💾 レガシー保存: data/stock_history.json ({len(stock_history)} 銘柄)")


if __name__ == "__main__":
    main()
