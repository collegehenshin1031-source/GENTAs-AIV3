"""
自動監視スクリプト（GitHub Actions で実行）
──────────────────────────────────────────
1. KABU+ から全銘柄データを一括取得
2. ウォッチリスト銘柄のニュースをスクレイピング
3. 出来高履歴を蓄積
4. 結果を data/ に JSON 保存 → git push
5. アラート条件を満たしたらメール送信
"""

from __future__ import annotations
import os
import sys
import json
import time
from datetime import datetime

# Streamlit なしでインポートできるよう調整
os.environ.setdefault("STREAMLIT_SERVER_HEADLESS", "true")


def main():
    print(f"=== 自動監視開始: {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")

    # ── 環境変数から認証情報を取得 ──
    kabuplus_id = os.environ.get("KABUPLUS_ID", "")
    kabuplus_pw = os.environ.get("KABUPLUS_PASSWORD", "")

    if not kabuplus_id or not kabuplus_pw:
        print("ERROR: KABUPLUS_ID / KABUPLUS_PASSWORD が未設定")
        sys.exit(1)

    # ── KABU+ データ取得 ──
    print("[1/5] KABU+ データ取得中...")
    from kabuplus_client import (
        fetch_stock_prices_nocache,
        fetch_stock_indicators_nocache,
    )

    prices_df = fetch_stock_prices_nocache(kabuplus_id, kabuplus_pw)
    if prices_df.empty:
        print("ERROR: 株価データの取得に失敗（休日の可能性あり）")
        sys.exit(0)  # 休日は正常終了

    indicators_df = fetch_stock_indicators_nocache(kabuplus_id, kabuplus_pw)

    # マージ
    if not indicators_df.empty:
        ind_cols = [c for c in indicators_df.columns
                    if c not in ("name", "market", "industry") or c == "code"]
        merged_df = prices_df.merge(
            indicators_df[ind_cols], on="code", how="left", suffixes=("", "_ind")
        )
    else:
        merged_df = prices_df

    print(f"  → {len(merged_df)}銘柄のデータを取得")

    # ── 出来高履歴を更新 ──
    print("[2/5] 出来高履歴を更新中...")
    from hagetaka_scanner import (
        load_volume_history, save_volume_history, update_volume_history,
        scan_dataframe, filter_scan_targets, ScanMode,
    )

    vol_history = load_volume_history()
    vol_history = update_volume_history(prices_df, vol_history)
    save_volume_history(vol_history)
    print(f"  → {len(vol_history)}銘柄の出来高履歴を保存")

    # ── ハゲタカスキャン ──
    print("[3/5] ハゲタカスキャン実行中...")
    from kabuplus_client import filter_active_stocks
    active_df = filter_active_stocks(prices_df)
    signals = scan_dataframe(active_df, vol_history)

    lockons = [s for s in signals if s.total_score >= 60]
    print(f"  → 分析完了: {len(signals)}銘柄, ロックオン候補: {len(lockons)}件")

    # ── ウォッチリスト銘柄のニューススクレイピング ──
    print("[4/5] ニューススクレイピング中...")
    from notifier import load_watchlist
    from ma_detector import (
        scrape_news_for_stock, save_news_cache, load_news_cache,
    )

    watchlist = load_watchlist()

    # ロックオン銘柄もニュース対象に追加
    lockon_codes = [s.code for s in lockons[:10]]
    news_targets = list(set(watchlist + lockon_codes))

    news_cache = load_news_cache()
    scraped_count = 0

    for code in news_targets:
        # コードから銘柄名を取得
        match = merged_df[merged_df["code"] == code]
        if match.empty:
            continue
        name = str(match.iloc[0].get("name", code))

        try:
            items = scrape_news_for_stock(name, code)
            if items:
                news_cache[code] = items
                scraped_count += 1
                print(f"  {code} ({name}): {len(items)}件のニュース取得")
        except Exception as e:
            print(f"  {code}: スクレイピングエラー - {e}")

        time.sleep(1.5)  # レート制限

    save_news_cache(news_cache)
    print(f"  → {scraped_count}銘柄のニュースを更新")

    # ── 通知チェック ──
    print("[5/5] 通知チェック中...")
    from notifier import (
        load_notification_config, load_score_history,
        save_score_history, update_score_history,
        send_hagetaka_alert, send_ma_alert,
    )
    from ma_detector import analyze_ma_from_dataframe

    config = load_notification_config()

    # 環境変数から通知設定を上書き（GitHub Secrets 経由）
    if os.environ.get("EMAIL_ENABLED", "").lower() == "true":
        config.enabled = True
        config.email_enabled = True
        config.email_address = os.environ.get("EMAIL_ADDRESS", config.email_address)
        config.smtp_server = os.environ.get("SMTP_SERVER", config.smtp_server)
        config.smtp_port = int(os.environ.get("SMTP_PORT", str(config.smtp_port)))
        config.smtp_user = os.environ.get("SMTP_USER", config.smtp_user)
        config.smtp_password = os.environ.get("SMTP_PASSWORD", config.smtp_password)
        config.min_score_threshold = int(
            os.environ.get("MIN_SCORE_THRESHOLD", str(config.min_score_threshold))
        )

    # スコア履歴
    score_history = load_score_history()
    increase_threshold = int(os.environ.get("INCREASE_THRESHOLD", "15"))

    # ハゲタカアラート
    if config.enabled and lockons:
        alert_signals = [s for s in lockons if s.total_score >= config.min_score_threshold]
        if alert_signals:
            send_hagetaka_alert(config, alert_signals)
            print(f"  → ハゲタカアラート送信: {len(alert_signals)}件")

    # M&A アラート
    if config.enabled and watchlist:
        ma_results = analyze_ma_from_dataframe(merged_df, watchlist, news_cache)
        alert_ma = []

        for s in ma_results:
            if s.total_score < config.min_score_threshold:
                continue

            # スコア急上昇チェック
            prev_scores = score_history.get(s.code, [])
            if prev_scores:
                last = prev_scores[-1].get("score", 0)
                if s.total_score - last >= increase_threshold:
                    alert_ma.append(s)
                elif s.total_score >= 70:  # 緊急レベルは常に通知
                    alert_ma.append(s)
            else:
                # 初回は閾値超えなら通知
                alert_ma.append(s)

            score_history = update_score_history(
                score_history, s.code, s.total_score, s.signal_level.value
            )

        if alert_ma:
            send_ma_alert(config, alert_ma)
            print(f"  → M&Aアラート送信: {len(alert_ma)}件")

    save_score_history(score_history)

    print(f"\n=== 自動監視完了: {datetime.now().strftime('%Y-%m-%d %H:%M')} ===")


if __name__ == "__main__":
    main()
