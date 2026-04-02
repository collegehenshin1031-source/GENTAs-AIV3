# 源太AI🤖ハゲタカSCOPE v3（KABU+ 対応版）

## 🚀 v3 の改善点

| 項目 | v2（旧版） | v3（新版） |
|------|-----------|-----------|
| データ取得 | yfinance（1銘柄ずつ API） | **KABU+ 一括CSV**（全銘柄を1リクエスト） |
| 全銘柄スキャン | 20〜30分（頻繁にタイムアウト） | **3〜5秒** |
| ニュース取得 | アプリ側でスクレイピング | **GitHub Actions でバックグラウンド取得** |
| 投資指標 | yfinance（不安定） | **KABU+ 投資指標CSV**（PBR/PER等） |
| 銘柄リスト | JPX XLS（不安定） | **KABU+ CSV に含まれる** |
| 認証情報 | コード内ハードコード | **Streamlit Secrets** |

---

## 📁 ファイル構成

```
├── app.py                    # Streamlit メインアプリ
├── kabuplus_client.py        # KABU+ データ取得クライアント（新規）
├── hagetaka_scanner.py       # ハゲタカ検知エンジン v3
├── ma_detector.py            # M&A 予兆検知エンジン v3
├── notifier.py               # 通知機能
├── auto_monitor.py           # 自動監視（GitHub Actions）
├── requirements.txt          # 依存パッケージ
├── .streamlit/
│   └── secrets.toml.example  # Secrets テンプレート
├── data/
│   ├── watchlist.json        # 監視リスト
│   ├── score_history.json    # スコア履歴
│   ├── news_cache.json       # ニュースキャッシュ（自動更新）
│   └── volume_history.json   # 出来高履歴（自動蓄積）
└── .github/
    └── workflows/
        └── auto_monitor.yml  # GitHub Actions 設定
```

---

## 🔧 セットアップ手順

### 1. KABU+ の契約確認

[KABU+](https://kabu.plus/) のスタンダードプラン以上が必要です。

### 2. Streamlit Secrets の設定

**Streamlit Cloud の場合:**

Settings → Secrets に以下を貼り付け:

```toml
[kabuplus]
id = "あなたのKABU+ ID"
password = "あなたのKABU+ パスワード"

[app]
login_password = "88888"
admin_code = "888888"
```

**ローカル開発の場合:**

`.streamlit/secrets.toml` ファイルを作成（`.gitignore` に追加を忘れずに）

### 3. GitHub Secrets の設定

リポジトリの **Settings** → **Secrets and variables** → **Actions** で追加:

| Secret 名 | 値 | 必須 |
|---|---|---|
| `KABUPLUS_ID` | KABU+ の ID | ✅ |
| `KABUPLUS_PASSWORD` | KABU+ のパスワード | ✅ |
| `EMAIL_ENABLED` | `true` | 任意 |
| `EMAIL_ADDRESS` | 送信先メールアドレス | 任意 |
| `SMTP_SERVER` | `smtp.gmail.com` | 任意 |
| `SMTP_PORT` | `587` | 任意 |
| `SMTP_USER` | Gmail アドレス | 任意 |
| `SMTP_PASSWORD` | Gmail アプリパスワード | 任意 |
| `MIN_SCORE_THRESHOLD` | `50` | 任意 |

### 4. `.gitignore` の確認

```
.streamlit/secrets.toml
__pycache__/
*.pyc
```

---

## ⏰ 自動実行スケジュール

GitHub Actions で以下のタイミングで自動実行:

- **朝 8:00**（日本時間）— 前日データの分析
- **夜 20:00**（日本時間）— 当日データの分析

自動実行の内容:
1. KABU+ から全銘柄データを取得
2. 出来高履歴を蓄積（volume_ratio 計算用）
3. ウォッチリスト銘柄のニュースをスクレイピング
4. アラート条件に合致すればメール通知
5. 結果を JSON に保存して自動 commit

---

## 📊 出来高履歴について

v3 では出来高倍率（volume_ratio）の計算に過去データが必要です。
GitHub Actions が毎日実行されることで出来高履歴が蓄積されます。

- **運用開始直後**: 回転率（turnover_rate）ベースの判定
- **3日後〜**: 出来高倍率の計算が徐々に精度向上
- **20日後〜**: 完全な 20日平均出来高との比較が可能

---

## ⚠️ 注意事項

- 投資は自己責任でお願いします
- KABU+ の利用規約に従ってご利用ください
- 認証情報は絶対にコード内にハードコードしないでください
