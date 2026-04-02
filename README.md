# 🦅 源太AI ハゲタカSCOPE - 統合版

中型株（時価総額300億〜2000億円）の出来高急動を**毎日自動検知**し、**メール通知**できるツールです。

---

## 🎯 機能

| 機能 | 説明 |
|------|------|
| **自動更新** | 毎日 16:30 JST に GitHub Actions が自動実行 |
| **出来高急動検知** | 252日平均と比較して急増した銘柄を検知 |
| **時価総額フィルター** | 300億〜2000億円の中型株に特化 |
| **メール通知** | 利用者が自分のGmailを設定して通知受信 |

---

## 📊 指標

```
ratio = 当日出来高 / 直近252日平均出来高
```

| ratio | 判定 |
|-------|------|
| 3.0倍以上 | 🔴 異常な資金流入 |
| 1.5倍以上 | 🟠 注目すべき動き |

---

## 📂 ファイル構成

```
hagetaka-scope/
├── .github/
│   └── workflows/
│       └── daily.yml       ← 自動実行設定
├── data/
│   └── ratios.json         ← 計算結果（自動更新）
├── app.py                  ← Streamlitアプリ
├── fetch_data.py           ← データ取得スクリプト
├── requirements.txt
└── README.md
```

---

## 🛠 セットアップ手順

### Step 1: リポジトリ作成

1. GitHubで「**New repository**」をクリック
2. Repository name: `hagetaka-scope`（任意）
3. **Public** を選択
4. 「**Create repository**」をクリック

---

### Step 2: ファイルをアップロード

1. 「**uploading an existing file**」をクリック
2. ZIPを解凍して、**すべてのファイル・フォルダ**をドラッグ＆ドロップ

**重要**: `.github` フォルダも忘れずにアップロード！

Macで見えない場合: `Cmd + Shift + .` で表示

3. 「**Commit changes**」をクリック

---

### Step 3: Actions権限設定（重要！）

1. リポジトリの「**Settings**」を開く
2. 左メニュー「**Actions**」→「**General**」
3. 下にスクロール →「**Workflow permissions**」
4. ✅「**Read and write permissions**」を選択
5. 「**Save**」をクリック

---

### Step 4: 初回データ取得（手動実行）

1. 「**Actions**」タブを開く
2. 左側「**Daily Volume Spike Update**」をクリック
3. 「**Run workflow**」→「**Run workflow**」
4. 完了まで約2〜3分待つ

---

### Step 5: Streamlit Cloudでデプロイ

1. [share.streamlit.io](https://share.streamlit.io/) にアクセス
2. 「**Continue with GitHub**」でログイン
3. 「**New app**」をクリック
4. 設定:
   - **Repository**: `あなたのユーザー名/hagetaka-scope`
   - **Branch**: `main`
   - **Main file path**: `app.py`
5. 「**Deploy!**」をクリック

---

### Step 6: 完了！

アプリURL:
```
https://あなたのユーザー名-hagetaka-scope-app-xxxxx.streamlit.app
```

---

## 📧 メール通知の設定（利用者向け）

1. アプリの「🔔 通知設定」タブを開く
2. Gmailアドレスを入力
3. Gmailアプリパスワード（16桁）を入力
4. 「保存」→「テスト送信」で確認

### アプリパスワードの取得

1. [myaccount.google.com](https://myaccount.google.com/) にアクセス
2. セキュリティ → 2段階認証を有効化
3. [アプリパスワード](https://myaccount.google.com/apppasswords) で生成
4. 16桁のパスワードをコピー

---

## ⏰ 自動更新スケジュール

**平日 16:30 JST** に自動実行されます。

手動で更新したい場合は、Actionsタブから「Run workflow」を実行してください。

---

## ⚠️ 注意事項

- 投資判断は自己責任でお願いします
- データはYahoo Financeから取得しています
- メール設定はブラウザに保存（サーバーには送信しません）
