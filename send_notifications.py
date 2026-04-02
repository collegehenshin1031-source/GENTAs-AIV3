"""
ハゲタカSCOPE - 自動メール通知スクリプト（GitHub Actions用）

目的：
- data/ratios.json（fetch_data.py が生成）を読み込み
- 登録ユーザーへ「候補一覧」を通知
- 本通知は市場データの可視化に基づく候補の共有であり、銘柄推奨・売買助言ではない
"""
import os
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime
import pytz

import gspread
from google.oauth2.service_account import Credentials
from cryptography.fernet import Fernet

JST = pytz.timezone("Asia/Tokyo")

# 通知対象条件（安全側のデフォルト）
NOTIFY_LEVEL_MIN = 3      # LEVEL 3以上
NOTIFY_FLOW_MIN = 70.0    # FlowScore 70以上


# ==========================================
# 暗号化キー取得・復号化
# ==========================================
def get_encryption_key() -> str:
    key = os.environ.get("ENCRYPTION_KEY")
    if not key or key == "false":
        raise ValueError("ENCRYPTION_KEY environment variable is not set correctly.")
    return key


def decrypt_password(encrypted_password: str) -> str:
    if not encrypted_password:
        return ""
    try:
        key = get_encryption_key()
        fernet = Fernet(key.encode())
        return fernet.decrypt(encrypted_password.encode()).decode()
    except Exception as e:
        print(f"復号化エラー: {e}")
        return ""


# ==========================================
# Google Sheets接続
# ==========================================
def get_gspread_client():
    credentials_json = os.environ.get("GSHEETS_CREDENTIALS")
    if not credentials_json:
        raise ValueError("GSHEETS_CREDENTIALS environment variable is not set")

    credentials_dict = json.loads(credentials_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
    return gspread.authorize(credentials)


def load_all_users() -> list:
    try:
        client = get_gspread_client()
        spreadsheet_url = os.environ.get("SPREADSHEET_URL")
        if not spreadsheet_url:
            raise ValueError("SPREADSHEET_URL environment variable is not set")

        spreadsheet = client.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet("settings")
        records = worksheet.get_all_records()

        users = []
        for record in records:
            email = (record.get("email", "") or "").strip()
            encrypted_password = (record.get("encrypted_password", "") or "").strip()
            if not email or not encrypted_password:
                continue

            password = decrypt_password(encrypted_password)
            if not password:
                continue

            users.append({"email": email, "app_password": password})

        return users

    except Exception as e:
        print(f"ユーザー読み込みエラー: {e}")
        return []


# ==========================================
# データ読み込み
# ==========================================
def load_data() -> dict:
    p = Path("data/ratios.json")
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def select_notify_items(data: dict) -> list[dict]:
    stocks_data = data.get("data", {}) or {}
    items = []
    for ticker, d in stocks_data.items():
        level = int(d.get("level", 0))
        flow = float(d.get("flow_score", 0))
        if level >= NOTIFY_LEVEL_MIN or flow >= NOTIFY_FLOW_MIN:
            items.append({"ticker": ticker, **d})
    items.sort(key=lambda x: (int(x.get("level", 0)), float(x.get("ma_score", 0)), float(x.get("flow_score", 0))), reverse=True)
    return items


# ==========================================
# メール本文（★変更箇所）
# ==========================================
def create_email(data: dict, items: list[dict]) -> tuple[str, str] | tuple[None, None]:
    if not items:
        return None, None

    updated_at = data.get("updated_at", "不明")
    date_str = updated_at[:10] if isinstance(updated_at, str) else datetime.now(JST).strftime("%Y-%m-%d")

    subject = f"🦅 ハゲタカSCOPE 候補通知: {len(items)}件 - {date_str}"

    lines = [
        "━" * 38,
        " 🦅 ハゲタカSCOPE 検出レポート",
        "━" * 38,
        f"📅 更新日時: {updated_at}",
        f"🎯 検出条件: LEVEL {NOTIFY_LEVEL_MIN}以上 または 需給スコア {NOTIFY_FLOW_MIN}以上",
        "",
        "本日の市場から、大口資金の介入や異常な出来高変化が疑われる銘柄をピックアップしました。",
        "気になった銘柄があれば、アプリの【🦅 ハゲタカAIで診断する】にコードを入力して、",
        "上値余地や安全性を必ずチェックしてください！",
        "",
        "▼ アプリはこちら（ログインして確認）",
        "https://gentaai-hagetaka-scope.streamlit.app/",
        "",
        "━" * 38,
        " 📊 本日のピックアップ銘柄",
        "━" * 38,
        ""
    ]

    # LEVELでグルーピング
    levels_dict = {4: [], 3: [], 2: [], 1: [], 0: []}
    for s in items[:30]:  # 上位30件まで
        lv = int(s.get("level", 0))
        levels_dict[lv].append(s)

    for lv in [4, 3, 2, 1, 0]:
        group = levels_dict[lv]
        if not group:
            continue
        
        # LEVELごとの見出し
        if lv == 4:
            lines.append(f"🟥 【LEVEL 4】 （{len(group)}件） - 特異性高・要注目！")
        elif lv == 3:
            lines.append(f"🟧 【LEVEL 3】 （{len(group)}件）")
        elif lv == 2:
            lines.append(f"🟨 【LEVEL 2】 （{len(group)}件）")
        elif lv == 1:
            lines.append(f"🟦 【LEVEL 1】 （{len(group)}件）")
        else:
            lines.append(f"⬜ 【LEVEL 0】 （{len(group)}件）")
            
        lines.append("-" * 38)
        
        for s in group:
            ticker = s.get("ticker", "").replace(".T", "")
            name = (s.get("name", "") or "")[:15]
            flow = s.get("flow_score", 0)
            state = s.get("display_state", s.get("state", ""))
            tags = s.get("tags", [])
            tag_txt = " / ".join(tags[:4]) if tags else "-"
            
            lines.append(f"・{ticker} {name}")
            lines.append(f"  [需給スコア: {flow}] 状態: {state}")
            if tag_txt != "-":
                lines.append(f"  {tag_txt}")
            lines.append("")

    lines.extend([
        "━" * 38,
        "⚠️ 免責事項",
        "本通知は市場データの可視化に基づく情報提供であり、投資推奨や売買助言ではありません。",
        "実際の投資判断は、利用者ご自身の責任において行ってください。",
        "━" * 38,
    ])

    return subject, "\n".join(lines)


def send_email(to_email: str, app_password: str, subject: str, body: str) -> bool:
    try:
        msg = MIMEMultipart()
        msg["From"] = to_email
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(to_email, app_password)
            server.send_message(msg)
        return True
    except smtplib.SMTPAuthenticationError:
        print(f"認証エラー: {to_email}")
        return False
    except Exception as e:
        print(f"送信エラー: {to_email}: {e}")
        return False


def main():
    data = load_data()
    items = select_notify_items(data)

    subject, body = create_email(data, items)
    if not subject:
        print("📭 通知対象がないため、メール送信は行いません。")
        return

    users = load_all_users()
    if not users:
        print("⚠️ ユーザーが取得できないため、送信をスキップします。")
        return

    ok = 0
    ng = 0
    for u in users:
        if send_email(u["email"], u["app_password"], subject, body):
            ok += 1
        else:
            ng += 1

    print(f"✅ 送信完了: 成功={ok}, 失敗={ng}")


if __name__ == "__main__":
    main()
