"""
通知機能モジュール
- メール通知（Gmail SMTP）
- ウォッチリスト管理
"""
from __future__ import annotations
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import os


# ==========================================
# データクラス
# ==========================================
@dataclass
class NotificationConfig:
    enabled: bool = False
    email_enabled: bool = False
    email_address: str = ""
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    line_enabled: bool = False
    line_token: str = ""
    min_score_threshold: int = 50
    notify_critical_only: bool = False


@dataclass
class NotificationResult:
    success: bool
    method: str
    message: str
    timestamp: datetime = field(default_factory=datetime.now)


# ==========================================
# ファイルパス
# ==========================================
DATA_DIR = "data"
CONFIG_PATH = os.path.join(DATA_DIR, "notification_config.json")
WATCHLIST_PATH = os.path.join(DATA_DIR, "watchlist.json")
SCORE_HISTORY_PATH = os.path.join(DATA_DIR, "score_history.json")


def _ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


# ==========================================
# 通知設定の永続化
# ==========================================
def load_notification_config() -> NotificationConfig:
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as f:
                data = json.load(f)
            return NotificationConfig(**data)
        except Exception:
            pass
    return NotificationConfig()


def save_notification_config(config: NotificationConfig):
    _ensure_data_dir()
    with open(CONFIG_PATH, "w") as f:
        json.dump(config.__dict__, f, indent=2)


# ==========================================
# ウォッチリスト
# ==========================================
def load_watchlist() -> List[str]:
    if os.path.exists(WATCHLIST_PATH):
        try:
            with open(WATCHLIST_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return []


def save_watchlist(watchlist: List[str]):
    _ensure_data_dir()
    with open(WATCHLIST_PATH, "w") as f:
        json.dump(watchlist, f)


# ==========================================
# スコア履歴
# ==========================================
def load_score_history() -> Dict[str, List[dict]]:
    if os.path.exists(SCORE_HISTORY_PATH):
        try:
            with open(SCORE_HISTORY_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_score_history(history: Dict[str, List[dict]]):
    _ensure_data_dir()
    with open(SCORE_HISTORY_PATH, "w") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def update_score_history(
    history: Dict[str, List[dict]],
    code: str,
    score: int,
    level: str,
) -> Dict[str, List[dict]]:
    """スコア履歴を更新（直近30件保持）"""
    if code not in history:
        history[code] = []
    history[code].append({
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "score": score,
        "level": level,
    })
    history[code] = history[code][-30:]
    return history


# ==========================================
# メール送信
# ==========================================
def send_email(
    to_address: str,
    subject: str,
    body: str,
    smtp_server: str = "smtp.gmail.com",
    smtp_port: int = 587,
    smtp_user: str = "",
    smtp_password: str = "",
) -> NotificationResult:
    try:
        msg = MIMEMultipart()
        msg["From"] = smtp_user
        msg["To"] = to_address
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, to_address, msg.as_string())

        return NotificationResult(True, "email", "送信成功")
    except Exception as e:
        return NotificationResult(False, "email", str(e))


def send_ma_alert(config: NotificationConfig, scores: list):
    """M&Aアラートをメールで送信"""
    if not config.email_enabled or not config.email_address:
        return

    lines = ["🎯 M&A予兆アラート\n"]
    for s in scores:
        lines.append(
            f"【{s.signal_level.value}】{s.name}（{s.code}）: {s.total_score}点"
        )
        if s.reason_tags:
            lines.append(f"  理由: {', '.join(s.reason_tags)}")
        lines.append("")

    lines.append(f"\n検知日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    body = "\n".join(lines)

    send_email(
        to_address=config.email_address,
        subject=f"🦅 ハゲタカSCOPE アラート（{len(scores)}件検知）",
        body=body,
        smtp_server=config.smtp_server,
        smtp_port=config.smtp_port,
        smtp_user=config.smtp_user,
        smtp_password=config.smtp_password,
    )


def send_hagetaka_alert(config: NotificationConfig, signals: list):
    """ハゲタカアラートをメールで送信"""
    if not config.email_enabled or not config.email_address:
        return

    lines = ["🦅 ハゲタカ検知アラート\n"]
    for s in signals:
        lines.append(
            f"【{s.signal_level.value}】{s.name}（{s.code}）: {s.total_score}点"
        )
        if s.signals:
            lines.append(f"  シグナル: {', '.join(s.signals[:3])}")
        lines.append("")

    lines.append(f"\n検知日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    body = "\n".join(lines)

    send_email(
        to_address=config.email_address,
        subject=f"🦅 ハゲタカSCOPE ロックオン（{len(signals)}件）",
        body=body,
        smtp_server=config.smtp_server,
        smtp_port=config.smtp_port,
        smtp_user=config.smtp_user,
        smtp_password=config.smtp_password,
    )
