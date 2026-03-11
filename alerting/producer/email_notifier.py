import os
import smtplib
from email.message import EmailMessage

from dotenv import load_dotenv

load_dotenv()


class EmailNotifier:
    def __init__(self) -> None:
        self.smtp_host = os.getenv("ALERT_SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("ALERT_SMTP_PORT", "587"))
        self.smtp_user = os.getenv("ALERT_SMTP_USER", "")
        self.smtp_pass = os.getenv("ALERT_SMTP_PASS", "")
        self.smtp_tls = os.getenv("ALERT_SMTP_TLS", "true").lower() == "true"

        self.from_email = os.getenv("ALERT_FROM_EMAIL", self.smtp_user)
        self.to_email = os.getenv("ALERT_TO_EMAIL", "")

    def send(self, subject: str, body: str) -> None:
        if not self.to_email:
            print("[alerting] ALERT_TO_EMAIL não configurado. Alerta não enviado.")
            return

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.from_email
        msg["To"] = self.to_email
        msg.set_content(body)

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
            if self.smtp_tls:
                server.starttls()
            if self.smtp_user and self.smtp_pass:
                server.login(self.smtp_user, self.smtp_pass)
            server.send_message(msg)