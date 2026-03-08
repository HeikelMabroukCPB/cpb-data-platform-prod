import logging
import os
import smtplib
from email.message import EmailMessage


logger = logging.getLogger(__name__)


def send_email(subject: str, body: str) -> None:
    sender = os.environ.get("EMAIL_SENDER")
    receiver = os.environ.get("EMAIL_RECEIVER")
    password = os.environ.get("EMAIL_APP_PASSWORD")

    if not sender or not receiver or not password:
        logger.warning("Email not configured, skipping email notification")
        return

    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = receiver

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        logger.info("Email sent successfully")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")