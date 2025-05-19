#!/usr/bin/env python3

import os
import smtplib
import ssl
from email.message import EmailMessage
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from generate_ics import fetch_team_fixtures

TIME_ZONE = ZoneInfo("Europe/London")
DAYS_BEFORE_REMINDER = 0

SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ.get("SMTP_PORT", 465))
SMTP_USERNAME = os.environ["SMTP_USERNAME"]
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]
EMAIL_SENDER_ADDRESS = os.environ.get("EMAIL_SENDER_ADDRESS", SMTP_USERNAME)
EMAIL_RECIPIENT_LIST = os.environ["RECIPIENTS"].split(",")


def send_reminder(fixture_datetime: datetime, opponent_team: str) -> None:
    """
    Send an email reminder for the given fixture date/time and opponent.
    """
    fixture_time = fixture_datetime.strftime("%H:%M")

    message = EmailMessage()
    message["Subject"] = f"Available v {opponent_team}"
    message["From"] = EMAIL_SENDER_ADDRESS
    message["To"] = ", ".join(EMAIL_RECIPIENT_LIST)

    email_body = (
        f"Hi,\n\n"
        f"Can you make **{fixture_time}** versus **{opponent_team}**?\n\n"
        "Cheers,\n"
        "Mark"
    )
    message.set_content(email_body)

    ssl_context = ssl.create_default_context()
    with smtplib.SMTP_SSL(
        host=SMTP_HOST, port=SMTP_PORT, context=ssl_context, timeout=10
    ) as smtp_connection:
        smtp_connection.login(SMTP_USERNAME, SMTP_PASSWORD)
        smtp_connection.send_message(message)


def main() -> None:
    """
    Fetch fixtures and send reminders for any fixtures happening today + DAYS_BEFORE_REMINDER.
    """
    upcoming_fixtures = fetch_team_fixtures()
    current_datetime = datetime.now(TIME_ZONE)
    reminder_date = (current_datetime + timedelta(days=DAYS_BEFORE_REMINDER)).date()

    for fixture_datetime, opponent_team in upcoming_fixtures:
        if fixture_datetime.date() == reminder_date:
            send_reminder(fixture_datetime, opponent_team)


if __name__ == "__main__":
    main()
