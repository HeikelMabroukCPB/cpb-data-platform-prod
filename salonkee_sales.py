import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import smtplib
from email.message import EmailMessage

# 📧 Email notification helper
def send_email(subject, body):
    sender = os.environ.get("EMAIL_SENDER")
    receiver = os.environ.get("EMAIL_RECEIVER")
    app_password = os.environ.get("EMAIL_APP_PASSWORD")

    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(sender, app_password)
            smtp.send_message(msg)
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# 🗓️ Get previous month start to yesterday
def get_previous_month_range():
    today = datetime.today()
    first_of_current = today.replace(day=1)
    last_of_previous = first_of_current - timedelta(days=1)
    start = last_of_previous.replace(day=1)
    end = today - timedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

# 🚀 ETL entry point
def run_etl(request):
    start_date, end_date = get_previous_month_range()

    # 📡 API call
    url = "http://salonkee.be/api/web-services/v1/salon-group/157/sales"
    headers = {
        "Authorization": f"Bearer {os.environ.get('SALONKEE_API_TOKEN')}",
        "Accept": "application/json"
    }
    params = {
        "lang": "en",
        "startTime": start_date,
        "endTime": end_date
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        df = pd.json_normalize(data)
    except Exception as e:
        msg = f"❌ Failed to fetch or parse data: {str(e)}"
        send_email("❌ Salonkee_sales Failed — API Call", msg)
        return msg

    # 🧹 Delete existing data
    try:
        client = bigquery.Client()
        table_id = os.environ.get("BQ_TABLE_ID")
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE DATE(created) BETWEEN '{start_date}' AND '{end_date}'
        """
        client.query(delete_query).result()
    except Exception as e:
        msg = f"❌ Failed to delete existing data in BigQuery: {str(e)}"
        send_email("❌ Salonkee_sales Failed — DELETE", msg)
        return msg

    # 📤 Load new data
    try:
        client.load_table_from_dataframe(df, table_id).result()
    except Exception as e:
        msg = f"❌ Failed to load data into BigQuery: {str(e)}"
        send_email("❌ Salonkee_sales Failed — LOAD", msg)
        return msg

    # ✅ Success
    msg = f"✅ ETL success: {len(df)} rows loaded from {start_date} to {end_date}"
    send_email("✅ Salonkee_sales Success", msg)
    return msg
