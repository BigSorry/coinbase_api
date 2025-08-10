import os
import smtplib
import requests
from email.mime.text import MIMEText
from dotenv import load_dotenv
from typing import List, Tuple
from datetime import datetime

load_dotenv()
EMAIL_SECRET = os.getenv("EMAIL_SECRET")

def make_mail_content(product_id: str, price_history: List[Tuple[datetime, float]]):
    subject = f"Price history for {product_id}"
    body_lines = []

    for time, price in price_history:
        # Format datetime without seconds and microseconds
        formatted_time = time.strftime("%Y-%m-%d %H:%M")
        body_lines.append(f"{formatted_time}: {price}")

    body = "\n".join(body_lines)
    return subject, body

def send_mail(product_id, price_history):
    # Load environment variables from .env file
    email_config = {
        'sender': 'lexmeulenkamp@gmail.com',
        'recipient': 'lexmeulenkamp@hotmail.nl',
        'smtp_host': 'smtp.gmail.com',
        'smtp_port': 587,
        'smtp_user': 'lexmeulenkamp@gmail.com',
        'smtp_password': EMAIL_SECRET
    }

    subject, body = make_mail_content(product_id, price_history)
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = email_config["sender"]
    msg['To'] = email_config["recipient"]

    try:
        with smtplib.SMTP(email_config["smtp_host"], email_config["smtp_port"]) as server:
            server.starttls()
            server.login(email_config["smtp_user"], email_config["smtp_password"])
            server.send_message(msg)
    except Exception as e:
        print(f"Error sending email: {e}")


def make_telegram_message(renting_item):
    price = f"💶 Prijs: {renting_item.get('price', 'onbekend')} {renting_item.get('price_postfix', '')}"
    oppervlakte = f"📐 Oppervlakte: {renting_item.get('surface_area', '')}"
    date = f"Datum: {renting_item.get('date', 'onbekend')}"
    url = f"{renting_item.get('url', 'onbekend')}"
    message = f"{price} {oppervlakte} {date} \n {url}"

    return message

def send_telegram(scraped_item, CHAT_ID, PRIVATE_KEY):
    url = f"https://api.telegram.org/{PRIVATE_KEY}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": make_telegram_message(scraped_item)
    }
    try:
        requests.post(url, data=payload)
    except Exception as e:
        print(f"Error sending Telegram message: {e}")

def send_telegram_notifications(scraped_items, old_json, always_send=False):
    # Load environment variables from .env file
    load_dotenv()
    # Access the API key and private key from environment variables
    CHAT_ID = os.getenv("CHAT_ID")
    PRIVATE_KEY = os.getenv("PRIVATE_KEY")
    for json_item in scraped_items:
        url_key = list(json_item.keys())[0]
        item_info = json_item[url_key]
        date = item_info["date"] # TODO based on time not email_sent
        if url_key not in old_json or always_send:
            send_telegram(item_info, CHAT_ID, PRIVATE_KEY)
            item_info["email_sent"] = True
            old_json[url_key] = item_info
        else:
            print(f"Already sent email with url\n {url_key}\n")