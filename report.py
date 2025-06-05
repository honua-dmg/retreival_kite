import redis
import os
import datetime as dt
import dotenv
import smtplib
import ssl
import os
from email.message import EmailMessage
import requests


def send_email_alert(subject, body):
    email_address = os.getenv("EMAIL_ADDRESS")
    email_password = os.getenv("EMAIL_PASSWORD")
    to_email = os.getenv("TO_EMAIL")

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = email_address
    msg['To'] = to_email
    msg.set_content(body)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(email_address, email_password)
            smtp.send_message(msg)
        print("✅ Email sent successfully.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

def report(body):
    send_email_alert(
        subject= f" DATA REVIEW: {dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y:%m:%d%H:%M:%S")}",
        body= body
    )
            

def count_lines_safe(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        return sum(1 for _ in f)-1

def count(path,date):
    files = {}
    total = 0
    try:
        for file in os.listdir(path=path):
            if file=='.DS_Store':
                continue
            filepath = os.path.join(path,f'{file}')

            today = os.path.join(filepath,f'{date}.csv')
            try:
                with open(today,'r') as f:
                    files[file] = len(f.readlines())
            except FileNotFoundError:
                files[file] = 0
            #files[file] = count_lines_safe(today)
            total += files[file]
    except FileNotFoundError:
        return [(0,0)]
    files['total'] = total
    sortedd = sorted(files.items(),key = lambda x: x[1],reverse=True)
    return sortedd

import os

def format_table(title, data):
    # Ensure columns align
    max_len = max((len(str(k)) for k, _ in data), default=5)
    lines = [f"{title}:\n"]
    lines.append(f"{'Stock':<{max_len}}  Count")
    lines.append("-" * (max_len + 8))
    for stock, count in data:
        if stock != 'total':
            lines.append(f"{stock:<{max_len}}  {count}")
    lines.append("\n")
    return "\n".join(lines)

def build_email_body(redis_count, nse_data, bse_data, extra_sections=None):
    body = []
    body.append("HELLO THERE!\n")
    body.append(f"We got a total of {redis_count} records\n")

    body.append(format_table("NSE", nse_data))
    body.append(format_table("BSE", bse_data))

    if extra_sections:
        for title, content in extra_sections.items():
            body.append(f"{title}:\n{content}\n")

    body.append("REGARDS:\nGURU SAI")
    return "\n".join(body)

dotenv.load_dotenv()
r = redis.Redis(host="redis",port="6379",db=0)
path = r'/Users/gurusai/data/kite'
date= dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")
nse = count(path=os.path.join(path,'NSE'),date=date)
bse = count(path=os.path.join(path,'BSE'),date=date)
extra = {'actual count':nse[0][1]+bse[0][1]}
body = build_email_body(
    redis_count=sum([r.xlen(x) for x in os.getenv("STOCKS").split(",")]),

    nse_data=nse,
    bse_data=bse,
    extra_sections=extra
    )

