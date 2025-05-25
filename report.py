import redis
import os
import datetime as dt

def count_lines_safe(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        return sum(1 for _ in f)

def count(path,date):
    files = {}
    total = 0
    
    for file in os.listdir(path=path):
        if file=='.DS_Store':
            continue
        filepath = os.path.join(path,f'{file}')

        today = os.path.join(filepath,f'{date}.csv')
        with open(today,'r') as f:
            files[file] = len(f.readlines())
        #files[file] = count_lines_safe(today)
        total += files[file]
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

r = redis.Redis(host="localhost",port="6379",db=0)
path = r'/Users/gurusai/data/kite'
date= dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")
nse = count(path=os.path.join(path,'NSE'),date=date)
bse = count(path=os.path.join(path,'BSE'),date=date)
extra = {}
body = build_email_body(
    redis_count=r.xlen('data'),
    nse_data=nse,
    bse_data=bse,
    extra_sections=extra
    )

