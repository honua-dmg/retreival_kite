import redis
import os
import datetime as dt
import dotenv
import smtplib
import ssl
import os
from email.message import EmailMessage


ENVLOC = '/app/.env'
def send_email_alert(subject, body):
    """
    Sends an email alert with the given subject and body.
    
    Args:
        subject (str): The subject of the email.
        body (str): The body of the email.
    
    Returns:
        None
    """
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
    """
    Sends an email alert with the given body.
    
    Args:
        body (str): The body of the email.
    
    Returns:
        None
    """
    send_email_alert(
        subject= f" DATA REVIEW: {dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),'%Y:%m:%d%H:%M:%S')}",
        body= body
    )
            
def count_lines_safe(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        return sum(1 for _ in f)-1

def count(path,date):
    """
    Counts the number of lines in the files in the given path for the given date.
    
    Args:
        path (str): The path to the directory containing the files.
        date (str): The date to count the lines for.
    
    Returns:
        list: A list of tuples containing the filename and the number of lines.
    """
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

def format_table(title, data):
    """
    Formats the given data into a table with the given title.
    
    Args:
        title (str): The title of the table.
        data (list): A list of tuples containing the data to be formatted.
    
    Returns:
        str: A string containing the formatted table.
    """
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
    """
    Builds the email body with the given data.
    
    Args:
        redis_count (int): The total number of records in Redis.
        nse_data (list): A list of tuples containing the NSE data.
        bse_data (list): A list of tuples containing the BSE data.
        extra_sections (dict, optional): A dictionary containing extra sections to be added to the email body.
    
    Returns:
        str: A string containing the email body.
    """
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


if __name__ == "__main__":
    dotenv.load_dotenv(ENVLOC)
    r = redis.Redis(host="redis",port="6379",db=0)
    path = os.getenv("FILEPATH")
    date= dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),"%Y-%m-%d")
    nse = count(path=os.path.join(path,'NSE'),date=date)
    bse = count(path=os.path.join(path,'BSE'),date=date)
    extra = {'actual count':nse[0][1]+bse[0][1]}
    body = build_email_body(
        redis_count=sum([r.xlen(x) for x in os.getenv("STOCKS").split(",")]),

        nse_data=nse,
        bse_data=bse,
        extra_sections=extra
        )

