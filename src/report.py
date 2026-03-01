"""
Email reporting module for the Stock Market Data Collection System.

This module provides email notification functionality for system alerts
and daily data collection reports.

Functions:
    - send_email_alert: Send an email notification
    - report: Send a daily data collection report
    - count: Count lines in CSV files for a given date
    - build_email_body: Build formatted email body for reports
"""

import os
from typing import Dict, List, Tuple, Optional

import resend
from dotenv import load_dotenv

import config
from utils import get_ist_now, get_ist_date


def send_email_alert(subject: str, body: str):
    """
    Send an email alert using the Resend API.
    
    Args:
        subject: Email subject line.
        body: Email body (HTML supported).
    """
    load_dotenv(config.ENVLOC)
    
    to_email = os.getenv("TO_EMAIL")
    resend_api_key = os.getenv("RESEND_API_KEY")
    from_email = os.getenv("FROM_EMAIL", "onboarding@resend.dev")
    
    if not resend_api_key:
        print("⚠️ RESEND_API_KEY not configured. Email not sent.", flush=True)
        return
    
    resend.api_key = resend_api_key
    
    try:
        resend.Emails.send({
            "from": from_email,
            "to": to_email,
            "subject": subject,
            "html": body
        })
        print('✅ Email sent successfully.', flush=True)
    except Exception as e:
        print(f"❌ Failed to send email: {e}", flush=True)


def report(body: str):
    """
    Send a daily data collection report email.
    
    Args:
        body: Report body content.
    """
    timestamp = get_ist_now().strftime('%Y-%m-%d %H:%M:%S')
    send_email_alert(
        subject=f"📊 DATA REVIEW: {timestamp}",
        body=body
    )


def count(path: str, date: str) -> List[Tuple[str, int]]:
    """
    Count the number of data lines in CSV files for a given date.
    
    Args:
        path: Directory path containing stock subdirectories.
        date: Date string (YYYY-MM-DD format) to count data for.
    
    Returns:
        list: Sorted list of (stock_name, line_count) tuples, including a 'total' entry.
              Returns [(0, 0)] if directory doesn't exist.
    
    Example:
        >>> count('/app/data/NSE', '2026-03-01')
        [('total', 45000), ('RELIANCE', 5000), ('INFY', 4500), ...]
    """
    files = {}
    total = 0
    
    try:
        for stock in os.listdir(path):
            # Skip hidden files
            if stock.startswith('.'):
                continue
            
            stock_dir = os.path.join(path, stock)
            csv_file = os.path.join(stock_dir, f'{date}.csv')
            
            try:
                with open(csv_file, 'r') as f:
                    # Subtract 1 for header row
                    line_count = len(f.readlines()) - 1
                    files[stock] = max(0, line_count)
            except FileNotFoundError:
                files[stock] = 0
            
            total += files[stock]
            
    except FileNotFoundError:
        return [(0, 0)]
    
    files['total'] = total
    
    # Sort by count descending
    sorted_files = sorted(files.items(), key=lambda x: x[1], reverse=True)
    return sorted_files


def _format_table(title: str, data: List[Tuple[str, int]]) -> str:
    """
    Format data as an ASCII table.
    
    Args:
        title: Table title.
        data: List of (name, count) tuples.
    
    Returns:
        str: Formatted table string.
    """
    if not data:
        return f"{title}:\nNo data\n"
    
    max_len = max(len(str(k)) for k, _ in data)
    max_len = max(max_len, 5)  # Minimum width for "Stock"
    
    lines = [f"<b>{title}:</b>"]
    lines.append(f"<pre>{'Stock':<{max_len}}  Count")
    lines.append("-" * (max_len + 10))
    
    for stock, count in data:
        if stock != 'total':
            lines.append(f"{stock:<{max_len}}  {count:,}")
    
    # Add total at the end
    total = next((c for s, c in data if s == 'total'), 0)
    lines.append("-" * (max_len + 10))
    lines.append(f"{'TOTAL':<{max_len}}  {total:,}</pre>")
    lines.append("")
    
    return "\n".join(lines)


def build_email_body(
    redis_count: int,
    nse_data: List[Tuple[str, int]],
    bse_data: List[Tuple[str, int]],
    extra_sections: Optional[Dict[str, str]] = None
) -> str:
    """
    Build a formatted HTML email body for the daily report.
    
    Args:
        redis_count: Total number of records currently in Redis streams.
        nse_data: NSE stock data counts from count().
        bse_data: BSE stock data counts from count().
        extra_sections: Optional additional sections to include.
    
    Returns:
        str: Formatted HTML email body.
    """
    date = get_ist_date()
    time = get_ist_now().strftime('%H:%M:%S')
    
    body = [
        f"<h2>📊 Daily Data Collection Report</h2>",
        f"<p><b>Date:</b> {date}<br><b>Time:</b> {time} IST</p>",
        f"<p><b>Redis Stream Records:</b> {redis_count:,}</p>",
        "<hr>",
        _format_table("NSE Stocks", nse_data),
        _format_table("BSE Stocks", bse_data),
    ]
    
    if extra_sections:
        body.append("<hr>")
        for title, content in extra_sections.items():
            body.append(f"<p><b>{title}:</b> {content}</p>")
    
    body.append("<hr>")
    body.append("<p>Regards,<br><b>Stock Data Collection System</b></p>")
    
    return "\n".join(body)


if __name__ == "__main__":
    # Test report generation
    load_dotenv(config.ENVLOC)
    
    path = config.DATA_PATH
    date = get_ist_date()
    
    nse = count(path=os.path.join(path, 'NSE'), date=date)
    bse = count(path=os.path.join(path, 'BSE'), date=date)
    
    body = build_email_body(
        redis_count=0,
        nse_data=nse,
        bse_data=bse,
        extra_sections={'Test': 'This is a test report'}
    )
    
    print(body)
