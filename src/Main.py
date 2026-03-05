"""
Main entry point for the Stock Market Data Collection System.

This module orchestrates the entire data collection pipeline:
1. Waits for market open (9:15 AM IST)
2. Starts producer (WebSocket) and consumer (CSV writer) threads
3. Monitors data collection until market close (3:30 PM IST)
4. Sends daily report email
5. Uploads data to cloud storage
6. Cleans up old local files

Usage:
    python Main.py

Environment:
    Requires .env file with API credentials and configuration.
    See README.md for full list of required environment variables.
"""

import os
import time
import logging
import threading
import datetime as dt

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

import config
import producer
import Consumers
import report
from upload import Upload
from utils import get_ist_now, get_ist_date, IST


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def _seconds_until_market_open() -> int:
    """
    Calculate seconds until market opens (9:15 AM IST).
    
    Returns:
        int: Seconds to wait. Returns 0 if market is already open.
    """
    now = get_ist_now()
    market_open = now.replace(
        hour=config.MARKET_OPEN_HOUR,
        minute=config.MARKET_OPEN_MINUTE,
        second=0,
        microsecond=0
    )
    
    if now >= market_open:
        return 0
    
    return int((market_open - now).total_seconds())


def _is_after_market_close() -> bool:
    """
    Check if current time is after market close (3:30 PM IST).
    
    Returns:
        bool: True if market is closed.
    """
    now = get_ist_now()
    return (now.hour > config.MARKET_CLOSE_HOUR or 
            (now.hour == config.MARKET_CLOSE_HOUR and now.minute >= config.MARKET_CLOSE_MINUTE))


def get_holidays() -> list:
    """
    Fetch holiday dates from the Nifty Indices website.
    
    Returns:
        list: List of holiday date strings (e.g., ['01-Jan-2026', '26-Jan-2026']).
    
    Note:
        Returns empty list if fetching fails.
    """
    url = "https://www.niftyindices.com/resources/holiday-calendar"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        holiday_table = soup.find_all('tr')
        
        dates = []
        for row in holiday_table[1:]:  # Skip header row
            cols = row.find_all('td')
            if len(cols) > 3:
                date = cols[1].get_text(strip=True)
                dates.append(date)
        
        return dates
        
    except Exception as e:
        logger.warning(f"Failed to fetch holidays: {e}")
        return []


def is_market_open_today() -> bool:
    """
    Check if the market is open today.
    
    Checks for:
    - Weekends (Saturday/Sunday)
    - Public holidays from Nifty calendar
    
    Returns:
        bool: True if market should be open today.
    """
    today = get_ist_now()
    
    # Check weekend
    if today.weekday() >= 5:  # Saturday=5, Sunday=6
        logger.info(f"Market closed: Weekend ({today.strftime('%A')})")
        return False
    
    # Check holidays
    holidays = get_holidays()
    today_str = today.strftime('%d-%b-%Y')
    
    if today_str in holidays:
        logger.info(f"Market closed: Holiday ({today_str})")
        return False
    
    return True


def begin():
    """
    Start the main data collection process.
    
    Initializes Redis state, starts consumer threads, and launches
    the producer heartbeat monitor.
    """
    r = config.redis_client
    m = config.memcache_client
    
    # Log active threads
    logger.info("Active threads:")
    for thread in threading.enumerate():
        logger.info(f"  - {thread.name} (alive={thread.is_alive()}, daemon={thread.daemon})")
    
    # Initialize Redis state
    r.set('end', 'false')
    r.set('time', get_ist_now().timestamp())

    # Initialize Memcached offsets for tracked stocks
    for stock in config.get_stocks_list():
        cache_key = f"{config.OFFSETS_PREFIX}{stock}"
        if m.get(cache_key) is None:
            m.set(cache_key, "0")
    
    # Start consumers
    logger.info(f"Starting {config.DEFAULT_NUM_CONSUMERS} consumer threads...")
    consumer_threads = Consumers.start_consumer_threads(
        config.DATA_PATH,
        num_consumers=config.DEFAULT_NUM_CONSUMERS
    )
    
    # Start producer with heartbeat monitoring (blocking)
    logger.info("Starting producer heartbeat monitor...")
    producer_thread = threading.Thread(target=producer.heartbeat_monitor)
    producer_thread.start()
    producer_thread.join()
    
    # Wait for consumers to finish
    for thread in consumer_threads:
        thread.join()
    
    logger.info("Data collection complete.")


def end():
    """
    Send end-of-day report and cleanup.
    
    Generates a report with data collection statistics and sends it via email.
    """
    r = config.redis_client
    m = config.memcache_client
    load_dotenv(config.ENVLOC)
    
    date = get_ist_date()
    path = config.DATA_PATH
    
    # Count collected data
    nse_data = report.count(path=os.path.join(path, 'NSE'), date=date)
    bse_data = report.count(path=os.path.join(path, 'BSE'), date=date)
    
    # Calculate total from CSVs
    nse_total = next((c for s, c in nse_data if s == 'total'), 0)
    bse_total = next((c for s, c in bse_data if s == 'total'), 0)
    
    # Get Redis stream counts
    stocks = config.get_stocks_list()
    redis_count = sum(r.xlen(s) for s in stocks if r.exists(s))
    
    # Build and send report
    body = report.build_email_body(
        redis_count=redis_count,
        nse_data=nse_data,
        bse_data=bse_data,
        extra_sections={
            'Total CSV Records': f'{nse_total + bse_total:,}',
            'Collection Date': date
        }
    )
    
    report.report(body)
    
    # Cleanup Redis if after market close
    if _is_after_market_close():
        r.set('end', 'true')
        for stock in config.get_stocks_list():
            m.delete(f"{config.OFFSETS_PREFIX}{stock}")
        r.flushall()
        logger.info("Redis flushed and Memcached offsets cleared after market close.")


def upload_data():
    """
    Upload today's data to cloud storage and cleanup old files.
    """
    logger.info("Starting cloud upload...")
    
    try:
        uploader = Upload(config.DATA_PATH)
        uploader.upload()
        uploader.delete_old()
        logger.info("Cloud upload complete.")
    except Exception as e:
        logger.error(f"Upload failed: {e}")


def main():
    """
    Main entry point for the data collection system.
    
    Workflow:
    1. Load configuration
    2. Wait for market open if before 9:15 AM
    3. Run data collection
    4. Send report
    5. Upload to cloud (if after market close)
    """
    load_dotenv(config.ENVLOC)
    r = config.redis_client
    m = config.memcache_client
    
    logger.info("=" * 60)
    logger.info("Stock Market Data Collection System Starting")
    logger.info(f"Data Path: {config.DATA_PATH}")
    logger.info(f"Current Time: {get_ist_now().strftime('%Y-%m-%d %H:%M:%S')} IST")
    logger.info("=" * 60)
    
    # Check if market is open today
    if not is_market_open_today():
        logger.info("Market is closed today. Exiting.")
        return
    
    # Wait for market open if needed
    sleep_time = _seconds_until_market_open()
    if sleep_time > 0:
        logger.info(f"Waiting {sleep_time} seconds until market open (9:15 AM)...")
        
        # Flush Redis before market open
        if r.dbsize() > 0:
            logger.info("Flushing Redis before market open...")
            r.flushall()

        for stock in config.get_stocks_list():
            m.delete(f"{config.OFFSETS_PREFIX}{stock}")
        logger.info("Cleared Memcached offsets before market open.")
        
        time.sleep(sleep_time)
    
    # Run data collection
    logger.info("Starting data collection...")
    begin()
    
    # Send report
    logger.info("Sending end-of-day report...")
    end()
    
    # Upload if after market close
    if _is_after_market_close():
        upload_data()
    else:
        logger.warning("Market not closed yet. Skipping upload.")
    
    logger.info("=" * 60)
    logger.info("Data collection system shutdown complete.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()




