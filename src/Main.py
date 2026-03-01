import producer
import Consumers 
import redis
import threading
import datetime as dt
import time
import report
import dotenv
import os
import requests
from bs4 import BeautifulSoup

import logging
from upload import Upload
"""
tail -f /root/stonks/cron.log - to view live - you can also run docker logs -f stonks_app_1
"""

ENVLOC = '/app/.env'
dotenv.load_dotenv(ENVLOC)

PATH = '/app/data'
def sleep_till9(hours,mins,seconds):
    
    return 9*3600+15*60- ( int(hours)*3600 + int(mins)*60+int(seconds) )

def get_holidays():
    """
    Fetches the holiday dates from the Nifty Indices website.
    
    Returns:
        list: A list of holiday dates as strings.
    """
    # URL for Nifty Indices Holiday Calendar
    url = "https://www.niftyindices.com/resources/holiday-calendar"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')
    holiday_table = soup.find_all('tr')
    dates = []


    # Iterate through each row (skipping the header row)
    for row in holiday_table[1:]:  # Starting from the second row
        cols = row.find_all('td')
        
        # Check if there are columns in this row
        if len(cols) > 3:
            date = cols[1].get_text(strip=True)
            dates.append(date)
    return dates

def is_market_open():
    """
    Checks if the market is open today based on the holiday calendar.
    
    Returns:
        bool: True if the market is open, False if it is a holiday.
    """

    today = dt.datetime.now().strftime('%d-%b-%Y')
    holidays = get_holidays()
    today = dt.datetime.today()
    
    # Check if today is in the list of holidays
    return today not in holidays or today.weekday() < 5  # Market is closed on weekends (Saturday=5, Sunday=6)

def begin(r):
    """
    Starts the main program.
    
    Args:
        r (redis.Redis): The Redis connection object.
    """
    print("Active threads:")
    for thread in threading.enumerate():
        print(f"Name: {thread.name}, \n\tAlive: {thread.is_alive()}\tDaemon: {thread.daemon} ")
    r.set('end','false')
    r.set('time',dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes= 30))).timestamp())# keep track of last tick time for watchdog

    consumerThreads = Consumers.start_consumer_threads(PATH, num_consumers=5)
    producer_thread = threading.Thread(target=producer.heartbeat_monitor)
    
    producer_thread.start()
    producer_thread.join()
    for thread in consumerThreads:
        thread.join()   
    

def end(r):
    """
    Ends the main program.
    
    Args:
        r (redis.Redis): The Redis connection object.
    """
    dotenv.load_dotenv(ENVLOC)
    path = PATH
    date= dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),"%Y-%m-%d")
    nse = report.count(path=os.path.join(path,'NSE'),date=date)
    bse = report.count(path=os.path.join(path,'BSE'),date=date)
    extra = {'actual count':nse[0][1]+bse[0][1]}
    body = report.build_email_body(
        redis_count=sum([r.xlen(x) for x in os.getenv("STOCKS").split(",")]),

        nse_data=nse,
        bse_data=bse,
        extra_sections=extra
        )
    
    report.report(body)
    hours, mins,seconds = dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),"%H:%M:%S").split(':')
    if int(hours)>=15 and int(mins)>=30:
        r.set('end','true')
        r.flushall() 


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('shutdown.log'),
        logging.StreamHandler()
    ]
)



if __name__ == "__main__":
    """
    Main entry point of the program.
    
    This function is the main entry point of the program. It checks if the market is open and starts the main program.
    """
    dotenv.load_dotenv(ENVLOC)
    r  = redis.Redis(host="redis",port="6379",db=0,decode_responses=True)
    print("Starting main program", flush=True)
    print(f"PATH: {PATH}", flush=True)


 
    hours, mins, seconds = dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5), "%H:%M:%S").split(':')
    print(f"Current time: {hours}:{mins}:{seconds}", flush=True)
    
    if int(hours) < 9 or (int(hours) == 9 and int(mins) < 15):
        print("Time is before market hours", flush=True)
        if len(r.keys()) > 0:
            print("Flushing Redis", flush=True)
            r.flushall()
        sleep_time = sleep_till9(hours, mins, seconds)
        print(f"Sleeping for {sleep_time} seconds", flush=True)
        time.sleep(sleep_time)
    
    print("Starting main program", flush=True)
    print("Calling begin()", flush=True)
    begin(r)
    print("Calling end()", flush=True)
    end(r)    
    hours, mins, seconds = dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5), "%H:%M:%S").split(':')
    if int(hours) >= 15 and int(mins) >= 30:
        print("Calling upload()", flush=True)
        upload = Upload(PATH)
        upload.upload()
        upload.delete_old()
    else:
        print('either some error happened or market is closed, not uploading files',flush=True)
    #print("Calling shutdown_containers()", flush=True)
    #shutdown_containers()
    print("Main program complete", flush=True)




