import producer
import Consumers 
import redis
import threading
import datetime as dt
import time
import Report
import dotenv
import os
import requests
from bs4 import BeautifulSoup
dotenv.load_dotenv()
PATH = os.getenv("FILEPATH")
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
    if not is_market_open():
        print("Market is closed today. Exiting...")
        r.set('end','true')
        return

    print("Active threads:")
    for thread in threading.enumerate():
        print(f"Name: {thread.name}, \n\tAlive: {thread.is_alive()}\tDaemon: {thread.daemon} ")
    r.set('end','false')
    r.set('time',dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes= 30))).timestamp())# keep track of last tick time for watchdog

    consumerThreads = Consumers.start_consumer_threads(PATH, num_consumers=10)
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
    dotenv.load_dotenv()
    path = PATH
    date= dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")
    nse = Report.count(path=os.path.join(path,'NSE'),date=date)
    bse = Report.count(path=os.path.join(path,'BSE'),date=date)
    extra = {'actual count':nse[0][1]+bse[0][1]}
    body = Report.build_email_body(
        redis_count=sum([r.xlen(x) for x in os.getenv("STOCKS").split(",")]),

        nse_data=nse,
        bse_data=bse,
        extra_sections=extra
        )
    
    Report.report(body)
    hours, mins,seconds = dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%H:%M:%S").split(':')
    if int(hours)>=15 and int(mins)>=30:
        r.set('end','true')
        r.flushall() 

if __name__ == "__main__":
    """
    Main entry point of the program.
    
    This function is the main entry point of the program. It checks if the market is open and starts the main program.
    """
    r = redis.Redis(host="redis", port="6379", db=0)
    hours, mins, seconds = dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5), "%H:%M:%S").split(':')
    if int(hours) < 9 or (int(hours) == 9 and int(mins) < 15):
        if len(r.keys()) > 0:
            r.flushall()  # to ensure no extra data remains in cache. 
            print('flushed redis db (not done earlier)')
        print(f'present time is: {hours}: {mins}: {seconds}, we need to sleep for a bit.')
        sleep_time = sleep_till9(hours,mins,seconds)
        print(f'sleeping for {sleep_time}')
        time.sleep(sleep_time)

    print('beigning')
    begin(r)
    end(r)



