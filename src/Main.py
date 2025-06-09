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

import logging
from upload import Upload


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
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    response = requests.get(url, headers=headers, timeout=30)
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
    print("Market open: ",is_market_open())
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
    dotenv.load_dotenv(ENVLOC)
    path = PATH
    date= dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),"%Y-%m-%d")
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

def run_command(command):
    """Run a shell command and return output"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            logging.error(f"Command failed: {command}")
            logging.error(f"Error: {result.stderr}")
            return False
        return result.stdout
    except Exception as e:
        logging.error(f"Error running command: {command}")
        logging.error(str(e))
        return False

def shutdown_containers():
    """
    Shuts down all containers and removes unused resources.
    """ 
    # Load environment variables
    load_dotenv()
    
    # Stop all containers
    logging.info("Stopping all containers...")
    run_command("docker-compose down")
    
    # Remove stopped containers
    logging.info("Removing stopped containers...")
    run_command("docker rm -f $(docker ps -aq)")
    
    # Remove unused networks
    logging.info("Removing unused networks...")
    run_command("docker network prune -f")
    
    # Remove unused volumes
    logging.info("Removing unused volumes...")
    run_command("docker volume prune -f")
    
    # Check if any containers are still running
    running_containers = run_command("docker ps -q")
    if not running_containers:
        logging.info("All containers have been successfully stopped and cleaned up.")
    else:
        logging.warning("Warning: Some containers are still running:")
        run_command("docker ps")
    
    logging.info("Shutdown complete!")


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
    print("Calling upload()", flush=True)
    upload = Upload(PATH)
    upload.upload()
    upload.delete_old()
    #print("Calling shutdown_containers()", flush=True)
    #shutdown_containers()
    print("Main program complete", flush=True)




