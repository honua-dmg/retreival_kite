import producer
import Consumers 
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
from redis_client import r
import simulator

"""
tail -f /root/stonks/cron.log - to view live - you can also run docker logs -f stonks_app_1
"""

ENVLOC = '/app/.env'
dotenv.load_dotenv(ENVLOC)

PATH = '/app/data'
def sleep_till9(hours,mins,seconds):
    return 9*3600+15*60- ( int(hours)*3600 + int(mins)*60+int(seconds) )

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

def begin():
    """
    This function is called at the beginning of the program. It starts the consumer threads and the producer/simulator.
    """
    print("Active threads:")
    for thread in threading.enumerate():
        print(f"Name: {thread.name}, \n\tAlive: {thread.is_alive()}\tDaemon: {thread.daemon} ")
    r.set('end','false')
    r.set('time',dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes= 30))).timestamp())# keep track of last tick time for watchdog
    simulation_mode = os.getenv("SIMULATION_MODE", "false").lower() == "true"

    if simulation_mode:
        simulation_date = os.getenv("SIMULATION_DATE")
        if not simulation_date:
            print("[MAIN] Error: SIMULATION_MODE is true but SIMULATION_DATE is not set. Exiting.", flush=True)
            r.set('end', 'true')
            return
        print(f"[MAIN] Starting in SIMULATION mode for date: {simulation_date}", flush=True)
        p = simulator.InitialiseSimulator(simulation_date)
        consumerThreads = Consumers.start_consumer_threads(PATH, num_consumers=5)
        for thread in consumerThreads:
            thread.join()
    else:
        producer_thread = threading.Thread(target=producer.heartbeat_monitor)
        producer_thread.start()
        consumerThreads = Consumers.start_consumer_threads(PATH, num_consumers=5)
        producer_thread.join()
        for thread in consumerThreads:
            thread.join()   
    
def end():
    """
    Ends the main program.
    
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

if __name__ == "__main__":
    """
    Main entry point of the program.
    
    This function is the main entry point of the program. It checks if the market is open and starts the main program.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    dotenv.load_dotenv(ENVLOC)
    r.config_set('notify-keyspace-events', 'AKE')
    logging.info("Enabled Redis Keyspace Notifications.")
    logging.info("Starting main program")
    logging.info(f"PATH: {PATH}")


 
    hours, mins, seconds = dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5), "%H:%M:%S").split(':')
    logging.info(f"Current time: {hours}:{mins}:{seconds}")
    if not os.getenv("SIMULATION_MODE", "false").lower() == "true": # only when not in simulation mode
        if int(hours) < 9 or (int(hours) == 9 and int(mins) < 15):
            logging.info("Time is before market hours")
            if len(r.keys()) > 0:
                logging.info("Flushing Redis")
                r.flushall()
            sleep_time = sleep_till9(hours, mins, seconds)
            logging.info(f"Sleeping for {sleep_time} seconds")
            time.sleep(sleep_time)
    

    # begin runs irrespective of simulation mode. end runs only when not in simulation mode
    logging.info("Starting main program")
    logging.info("Calling begin()")
    begin()
    if not os.getenv("SIMULATION_MODE", "false").lower() == "true": # only when not in simulation mode

        logging.info("Calling end()")
        end()    
        hours, mins, seconds = dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5), "%H:%M:%S").split(':')
        if int(hours) >= 15 and int(mins) >= 30:
            logging.info("Calling upload()")
            upload = Upload(PATH)
            upload.upload()
            upload.delete_old()
        else:
            logging.info('either some error happened or market is closed, not uploading files')
    #print("Calling shutdown_containers()", flush=True)
    #shutdown_containers()
    logging.info("Main program complete")
