import producer
import Consumers 
import redis
import threading
import datetime as dt
import time
import Report
import dotenv
import os


def sleep_till9(hours,mins,seconds):
    
    return 9*3600+15*60- ( int(hours)*3600 + int(mins)*60+int(seconds) )

def begin(r):
    print("Active threads:")
    for thread in threading.enumerate():
        print(f"Name: {thread.name}, \n\tAlive: {thread.is_alive()}\tDaemon: {thread.daemon} ")
    r.set('end','false')
    r.set('time',dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes= 30))).timestamp())
    saveConsumer = Consumers.Consumer(directory=r'/Users/gurusai/data/kite')
    consumer_thread = threading.Thread(target=saveConsumer.saveData,args=(10,))
    producer_thread = threading.Thread(target=producer.heartbeat_monitor)
    producer_thread.start()
    consumer_thread.start()
    producer_thread.join()
    consumer_thread.join()
        
    hours, mins = dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%H:%M").split(':')

    if int(hours)>=15 and int(mins)>=30:
        r.set('end','true')
        #r.flushall() - want to debug later on. 




if __name__=="__main__":
    r = redis.Redis(host="localhost",port="6379",db=0)
    hours, mins,seconds = dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%H:%M:%S").split(':')
    if int(hours)<9  or (int(hours)==9 and int(mins)<15):
        if len(r.keys())>0:
            r.flushall() # to ensure no extra data remains in cache. 
            print('flushed redis db (not done earlier)')
        print(f'present time is: {hours}: {mins}: {seconds}, we need to sleep for a bit.')
        sleep_time = sleep_till9(hours,mins,seconds)
        print(f'sleeping for {sleep_time}')
        time.sleep(sleep_time)
    print('beigning')
    begin(r)

    # report body:
    dotenv.load_dotenv()
    r = redis.Redis(host="localhost",port="6379",db=0)
    path = r'/Users/gurusai/data/kite'
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



