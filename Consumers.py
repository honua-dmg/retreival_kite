import redis
import Save
import os
import dotenv
from kiteconnect import KiteConnect
import json
import threading
import pandas as pd
import Report
import datetime as dt
import time
from collections import defaultdict
import math
class Consumer():
    def __init__(self,directory,num_consumers):
        dotenv.load_dotenv()
        self.directory = directory
        self.num_consumers = num_consumers
        self.api_key = os.getenv('APIKEY')
        self.kite = KiteConnect(api_key=self.api_key) 
        self.nse = self.tokenStockMapping("NSE")
        self.bse = self.tokenStockMapping("BSE")

        self.consumers = {}
        self.date = dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")
        self.rebalance_flag = threading.Event()  # shared across threads
        self.rebalance_flag.set()
        self.r  = redis.Redis(host="localhost",port="6379",db=0,decode_responses=True)
        if not self.r.exists('stocks'):
            # Initialize stocks in Redis if not already present
            print("[INFO] Initializing stocks in Redis...")
            self.r.hset('stocks', mapping={x:"0" for x in os.getenv("STOCKS").split(",")})
    def tokenStockMapping(self,exchange):
        df = pd.DataFrame(self.kite.instruments(exchange))
        return dict(zip( df['instrument_token'],df['tradingsymbol']))
    
    def ConvertToken(self,token):
        if token in self.nse.keys():
            return f"NSE:{self.nse[token]}"
        elif token in self.bse.keys():
            return f"BSE:{self.bse[token]}"
        

    def CSVConsumer(self,id):
        """
        which directory will 
        """
        
        worker = Save.CSV(self.directory,self.kite )
        
        
        
        while self.r.get('end')!='true' : # continuosly reading the incoming stream of data.
            self.rebalance_flag.wait()
                
            offsets = self.r.hmget("stocks", self.consumers[id])
            streams = {key:val for key,val in zip(self.consumers[id], offsets) if val is not None}
            messages = self.r.xread(streams,block=100)
            if messages == []:
                continue
            #print(messages)
            for stream in messages:
                for uncoded_msg in stream[1]:
                    msg_id = uncoded_msg[0]

                    try:
                        data = json.loads(uncoded_msg[1]['data'])
                        stock_name = self.ConvertToken(data['instrument_token']).split(':')[1]
                        self.r.hset('stocks',stock_name,msg_id)
                        worker.save_tick(data)
                    except Exception as e:
                        print(f"[ERROR] Failed to process tick: {e}")
                        continue
        print('ending csvWorker')


    def saveData(self):
        dotenv.load_dotenv()
        Save.CSV(self.directory,self.kite).initialise()
        No_stocks = len(os.getenv("STOCKS").split(","))
        stocksPerConsumer = math.ceil(No_stocks/self.num_consumers)
        threads = []
        for i in range(0,No_stocks,stocksPerConsumer):
            self.consumers[math.ceil(i/stocksPerConsumer)] = [key for key in self.r.hkeys("stocks")][i:i+stocksPerConsumer]
            thread = threading.Thread(target=self.CSVConsumer,args=(math.ceil(i/stocksPerConsumer),),name=f'CSVCONSUMER_{math.ceil(i/stocksPerConsumer)}')
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
        
    def jobscheduler(self):
        self.rebalance_flag.clear()

        bse = Report.count(path=os.path.join(self.directory, 'BSE'), date=self.date)[1:]

        num_consumers = self.num_consumers  # safer than len(self.consumers)
        new_assignments = defaultdict(list)
        totals = [0] * num_consumers

        for stock, count in bse:
            min_index = totals.index(min(totals))
            new_assignments[min_index].append(stock)
            totals[min_index] += count

        self.consumers = dict(new_assignments)
        for cid, stocks in self.consumers.items():
            total_count = totals[cid]  # total count assigned to this consumer

        self.rebalance_flag.set()

    def start_thread_monitor(self, check_interval=10):
        def monitor():
            while self.r.get('end')!='true':
                time.sleep(check_interval)
                active = {t.name for t in threading.enumerate()}
                for cid in self.consumers:
                    tname = f"CSVCONSUMER_{cid}"
                    if tname not in active:
                        print(f"[Monitor] {tname}, responsible for :\n\t{self.consumers[cid]}\n is down. Restarting...")

                        thread = threading.Thread(target=self.CSVConsumer, args=(cid,), name=tname)
                        thread.start()
        threading.Thread(target=monitor, daemon=True).start()

    def start_scheduler(self, interval=3600):
        def loop():
            while self.r.get('end')!='true' :
                time.sleep(interval)
                self.jobscheduler()
        threading.Thread(target=loop, daemon=True).start()
def start_consumer_threads(directory,num_consumers):
        self = Consumer(directory, num_consumers)
        """
        Starts the following methods in separate threads within the same process:
        - start_thread_monitor: launches internal monitoring thread(s)
        - start_scheduler: launches internal scheduling thread(s)
        - saveData: launches worker threads for data saving
        """

        def run_thread_monitor():
            self.start_thread_monitor()

        def run_scheduler():
            self.start_scheduler()

        def run_save_data():
            self.saveData()

        # Create threads
        t_monitor = threading.Thread(target=run_thread_monitor, name="ThreadMonitorStarter")
        t_scheduler = threading.Thread(target=run_scheduler, name="SchedulerStarter")
        t_save_data = threading.Thread(target=run_save_data, name="SaveDataStarter")

        # Start threads
        t_monitor.start()
        t_scheduler.start()
        t_save_data.start()

        #return [t_monitor, t_scheduler, t_save_data]
        return [t_save_data]

import os
import threading
from unittest.mock import patch
import datetime as dt

# Assuming your Consumer class and imports are defined above or imported

def test_jobscheduler_with_init():
    # Mock BSE tick count data (simulate what Report.count returns)
    mock_bse_data = [
        ('RELIANCE', 34000),
        ('HDFCBANK', 33000),
        ('INFY', 31000),
        ('TCS', 30000),
        ('IOC', 12000),
        ('BPCL', 11000),
        ('GAIL', 9000),
        ('ONGC', 8000),
        ('POWERGRID', 7000),
        ('NTPC', 6000),
    ]

    # Mock environment variables expected by Consumer
    mock_env = {
        "APIKEY": "test_api_key",
        "STOCKS": "RELIANCE,HDFCBANK,INFY,TCS,IOC,BPCL,GAIL,ONGC,POWERGRID,NTPC"
    }

    # Mock instruments returned by kite.instruments()
    mock_instruments = [
        {"instrument_token": 1111, "tradingsymbol": "RELIANCE"},
        {"instrument_token": 2222, "tradingsymbol": "HDFCBANK"},
        {"instrument_token": 3333, "tradingsymbol": "INFY"},
        {"instrument_token": 4444, "tradingsymbol": "TCS"},
        {"instrument_token": 5555, "tradingsymbol": "IOC"},
        {"instrument_token": 6666, "tradingsymbol": "BPCL"},
        {"instrument_token": 7777, "tradingsymbol": "GAIL"},
        {"instrument_token": 8888, "tradingsymbol": "ONGC"},
        {"instrument_token": 9999, "tradingsymbol": "POWERGRID"},
        {"instrument_token": 1010, "tradingsymbol": "NTPC"},
    ]

    with patch.dict(os.environ, mock_env), \
         patch('kiteconnect.KiteConnect') as MockKite, \
         patch('Report.count', return_value=[None] + mock_bse_data):

        # Setup the mock kite instance
        mock_kite = MockKite.return_value
        mock_kite.instruments.return_value = mock_instruments

        # Create the Consumer instance
        consumer = Consumer(directory="./mockdata", num_consumers=3)

        # Run the jobscheduler method which reassigns stocks to consumers
        consumer.jobscheduler()

        # Print the output for verification
        print("\n[TEST] Rebalanced Assignments:")
        for cid, stocks in consumer.consumers.items():
            total_ticks = sum(dict(mock_bse_data).get(stock, 0) for stock in stocks)
            #print(f"Consumer {cid}: {stocks} | Total Tick Load: {total_ticks}")

        # Assertions (basic checks)
        assert len(consumer.consumers) == 3, "Should have 3 consumer groups"
        assert all(len(stocks) > 0 for stocks in consumer.consumers.values()), "Each consumer must have stocks assigned"


if __name__ == '__main__':
    test_jobscheduler_with_init()