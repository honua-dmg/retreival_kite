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
import Report
ENVLOC = '/app/.env'
class Consumer():
    def __init__(self,directory,num_consumers):
        """
        Initializes a Consumer object with the given directory and number of consumers.
        
        Args:
            directory (str): The directory where the CSV files are stored.
            num_consumers (int): The number of consumers to be used.
        """
        dotenv.load_dotenv( ENVLOC)
        self.directory = directory
        self.num_consumers = num_consumers
        self.api_key = os.getenv('APIKEY')
        self.kite = KiteConnect(api_key=self.api_key) 
        self.nse = self.tokenStockMapping("NSE")
        self.bse = self.tokenStockMapping("BSE")

        self.consumers = {}
        self.consumerLock = threading.Lock()
        self.date = dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),"%Y-%m-%d")
        self.rebalance_flag = threading.Event()  # shared across threads
        self.rebalance_flag.set()
        self.r  = redis.Redis(host="redis",port="6379",db=0,decode_responses=True)
        self.r.set('end','false')
        print(f"[DEBUG] Stocks in Redis: {self.r.hkeys('stocks')}")
        if not self.r.exists('stocks'):
            # Initialize stocks in Redis if not already present
            print("[INFO] Initializing stocks in Redis...")
            self.r.hset('stocks', mapping={x:"0" for x in os.getenv("STOCKS").split(",")})
            
                # Add cleanup thread
        self.cleanup_thread = None
        self.cleanup_interval = 10  # Run cleanup every 5 minutes
        self.cleanup_running = False
        self.cleanup_lag = 100  
    def ConvertToken(self,token):
        """
        Converts a token to a stock symbol.
        
        Args:
            token (int): The token to convert.
        
        Returns:
            str: The stock symbol corresponding to the token.
        """
        if token in self.nse.keys():
            return f"NSE:{self.nse[token]}"
        elif token in self.bse.keys():
            return f"BSE:{self.bse[token]}"

    def start_cleanup_thread(self):
        """Starts a thread that periodically cleans up Redis streams."""
        
        def cleanup_loop():
        
            while self.r.get('end')!='true':
                time.sleep(self.cleanup_interval)
                print(f"STARTING CLEAN UP AT {dt.datetime.now()}",flush=True)
                try:
                    processed_stocks = self.r.hkeys('stocks')
                    for stock in processed_stocks:
                        # Get the last processed ID for this stock
                        last_id = self.r.hget('stocks', stock)

                        if not last_id or last_id == "0":
                            continue
                        # Get the length of the stream
                        stream_length = self.r.xlen(stock)
                        
                        if stream_length <= self.cleanup_lag:
                            continue
                            
                        # Update cleanup ID and trim stream
                        self.r.xtrim(stock, minid=last_id,approximate=True)
                    print(f"Trimmed stream {stock} to {last_id}, there are {self.r.xlen(stock)} messages left, we trimmed ±{stream_length - self.cleanup_lag} messages ",flush=True)
                    print(f"[DEBUG] Stocks in Redis: {self.r.hgetall('stocks')}")
                    
                except Exception as e:
                    print(f"[ERROR] Failed to clean up streams: {e}",flush=True)
                    time.sleep(1)  # Wait a bit before retrying
                    
        self.cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True,name='Cleanup manager')
        self.cleanup_thread.start()

    def _stock_hash_watchdog(self):
        """A diagnostic thread that continuously monitors the existence of the 'stocks' hash."""
        print("[WATCHDOG] Starting 'stocks' hash monitor.")
        key_existed = self.r.exists('stocks')
        while self.r.get('end') != 'true':
            time.sleep(1)  # Check every second
            currently_exists = self.r.exists('stocks')
            if key_existed and not currently_exists:
                print(f"[CRITICAL] WATCHDOG DETECTED 'STOCKS' HASH DISAPPEARED AT {dt.datetime.now()}", flush=True)
                Report.send_email("STOCKS HASH DISAPPEARED",f"[CRITICAL] WATCHDOG DETECTED 'STOCKS' HASH DISAPPEARED AT {dt.datetime.now()}")
                self.r.set('end','true')
            if not key_existed and currently_exists:
                print(f"[INFO] WATCHDOG DETECTED 'STOCKS' HASH REAPPEARED AT {dt.datetime.now()}", flush=True)
                Report.send_email("STOCKS HASH REAPPEARED",f"[INFO] WATCHDOG DETECTED 'STOCKS' HASH REAPPEARED AT {dt.datetime.now()}")

            key_existed = currently_exists
        print("[WATCHDOG] Shutting down 'stocks' hash monitor.")


    def tokenStockMapping(self,exchange):
        """
        Maps tokens to their corresponding stock symbols.
        
        Args:
            exchange (str): The exchange name ('NSE' or 'BSE').
        
        Returns:
            dict: A dictionary mapping tokens to their stock symbols.
        """
        df = pd.read_csv(f"{exchange}.csv")
        return dict(zip( df['instrument_token'],df['tradingsymbol']))
    
    def next_redis_id(self,msg_id):
        if not msg_id or '-' not in msg_id:
            return "0-0"  # or optionally raise an error
        ts, seq = map(int, msg_id.split('-'))
        return f"{ts}-{seq + 1}"

    def CSVConsumer(self,id):
        """
        Consumes data from Redis and saves it to CSV files.
        
        Args:
            id (int): The ID of the consumer.
        """

        worker = Save.CSV(self.directory,self.kite )
        
        
        while self.r.get('end')!='true' : # continuosly reading the incoming stream of data.
            self.rebalance_flag.wait()
            with self.consumerLock:
                my_stocks = self.consumers.get(id)
            if not my_stocks:
                print(f"CONSUMER {id} NO STOCKS ASSIGNED AT {dt.datetime.now()}")
                time.sleep(2)
                continue
            
            offsets = self.r.hmget("stocks", my_stocks)
            if None in offsets:
                print(f"[CONSUMER {id}] None in offsets, sleeping...",flush=True)

            streams = {key:self.next_redis_id(val) 
                    for key,val in zip(my_stocks, offsets) 
                    if val is not None}
            
            if not streams:
                print(f"[CONSUMER {id}] No STREASM assigned, sleeping...",flush=True)
                time.sleep(1) # in the worst case event that my loadbalancer fucks up and doesn't assign any stocks to this consumer
                continue
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
                        worker.save_tick(data)
                        if msg_id == None:
                            print("AYOOO ISSUE FOUND MESSAGE ID IS NONE")
                        self.r.hset('stocks',stock_name,msg_id)
                    except Exception as e:
                        print(f"[ERROR] Failed to process tick {msg_id} for {stream[0]}: {e}",flush=True)
                        continue
        print('ending csvWorker')

    def saveData(self):
        """
        Saves data to CSV files.
        
        initialises the CSV files and starts the CSVConsumer threads.
        assigns each thread with an even number of stocks at random.
        """
        dotenv.load_dotenv(ENVLOC)
        Save.CSV(self.directory,self.kite).initialise()
        No_stocks = len(os.getenv("STOCKS").split(","))
        stocksPerConsumer = math.ceil(No_stocks/self.num_consumers)
        threads = []
        stocks = [key for key in self.r.hkeys("stocks")]
        for i in range(0,No_stocks,stocksPerConsumer):
            self.consumers[math.ceil(i/stocksPerConsumer)] = stocks[i:i+stocksPerConsumer]
            thread = threading.Thread(target=self.CSVConsumer,args=(math.ceil(i/stocksPerConsumer),),name=f'CSVCONSUMER_{math.ceil(i/stocksPerConsumer)}')
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
        
    def jobscheduler(self):
        """
        Rebalances the stocks between the CSVConsumer threads.
        
        This function is called every hour to rebalance the stocks between the CSVConsumer threads.
        It counts the number of lines in the CSV files for the current date and assigns the stocks to the threads
        in a way that minimizes the total number of lines in each thread.
        """
        print(f"[DEBUG] Stocks in Redis: {self.r.hkeys('stocks')}")
        self.rebalance_flag.clear()
        print('set wait state.',flush=True)
        time.sleep(.5)
        print(f"[REBALANCE] Starting rebalancing cycle at {dt.datetime.now()}",flush=True)

        all_stocks = self.r.hkeys("stocks")  # get stock names
        bse = []

        for stock in all_stocks:
            try:
                count = self.r.xlen(stock)
                bse.append((stock, count))
            except Exception as e:
                print(f"[ERROR] Could not get xlen for {stock}: {e}",flush=True)
        bse.sort(key=lambda x: -x[1])
        print(f"[REBALANCE] COUNT DONE: {dt.datetime.now()} Number of stocks: {len(bse)}",flush=True)
        num_consumers = self.num_consumers  # safer than len(self.consumers)
        new_assignments = defaultdict(list)
        totals = [0] * num_consumers
        for i, (stock, count) in enumerate(bse[:num_consumers]):
            new_assignments[i].append(stock)
            totals[i] += count
        for stock, count in bse[num_consumers:]:
            min_index = totals.index(min(totals))
            new_assignments[min_index].append(stock)
            totals[min_index] += count
        with self.consumerLock:
            self.consumers = dict(new_assignments)
        for cid, stocks in self.consumers.items():
            total_count = totals[cid]  # total count assigned to this consumer
            print(f"[REBALANCE] Assigned {total_count} stocks to consumer {cid}, {stocks}",flush=True)
        self.rebalance_flag.set()

    def start_thread_monitor(self, check_interval=11):
        """
        Starts a thread that monitors the CSVConsumer threads.
        
        This function is called when the Consumer object is initialized.
        It starts a thread that monitors the CSVConsumer threads and restarts them if they are down.
        """
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
 
    def start_scheduler(self, interval=11):
        """
        Starts a thread that runs the jobscheduler function every hour.
        
        This function is called when the Consumer object is initialized.
        It starts a thread that runs the jobscheduler function every hour.
        """
        def loop():
            while self.r.get('end')!='true' :
                time.sleep(interval)
                self.jobscheduler()
        threading.Thread(target=loop, daemon=True).start()

def start_consumer_threads(directory,num_consumers):
        """
        Starts the following methods in separate threads within the same process:
        - start_thread_monitor: launches internal monitoring thread(s)
        - start_scheduler: launches internal scheduling thread(s)
        - saveData: launches worker threads for data saving
        """
        self = Consumer(directory, num_consumers)
        

        def run_thread_monitor():
            self.start_thread_monitor()

        def run_scheduler():
            self.start_scheduler()

        def run_save_data():
            self.saveData()
        

        # Create threads
        t_monitor = threading.Thread(target=run_thread_monitor, name="ThreadMonitorStarter")

        #t_scheduler = threading.Thread(target=run_scheduler, name="SchedulerStarter")
        t_save_data = threading.Thread(target=run_save_data, name="SaveDataStarter")
        t_stock_hash_watchdog = threading.Thread(target=self._stock_hash_watchdog, name="StockHashWatchdog")
        self.start_cleanup_thread()
        # Start threads
        t_monitor.start()
        #t_scheduler.start()
        t_save_data.start()
        t_stock_hash_watchdog.start()

        #return [t_monitor, t_scheduler, t_save_data]
        return [t_save_data]


from unittest.mock import patch


# Assuming your Consumer class and imports are defined above or imported

def test_jobscheduler_with_init():
    """
    Tests the jobscheduler function with the Consumer object initialized.
    
    This function is called when the Consumer object is initialized.
    It tests the jobscheduler function with the Consumer object initialized.
    """
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