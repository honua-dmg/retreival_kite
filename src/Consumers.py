import redis
import Save
import os
import dotenv
from kiteconnect import KiteConnect
import json
import threading
import pandas as pd

import datetime as dt
import time
from collections import defaultdict
from redis_client import r
import math
import logging
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
        self.date = dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),"%Y-%m-%d")
        # Synchronization primitives for safe Rebalancing
        self.rebalance_interval = 60*60
        self.rebalance_lock = threading.Lock()
        self.rebalance_condition = threading.Condition(self.rebalance_lock)
        self.rebalance_in_progress = False
        self.paused_consumers_count = 0
        self.r  = r
        self.r.set('end','false')   
    
        self.stock_list = os.getenv("STOCKS").split(",")

        #initialised stock offsets in memcached
        memcached_client = Client(os.getenv('MEMCACHE_HOST','memcache'), os.getenv("MEMCACHE_PORT",11211), encoding='utf-8')
        for stock in self.stock_list:
            offset_key = f'stock_offset:{stock}'
            if memcached_client.get(offset_key) is None:
                memcached_client.set(offset_key, "0")

        # Add cleanup thread
        self.cleanup_thread = None 
        self.cleanup_interval = 60*20  # Run cleanup every 20 minutes
        self.cleanup_running = False
        self.cleanup_lag = 100  

        self.simulation_mode = os.getenv("SIMULATION_MODE", "false").lower() == "true"
    
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
            memcached_client = Client((os.getenv('MEMCACHE_HOST','memcache'), os.getenv("MEMCACHE_PORT",11211)), encoding='utf-8')
            logging.info(f"STARTING CLEAN UP AT {dt.datetime.now()}")
            while self.r.get('end')!='true':
                time.sleep(self.cleanup_interval)
                logging.info(f"[CLEANUP] Processing {len(self.stock_list)} stocks for cleanup.")
                for stock in self.stock_list:
                    try:
                        offset_key = f'stock_offset:{stock}'
                        last_id = memcached_client.get(offset_key)
                        if not last_id:
                            continue
                        
                        stream_length = self.r.xlen(stock)
                        if stream_length <= self.cleanup_lag:
                            continue

                        self.r.xtrim(stock, minid=last_id, approximate=True)
                        new_length = self.r.xlen(stock)
                        
                        logging.info(f"[CLEANUP] Trimmed stream {stock} using minid={last_id}. Previous length: {stream_length}, New length: {new_length}")
                    except Exception as e:
                        logging.error(f"[CLEANUP] Error in cleanup loop: {e}")
                
        if not self.cleanup_running:
            self.cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True, name='CleanupManager')
            self.cleanup_thread.start()
            logging.info("Cleanup thread started.")
            self.cleanup_running = True

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
        memcached_client = Client((os.getenv('MEMCACHE_HOST','memcache'), os.getenv("MEMCACHE_PORT",11211)), encoding='utf-8')
        while self.r.get('end')!='true':
            # Check for rebalancing flag
            with self.rebalance_lock:
                if self.rebalance_in_progress:
                    logging.info(f"Consumer {id} pausing for rebalancing.")
                    self.paused_consumers_count += 1
                    self.rebalance_condition.notify()
                    while self.rebalance_in_progress:
                        self.rebalance_condition.wait()
                    logging.info(f"Consumer {id} resuming after rebalancing.")

            for stock in self.consumers[id]:
                # Construct the key for storing the last processed ID in Redis
                offset_key = f'stock_offset:{stock}'
                
                # Get the last processed message ID for this stock from Memcached, default to "0"
                last_id = memcached_client.get(offset_key)
                if last_id is None:
                    last_id = "0"

                # Poll Redis for new messages in the stream for this stock
                messages = self.r.xread({stock: last_id}, count=10, block=1000)
                
                if messages == []:
                    continue
                #print(messages)
                for stream in messages:
                    for uncoded_msg in stream[1]:
     
                        msg_id = uncoded_msg[0]
                        try:
                            if self.simulation_mode:
                                time.sleep(.01)
                                if msg_id == None:
                                    logging.info("AYOOO ISSUE FOUND MESSAGE ID IS NONE")
                                memcached_client.set(offset_key, msg_id)
                                continue
                            else:
                                data = json.loads(uncoded_msg[1]['data'])
                                stock_name = self.ConvertToken(data['instrument_token']).split(':')[1]
                                worker.save_tick(data)
                                if msg_id == None:
                                    logging.info("AYOOO ISSUE FOUND MESSAGE ID IS NONE")
                                memcached_client.set(offset_key, msg_id)
                        except Exception as e:
                            logging.error(f"[ERROR] Failed to process tick {msg_id} for {stream[0]}: {e}")
                            continue
        logging.info('ending csvWorker')

    def saveData(self):
        """
        Saves data to CSV files.
        
        initialises the CSV files and starts the CSVConsumer threads.
        assigns each thread with an even number of stocks at random.
        """
        dotenv.load_dotenv(ENVLOC)
        Save.CSV(self.directory,self.kite).initialise()
        stock_keys = self.stock_list
        No_stocks = len(stock_keys)
        stocksPerConsumer = math.ceil(No_stocks/self.num_consumers)
        threads = []
        stocks = stock_keys # completely redundant, there's absolutely no need to reassign the stocks list, but this is proof this was written by a human low on sleep :)
        assignments = {}
        for i in range(0,No_stocks,stocksPerConsumer):
            cid = math.ceil(i/stocksPerConsumer)
            assignments[cid] = stocks[i:i+stocksPerConsumer]
            thread = threading.Thread(target=self.CSVConsumer,args=(cid,),name=f'CSVCONSUMER_{cid}')
            threads.append(thread)

        with self.rebalance_lock:
            self.consumers = assignments

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
        with self.rebalance_lock:
            logging.info(f"[REBALANCE] Starting rebalancing cycle at {dt.datetime.now()}")
            self.rebalance_in_progress = True

            # Wait until all consumers have paused
            while self.paused_consumers_count < self.num_consumers:
                logging.info(f"[REBALANCE] Waiting for consumers to pause... ({self.paused_consumers_count}/{self.num_consumers})")
                self.rebalance_condition.wait(timeout=10) # Wait for a signal

            logging.info("[REBALANCE] All consumers paused. Rebalancing...")

        # --- Rebalancing Logic (can be outside the main lock) ---
        all_stocks = self.stock_list
        logging.info(f"[REBALANCE] COUNT DONE: {dt.datetime.now()} Number of stocks: {len(all_stocks)}")
        bse = []

        for stock in all_stocks:
            try:
                count = self.r.xlen(stock)
                bse.append((stock, count))
            except Exception as e:
                logging.error(f"[ERROR] Could not get xlen for {stock}: {e}")
        bse.sort(key=lambda x: -x[1])
        logging.info(f"[REBALANCE] COUNT DONE: {dt.datetime.now()} Number of stocks: {len(bse)}")
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
        with self.rebalance_lock:
            self.consumers = dict(new_assignments)
            for cid, stocks in self.consumers.items():
                total_count = totals[cid]  # total count assigned to this consumer
                logging.info(f"[REBALANCE] Assigned {total_count} stocks to consumer {cid}, {stocks}")
        
            self.rebalance_in_progress = False
            self.paused_consumers_count = 0
            self.rebalance_condition.notify_all() # Wake up all waiting consumers
            logging.info("[REBALANCE] Rebalance complete. Consumers resuming.")

    def start_thread_monitor(self, check_interval=6):
        """
        Starts a thread that monitors the CSVConsumer threads.
        
        This function is called when the Consumer object is initialized.
        It starts a thread that monitors the CSVConsumer threads and restarts them if they are down.
        """
        def monitor():
            while self.r.get('end')!='true':
                time.sleep(check_interval)
                active = {t.name for t in threading.enumerate()}
                with self.rebalance_lock:
                    for cid in self.consumers:
                        tname = f"CSVCONSUMER_{cid}"
                        if tname not in active:
                            logging.info(f"[Monitor] {tname}, responsible for :\n\t{self.consumers[cid]}\n is down. Restarting...")

                            thread = threading.Thread(target=self.CSVConsumer, args=(cid,), name=tname)
                            thread.start()
        threading.Thread(target=monitor, daemon=True).start()
    def start_key_event_listener(self):
        """Starts a thread to listen for Redis key events."""
        listener_thread = threading.Thread(target=self.key_event_loop, daemon=True)
        listener_thread.start()
        logging.info("Redis key event listener started.")

    def start_scheduler(self, interval=120):
        """
        Starts a thread that runs the jobscheduler function every hour.
        
        This function is called when the Consumer object is initialized.
        It starts a thread that runs the jobscheduler function every 10 mins.
        """
        def loop():
            while self.r.get('end')!='true' :
                time.sleep(interval)
                self.jobscheduler()
        threading.Thread(target=loop, daemon=True).start()

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

def start_consumer_threads(directory,num_consumers=5):
        """
        Starts the following methods in separate threads within the same process:
        - start_thread_monitor: launches internal monitoring thread(s)
        - start_scheduler: launches internal scheduling thread(s)
        - saveData: launches worker threads for data saving
        """
        consumer = Consumer(directory, num_consumers)

        def run_thread_monitor():
            consumer.start_thread_monitor()

        def run_scheduler():
            consumer.start_scheduler()

        def run_save_data():
            consumer.saveData()

        # Create threads
        t_monitor = threading.Thread(target=run_thread_monitor, name="ThreadMonitorStarter")
        t_scheduler = threading.Thread(target=run_scheduler, name="SchedulerStarter")
        t_save_data = threading.Thread(target=run_save_data, name="SaveDataStarter")


        # Start core threads
        t_monitor.start()
        t_scheduler.start()
        t_save_data.start()
        # Start optional threads
        consumer.start_cleanup_thread()

        return [t_save_data]
