import redis
import Save
import os
import dotenv
from kiteconnect import KiteConnect
import json
import threading
import pandas as pd
class Consumer():
    def __init__(self,directory):
        dotenv.load_dotenv()
        self.directory = directory
        self.api_key = os.getenv('APIKEY')
        self.kite = KiteConnect(api_key=self.api_key) 
        self.nse = self.tokenStockMapping("NSE")
        self.bse = self.tokenStockMapping("BSE")
        self.streams = {x:"0" for x in os.getenv("STOCKS").split(",")}
        self.consumers = {}
    
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
        r = redis.Redis(host="localhost",port="6379",db=0,decode_responses=True)
        worker = Save.CSV(self.directory,self.kite )
        
        
        
        while r.get('end')!='true' : # continuosly reading the incoming stream of data.
            streams = {x:"0" for x in self.consumers[id]}
            messages = r.xread(streams,block=100)
            if messages == []:
                continue
            #print(messages)
            for stream in messages:
                for uncoded_msg in stream[1]:
                    msg_id = uncoded_msg[0]

                    try:
                        data = json.loads(uncoded_msg[1]['data'])
                        stock_name = self.ConvertToken(data['instrument_token']).split(':')[1]
                        streams[stock_name] = msg_id
                        worker.save_tick(data)
                    except Exception as e:
                        print(f"[ERROR] Failed to process tick: {e}")
                        continue
        print('ending csvWorker')


    def saveData(self,N):
        dotenv.load_dotenv()
        Save.CSV(self.directory,KiteConnect(api_key= os.getenv('APIKEY')) ).initialise()
        No_stocks = len(os.getenv("STOCKS").split(","))
        stocksPerConsumer = No_stocks//N
        threads = []
        for i in range(0,No_stocks,stocksPerConsumer):
            self.consumers[i] = self.streams.keys()[i:i+stocksPerConsumer]
            thread = threading.Thread(target=self.CSVConsumer,args=(i),name=f'CSVCONSUMER_{i/stocksPerConsumer}')
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()
        