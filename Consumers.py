import redis
import Save
import os
import dotenv
from kiteconnect import KiteConnect
import json

def saveData(directory):
    dotenv.load_dotenv()
    r = redis.Redis(host="localhost",port="6379",db=0,decode_responses=True)
    worker = Save.CSV(directory,os.getenv("STOCKS").strip('[]').split(","),KiteConnect(api_key= os.getenv('APIKEY')) )
    worker.initialise()
    while r.get('end')!='true' : # continuosly reading the incoming stream of data.
        messages = r.xread({'data':'$'},block=100)
        if messages == []:
            continue
        #print(messages)
        for stream in messages:
            for uncoded_msg in stream[1]:
                try:
                    data = json.loads(uncoded_msg[1]['data'])
                    
                    worker.save_tick(data)
                except Exception as e:
                    print(e)
                    return
    print('ending csvWorker')