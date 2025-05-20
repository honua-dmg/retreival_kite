import producer
from Consumers import saveData
import redis
import threading

r = redis.Redis(host="localhost",port="6379",db=0,decode_responses=True)
consumer_thread = threading.Thread(target=saveData,args=('./test',))
producer_thread = threading.Thread(target=producer.heartbeat_monitor)
r.set('end','false')
producer_thread.start()
consumer_thread.start()
producer_thread.join()
consumer_thread.join()
