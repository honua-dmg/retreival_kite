import os
import dotenv
from kiteconnect import KiteConnect,KiteTicker
import Save
import Auth
import pandas as pd
import time
import threading
import redis
import json
import multiprocessing
import Auth
import datetime as dt

import smtplib
import ssl
import os
from email.message import EmailMessage
from dotenv import load_dotenv


HEARTBEAT_TIMEOUT = 20
class Data():
    def __init__(self):
        self.api_key = os.getenv('APIKEY')
        self.api_secret = os.getenv("APISECRET")
        self.user_id = os.getenv('USERID')
        self.password = os.getenv('PASSWORD')
        self.totp_key = os.getenv('TOTPKEY')
        self.stocks = os.getenv("STOCKS").strip('[]').split(",")
        self.kite = KiteConnect(api_key=self.api_key) 
        nse = self.stockTokenMapping('NSE')
        bse = self.stockTokenMapping('BSE')
        self.tokens = [ nse[x] for x in self.stocks]+ [bse[x] for x in self.stocks] #nse stocks
        self.nse = self.tokenStockMapping("NSE")
        self.bse = self.tokenStockMapping("BSE")
        self.r = redis.Redis(host="localhost",port="6379",db=0,decode_responses=True)
        self.access_token = Auth.getAuth()
        # our websocket will be running here
        self.runningThread = None
    
    def stockTokenMapping(self,exchange):
        df = pd.DataFrame(self.kite.instruments(exchange))
        return dict(zip( df['tradingsymbol'],df['instrument_token']))

    def tokenStockMapping(self,exchange):
        df = pd.DataFrame(self.kite.instruments(exchange))
        return dict(zip( df['instrument_token'],df['tradingsymbol']))
    
    def ConvertToken(self,token):
        if token in self.nse.keys():
            return f"NSE:{self.nse[token]}"
        elif token in self.bse.keys():
            return f"BSE:{self.bse[token]}"
        
    ##### WEBSOCKET FUNCTIONS ######
    def on_ticks(self,ws, ticks):
        self.r.set('time',time.time())
        for tick in ticks:
            
            #print(f"{self.ConvertToken(tick['instrument_token'])}: {tick}")
            if 'instrument_token' in tick.keys():
                tick['tradable'] = ''
                self.r.xadd("data",{'data':json.dumps(tick,default=str)})
                #self.save.save_tick(tick)

    def on_connect(self,ws, response):
        print("🔗 Connected. Subscribing to tokens...")
        ws.subscribe(self.tokens)
        ws.set_mode(ws.MODE_FULL, self.tokens)  # You can use MODE_QUOTE or MODE_LTP too

    def on_close(self,ws, code, reason):
        print("❌ Connection closed:", code, reason)

    def on_error(self,ws, code, reason):
        print("⚠️ Error:", code, reason)

    def on_noreconnect(self,ws):
        print("❗ No reconnect will be attempted.")

    def on_reconnect(self,ws, attempts_count):
        print(f"🔄 Reconnect attempt #{attempts_count}")

    #test this.
    def subscribe(self):
        self.kws.subscribe(self.tokens)

    #test this.
    def unsubscribe(self):
        self.kws.unsubscribe(self.tokens)

    def close(self):
        self.kws.close()
        del self.kws
        #self.runningThread.join()
        
    
    # Start WebSocket (blocking call)
    def open(self):
        self.r.set('time',time.time())
        self.kws = KiteTicker(self.api_key, self.access_token)
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_noreconnect = self.on_noreconnect
        self.kws.on_reconnect = self.on_reconnect
        self.kws.connect()



def Producer_worker():
    r = redis.Redis(host="localhost",port="6379",db=0,decode_responses=True)
    main = Data()
    r.set('end','false')
    try:
        print('starting socket connection')
        main.open()
    except Exception as e:
        print(e)


def InitialiseProducer():
    p = multiprocessing.Process(target=Producer_worker)
    p.start()
    return p



def send_email_alert(subject, body):
    email_address = os.getenv("EMAIL_ADDRESS")
    email_password = os.getenv("EMAIL_PASSWORD")
    to_email = os.getenv("TO_EMAIL")

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = email_address
    msg['To'] = to_email
    msg.set_content(body)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(email_address, email_password)
            smtp.send_message(msg)
        print("✅ Email sent successfully.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

def heartbeat_monitor():
    p = InitialiseProducer()
    r = redis.Redis(host="localhost",port="6379",db=0,decode_responses=True)
    try:
        last_tick_time = float(r.get('time'))
    except TypeError:
        last_tick_time = time.time()
        r.set('time',last_tick_time)
    counter = 0
    while True:
        time.sleep(HEARTBEAT_TIMEOUT)
        last_tick_time = float(r.get('time'))
        now = time.time()
        diff = now - last_tick_time

        now_time = dt.datetime.now()
        if now_time.hour >= 15 and now_time.minute >= 30:
            print("Market closed (past 15:30). Shutting down heartbeat monitor.")
            p.terminate() # shutting the connection down
            print(f"terminating {p}")
            p.join()
            break

        if diff > HEARTBEAT_TIMEOUT:
            print(f"💔 No tick for {diff:.1f}s. Attempting reconnect...")
            try:
                p.terminate()
                print(f"terminating {p}")
                p.join()
                time.sleep(2)  # short wait before reconnect
                p = InitialiseProducer()
                counter +=1
                print(f'***** counter:{counter}')
                

                if counter ==10:
                    send_email_alert(
                        subject=f"TIME:{dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y:%m:%d%H:%M:%S")} KITE WEBSOCKET MALFUNCTION",
                        body="Dear Guru Sai," \
                        "\n I hope you are doing well. It should be brought to your immediate attention that something has gone awry and\n" \
                        "needs your immediate attention.\n" \
                        "Best regards,\n" \
                        "Guru Sai. "
                    )
                    break
            except Exception as e:
                if counter==5:
                    send_email_alert(
                        subject=f"TIME:{dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y:%m:%d%H:%M:%S")}",
                        body=f"Dear Guru Sai," \
                        "\n I hope you are doing well. It should be brought to your immediate attention that something has gone awry and\n" \
                        "needs your immediate attention. The following error has been observed\n " \
                        "{e}\n"\
                        "Best regards,\n" \
                        "Guru Sai. "
                    )
                print(f"⚠️ Reconnect failed: {e}")
        else:
            counter=0
            
            