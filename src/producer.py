import os
from kiteconnect import KiteConnect,KiteTicker
import Auth
import pandas as pd
import time
import redis
import json
import multiprocessing
import Auth
import datetime as dt
import requests
import Report


HEARTBEAT_TIMEOUT = 20
SEND_MAIL_TIMEOUT = 200
class Data():
    """
    A class to manage real-time market data.
    """

    def __init__(self):
        """
        Initializes the Data class with necessary attributes.
        
        """
        self.api_key = os.getenv('APIKEY')
        self.api_secret = os.getenv("APISECRET")
        self.user_id = os.getenv('USERID')
        self.password = os.getenv('PASSWORD')
        self.totp_key = os.getenv('TOTPKEY')
        self.stocks = os.getenv("STOCKS").split(",")
        self.kite = KiteConnect(api_key=self.api_key) 
        nse = self.stockTokenMapping('NSE')
        bse = self.stockTokenMapping('BSE')
        self.tokens = [ nse[x] for x in self.stocks if x in nse.keys()]+ [bse[x] for x in self.stocks if x in bse.keys()] #nse stocks
        self.nse = self.tokenStockMapping("NSE")
        self.bse = self.tokenStockMapping("BSE")
        self.r = redis.Redis(host="redis",port="6379",db=0,decode_responses=True)
        self.access_token = Auth.getAuth()
        # our websocket will be running here
        self.runningThread = None

    def stockTokenMapping(self,exchange):
        """
        Maps stock symbols to their corresponding tokens.
        
        Args:
            exchange (str): The exchange name ('NSE' or 'BSE').
        
        Returns:
            dict: A dictionary mapping stock symbols to their tokens.
        """
        df = pd.DataFrame(self.kite.instruments(exchange))
        return dict(zip( df['tradingsymbol'],df['instrument_token']))

    def tokenStockMapping(self,exchange):
        """
        Maps tokens to their corresponding stock symbols.
        
        Args:
            exchange (str): The exchange name ('NSE' or 'BSE').
        
        Returns:
            dict: A dictionary mapping tokens to their stock symbols.
        """
        df = pd.DataFrame(self.kite.instruments(exchange))
        return dict(zip( df['instrument_token'],df['tradingsymbol']))
    
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
        
    ##### WEBSOCKET FUNCTIONS ######
    def on_ticks(self,ws, ticks):
        """
        Handles the ticks event.
        
        Args:
            ws (KiteTicker): The KiteTicker object.
            ticks (list): The list of ticks.
        """
        for tick in ticks:
            
            #print(f"{self.ConvertToken(tick['instrument_token'])}: {tick}")
            if 'instrument_token' in tick.keys():
                self.r.set('time',dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes= 30))).timestamp())
                tick['tradable'] = ''
                stream = self.ConvertToken(tick['instrument_token']).split(':')[1] # only token not NSE OR BSE will be accounted for. 
                self.r.xadd(stream,{'data':json.dumps(tick,default=str)})

    def on_connect(self,ws, response):
        """
        Handles the connection event.
        
        Args:
            ws (KiteTicker): The KiteTicker object.
            response (dict): The response from the server.
        """
        print("🔗 Connected. Subscribing to tokens...")
        ws.subscribe(self.tokens)
        ws.set_mode(ws.MODE_FULL, self.tokens)  # You can use MODE_QUOTE or MODE_LTP too

    def on_close(self,ws, code, reason):
        """
        Handles the close event.
        
        Args:
            ws (KiteTicker): The KiteTicker object.
            code (int): The close code.
            reason (str): The reason for the close.
        """
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
        """
        Starts the WebSocket connection.
        """
        self.r.set('time',dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes= 30))).timestamp())
        self.kws = KiteTicker(self.api_key, self.access_token)
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_noreconnect = self.on_noreconnect
        self.kws.on_reconnect = self.on_reconnect
        self.kws.connect()



def Producer_worker():
    """
    The main function to start the producer.
    """
    r = redis.Redis(host="redis",port="6379",db=0,decode_responses=True)
    main = Data()
    r.set('end','false')
    try:
        print('starting socket connection')
        main.open()
    except Exception as e:
        print(e)


def InitialiseProducer():
    """
    Initializes the producer process.
    """
    p = multiprocessing.Process(target=Producer_worker)
    p.start()
    return p

def is_connected():
    """
    Checks if the internet connection is available.
    """
    try:
        requests.get("https://www.google.com", timeout=5)
        return True
    except requests.RequestException:
        return False


def heartbeat_monitor():
    """

    Monitors the producer heartbeat and restarts the connection if it fails.
    
    This function starts a producer process and then continuously checks the time
    of the last tick stored in Redis. 
    If the time difference between the current
    time and the last tick time is greater than the HEARTBEAT_TIMEOUT, it assumes
    that the connection has failed and restarts the connection by terminating the
    current process and starting a new one. This process is repeated indefinitely.
    
    This function also sends an email to the user if the connection fails.

    """
    p = InitialiseProducer()
    r = redis.Redis(host="redis",port="6379",db=0,decode_responses=True)
    try:
        last_tick_time = float(r.get('time'))
    except TypeError:
        last_tick_time = dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes= 30))).timestamp()
        r.set('time',last_tick_time)
    
    while True:
        time.sleep(HEARTBEAT_TIMEOUT)
        last_tick_time = float(r.get('time'))
        now = dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes= 30))).timestamp()
        diff = now - last_tick_time

        now_time = dt.datetime.now(dt.timezone(dt.timedelta(hours=5,minutes= 30)))
        if now_time.hour >= 15 and now_time.minute >= 30:
            print("Market closed (past 15:30). Shutting down heartbeat monitor.")
            p.terminate() # shutting the connection down
            print(f"terminating {p}")
            p.join()
            r.set('end','true')
            break

        if diff > HEARTBEAT_TIMEOUT:
            print(f"💔 No tick for {diff:.1f}s. Attempting reconnect...")
            

            if diff>SEND_MAIL_TIMEOUT:
                if is_connected():
                    Report.send_email_alert(
                        subject=f"TIME:{dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),'%Y:%m:%d%H:%M:%S')} KITE WEBSOCKET MALFUNCTION",
                        body="Dear Guru Sai," \
                        "\n I hope you are doing well. It should be brought to your immediate attention that something has gone awry and\n" \
                        "needs your immediate attention.\n" \
                        "Best regards,\n" \
                        "Guru Sai. "
                    )
                else:
                    print(f"TOO MANY RECONNECT ISSUES at time: {dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),'H:%M:%S')}")
                p.terminate() # shutting the connection down
                print(f"terminating {p}")
                p.join()
                r.set('end','true')
                break
            try:
                # resetting the terminal link
                p.terminate()
                
                p.join()
                time.sleep(2)  # short wait before reconnect
                p = InitialiseProducer()
                
                print(f'***** TIME:{diff}: time: {dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),"H:%M:%S")}')
                

            except Exception as e:
                if diff==SEND_MAIL_TIMEOUT/2:
                    if is_connected():
                        Report.send_email_alert(
                            subject=f"TIME:{dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),'%Y:%m:%d%H:%M:%S')}",
                            body=f"Dear Guru Sai," \
                            "\n I hope you are doing well. It should be brought to your immediate attention that something has gone awry and\n" \
                            "needs your immediate attention. The following error has been observed\n " \
                            "{e}\n"\
                            "Best regards,\n" \
                            "Guru Sai. "
                        )

                    else:
                        print(f"internet not connected at time: {dt.datetime.strftime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5.5),'H:%M:%S')}")
                    p.terminate() # shutting the connection down
                    print(f"terminating {p}")
                    p.join()
                    r.set('end','true')
                    break
                print(f"RECONNECT ISSUES: {e}")
                print(f"⚠️ Reconnect failed: {e}")


        
