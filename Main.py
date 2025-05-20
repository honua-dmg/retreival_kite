import dotenv
import os
import pandas as pd
from kiteconnect import KiteConnect,KiteTicker
dotenv.load_dotenv()
import datetime as dt
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os
import time
import pyotp

import threading
import time

HEARTBEAT_TIMEOUT = 20  # seconds

# Load environment variables
def getAuth():
    dotenv.load_dotenv()

    api_key = os.getenv('APIKEY')
    api_secret = os.getenv("APISECRET")
    user_id = os.getenv('USERID')
    password = os.getenv('PASSWORD')
    totp_key = os.getenv('TOTPKEY')

    # URL to initiate login
    login_url = f'https://kite.zerodha.com/connect/login?v=3&api_key={api_key}'

    # Setup Chrome
    chrome_options = Options()
    #chrome_options.add_argument('--headless')  # Comment this out to see browser
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    # Path to chromedriver if needed (optional)
    # service = Service("/path/to/chromedriver")
    # driver = webdriver.Chrome(service=service, options=chrome_options)

    # If chromedriver is in PATH
    driver = webdriver.Chrome(options=chrome_options)

    
    driver.get(login_url)
    time.sleep(2)

    # Step 1: Username and password
    driver.find_element(By.ID, "userid").send_keys(user_id)
    driver.find_element(By.ID, "password").send_keys(password)
    driver.find_element(By.XPATH, "//button[@type='submit']").click()

    time.sleep(2)

    # Step 2: TOTP-based 2FA
    totp = pyotp.TOTP(totp_key).now()
    driver.find_element(By.ID, "userid").send_keys(totp)
    #driver.find_element(By.XPATH, "//button[@type='submit']").click()
    time.sleep(.5)

    for i in driver.current_url.split('?')[1].split('&'):
        if i.split('=')[0] == 'request_token':
            request_token = i.split('=')[1]
    driver.close()

    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = data["access_token"]
    return access_token
class Save():
        def __init__(self,directory:str,stonks:list,kite) -> None:
        
            self.dir = directory # to know where we have to save our shit
            self.initialised = False
            self.kite = kite
            dotenv.load_dotenv()
            self.stonks = stonks # ['LTIM',"SBIN",'BAJFINANCE',...]
            self.nse = self.tokenStockMapping("NSE") # {token: stockname NSE}
            self.bse = self.tokenStockMapping("BSE") # {token :stockname BSE}

            self.india_date=dt.datetime.strftime(dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5),"%Y-%m-%d")
        
        def tokenStockMapping(self,exchange):
            df = pd.DataFrame(self.kite.instruments(exchange))
            return dict(zip( df['instrument_token'],df['tradingsymbol']))
        
        def ConvertToken(self,token):
            if token in self.nse.keys():
                return f"NSE:{self.nse[token]}"
            elif token in self.bse.keys():
                return f"BSE:{self.bse[token]}"

        def _initcols(self,file_path):
            """
            args:
                file_path: location of csv file
            initialises columns within newly made csv files 
            
            """
           
            header = [
                'timestamp', 'stonk', 'last_price', 'last_traded_quantity',
                'average_traded_price', 'volume_traded', 'total_buy_quantity', 'total_sell_quantity',
                'open', 'high', 'low', 'close', 'change'
                ]

            # Add depth columns
            for i in range(1, 6):
                header += [f'buy_price_{i}', f'buy_qty_{i}', f'buy_orders_{i}']
                header += [f'sell_price_{i}', f'sell_qty_{i}', f'sell_orders_{i}']

            with open(file_path, mode='a', newline='') as file:
                writer = csv.writer(file)

                # Write header if file is empty
                if file.tell() == 0:
                    writer.writerow(header)

        def initialise(self):
            for stonk in self.stonks:
                #check if directories exist
            

                NSE = os.path.join(self.dir,"NSE",stonk)
                BSE =  os.path.join(self.dir,"BSE",stonk)

                if not os.path.exists(NSE): #checking to see if file path exists
                    os.makedirs(NSE)
                if not os.path.exists(BSE): #checking to see if file path exists
                    os.makedirs(BSE)          
 
                #check if file with type and datestamp is initialised
                # each file will have a symbol and depth file
                file_path_NSE = os.path.join(NSE,f'{self.india_date}.csv')
                file_path_BSE = os.path.join(BSE,f'{self.india_date}.csv')
                self._initcols(file_path_NSE)
                self._initcols(file_path_BSE)


        def save_tick(self,tick):
            exchg,stock = self.ConvertToken(tick['instrument_token']).split(':')
            directory = os.path.join(self.dir,exchg,stock)
            file_path = os.path.join(directory,f'{self.india_date}.csv')
            # get ticker
            row = [
            tick['exchange_timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            tick['instrument_token'],
            tick.get('last_price'),
            tick.get('last_traded_quantity'),
            tick.get('average_traded_price'),
            tick.get('volume_traded'),
            tick.get('total_buy_quantity'),
            tick.get('total_sell_quantity'),
            tick['ohlc']['open'],
            tick['ohlc']['high'],
            tick['ohlc']['low'],
            tick['ohlc']['close'],
            tick.get('change'),
            ]
            # Buy depth
            for level in tick['depth']['buy']:
                row.extend([level['price'], level['quantity'], level['orders']])
            for _ in range(5 - len(tick['depth']['buy'])):
                row.extend([None, None, None])
            
            # Sell depth
            for level in tick['depth']['sell']:
                row.extend([level['price'], level['quantity'], level['orders']])
            for _ in range(5 - len(tick['depth']['sell'])):
                row.extend([None, None, None])

            with open(file_path, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(row)
class Data():
    def __init__(self,access_token,directory):
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
        self.save = Save(directory,self.stocks,self.kite)
        self.save.initialise()
        self.last_tick_time = time.time()
        self.kws = KiteTicker(self.api_key, access_token)
    
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
        global last_tick_time
        for tick in ticks:
            last_tick_time = time.time()
            print(f"{self.ConvertToken(tick['instrument_token'])}: {tick}")
            if 'instrument_token' in tick.keys():
                self.save.save_tick(tick)

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
    # Assign callbacks

    def open(self):
        self.kws.connect(threaded=True)


    # Start WebSocket (blocking call)
    def begin(self):
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_noreconnect = self.on_noreconnect
        self.kws.on_reconnect = self.on_reconnect
        self.kws.connect(threaded=True)
    
        
def heartbeat_monitor(data:Data):
    global last_tick_time
    counter = 0
    while True:
        time.sleep(HEARTBEAT_TIMEOUT)
        now = time.time()
        diff = now - last_tick_time

        if diff > HEARTBEAT_TIMEOUT:
            print(f"💔 No tick for {diff:.1f}s. Attempting reconnect...")
            try:
                data.close()
                time.sleep(2)  # short wait before reconnect
                data.open()
                counter +=1
                print(f'***** counter:{counter}')
                if counter ==10:
                    break
            except Exception as e:
                print(f"⚠️ Reconnect failed: {e}")
        else:
            counter=0
            

if __name__== "__main__":
    authcode= 'uRXmlwg7JzpzKg5qiLe3Ot5QPMDy7M60'
    test = Data(authcode,directory='./test')
    test.begin()
    threading.Thread(target=heartbeat_monitor, daemon=True,args=(test,)).start()    