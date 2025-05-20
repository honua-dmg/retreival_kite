import datetime as dt
import dotenv
import os
import csv
import pandas as pd


class CSV():
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
            tick['exchange_timestamp'],
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