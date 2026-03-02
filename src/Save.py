"""
CSV persistence module for the Stock Market Data Collection System.

This module handles writing tick data to CSV files organized by
exchange, stock symbol, and date.

Classes:
    - CSV: Manages CSV file creation and tick data persistence

File Structure:
    data/
    ├── NSE/
    │   ├── RELIANCE/
    │   │   ├── 2026-03-01.csv
    │   │   └── 2026-03-02.csv
    │   └── INFY/
    │       └── ...
    └── BSE/
        └── ...
"""

import os
import csv
import datetime as dt
import threading
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo
from tzlocal import get_localzone

from dotenv import load_dotenv

import config
from utils import (
    get_instrument_mapper,
    get_ist_date,
    IST_ZONE
)


# CSV header columns
CSV_HEADER = [
    'timestamp', 'stonk', 'last_price', 'last_traded_quantity',
    'average_traded_price', 'volume_traded', 'total_buy_quantity', 'total_sell_quantity',
    'open', 'high', 'low', 'close', 'change', 'oi', 'oi_day_high', 'oi_day_low'
]

# Add depth columns (5 levels of buy/sell)
for i in range(1, 6):
    CSV_HEADER.extend([f'buy_price_{i}', f'buy_qty_{i}', f'buy_orders_{i}'])
    CSV_HEADER.extend([f'sell_price_{i}', f'sell_qty_{i}', f'sell_orders_{i}'])


class CSV:
    """
    Manages CSV file creation and tick data persistence.
    
    This class handles the storage of real-time tick data to CSV files,
    organized by exchange (NSE/BSE), stock symbol, and date.
    
    Attributes:
        dir (str): Base directory for data storage.
        nse (dict): NSE token-to-symbol mapping.
        bse (dict): BSE token-to-symbol mapping.
        stocks (list): List of stock symbols to track.
        date (str): Current date string (YYYY-MM-DD format).
    """

    def __init__(self, directory: str):
        """
        Initialize the CSV manager.
        
        Args:
            directory: Base directory for data storage.
        """
        load_dotenv(config.ENVLOC)
        
        self.dir = directory
        self.stocks = config.get_stocks_list()
        
        # File locks for thread-safe writes (keyed by file path)
        self._file_locks: Dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()  # Lock to protect the locks dict
        
        # Initialize instrument mapper (handles all token mappings)
        self.mapper = get_instrument_mapper()
        
        # Timezone handling
        self.local_tz = get_localzone()
        self.ist = IST_ZONE
        
        # Date for file naming
        self.date = get_ist_date()

    def _convert_token(self, token: int) -> Optional[str]:
        """
        Convert instrument token to EXCHANGE:SYMBOL format.
        
        Args:
            token: Instrument token.
            
        Returns:
            str: "EXCHANGE:SYMBOL" format (e.g., "NSE:RELIANCE"), or None if not found.
        """
        return self.mapper.convert_token(token)

    def _init_columns(self, file_path: str):
        """
        Initialize CSV file with header row if empty.
        
        Args:
            file_path: Path to the CSV file.
        """
        # Skip if file already has content
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            return
        
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(CSV_HEADER)

    def initialise(self):
        """
        Initialize directory structure and CSV files for all tracked stocks.
        
        Creates the following structure:
            data/
            ├── NSE/
            │   └── {STOCK}/
            │       └── {DATE}.csv
            └── BSE/
                └── {STOCK}/
                    └── {DATE}.csv
        """
        # Create exchange directories
        os.makedirs(os.path.join(self.dir, "NSE"), exist_ok=True)
        os.makedirs(os.path.join(self.dir, "BSE"), exist_ok=True)
        
        for stock in self.stocks:
            # Create stock directories for both exchanges
            nse_dir = os.path.join(self.dir, "NSE", stock)
            bse_dir = os.path.join(self.dir, "BSE", stock)
            
            os.makedirs(nse_dir, exist_ok=True)
            os.makedirs(bse_dir, exist_ok=True)
            
            # Initialize today's CSV files
            nse_file = os.path.join(nse_dir, f'{self.date}.csv')
            bse_file = os.path.join(bse_dir, f'{self.date}.csv')
            
            self._init_columns(nse_file)
            self._init_columns(bse_file)

    def save_tick(self, tick: Dict):
        """
        Save a single tick to the appropriate CSV file.
        
        Args:
            tick: Dictionary containing tick data from KiteTicker.
                  Expected keys: instrument_token, last_price, ohlc, depth, etc.
        """
        # Get exchange and stock from token
        converted = self._convert_token(tick['instrument_token'])
        if not converted:
            return
        
        exchange, stock = converted.split(':')
        
        # Build file path
        file_path = os.path.join(self.dir, exchange, stock, f'{self.date}.csv')
        
        # Ensure directory exists (for dynamically added stocks)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Ensure file has headers
        self._init_columns(file_path)
        
        # Convert timestamp to IST
        timestamp = dt.datetime.now(self.ist).strftime("%H:%M:%S")
        
        # Build row data
        row = [
            timestamp,
            tick['instrument_token'],
            tick.get('last_price'),
            tick.get('last_traded_quantity'),
            tick.get('average_traded_price'),
            tick.get('volume_traded'),
            tick.get('total_buy_quantity'),
            tick.get('total_sell_quantity'),
            tick.get('ohlc', {}).get('open'),
            tick.get('ohlc', {}).get('high'),
            tick.get('ohlc', {}).get('low'),
            tick.get('ohlc', {}).get('close'),
            tick.get('change'),
            tick.get('oi'),
            tick.get('oi_day_high'),
            tick.get('oi_day_low'),
        ]
        
        # Add buy depth (5 levels)
        buy_depth = tick.get('depth', {}).get('buy', [])
        for i in range(5):
            if i < len(buy_depth):
                level = buy_depth[i]
                row.extend([level['price'], level['quantity'], level['orders']])
            else:
                row.extend([None, None, None])
        
        # Add sell depth (5 levels)
        sell_depth = tick.get('depth', {}).get('sell', [])
        for i in range(5):
            if i < len(sell_depth):
                level = sell_depth[i]
                row.extend([level['price'], level['quantity'], level['orders']])
            else:
                row.extend([None, None, None])
        
        # Get or create lock for this file
        with self._locks_lock:
            if file_path not in self._file_locks:
                self._file_locks[file_path] = threading.Lock()
            file_lock = self._file_locks[file_path]
        
        # Write to file with lock
        with file_lock:
            with open(file_path, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(row)
