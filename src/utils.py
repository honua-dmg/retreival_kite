"""
Shared utility functions for the Stock Market Data Collection System.

This module contains common helper functions used across multiple modules,
including token/symbol mapping, timezone handling, and instrument fetching.

Classes:
    - InstrumentMapper: Smart caching instrument mapper with expiry handling

Functions:
    - get_ist_now: Get current datetime in IST timezone
    - get_ist_date: Get current date string in IST timezone
    - token_to_stock_mapping: Map instrument tokens to trading symbols
    - stock_to_token_mapping: Map trading symbols to instrument tokens
    - convert_token: Convert instrument token to "EXCHANGE:SYMBOL" format
    - get_fno_instruments: Fetch F&O instrument tokens for indices
"""

import datetime as dt
import os
import pandas as pd
import requests
import io
from typing import Dict, Optional, List, Tuple
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# ============================================================================
# CONSTANTS
# ============================================================================

# Indian Standard Time timezone
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
IST_ZONE = ZoneInfo("Asia/Kolkata")

# Script directory for resolving relative paths (works in Docker and locally)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _detect_env_file() -> str:
    """Auto-detect .env file location (local dev vs Docker)."""
    # Check for explicit override
    if os.getenv("ENVLOC"):
        return os.getenv("ENVLOC")
    
    # Check local .env in workspace root (parent of src/)
    local_env = os.path.join(os.path.dirname(_SCRIPT_DIR), ".env")
    if os.path.exists(local_env):
        return local_env
    
    # Check .env in current directory
    if os.path.exists(".env"):
        return ".env"
    
    # Default to Docker path
    return "/app/.env"


# Environment file location (auto-detected)
ENVLOC = _detect_env_file()

# Index futures to track (these need expiry handling)
INDEX_FUTURES = ["SENSEX", "BANKEX", "NIFTY"]

# Map index names to their display exchange (for file storage)
# NIFTY futures trade on NFO but we store under NSE
# SENSEX/BANKEX futures trade on BFO but we store under BSE
INDEX_EXCHANGE_MAP = {
    "NIFTY": "NSE",
    "SENSEX": "BSE",
    "BANKEX": "BSE",
}

# F&O exchanges
FNO_EXCHANGES = {"NFO", "BFO"}  # NSE F&O and BSE F&O

# Zerodha instruments API endpoint (public, no auth required)
INSTRUMENTS_URL = "https://api.kite.trade/instruments"


# ============================================================================
# TIMEZONE UTILITIES
# ============================================================================

def get_ist_now() -> dt.datetime:
    """
    Get the current datetime in Indian Standard Time (IST).
    
    Returns:
        datetime: Current datetime with IST timezone.
    """
    return dt.datetime.now(IST)


def get_ist_date(fmt: str = "%Y-%m-%d") -> str:
    """
    Get the current date string in Indian Standard Time (IST).
    
    Args:
        fmt: strftime format string. Defaults to "%Y-%m-%d".
    
    Returns:
        str: Formatted date string in IST.
    """
    return get_ist_now().strftime(fmt)


def get_ist_timestamp() -> float:
    """
    Get the current Unix timestamp in IST.
    
    Returns:
        float: Unix timestamp.
    """
    return get_ist_now().timestamp()


 
# ============================================================================
# REDIS STREAM UTILITIES
# ============================================================================

def next_redis_stream_id(msg_id: str) -> str:
    """
    Increment a Redis stream message ID by 1.
    
    Redis stream IDs are in format "timestamp-sequence". This function
    increments the sequence number by 1.
    
    Args:
        msg_id: Redis stream message ID (e.g., "1234567890-0").
    
    Returns:
        str: Incremented message ID (e.g., "1234567890-1").
    """
    if not msg_id or '-' not in msg_id:
        return "0-0"
    ts, seq = map(int, msg_id.split('-'))
    return f"{ts}-{seq + 1}"


# ============================================================================
# INSTRUMENT MAPPER CLASS
# ============================================================================

class InstrumentMapper:
    """
    Smart caching instrument mapper with automatic expiry handling.
    
    This class manages a local cache of instrument tokens and handles:
    - Token <-> Symbol mapping for stocks
    - F&O contract expiry detection and rollover
    - Automatic cache refresh when data is stale
    
    Attributes:
        cache_file (str): Path to the local CSV cache file.
        instruments_df (pd.DataFrame): Cached instruments data.
    """
    
    def __init__(self, cache_file: str = None):
        """
        Initialize the InstrumentMapper.
        
        Args:
            cache_file: Path to cache file. Defaults to src/instruments_cache.csv
        """
        if cache_file is None:
            cache_file = os.path.join(_SCRIPT_DIR, "instruments_cache.csv")
        
        self.cache_file = cache_file
        self.instruments_df = pd.DataFrame()
        self._load_env()
        self._load_cache()
    
    def _load_env(self):
        """Load environment variables."""
        load_dotenv(ENVLOC)
        self.stocks = self._get_stocks_list()
    
    def _get_stocks_list(self) -> List[str]:
        """Get configured stocks from environment."""
        stocks_str = os.getenv("STOCKS", "")
        return [s.strip() for s in stocks_str.split(",") if s.strip()]
    
    def _load_cache(self):
        """Load cached instruments from local CSV file."""
        if os.path.exists(self.cache_file):
            try:
                self.instruments_df = pd.read_csv(self.cache_file)
                # Ensure expiry is parsed as date
                if 'expiry' in self.instruments_df.columns:
                    self.instruments_df['expiry'] = pd.to_datetime(
                        self.instruments_df['expiry'], errors='coerce'
                    )
                print(f"✅ Loaded {len(self.instruments_df)} instruments from cache")
            except Exception as e:
                print(f"⚠️ Failed to load cache: {e}")
                self.instruments_df = pd.DataFrame()
        else:
            print(f"📁 No cache file found at {self.cache_file}")
    
    def _save_cache(self):
        """Save instruments to local CSV cache file."""
        if not self.instruments_df.empty:
            self.instruments_df.to_csv(self.cache_file, index=False)
            print(f"💾 Saved {len(self.instruments_df)} instruments to cache")
    
    def _fetch_all_instruments(self) -> pd.DataFrame:
        """
        Fetch all instruments from Zerodha API.
        
        Returns:
            pd.DataFrame: All available instruments.
        """
        print("🌐 Fetching instruments from Zerodha API...")
        headers = {"X-Kite-Version": "3"}
        
        response = requests.get(INSTRUMENTS_URL, headers=headers, timeout=30)
        response.raise_for_status()
        
        df = pd.read_csv(io.BytesIO(response.content))
        print(f"📥 Fetched {len(df)} instruments from Zerodha")
        return df
    
    def needs_refresh(self) -> Tuple[bool, List[str]]:
        """
        Check if cache needs refresh.
        
        Returns:
            Tuple of (needs_refresh: bool, missing_symbols: list)
        """
        if self.instruments_df.empty:
            return True, ["ALL"]
        
        today = dt.date.today()
        missing = []
        
        # Check each configured stock
        for stock in self.stocks:
            stock = stock.strip()
            if not stock:
                continue
            
            if stock in INDEX_FUTURES:
                # For indices, check if we have valid future expiries
                index_futures = self.instruments_df[
                    (self.instruments_df['name'] == stock) & 
                    (self.instruments_df['instrument_type'] == 'FUT')
                ]
                
                if index_futures.empty:
                    missing.append(stock)
                    continue
                
                # Check for at least 2 future expiries
                future_expiries = index_futures[
                    pd.to_datetime(index_futures['expiry']).dt.date >= today
                ]
                
                if len(future_expiries) < 2:
                    missing.append(stock)
            else:
                # For regular stocks, check if symbol exists
                exists = (
                    (self.instruments_df['tradingsymbol'] == stock) & 
                    (self.instruments_df['exchange'].isin(['NSE', 'BSE']))
                ).any()
                
                if not exists:
                    missing.append(stock)
        
        return len(missing) > 0, missing
    
    def refresh(self, force: bool = False):
        """
        Refresh instruments cache from Zerodha API.
        
        Args:
            force: If True, refresh even if cache is valid.
        """
        needs_refresh, missing = self.needs_refresh()
        
        if not needs_refresh and not force:
            print("✅ Cache is up to date, no refresh needed")
            return
        
        if missing:
            print(f"🔄 Refreshing cache for: {missing}")
        
        # Fetch all instruments
        all_instruments = self._fetch_all_instruments()
        
        filtered_rows = []
        
        for stock in self.stocks:
            stock = stock.strip()
            if not stock:
                continue
            
            if stock in INDEX_FUTURES:
                # Get FUT contracts for indices
                fut_df = all_instruments[
                    (all_instruments['name'] == stock) & 
                    (all_instruments['instrument_type'] == 'FUT')
                ]
                if not fut_df.empty:
                    filtered_rows.append(fut_df)
                    print(f"  📈 {stock}: Found {len(fut_df)} futures contracts")
            else:
                # Get equity for stocks from both NSE and BSE
                eq_df = all_instruments[
                    (all_instruments['tradingsymbol'] == stock) & 
                    (all_instruments['exchange'].isin(['NSE', 'BSE']))
                ]
                if not eq_df.empty:
                    filtered_rows.append(eq_df)
                    print(f"  📊 {stock}: Found on {eq_df['exchange'].tolist()}")
        
        if filtered_rows:
            self.instruments_df = pd.concat(filtered_rows, ignore_index=True)
            self._save_cache()
            self.invalidate_cache()  # Clear token cache after refresh
        else:
            print("⚠️ No matching instruments found")
    
    def get_token(self, tradingsymbol: str, exchange: str = None) -> Optional[int]:
        """
        Get instrument token for a trading symbol.
        
        Args:
            tradingsymbol: The trading symbol (e.g., "RELIANCE").
            exchange: Optional exchange filter ("NSE", "BSE", "NFO", "BFO").
        
        Returns:
            int: Instrument token, or None if not found.
        """
        df = self.instruments_df
        
        if exchange:
            df = df[df['exchange'] == exchange]
        
        match = df[df['tradingsymbol'] == tradingsymbol]
        
        if match.empty:
            return None
        
        return int(match.iloc[0]['instrument_token'])
    
    def get_symbol(self, instrument_token: int) -> Optional[Dict]:
        """
        Get symbol info for an instrument token.
        
        Args:
            instrument_token: The instrument token.
        
        Returns:
            dict: Symbol info with keys: tradingsymbol, exchange, name, instrument_type
        """
        match = self.instruments_df[
            self.instruments_df['instrument_token'] == instrument_token
        ]
        
        if match.empty:
            return None
        
        row = match.iloc[0]
        return {
            'tradingsymbol': row['tradingsymbol'],
            'exchange': row['exchange'],
            'name': row.get('name', row['tradingsymbol']),
            'instrument_type': row.get('instrument_type', 'EQ')
        }
    
    def get_current_month_future(self, index: str) -> Optional[Dict]:
        """
        Get current month's future contract for an index.
        
        Automatically handles expiry rollover:
        - Returns nearest expiring contract
        - After 3:30 PM on expiry day, returns next month's contract
        
        Args:
            index: Index name ("NIFTY", "SENSEX", "BANKEX").
        
        Returns:
            dict: Contract info with keys: instrument_token, tradingsymbol, exchange, expiry
        """
        today = dt.date.today()
        now = dt.datetime.now(IST_ZONE)
        
        # Filter for this index's futures
        futures = self.instruments_df[
            (self.instruments_df['name'] == index) & 
            (self.instruments_df['instrument_type'] == 'FUT')
        ].copy()
        
        if futures.empty:
            print(f"⚠️ No futures found for {index}")
            return None
        
        # Parse expiry dates
        futures['expiry_date'] = pd.to_datetime(futures['expiry']).dt.date
        futures = futures.sort_values('expiry_date')
        
        # Get futures expiring today or later
        valid_futures = futures[futures['expiry_date'] >= today]
        
        if valid_futures.empty:
            print(f"⚠️ No valid futures found for {index}")
            return None
        
        current_future = valid_futures.iloc[0]
        
        # If today is expiry day and market is closed (after 3:30 PM IST)
        if current_future['expiry_date'] == today:
            if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
                if len(valid_futures) > 1:
                    current_future = valid_futures.iloc[1]
                    print(f"📅 {index}: Expiry day rollover to next month")
                else:
                    print(f"⚠️ {index}: No next month future available")
                    return None
        
        return {
            'instrument_token': int(current_future['instrument_token']),
            'tradingsymbol': current_future['tradingsymbol'],
            'exchange': current_future['exchange'],
            'expiry': str(current_future['expiry_date']),
            'name': index
        }
    
    def get_all_tokens(self) -> List[int]:
        """
        Get all instrument tokens for subscribed stocks.
        
        For stocks: Returns NSE and BSE tokens.
        For indices: Returns only current month's future token.
        
        Returns:
            list: List of instrument tokens.
        """
        tokens = []
        
        for stock in self.stocks:
            stock = stock.strip()
            if not stock:
                continue
            
            if stock in INDEX_FUTURES:
                # Get current month future
                future = self.get_current_month_future(stock)
                if future:
                    tokens.append(future['instrument_token'])
            else:
                # Get equity tokens from both exchanges
                stock_df = self.instruments_df[
                    (self.instruments_df['tradingsymbol'] == stock) & 
                    (self.instruments_df['exchange'].isin(['NSE', 'BSE']))
                ]
                tokens.extend(stock_df['instrument_token'].astype(int).tolist())
        
        return tokens
    
    def get_token_to_symbol_mapping(self) -> Dict[int, str]:
        """
        Get mapping of token -> "EXCHANGE:SYMBOL" for all tracked instruments.
        
        For indices, uses the index name and mapped exchange (NSE for NIFTY, BSE for SENSEX/BANKEX).
        
        Returns:
            dict: Mapping of instrument_token -> "EXCHANGE:SYMBOL"
        """
        mapping = {}
        
        for stock in self.stocks:
            stock = stock.strip()
            if not stock:
                continue
            
            if stock in INDEX_FUTURES:
                future = self.get_current_month_future(stock)
                if future:
                    # Use mapped exchange (NSE for NIFTY, BSE for SENSEX/BANKEX)
                    display_exchange = INDEX_EXCHANGE_MAP.get(stock, "NSE")
                    mapping[future['instrument_token']] = f"{display_exchange}:{stock}"
            else:
                stock_df = self.instruments_df[
                    (self.instruments_df['tradingsymbol'] == stock) & 
                    (self.instruments_df['exchange'].isin(['NSE', 'BSE']))
                ]
                for _, row in stock_df.iterrows():
                    token = int(row['instrument_token'])
                    mapping[token] = f"{row['exchange']}:{row['tradingsymbol']}"
        
        return mapping
    
    def convert_token(self, token: int) -> Optional[str]:
        """
        Convert an instrument token to "EXCHANGE:SYMBOL" format.
        
        This is the unified method for token conversion, handling both
        regular stocks and F&O instruments.
        
        Args:
            token: The instrument token to convert.
        
        Returns:
            str: "EXCHANGE:SYMBOL" format (e.g., "NSE:RELIANCE", "BSE:SENSEX"),
                 or None if not found.
        """
        # Check cache first
        if not hasattr(self, '_token_cache'):
            self._token_cache = self.get_token_to_symbol_mapping()
        
        if token in self._token_cache:
            return self._token_cache[token]
        
        # Try to find in instruments_df
        match = self.instruments_df[
            self.instruments_df['instrument_token'] == token
        ]
        
        if match.empty:
            return None
        
        row = match.iloc[0]
        exchange = row['exchange']
        
        # For F&O instruments, map to display exchange
        if exchange in FNO_EXCHANGES:
            name = row.get('name', '')
            if name in INDEX_EXCHANGE_MAP:
                return f"{INDEX_EXCHANGE_MAP[name]}:{name}"
        
        return f"{exchange}:{row['tradingsymbol']}"
    
    def invalidate_cache(self):
        """Invalidate the token cache (call after refresh)."""
        if hasattr(self, '_token_cache'):
            del self._token_cache
    
    def summary(self):
        """Print a summary of cached instruments."""
        if self.instruments_df.empty:
            print("📭 Cache is empty")
            return
        
        print("\n" + "="*60)
        print("📋 INSTRUMENT MAPPER SUMMARY")
        print("="*60)
        
        today = dt.date.today()
        
        # Stocks
        stocks = [s for s in self.stocks if s not in INDEX_FUTURES]
        print(f"\n📊 STOCKS ({len(stocks)}):")
        for stock in stocks:
            stock_df = self.instruments_df[
                (self.instruments_df['tradingsymbol'] == stock) & 
                (self.instruments_df['exchange'].isin(['NSE', 'BSE']))
            ]
            if not stock_df.empty:
                exchanges = stock_df['exchange'].tolist()
                tokens = stock_df['instrument_token'].tolist()
                print(f"   {stock}: {', '.join([f'{e}:{t}' for e, t in zip(exchanges, tokens)])}")
            else:
                print(f"   {stock}: ❌ NOT FOUND")
        
        # Indices
        indices = [s for s in self.stocks if s in INDEX_FUTURES]
        print(f"\n📈 INDEX FUTURES ({len(indices)}):")
        for index in indices:
            future = self.get_current_month_future(index)
            if future:
                print(f"   {index}: {future['tradingsymbol']} (Token: {future['instrument_token']}, Expiry: {future['expiry']})")
            else:
                print(f"   {index}: ❌ NO VALID FUTURES")
        
        print("\n" + "="*60)
        print(f"💾 Cache file: {self.cache_file}")
        print(f"📅 Today: {today}")
        print("="*60 + "\n")


# Global singleton instance (lazy initialization)
_mapper_instance: Optional[InstrumentMapper] = None


def get_instrument_mapper() -> InstrumentMapper:
    """
    Get the global InstrumentMapper singleton instance.
    
    Returns:
        InstrumentMapper: The singleton instance.
    """
    global _mapper_instance
    if _mapper_instance is None:
        _mapper_instance = InstrumentMapper()
    return _mapper_instance


# ============================================================================
# DEMO / TEST
# ============================================================================

if __name__ == "__main__":
    print("\n🚀 InstrumentMapper Demo\n")
    
    # Create mapper
    mapper = InstrumentMapper()
    
    # Check if refresh needed
    needs_refresh, missing = mapper.needs_refresh()
    print(f"Needs refresh: {needs_refresh}")
    if missing:
        print(f"Missing: {missing}")
    
    # Refresh if needed
    if needs_refresh:
        mapper.refresh()
    
    # Show summary
    mapper.summary()
    
    # Get all tokens
    print("\n🎯 All Tokens for Subscription:")
    tokens = mapper.get_all_tokens()
    print(f"   {tokens}")
    
    # Get token to symbol mapping
    print("\n🗺️ Token -> Symbol Mapping:")
    mapping = mapper.get_token_to_symbol_mapping()
    for token, symbol in list(mapping.items())[:10]:  # Show first 10
        print(f"   {token} -> {symbol}")
    if len(mapping) > 10:
        print(f"   ... and {len(mapping) - 10} more")
