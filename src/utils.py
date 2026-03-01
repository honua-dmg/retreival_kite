"""
Shared utility functions for the Stock Market Data Collection System.

This module contains common helper functions used across multiple modules,
including token/symbol mapping, timezone handling, and instrument fetching.

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
from typing import Dict, Optional, List
from zoneinfo import ZoneInfo

# ============================================================================
# CONSTANTS
# ============================================================================

# Indian Standard Time timezone
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
IST_ZONE = ZoneInfo("Asia/Kolkata")

# Environment file location (can be overridden)
ENVLOC = os.getenv("ENVLOC", "/app/.env")

# Index futures to track
INDEX_FUTURES = ["SENSEX", "BANKEX", "NIFTY"]

# Script directory for resolving relative paths (works in Docker and locally)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


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
# INSTRUMENT MAPPING UTILITIES
# ============================================================================

def token_to_stock_mapping(exchange: str) -> Dict[int, str]:
    """
    Create a mapping from instrument tokens to trading symbols.
    
    Args:
        exchange: Exchange name ("NSE" or "BSE").
    
    Returns:
        dict: Mapping of instrument_token (int) -> tradingsymbol (str).
    
    Raises:
        FileNotFoundError: If the exchange CSV file doesn't exist.
    """
    csv_path = os.path.join(_SCRIPT_DIR, f"{exchange}.csv")
    df = pd.read_csv(csv_path)
    return dict(zip(df['instrument_token'], df['tradingsymbol']))


def stock_to_token_mapping(exchange: str) -> Dict[str, int]:
    """
    Create a mapping from trading symbols to instrument tokens.
    
    Args:
        exchange: Exchange name ("NSE" or "BSE").
    
    Returns:
        dict: Mapping of tradingsymbol (str) -> instrument_token (int).
    
    Raises:
        FileNotFoundError: If the exchange CSV file doesn't exist.
    """
    csv_path = os.path.join(_SCRIPT_DIR, f"{exchange}.csv")
    df = pd.read_csv(csv_path)
    return dict(zip(df['tradingsymbol'], df['instrument_token']))


def convert_token(token: int, nse_map: Dict[int, str], bse_map: Dict[int, str]) -> Optional[str]:
    """
    Convert an instrument token to "EXCHANGE:SYMBOL" format.
    
    Args:
        token: The instrument token to convert.
        nse_map: NSE token-to-symbol mapping dictionary.
        bse_map: BSE token-to-symbol mapping dictionary.
    
    Returns:
        str: "EXCHANGE:SYMBOL" format (e.g., "NSE:RELIANCE"), or None if not found.
    """
    if token in nse_map:
        return f"NSE:{nse_map[token]}"
    elif token in bse_map:
        return f"BSE:{bse_map[token]}"
    return None


def get_fno_instruments() -> Dict[int, str]:
    """
    Fetch F&O instrument tokens for major indices (SENSEX, BANKEX, NIFTY).
    
    This function calls the Kite API to get the current futures contracts
    for major indices and returns a mapping of their tokens to names.
    
    Args:
        api_key: KiteConnect API key.
        access_token: Valid access token for authentication.
    
    Returns:
        dict: Mapping of instrument_token (int) -> index_name (str).
    
    Raises:
        requests.RequestException: If the API call fails.
    """
    base_url = "https://api.kite.trade/instruments"
    headers = {
        "X-Kite-Version": "3",
    }
    
    response = requests.get(base_url, headers=headers, timeout=30)
    response.raise_for_status()
    
    df = pd.read_csv(io.BytesIO(response.content))
    
    fno_mapping = {}
    for index_name in INDEX_FUTURES:
        filtered = df[(df["name"] == index_name) & (df["instrument_type"] == "FUT")]
        if not filtered.empty:
            token = int(filtered.head(1)['instrument_token'].values[0])
            fno_mapping[token] = index_name
    
    return fno_mapping


def get_all_fno_tokens() -> List[int]:
    """
    Get list of all F&O instrument tokens for tracked indices.
    
    Args:
        api_key: KiteConnect API key.
        access_token: Valid access token for authentication.
    
    Returns:
        list: List of instrument tokens (int).
    """
    fno_map = get_fno_instruments()
    return list(fno_map.keys())


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
