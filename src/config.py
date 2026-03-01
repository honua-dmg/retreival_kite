"""
Configuration module for the Stock Market Data Collection System.

This module provides centralized configuration including:
- Redis connection singleton
- Environment file location
- Common configuration constants

Usage:
    from config import redis_client, ENVLOC, DATA_PATH
"""

import os
import redis
from dotenv import load_dotenv

# ============================================================================
# ENVIRONMENT CONFIGURATION
# ============================================================================

# Default environment file location (container path, can be overridden)
ENVLOC = os.getenv("ENVLOC", "/app/.env")

# Load environment variables
load_dotenv(ENVLOC)

# ============================================================================
# REDIS CONFIGURATION
# ============================================================================

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Singleton Redis client - use this across all modules
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

# Alias for backward compatibility
r = redis_client

# ============================================================================
# APPLICATION CONFIGURATION
# ============================================================================

# Data storage path
DATA_PATH = os.getenv("DATA_PATH", "/app/data")

# Market hours (IST)
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 15
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30

# Monitoring timeouts (seconds)
HEARTBEAT_TIMEOUT = 20
SEND_MAIL_TIMEOUT = 80

# Consumer configuration
DEFAULT_NUM_CONSUMERS = 5
CLEANUP_INTERVAL = 10  # seconds
CLEANUP_LAG = 100  # messages to keep in stream


def get_stocks_list() -> list:
    """
    Get the list of stocks to track from environment variables.
    
    Returns:
        list: List of stock symbols.
    """
    stocks_str = os.getenv("STOCKS", "")
    return [s.strip() for s in stocks_str.split(",") if s.strip()]


def is_redis_connected() -> bool:
    """
    Check if Redis connection is alive.
    
    Returns:
        bool: True if connected, False otherwise.
    """
    try:
        redis_client.ping()
        return True
    except redis.ConnectionError:
        return False
