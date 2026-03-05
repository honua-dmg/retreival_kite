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
from pymemcache.client.base import Client as MemcacheClient
from dotenv import load_dotenv


# ============================================================================
# CONTAINER DETECTION
# ============================================================================

def is_running_in_container() -> bool:
    """
    Detect if we're running inside a Docker container.
    
    Checks:
        1. /.dockerenv file (Docker creates this)
        2. /proc/self/cgroup for docker/containerd references (Linux)
        3. DOCKER_CONTAINER environment variable (explicit override)
    
    Returns:
        bool: True if running in container, False otherwise.
    """
    # Explicit override via environment variable
    if os.getenv("DOCKER_CONTAINER", "").lower() in ("1", "true", "yes"):
        return True
    
    # Check for .dockerenv file
    if os.path.exists("/.dockerenv"):
        return True
    
    # Check cgroup (Linux containers)
    try:
        with open("/proc/self/cgroup", "r") as f:
            content = f.read()
            if "docker" in content or "containerd" in content or "kubepods" in content:
                return True
    except (FileNotFoundError, PermissionError):
        pass
    
    return False


IN_CONTAINER = is_running_in_container()


# ============================================================================
# ENVIRONMENT CONFIGURATION
# ============================================================================

# Default environment file location (container path, can be overridden)
ENVLOC = os.getenv("ENVLOC", "/app/.env" if IN_CONTAINER else ".env")

# Load environment variables
load_dotenv(ENVLOC)

# ============================================================================
# REDIS CONFIGURATION
# ============================================================================

# Auto-detect Redis host: "redis" in Docker, "localhost" locally
_DEFAULT_REDIS_HOST = "redis" if IN_CONTAINER else "localhost"
REDIS_HOST = os.getenv("REDIS_HOST", _DEFAULT_REDIS_HOST)
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Auto-detect Memcached host: "memcached" in Docker, "localhost" locally
_DEFAULT_MEMCACHED_HOST = "memcached" if IN_CONTAINER else "localhost"
MEMCACHED_HOST = os.getenv("MEMCACHED_HOST", _DEFAULT_MEMCACHED_HOST)
MEMCACHED_PORT = int(os.getenv("MEMCACHED_PORT", 11211))

# Key namespace for stock offsets in Memcached
OFFSETS_PREFIX = "stock_offset:"

# Singleton Redis client - use this across all modules
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

# Alias for backward compatibility
r = redis_client

# Singleton Memcached client - use for stock offsets
memcache_client = MemcacheClient(
    (MEMCACHED_HOST, MEMCACHED_PORT),
    connect_timeout=1,
    timeout=1,
    no_delay=True,
)

# ============================================================================
# APPLICATION CONFIGURATION
# ============================================================================

# Data storage path
_DEFAULT_DATA_PATH = "/data" if IN_CONTAINER else os.path.abspath("./data")
DATA_PATH = os.getenv("DATA_PATH", _DEFAULT_DATA_PATH)

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


def is_memcache_connected() -> bool:
    """
    Check if Memcached connection is alive.

    Returns:
        bool: True if connected, False otherwise.
    """
    try:
        memcache_client.set("__healthcheck__", "ok", expire=2)
        value = memcache_client.get("__healthcheck__")
        return value == b"ok"
    except Exception:
        return False
