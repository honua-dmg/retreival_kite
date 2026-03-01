"""
Producer module for the Stock Market Data Collection System.

This module handles the WebSocket connection to Zerodha KiteTicker and produces
real-time market tick data to Redis streams for consumption by worker threads.

Classes:
    - TickerProducer: Manages WebSocket connection and tick data publishing

Functions:
    - heartbeat_monitor: Monitors connection health and handles reconnection
    - is_connected: Check internet connectivity
"""

import os
import json
import time
import multiprocessing
import datetime as dt
import requests
from kiteconnect import KiteConnect, KiteTicker

import Auth
import report
import config
from utils import (
    token_to_stock_mapping,
    stock_to_token_mapping,
    convert_token,
    get_fno_instruments,
    get_ist_now,
    get_ist_timestamp,
    IST
)


class TickerProducer:
    """
    Manages real-time market data collection via WebSocket.
    
    This class connects to the Zerodha KiteTicker WebSocket API, subscribes to
    instrument tokens, and publishes incoming tick data to Redis streams.
    
    Attributes:
        api_key (str): KiteConnect API key.
        stocks (list): List of stock symbols to track.
        tokens (list): List of instrument tokens to subscribe to.
        nse (dict): NSE token-to-symbol mapping.
        bse (dict): BSE token-to-symbol mapping.
        access_token (str): Valid Kite access token.
        kws (KiteTicker): WebSocket ticker instance.
    """

    def __init__(self):
        """
        Initialize the TickerProducer with credentials and token mappings.
        
        Loads API credentials from environment, builds token mappings for NSE/BSE,
        and fetches F&O instrument tokens for major indices.
        """
        self.api_key = os.getenv('APIKEY')
        self.api_secret = os.getenv("APISECRET")
        self.stocks = config.get_stocks_list()
        
        # Build token mappings
        nse_stock_to_token = stock_to_token_mapping('NSE')
        bse_stock_to_token = stock_to_token_mapping('BSE')
        
        # Get tokens for configured stocks from both exchanges
        self.tokens = [
            nse_stock_to_token[s] for s in self.stocks if s in nse_stock_to_token
        ] + [
            bse_stock_to_token[s] for s in self.stocks if s in bse_stock_to_token
        ]
        
        # Reverse mappings (token -> symbol)
        self.nse = token_to_stock_mapping("NSE")
        self.bse = token_to_stock_mapping("BSE")
        
        # Redis client
        self.r = config.redis_client
        
        # Get authentication
        self.access_token = Auth.getAuth()
        
        # Add F&O tokens for indices
        self._add_fno_tokens()
        
        # WebSocket instance (created on open)
        self.kws = None

    def _add_fno_tokens(self):
        """Fetch and add F&O tokens for major indices (SENSEX, BANKEX, NIFTY)."""
        try:
            fno_mapping = get_fno_instruments()
            self.nse.update(fno_mapping)
            self.tokens.extend(fno_mapping.keys())
        except Exception as e:
            print(f"⚠️ Failed to fetch F&O instruments: {e}", flush=True)

    def _convert_token(self, token: int) -> str:
        """
        Convert instrument token to EXCHANGE:SYMBOL format.
        
        Args:
            token: Instrument token.
            
        Returns:
            str: "EXCHANGE:SYMBOL" format (e.g., "NSE:RELIANCE").
        """
        return convert_token(token, self.nse, self.bse)

    # =========================================================================
    # WEBSOCKET CALLBACKS
    # =========================================================================

    def on_ticks(self, ws, ticks):
        """
        Handle incoming tick data from WebSocket.
        
        For each tick, updates the heartbeat timestamp and publishes
        the tick data to the appropriate Redis stream.
        
        Args:
            ws: KiteTicker WebSocket instance.
            ticks: List of tick dictionaries.
        """
        for tick in ticks:
            if 'instrument_token' not in tick:
                continue
            
            # Update heartbeat timestamp
            self.r.set('time', get_ist_timestamp())
            
            # Prepare tick for Redis
            tick['tradable'] = ''
            
            # Get stream name (stock symbol without exchange prefix)
            converted = self._convert_token(tick['instrument_token'])
            if not converted:
                continue
            stream = converted.split(':')[1]
            
            # Publish to Redis stream
            self.r.xadd(
                stream,
                {'data': json.dumps(tick, default=str)},
                approximate=True
            )

    def on_connect(self, ws, response):
        """
        Handle WebSocket connection establishment.
        
        Subscribes to all configured instrument tokens in FULL mode
        (includes order book depth data).
        
        Args:
            ws: KiteTicker WebSocket instance.
            response: Server response dictionary.
        """
        print("🔗 Connected. Subscribing to tokens...", flush=True)
        ws.subscribe(self.tokens)
        ws.set_mode(ws.MODE_FULL, self.tokens)

    def on_close(self, ws, code, reason):
        """Handle WebSocket connection close."""
        print(f"❌ Connection closed: {code} - {reason}", flush=True)

    def on_error(self, ws, code, reason):
        """Handle WebSocket errors."""
        print(f"⚠️ Error: {code} - {reason}", flush=True)

    def on_noreconnect(self, ws):
        """Handle when reconnection attempts are exhausted."""
        print("❗ No reconnect will be attempted.", flush=True)

    def on_reconnect(self, ws, attempts_count):
        """Handle reconnection attempts."""
        print(f"🔄 Reconnect attempt #{attempts_count}", flush=True)

    # =========================================================================
    # CONNECTION MANAGEMENT
    # =========================================================================

    def open(self):
        """
        Start the WebSocket connection (blocking call).
        
        Initializes the KiteTicker, registers callbacks, and starts
        the connection. This method blocks until the connection closes.
        """
        self.r.set('time', get_ist_timestamp())
        
        self.kws = KiteTicker(self.api_key, self.access_token)
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_noreconnect = self.on_noreconnect
        self.kws.on_reconnect = self.on_reconnect
        
        self.kws.connect()

    def close(self):
        """Close the WebSocket connection."""
        if self.kws:
            self.kws.close()
            self.kws = None


# =============================================================================
# PRODUCER PROCESS MANAGEMENT
# =============================================================================

def _producer_worker():
    """
    Worker function for the producer process.
    
    Creates a TickerProducer instance and starts the WebSocket connection.
    This runs in a separate process to isolate WebSocket handling.
    """
    config.redis_client.set('end', 'false')
    producer = TickerProducer()
    
    try:
        print('🚀 Starting WebSocket connection...', flush=True)
        producer.open()
    except Exception as e:
        print(f"❌ Producer error: {e}", flush=True)


def _initialize_producer() -> multiprocessing.Process:
    """
    Initialize and start a new producer process.
    
    Returns:
        multiprocessing.Process: The started producer process.
    """
    process = multiprocessing.Process(target=_producer_worker)
    process.start()
    return process


def is_connected() -> bool:
    """
    Check if internet connection is available.
    
    Returns:
        bool: True if connected, False otherwise.
    """
    try:
        requests.get("https://www.google.com", timeout=5)
        return True
    except requests.RequestException:
        return False


def heartbeat_monitor():
    """
    Monitor the producer heartbeat and handle reconnection.
    
    This function runs in the main process and monitors the WebSocket
    connection health by checking the timestamp of the last received tick.
    
    If no tick is received within HEARTBEAT_TIMEOUT seconds, it attempts
    to restart the connection. After SEND_MAIL_TIMEOUT seconds of failures,
    it sends an email alert and terminates.
    
    The monitor also handles graceful shutdown when market hours end (15:30 IST).
    """
    process = _initialize_producer()
    r = config.redis_client
    
    # Initialize heartbeat timestamp
    try:
        last_tick_time = float(r.get('time'))
    except (TypeError, ValueError):
        last_tick_time = get_ist_timestamp()
        r.set('time', last_tick_time)
    
    failure_count = 0
    max_failures = config.SEND_MAIL_TIMEOUT // config.HEARTBEAT_TIMEOUT
    
    while True:
        # Check Redis connectivity
        try:
            r.get('time')
        except Exception:
            print("❌ Redis connection lost. Shutting down.", flush=True)
            process.terminate()
            process.join()
            r.set('end', 'true')
            break
        
        time.sleep(config.HEARTBEAT_TIMEOUT)
        
        # Check if market is closed
        now = get_ist_now()
        if now.hour >= config.MARKET_CLOSE_HOUR and now.minute >= config.MARKET_CLOSE_MINUTE:
            print("📈 Market closed (past 15:30). Shutting down heartbeat monitor.", flush=True)
            process.terminate()
            process.join()
            r.set('end', 'true')
            break
        
        # Check heartbeat
        try:
            last_tick_time = float(r.get('time'))
        except (TypeError, ValueError):
            continue
            
        diff = get_ist_timestamp() - last_tick_time
        
        if diff > config.HEARTBEAT_TIMEOUT:
            failure_count += 1
            print(f"💔 No tick for {diff:.1f}s. Failure count: {failure_count}/{max_failures}", flush=True)
            
            if failure_count >= max_failures:
                # Too many failures - send alert and shutdown
                if is_connected():
                    _send_failure_alert()
                else:
                    print(f"🌐 No internet connection at {now.strftime('%H:%M:%S')}", flush=True)
                
                process.terminate()
                process.join()
                r.set('end', 'true')
                break
            
            # Attempt reconnection
            try:
                process.terminate()
                process.join()
                time.sleep(2)
                process = _initialize_producer()
                print(f"🔄 Reconnected at {now.strftime('%H:%M:%S')}", flush=True)
            except Exception as e:
                print(f"⚠️ Reconnect failed: {e}", flush=True)
        else:
            # Reset failure count on successful tick
            failure_count = 0


def _send_failure_alert():
    """Send email alert for WebSocket connection failure."""
    timestamp = get_ist_now().strftime('%Y-%m-%d %H:%M:%S')
    report.send_email_alert(
        subject=f"🚨 KITE WEBSOCKET MALFUNCTION - {timestamp}",
        body=(
            "Dear User,<br><br>"
            "The Kite WebSocket connection has failed multiple times and requires attention.<br><br>"
            f"<b>Time:</b> {timestamp}<br>"
            "<b>Status:</b> Connection terminated after maximum retry attempts.<br><br>"
            "Please check the system logs for more details.<br><br>"
            "Regards,<br>"
            "Stock Data Collection System"
        )
    )


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    heartbeat_monitor()

