"""
Consumer module for the Stock Market Data Collection System.

This module provides multi-threaded consumers that read tick data from Redis
streams and persist them to CSV files organized by exchange/stock/date.

Classes:
    - Consumer: Manages consumer threads, load balancing, and data persistence

Functions:
    - start_consumer_threads: Initialize and start all consumer threads
"""

import os
import json
import time
import math
import threading
import datetime as dt
from collections import defaultdict
from typing import Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv

import Save
import report
import config
from utils import (
    get_instrument_mapper,
    next_redis_stream_id,
    get_ist_date,
    IST
)


class Consumer:
    """
    Manages multi-threaded consumption of tick data from Redis streams.
    
    This class coordinates multiple consumer threads that read from Redis streams,
    handles load balancing across stocks, monitors thread health, and manages
    stream cleanup to prevent memory bloat.
    
    Attributes:
        directory (str): Base directory for CSV file storage.
        num_consumers (int): Number of consumer threads to run.
        nse (dict): NSE token-to-symbol mapping.
        bse (dict): BSE token-to-symbol mapping.
        consumers (dict): Mapping of consumer_id -> list of assigned stocks.
        date (str): Current date string for file naming.
    """

    def __init__(self, directory: str, num_consumers: int):
        """
        Initialize the Consumer manager.
        
        Args:
            directory: Base directory for CSV file storage.
            num_consumers: Number of consumer threads to run.
        """
        load_dotenv(config.ENVLOC)
        
        self.directory = directory
        self.num_consumers = num_consumers
        
        # Initialize instrument mapper (handles all token mappings)
        self.mapper = get_instrument_mapper()
        
        # Refresh if needed
        needs_refresh, _ = self.mapper.needs_refresh()
        if needs_refresh:
            self.mapper.refresh()
        
        # Thread coordination
        self.consumers: Dict[int, List[str]] = {}
        self.consumer_lock = threading.Lock()
        
        # Barrier-based synchronization for rebalancing
        # +1 for the rebalancer thread itself
        self.pause_barrier = threading.Barrier(num_consumers + 1)
        self.resume_barrier = threading.Barrier(num_consumers + 1)
        self.rebalance_requested = threading.Event()
        self.barrier_timeout = 10  # seconds to wait for all consumers

        # Date for file naming
        self.date = get_ist_date()
        
        # Redis client
        self.r = config.redis_client
        self.m = config.memcache_client
        self.r.set('end', 'false')

        self.tracked_stocks = config.get_stocks_list()
        
        # Initialize offsets in Memcached
        self._init_offsets_store()
        
        # Cleanup configuration
        self.cleanup_interval = config.CLEANUP_INTERVAL
        self.cleanup_lag = config.CLEANUP_LAG

    def _offset_key(self, stock: str) -> str:
        """Build Memcached key for stock offset."""
        return f"{config.OFFSETS_PREFIX}{stock}"

    def _decode_memcache_value(self, value) -> Optional[str]:
        """Decode Memcached values into str offsets."""
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    def _init_offsets_store(self):
        """
        Initialize stock offsets in Memcached.
        """
        if not self.tracked_stocks:
            return

        print("[INFO] Initializing offset store in Memcached...", flush=True)

        for stock in self.tracked_stocks:
            try:
                mem_key = self._offset_key(stock)
                mem_val = self._decode_memcache_value(self.m.get(mem_key))

                if mem_val is None:
                    self.m.set(mem_key, "0")
            except Exception as e:
                print(f"[WARN] Failed to initialize offset for {stock}: {e}", flush=True)

    def _get_offsets(self, stocks: List[str]) -> List[Optional[str]]:
        """
        Get offsets for stocks from Memcached.

        Args:
            stocks: Stock symbols.

        Returns:
            list: Offset list aligned to input stocks.
        """
        offsets: List[Optional[str]] = []

        for stock in stocks:
            offset = None
            mem_key = self._offset_key(stock)

            try:
                offset = self._decode_memcache_value(self.m.get(mem_key))
            except Exception as e:
                print(f"[WARN] Memcached read failed for {stock}: {e}", flush=True)

            if offset is None:
                offset = "0"
                try:
                    self.m.set(mem_key, offset)
                except Exception as e:
                    print(f"[WARN] Failed to seed missing offset for {stock}: {e}", flush=True)

            offsets.append(offset)

        return offsets

    def _set_offset(self, stock: str, msg_id: str):
        """Persist offset to Memcached."""
        try:
            self.m.set(self._offset_key(stock), msg_id)
        except Exception as e:
            print(f"[WARN] Memcached write failed for {stock}: {e}", flush=True)

    def _get_all_stocks(self) -> List[str]:
        """Get active stock universe from config with safe fallback."""
        stocks = config.get_stocks_list()
        return stocks if stocks else self.tracked_stocks

    def _check_redis_health(self) -> bool:
        """
        Verify Redis and Memcached connectivity.
        
        Returns:
            bool: True if healthy, False otherwise.
        """
        try:
            self.r.ping()
        except Exception as e:
            print(f"[ERROR] Redis health check failed: {e}", flush=True)
            return False

        try:
            test_key = "__offset_store_healthcheck__"
            self.m.set(test_key, "ok", expire=2)
            value = self._decode_memcache_value(self.m.get(test_key))
            if value != "ok":
                print("[ERROR] Memcached health check failed: bad echo value", flush=True)
                return False
        except Exception as e:
            print(f"[ERROR] Memcached health check failed: {e}", flush=True)
            return False

        return True

    def _convert_token(self, token: int) -> Optional[str]:
        """Convert instrument token to EXCHANGE:SYMBOL format."""
        return self.mapper.convert_token(token)

    # =========================================================================
    # CONSUMER THREAD
    # =========================================================================

    def _csv_consumer(self, consumer_id: int):
        """
        Consumer thread that reads from Redis streams and saves to CSV.
        
        Each consumer is assigned a subset of stocks to process. It reads
        from the corresponding Redis streams and writes tick data to CSV files.
        
        Args:
            consumer_id: Unique identifier for this consumer thread.
        """
        worker = Save.CSV(self.directory)
        
        while self.r.get('end') != 'true':
            # Check if rebalance is requested - synchronize with barrier
            if self.rebalance_requested.is_set():
                try:
                    print(f"[CONSUMER {consumer_id}] Pausing for rebalance...", flush=True)
                    self.pause_barrier.wait(timeout=self.barrier_timeout)
                    print(f"[CONSUMER {consumer_id}] Waiting for rebalance to complete...", flush=True)
                    self.resume_barrier.wait(timeout=self.barrier_timeout)
                    print(f"[CONSUMER {consumer_id}] Resuming after rebalance.", flush=True)
                except threading.BrokenBarrierError:
                    print(f"[ERROR] Consumer {consumer_id}: Barrier broken during rebalance. "
                          f"Another thread may have died or timed out. Continuing...", flush=True)
                    # Reset barriers to recover
                    try:
                        self.pause_barrier.reset()
                        self.resume_barrier.reset()
                    except Exception as e:
                        print(f"[ERROR] Consumer {consumer_id}: Failed to reset barriers: {e}", flush=True)
                    self.rebalance_requested.clear()
                except Exception as e:
                    print(f"[ERROR] Consumer {consumer_id}: Unexpected error during rebalance sync: {e}", flush=True)
                    self.rebalance_requested.clear()
            
            # Get assigned stocks
            with self.consumer_lock:
                my_stocks = self.consumers.get(consumer_id, [])
            
            if not my_stocks:
                print(f"[CONSUMER {consumer_id}] No stocks assigned, waiting...")
                time.sleep(2)
                continue
            
            # Build stream offsets from Memcached
            offsets = self._get_offsets(my_stocks)
            
            streams = {
                stock: next_redis_stream_id(offset)
                for stock, offset in zip(my_stocks, offsets)
                if offset is not None
            }
            
            if not streams:
                print(f"[CONSUMER {consumer_id}] No valid streams available, waiting...", flush=True)
                time.sleep(1)
                continue
            
            # Read from streams (blocking with 100ms timeout)
            messages = self.r.xread(streams, block=100)
            
            if not messages:
                continue
            
            # Process messages
            for stream_name, stream_messages in messages:
                for msg_id, msg_data in stream_messages:
                    try:
                        data = json.loads(msg_data['data'])
                        converted = self._convert_token(data['instrument_token'])
                        if not converted:
                            continue
                        
                        stock_name = converted.split(':')[1]
                        worker.save_tick(data)
                        
                        # Update last processed message ID
                        self._set_offset(stock_name, msg_id)
                        
                    except Exception as e:
                        print(f"[ERROR] Consumer {consumer_id} failed to process {msg_id}: {e}", flush=True)
                        continue
        
        print(f"[CONSUMER {consumer_id}] Shutting down.")

    # =========================================================================
    # LOAD BALANCING
    # =========================================================================

    def _rebalance_stocks(self):
        """
        Rebalance stock assignments across consumers based on stream sizes.
        
        Uses a greedy algorithm to distribute stocks to consumers, prioritizing
        even distribution of message counts to prevent any single consumer
        from being overloaded.
        
        Synchronization:
            1. Set rebalance_requested flag
            2. Wait at pause_barrier for all consumers to pause
            3. Perform rebalancing
            4. Wait at resume_barrier to release all consumers
            5. Reset barriers for next cycle
        """
        print(f"[REBALANCE] Starting rebalancing at {dt.datetime.now()}", flush=True)
        
        # Signal all consumers to pause
        self.rebalance_requested.set()
        
        try:
            # Wait for all consumers to reach the pause barrier
            print(f"[REBALANCE] Waiting for {self.num_consumers} consumers to pause...", flush=True)
            self.pause_barrier.wait(timeout=self.barrier_timeout)
            print("[REBALANCE] All consumers paused. Performing rebalance...", flush=True)
            
        except threading.BrokenBarrierError:
            print("[ERROR] Rebalance: Pause barrier broken! One or more consumer threads "
                  "may have died or timed out. Aborting rebalance.", flush=True)
            self._recover_from_broken_barrier()
            return
        except Exception as e:
            print(f"[ERROR] Rebalance: Failed to synchronize consumers at pause: {e}", flush=True)
            self._recover_from_broken_barrier()
            return
        
        # ===== CRITICAL SECTION: All consumers are paused =====
        try:
            # Get stream sizes for all tracked stocks
            all_stocks = self._get_all_stocks()
            stock_counts = []
            
            for stock in all_stocks:
                try:
                    count = self.r.xlen(stock)
                    stock_counts.append((stock, count))
                except Exception as e:
                    print(f"[ERROR] Could not get stream length for {stock}: {e}", flush=True)
            
            # Sort by count descending (assign largest first)
            stock_counts.sort(key=lambda x: -x[1])
            
            # Greedy assignment
            new_assignments = defaultdict(list)
            totals = [0] * self.num_consumers
            
            for stock, count in stock_counts:
                # Assign to consumer with lowest total
                min_idx = totals.index(min(totals))
                new_assignments[min_idx].append(stock)
                totals[min_idx] += count
            
            # Update assignments atomically
            with self.consumer_lock:
                self.consumers = dict(new_assignments)
            
            # Log assignments
            for cid, stocks in self.consumers.items():
                print(f"[REBALANCE] Consumer {cid}: {len(stocks)} stocks, ~{totals[cid]} messages", flush=True)
                
        except Exception as e:
            print(f"[ERROR] Rebalance: Failed during stock reassignment: {e}", flush=True)
        # ===== END CRITICAL SECTION =====
        
        # Clear the request flag before releasing consumers
        self.rebalance_requested.clear()
        
        try:
            # Release all consumers
            print("[REBALANCE] Releasing consumers...", flush=True)
            self.resume_barrier.wait(timeout=self.barrier_timeout)
            print("[REBALANCE] All consumers resumed.", flush=True)
            
        except threading.BrokenBarrierError:
            print("[ERROR] Rebalance: Resume barrier broken! Attempting recovery...", flush=True)
            self._recover_from_broken_barrier()
            return
        except Exception as e:
            print(f"[ERROR] Rebalance: Failed to release consumers: {e}", flush=True)
            self._recover_from_broken_barrier()
            return
        
        # Reset barriers for the next rebalance cycle
        try:
            self.pause_barrier.reset()
            self.resume_barrier.reset()
        except Exception as e:
            print(f"[ERROR] Rebalance: Failed to reset barriers: {e}", flush=True)
        
        print(f"[REBALANCE] Completed at {dt.datetime.now()}", flush=True)

    def _recover_from_broken_barrier(self):
        """
        Attempt to recover from a broken barrier state.
        
        This is called when a barrier times out or breaks, typically because
        a consumer thread has died. Resets all synchronization primitives.
        """
        print("[REBALANCE] Attempting barrier recovery...", flush=True)
        self.rebalance_requested.clear()
        
        try:
            self.pause_barrier.reset()
        except Exception as e:
            print(f"[ERROR] Recovery: Failed to reset pause_barrier: {e}", flush=True)
        
        try:
            self.resume_barrier.reset()
        except Exception as e:
            print(f"[ERROR] Recovery: Failed to reset resume_barrier: {e}", flush=True)
        
        print("[REBALANCE] Barrier recovery complete. Next rebalance may succeed.", flush=True)

    # =========================================================================
    # MONITORING THREADS
    # =========================================================================

    def _cleanup_loop(self):
        """
        Periodically trim processed messages from Redis streams.
        
        This prevents unbounded memory growth by removing messages that
        have already been processed by all consumers.
        """
        while self.r.get('end') != 'true':
            time.sleep(self.cleanup_interval)
            print(f"[CLEANUP] Starting cleanup at {dt.datetime.now()}", flush=True)
            
            try:
                for stock in self._get_all_stocks():
                    last_id = self._get_offsets([stock])[0]
                    
                    if not last_id or last_id == "0":
                        continue
                    
                    stream_length = self.r.xlen(stock)
                    
                    if stream_length <= self.cleanup_lag:
                        continue
                    
                    # Trim stream up to last processed ID
                    self.r.xtrim(stock, minid=last_id, approximate=True)
                    new_length = self.r.xlen(stock)
                    print(f"[CLEANUP] Trimmed {stock}: {stream_length} -> {new_length}", flush=True)
                    
            except Exception as e:
                print(f"[ERROR] Cleanup failed: {e}", flush=True)
                time.sleep(1)
        
        print("[CLEANUP] Shutting down.")

    def _thread_monitor(self, check_interval: int = 11):
        """
        Monitor consumer threads and restart any that have died.
        
        Args:
            check_interval: Seconds between health checks.
        """
        health_check_counter = 0
        while self.r.get('end') != 'true':
            time.sleep(check_interval)
            
            # Periodically check Redis health
            health_check_counter += 1
            if health_check_counter >= 5:  # Every ~55 seconds
                if not self._check_redis_health():
                    print("[ALERT] Redis health check failed! This may cause consumer failures.", flush=True)
                health_check_counter = 0
            
            active_threads = {t.name for t in threading.enumerate()}
            
            for cid in self.consumers:
                thread_name = f"CSVConsumer_{cid}"
                if thread_name not in active_threads:
                    print(f"[MONITOR] {thread_name} is down. Restarting...", flush=True)
                    thread = threading.Thread(
                        target=self._csv_consumer,
                        args=(cid,),
                        name=thread_name
                    )
                    thread.start()
        
        print("[MONITOR] Shutting down.")

    def _scheduler_loop(self, interval: int = 60):
        """
        Periodically trigger stock rebalancing.
        
        Args:
            interval: Seconds between rebalance cycles.
        """
        while self.r.get('end') != 'true':
            time.sleep(interval)
            self._rebalance_stocks()
        
        print("[SCHEDULER] Shutting down.")

    def _offset_store_watchdog(self):
        """
        Monitor offset store health and auto-recover from partial loss.

        This watchdog alerts on degradation and reseeds missing Memcached
        offsets instead of hard-shutting down consumers.
        """
        print("[WATCHDOG] Starting offset-store monitor.")
        
        while self.r.get('end') != 'true':
            time.sleep(1)

            if not self._check_redis_health():
                timestamp = dt.datetime.now()
                print(f"[CRITICAL] Offset store unhealthy at {timestamp}!", flush=True)
                report.send_email_alert(
                    "🚨 OFFSET STORE DEGRADED",
                    f"Offset store health check failed at {timestamp}. Auto-recovery will continue."
                )

            # Reseed missing Memcached offsets with the default starting ID
            for stock in self._get_all_stocks():
                try:
                    mem_val = self._decode_memcache_value(self.m.get(self._offset_key(stock)))
                    if mem_val is None:
                        self.m.set(self._offset_key(stock), "0")
                except Exception as e:
                    print(f"[WARN] Watchdog failed to reseed offset for {stock}: {e}", flush=True)

        print("[WATCHDOG] Shutting down.")

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def start(self) -> List[threading.Thread]:
        """
        Start all consumer and monitoring threads.
        
        Returns:
            list: List of main consumer threads (for joining).
        """
        # Initialize CSV files
        Save.CSV(self.directory).initialise()
        
        # Calculate initial stock assignments
        stocks = self._get_all_stocks()
        stocks_per_consumer = math.ceil(len(stocks) / self.num_consumers)
        
        threads = []
        for i in range(self.num_consumers):
            start_idx = i * stocks_per_consumer
            end_idx = start_idx + stocks_per_consumer
            self.consumers[i] = stocks[start_idx:end_idx]
            
            thread = threading.Thread(
                target=self._csv_consumer,
                args=(i,),
                name=f"CSVConsumer_{i}"
            )
            threads.append(thread)
        
        # Start consumer threads
        for thread in threads:
            thread.start()
        
        # Start monitoring threads (as daemons)
        threading.Thread(target=self._cleanup_loop, daemon=True, name="CleanupManager").start()
        threading.Thread(target=self._thread_monitor, daemon=True, name="ThreadMonitor").start()
        threading.Thread(target=self._scheduler_loop, daemon=True, name="Scheduler").start()
        threading.Thread(target=self._offset_store_watchdog, daemon=True, name="OffsetStoreWatchdog").start()
        
        return threads


def start_consumer_threads(directory: str, num_consumers: int) -> List[threading.Thread]:
    """
    Initialize and start all consumer threads.
    
    This is the main entry point for the consumer subsystem. It creates
    a Consumer instance and starts all worker and monitoring threads.
    
    Args:
        directory: Base directory for CSV file storage.
        num_consumers: Number of consumer threads to run.
    
    Returns:
        list: List of main consumer threads (for joining).
    """
    consumer = Consumer(directory, num_consumers)
    return consumer.start()
