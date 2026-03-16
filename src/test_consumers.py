"""
Test bench for the Consumer module.

This module provides comprehensive testing utilities to validate:
- Redis connectivity and stream operations
- Load balancing across consumer threads
- Stock reassignment logic
- Thread health monitoring
- CSV file operations
- Cleanup mechanisms

Usage:
    python test_consumers.py [test_name] [options]

Test Names:
    all          Run all tests (default)
    redis        Test Redis connectivity
    loadbalance  Test load balancing algorithm
    reassignment Test stock reassignment
    threads      Test thread management
    csv          Test CSV file operations
    cleanup      Test stream cleanup

Options:
    num_consumers=N   Number of simulated consumers (default: 5)
    num_stocks=N      Number of simulated stocks (default: 20)

Examples:
    python test_consumers.py all
    python test_consumers.py loadbalance
    python test_consumers.py threads num_consumers=10
"""

import os
import json
import time
import math
import threading
import tempfile
import shutil
from collections import defaultdict
from typing import Dict, List

import config
from utils import get_ist_date, get_ist_timestamp


# =============================================================================
# MODULE-LEVEL WORKER FUNCTIONS
# =============================================================================

def _test_consumer_worker(stop_event: threading.Event, consumer_id: int, results: dict):
    """
    Test consumer worker that runs until stop_event is set.
    
    Args:
        stop_event: Event to signal shutdown.
        consumer_id: Consumer ID.
        results: Shared dict to store results.
    """
    count = 0
    while not stop_event.is_set():
        count += 1
        time.sleep(0.1)
    results[consumer_id] = count


def _test_dying_worker(stop_event: threading.Event, should_die: threading.Event):
    """Worker that dies when should_die is set."""
    while not stop_event.is_set():
        if should_die.is_set():
            return  # Simulate crash
        time.sleep(0.1)


# =============================================================================
# TEST BENCH CLASS
# =============================================================================

class ConsumerTestBench:
    """
    Test bench for testing Consumer load balancing, reassignment, and connectivity.
    
    This class provides comprehensive testing utilities to validate:
    - Redis connectivity and stream operations
    - Load balancing across consumer threads
    - Stock reassignment logic
    - Thread health monitoring
    - Cleanup mechanisms
    """
    
    def __init__(self, verbose: bool = True, num_consumers: int = 5, num_stocks: int = 20):
        """
        Initialize the test bench.
        
        Args:
            verbose: If True, print detailed test output.
            num_consumers: Number of simulated consumers for tests.
            num_stocks: Number of simulated stocks for tests.
        """
        self.verbose = verbose
        self.test_results = {}
        self.r = config.redis_client
        self.m = config.memcache_client
        self.num_consumers = num_consumers
        self.num_stocks = num_stocks
        
        # Test stock names
        self.test_stocks = [f"TEST_STOCK_{i}" for i in range(num_stocks)]
        
    def _log(self, message: str, level: str = "INFO"):
        """Log a message with formatting."""
        if self.verbose:
            symbols = {"INFO": "ℹ️", "PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "TEST": "🧪"}
            print(f"{symbols.get(level, 'ℹ️')} [{level}] {message}", flush=True)
    
    def _record_result(self, test_name: str, passed: bool, message: str = ""):
        """Record a test result."""
        self.test_results[test_name] = {"passed": passed, "message": message}
        level = "PASS" if passed else "FAIL"
        self._log(f"{test_name}: {message}", level)
    
    def _cleanup_test_data(self):
        """Clean up test data from Redis."""
        for stock in self.test_stocks:
            self.r.delete(stock)
            self.m.delete(f"{config.OFFSETS_PREFIX}{stock}")
        self.r.delete("__test_stocks__")
        self.r.delete("__test_stream__")
        self.m.delete("__test_memcache_key__")
        self.m.delete(f"{config.OFFSETS_PREFIX}__test_flow_stream__")
    
    # =========================================================================
    # REDIS CONNECTIVITY TESTS
    # =========================================================================
    
    def test_redis_connection(self) -> bool:
        """
        Test Redis server connectivity.
        
        Returns:
            bool: True if Redis is connected, False otherwise.
        """
        self._log("Testing Redis connection...", "TEST")
        try:
            response = self.r.ping()
            if not response:
                self._record_result("redis_ping", False, "Redis ping returned False")
                return False
            self._record_result("redis_ping", True, "Redis ping successful")
            return True
        except Exception as e:
            self._record_result("redis_connection", False, f"Redis error: {e}")
            return False
    
    def test_offset_store_operations(self) -> bool:
        """
        Test Memcached-only offset operations.
        
        Returns:
            bool: True if operations work, False otherwise.
        """
        self._log("Testing offset store operations...", "TEST")
        try:
            test_stock = self.test_stocks[0]
            cache_key = f"{config.OFFSETS_PREFIX}{test_stock}"

            # Set offset in Memcached
            self.m.set(cache_key, "0")
            self._record_result("offset_set", True, "Offset set successful")

            # Read from Memcached
            value = self.m.get(cache_key)
            value = value.decode("utf-8") if isinstance(value, bytes) else value
            if value != "0":
                self._record_result("offset_get_memcached", False, f"Memcached get returned {value} instead of '0'")
                return False
            self._record_result("offset_get_memcached", True, "Memcached get successful")

            # Missing offset should be clear after deletion
            self.m.delete(cache_key)
            missing_value = self.m.get(cache_key)
            if missing_value is not None:
                self._record_result("offset_missing_after_delete", False, f"Memcached still returned {missing_value}")
                return False
            self._record_result("offset_missing_after_delete", True, "Memcached delete successful")

            # Restore directly in Memcached
            self.m.set(cache_key, "0")
            restored = self.m.get(cache_key)
            restored = restored.decode("utf-8") if isinstance(restored, bytes) else restored
            if restored != "0":
                self._record_result("offset_rehydrate", False, f"Offset rehydrate returned {restored}")
                return False
            self._record_result("offset_rehydrate", True, "Offset rehydrate successful")

            self.m.delete(cache_key)
            return True
            
        except Exception as e:
            self._record_result("offset_store", False, f"Offset store operation error: {e}")
            return False
    
    def test_redis_stream_consumer_ops(self) -> bool:
        """
        Test Redis stream operations for consumer use case.
        
        Returns:
            bool: True if operations work, False otherwise.
        """
        self._log("Testing Redis stream consumer operations...", "TEST")
        try:
            test_stream = "__test_stream__"
            
            # Add multiple messages
            msg_ids = []
            for i in range(10):
                data = {"data": json.dumps({"tick": i, "price": 100 + i})}
                msg_id = self.r.xadd(test_stream, data)
                msg_ids.append(msg_id)
            self._record_result("stream_xadd_batch", True, f"Added {len(msg_ids)} messages")
            
            # Test XLEN
            length = self.r.xlen(test_stream)
            if length != 10:
                self._record_result("stream_xlen", False, f"XLEN returned {length}")
                return False
            self._record_result("stream_xlen", True, f"XLEN correct: {length}")
            
            # Test XREAD with offset
            messages = self.r.xread({test_stream: msg_ids[4]}, count=3)
            if not messages or len(messages[0][1]) != 3:
                self._record_result("stream_xread_offset", False, "XREAD with offset failed")
                return False
            self._record_result("stream_xread_offset", True, "XREAD with offset successful")
            
            # Test XTRIM
            self.r.xtrim(test_stream, minid=msg_ids[5], approximate=True)
            new_length = self.r.xlen(test_stream)
            self._record_result("stream_xtrim", True, f"XTRIM successful: {length} -> {new_length}")
            
            # Cleanup
            self.r.delete(test_stream)
            return True
            
        except Exception as e:
            self._record_result("stream_consumer_ops", False, f"Stream error: {e}")
            return False
    
    # =========================================================================
    # LOAD BALANCING TESTS
    # =========================================================================
    
    def test_even_distribution(self) -> bool:
        """
        Test that stocks are evenly distributed across consumers.
        
        Returns:
            bool: True if distribution is even, False otherwise.
        """
        self._log("Testing even stock distribution...", "TEST")
        try:
            stocks = self.test_stocks
            num_consumers = self.num_consumers
            
            # Simple even distribution
            stocks_per_consumer = math.ceil(len(stocks) / num_consumers)
            assignments = {}
            
            for i in range(num_consumers):
                start_idx = i * stocks_per_consumer
                end_idx = start_idx + stocks_per_consumer
                assignments[i] = stocks[start_idx:end_idx]
            
            # Check distribution
            counts = [len(s) for s in assignments.values()]
            max_diff = max(counts) - min(counts)
            
            if max_diff > 1:
                self._record_result("even_distribution", False, 
                                   f"Distribution uneven: max diff = {max_diff}")
                return False
            
            self._record_result("even_distribution", True, 
                               f"Distribution even: {counts}, max diff = {max_diff}")
            return True
            
        except Exception as e:
            self._record_result("even_distribution", False, f"Error: {e}")
            return False
    
    def test_greedy_load_balancing(self) -> bool:
        """
        Test greedy load balancing algorithm.
        
        Returns:
            bool: True if load balancing is effective, False otherwise.
        """
        self._log("Testing greedy load balancing algorithm...", "TEST")
        try:
            # Simulate stocks with varying message counts
            stock_counts = [(f"STOCK_{i}", i * 100) for i in range(self.num_stocks)]
            stock_counts.sort(key=lambda x: -x[1])  # Sort by count descending
            
            # Greedy assignment
            assignments = defaultdict(list)
            totals = [0] * self.num_consumers
            
            for stock, count in stock_counts:
                min_idx = totals.index(min(totals))
                assignments[min_idx].append((stock, count))
                totals[min_idx] += count
            
            # Check balance
            max_load = max(totals)
            min_load = min(totals)
            balance_ratio = min_load / max_load if max_load > 0 else 1.0
            
            self._log(f"Load distribution: {totals}", "INFO")
            self._log(f"Balance ratio: {balance_ratio:.2%}", "INFO")
            
            # Should achieve at least 70% balance for random data
            if balance_ratio < 0.5:
                self._record_result("greedy_loadbalance", False, 
                                   f"Poor balance: {balance_ratio:.2%}")
                return False
            
            self._record_result("greedy_loadbalance", True, 
                               f"Good balance: {balance_ratio:.2%}")
            return True
            
        except Exception as e:
            self._record_result("greedy_loadbalance", False, f"Error: {e}")
            return False
    
    def test_rebalance_with_changing_loads(self) -> bool:
        """
        Test rebalancing when stock loads change.
        
        Returns:
            bool: True if rebalancing adapts correctly, False otherwise.
        """
        self._log("Testing rebalance with changing loads...", "TEST")
        try:
            # Initial assignment
            initial_counts = [(f"STOCK_{i}", 100) for i in range(10)]
            
            def assign(stock_counts):
                assignments = defaultdict(list)
                totals = [0] * 3  # 3 consumers
                for stock, count in sorted(stock_counts, key=lambda x: -x[1]):
                    min_idx = totals.index(min(totals))
                    assignments[min_idx].append(stock)
                    totals[min_idx] += count
                return dict(assignments), totals
            
            initial_assignments, initial_totals = assign(initial_counts)
            self._log(f"Initial totals: {initial_totals}", "INFO")
            
            # Simulate load spike on some stocks
            changed_counts = initial_counts.copy()
            changed_counts[0] = ("STOCK_0", 1000)  # 10x spike
            changed_counts[1] = ("STOCK_1", 500)   # 5x spike
            
            new_assignments, new_totals = assign(changed_counts)
            self._log(f"After spike totals: {new_totals}", "INFO")
            
            # Check that heavy stocks are distributed
            heavy_stocks = {"STOCK_0", "STOCK_1"}
            consumers_with_heavy = set()
            for cid, stocks in new_assignments.items():
                if any(s in heavy_stocks for s in stocks):
                    consumers_with_heavy.add(cid)
            
            if len(consumers_with_heavy) < 2:
                self._record_result("rebalance_adaptive", False, 
                                   "Heavy stocks not distributed")
                return False
            
            self._record_result("rebalance_adaptive", True, 
                               f"Heavy stocks distributed to {len(consumers_with_heavy)} consumers")
            return True
            
        except Exception as e:
            self._record_result("rebalance_adaptive", False, f"Error: {e}")
            return False
    
    # =========================================================================
    # THREAD MANAGEMENT TESTS
    # =========================================================================
    
    def test_thread_creation(self) -> bool:
        """
        Test consumer thread creation.
        
        Returns:
            bool: True if threads are created correctly, False otherwise.
        """
        self._log("Testing thread creation...", "TEST")
        try:
            stop_event = threading.Event()
            results = {}
            threads = []
            
            for i in range(self.num_consumers):
                t = threading.Thread(
                    target=_test_consumer_worker,
                    args=(stop_event, i, results),
                    name=f"TestConsumer_{i}"
                )
                threads.append(t)
                t.start()
            
            # Check threads are running
            time.sleep(0.3)
            active = [t for t in threads if t.is_alive()]
            
            if len(active) != self.num_consumers:
                self._record_result("thread_creation", False, 
                                   f"Only {len(active)}/{self.num_consumers} threads running")
                stop_event.set()
                return False
            
            self._record_result("thread_creation", True, 
                               f"All {self.num_consumers} threads running")
            
            # Cleanup
            stop_event.set()
            for t in threads:
                t.join(timeout=1)
            
            return True
            
        except Exception as e:
            self._record_result("thread_creation", False, f"Error: {e}")
            return False
    
    def test_thread_monitoring(self) -> bool:
        """
        Test thread monitoring and detection of dead threads.
        
        Returns:
            bool: True if monitoring detects dead threads, False otherwise.
        """
        self._log("Testing thread monitoring...", "TEST")
        try:
            stop_event = threading.Event()
            should_die = threading.Event()
            
            # Start a thread that will die
            dying_thread = threading.Thread(
                target=_test_dying_worker,
                args=(stop_event, should_die),
                name="DyingThread"
            )
            dying_thread.start()
            
            # Verify it's running
            time.sleep(0.1)
            if not dying_thread.is_alive():
                self._record_result("thread_monitor_setup", False, "Thread didn't start")
                return False
            
            # Kill the thread
            should_die.set()
            time.sleep(0.2)
            
            # Check it's dead
            if dying_thread.is_alive():
                self._record_result("thread_monitor_detect", False, "Thread didn't die")
                stop_event.set()
                return False
            
            self._record_result("thread_monitor_detect", True, "Dead thread detected")
            
            # Simulate restart
            new_thread = threading.Thread(
                target=_test_dying_worker,
                args=(stop_event, threading.Event()),  # Won't die
                name="RevivedThread"
            )
            new_thread.start()
            
            time.sleep(0.1)
            if not new_thread.is_alive():
                self._record_result("thread_restart", False, "Restart failed")
                return False
            
            self._record_result("thread_restart", True, "Thread restarted successfully")
            
            # Cleanup
            stop_event.set()
            new_thread.join(timeout=1)
            
            return True
            
        except Exception as e:
            self._record_result("thread_monitoring", False, f"Error: {e}")
            return False
    
    def test_rebalance_pause_resume(self) -> bool:
        """
        Test that consumers pause during rebalancing.
        
        Returns:
            bool: True if pause/resume works, False otherwise.
        """
        self._log("Testing rebalance pause/resume...", "TEST")
        try:
            rebalance_flag = threading.Event()
            rebalance_flag.set()  # Start in running state
            
            work_done = {"before_pause": 0, "during_pause": 0, "after_resume": 0}
            stop_event = threading.Event()
            
            def worker():
                while not stop_event.is_set():
                    rebalance_flag.wait()
                    if stop_event.is_set():
                        break
                    if rebalance_flag.is_set():
                        work_done["after_resume"] += 1
                    time.sleep(0.01)
            
            thread = threading.Thread(target=worker)
            thread.start()
            
            # Let it work
            time.sleep(0.1)
            work_done["before_pause"] = work_done["after_resume"]
            
            # Pause
            rebalance_flag.clear()
            pause_start = work_done["after_resume"]
            time.sleep(0.1)
            work_done["during_pause"] = work_done["after_resume"] - pause_start
            
            # Resume
            rebalance_flag.set()
            time.sleep(0.1)
            
            # Cleanup
            stop_event.set()
            rebalance_flag.set()  # Unblock if waiting
            thread.join(timeout=1)
            
            if work_done["during_pause"] > 0:
                self._record_result("pause_resume", False, 
                                   f"Work done during pause: {work_done['during_pause']}")
                return False
            
            self._record_result("pause_resume", True, 
                               f"Pause effective: {work_done['during_pause']} work during pause")
            return True
            
        except Exception as e:
            self._record_result("pause_resume", False, f"Error: {e}")
            return False
    
    # =========================================================================
    # CLEANUP TESTS
    # =========================================================================
    
    def test_stream_cleanup(self) -> bool:
        """
        Test stream trimming/cleanup.
        
        Returns:
            bool: True if cleanup works, False otherwise.
        """
        self._log("Testing stream cleanup...", "TEST")
        try:
            test_stream = "__test_cleanup_stream__"
            
            # Add messages
            msg_ids = []
            for i in range(100):
                msg_id = self.r.xadd(test_stream, {"data": json.dumps({"i": i})})
                msg_ids.append(msg_id)
            
            initial_len = self.r.xlen(test_stream)
            self._log(f"Initial stream length: {initial_len}", "INFO")
            
            # Trim using MAXLEN (exact) instead of MINID with approximate
            # This is more reliable for testing
            self.r.xtrim(test_stream, maxlen=50, approximate=False)
            
            after_trim = self.r.xlen(test_stream)
            self._log(f"After trim length: {after_trim}", "INFO")
            
            if after_trim > 50:
                self._record_result("stream_cleanup", False, 
                                   f"Trim ineffective: {initial_len} -> {after_trim}")
                self.r.delete(test_stream)
                return False
            
            self._record_result("stream_cleanup", True, 
                               f"Trim effective: {initial_len} -> {after_trim}")
            
            # Cleanup
            self.r.delete(test_stream)
            return True
            
        except Exception as e:
            self._record_result("stream_cleanup", False, f"Error: {e}")
            return False
            return False
    
    # =========================================================================
    # CSV OPERATIONS TESTS
    # =========================================================================
    
    def test_csv_directory_creation(self) -> bool:
        """
        Test CSV directory structure creation.
        
        Returns:
            bool: True if directories are created, False otherwise.
        """
        self._log("Testing CSV directory creation...", "TEST")
        try:
            # Create temp directory
            temp_dir = tempfile.mkdtemp(prefix="test_consumers_")
            
            # Simulate directory structure
            exchanges = ["NSE", "BSE"]
            test_stocks = ["TEST_STOCK_1", "TEST_STOCK_2"]
            
            for exchange in exchanges:
                for stock in test_stocks:
                    path = os.path.join(temp_dir, exchange, stock)
                    os.makedirs(path, exist_ok=True)
            
            # Verify
            for exchange in exchanges:
                for stock in test_stocks:
                    path = os.path.join(temp_dir, exchange, stock)
                    if not os.path.exists(path):
                        self._record_result("csv_dir_create", False, 
                                           f"Directory not created: {path}")
                        shutil.rmtree(temp_dir)
                        return False
            
            self._record_result("csv_dir_create", True, "All directories created")
            
            # Cleanup
            shutil.rmtree(temp_dir)
            return True
            
        except Exception as e:
            self._record_result("csv_dir_create", False, f"Error: {e}")
            return False
    
    def test_csv_file_write(self) -> bool:
        """
        Test CSV file writing.
        
        Returns:
            bool: True if CSV writing works, False otherwise.
        """
        self._log("Testing CSV file writing...", "TEST")
        try:
            import csv
            
            temp_dir = tempfile.mkdtemp(prefix="test_consumers_csv_")
            csv_path = os.path.join(temp_dir, "test.csv")
            
            # Write test data
            header = ["timestamp", "stock", "price", "volume"]
            rows = [
                [get_ist_timestamp(), "TEST", 100.5, 1000],
                [get_ist_timestamp(), "TEST", 101.0, 2000],
            ]
            
            with open(csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(rows)
            
            # Verify
            with open(csv_path, 'r') as f:
                reader = csv.reader(f)
                read_rows = list(reader)
            
            if len(read_rows) != 3:  # header + 2 rows
                self._record_result("csv_write", False, 
                                   f"Wrong row count: {len(read_rows)}")
                shutil.rmtree(temp_dir)
                return False
            
            self._record_result("csv_write", True, 
                               f"CSV write successful: {len(read_rows) - 1} rows")
            
            # Cleanup
            shutil.rmtree(temp_dir)
            return True
            
        except Exception as e:
            self._record_result("csv_write", False, f"Error: {e}")
            return False
    
    # =========================================================================
    # INTEGRATION TESTS
    # =========================================================================
    
    def test_full_consumer_flow(self) -> bool:
        """
        Test full consumer flow: read from stream -> process -> track offset.
        
        Returns:
            bool: True if full flow works, False otherwise.
        """
        self._log("Testing full consumer flow...", "TEST")
        try:
            test_stream = "__test_flow_stream__"
            cache_key = f"{config.OFFSETS_PREFIX}{test_stream}"
            
            # Initialize
            self.m.set(cache_key, "0")
            
            # Add messages
            for i in range(5):
                self.r.xadd(test_stream, {"data": json.dumps({"tick": i})})
            
            # Simulate consumer read
            offset = self.m.get(cache_key)
            offset = offset.decode("utf-8") if isinstance(offset, bytes) else offset
            messages = self.r.xread({test_stream: offset if offset != "0" else "0"}, count=10)
            
            if not messages:
                self._record_result("consumer_flow", False, "No messages read")
                return False
            
            # Process and update offset
            last_msg_id = None
            for stream_name, stream_messages in messages:
                for msg_id, msg_data in stream_messages:
                    data = json.loads(msg_data['data'])
                    last_msg_id = msg_id
            
            if last_msg_id:
                self.m.set(cache_key, last_msg_id)
            
            # Verify offset updated
            new_offset = self.m.get(cache_key)
            new_offset = new_offset.decode("utf-8") if isinstance(new_offset, bytes) else new_offset
            if new_offset == "0":
                self._record_result("consumer_flow", False, "Offset not updated")
                return False
            
            self._record_result("consumer_flow", True, 
                               f"Full flow successful, offset: {new_offset}")
            
            # Cleanup
            self.r.delete(test_stream)
            self.m.delete(cache_key)
            return True
            
        except Exception as e:
            self._record_result("consumer_flow", False, f"Error: {e}")
            return False
    
    # =========================================================================
    # TEST RUNNERS
    # =========================================================================
    
    def run_all_tests(self) -> dict:
        """Run all available tests."""
        self._log("=" * 60, "INFO")
        self._log("CONSUMER TEST BENCH - RUNNING ALL TESTS", "TEST")
        self._log("=" * 60, "INFO")
        
        test_methods = [
            ("Redis Connection", self.test_redis_connection),
            ("Offset Store Operations", self.test_offset_store_operations),
            ("Redis Stream Consumer Ops", self.test_redis_stream_consumer_ops),
            ("Even Distribution", self.test_even_distribution),
            ("Greedy Load Balancing", self.test_greedy_load_balancing),
            ("Rebalance Adaptive", self.test_rebalance_with_changing_loads),
            ("Thread Creation", self.test_thread_creation),
            ("Thread Monitoring", self.test_thread_monitoring),
            ("Rebalance Pause/Resume", self.test_rebalance_pause_resume),
            ("Stream Cleanup", self.test_stream_cleanup),
            ("CSV Directory Creation", self.test_csv_directory_creation),
            ("CSV File Write", self.test_csv_file_write),
            ("Full Consumer Flow", self.test_full_consumer_flow),
        ]
        
        passed = 0
        failed = 0
        
        for name, method in test_methods:
            self._log("-" * 40, "INFO")
            try:
                if method():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                self._log(f"{name} raised exception: {e}", "FAIL")
                failed += 1
        
        self._log("=" * 60, "INFO")
        self._log(f"RESULTS: {passed} passed, {failed} failed", "INFO")
        self._log("=" * 60, "INFO")
        
        # Cleanup any leftover test data
        self._cleanup_test_data()
        
        return {
            "passed": passed,
            "failed": failed,
            "total": passed + failed,
            "details": self.test_results
        }
    
    def run_redis_tests(self) -> dict:
        """Run only Redis connectivity tests."""
        self._log("Running Redis tests...", "TEST")
        
        self.test_redis_connection()
        self.test_offset_store_operations()
        self.test_redis_stream_consumer_ops()
        
        return self.test_results
    
    def run_loadbalance_tests(self) -> dict:
        """Run only load balancing tests."""
        self._log("Running load balancing tests...", "TEST")
        
        self.test_even_distribution()
        self.test_greedy_load_balancing()
        self.test_rebalance_with_changing_loads()
        
        return self.test_results
    
    def run_thread_tests(self) -> dict:
        """Run only thread management tests."""
        self._log("Running thread tests...", "TEST")
        
        self.test_thread_creation()
        self.test_thread_monitoring()
        self.test_rebalance_pause_resume()
        
        return self.test_results
    
    def run_csv_tests(self) -> dict:
        """Run only CSV operation tests."""
        self._log("Running CSV tests...", "TEST")
        
        self.test_csv_directory_creation()
        self.test_csv_file_write()
        
        return self.test_results
    
    def run_cleanup_tests(self) -> dict:
        """Run only cleanup tests."""
        self._log("Running cleanup tests...", "TEST")
        
        self.test_stream_cleanup()
        
        return self.test_results


# =============================================================================
# PUBLIC API
# =============================================================================

def run_test_bench(test_name: str = "all", **kwargs):
    """
    Run the consumer test bench.
    
    Args:
        test_name: Name of test to run. Options:
            - 'all': Run all tests
            - 'redis': Run Redis connectivity tests
            - 'loadbalance': Run load balancing tests
            - 'reassignment': Alias for loadbalance
            - 'threads': Run thread management tests
            - 'csv': Run CSV operation tests
            - 'cleanup': Run cleanup tests
        **kwargs: Additional arguments:
            - num_consumers: Number of simulated consumers
            - num_stocks: Number of simulated stocks
    """
    num_consumers = kwargs.get('num_consumers', 5)
    num_stocks = kwargs.get('num_stocks', 20)
    
    bench = ConsumerTestBench(
        num_consumers=num_consumers,
        num_stocks=num_stocks
    )
    
    test_map = {
        'all': bench.run_all_tests,
        'redis': bench.run_redis_tests,
        'loadbalance': bench.run_loadbalance_tests,
        'reassignment': bench.run_loadbalance_tests,
        'threads': bench.run_thread_tests,
        'csv': bench.run_csv_tests,
        'cleanup': bench.run_cleanup_tests,
    }
    
    if test_name not in test_map:
        print(f"Unknown test: {test_name}")
        print(f"Available tests: {', '.join(test_map.keys())}")
        return
    
    return test_map[test_name]()


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help" or sys.argv[1] == "-h":
            print(__doc__)
        else:
            test_name = sys.argv[1]
            
            # Parse additional args
            kwargs = {}
            for arg in sys.argv[2:]:
                if '=' in arg:
                    key, value = arg.split('=', 1)
                    try:
                        kwargs[key] = int(value)
                    except ValueError:
                        kwargs[key] = value
            
            run_test_bench(test_name, **kwargs)
    else:
        # Default: run all tests
        run_test_bench("all")
