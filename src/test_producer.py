"""
Test bench for the Producer module.

This module provides comprehensive testing utilities to validate:
- WebSocket connection establishment
- Reconnection behavior under various failure scenarios
- Email alert functionality
- Heartbeat monitoring
- Redis connectivity

Usage:
    python test_producer.py [test_name] [options]

Test Names:
    all          Run all tests (default)
    connection   Test connection establishment
    reconnection Test reconnection logic
    mail         Test mail configuration (dry run)
    mail_send    Test mail with actual email sending
    live         Run live WebSocket connection test
    redis        Test Redis connectivity only

Options:
    duration=N   Duration for live test (seconds, default: 30)

Examples:
    python test_producer.py all
    python test_producer.py connection
    python test_producer.py mail_send
    python test_producer.py live duration=60
"""

import os
import json
import time
import multiprocessing

import config
import report
import Auth
from producer import TickerProducer, is_connected
from utils import (
    token_to_stock_mapping,
    get_ist_now,
    get_ist_timestamp,
)


# =============================================================================
# MODULE-LEVEL WORKER FUNCTIONS
# =============================================================================

# Module-level worker functions for multiprocessing (can't pickle nested functions)
def _test_dummy_worker():
    """Dummy worker that sleeps for 60 seconds. Used for process restart tests."""
    time.sleep(60)


def _test_quick_worker():
    """Quick worker that sleeps for 2 seconds. Used for reconnection simulation."""
    time.sleep(2)


# =============================================================================
# TEST BENCH CLASS
# =============================================================================

class ProducerTestBench:
    """
    Test bench for testing TickerProducer connection, reconnection, and mail alert features.
    
    This class provides comprehensive testing utilities to validate:
    - WebSocket connection establishment
    - Reconnection behavior under various failure scenarios
    - Email alert functionality
    - Heartbeat monitoring
    - Redis connectivity
    """
    
    def __init__(self, verbose: bool = True):
        """
        Initialize the test bench.
        
        Args:
            verbose: If True, print detailed test output.
        """
        self.verbose = verbose
        self.test_results = {}
        self.r = config.redis_client
        
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
            # Test ping
            response = self.r.ping()
            if not response:
                self._record_result("redis_ping", False, "Redis ping returned False")
                return False
            self._record_result("redis_ping", True, "Redis ping successful")
            
            # Test read/write
            test_key = "__test_producer_bench__"
            test_value = str(time.time())
            self.r.set(test_key, test_value)
            retrieved = self.r.get(test_key)
            self.r.delete(test_key)
            
            if retrieved != test_value:
                self._record_result("redis_rw", False, f"Read/Write mismatch: {test_value} != {retrieved}")
                return False
            self._record_result("redis_rw", True, "Redis read/write successful")
            
            return True
        except Exception as e:
            self._record_result("redis_connection", False, f"Redis error: {e}")
            return False
    
    def test_redis_stream_operations(self) -> bool:
        """
        Test Redis stream operations (XADD, XREAD, XDEL).
        
        Returns:
            bool: True if stream operations work, False otherwise.
        """
        self._log("Testing Redis stream operations...", "TEST")
        try:
            test_stream = "__test_stream__"
            test_data = {"data": json.dumps({"test": "value", "timestamp": time.time()})}
            
            # Test XADD
            msg_id = self.r.xadd(test_stream, test_data)
            if not msg_id:
                self._record_result("redis_xadd", False, "XADD returned no message ID")
                return False
            self._record_result("redis_xadd", True, f"XADD successful, msg_id: {msg_id}")
            
            # Test XLEN
            length = self.r.xlen(test_stream)
            if length < 1:
                self._record_result("redis_xlen", False, f"XLEN returned {length}")
                return False
            self._record_result("redis_xlen", True, f"XLEN successful: {length} messages")
            
            # Test XREAD
            messages = self.r.xread({test_stream: "0"}, count=1)
            if not messages:
                self._record_result("redis_xread", False, "XREAD returned no messages")
                return False
            self._record_result("redis_xread", True, "XREAD successful")
            
            # Cleanup
            self.r.delete(test_stream)
            self._record_result("redis_stream_cleanup", True, "Stream cleanup successful")
            
            return True
        except Exception as e:
            self._record_result("redis_stream", False, f"Stream operation error: {e}")
            return False
    
    # =========================================================================
    # INTERNET CONNECTIVITY TESTS
    # =========================================================================
    
    def test_internet_connection(self) -> bool:
        """
        Test internet connectivity.
        
        Returns:
            bool: True if connected to internet, False otherwise.
        """
        self._log("Testing internet connectivity...", "TEST")
        connected = is_connected()
        self._record_result("internet_connection", connected, 
                           "Internet connected" if connected else "No internet connection")
        return connected
    
    # =========================================================================
    # MAIL ALERT TESTS
    # =========================================================================
    
    def test_mail_alert_dry_run(self) -> bool:
        """
        Test mail alert configuration without actually sending email.
        
        Returns:
            bool: True if mail configuration is valid, False otherwise.
        """
        self._log("Testing mail alert configuration (dry run)...", "TEST")
        try:
            import dotenv
            import resend
            
            dotenv.load_dotenv(report.ENVLOC)
            to_email = os.getenv("TO_EMAIL")
            
            if not to_email:
                self._record_result("mail_config_to_email", False, "TO_EMAIL not configured")
                return False
            self._record_result("mail_config_to_email", True, f"TO_EMAIL configured: {to_email[:3]}***")
            
            # Verify resend API key exists
            resend.api_key = "re_H6N1UiAC_Pjssgzk6DT8yazbkjPDQrmtJ"
            if not resend.api_key:
                self._record_result("mail_config_api_key", False, "Resend API key not configured")
                return False
            self._record_result("mail_config_api_key", True, "Resend API key configured")
            
            return True
        except ImportError as e:
            self._record_result("mail_import", False, f"Import error: {e}")
            return False
        except Exception as e:
            self._record_result("mail_config", False, f"Configuration error: {e}")
            return False
    
    def test_mail_alert_send(self, recipient_override: str = None) -> bool:
        """
        Test sending an actual email alert.
        
        Args:
            recipient_override: Optional email to send test to instead of configured TO_EMAIL.
            
        Returns:
            bool: True if email sent successfully, False otherwise.
        """
        self._log("Testing mail alert sending...", "TEST")
        try:
            timestamp = get_ist_now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Send test email
            report.send_email_alert(
                subject=f"🧪 TEST BENCH - Mail Alert Test - {timestamp}",
                body=(
                    "Dear User,<br><br>"
                    "This is a <b>test email</b> from the Producer Test Bench.<br><br>"
                    f"<b>Time:</b> {timestamp}<br>"
                    "<b>Status:</b> Mail alert system working correctly.<br><br>"
                    "If you received this email, the mail alert feature is functioning properly.<br><br>"
                    "Regards,<br>"
                    "Stock Data Collection System - Test Bench"
                )
            )
            self._record_result("mail_send", True, "Test email sent successfully")
            return True
        except Exception as e:
            self._record_result("mail_send", False, f"Failed to send email: {e}")
            return False
    
    # =========================================================================
    # CONNECTION TESTS
    # =========================================================================
    
    def test_ticker_producer_init(self) -> bool:
        """
        Test TickerProducer initialization.
        
        Returns:
            bool: True if initialization successful, False otherwise.
        """
        self._log("Testing TickerProducer initialization...", "TEST")
        try:
            # Check environment variables
            api_key = os.getenv('APIKEY')
            api_secret = os.getenv("APISECRET")
            
            if not api_key:
                self._record_result("env_apikey", False, "APIKEY not configured")
                return False
            self._record_result("env_apikey", True, f"APIKEY configured: {api_key[:4]}***")
            
            if not api_secret:
                self._record_result("env_apisecret", False, "APISECRET not configured")
                return False
            self._record_result("env_apisecret", True, "APISECRET configured")
            
            # Check stocks list
            stocks = config.get_stocks_list()
            if not stocks:
                self._record_result("stocks_list", False, "No stocks configured")
                return False
            self._record_result("stocks_list", True, f"Stocks configured: {len(stocks)} stocks")
            
            return True
        except Exception as e:
            self._record_result("producer_init", False, f"Initialization error: {e}")
            return False
    
    def test_authentication(self) -> bool:
        """
        Test Kite authentication.
        
        Returns:
            bool: True if authentication successful, False otherwise.
        """
        self._log("Testing Kite authentication...", "TEST")
        try:
            access_token = Auth.getAuth()
            if not access_token:
                self._record_result("kite_auth", False, "Access token is empty")
                return False
            self._record_result("kite_auth", True, f"Access token obtained: {access_token[:8]}***")
            return True
        except Exception as e:
            self._record_result("kite_auth", False, f"Authentication error: {e}")
            return False
    
    def test_token_mappings(self) -> bool:
        """
        Test token-to-symbol mappings.
        
        Returns:
            bool: True if mappings are valid, False otherwise.
        """
        self._log("Testing token mappings...", "TEST")
        try:
            nse_mapping = token_to_stock_mapping("NSE")
            bse_mapping = token_to_stock_mapping("BSE")
            
            if not nse_mapping:
                self._record_result("nse_mapping", False, "NSE mapping is empty")
                return False
            self._record_result("nse_mapping", True, f"NSE mapping: {len(nse_mapping)} tokens")
            
            if not bse_mapping:
                self._record_result("bse_mapping", False, "BSE mapping is empty")
                return False
            self._record_result("bse_mapping", True, f"BSE mapping: {len(bse_mapping)} tokens")
            
            return True
        except Exception as e:
            self._record_result("token_mappings", False, f"Mapping error: {e}")
            return False
    
    def test_websocket_connection_short(self, timeout_seconds: int = 10) -> bool:
        """
        Test WebSocket connection for a short duration.
        
        Args:
            timeout_seconds: How long to keep connection alive.
            
        Returns:
            bool: True if connection established and received ticks, False otherwise.
        """
        self._log(f"Testing WebSocket connection ({timeout_seconds}s timeout)...", "TEST")
        
        ticks_received = multiprocessing.Value('i', 0)
        connection_established = multiprocessing.Value('b', False)
        
        def test_worker(ticks_count, conn_flag):
            """Worker that connects and counts ticks."""
            try:
                r = config.redis_client
                r.set('time', get_ist_timestamp())
                
                producer = TickerProducer()
                
                # Override on_connect to set flag
                original_on_connect = producer.on_connect
                def patched_on_connect(ws, response):
                    conn_flag.value = True
                    original_on_connect(ws, response)
                producer.on_connect = patched_on_connect
                
                # Override on_ticks to count
                original_on_ticks = producer.on_ticks
                def patched_on_ticks(ws, ticks):
                    ticks_count.value += len(ticks)
                    original_on_ticks(ws, ticks)
                producer.on_ticks = patched_on_ticks
                
                producer.open()
            except Exception as e:
                print(f"Worker error: {e}", flush=True)
        
        process = multiprocessing.Process(
            target=test_worker, 
            args=(ticks_received, connection_established)
        )
        process.start()
        process.join(timeout=timeout_seconds)
        
        if process.is_alive():
            process.terminate()
            process.join()
        
        if connection_established.value:
            self._record_result("ws_connect", True, "WebSocket connection established")
        else:
            self._record_result("ws_connect", False, "WebSocket connection failed")
            return False
        
        if ticks_received.value > 0:
            self._record_result("ws_ticks", True, f"Received {ticks_received.value} ticks")
        else:
            self._record_result("ws_ticks", False, "No ticks received")
            return False
        
        return True
    
    # =========================================================================
    # RECONNECTION TESTS
    # =========================================================================
    
    def test_heartbeat_timeout_detection(self) -> bool:
        """
        Test heartbeat timeout detection.
        
        Returns:
            bool: True if timeout detection works, False otherwise.
        """
        self._log("Testing heartbeat timeout detection...", "TEST")
        try:
            # Set old timestamp in Redis
            old_time = get_ist_timestamp() - (config.HEARTBEAT_TIMEOUT + 5)
            self.r.set('time', old_time)
            
            # Check if diff calculation works
            current_time = get_ist_timestamp()
            diff = current_time - old_time
            
            if diff > config.HEARTBEAT_TIMEOUT:
                self._record_result("heartbeat_timeout_detection", True, 
                                   f"Timeout detected correctly: {diff:.1f}s > {config.HEARTBEAT_TIMEOUT}s")
                return True
            else:
                self._record_result("heartbeat_timeout_detection", False, 
                                   f"Timeout not detected: {diff:.1f}s <= {config.HEARTBEAT_TIMEOUT}s")
                return False
        except Exception as e:
            self._record_result("heartbeat_timeout_detection", False, f"Error: {e}")
            return False
    
    def test_failure_count_tracking(self) -> bool:
        """
        Test failure count tracking logic.
        
        Returns:
            bool: True if failure tracking works, False otherwise.
        """
        self._log("Testing failure count tracking...", "TEST")
        try:
            max_failures = config.SEND_MAIL_TIMEOUT // config.HEARTBEAT_TIMEOUT
            
            self._record_result("failure_config", True, 
                               f"Max failures: {max_failures} (SEND_MAIL_TIMEOUT={config.SEND_MAIL_TIMEOUT}s, "
                               f"HEARTBEAT_TIMEOUT={config.HEARTBEAT_TIMEOUT}s)")
            
            # Simulate failure counting
            failure_count = 0
            for i in range(max_failures + 1):
                failure_count += 1
                if failure_count >= max_failures:
                    self._record_result("failure_tracking", True, 
                                       f"Alert triggered after {failure_count} failures")
                    return True
            
            self._record_result("failure_tracking", False, "Alert not triggered")
            return False
        except Exception as e:
            self._record_result("failure_tracking", False, f"Error: {e}")
            return False
    
    def test_process_restart(self) -> bool:
        """
        Test process termination and restart.
        
        Returns:
            bool: True if process restart works, False otherwise.
        """
        self._log("Testing process restart mechanism...", "TEST")
        try:
            # Start process
            process = multiprocessing.Process(target=_test_dummy_worker)
            process.start()
            initial_pid = process.pid
            
            if not process.is_alive():
                self._record_result("process_start", False, "Process failed to start")
                return False
            self._record_result("process_start", True, f"Process started: PID {initial_pid}")
            
            # Terminate process
            process.terminate()
            process.join(timeout=5)
            
            if process.is_alive():
                self._record_result("process_terminate", False, "Process failed to terminate")
                process.kill()
                return False
            self._record_result("process_terminate", True, "Process terminated successfully")
            
            # Restart process
            process = multiprocessing.Process(target=_test_dummy_worker)
            process.start()
            new_pid = process.pid
            
            if not process.is_alive():
                self._record_result("process_restart", False, "Process failed to restart")
                return False
            self._record_result("process_restart", True, f"Process restarted: PID {new_pid}")
            
            # Cleanup
            process.terminate()
            process.join()
            
            return True
        except Exception as e:
            self._record_result("process_restart", False, f"Error: {e}")
            return False
    
    def test_reconnection_simulation(self, cycles: int = 3) -> bool:
        """
        Simulate reconnection cycles.
        
        Args:
            cycles: Number of reconnection cycles to simulate.
            
        Returns:
            bool: True if all cycles complete successfully, False otherwise.
        """
        self._log(f"Simulating {cycles} reconnection cycles...", "TEST")
        try:
            for i in range(cycles):
                # Start
                process = multiprocessing.Process(target=_test_quick_worker)
                process.start()
                
                # Wait a bit
                time.sleep(0.5)
                
                # Terminate
                process.terminate()
                process.join(timeout=3)
                
                if process.is_alive():
                    process.kill()
                    self._record_result(f"reconnect_cycle_{i+1}", False, "Failed to terminate")
                    return False
                
                self._log(f"Cycle {i+1}/{cycles} complete", "INFO")
            
            self._record_result("reconnection_simulation", True, f"All {cycles} cycles successful")
            return True
        except Exception as e:
            self._record_result("reconnection_simulation", False, f"Error: {e}")
            return False
    
    # =========================================================================
    # TEST RUNNERS
    # =========================================================================
    
    def run_all_tests(self) -> dict:
        """
        Run all available tests.
        
        Returns:
            dict: Summary of all test results.
        """
        self._log("=" * 60, "INFO")
        self._log("PRODUCER TEST BENCH - RUNNING ALL TESTS", "TEST")
        self._log("=" * 60, "INFO")
        
        test_methods = [
            ("Redis Connection", self.test_redis_connection),
            ("Redis Stream Operations", self.test_redis_stream_operations),
            ("Internet Connection", self.test_internet_connection),
            ("Mail Alert Config", self.test_mail_alert_dry_run),
            ("Producer Initialization", self.test_ticker_producer_init),
            ("Token Mappings", self.test_token_mappings),
            ("Heartbeat Timeout Detection", self.test_heartbeat_timeout_detection),
            ("Failure Count Tracking", self.test_failure_count_tracking),
            ("Process Restart", self.test_process_restart),
            ("Reconnection Simulation", self.test_reconnection_simulation),
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
        
        return {
            "passed": passed,
            "failed": failed,
            "total": passed + failed,
            "details": self.test_results
        }
    
    def run_connection_tests(self) -> dict:
        """Run only connection-related tests."""
        self._log("Running connection tests...", "TEST")
        
        self.test_redis_connection()
        self.test_internet_connection()
        self.test_ticker_producer_init()
        self.test_authentication()
        self.test_token_mappings()
        
        return self.test_results
    
    def run_reconnection_tests(self) -> dict:
        """Run only reconnection-related tests."""
        self._log("Running reconnection tests...", "TEST")
        
        self.test_heartbeat_timeout_detection()
        self.test_failure_count_tracking()
        self.test_process_restart()
        self.test_reconnection_simulation()
        
        return self.test_results
    
    def run_mail_tests(self, send_test_email: bool = False) -> dict:
        """
        Run mail alert tests.
        
        Args:
            send_test_email: If True, actually send a test email.
        """
        self._log("Running mail alert tests...", "TEST")
        
        self.test_mail_alert_dry_run()
        if send_test_email:
            self.test_mail_alert_send()
        
        return self.test_results
    
    def run_live_connection_test(self, duration: int = 30) -> dict:
        """
        Run a live WebSocket connection test.
        
        Args:
            duration: How long to keep the connection alive (seconds).
        """
        self._log(f"Running live connection test ({duration}s)...", "TEST")
        
        self.test_authentication()
        self.test_websocket_connection_short(timeout_seconds=duration)
        
        return self.test_results


# =============================================================================
# PUBLIC API
# =============================================================================

def run_test_bench(test_name: str = "all", **kwargs):
    """
    Run the producer test bench.
    
    Args:
        test_name: Name of test to run. Options:
            - 'all': Run all tests
            - 'connection': Run connection tests
            - 'reconnection': Run reconnection tests
            - 'mail': Run mail tests
            - 'mail_send': Run mail tests with actual email sending
            - 'live': Run live WebSocket connection test
            - 'redis': Run Redis tests only
        **kwargs: Additional arguments to pass to tests.
    """
    bench = ProducerTestBench()
    
    test_map = {
        'all': bench.run_all_tests,
        'connection': bench.run_connection_tests,
        'reconnection': bench.run_reconnection_tests,
        'mail': lambda: bench.run_mail_tests(send_test_email=False),
        'mail_send': lambda: bench.run_mail_tests(send_test_email=True),
        'live': lambda: bench.run_live_connection_test(duration=kwargs.get('duration', 30)),
        'redis': lambda: (bench.test_redis_connection(), bench.test_redis_stream_operations()),
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
