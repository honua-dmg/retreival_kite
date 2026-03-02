"""
Test Bench for Main.py - Orchestration Module

Tests market time calculations, holiday detection, workflow orchestration,
and end-of-day report generation.

Usage:
    python src/test_main.py all              # Run all tests
    python src/test_main.py time             # Time calculation tests
    python src/test_main.py holiday          # Holiday detection tests
    python src/test_main.py workflow         # Workflow orchestration tests
    python src/test_main.py report           # Report generation tests
"""

import os
import sys
import threading
import datetime as dt
from typing import Dict, List, Tuple
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from Main import (
    _seconds_until_market_open,
    _is_after_market_close,
    get_holidays,
    is_market_open_today,
)
from utils import IST_ZONE


class MainTestBench:
    """
    Test bench for the Main orchestration module.
    
    Uses mocking to test time-dependent functions without waiting.
    """
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.results: List[Tuple[str, bool, str]] = []
    
    def _log(self, test_name: str, passed: bool, message: str = ""):
        """Log test result."""
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {test_name}")
        if message and not passed:
            print(f"         {message}")
        
        if passed:
            self.passed += 1
        else:
            self.failed += 1
        self.results.append((test_name, passed, message))
    
    # =========================================================================
    # Category 1: Time Calculation Tests
    # =========================================================================
    
    def test_seconds_until_market_open_before_open(self) -> bool:
        """Test that correct seconds are calculated before market opens."""
        # Mock time to 8:15 AM IST (1 hour before market open at 9:15)
        mock_time = dt.datetime(2026, 3, 2, 8, 15, 0, tzinfo=IST_ZONE)
        
        with patch('Main.get_ist_now', return_value=mock_time):
            seconds = _seconds_until_market_open()
            # Should be 3600 seconds (1 hour)
            expected = 3600
            passed = seconds == expected
            
            self._log(
                "test_seconds_until_market_open_before_open",
                passed,
                f"Expected {expected}s, got {seconds}s"
            )
            return passed
    
    def test_seconds_until_market_open_after_open(self) -> bool:
        """Test that 0 is returned when market is already open."""
        # Mock time to 10:00 AM IST (after 9:15 open)
        mock_time = dt.datetime(2026, 3, 2, 10, 0, 0, tzinfo=IST_ZONE)
        
        with patch('Main.get_ist_now', return_value=mock_time):
            seconds = _seconds_until_market_open()
            passed = seconds == 0
            
            self._log(
                "test_seconds_until_market_open_after_open",
                passed,
                f"Expected 0s, got {seconds}s"
            )
            return passed
    
    def test_seconds_until_market_open_exactly_at_open(self) -> bool:
        """Test that 0 is returned at exactly market open time."""
        # Mock time to exactly 9:15 AM IST
        mock_time = dt.datetime(2026, 3, 2, 9, 15, 0, tzinfo=IST_ZONE)
        
        with patch('Main.get_ist_now', return_value=mock_time):
            seconds = _seconds_until_market_open()
            passed = seconds == 0
            
            self._log(
                "test_seconds_until_market_open_exactly_at_open",
                passed,
                f"Expected 0s, got {seconds}s"
            )
            return passed
    
    def test_is_after_market_close_before_close(self) -> bool:
        """Test that False is returned before market close."""
        # Mock time to 2:00 PM IST (before 3:30 close)
        mock_time = dt.datetime(2026, 3, 2, 14, 0, 0, tzinfo=IST_ZONE)
        
        with patch('Main.get_ist_now', return_value=mock_time):
            result = _is_after_market_close()
            passed = result is False
            
            self._log(
                "test_is_after_market_close_before_close",
                passed,
                f"Expected False, got {result}"
            )
            return passed
    
    def test_is_after_market_close_after_close(self) -> bool:
        """Test that True is returned after market close."""
        # Mock time to 4:00 PM IST (after 3:30 close)
        mock_time = dt.datetime(2026, 3, 2, 16, 0, 0, tzinfo=IST_ZONE)
        
        with patch('Main.get_ist_now', return_value=mock_time):
            result = _is_after_market_close()
            passed = result is True
            
            self._log(
                "test_is_after_market_close_after_close",
                passed,
                f"Expected True, got {result}"
            )
            return passed
    
    def test_is_after_market_close_exactly_at_close(self) -> bool:
        """Test that True is returned at exactly market close time."""
        # Mock time to exactly 3:30 PM IST
        mock_time = dt.datetime(2026, 3, 2, 15, 30, 0, tzinfo=IST_ZONE)
        
        with patch('Main.get_ist_now', return_value=mock_time):
            result = _is_after_market_close()
            passed = result is True
            
            self._log(
                "test_is_after_market_close_exactly_at_close",
                passed,
                f"Expected True, got {result}"
            )
            return passed
    
    # =========================================================================
    # Category 2: Holiday Detection Tests
    # =========================================================================
    
    def test_get_holidays_returns_list(self) -> bool:
        """Test that get_holidays returns a list (even if empty on failure)."""
        result = get_holidays()
        passed = isinstance(result, list)
        
        self._log(
            "test_get_holidays_returns_list",
            passed,
            f"Got type: {type(result).__name__}"
        )
        return passed
    
    def test_get_holidays_network_failure_graceful(self) -> bool:
        """Test that get_holidays handles network errors gracefully."""
        with patch('Main.requests.get', side_effect=Exception("Network error")):
            result = get_holidays()
            passed = result == []
            
            self._log(
                "test_get_holidays_network_failure_graceful",
                passed,
                f"Expected [], got {result}"
            )
            return passed
    
    def test_is_market_open_weekday(self) -> bool:
        """Test that market is open on a weekday (non-holiday)."""
        # Mock Monday with no holidays
        mock_time = dt.datetime(2026, 3, 2, 10, 0, 0, tzinfo=IST_ZONE)  # Monday
        
        with patch('Main.get_ist_now', return_value=mock_time):
            with patch('Main.get_holidays', return_value=[]):
                result = is_market_open_today()
                passed = result is True
                
                self._log(
                    "test_is_market_open_weekday",
                    passed,
                    f"Expected True, got {result}"
                )
                return passed
    
    def test_is_market_closed_saturday(self) -> bool:
        """Test that market is closed on Saturday."""
        # Mock Saturday
        mock_time = dt.datetime(2026, 3, 7, 10, 0, 0, tzinfo=IST_ZONE)  # Saturday
        
        with patch('Main.get_ist_now', return_value=mock_time):
            result = is_market_open_today()
            passed = result is False
            
            self._log(
                "test_is_market_closed_saturday",
                passed,
                f"Expected False, got {result}"
            )
            return passed
    
    def test_is_market_closed_sunday(self) -> bool:
        """Test that market is closed on Sunday."""
        # Mock Sunday
        mock_time = dt.datetime(2026, 3, 8, 10, 0, 0, tzinfo=IST_ZONE)  # Sunday
        
        with patch('Main.get_ist_now', return_value=mock_time):
            result = is_market_open_today()
            passed = result is False
            
            self._log(
                "test_is_market_closed_sunday",
                passed,
                f"Expected False, got {result}"
            )
            return passed
    
    def test_is_market_closed_holiday(self) -> bool:
        """Test that market is closed on a holiday."""
        # Mock a weekday that's a holiday (Republic Day)
        mock_time = dt.datetime(2026, 1, 26, 10, 0, 0, tzinfo=IST_ZONE)  # Monday, Jan 26
        
        with patch('Main.get_ist_now', return_value=mock_time):
            with patch('Main.get_holidays', return_value=['26-Jan-2026']):
                result = is_market_open_today()
                passed = result is False
                
                self._log(
                    "test_is_market_closed_holiday",
                    passed,
                    f"Expected False, got {result}"
                )
                return passed
    
    # =========================================================================
    # Category 3: Workflow Orchestration Tests
    # =========================================================================
    
    def test_redis_client_available(self) -> bool:
        """Test that Redis client is accessible from config."""
        try:
            r = config.redis_client
            r.ping()
            passed = True
        except Exception as e:
            passed = False
            self._log("test_redis_client_available", False, str(e))
            return False
        
        self._log("test_redis_client_available", passed, "")
        return passed
    
    def test_config_constants_defined(self) -> bool:
        """Test that required config constants are defined."""
        required = [
            'MARKET_OPEN_HOUR',
            'MARKET_OPEN_MINUTE', 
            'MARKET_CLOSE_HOUR',
            'MARKET_CLOSE_MINUTE',
            'DATA_PATH',
            'DEFAULT_NUM_CONSUMERS',
            'ENVLOC'
        ]
        
        missing = []
        for const in required:
            if not hasattr(config, const):
                missing.append(const)
        
        passed = len(missing) == 0
        self._log(
            "test_config_constants_defined",
            passed,
            f"Missing: {missing}" if missing else ""
        )
        return passed
    
    def test_data_path_exists_or_creatable(self) -> bool:
        """Test that DATA_PATH exists or can be created."""
        path = config.DATA_PATH
        
        if os.path.exists(path):
            passed = True
            msg = f"Path exists: {path}"
        elif path.startswith('/app'):
            # Docker path - skip when running locally
            passed = True
            msg = f"Skipped (Docker path): {path}"
        else:
            try:
                os.makedirs(path, exist_ok=True)
                passed = os.path.exists(path)
                msg = f"Created: {path}"
            except Exception as e:
                passed = False
                self._log("test_data_path_exists_or_creatable", False, str(e))
                return False
        
        self._log(
            "test_data_path_exists_or_creatable",
            passed,
            msg
        )
        return passed
    
    def test_producer_module_importable(self) -> bool:
        """Test that producer module is importable."""
        try:
            import producer
            has_heartbeat = hasattr(producer, 'heartbeat_monitor')
            passed = has_heartbeat
            
            self._log(
                "test_producer_module_importable",
                passed,
                "Missing heartbeat_monitor" if not passed else ""
            )
            return passed
        except ImportError as e:
            self._log("test_producer_module_importable", False, str(e))
            return False
    
    def test_consumers_module_importable(self) -> bool:
        """Test that Consumers module is importable."""
        try:
            import Consumers
            has_start = hasattr(Consumers, 'start_consumer_threads')
            passed = has_start
            
            self._log(
                "test_consumers_module_importable",
                passed,
                "Missing start_consumer_threads" if not passed else ""
            )
            return passed
        except ImportError as e:
            self._log("test_consumers_module_importable", False, str(e))
            return False
    
    def test_report_module_importable(self) -> bool:
        """Test that report module is importable."""
        try:
            import report
            has_count = hasattr(report, 'count')
            has_build = hasattr(report, 'build_email_body')
            has_report = hasattr(report, 'report')
            passed = has_count and has_build and has_report
            
            self._log(
                "test_report_module_importable",
                passed,
                "Missing required functions" if not passed else ""
            )
            return passed
        except ImportError as e:
            self._log("test_report_module_importable", False, str(e))
            return False
    
    def test_upload_module_importable(self) -> bool:
        """Test that upload module is importable."""
        try:
            from upload import Upload
            passed = True
            
            self._log("test_upload_module_importable", passed, "")
            return passed
        except ImportError as e:
            self._log("test_upload_module_importable", False, str(e))
            return False
    
    # =========================================================================
    # Category 4: Report Generation Tests
    # =========================================================================
    
    def test_report_count_function(self) -> bool:
        """Test that report.count returns expected format."""
        import report
        
        # Call with existing data path
        path = os.path.join(config.DATA_PATH, 'NSE')
        date = dt.datetime.now(IST_ZONE).strftime('%Y-%m-%d')
        
        try:
            result = report.count(path=path, date=date)
            # Should return list of tuples
            passed = isinstance(result, list)
            
            self._log(
                "test_report_count_function",
                passed,
                f"Got type: {type(result).__name__}"
            )
            return passed
        except Exception as e:
            self._log("test_report_count_function", False, str(e))
            return False
    
    def test_report_build_email_body(self) -> bool:
        """Test that build_email_body generates valid HTML/text."""
        import report
        
        try:
            body = report.build_email_body(
                redis_count=1000,
                nse_data=[('RELIANCE', 500), ('INFY', 300), ('total', 800)],
                bse_data=[('RELIANCE', 200), ('total', 200)],
                extra_sections={'Test': 'Value'}
            )
            
            # Should be non-empty string
            passed = isinstance(body, str) and len(body) > 0
            
            self._log(
                "test_report_build_email_body",
                passed,
                f"Body length: {len(body) if body else 0}"
            )
            return passed
        except Exception as e:
            self._log("test_report_build_email_body", False, str(e))
            return False
    
    def test_redis_end_flag_settable(self) -> bool:
        """Test that 'end' flag can be set in Redis."""
        try:
            r = config.redis_client
            r.set('end', 'false')
            value = r.get('end')
            
            passed = value == b'false' or value == 'false'
            
            self._log(
                "test_redis_end_flag_settable",
                passed,
                f"Got: {value}"
            )
            return passed
        except Exception as e:
            self._log("test_redis_end_flag_settable", False, str(e))
            return False
    
    def test_redis_time_tracking(self) -> bool:
        """Test that timestamp can be stored in Redis."""
        try:
            r = config.redis_client
            from utils import get_ist_now
            
            timestamp = get_ist_now().timestamp()
            r.set('time', timestamp)
            value = float(r.get('time'))
            
            passed = abs(value - timestamp) < 1  # Within 1 second
            
            self._log(
                "test_redis_time_tracking",
                passed,
                f"Stored: {timestamp}, Retrieved: {value}"
            )
            return passed
        except Exception as e:
            self._log("test_redis_time_tracking", False, str(e))
            return False
    
    # =========================================================================
    # Test Runner
    # =========================================================================
    
    def run_category(self, category: str) -> Tuple[int, int]:
        """Run tests for a specific category."""
        categories = {
            'time': [
                self.test_seconds_until_market_open_before_open,
                self.test_seconds_until_market_open_after_open,
                self.test_seconds_until_market_open_exactly_at_open,
                self.test_is_after_market_close_before_close,
                self.test_is_after_market_close_after_close,
                self.test_is_after_market_close_exactly_at_close,
            ],
            'holiday': [
                self.test_get_holidays_returns_list,
                self.test_get_holidays_network_failure_graceful,
                self.test_is_market_open_weekday,
                self.test_is_market_closed_saturday,
                self.test_is_market_closed_sunday,
                self.test_is_market_closed_holiday,
            ],
            'workflow': [
                self.test_redis_client_available,
                self.test_config_constants_defined,
                self.test_data_path_exists_or_creatable,
                self.test_producer_module_importable,
                self.test_consumers_module_importable,
                self.test_report_module_importable,
                self.test_upload_module_importable,
            ],
            'report': [
                self.test_report_count_function,
                self.test_report_build_email_body,
                self.test_redis_end_flag_settable,
                self.test_redis_time_tracking,
            ],
        }
        
        if category not in categories:
            print(f"Unknown category: {category}")
            print(f"Available: {list(categories.keys())}")
            return 0, 0
        
        print(f"\n{'='*60}")
        print(f"  {category.upper()} TESTS")
        print(f"{'='*60}")
        
        for test_func in categories[category]:
            test_func()
        
        return self.passed, self.failed
    
    def run_all(self) -> Tuple[int, int]:
        """Run all test categories."""
        for category in ['time', 'holiday', 'workflow', 'report']:
            self.run_category(category)
        
        return self.passed, self.failed
    
    def print_summary(self):
        """Print test summary."""
        print(f"\n{'='*60}")
        print(f"  TEST SUMMARY")
        print(f"{'='*60}")
        total = self.passed + self.failed
        print(f"  Total: {total} | Passed: {self.passed} | Failed: {self.failed}")
        
        if self.failed > 0:
            print(f"\n  Failed tests:")
            for name, passed, msg in self.results:
                if not passed:
                    print(f"    - {name}: {msg}")
        
        print(f"{'='*60}\n")


def main():
    """Main entry point for test bench."""
    bench = MainTestBench()
    
    if len(sys.argv) < 2:
        print("Usage: python test_main.py <category|all>")
        print("Categories: time, holiday, workflow, report, all")
        sys.exit(1)
    
    category = sys.argv[1].lower()
    
    if category == 'all':
        bench.run_all()
    else:
        bench.run_category(category)
    
    bench.print_summary()
    
    sys.exit(0 if bench.failed == 0 else 1)


if __name__ == "__main__":
    main()
