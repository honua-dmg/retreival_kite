"""
Test Bench for Save.py - CSV Persistence Module

Tests directory structure, CSV headers, token conversion,
tick saving, depth data handling, date handling, and edge cases.

Usage:
    python src/test_save.py all              # Run all tests
    python src/test_save.py directory        # Directory structure tests
    python src/test_save.py headers          # CSV header tests
    python src/test_save.py tokens           # Token conversion tests
    python src/test_save.py saving           # Tick saving tests
    python src/test_save.py depth            # Depth data tests
    python src/test_save.py dates            # Date handling tests
    python src/test_save.py edge             # Edge case tests
"""

import os
import sys
import csv
import tempfile
import shutil
import threading
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from Save import CSV, CSV_HEADER
from utils import token_to_stock_mapping, IST_ZONE


class SaveTestBench:
    """
    Test bench for the CSV persistence module.
    
    All tests use temporary directories to avoid polluting real data.
    """
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.results: List[Tuple[str, bool, str]] = []
        
        # Load actual token mappings for realistic tests
        self.nse_tokens = token_to_stock_mapping("NSE")
        self.bse_tokens = token_to_stock_mapping("BSE")
        
        # Get sample tokens for testing
        self.sample_nse_token = list(self.nse_tokens.keys())[0] if self.nse_tokens else None
        self.sample_nse_stock = self.nse_tokens.get(self.sample_nse_token) if self.sample_nse_token else "RELIANCE"
        
        self.sample_bse_token = list(self.bse_tokens.keys())[0] if self.bse_tokens else None
        self.sample_bse_stock = self.bse_tokens.get(self.sample_bse_token) if self.sample_bse_token else "RELIANCE"
    
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
    
    def _create_temp_dir(self) -> str:
        """Create a temporary directory for testing."""
        return tempfile.mkdtemp(prefix="save_test_")
    
    def _cleanup_temp_dir(self, temp_dir: str):
        """Remove temporary directory."""
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    
    def _create_mock_tick(
        self,
        token: int,
        last_price: float = 1500.0,
        include_depth: bool = True,
        depth_levels: int = 5
    ) -> Dict:
        """
        Create a mock tick matching KiteTicker format.
        
        Args:
            token: Instrument token
            last_price: Last traded price
            include_depth: Whether to include depth data
            depth_levels: Number of buy/sell levels (0-5)
        """
        tick = {
            'instrument_token': token,
            'last_price': last_price,
            'last_traded_quantity': 100,
            'average_traded_price': last_price - 5,
            'volume_traded': 1000000,
            'total_buy_quantity': 50000,
            'total_sell_quantity': 45000,
            'ohlc': {
                'open': last_price - 10,
                'high': last_price + 20,
                'low': last_price - 15,
                'close': last_price - 5
            },
            'change': 2.5,
            'oi': 10000,
            'oi_day_high': 12000,
            'oi_day_low': 9000
        }
        
        if include_depth:
            buy_depth = []
            sell_depth = []
            
            for i in range(depth_levels):
                buy_depth.append({
                    'price': last_price - (i + 1) * 0.5,
                    'quantity': 1000 * (i + 1),
                    'orders': 10 + i
                })
                sell_depth.append({
                    'price': last_price + (i + 1) * 0.5,
                    'quantity': 900 * (i + 1),
                    'orders': 8 + i
                })
            
            tick['depth'] = {'buy': buy_depth, 'sell': sell_depth}
        
        return tick
    
    # =========================================================================
    # Category 1: Directory Structure Tests
    # =========================================================================
    
    def test_exchange_directories_created(self) -> bool:
        """Test that initialise() creates NSE/ and BSE/ directories."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            nse_exists = os.path.isdir(os.path.join(temp_dir, "NSE"))
            bse_exists = os.path.isdir(os.path.join(temp_dir, "BSE"))
            
            passed = nse_exists and bse_exists
            self._log(
                "test_exchange_directories_created",
                passed,
                f"NSE: {nse_exists}, BSE: {bse_exists}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_stock_directories_created(self) -> bool:
        """Test that stock subdirectories are created for tracked stocks."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            # Check a few stocks exist in both exchanges
            stocks_to_check = csv_manager.stocks[:3] if len(csv_manager.stocks) >= 3 else csv_manager.stocks
            
            all_exist = True
            missing = []
            
            for stock in stocks_to_check:
                nse_path = os.path.join(temp_dir, "NSE", stock)
                bse_path = os.path.join(temp_dir, "BSE", stock)
                
                if not os.path.isdir(nse_path):
                    all_exist = False
                    missing.append(f"NSE/{stock}")
                if not os.path.isdir(bse_path):
                    all_exist = False
                    missing.append(f"BSE/{stock}")
            
            self._log(
                "test_stock_directories_created",
                all_exist,
                f"Missing: {missing}" if missing else ""
            )
            return all_exist
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_idempotent_initialisation(self) -> bool:
        """Test that calling initialise() multiple times is safe."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            
            # Call initialise() three times
            csv_manager.initialise()
            csv_manager.initialise()
            csv_manager.initialise()
            
            # Count files - should have exactly one CSV per stock per exchange
            nse_files = 0
            bse_files = 0
            
            for stock in csv_manager.stocks:
                nse_dir = os.path.join(temp_dir, "NSE", stock)
                bse_dir = os.path.join(temp_dir, "BSE", stock)
                
                if os.path.isdir(nse_dir):
                    nse_files += len([f for f in os.listdir(nse_dir) if f.endswith('.csv')])
                if os.path.isdir(bse_dir):
                    bse_files += len([f for f in os.listdir(bse_dir) if f.endswith('.csv')])
            
            expected = len(csv_manager.stocks)
            passed = nse_files == expected and bse_files == expected
            
            self._log(
                "test_idempotent_initialisation",
                passed,
                f"Expected {expected} files each, got NSE:{nse_files}, BSE:{bse_files}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    # =========================================================================
    # Category 2: CSV Header Tests
    # =========================================================================
    
    def test_csv_header_written(self) -> bool:
        """Test that new CSV files have the correct header row."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            # Read first stock's CSV file
            first_stock = csv_manager.stocks[0]
            file_path = os.path.join(temp_dir, "NSE", first_stock, f"{csv_manager.date}.csv")
            
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                header = next(reader)
            
            passed = header == CSV_HEADER
            self._log(
                "test_csv_header_written",
                passed,
                f"Header mismatch: got {len(header)} columns" if not passed else ""
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_header_column_count(self) -> bool:
        """Test that header has exactly 46 columns (16 base + 30 depth)."""
        # 16 base columns + 5 buy levels * 3 + 5 sell levels * 3 = 16 + 15 + 15 = 46
        expected_columns = 46
        actual_columns = len(CSV_HEADER)
        
        passed = actual_columns == expected_columns
        self._log(
            "test_header_column_count",
            passed,
            f"Expected {expected_columns}, got {actual_columns}"
        )
        return passed
    
    def test_existing_file_header_preserved(self) -> bool:
        """Test that _init_columns() doesn't duplicate header in existing file."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            first_stock = csv_manager.stocks[0]
            file_path = os.path.join(temp_dir, "NSE", first_stock, f"{csv_manager.date}.csv")
            
            # Save a tick (adds data row)
            if self.sample_nse_token:
                tick = self._create_mock_tick(self.sample_nse_token)
                # Temporarily override stocks to match our test
                original_stocks = csv_manager.stocks
                csv_manager.stocks = [self.sample_nse_stock]
                csv_manager.save_tick(tick)
                csv_manager.stocks = original_stocks
            
            # Call _init_columns again
            csv_manager._init_columns(file_path)
            
            # Count header rows
            header_count = 0
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    if row == CSV_HEADER:
                        header_count += 1
            
            passed = header_count == 1
            self._log(
                "test_existing_file_header_preserved",
                passed,
                f"Found {header_count} header rows, expected 1"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    # =========================================================================
    # Category 3: Token Conversion Tests
    # =========================================================================
    
    def test_nse_token_conversion(self) -> bool:
        """Test conversion of NSE token to EXCHANGE:SYMBOL format."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            
            if not self.sample_nse_token:
                self._log("test_nse_token_conversion", False, "No NSE tokens available")
                return False
            
            result = csv_manager._convert_token(self.sample_nse_token)
            expected = f"NSE:{self.sample_nse_stock}"
            
            passed = result == expected
            self._log(
                "test_nse_token_conversion",
                passed,
                f"Expected {expected}, got {result}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_bse_token_conversion(self) -> bool:
        """Test conversion of BSE token to EXCHANGE:SYMBOL format."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            
            if not self.sample_bse_token:
                self._log("test_bse_token_conversion", False, "No BSE tokens available")
                return False
            
            result = csv_manager._convert_token(self.sample_bse_token)
            expected = f"BSE:{self.sample_bse_stock}"
            
            passed = result == expected
            self._log(
                "test_bse_token_conversion",
                passed,
                f"Expected {expected}, got {result}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_unknown_token_returns_none(self) -> bool:
        """Test that invalid token returns None without crashing."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            
            # Use an obviously invalid token
            invalid_token = 999999999
            result = csv_manager._convert_token(invalid_token)
            
            passed = result is None
            self._log(
                "test_unknown_token_returns_none",
                passed,
                f"Expected None, got {result}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    # =========================================================================
    # Category 4: Tick Saving Tests
    # =========================================================================
    
    def test_save_single_tick(self) -> bool:
        """Test saving a single tick writes one row to correct file."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            if not self.sample_nse_token:
                self._log("test_save_single_tick", False, "No NSE tokens available")
                return False
            
            tick = self._create_mock_tick(self.sample_nse_token, last_price=1500.0)
            csv_manager.save_tick(tick)
            
            # Read file and count data rows
            file_path = os.path.join(temp_dir, "NSE", self.sample_nse_stock, f"{csv_manager.date}.csv")
            
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                rows = list(reader)
            
            # Should have header + 1 data row
            data_rows = len(rows) - 1
            passed = data_rows == 1
            
            self._log(
                "test_save_single_tick",
                passed,
                f"Expected 1 data row, got {data_rows}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_save_multiple_ticks(self) -> bool:
        """Test saving 100 ticks writes exactly 100 rows."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            if not self.sample_nse_token:
                self._log("test_save_multiple_ticks", False, "No NSE tokens available")
                return False
            
            # Save 100 ticks
            for i in range(100):
                tick = self._create_mock_tick(self.sample_nse_token, last_price=1500.0 + i)
                csv_manager.save_tick(tick)
            
            # Count rows
            file_path = os.path.join(temp_dir, "NSE", self.sample_nse_stock, f"{csv_manager.date}.csv")
            
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                rows = list(reader)
            
            data_rows = len(rows) - 1
            passed = data_rows == 100
            
            self._log(
                "test_save_multiple_ticks",
                passed,
                f"Expected 100 data rows, got {data_rows}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_tick_timestamp_format(self) -> bool:
        """Test that timestamp is in HH:MM:SS IST format."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            if not self.sample_nse_token:
                self._log("test_tick_timestamp_format", False, "No NSE tokens available")
                return False
            
            tick = self._create_mock_tick(self.sample_nse_token)
            csv_manager.save_tick(tick)
            
            file_path = os.path.join(temp_dir, "NSE", self.sample_nse_stock, f"{csv_manager.date}.csv")
            
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                data_row = next(reader)
            
            timestamp = data_row[0]
            
            # Validate format HH:MM:SS
            try:
                dt.datetime.strptime(timestamp, "%H:%M:%S")
                passed = True
            except ValueError:
                passed = False
            
            self._log(
                "test_tick_timestamp_format",
                passed,
                f"Timestamp: {timestamp}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_tick_file_routing(self) -> bool:
        """Test NSE tick goes to NSE folder, BSE tick to BSE folder."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            if not self.sample_nse_token or not self.sample_bse_token:
                self._log("test_tick_file_routing", False, "Need both NSE and BSE tokens")
                return False
            
            # Save NSE tick
            nse_tick = self._create_mock_tick(self.sample_nse_token, last_price=1500.0)
            csv_manager.save_tick(nse_tick)
            
            # Save BSE tick
            bse_tick = self._create_mock_tick(self.sample_bse_token, last_price=1500.0)
            csv_manager.save_tick(bse_tick)
            
            # Check NSE file has 1 row
            nse_file = os.path.join(temp_dir, "NSE", self.sample_nse_stock, f"{csv_manager.date}.csv")
            bse_file = os.path.join(temp_dir, "BSE", self.sample_bse_stock, f"{csv_manager.date}.csv")
            
            def count_data_rows(filepath):
                if not os.path.exists(filepath):
                    return 0
                with open(filepath, 'r') as f:
                    return sum(1 for _ in f) - 1  # Subtract header
            
            nse_rows = count_data_rows(nse_file)
            bse_rows = count_data_rows(bse_file)
            
            passed = nse_rows >= 1 and bse_rows >= 1
            
            self._log(
                "test_tick_file_routing",
                passed,
                f"NSE rows: {nse_rows}, BSE rows: {bse_rows}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_partial_tick_data(self) -> bool:
        """Test saving tick with missing optional fields doesn't crash."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            if not self.sample_nse_token:
                self._log("test_partial_tick_data", False, "No NSE tokens available")
                return False
            
            # Create minimal tick (no depth, no OI, etc.)
            partial_tick = {
                'instrument_token': self.sample_nse_token,
                'last_price': 1500.0
                # All other fields missing
            }
            
            try:
                csv_manager.save_tick(partial_tick)
                passed = True
            except Exception as e:
                passed = False
                self._log("test_partial_tick_data", False, str(e))
                return False
            
            # Verify row was written
            file_path = os.path.join(temp_dir, "NSE", self.sample_nse_stock, f"{csv_manager.date}.csv")
            
            with open(file_path, 'r') as f:
                rows = list(csv.reader(f))
            
            passed = len(rows) == 2  # Header + 1 data row
            
            self._log(
                "test_partial_tick_data",
                passed,
                f"Rows written: {len(rows) - 1}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_invalid_token_tick_ignored(self) -> bool:
        """Test that tick with unknown token is silently skipped."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            # Create tick with invalid token
            invalid_tick = self._create_mock_tick(999999999, last_price=1500.0)
            
            try:
                csv_manager.save_tick(invalid_tick)
                passed = True
            except Exception as e:
                passed = False
                self._log("test_invalid_token_tick_ignored", False, str(e))
                return False
            
            self._log("test_invalid_token_tick_ignored", passed, "")
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    # =========================================================================
    # Category 5: Depth Data Tests
    # =========================================================================
    
    def test_full_depth_data(self) -> bool:
        """Test tick with all 5 buy/sell levels populates 30 depth columns."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            if not self.sample_nse_token:
                self._log("test_full_depth_data", False, "No NSE tokens available")
                return False
            
            tick = self._create_mock_tick(self.sample_nse_token, include_depth=True, depth_levels=5)
            csv_manager.save_tick(tick)
            
            file_path = os.path.join(temp_dir, "NSE", self.sample_nse_stock, f"{csv_manager.date}.csv")
            
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                data_row = next(reader)
            
            # Depth columns start at index 16 (after base columns)
            depth_columns = data_row[16:]
            
            # All 30 depth columns should have values
            non_empty = [c for c in depth_columns if c and c != 'None']
            passed = len(non_empty) == 30
            
            self._log(
                "test_full_depth_data",
                passed,
                f"Expected 30 depth values, got {len(non_empty)}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_partial_depth_data(self) -> bool:
        """Test tick with only 2 buy/sell levels handles sparse depth."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            if not self.sample_nse_token:
                self._log("test_partial_depth_data", False, "No NSE tokens available")
                return False
            
            tick = self._create_mock_tick(self.sample_nse_token, include_depth=True, depth_levels=2)
            csv_manager.save_tick(tick)
            
            file_path = os.path.join(temp_dir, "NSE", self.sample_nse_stock, f"{csv_manager.date}.csv")
            
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                next(reader)
                data_row = next(reader)
            
            depth_columns = data_row[16:]
            
            # 2 levels * 3 fields * 2 (buy+sell) = 12 non-empty values
            non_empty = [c for c in depth_columns if c and c != 'None' and c != '']
            passed = len(non_empty) == 12
            
            self._log(
                "test_partial_depth_data",
                passed,
                f"Expected 12 depth values, got {len(non_empty)}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_empty_depth_data(self) -> bool:
        """Test tick with no depth has all None depth columns."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            if not self.sample_nse_token:
                self._log("test_empty_depth_data", False, "No NSE tokens available")
                return False
            
            tick = self._create_mock_tick(self.sample_nse_token, include_depth=False)
            csv_manager.save_tick(tick)
            
            file_path = os.path.join(temp_dir, "NSE", self.sample_nse_stock, f"{csv_manager.date}.csv")
            
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                next(reader)
                data_row = next(reader)
            
            depth_columns = data_row[16:]
            
            # All should be empty or None
            non_empty = [c for c in depth_columns if c and c != 'None' and c != '']
            passed = len(non_empty) == 0
            
            self._log(
                "test_empty_depth_data",
                passed,
                f"Expected 0 depth values, got {len(non_empty)}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    # =========================================================================
    # Category 6: Date Handling Tests
    # =========================================================================
    
    def test_date_in_filename(self) -> bool:
        """Test that file is named with today's IST date."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            first_stock = csv_manager.stocks[0]
            expected_filename = f"{csv_manager.date}.csv"
            
            nse_dir = os.path.join(temp_dir, "NSE", first_stock)
            files = os.listdir(nse_dir)
            
            passed = expected_filename in files
            
            self._log(
                "test_date_in_filename",
                passed,
                f"Expected {expected_filename}, found {files}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_date_rollover_simulation(self) -> bool:
        """Test that changing date creates new file."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            if not self.sample_nse_token:
                self._log("test_date_rollover_simulation", False, "No NSE tokens available")
                return False
            
            # Save tick with today's date
            tick = self._create_mock_tick(self.sample_nse_token)
            csv_manager.save_tick(tick)
            
            original_date = csv_manager.date
            
            # Simulate date change
            tomorrow = (dt.datetime.now(IST_ZONE) + dt.timedelta(days=1)).strftime("%Y-%m-%d")
            csv_manager.date = tomorrow
            
            # Re-initialize and save another tick
            csv_manager.initialise()
            csv_manager.save_tick(tick)
            
            # Check both files exist
            stock_dir = os.path.join(temp_dir, "NSE", self.sample_nse_stock)
            files = os.listdir(stock_dir) if os.path.exists(stock_dir) else []
            
            has_today = f"{original_date}.csv" in files
            has_tomorrow = f"{tomorrow}.csv" in files
            
            passed = has_today and has_tomorrow
            
            self._log(
                "test_date_rollover_simulation",
                passed,
                f"Files: {files}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    # =========================================================================
    # Category 7: Edge Cases & Robustness
    # =========================================================================
    
    def test_concurrent_writes(self) -> bool:
        """Test multiple threads writing ticks simultaneously."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            csv_manager.initialise()
            
            if not self.sample_nse_token:
                self._log("test_concurrent_writes", False, "No NSE tokens available")
                return False
            
            errors = []
            write_count = [0]
            lock = threading.Lock()
            
            def write_tick(tick_id: int):
                try:
                    tick = self._create_mock_tick(self.sample_nse_token, last_price=1500.0 + tick_id)
                    csv_manager.save_tick(tick)
                    with lock:
                        write_count[0] += 1
                except Exception as e:
                    errors.append(str(e))
            
            # Use 10 threads to write 100 ticks
            with ThreadPoolExecutor(max_workers=10) as executor:
                executor.map(write_tick, range(100))
            
            # Verify all writes succeeded
            file_path = os.path.join(temp_dir, "NSE", self.sample_nse_stock, f"{csv_manager.date}.csv")
            
            with open(file_path, 'r') as f:
                rows = list(csv.reader(f))
            
            data_rows = len(rows) - 1
            
            # Due to file locking, some writes may interleave but all should succeed
            passed = data_rows == 100 and len(errors) == 0
            
            self._log(
                "test_concurrent_writes",
                passed,
                f"Wrote {data_rows} rows, {len(errors)} errors"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_dynamic_stock_creation(self) -> bool:
        """Test saving tick for stock not in initial list creates directory."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            # Don't call initialise() - simulating dynamic stock
            
            if not self.sample_nse_token:
                self._log("test_dynamic_stock_creation", False, "No NSE tokens available")
                return False
            
            # Create directories manually for exchange only
            os.makedirs(os.path.join(temp_dir, "NSE"), exist_ok=True)
            os.makedirs(os.path.join(temp_dir, "BSE"), exist_ok=True)
            
            tick = self._create_mock_tick(self.sample_nse_token)
            csv_manager.save_tick(tick)
            
            # Check file was created
            file_path = os.path.join(temp_dir, "NSE", self.sample_nse_stock, f"{csv_manager.date}.csv")
            passed = os.path.exists(file_path)
            
            self._log(
                "test_dynamic_stock_creation",
                passed,
                f"File exists: {passed}"
            )
            return passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    def test_special_characters_handling(self) -> bool:
        """Test stock names with numbers and special chars are handled."""
        temp_dir = self._create_temp_dir()
        try:
            csv_manager = CSV(temp_dir)
            
            # Test creating directory with various stock name patterns
            test_names = ["STOCK123", "ABC-DEF", "TEST_STOCK"]
            
            all_passed = True
            for name in test_names:
                dir_path = os.path.join(temp_dir, "NSE", name)
                try:
                    os.makedirs(dir_path, exist_ok=True)
                    file_path = os.path.join(dir_path, f"{csv_manager.date}.csv")
                    with open(file_path, 'w') as f:
                        f.write("test")
                    
                    if not os.path.exists(file_path):
                        all_passed = False
                except Exception as e:
                    all_passed = False
            
            self._log(
                "test_special_characters_handling",
                all_passed,
                ""
            )
            return all_passed
        finally:
            self._cleanup_temp_dir(temp_dir)
    
    # =========================================================================
    # Test Runner
    # =========================================================================
    
    def run_category(self, category: str) -> Tuple[int, int]:
        """Run tests for a specific category."""
        categories = {
            'directory': [
                self.test_exchange_directories_created,
                self.test_stock_directories_created,
                self.test_idempotent_initialisation,
            ],
            'headers': [
                self.test_csv_header_written,
                self.test_header_column_count,
                self.test_existing_file_header_preserved,
            ],
            'tokens': [
                self.test_nse_token_conversion,
                self.test_bse_token_conversion,
                self.test_unknown_token_returns_none,
            ],
            'saving': [
                self.test_save_single_tick,
                self.test_save_multiple_ticks,
                self.test_tick_timestamp_format,
                self.test_tick_file_routing,
                self.test_partial_tick_data,
                self.test_invalid_token_tick_ignored,
            ],
            'depth': [
                self.test_full_depth_data,
                self.test_partial_depth_data,
                self.test_empty_depth_data,
            ],
            'dates': [
                self.test_date_in_filename,
                self.test_date_rollover_simulation,
            ],
            'edge': [
                self.test_concurrent_writes,
                self.test_dynamic_stock_creation,
                self.test_special_characters_handling,
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
        for category in ['directory', 'headers', 'tokens', 'saving', 'depth', 'dates', 'edge']:
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
    import sys
    
    bench = SaveTestBench()
    
    if len(sys.argv) < 2:
        print("Usage: python test_save.py <category|all>")
        print("Categories: directory, headers, tokens, saving, depth, dates, edge, all")
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
