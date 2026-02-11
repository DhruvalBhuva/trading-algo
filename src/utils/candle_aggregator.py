import os
import sys
from typing import Optional
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.candle_data_store import CandleDataStore


class CandleAggregator:
    """
    Build time-based candles from real-time tick data (BID price).

    - Supports multiple epics
    - Uses BID price for candle OHLC
    - Tracks last tick for execution logic
    - Emits candle immediately on close
    """

    def __init__(self, resolution: str = "MINUTE_15"):
        self.resolution_seconds = self._get_resolution_second(resolution)

        # Candle state per epic
        self.candle_data: dict[str, dict] = {}
        self.current_candle_start: dict[str, datetime] = {}
        self.last_candle: dict[str, dict] = {}

        # Real-time tracking
        self.last_price: dict[str, float] = {}  # BID price
        self.last_tick: dict[str, dict] = {}

        # Statistics
        self.tick_count = 0
        self.last_tick_time: Optional[datetime] = None
        self.last_candle_time: Optional[datetime] = None

        # CSV persistence
        self.csv_storage = CandleDataStore(base_path="data/historical-data")

    # -------------------------------------------------
    # Resolution Helpers
    # -------------------------------------------------
    def _get_resolution_second(self, resolution: str) -> int:
        mapping = {
            "MINUTE": 60,
            "MINUTE_5": 300,
            "MINUTE_15": 900,
            "MINUTE_30": 1800,
            "HOUR_1": 3600,
            "HOUR_4": 14400,
            "DAY_1": 86400,
            "WEEK_1": 604800,
        }

        return mapping.get(resolution)

    # -------------------------------------------------
    # Configuration
    # -------------------------------------------------
    def set_csv_storage(self, csv_storage: CandleDataStore):
        self.csv_storage = csv_storage

    # -------------------------------------------------
    # Core Tick Processing
    # -------------------------------------------------
    def process_tick(self, tick: dict) -> Optional[dict]:
        """
        Process a single tick.
        Returns a completed candle if one closed, else None.
        """
        epic = tick["epic"]
        bid_price = tick["bid"]
        timestamp = tick["timestamp"]

        # --- realtime tracking
        self.last_price[epic] = bid_price
        self.last_tick[epic] = tick

        self.tick_count += 1
        self.last_tick_time = datetime.now()

        candle_start = self._calculate_candle_start(timestamp)

        # --- first candle
        if epic not in self.candle_data:
            self._start_new_candle(epic, bid_price, candle_start)
            return None

        # --- candle rollover
        if candle_start > self.current_candle_start[epic]:
            closed = self._close_candle(epic, tick)
            self._start_new_candle(epic, bid_price, candle_start)
            return closed

        # --- update active candle
        candle = self.candle_data[epic]
        candle["high"] = max(candle["high"], bid_price)
        candle["low"] = min(candle["low"], bid_price)
        candle["close"] = bid_price
        candle["volume"] += 1
        candle["last_update"] = timestamp

        return None

    # -------------------------------------------------
    # Time Calculation
    # -------------------------------------------------
    def _calculate_candle_start(self, timestamp: datetime) -> datetime:
        if self.resolution_seconds >= 60:
            minutes = self.resolution_seconds // 60
            return timestamp.replace(
                second=0,
                microsecond=0,
                minute=timestamp.minute - (timestamp.minute % minutes),
            )

        return timestamp.replace(
            microsecond=0,
            second=timestamp.second - (timestamp.second % self.resolution_seconds),
        )

    # -------------------------------------------------
    # Candle Lifecycle
    # -------------------------------------------------
    def _start_new_candle(self, epic: str, price: float, start_time: datetime):
        self.candle_data[epic] = {
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": 1,
            "start_time": start_time,
        }
        self.current_candle_start[epic] = start_time

    def _close_candle(self, epic: str, tick: dict) -> dict:
        candle = self.candle_data[epic].copy()

        candle["epic"] = epic
        candle["end_time"] = self.current_candle_start[epic] + timedelta(
            seconds=self.resolution_seconds
        )
        candle["closed_at"] = tick.get("received_at", datetime.now())

        self.last_candle[epic] = candle
        self.last_candle_time = candle["end_time"]

        if self.csv_storage:
            try:
                self.csv_storage.save_candle(candle, self.resolution_seconds)
            except Exception as e:
                print(f"⚠️ CSV save failed: {e}")

        return candle

    # -------------------------------------------------
    # Public Accessors
    # -------------------------------------------------
    def get_current_price(self, epic: str) -> Optional[float]:
        """Latest BID price."""
        return self.last_price.get(epic)

    def get_last_tick(self, epic: str) -> Optional[dict]:
        return self.last_tick.get(epic)

    def get_current_candle(self, epic: str) -> Optional[dict]:
        if epic not in self.candle_data:
            return None

        candle = self.candle_data[epic].copy()
        candle["epic"] = epic
        candle["is_complete"] = False
        return candle

    def get_last_candle(self, epic: str) -> Optional[dict]:
        return self.last_candle.get(epic)

    def get_stats(self) -> dict:
        time_since_last = None
        if self.last_tick_time:
            time_since_last = (datetime.now() - self.last_tick_time).total_seconds()

        return {
            "ticks_received": self.tick_count,
            "time_since_last_tick": time_since_last,
            "last_candle_time": self.last_candle_time,
            "active_epics": list(self.candle_data.keys()),
        }
