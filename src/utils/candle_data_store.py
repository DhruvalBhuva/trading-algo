import os
import csv
import sys
import pandas as pd
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from src.logger import logger


class CandleDataStore:
    """Store candle data in CSV files organized by market and resolution."""

    def __init__(self, base_path: str = "data/historical-data"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

        # File handle management
        self._file_handles = {}
        self._csv_writers = {}
        self._current_dates = {}
        self._file_paths = {}

    def _get_market_name(self, epic: str) -> str:
        """Extract market name from epic."""
        # Simple mapping or extraction logic
        epic_lower = epic.lower()

        if "gold" in epic_lower:
            return "gold"
        elif "btc" in epic_lower or "bitcoin" in epic_lower:
            return "bitcoin"
        elif "eurusd" in epic_lower:
            return "eurusd"
        elif "gbpusd" in epic_lower:
            return "gbpusd"
        elif "usdjpy" in epic_lower:
            return "usdjpy"
        elif "nasdaq" in epic_lower or "nas100" in epic_lower:
            return "nasdaq"
        elif "spx" in epic_lower or "sp500" in epic_lower:
            return "sp500"
        else:
            # Use epic as fallback (clean it up)
            clean_epic = epic.replace(".", "_").replace("-", "_").lower()
            return clean_epic

    def _get_resolution_name(self, resolution_seconds: int) -> str:
        """Convert resolution in seconds to string name."""
        if resolution_seconds == 60:
            return "m1"
        elif resolution_seconds == 300:
            return "m5"
        elif resolution_seconds == 900:
            return "m15"
        elif resolution_seconds == 1800:
            return "m30"
        elif resolution_seconds == 3600:
            return "h1"
        elif resolution_seconds == 14400:
            return "h4"
        elif resolution_seconds == 86400:
            return "d1"
        elif resolution_seconds == 604800:
            return "w1"
        else:
            return f"{resolution_seconds}s"

    def _get_file_path(
        self, epic: str, resolution_seconds: int, date: datetime = None
    ) -> str:
        """Get the CSV file path for a specific date."""
        if date is None:
            date = datetime.now()

        market_name = self._get_market_name(epic)
        resolution_name = self._get_resolution_name(resolution_seconds)
        date_str = date.strftime("%Y-%m-%d")

        # Create market directory
        market_dir = os.path.join(self.base_path, market_name)
        os.makedirs(market_dir, exist_ok=True)

        # File name pattern: market-resolution-date.csv
        filename = f"{market_name}_{resolution_name}_{date_str}.csv"
        return os.path.join(market_dir, filename)

    def _get_file_key(self, epic: str, resolution_seconds: int) -> str:
        """Get a unique key for file management."""
        return f"{epic}_{resolution_seconds}"

    def _ensure_file_open(self, epic: str, resolution_seconds: int, date: datetime):
        """Ensure the CSV file is open and ready for writing."""
        file_key = self._get_file_key(epic, resolution_seconds)

        # Get new file path
        new_file_path = self._get_file_path(epic, resolution_seconds, date)

        # Check if we need to open a new file
        if (
            file_key not in self._file_paths
            or self._file_paths[file_key] != new_file_path
        ):

            # Close existing file if open
            self._close_file_if_open(file_key)

            # Open new file
            self._open_new_file(file_key, new_file_path, date)

    def _close_file_if_open(self, file_key: str):
        """Close file if it's open."""
        if file_key in self._file_handles:
            try:
                self._file_handles[file_key].close()
                logger.info(f"Closed file: {self._file_paths.get(file_key, 'unknown')}")
            except Exception as e:
                logger.info(f"Error closing file: {e}")

            # Clean up
            for dict_name in [
                "_file_handles",
                "_csv_writers",
                "_current_dates",
                "_file_paths",
            ]:
                if file_key in getattr(self, dict_name):
                    getattr(self, dict_name).pop(file_key, None)

    def _open_new_file(self, file_key: str, file_path: str, date: datetime):
        """Open a new CSV file for writing."""
        try:
            # Check if file exists to determine if we need a header
            file_exists = os.path.exists(file_path)

            # Open file in append mode
            file_handle = open(file_path, "a", newline="", encoding="utf-8")
            csv_writer = csv.writer(file_handle)

            # Write header if file is new
            if not file_exists:
                csv_writer.writerow(
                    [
                        "timestamp",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "start_time",
                        "end_time",
                        "closed_at",
                        "epic",
                    ]
                )
                logger.info(f"Created new file with header: {file_path}")

            # Store handles and metadata
            self._file_handles[file_key] = file_handle
            self._csv_writers[file_key] = csv_writer
            self._current_dates[file_key] = date
            self._file_paths[file_key] = file_path

            logger.info(f"Opened file for writing: {file_path}")

        except Exception as e:
            logger.info(f"Error opening file {file_path}: {e}")
            raise

    def save_candle(self, candle: dict, resolution_seconds: int):
        """Save a completed candle to CSV."""
        try:
            epic = candle["epic"]
            end_time = candle["end_time"]

            # Ensure file is open for this date
            self._ensure_file_open(epic, resolution_seconds, end_time)

            file_key = self._get_file_key(epic, resolution_seconds)

            if file_key not in self._csv_writers:
                logger.info(f"CSV writer not found for {file_key}")
                return False

            csv_writer = self._csv_writers[file_key]

            # Prepare row data
            row = [
                candle["start_time"].isoformat(),  # timestamp
                float(candle["open"]),
                float(candle["high"]),
                float(candle["low"]),
                float(candle["close"]),
                int(candle["volume"]),
                candle["start_time"].isoformat(),
                candle["end_time"].isoformat(),
                candle["closed_at"].isoformat(),
                epic,
            ]

            # Write to CSV
            csv_writer.writerow(row)

            # Flush immediately to ensure data is written
            self._file_handles[file_key].flush()

            # logger.info confirmation with file info
            file_path = self._file_paths[file_key]
            filename = os.path.basename(file_path)
            logger.info(
                f"Saved candle to {filename}: "
                f"{candle['start_time'].strftime('%H:%M:%S')} - "
                f"{candle['end_time'].strftime('%H:%M:%S')} "
                f"(O:{candle['open']:.2f} H:{candle['high']:.2f} "
                f"L:{candle['low']:.2f} C:{candle['close']:.2f})"
            )

            return True

        except Exception as e:
            logger.info(f"Error saving candle to CSV: {e}")
            import traceback

            traceback.logger.info_exc()
            return False

    def get_recent_candles(
        self, epic: str, resolution_seconds: int, num_candles: int = 100
    ):
        """Read recent candles from CSV files."""
        try:
            market_name = self._get_market_name(epic)
            resolution_name = self._get_resolution_name(resolution_seconds)
            market_dir = os.path.join(self.base_path, market_name)

            if not os.path.exists(market_dir):
                logger.info(f"Directory not found: {market_dir}")
                return []

            # Find all CSV files for this market and resolution
            csv_files = []
            for filename in os.listdir(market_dir):
                if filename.startswith(
                    f"{market_name}_{resolution_name}_"
                ) and filename.endswith(".csv"):
                    csv_files.append(os.path.join(market_dir, filename))

            if not csv_files:
                logger.info(f"No CSV files found for {market_name}_{resolution_name}_*")
                return []

            # Sort by date (newest first)
            csv_files.sort(reverse=True)
            logger.info(f"Found {len(csv_files)} CSV files")

            # Read candles from files
            all_candles = []
            for csv_file in csv_files:
                try:
                    logger.info(f"Reading: {os.path.basename(csv_file)}")
                    df = pd.read_csv(csv_file)

                    # Check if dataframe is empty
                    if df.empty:
                        continue

                    # Convert timestamp strings back to datetime
                    datetime_columns = [
                        "timestamp",
                        "start_time",
                        "end_time",
                        "closed_at",
                    ]
                    for col in datetime_columns:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col])

                    # Filter for this epic if multiple in same file
                    if "epic" in df.columns:
                        df = df[df["epic"] == epic]

                    # Add to collection
                    candles = df.to_dict("records")
                    all_candles.extend(candles)

                    logger.info(f"   ↳ Read {len(candles)} candles")

                    if len(all_candles) >= num_candles:
                        logger.info(f"Reached requested limit of {num_candles} candles")
                        break

                except Exception as e:
                    logger.info(f"Error reading CSV file {csv_file}: {e}")

            # Sort by timestamp and return requested number
            if all_candles:
                all_candles.sort(
                    key=lambda x: x.get("timestamp", x.get("start_time", datetime.min)),
                    reverse=True,
                )
                return all_candles[:num_candles]
            else:
                return []

        except Exception as e:
            logger.info(f"Error in get_recent_candles: {e}")
            import traceback

            traceback.logger.info_exc()
            return []

    def close_all(self):
        """Close all open file handles."""
        logger.info("Closing all CSV files...")
        for file_key in list(self._file_handles.keys()):
            self._close_file_if_open(file_key)

        logger.info(f"All files closed. Total handles: {len(self._file_handles)}")

    def __del__(self):
        """Destructor to ensure files are closed."""
        self.close_all()

    def get_file_info(self):
        """Get information about open files."""
        info = []
        for file_key, file_path in self._file_paths.items():
            info.append(
                {
                    "file_key": file_key,
                    "path": file_path,
                    "date": self._current_dates.get(file_key, "unknown"),
                    "is_open": file_key in self._file_handles,
                }
            )
        return info
