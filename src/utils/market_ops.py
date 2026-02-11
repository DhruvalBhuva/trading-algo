import ast
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


class MarketOps:
    """
    Common market operations.
    This class does NOT control flow.
    It only provides reusable market utilities.
    """

    def __init__(self, client):
        self.client = client

    # -----------------------------
    # Helpers
    # -----------------------------
    @staticmethod
    def _parse_price(x) -> dict:
        if isinstance(x, dict):
            return x
        if isinstance(x, str):
            try:
                return ast.literal_eval(x)
            except Exception:
                return json.loads(x.replace("'", '"'))
        raise ValueError(f"Unexpected price format: {type(x)}")

    # -----------------------------
    # Yesterday levels
    # -----------------------------
    def update_yesterday_levels(
        self,
        epic: str,
        resolution: str,
        csv_path: str | Path,
        session_start_hour: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Fetch yesterday's candles, compute high/low,
        append to CSV, and return yesterday's levels.
        """

        # --- define yesterday (UTC)
        yesterday_start = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        yesterday_end = yesterday_start.replace(hour=23, minute=59, second=59)

        # --- fetch data
        df = self.client.get_historical_prices(
            epic=epic,
            resolution=resolution,
            from_date=yesterday_start.isoformat(),
            to_date=yesterday_end.isoformat(),
        )

        if df.empty:
            return pd.DataFrame()

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df[df["timestamp"].dt.date == yesterday_start.date()].copy()

        if df.empty:
            return pd.DataFrame()

        # --- extract bid / ask
        for col in ["openPrice", "highPrice", "lowPrice", "closePrice"]:
            parsed = df[col].apply(self._parse_price)
            df[f"{col}_bid"] = parsed.apply(lambda x: x["bid"])
            df[f"{col}_ask"] = parsed.apply(lambda x: x["ask"])

        # --- trading day logic
        ts = df["timestamp"]
        if session_start_hour is not None:
            ts = ts - pd.Timedelta(hours=session_start_hour)

        df["trading_day"] = ts.dt.date

        # --- compute levels
        levels = (
            df.groupby("trading_day")
            .agg(
                high_bid=("highPrice_bid", "max"),
                high_ask=("highPrice_ask", "max"),
                low_bid=("lowPrice_bid", "min"),
                low_ask=("lowPrice_ask", "min"),
            )
            .reset_index()
        )

        # --- persist
        csv_path = Path(csv_path)
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        if csv_path.exists():
            existing = pd.read_csv(csv_path)
            levels = pd.concat([existing, levels], ignore_index=True)

        levels.to_csv(csv_path, index=False)

        return levels.tail(1).to_dict(orient="records")[0]
