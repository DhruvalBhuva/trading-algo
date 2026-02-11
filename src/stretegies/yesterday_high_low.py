import os
import sys
import pandas as pd
from datetime import date, timedelta

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)


def get_previous_trading_day(today: date) -> date:
    if today.weekday() == 0:  # Monday
        return today - timedelta(days=3)
    elif today.weekday() == 6:  # Sunday
        return today - timedelta(days=2)
    else:
        return today - timedelta(days=1)


class YesterdayHighLowStrategy:
    def __init__(
        self,
        epic: str,
        levels_csv: str = "data/gold_yesterday_levels.csv",
        account_balance: float = 10000,
        risk_percent: float = 2.0,
        tp_pips: float = 300,
        pip_size: float = 0.01,
        contract_size: float = 100,
    ):
        super().__init__()
        self.epic = epic
        self.levels_csv = levels_csv
        self.account_balance = account_balance
        self.risk_percent = risk_percent / 100
        self.tp_pips = tp_pips
        self.pip_size = pip_size
        self.contract_size = contract_size

        # Daily state
        self.today = None
        self.traded_today = False

        # Levels
        self.y_high = None
        self.y_low = None

        # Setup state
        self.c1 = None
        self.c2 = None
        self.direction = None

    def load_yesterday_levels(self, trading_date: date):
        prev_day = get_previous_trading_day(trading_date)
        df = pd.read_csv(self.levels_csv)

        required_cols = {"trading_day", "high_bid", "low_bid"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Levels CSV missing columns: {missing}")

        row = df[df["trading_day"] == prev_day.strftime("%Y-%m-%d")]
        if row.empty:
            raise ValueError(f"No levels for {prev_day}")

        self.y_high = float(row.iloc[-1]["high_bid"])
        self.y_low = float(row.iloc[-1]["low_bid"])

        self.today = trading_date
        self.traded_today = False
        self._reset_setup()

    def _calc_size(self, entry: float, stop: float) -> float:
        """Calculate position size based on risk management."""
        risk_amount = self.account_balance * self.risk_percent
        dist = abs(entry - stop)
        if dist <= 0:
            return 0
        return round(risk_amount / (dist * self.contract_size), 2)

    def on_candle_close(self, candle: dict) -> dict:
        """Process a closed candle and generate trading signals."""
        result = {
            "time": candle["start_time"],
            "decision": "NO_TRADE",
            "reason": "",
        }

        candle_date = candle["start_time"].date()

        # NEW TRADING DAY
        if self.today != candle_date:
            self.load_yesterday_levels(candle_date)
            result["decision"] = "INIT_DAY"
            result["reason"] = (
                f"New trading day detected. Loaded previous trading day levels. "
                f"High: {self.y_high:.2f}, Low: {self.y_low:.2f}. "
                "Waiting for breakout candle (C1)."
            )
            return result

        # ONE TRADE PER DAY — HARD BLOCK
        if self.traded_today:
            result["decision"] = "BLOCKED"
            result["reason"] = (
                "Trade already executed today. Strategy locked until next trading day."
            )
            return result

        # C1 — BREAKOUT
        if self.c1 is None:
            if candle["close"] > self.y_high:
                self.c1 = candle
                self.direction = "BUY"
                result["decision"] = "C1"
                result["reason"] = (
                    f"Breakout detected: candle closed above yesterday high "
                    f"({self.y_high:.2f}). Waiting for acceptance candle (C2)."
                )
                return result

            if candle["close"] < self.y_low:
                self.c1 = candle
                self.direction = "SELL"
                result["decision"] = "C1"
                result["reason"] = (
                    f"Breakout detected: candle closed below yesterday low "
                    f"({self.y_low:.2f}). Waiting for acceptance candle (C2)."
                )
                return result

            result["reason"] = (
                "No breakout detected. Candle closed inside yesterday range. "
                "Waiting for valid C1 breakout."
            )
            return result

        # C2 — ACCEPTANCE
        if self.c2 is None:
            if self.direction == "BUY" and candle["close"] > self.y_high:
                self.c2 = candle
                result["decision"] = "C2"
                result["reason"] = (
                    "Acceptance confirmed: second candle closed above yesterday high. "
                    "Entry planned at next candle open (C3)."
                )
                return result

            if self.direction == "SELL" and candle["close"] < self.y_low:
                self.c2 = candle
                result["decision"] = "C2"
                result["reason"] = (
                    "Acceptance confirmed: second candle closed below yesterday low. "
                    "Entry planned at next candle open (C3)."
                )
                return result

            self._reset_setup()
            result["decision"] = "INVALIDATED"
            result["reason"] = (
                "Breakout failed: acceptance candle closed back inside yesterday range. "
                "Setup cancelled. Waiting for a fresh breakout (C1)."
            )
            return result

        # C3 — ENTRY
        entry = candle["open"]
        direction = self.direction  # cache before reset

        if direction == "BUY":
            sl = min(self.c1["low"], self.c2["low"])
            tp = entry + self.tp_pips * self.pip_size
        else:
            sl = max(self.c1["high"], self.c2["high"])
            tp = entry - self.tp_pips * self.pip_size

        size = self._calc_size(entry, sl)
        if size <= 0:
            self._reset_setup()
            result["decision"] = "REJECTED"
            result["reason"] = (
                "Entry rejected: invalid position size calculated. Setup reset."
            )
            return result

        # 🔒 LOCK STRATEGY FOR THE DAY
        self.traded_today = True

        result["decision"] = "SIGNAL"
        result["reason"] = (
            f"Entry triggered: C1 breakout and C2 acceptance confirmed. "
            f"{direction} order placed at C3 price."
        )
        result["order"] = {
            "epic": self.epic,
            "direction": direction,
            "size": size,
            "orderType": "STOP",
            "level": entry,
            "stopLevel": sl,
            "profitLevel": tp,
        }

        self._reset_setup()
        return result

    def _reset_setup(self):
        """Reset the trading setup state."""
        self.c1 = None
        self.c2 = None
        self.direction = None

    def get_status(self) -> dict:
        """Get current strategy status."""
        return {
            "today": self.today,
            "traded_today": self.traded_today,
            "y_high": self.y_high,
            "y_low": self.y_low,
            "c1": self.c1 is not None,
            "c2": self.c2 is not None,
            "direction": self.direction,
        }
