import os
import ast
import sys
import json
import time
import base64
import requests
import threading
import websocket
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone
from collections import defaultdict, deque
from datetime import datetime, timedelta
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from src.logger import logger


load_dotenv()


# -------------------------------------------------
# CAPITAL.COM CLIENT
# -------------------------------------------------
class CapitalClient:
    CANDLES_PER_MINUTE = {
        "MINUTE": 1,
        "MINUTE_5": 1 / 5,
        "MINUTE_15": 1 / 15,
        "MINUTE_30": 1 / 30,
        "HOUR": 1 / 60,
        "HOUR_4": 1 / 240,
        "DAY": 1 / 1440,
        "WEEK": 1 / 10080,
    }

    CHUNK_MINUTES = {
        "MINUTE": 600,
        "MINUTE_5": 3000,
        "MINUTE_15": 6000,
        "MINUTE_30": 12000,
        "HOUR": 43200,
        "HOUR_4": 172800,
        "DAY": 525600,
        "WEEK": 1048320,
    }

    def __init__(self, api_key: str, identifier: str, password: str):
        self.api_key = api_key
        self.identifier = identifier
        self.password = password
        self.base_url = os.getenv("CAPITAL_BASE_URL")

        # Session management
        self.cst = None
        self.security_token = None
        self.session_expiry = None
        self._session_lock = threading.Lock()

        # Real-time tick data
        self._tick_callback = None
        self._tick_epics = []
        self._ws_active = False
        self._ws_stop = threading.Event()
        self._ws_instance = None

    # ----------------------------------
    # SESSION MANAGEMENT
    # ----------------------------------
    def _is_session_valid(self) -> bool:
        """Check if the current session is still valid."""
        if not self.cst or not self.security_token:
            return False

        # If we don't have expiry time, assume it's valid
        if not self.session_expiry:
            return True

        # Add a buffer of 5 minutes before actual expiry
        return datetime.now(timezone.utc) < (self.session_expiry - timedelta(minutes=5))

    def _renew_session(self):
        """Renew the session by logging in again."""
        with self._session_lock:
            logger.info("Renewing session...")
            self.login()

    def _ensure_valid_session(self):
        """Ensure we have a valid session, renew if needed."""
        if not self._is_session_valid():
            self._renew_session()

    # ----------------------------------
    # AUTH
    # ----------------------------------
    def login(self):
        """Login and store session expiry information."""
        try:
            r = requests.get(
                f"{self.base_url}/session/encryptionKey",
                headers={"X-CAP-API-KEY": self.api_key},
            )
            r.raise_for_status()

            encryption_key = r.json()["encryptionKey"]
            timestamp = r.json()["timeStamp"]

            message = f"{self.password}|{timestamp}".encode()
            message_b64 = base64.b64encode(message)

            public_key = serialization.load_der_public_key(
                base64.b64decode(encryption_key)
            )
            encrypted = public_key.encrypt(message_b64, padding.PKCS1v15())
            encrypted_password = base64.b64encode(encrypted).decode()

            r = requests.post(
                f"{self.base_url}/session",
                headers={
                    "X-CAP-API-KEY": self.api_key,
                    "Content-Type": "application/json",
                },
                json={
                    "identifier": self.identifier,
                    "password": encrypted_password,
                    "encryptedPassword": True,
                },
            )
            r.raise_for_status()

            self.cst = r.headers["CST"]
            self.security_token = r.headers["X-SECURITY-TOKEN"]

            self.session_expiry = datetime.now(timezone.utc) + timedelta(hours=23)

            logger.info("Logged in successfully")

        except Exception as e:
            logger.exception(" Login failed")
            raise  # re-raise so downstream stops

    @property
    def headers(self):
        """Get headers with automatic session renewal."""
        self._ensure_valid_session()

        if not self.cst or not self.security_token:
            raise RuntimeError("Call login() first")

        return {
            "X-CAP-API-KEY": self.api_key,
            "CST": self.cst,
            "X-SECURITY-TOKEN": self.security_token,
        }

    # ----------------------------------
    # WEB SOCKET SESSION MANAGEMENT
    # ----------------------------------
    def get_websocket_headers(self):
        """Get headers specifically for WebSocket connections."""
        self._ensure_valid_session()
        return {
            "X-CAP-API-KEY": self.api_key,
            "CST": self.cst,
            "X-SECURITY-TOKEN": self.security_token,
        }

    # ----------------------------------
    # RAW PRICE CALL (UPDATED WITH TIMEZONE HANDLING)
    # ----------------------------------
    def _fetch_prices(
        self,
        epic: str,
        resolution: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:

        minutes = (end - start).total_seconds() / 60
        max_points = int(minutes * self.CANDLES_PER_MINUTE[resolution]) + 5

        # Convert to UTC and remove timezone info for Capital.com API
        if start.tzinfo is not None:
            start = start.astimezone(timezone.utc).replace(tzinfo=None)

        if end.tzinfo is not None:
            end = end.astimezone(timezone.utc).replace(tzinfo=None)

        # Format as simple ISO string without timezone
        start_iso = start.strftime("%Y-%m-%dT%H:%M:%S")
        end_iso = end.strftime("%Y-%m-%dT%H:%M:%S")

        params = {
            "resolution": resolution,
            "from": start_iso,
            "to": end_iso,
            "max": max_points,
        }

        r = requests.get(
            f"{self.base_url}/prices/{epic}",
            headers=self.headers,
            params=params,
        )
        r.raise_for_status()

        prices = r.json().get("prices", [])
        if not prices:
            return pd.DataFrame()

        df = pd.DataFrame(prices)

        # Convert to UTC timezone-aware datetime
        df["timestamp"] = pd.to_datetime(df["snapshotTime"]).dt.tz_localize(None)

        # For 15-minute candles, check if we need to round to the nearest 15 minutes
        if resolution == "MINUTE_15":
            # Round down to the nearest 15 minutes
            df["timestamp"] = df["timestamp"].dt.floor("15min")

        df = df.sort_values("timestamp")

        return df[["timestamp", "openPrice", "highPrice", "lowPrice", "closePrice"]]

    # ----------------------------------
    # SMART HISTORICAL DOWNLOAD (UPDATED)
    # ----------------------------------
    def get_historical_prices(
        self,
        epic: str,
        resolution: str,
        from_date: str,
        to_date: str,
        timezone_offset: int = 0,  # Timezone offset in hours (e.g., 5.5 for IST)
    ) -> pd.DataFrame:
        """
        Get historical prices with timezone handling.
        """

        # Parse dates (handle both date-only and datetime strings)
        try:
            start_dt = datetime.fromisoformat(from_date)
        except ValueError:
            # If only date is provided, add time
            start_dt = datetime.strptime(from_date, "%Y-%m-%d")

        try:
            end_dt = datetime.fromisoformat(to_date)
        except ValueError:
            # If only date is provided, add end of day
            end_dt = datetime.strptime(to_date, "%Y-%m-%d")
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

        # Apply timezone offset if needed
        if timezone_offset != 0:
            start_dt = start_dt + timedelta(hours=timezone_offset)
            end_dt = end_dt + timedelta(hours=timezone_offset)
            logger.info(f"Applied {timezone_offset:+} hour timezone offset")

        delta = timedelta(minutes=self.CHUNK_MINUTES[resolution])

        all_chunks = []
        current = start_dt

        logger.info(f"Downloading {epic} {resolution} candles")
        logger.info(
            f"   Date range: {start_dt.strftime('%Y-%m-%d %H:%M:%S')} to {end_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        while current < end_dt:
            chunk_end = min(current + delta, end_dt)

            logger.info(
                f"  {current.strftime('%Y-%m-%d %H:%M:%S')} → {chunk_end.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            df = self._fetch_prices(
                epic=epic,
                resolution=resolution,
                start=current,
                end=chunk_end,
            )

            if not df.empty:
                all_chunks.append(df)

            current = chunk_end

        if not all_chunks:
            return pd.DataFrame()

        df = (
            pd.concat(all_chunks)
            .drop_duplicates("timestamp")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

        # Apply reverse timezone offset to display in local time
        if timezone_offset != 0:
            df["timestamp"] = df["timestamp"] - pd.Timedelta(hours=timezone_offset)

        logger.info(
            f"Done: {len(df):,} candles "
            f"({df.timestamp.min().strftime('%Y-%m-%d %H:%M:%S')} → {df.timestamp.max().strftime('%Y-%m-%d %H:%M:%S')})"
        )

        # Check for missing candles
        if len(df) > 1:
            expected_freq = "15min" if resolution == "MINUTE_15" else "1D"
            full_range = pd.date_range(
                start=df.timestamp.min(), end=df.timestamp.max(), freq=expected_freq
            )
            missing = set(full_range) - set(df.timestamp)
            if missing:
                logger.info(
                    f"Missing {len(missing)} candles at: {list(missing)[:5]}..."
                )

        return df

    # ----------------------------------
    # ACCOUNTS (BALANCE / MARGIN / P&L)
    # ----------------------------------
    def get_accounts(self) -> pd.DataFrame:
        """
        Fetch all trading accounts with balances and margin info.
        """
        r = requests.get(
            f"{self.base_url}/accounts",
            headers=self.headers,
        )
        r.raise_for_status()

        accounts = r.json().get("accounts", [])

        if not accounts:
            logger.info("No accounts found")
            return None

        return accounts

    # ----------------------------------
    # MARKET SEARCH
    # ----------------------------------
    def search_markets(self, search_term=""):
        r = requests.get(
            f"{self.base_url}/markets",
            headers=self.headers,
            params={"searchTerm": search_term, "limit": 20},
        )
        r.raise_for_status()
        return r.json().get("markets", [])

        # ----------------------------------

    # WORKING ORDERS (LIMIT/STOP ORDERS)
    # ----------------------------------
    def create_working_order(
        self,
        epic: str,
        direction: str,
        size: float,
        level: float,
        order_type: str,
        good_till_date: str = None,
        guaranteed_stop: bool = False,
        trailing_stop: bool = False,
        stop_level: float = None,
        stop_distance: float = None,
        stop_amount: float = None,
        profit_level: float = None,
        profit_distance: float = None,
        profit_amount: float = None,
        deal_reference: str = None,
    ) -> dict:
        """Create a limit or stop order (working order)."""

        # Validate required parameters
        if direction not in ["BUY", "SELL"]:
            raise ValueError("direction must be either 'BUY' or 'SELL'")

        if size <= 0:
            raise ValueError("size must be greater than 0")

        if level <= 0:
            raise ValueError("level must be greater than 0")

        if order_type not in ["LIMIT", "STOP"]:
            raise ValueError("order_type must be either 'LIMIT' or 'STOP'")

        # Validate stop parameters
        if guaranteed_stop and trailing_stop:
            raise ValueError("Cannot set both guaranteedStop and trailingStop to True")

        if guaranteed_stop:
            if not any([stop_level, stop_distance, stop_amount]):
                raise ValueError(
                    "If guaranteedStop=True, must provide stopLevel, stopDistance, or stopAmount"
                )

        if trailing_stop:
            if stop_distance is None:
                raise ValueError("If trailingStop=True, must provide stopDistance")

        # Validate good_till_date format if provided
        if good_till_date:
            try:
                datetime.fromisoformat(good_till_date.replace("Z", ""))
            except ValueError:
                raise ValueError("good_till_date must be in format YYYY-MM-DDTHH:MM:SS")

        # Prepare request body
        body = {
            "epic": epic,
            "direction": direction,
            "size": size,
            "level": level,
            "type": order_type,
            "guaranteedStop": guaranteed_stop,
            "trailingStop": trailing_stop,
        }

        # Add optional parameters if provided
        if good_till_date is not None:
            body["goodTillDate"] = good_till_date

        if stop_level is not None:
            body["stopLevel"] = stop_level

        if stop_distance is not None:
            body["stopDistance"] = stop_distance

        if stop_amount is not None:
            body["stopAmount"] = stop_amount

        if profit_level is not None:
            body["profitLevel"] = profit_level

        if profit_distance is not None:
            body["profitDistance"] = profit_distance

        if profit_amount is not None:
            body["profitAmount"] = profit_amount

        if deal_reference is not None:
            body["dealReference"] = deal_reference

        # Make the API request
        r = requests.post(
            f"{self.base_url}/workingorders", headers=self.headers, json=body
        )
        r.raise_for_status()

        response = r.json()

        # Add convenience fields
        if "dealReference" in response:
            response["order_id"] = response["dealReference"].replace("o_", "")
            response["dealId"] = response["order_id"]

        order_type_desc = "Limit" if order_type == "LIMIT" else "Stop"
        logger.info(
            f"{order_type_desc} order created: {direction} {size} {epic} @ {level}"
        )
        if "dealReference" in response:
            logger.info(f"   Deal Reference: {response['dealReference']}")

        return response

    # ----------------------------------
    # GET ALL WORKING ORDERS
    # ----------------------------------
    def get_working_orders(self) -> pd.DataFrame:
        """
        Get all pending working orders (limit/stop orders).

        """
        r = requests.get(f"{self.base_url}/workingorders", headers=self.headers)
        r.raise_for_status()

        orders = r.json().get("workingOrders", [])
        if not orders:
            return pd.DataFrame()

        df = pd.json_normalize(orders)

        # Optional: Clean up column names
        df = df.rename(columns=lambda x: x.replace(".", "_"))

        # Convert timestamp columns if they exist
        timestamp_cols = ["createdDate", "goodTillDate"]
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        return df

    # ----------------------------------
    # GET SPECIFIC WORKING ORDER
    # ----------------------------------
    def get_working_order(self, deal_id: str) -> dict:
        """
        Get details of a specific working order.
        """
        r = requests.get(
            f"{self.base_url}/workingorders/{deal_id}", headers=self.headers
        )
        r.raise_for_status()
        return r.json()

    # ----------------------------------
    # UPDATE WORKING ORDER
    # ----------------------------------
    def update_working_order(
        self,
        deal_id: str,
        level: float = None,
        good_till_date: str = None,
        guaranteed_stop: bool = None,
        trailing_stop: bool = None,
        stop_level: float = None,
        stop_distance: float = None,
        stop_amount: float = None,
        profit_level: float = None,
        profit_distance: float = None,
        profit_amount: float = None,
    ) -> dict:
        """
        Update a limit or stop working order.

        """

        # Validate parameter combinations
        if guaranteed_stop is not None and trailing_stop is not None:
            if guaranteed_stop and trailing_stop:
                raise ValueError(
                    "Cannot set both guaranteedStop and trailingStop to True"
                )

        if guaranteed_stop and guaranteed_stop is True:
            if not any([stop_level, stop_distance, stop_amount]):
                raise ValueError(
                    "If guaranteedStop=True, must provide stopLevel, stopDistance, or stopAmount"
                )

        if trailing_stop and trailing_stop is True:
            if stop_distance is None:
                raise ValueError("If trailingStop=True, must provide stopDistance")

        # Validate good_till_date format if provided
        if good_till_date:
            try:
                datetime.fromisoformat(good_till_date.replace("Z", ""))
            except ValueError:
                raise ValueError("good_till_date must be in format YYYY-MM-DDTHH:MM:SS")

        # Prepare request body
        body = {}

        # Add parameters if provided
        if level is not None:
            if level <= 0:
                raise ValueError("level must be greater than 0")
            body["level"] = level

        if good_till_date is not None:
            body["goodTillDate"] = good_till_date

        if guaranteed_stop is not None:
            body["guaranteedStop"] = guaranteed_stop

        if trailing_stop is not None:
            body["trailingStop"] = trailing_stop

        if stop_level is not None:
            body["stopLevel"] = stop_level

        if stop_distance is not None:
            body["stopDistance"] = stop_distance

        if stop_amount is not None:
            body["stopAmount"] = stop_amount

        if profit_level is not None:
            body["profitLevel"] = profit_level

        if profit_distance is not None:
            body["profitDistance"] = profit_distance

        if profit_amount is not None:
            body["profitAmount"] = profit_amount

        # Make the API request
        r = requests.put(
            f"{self.base_url}/workingorders/{deal_id}", headers=self.headers, json=body
        )
        r.raise_for_status()

        response = r.json()
        logger.info(f"Working order updated: Deal ID {deal_id}")

        return response

    # ----------------------------------
    # DELETE WORKING ORDER
    # ----------------------------------
    def delete_working_order(self, deal_id: str) -> dict:
        """
        Delete (cancel) a working order.
        """
        r = requests.delete(
            f"{self.base_url}/workingorders/{deal_id}", headers=self.headers
        )
        r.raise_for_status()

        response = r.json()
        logger.info(f"Working order deleted: Deal ID {deal_id}")

        return response

    # ----------------------------------
    # REAL-TIME TICK DATA STREAM
    # ----------------------------------
    def stream_ticks(
        self,
        epics: list[str],
        on_tick=None,
        auto_reconnect: bool = True,
        reconnect_delay: int = 2,
    ):
        """Stream real-time tick data (not delayed OHLC)."""
        if not on_tick:
            raise ValueError("on_tick callback is required")

        # Store callback
        self._tick_callback = on_tick
        self._tick_epics = epics

        ws_url = "wss://api-streaming-capital.backend-capital.com/connect"

        # Reset state
        self._ws_active = False
        self._ws_stop.clear()

        def on_message(ws, raw):
            try:
                msg = json.loads(raw)

                # Handle ping response
                if msg.get("destination") == "ping":
                    return

                # Handle subscription response
                if msg.get("destination") == "marketData.subscribe":
                    status = msg.get("status")
                    if status == "OK":
                        subscriptions = msg.get("payload", {}).get("subscriptions", {})
                        logger.info(
                            f"Real-time tick subscription successful: {subscriptions}"
                        )
                    else:
                        error_code = msg.get("payload", {}).get("errorCode")
                        logger.info(f"Subscription failed: {error_code}")

                        if error_code == "error.invalid.session.token":
                            logger.info("Session expired, renewing...")
                            self._renew_session()
                            subscribe_to_ticks(ws)
                    return

                # Handle real-time tick data
                if msg.get("destination") == "quote":
                    if self._ws_stop.is_set():
                        return

                    p = msg["payload"]
                    epic = p["epic"]

                    # Process tick immediately
                    tick_data = {
                        "epic": epic,
                        "bid": p.get("bid"),
                        "ask": p.get("ofr"),  # Note: "ofr" is ask price
                        "bid_qty": p.get("bidQty"),
                        "ask_qty": p.get("ofrQty"),
                        "timestamp": datetime.fromtimestamp(p["timestamp"] / 1000),
                        "received_at": datetime.now(),
                    }

                    # Call the tick callback
                    try:
                        self._tick_callback(tick_data)
                    except Exception as e:
                        logger.info(f"Error in tick callback: {e}")

            except Exception as e:
                if not self._ws_stop.is_set():
                    logger.info(f"Error processing tick message: {e}")
                    import traceback

                    traceback.print_exc()

        def subscribe_to_ticks(ws):
            """Helper function to subscribe to real-time tick data."""
            ws_headers = self.get_websocket_headers()

            sub_msg = {
                "destination": "marketData.subscribe",
                "correlationId": str(int(time.time() * 1000)),
                "cst": ws_headers["CST"],
                "securityToken": ws_headers["X-SECURITY-TOKEN"],
                "payload": {
                    "epics": epics,
                },
            }
            ws.send(json.dumps(sub_msg))
            logger.info(f"Sent real-time tick subscription for {epics}")

        def on_open(ws):
            logger.info("Real-time WebSocket connection opened")
            connect_time = datetime.now()
            logger.info(f"Connected at: {connect_time.strftime('%H:%M:%S.%f')[:-3]}")

            ws_headers = self.get_websocket_headers()

            ping_msg = {
                "destination": "ping",
                "correlationId": str(int(time.time() * 1000)),
                "cst": ws_headers["CST"],
                "securityToken": ws_headers["X-SECURITY-TOKEN"],
            }
            ws.send(json.dumps(ping_msg))

            # Minimal delay before subscription
            time.sleep(0.1)
            subscribe_to_ticks(ws)

            self._ws_active = True

        def on_error(ws, error):
            if not self._ws_stop.is_set():
                logger.info(f"WebSocket error: {error}")
                if auto_reconnect:
                    logger.info(f"⏳ Reconnecting in {reconnect_delay} seconds...")
                    time.sleep(reconnect_delay)
                    if not self._ws_stop.is_set():
                        connect_websocket()

        def on_close(ws, close_status_code, close_msg):
            if not self._ws_stop.is_set():
                logger.info(f"🔌 WebSocket closed: Code={close_status_code}")

            self._ws_active = False

            if auto_reconnect and not self._ws_stop.is_set():
                logger.info(f"⏳ Reconnecting in {reconnect_delay} seconds...")
                time.sleep(reconnect_delay)
                if not self._ws_stop.is_set():
                    connect_websocket()

        def connect_websocket():
            """Connect to WebSocket with current session."""
            if self._ws_stop.is_set():
                return None

            logger.info(f"Connecting to real-time WebSocket...")

            ws_headers = self.get_websocket_headers()

            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                header=[
                    f"X-CAP-API-KEY: {self.api_key}",
                    f"CST: {ws_headers['CST']}",
                    f"X-SECURITY-TOKEN: {ws_headers['X-SECURITY-TOKEN']}",
                ],
            )

            self._ws_instance = ws

            # Run in background thread
            def run_ws():
                ws.run_forever(
                    ping_interval=30,
                    ping_timeout=10,
                    ping_payload=json.dumps(
                        {
                            "destination": "ping",
                            "correlationId": str(int(time.time() * 1000)),
                        }
                    ),
                    reconnect=5,
                )

            thread = threading.Thread(target=run_ws, daemon=True)
            thread.start()

            # Wait a moment for connection to establish
            time.sleep(0.5)

            return ws

        # Start the WebSocket connection
        return connect_websocket()

    def stop_streaming(self):
        """Stop the WebSocket streaming."""
        logger.info("Stopping WebSocket stream...")
        self._ws_stop.set()
        self._ws_active = False

        if hasattr(self, "_ws_instance") and self._ws_instance:
            try:
                self._ws_instance.close()
                logger.info("WebSocket closed successfully")
            except Exception as e:
                logger.info(f" Error closing WebSocket: {e}")

        # Clear callback
        self._tick_callback = None
