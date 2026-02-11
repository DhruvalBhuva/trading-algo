from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from datetime import datetime


# =========================
# ENUMS
# =========================
class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"
    SL_MARKET = "SL_MARKET"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    PLACED = "PLACED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class EventType(str, Enum):
    TICK = "TICK"
    SIGNAL = "SIGNAL"
    ORDER = "ORDER"
    FILL = "FILL"


# =========================
# MARKET DATA MODELS
# =========================


@dataclass
class TickEvent:
    event_type: EventType = EventType.TICK
    symbol: str = ""
    price: float = 0.0
    quantity: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Candle:
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    start_time: datetime
    end_time: datetime


# =========================
# STRATEGY MODELS
# =========================


@dataclass
class SignalEvent:
    event_type: EventType = EventType.SIGNAL
    symbol: str = ""
    side: Side = Side.BUY
    strength: float = 1.0  # confidence (0–1)
    timestamp: datetime = field(default_factory=datetime.utcnow)


# =========================
# ORDER MODELS
# =========================


@dataclass
class OrderEvent:
    event_type: EventType = EventType.ORDER
    symbol: str = ""
    side: Side = Side.BUY
    quantity: int = 0
    order_type: OrderType = OrderType.MARKET
    price: Optional[float] = None
    stop_price: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class FillEvent:
    event_type: EventType = EventType.FILL
    symbol: str = ""
    side: Side = Side.BUY
    quantity: int = 0
    fill_price: float = 0.0
    order_id: Optional[str] = None
    commission: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)


# =========================
# POSITION & ACCOUNT MODELS
# =========================
@dataclass
class Position:
    symbol: str
    quantity: int = 0
    avg_price: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0


@dataclass
class Account:
    balance: float
    available_margin: float
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)


# =========================
# RISK MODELS
# =========================
@dataclass
class RiskLimits:
    max_position_size: int
    max_daily_loss: float
    max_trades_per_day: int
    max_open_positions: int


# =========================
# TRADE LOG MODEL
# =========================
@dataclass
class Trade:
    trade_id: str
    symbol: str
    side: Side
    quantity: int
    entry_price: float
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    entry_time: datetime = field(default_factory=datetime.utcnow)
    exit_time: Optional[datetime] = None
