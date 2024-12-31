import json
import logging
import asyncio
from typing import Optional, Dict
from redis import asyncio as aioredis
from enum import Enum
from datetime import datetime
from dataclasses import dataclass

class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"

# order 的 side
class PositionSide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

class OrderAction(Enum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"

@dataclass
class SignalData:
    timestamp: int
    action: OrderAction
    position_side: PositionSide
    order_type: OrderType
    symbol: str
    quantity: float
    price: Optional[float] = None
    reduce_only: bool = False
    margin_mode: str = 'CROSS'

    def to_dict(self) -> Dict:
        return {
            'timestamp': self.timestamp,
            'action': self.action.value,
            'position_side': self.position_side.value, # 倉為方向 LONG/ SHORT , 對沖模式
            'order_type': self.order_type.value,
            'symbol': self.symbol,
            'quantity': self.quantity,
            'price': self.price,
            'reduce_only': self.reduce_only,
            'margin_mode': self.margin_mode
        }

class Strategy:
    """Base class for all trading strategies"""
    
    def __init__(self, signal_channel: str, config: Dict = None):
        """
        Initialize strategy with configuration
        Args:
            signal_channel: Redis channel for publishing signals
            config: Strategy configuration dictionary
        """
        self.signal_channel = signal_channel
        self.strategy_name = self.__class__.__name__
        self.config = config or {}
        
        # Basic strategy states
        self.current_position: Optional[PositionSide] = None
        self.position_size: float = 0.0
        self.entry_price: Optional[float] = None

        # Managers and logging can be defined here or in subclass
        # For simplicity, assume self.redis_client 在 subclass 中賦值
        self.redis_client = None
        
        self.logger = self.setup_logger(self.strategy_name)

    def setup_logger(self, name: str, level: int = logging.INFO) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if not logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        return logger

    async def publish_signal(self, signal_data: SignalData) -> None:
        """
        Publish trading signal to Redis.
        Also update current_position status here since this is a good place to confirm the action.
        """
        # Update internal position state before publishing the signal
        if signal_data.action == OrderAction.CLOSE:
            self.current_position = None
            self.position_size = 0.0
            self.entry_price = None
        elif signal_data.action == OrderAction.OPEN:
            self.current_position = signal_data.position_side
            self.position_size = signal_data.quantity
            # 如果有需要設定 entry_price，可以在此處加入邏輯
            # self.entry_price = signal_data.price or current market price if needed

        if self.redis_client:
            signal_dict = signal_data.to_dict()
            await self.redis_client.publish(self.signal_channel, json.dumps(signal_dict))
            self.logger.info(f"Published signal to {self.signal_channel}: {signal_dict}")
        else:
            self.logger.warning("Redis client not available, cannot publish signal.")


    async def process_market_data(self, channel: str, data: dict, redis_client: aioredis.Redis) -> None:
        raise NotImplementedError("Subclass must implement process_market_data")

    async def process_private_data(self, channel: str, data: dict, redis_client: aioredis.Redis) -> None:
        raise NotImplementedError("Subclass must implement process_private_data")

    async def execute(self, channel: str, data: dict, redis_client: aioredis.Redis) -> None:
        raise NotImplementedError("Subclass must implement execute")

    def start(self) -> None:
        self.logger.info(f"Strategy {self.strategy_name} started")

    def stop(self) -> None:
        self.logger.info(f"Strategy {self.strategy_name} stopped")
