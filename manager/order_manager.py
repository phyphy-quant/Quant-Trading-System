# manager/order_manager.py
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

class OrderStatus(Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    PENDING = "PENDING"

@dataclass
class OrderInfo:
    order_id: str               # local_id, e.g. "LONG_LIMIT_1735624500111"
    client_order_id: str        # 同上 (或視需求不重複)
    symbol: str
    side: str                   # "BUY"/"SELL"
    order_type: str             # "MARKET"/"LIMIT"
    price: float
    quantity: float
    executed_quantity: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    create_time: datetime = None
    update_time: datetime = None
    fills: List[dict] = field(default_factory=list)

class OrderManager:
    def __init__(self):
        self.logger = logging.getLogger("OrderManager")
        
        self.active_orders: Dict[str, OrderInfo] = {}   # key=local_id
        self.filled_orders: Dict[str, OrderInfo] = {}
        self.cancelled_orders: Dict[str, OrderInfo] = {}

        # 新增兩個 map 用於 local_id <-> exchange_id 雙向對照
        # exchange_id 要用 str 或 int 均可 (交易所若必須 int，就用 int)
        self.local_to_exchange_map: Dict[str, int] = {}
        self.exchange_to_local_map: Dict[int, str] = {}

    def add_order(self, order_info: OrderInfo, exchange_id: Optional[int] = None):
        """新增本策略的一筆訂單"""
        now = datetime.now()
        order_info.create_time = now
        order_info.update_time = now
        self.active_orders[order_info.order_id] = order_info
        self.logger.info(f"Added new order => {order_info}")

        if exchange_id is not None:
            self.local_to_exchange_map[order_info.order_id] = exchange_id
            self.exchange_to_local_map[exchange_id] = order_info.order_id

    def update_order(self, local_id: str, updates: dict):
        """依 local_id 更新本地資料"""
        if local_id not in self.active_orders:
            self.logger.warning(f"update_order => {local_id} not found in active_orders")
            return

        order = self.active_orders[local_id]
        order.update_time = datetime.now()

        new_status = updates.get("status")
        if new_status:
            try:
                order.status = OrderStatus(new_status)
            except ValueError:
                self.logger.warning(f"Unknown status => {new_status}")

        new_executed_qty = updates.get("executed_quantity")
        if new_executed_qty is not None:
            order.executed_quantity = float(new_executed_qty)

        # fills or other details
        fill_info = updates.get("fill_info")
        if fill_info:
            order.fills.append(fill_info)

        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
            self._move_to_history(local_id)

        self.logger.info(f"Updated order {local_id}: {updates}")

    def _move_to_history(self, local_id: str):
        order = self.active_orders.pop(local_id)
        if order.status == OrderStatus.FILLED:
            self.filled_orders[local_id] = order
        else:
            self.cancelled_orders[local_id] = order

    def get_order_info(self, local_id: str) -> Optional[OrderInfo]:
        if local_id in self.active_orders:
            return self.active_orders[local_id]
        if local_id in self.filled_orders:
            return self.filled_orders[local_id]
        if local_id in self.cancelled_orders:
            return self.cancelled_orders[local_id]
        return None

    def get_exchange_id_by_local_id(self, local_id: str) -> Optional[int]:
        return self.local_to_exchange_map.get(local_id)

    def get_local_id_by_exchange_id(self, exchange_id: int) -> Optional[str]:
        return self.exchange_to_local_map.get(exchange_id)

    def cleanup_old_orders(self, days: int = 7):
        cutoff = datetime.now() - timedelta(days=days)

        # filled_orders
        self.filled_orders = {
            oid: oinfo for oid, oinfo in self.filled_orders.items()
            if oinfo.update_time and oinfo.update_time > cutoff
        }

        # cancelled_orders
        self.cancelled_orders = {
            oid: oinfo for oid, oinfo in self.cancelled_orders.items()
            if oinfo.update_time and oinfo.update_time > cutoff
        }