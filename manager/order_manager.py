import logging
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

class OrderStatus(Enum):
    PENDING = "PENDING"
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"

@dataclass
class OrderInfo:
    order_id: str
    client_order_id: str
    symbol: str
    side: str
    order_type: str
    price: float
    quantity: float
    executed_quantity: float = 0
    status: OrderStatus = OrderStatus.PENDING
    create_time: datetime = None
    update_time: datetime = None
    fills: List[dict] = None

class OrderManager:
    def __init__(self):
        self.logger = logging.getLogger("OrderManager")
        self.active_orders: Dict[str, OrderInfo] = {}
        self.filled_orders: Dict[str, OrderInfo] = {}
        self.cancelled_orders: Dict[str, OrderInfo] = {}
        
    def add_order(self, order_info: OrderInfo) -> None:
        """Add a new order to tracking"""
        try:
            order_info.create_time = datetime.now()
            order_info.update_time = datetime.now()
            order_info.fills = []
            self.active_orders[order_info.client_order_id] = order_info
            
            self.logger.info(f"Added new order: {order_info}")
        except Exception as e:
            self.logger.error(f"Error adding order: {e}")

    def update_order(self, client_order_id: str, update_data: dict) -> None:
        """Update order status and details"""
        try:
            if client_order_id not in self.active_orders:
                self.logger.warning(f"Order {client_order_id} not found in active orders")
                return

            order = self.active_orders[client_order_id]
            order.update_time = datetime.now()
            
            # Update status
            new_status = update_data.get('status')
            if new_status:
                order.status = OrderStatus(new_status)

            # Update executed quantity
            new_executed_qty = update_data.get('executed_quantity')
            if new_executed_qty is not None:
                order.executed_quantity = float(new_executed_qty)

            # Add fill information
            fill_info = update_data.get('fill_info')
            if fill_info:
                order.fills.append(fill_info)

            # Move order to appropriate dictionary based on status
            if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                self._move_order_to_history(client_order_id)

            self.logger.info(f"Updated order {client_order_id}: {update_data}")
        except Exception as e:
            self.logger.error(f"Error updating order: {e}")

    def _move_order_to_history(self, client_order_id: str) -> None:
        """Move order from active to historical tracking"""
        order = self.active_orders.pop(client_order_id)
        if order.status == OrderStatus.FILLED:
            self.filled_orders[client_order_id] = order
        else:
            self.cancelled_orders[client_order_id] = order

    def get_order_info(self, client_order_id: str) -> Optional[OrderInfo]:
        """Get information about an order"""
        # Check active orders first
        if client_order_id in self.active_orders:
            return self.active_orders[client_order_id]
        # Then check filled orders
        if client_order_id in self.filled_orders:
            return self.filled_orders[client_order_id]
        # Finally check cancelled orders
        if client_order_id in self.cancelled_orders:
            return self.cancelled_orders[client_order_id]
        return None

    def get_active_orders_for_symbol(self, symbol: str) -> List[OrderInfo]:
        """Get all active orders for a specific symbol"""
        return [order for order in self.active_orders.values() if order.symbol == symbol]

    def cleanup_old_orders(self, days: int = 7) -> None:
        """Clean up old filled and cancelled orders"""
        cutoff_time = datetime.now() - timedelta(days=days)
        
        # Clean up filled orders
        self.filled_orders = {
            client_order_id: order 
            for client_order_id, order in self.filled_orders.items()
            if order.update_time > cutoff_time
        }
        
        # Clean up cancelled orders
        self.cancelled_orders = {
            client_order_id: order 
            for client_order_id, order in self.cancelled_orders.items()
            if order.update_time > cutoff_time
        }