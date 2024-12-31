import logging
from typing import Dict, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class PositionInfo:
    symbol: str
    size: float
    entry_price: float
    unrealized_pnl: float
    timestamp: datetime

class RiskManager:
    def __init__(self, risk_params: dict):
        self.logger = logging.getLogger("RiskManager")
        
        # Load risk parameters
        self.max_position_size = risk_params.get('max_position_size', 0.0)
        self.max_drawdown = risk_params.get('max_drawdown', 0.0)
        self.max_daily_loss = risk_params.get('max_daily_loss', 0.0)
        self.max_leverage = risk_params.get('max_leverage', 1.0)
        
        # Track positions and performance
        self.positions: Dict[str, PositionInfo] = {}
        self.daily_pnl = 0.0
        self.peak_equity = 0.0
        self.current_equity = 0.0
        
        self.logger.info("Risk Manager initialized with parameters: %s", risk_params)

    def can_place_order(self, symbol: str, order_size: float, order_price: float) -> tuple[bool, str]:
        """
        Check if an order can be placed based on risk rules
        Returns: (can_place: bool, reason: str)
        """
        try:
            # Check position size limits
            current_position = self.positions.get(symbol, PositionInfo(symbol, 0, 0, 0, datetime.now()))
            new_position_size = abs(current_position.size + order_size)
            
            if new_position_size > self.max_position_size:
                return False, f"Position size {new_position_size} exceeds limit {self.max_position_size}"

            # Check drawdown limit
            current_drawdown = (self.peak_equity - self.current_equity) / self.peak_equity
            if current_drawdown > self.max_drawdown:
                return False, f"Current drawdown {current_drawdown:.2%} exceeds limit {self.max_drawdown:.2%}"

            # Check daily loss limit
            if self.daily_pnl < -self.max_daily_loss:
                return False, f"Daily loss {self.daily_pnl} exceeds limit {self.max_daily_loss}"

            return True, "Order allowed"
            
        except Exception as e:
            self.logger.error(f"Error in risk check: {e}")
            return False, f"Risk check error: {str(e)}"

    def update_position(self, symbol: str, position_info: PositionInfo) -> None:
        """Update position information"""
        try:
            self.positions[symbol] = position_info
            
            # Update equity and PnL tracking
            total_unrealized_pnl = sum(pos.unrealized_pnl for pos in self.positions.values())
            self.current_equity = self.initial_equity + total_unrealized_pnl
            self.peak_equity = max(self.peak_equity, self.current_equity)
            
            self.logger.info(f"Updated position for {symbol}: {position_info}")
        except Exception as e:
            self.logger.error(f"Error updating position: {e}")

    def reset_daily_metrics(self) -> None:
        """Reset daily tracking metrics"""
        self.daily_pnl = 0.0
        self.logger.info("Daily metrics reset")

    def get_position_risk_metrics(self, symbol: str) -> dict:
        """Get risk metrics for a specific position"""
        if symbol not in self.positions:
            return {}
            
        position = self.positions[symbol]
        return {
            "position_size": position.size,
            "entry_price": position.entry_price,
            "unrealized_pnl": position.unrealized_pnl,
            "position_value": abs(position.size * position.entry_price),
            "utilization": abs(position.size) / self.max_position_size if self.max_position_size > 0 else 0
        }