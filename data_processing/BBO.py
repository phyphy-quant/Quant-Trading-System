import json
import asyncio
import websockets

class BBO:
    def __init__(self, symbol: str):
        """Initialize the BBO structure for a specific symbol."""
        self.symbol = symbol
        self.best_bid = None  # Best bid price
        self.best_bid_size = 0  # Size of the best bid
        self.best_ask = None  # Best ask price
        self.best_ask_size = 0  # Size of the best ask

    def update(self, bid_price: float, bid_size: float, ask_price: float, ask_size: float):
        """Update the BBO with new bid and ask data."""
        if self.best_bid is None or bid_price > self.best_bid:
            self.best_bid = bid_price
            self.best_bid_size = bid_size
        elif bid_price == self.best_bid:
            self.best_bid_size += bid_size  # Aggregate size if bid price is the same

        if self.best_ask is None or ask_price < self.best_ask:
            self.best_ask = ask_price
            self.best_ask_size = ask_size
        elif ask_price == self.best_ask:
            self.best_ask_size += ask_size  # Aggregate size if ask price is the same

    def get_bbo(self):
        """Return the best bid and ask as a dictionary."""
        return {
            "symbol": self.symbol,
            "best_bid": self.best_bid,
            "best_bid_size": self.best_bid_size,
            "best_ask": self.best_ask,
            "best_ask_size": self.best_ask_size
        }

    def __str__(self):
        """String representation of the BBO."""
        return f"{self.symbol} BBO - Best Bid: {self.best_bid} ({self.best_bid_size}), Best Ask: {self.best_ask} ({self.best_ask_size})"
