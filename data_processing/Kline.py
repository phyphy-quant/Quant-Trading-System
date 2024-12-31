import json
import time
import datetime
from typing import Optional, Dict, Tuple

class KlineData:
    """only processed the finished time interval to make sure that getting the completed data"""
    def __init__(self, symbol: str):
        """Initialize the Kline structure for a specific symbol."""
        self.symbol = symbol
        self.current_kline = None
        self.latest_update_time = None

        self.open_price = None
        self.high_price = None
        self.low_price = None
        self.close_price = None
        self.volume = None
        self.amount = None
        # tracked the processed Kline 
        self.kline_last_process_time = {}
        # to compute the real time Kline 
        self.trades_in_current_kline = []
        self.start_time = None
        self.end_time = None
    
    def format_timestamp(self, ts):
        """Convert ms Timestamp into Date/Time"""
        if ts:
            return datetime.datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
        return 'N/A'

    def get_interval_seconds(self, interval: str) -> int:
        """Convert interval string to seconds"""
        unit = interval[-1]
        value = int(interval[:-1])

        if unit == 'm':
            return value * 60
        elif unit == 'h':
            return value * 3600
        elif unit == 'd':
            return value * 86400
        else:
            raise ValueError(f"Unsupported interval format: {interval}")
    
    def get_current_interval_time(self, interval: str) -> tuple[int, int]:
        """Calculate current interval's start and end time"""
        interval_seconds = self.get_interval_seconds(interval)
        current_time = int(time.time())

        # 計算當前時間所在的區間起始時間
        interval_start = (current_time // interval_seconds) * interval_seconds
        interval_end = interval_start + interval_seconds

        return interval_start, interval_end
    
    # only processed the finished time interval to make sure that getting the completed data
    def should_process_kline(self, symbol: str, interval: str, kline_data: dict) -> bool:
        """
        Check if we should process this kline data based on kline's own time
        Only process klines that have been completed (previous time period)
        """
        try:
            # Get the interval in seconds
            interval_seconds = self.get_interval_seconds(interval)

            # Get current timestamp in milliseconds
            current_time = int(time.time() * 1000)

            # Get kline end time from the data
            kline_end_time = int(kline_data.get('endTime', 0))

            # Calculate the end time of the last completed interval
            last_completed_interval_end = (current_time // (interval_seconds * 1000)) * (interval_seconds * 1000)
            
            # Only process if:
            # 1. This is a kline from a previous interval (end time < current interval start)
            # 2. We haven't processed this kline before (end time > last processed time)
            if kline_end_time <= last_completed_interval_end and (
                symbol not in self.kline_last_process_time or 
                kline_end_time > self.kline_last_process_time[symbol]
            ):
                self.kline_last_process_time[symbol] = kline_end_time
                return True
        
            return False
        
        except Exception as e:
            print(f"Error in should_process_kline: {e}")
            return False
    
    def process_kline_data(self, message_data: dict) -> dict:
        """Process kline data"""
        ts = message_data.get('ts')
        data = message_data.get('data', {})

        # Extract times for better logging
        start_time = self.format_timestamp(int(data.get('startTime', 0)))
        end_time = self.format_timestamp(int(data.get('endTime', 0)))

        # 更新基本價格數據
        self.open_price = float(data.get('open', 0))
        self.high_price = float(data.get('high', 0))
        self.low_price = float(data.get('low', 0))
        self.close_price = float(data.get('close', 0))
        self.volume = float(data.get('volume', 0))
        self.amount = float(data.get('amount', 0))

        processed_data = {
            "message_time": self.format_timestamp(ts),
            "current_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
            "start_time": self.format_timestamp(int(data.get('startTime', 0))),
            "end_time": self.format_timestamp(int(data.get('endTime', 0))),
            "kline_data": {
                "symbol": self.symbol,
                "open": data.get('open', ''),
                "high": data.get('high', ''),
                "low": data.get('low', ''),
                "close": data.get('close', ''),
                "volume": data.get('volume', '')
            }
        }
        # 更新當前K線數據
        self.current_kline = processed_data
        self.latest_update_time = int(time.time() * 1000)
        return processed_data

    def get_current_kline(self) -> dict:
        """Get the latest processed kline data(The data has not been processed.)"""
        return self.current_kline
    
    def reset(self) -> None:
        """Reset all current kline data"""
        self.trades_in_current_kline = []
        self.open_price = None
        self.high_price = None
        self.low_price = None
        self.close_price = None
        self.volume = None
        self.amount = None
        self.current_kline = None