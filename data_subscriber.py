import asyncio
import json
import datetime
import time
from redis import asyncio as aioredis

class DataSubscriber:
    def __init__(self, redis_host="localhost", redis_port=6379):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis = None
        self.active_channels = {}
        self.kline_last_process_time = {}
        # choose the sub channel
        self.current_channel = None
    
    async def connect_to_redis(self):
        """Connect to Redis Server"""
        self.redis = await aioredis.from_url(
            f"redis://{self.redis_host}:{self.redis_port}",
            encoding='utf-8',
            decode_responses=True
        )
        print(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
    
    def get_market_channel_names(self, symbol: str, config: dict, interval: str = '1m') -> list:
        """Get market data channel names"""
        channels = []
        if config.get("orderbook"):
            channels.append(f"[MD]{symbol}-orderbook")
        if config.get("bbo"):
            channels.append(f"[MD]{symbol}-bbo")
        if config.get("trade"):
            channels.append(f"[MD]{symbol}-trade")
        if config.get("kline"):
            channels.append(f"[MD]{symbol}-kline_{interval}")
        return channels
    
    def get_private_channel_names(self, config: dict) -> list:
        """Get private data channel names"""
        channels = []
        if config.get("executionreport"):
            channels.append("[PD]executionreport")
        if config.get("position"):
            channels.append("[PD]position")
        if config.get("balance"):
            channels.append("[PD]balance")
        return channels
    
    def get_processed_channel_names(self, symbol: str, config: dict, interval: str = '1m') -> list:
        """Get processed data channel names"""
        channels = []
        if config.get("kline"):
            channels.append(f"[MD]{symbol}-processed-kline_{interval}")
        return channels

    
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
    
    async def process_orderbook_data(self, message_data):
        """Process orderbook data"""
        # get the current timestamp and the order book timestamp
        ts = message_data.get("ts")
        data = message_data.get("data", {})

        # convert timestamp into Date & Time
        print(f"\nChannel: {self.current_channel}")
        print(f"Timestamp Message Time: {self.format_timestamp(ts)}")
        print(f"Timestamp Current Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
        
        print("\nOrderbook Data:")
        print(f"Asks (first 5): {data.get('asks', [])[:5]}")
        print(f"Bids (first 5): {data.get('bids', [])[:5]}")
    
    async def process_bbo_data(self, message_data):
        """Process BBO data"""
        # get the current timestamp and the order book timestamp
        ts = message_data.get("ts")
        data = message_data.get("data", {})

        # convert timestamp into Date & Time
        print(f"\nChannel: {self.current_channel}")
        print(f"Timestamp Message Time: {self.format_timestamp(ts)}")
        print(f"Timestamp Current Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")

        print("\nBBO Data:")
        print(f"Best Bid: {data.get('bid', '')}")
        print(f"Best Ask: {data.get('ask', '')}")
    
    async def process_trade_data(self, message_data):
        """Process trade data"""
        ts = message_data.get('ts')
        data = message_data.get('data', {})
        
        print(f"\nChannel: {self.current_channel}")
        print(f"Timestamp Message Time: {self.format_timestamp(ts)}")
        print(f"Timestamp Current Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
        
        print("\nTrade Data:")
        print(f"Price: {data.get('price', '')}")
        print(f"Size: {data.get('size', '')}")
        print(f"Side: {data.get('side', '')}")
    
    # async def process_kline_data(self, message_data):
    #     """Process kline data"""
    #     ts = message_data.get('ts')
    #     data = message_data.get('data', {})

    #     # Extract times for better logging
    #     start_time = self.format_timestamp(int(data.get('startTime', 0)))
    #     end_time = self.format_timestamp(int(data.get('endTime', 0)))

    #     print(f"\nChannel: {self.current_channel}")
    #     print(f"Timestamp Message Time: {self.format_timestamp(ts)}")
    #     print(f"Timestamp Current Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")

    #     print("\nKline Data:")
    #     print(f"Period: {start_time} to {end_time}")
    #     print(f"open: {data.get('open', '')}")
    #     print(f"high: {data.get('high', '')}")
    #     print(f"low: {data.get('low', '')}")
    #     print(f"close: {data.get('close', '')}")
    #     print(f"volume: {data.get('volume', '')}")
        
    #     # 將處理後的數據存儲到Redis
    #     await self.redis.set('latest_kline', json.dumps(data))
    #     # 添加這一行來發布消息
    #     await self.redis.publish('latest_kline', json.dumps(data))
    
    async def process_execution_report(self, message_data):
        """Process execution report data"""
        ts = message_data.get('ts')
        data = message_data.get('data', {})

        print(f"\nChannel: {self.current_channel}")
        print(f"Timestamp Message Time: {self.format_timestamp(ts)}")
        print(f"Timestamp Current Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")

        print("\nExecution Report:")
        print(f"Order ID: {data.get('orderId', '')}")
        print(f"Symbol: {data.get('symbol', '')}")
        print(f"Side: {data.get('side', '')}")
        print(f"Price: {data.get('price', '')}")
        print(f"Quantity: {data.get('quantity', '')}")
        print(f"Status: {data.get('status', '')}")
    
    async def process_position_data(self, message_data):
        """Process position data"""
        ts = message_data.get('ts')
        data = message_data.get('data', {})

        print(f"\nChannel: {self.current_channel}")
        print(f"Timestamp Message Time: {self.format_timestamp(ts)}")
        print(f"Timestamp Current Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")

        print("\nPosition Data:")
        print(f"Symbol: {data.get('symbol', '')}")
        print(f"Size: {data.get('size', '')}")
        print(f"Entry Price: {data.get('entryPrice', '')}")
        print(f"Unrealized PNL: {data.get('unrealizedPnl', '')}")
    
    async def process_balance_data(self, message_data):
        """Process balance data"""
        ts = message_data.get('ts')
        data = message_data.get('data', {})

        print(f"\nChannel: {self.current_channel}")
        print(f"Timestamp Message Time: {self.format_timestamp(ts)}")
        print(f"Timestamp Current Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
        
        print("\nBalance Data:")
        print(f"Asset: {data.get('asset', '')}")
        print(f"Total Balance: {data.get('total', '')}")
        print(f"Available Balance: {data.get('available', '')}")
        print(f"Frozen Balance: {data.get('frozen', '')}")
    
    async def process_message(self, channel: str, data: str):
        """Process channel message"""
        try:
            self.current_channel = channel
            # 解析數據
            message_data = json.loads(data)
            
            # 移除前綴以便處理
            channel_without_prefix = channel.replace('[MD]', '').replace('[PD]', '')
            
            # 在Terminal顯示原始內容
            print(message_data)

            # 檢查是否是kline數據
            if 'processed-kline' in channel_without_prefix:
                await self.process_processed_kline_data(message_data)
            elif 'kline' in channel_without_prefix:
                await self.process_kline_data(message_data)
            elif 'orderbook' in channel_without_prefix:
                await self.process_orderbook_data(message_data)
            elif 'bbo' in channel_without_prefix:
                await self.process_bbo_data(message_data)
            elif 'trade' in channel_without_prefix:
                await self.process_trade_data(message_data)
            elif channel_without_prefix == 'executionreport':
                await self.process_execution_report(message_data)
            elif channel_without_prefix == 'position':
                await self.process_position_data(message_data)
            elif channel_without_prefix == 'balance':
                await self.process_balance_data(message_data)
            
            print(f"{'='*50}\n")
            
        except Exception as e:
            print(f"Error processing message for channel {channel}: {e}")
    
    async def process_kline_data(self, message_data):
        """Process kline data"""
        ts = message_data.get('ts')
        data = message_data.get('data', {})

        # Extract times for better logging
        start_time = self.format_timestamp(int(data.get('startTime', 0)))
        end_time = self.format_timestamp(int(data.get('endTime', 0)))

        print(f"\nChannel: {self.current_channel}")
        print(f"Timestamp Message Time: {self.format_timestamp(ts)}")
        print(f"Timestamp Current Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")

        print("\nKline Data:")
        print(f"Period: {start_time} to {end_time}")
        print(f"open: {data.get('open', '')}")
        print(f"high: {data.get('high', '')}")
        print(f"low: {data.get('low', '')}")
        print(f"close: {data.get('close', '')}")
        print(f"volume: {data.get('volume', '')}")


    async def process_processed_kline_data(self, message_data):
        """Process the processed kline data"""
        try:
            ts = message_data.get('ts')
            data = message_data.get('data', {})
            kline_data = data.get('kline_data', {})

            print(f"\nChannel: {self.current_channel}")
            print(f"Timestamp Message Time: {data.get('message_time', 'N/A')}")
            print(f"Current Time: {data.get('current_time', 'N/A')}")

            print("\nProcessed Kline Data:")
            print(f"Start Time: {data.get('start_time', 'N/A')}")
            print(f"End Time: {data.get('end_time', 'N/A')}")
            print(f"Symbol: {kline_data.get('symbol', 'N/A')}")
            print(f"Open: {kline_data.get('open', 'N/A')}")
            print(f"High: {kline_data.get('high', 'N/A')}")
            print(f"Low: {kline_data.get('low', 'N/A')}")
            print(f"Close: {kline_data.get('close', 'N/A')}")
            print(f"Volume: {kline_data.get('volume', 'N/A')}")

        except Exception as e:
            print(f"Error processing processed kline data: {e}")
        
        # # 將處理後的數據存儲到Redis
        # await self.redis.set('latest_kline', json.dumps(data))
        # # 添加這一行來發布消息
        # await self.redis.publish('latest_kline', json.dumps(data))
    
    async def subscribe_to_data(self, symbol: str, market_config: dict, private_config: dict, interval: str = '1m'):
        """Subscribe to both market and private data"""
        if not self.redis:
            await self.connect_to_redis()
        
        pubsub = self.redis.pubsub()
        
        # Get all channel names
        market_channels = self.get_market_channel_names(symbol, market_config, interval)
        private_channels = self.get_private_channel_names(private_config)
        processed_channels = self.get_processed_channel_names(symbol, market_config, interval)
        channels = market_channels + private_channels + processed_channels
        
        if not channels:
            print("No channels selected for subscription")
            return
        
        await pubsub.subscribe(*channels)
        print(f"Subscribed to channels: {channels}")
        
        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    channel = message['channel']
                    try:
                        await self.process_message(channel, message['data'])
                    except json.JSONDecodeError:
                        print(f"Failed to decode message data: {message['data']}")
                
                await asyncio.sleep(0.1)
                
        except asyncio.CancelledError:
            print("\nUnsubscribing from channels...")
            await pubsub.unsubscribe()
            await self.redis.aclose()
        except Exception as e:
            print(f"Error in subscription: {e}")

async def main():
    subscriber = DataSubscriber()
    
    symbol = "PERP_BTC_USDT"
    interval = "1m"
    
    # Market data configuration
    market_config = {
        "orderbook": False,
        "bbo": False,
        "trade": False,
        "kline": True
    }
    
    # Private data configuration
    private_config = {
        "executionreport": True,
        "position": True,
        "balance": True
    }
    
    try:
        subscription_task = asyncio.create_task(
            subscriber.subscribe_to_data(symbol, market_config, private_config, interval)
        )
        
        await asyncio.gather(subscription_task)
        
    except KeyboardInterrupt:
        print("\nGracefully shutting down...")
    finally:
        if 'subscription_task' in locals():
            subscription_task.cancel()
            try:
                await subscription_task
            except asyncio.CancelledError:
                pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        print(f"Program error: {str(e)}")