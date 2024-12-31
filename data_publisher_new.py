import json
import asyncio
import websockets
import time
import datetime
import hmac
import hashlib
from redis import asyncio as aioredis

# Import your existing classes
from data_processing.Kline import KlineData
from data_processing.Orderbook import OrderBook
from data_processing.BBO import BBO
# from data_processing.Trade import TradeData

class WooXStagingAPI:
    def __init__(self, app_id: str, api_key: str, api_secret: str, redis_host: str, redis_port: int = 6379):
        self.app_id = app_id
        self.api_key = api_key
        self.api_secret = api_secret
        
        self.market_data_url = f"wss://wss.staging.woox.io/ws/stream/{self.app_id}"
        self.private_data_url = f"wss://wss.staging.woox.io/v2/ws/private/stream/{self.app_id}"
        
        self.redis_host = redis_host
        self.redis_port = redis_port
        
        self.market_connection = None
        self.private_connection = None
        self.redis_client = None
        
        self.orderbooks = {}
        self.bbo_data = {}
        from data_processing.Kline import KlineData
        self.kline_handlers = {}
        self.kline_handler = None
        self.symbol = None
        self.interval = None
    
    async def connect_to_redis(self):
        """Connect to Redis Server"""
        try:
            self.redis_client = await aioredis.from_url(
                f"redis://{self.redis_host}:{self.redis_port}",
                encoding='utf-8',
                decode_responses=True
            )
            print(f"[Data Publisher] Connected to Redis server at {self.redis_host}:{self.redis_port}")
        except Exception as e:
            print(f"[Data Publisher] Failed to connect to Redis: {str(e)}")
    
    # 處理資料序列的問題（疑問
    async def publish_to_redis(self, channel: str, data):
        """Publish data to Redis Channel with appropriate prefix"""
        if self.redis_client:
            # 根據頻道類型添加前綴
            if any(keyword in channel for keyword in ['kline', 'orderbook', 'bbo', 'trade']):
                prefixed_channel = f"[MD]{channel}"
            else:
                prefixed_channel = f"[PD]{channel}"

            # 如果數據是字符串，直接使用
            if isinstance(data, str):
                await self.redis_client.publish(prefixed_channel, data)
            else:
                # 如果數據是dict，序列化
                await self.redis_client.publish(prefixed_channel, json.dumps(data))
            
            print(f"[Data Publisher] Published to Redis channel: {prefixed_channel}")

    
    async def market_connect(self):
        """Handles WebSocket connection to market data."""
        if self.market_connection is None:
            self.market_connection = await websockets.connect(self.market_data_url)
            print(f"[Market Data Publisher] Connected to {self.market_data_url}")
        return self.market_connection
    
    async def private_connect(self):
        """Handles WebSocket connection to private data."""
        if self.private_connection is None:
            self.private_connection = await websockets.connect(self.private_data_url)
            print(f"[Private Data Publisher] Connected to {self.private_data_url}")
        return self.private_connection
    
    async def close_connections(self):
        """Close all connections in a coordinated way"""
        print("\nShutting down...")
        
        # Create tasks for closing all connections
        close_tasks = []
        
        # Close market connection if it exists
        if self.market_connection and not self.market_connection.closed:
            close_tasks.append(asyncio.create_task(
                self.market_connection.close(),
                name="market_close"
            ))
        
        # Close private connection if it exists
        if self.private_connection and not self.private_connection.closed:
            close_tasks.append(asyncio.create_task(
                self.private_connection.close(),
                name="private_close"
            ))
        
        # Close Redis connection if it exists
        if self.redis_client:
            close_tasks.append(asyncio.create_task(
                self.redis_client.aclose(),
                name="redis_close"
            ))
        
        # Wait for all connections to close with a timeout
        if close_tasks:
            try:
                await asyncio.wait(close_tasks, timeout=5.0)
                
                # Check which tasks completed
                for task in close_tasks:
                    if task.done():
                        if task.get_name() == "market_close":
                            print("[Market Data Publisher] Market WebSocket connection closed")
                        elif task.get_name() == "private_close":
                            print("[Private Data Publisher] Private WebSocket connection closed")
                        elif task.get_name() == "redis_close":
                            print("[Data Publisher] Redis connection closed")
                    else:
                        print(f"Warning: {task.get_name()} did not close properly")
                        
            except asyncio.TimeoutError:
                print("Warning: Some connections did not close within the timeout period")
                
            # Cancel any remaining tasks
            for task in close_tasks:
                if not task.done():
                    task.cancel()
        
        # Reset connection variables
        self.market_connection = None
        self.private_connection = None
        self.redis_client = None

    def generate_signature(self, body):
        """Generate signature for authentication"""
        key_bytes = bytes(self.api_secret, 'utf-8')
        body_bytes = bytes(body, 'utf-8')
        return hmac.new(key_bytes, body_bytes, hashlib.sha256).hexdigest()
    
    async def authenticate(self):
        """Authenticate private connection"""
        connection = await self.private_connect()
        timestamp = int(time.time() * 1000)
        data = f"|{timestamp}"
        signature = self.generate_signature(data)
        
        auth_message = {
            "event": "auth",
            "params": {
                "apikey": self.api_key,
                "sign": signature,
                "timestamp": timestamp
            }
        }
        await connection.send(json.dumps(auth_message))
        response = json.loads(await connection.recv())
        
        if response.get("success"):
            print("[Private Data Publisher] Authentication successful")
        else:
            print(f"[Private Data Publisher] Authentication failed: {response.get('errorMsg')}")
    
    async def respond_pong(self, connection, publisher_type="Market"):
        """Responds to server PINGs with a PONG."""
        if connection and not connection.closed:
            pong_message = {
                "event": "pong",
                "ts": int(time.time() * 1000)
            }
            await connection.send(json.dumps(pong_message))
            print(f"[{publisher_type} Data Publisher] Sent PONG response")
    
    async def handle_ping_pong(self, message, connection, publisher_type="Market"):
        """Handle ping-pong mechanism"""
        data = json.loads(message)
        if data.get("event") == "ping":
            print(f"[{publisher_type} Data Publisher] Received PING from server")
            await self.respond_pong(connection, publisher_type)
    
    async def subscribe_market(self, symbol, config, interval: str):
        """Subscribe to market data streams"""
        if self.market_connection is None or self.market_connection.closed:
            print("[Market Data Publisher] No active connection. Attempting to reconnect...")
            await self.market_connect()
        
        subscription_types = {
            "orderbook": {"topic": f"{symbol}@orderbook", "type": "orderbook"},
            "bbo": {"topic": f"{symbol}@bbo", "type": "bbo"},
            "trade": {"topic": f"{symbol}@trade"},
            "kline": {"topic": f"{symbol}@kline_{interval}" if config.get("kline") else None}
        }
        
        for sub_type, params in subscription_types.items():
            if config.get(sub_type) and params["topic"]:
                subscription = {
                    "event": "subscribe",
                    "topic": params["topic"],
                    "symbol": symbol
                }
                if "type" in params:
                    subscription["type"] = params["type"]
                
                await self.market_connection.send(json.dumps(subscription))
                print(f"[Market Data Publisher] Subscribed to {sub_type} for {symbol}")
    
    async def subscribe_private(self, config):
        """Subscribe to private data streams"""
        if self.private_connection is None or self.private_connection.closed:
            print("[Private Data Publisher] No active connection. Attempting to reconnect...")
            await self.private_connect()
        
        subscription_types = {
            "executionreport": {"topic": "executionreport"},
            "position": {"topic": "position"},
            "balance": {"topic": "balance"}
        }
        
        for sub_type, params in subscription_types.items():
            if config.get(sub_type):
                subscription = {
                    "event": "subscribe",
                    "topic": params["topic"]
                }
                await self.private_connection.send(json.dumps(subscription))
                print(f"[Private Data Publisher] Subscribed to {sub_type}")
    
    async def process_kline_data(self, symbol: str, interval: str, message: dict) -> None:
        """Process kline data and publish both raw and processed data"""
        try:
            # 1. 發布原始數據
            if message.get('topic'):
                channel = f"{symbol}-kline_{interval}"
                await self.publish_to_redis(channel, message)
                
            # 2. 處理 kline 數據
            if symbol not in self.kline_handlers:
                self.kline_handlers[symbol] = KlineData(symbol)
            
            kline_handler = self.kline_handlers[symbol]
            kline_data = message.get('data', {})
            
            # 檢查是否應該處理這個k線
            if kline_handler.should_process_kline(symbol, interval, kline_data):
                # 處理數據
                processed_data = kline_handler.process_kline_data(message)
                
                # 3. 發布處理後的數據
                processed_message = {
                    'topic': f"{symbol}@processed_kline_{interval}",
                    'ts': message.get('ts'),
                    'data': {
                        'kline_data': {
                            'symbol': symbol,
                            'startTime': kline_data.get('startTime'),
                            'endTime': kline_data.get('endTime'),
                            'open': kline_data.get('open'),
                            'high': kline_data.get('high'),
                            'low': kline_data.get('low'),
                            'close': kline_data.get('close'),
                            'volume': kline_data.get('volume'),
                        },
                        'message_time': kline_handler.format_timestamp(message.get('ts')),
                        'current_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                        'start_time': kline_handler.format_timestamp(kline_data.get('startTime')),
                        'end_time': kline_handler.format_timestamp(kline_data.get('endTime'))
                    }
                }
                
                processed_channel = f"{symbol}-processed-kline_{interval}"
                await self.publish_to_redis(processed_channel, processed_message)
                
        except Exception as e:
            print(f"[Data Publisher] Error processing kline data: {e}")

    async def process_market_data(self, symbol: str, interval: str, message: dict):
        """Process market data and publish to Redis"""
        try:
            data = json.loads(message) if isinstance(message, str) else message

            # Handle ping-pong
            if data.get("event") == "ping":
                await self.handle_ping_pong(message, self.market_connection)
                return
            
            # Handle subscription confirmation
            if data.get("event") == "subscribe":
                print(f"[Market Data Publisher] Subscription {data.get('success', False) and 'successful' or 'failed'} for {data.get('topic', '')}")
                return
            
            # 根據不同的數據類型進行處理
            if data.get('topic'):
                topic_parts = data['topic'].split('@')
                if len(topic_parts) > 1:
                    data_type = topic_parts[1]
                    if data_type.startswith('kline'):
                        await self.process_kline_data(symbol, interval, data)
                    else:
                        # 處理其他類型的市場數據...
                        channel = f"{symbol}-{data_type}"
                        await self.publish_to_redis(channel, data)
                        print(f"[Data Publisher] Published to Redis channel: {channel}")

        except Exception as e:
            print(f"[Market Data Publisher] Error: {e}")
    
    async def process_private_data(self, message):
        """Process private data and publish to Redis"""
        try:
            data = json.loads(message) if isinstance(message, str) else message

            # Handle ping-pong
            if data.get("event") == "ping":
                await self.handle_ping_pong(message, self.private_connection, "Private")
                return
            
            # Handle subscription confirmation
            if data.get("event") == "subscribe":
                print(f"[Private Data Publisher] Subscription {data.get('success', False) and 'successful' or 'failed'} for {data.get('topic', '')}")
                return

            # Print and publish all other messages
            # print("\n=== Private Data ===")
            # print(f"Topic: {data.get('topic')}")
            # print(f"Timestamp: {data.get('ts')}")
            # print("Data Content:")
            # print(json.dumps(data.get('data', {}), indent=2))
            # print("==================\n")

            # Publish to Redis with topic as channel name
            if data.get('topic'):
                await self.publish_to_redis(data['topic'], data)
                print(f"[Private Data Publisher] Published to Redis channel: {data['topic']}")

        except Exception as e:
            print(f"[Private Data Publisher] Error: {e}")
    
    async def start(self, symbol: str, market_config: dict, private_config: dict, interval: str='1m' ):
        """Start both market and private data streams with proper cleanup"""
        try:
            # Connect to Redis
            await self.connect_to_redis()
            
            # Connect and authenticate private stream
            await self.authenticate()
            await self.subscribe_private(private_config)
            
            # Connect and subscribe market stream
            await self.subscribe_market(symbol, market_config, interval)
            
            while True:
                market_task = None
                private_task = None
                
                if any(market_config.values()):
                    market_task = asyncio.create_task(self.market_connection.recv())
                if any(private_config.values()):
                    private_task = asyncio.create_task(self.private_connection.recv())
                
                tasks = [task for task in [market_task, private_task] if task is not None]
                
                try:
                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    for task in done:
                        if task == market_task:
                            message = await task
                            await self.process_market_data(symbol, interval, message)
                        elif task == private_task:
                            message = await task
                            await self.process_private_data(message)
                    
                    for task in pending:
                        task.cancel()
                        
                    await asyncio.sleep(0.1)
                    
                except asyncio.CancelledError:
                    # Handle the cancellation more gracefully
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    raise
                    
        except asyncio.CancelledError:
            print("\nReceived shutdown signal...")
        except Exception as e:
            print(f"Error in main loop: {e}")
        finally:
            await self.close_connections()
    
async def main():
    # Initialize 
    app_id = "460c97db-f51d-451c-a23e-3cce56d4c932"
    api_key = 'sdFgbf5mnyDD/wahfC58Kw=='
    api_secret = 'FWQGXZCW4P3V4D4EN4EIBL6KLTDA'
    
    # Initialize API
    api = WooXStagingAPI(app_id=app_id, api_key=api_key, api_secret=api_secret, redis_host="localhost")
    
    # Market data configuration
    symbol = "PERP_ETH_USDT"
    interval = "1m"
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
        await api.start(symbol, market_config, private_config, interval)
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        print(f"Program error: {str(e)}")

# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         print("\nProgram terminated by user")
#     except Exception as e:
#         print(f"Program error: {str(e)}")