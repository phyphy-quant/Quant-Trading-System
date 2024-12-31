import json
import asyncio
import logging
import time
import aiohttp
from typing import Dict, Optional
from redis import asyncio as aioredis
from datetime import datetime

from manager.order_manager import OrderManager, OrderInfo, OrderStatus
from manager.risk_manager import RiskManager
from WooX_REST_API_Client import WooX_REST_API_Client

class OrderExecutor:
    """Order execution manager that handles trading signals and executes orders"""
    
    def __init__(self, api_key: str, api_secret: str, redis_url: str = "redis://localhost:6379",hedge_mode: bool = False):
        self.redis_url = redis_url
        self.redis_client = None
        
        # Initialize API client
        self.api = WooX_REST_API_Client(api_key, api_secret)
        
        # Initialize managers
        self.order_manager = OrderManager()
        self.risk_manager = RiskManager({})  # Add risk params as needed
        
        # Setup logging
        self.logger = logging.getLogger("OrderExecutor")
        self.logger.setLevel(logging.INFO)
        
        # Track active orders and tasks
        self.active_orders: Dict[str, Dict] = {}
        self.order_tasks = []
        
        # Status flags
        self.is_running = False
        self.hedge_mode = hedge_mode  # 由外部決定: True => Hedge; False => One way

    async def connect_redis(self) -> None:
        """Connect to Redis server"""
        try:
            self.redis_client = await aioredis.from_url(self.redis_url)
            self.logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def subscribe_to_channels(self, signal_channel: str, execution_channel: str) -> tuple:
        """Subscribe to signal and execution channels"""
        try:
            signal_pubsub = self.redis_client.pubsub()
            execution_pubsub = self.redis_client.pubsub()
            self.logger.info(f"Starting subscribe_to_channels: signal={signal_channel}, exec={execution_channel}")
            
            await signal_pubsub.subscribe(signal_channel)
            await execution_pubsub.subscribe(execution_channel)
            
            self.logger.info(f"Subscribed to channels: {signal_channel}, {execution_channel}")
            return signal_pubsub, execution_pubsub
            
        except Exception as e:
            self.logger.error(f"Error subscribing to channels: {e}")
            raise

    async def execute_order(self, signal: dict, session: aiohttp.ClientSession) -> dict:
        """Execute order based on signal with or without hedge_mode"""
        try:
            # Check risk limits
            # 關掉風控
            # can_trade, reason = self.risk_manager.can_place_order(
            #     symbol=signal['symbol'],
            #     order_size=signal['quantity'],
            #     order_price=signal.get('price', 0)
            # )
            
            # if not can_trade:
            #     return {'success': False, 'error': reason}

            action = signal.get('action')              # 'OPEN'/'CLOSE'
            if self.hedge_mode:
                # Hedge => 會有 position_side='LONG'/'SHORT'
                pos_side = signal.get('position_side')
                action_side_map = {
                    ('OPEN',  'LONG'):  'BUY',
                    ('CLOSE', 'LONG'):  'SELL',
                    ('OPEN',  'SHORT'): 'SELL',
                    ('CLOSE', 'SHORT'): 'BUY'
                }
                side = action_side_map.get((action, pos_side))
                order_params = {
                    'client_order_id': int(time.time()*1000),
                    'symbol': signal['symbol'],
                    'side': side,
                    'position_side': pos_side,  # 'LONG'/'SHORT'
                    'order_type': signal['order_type'],
                    'order_quantity': signal['quantity'],
                    'reduce_only': signal.get('reduce_only', False),
                    'margin_mode': signal.get('margin_mode', 'CROSS')
                }
                
                # 若是 LIMIT，需加上 'order_price'
                if signal['order_type'] == 'LIMIT':
                    order_params['order_price'] = signal['price']
            else:
                # 單向 => 只會帶 'side'='BUY'/'SELL' in the signal
                side = signal.get('side')
                order_params = {
                    'client_order_id': int(time.time()*1000),
                    'symbol': signal['symbol'],
                    'side': side,   # 'BUY' or 'SELL'
                    'order_type': signal['order_type'],
                    'order_quantity': signal['quantity'],
                    'reduce_only': signal.get('reduce_only', False)
                }
                # 若是 LIMIT，需加上 'order_price'
                if signal['order_type'] == 'LIMIT':
                    order_params['order_price'] = signal['price']
            
            # Create order tracking
            order_info = OrderInfo(
                order_id=str(order_params['client_order_id']),
                client_order_id=str(order_params['client_order_id']),
                symbol=signal['symbol'],
                side=side,
                order_type=signal['order_type'],
                price=signal.get('price', 0),
                quantity=signal['quantity'],
                status=OrderStatus.PENDING,
                create_time=datetime.now()
            )
            self.order_manager.add_order(order_info)
            
            # Send order to exchange
            self.logger.info(f"Sending order: {order_params}")
            result = await self.api.send_order(session, order_params)
            
            if result.get('success'):
                self.logger.info(f"Order successfully placed: {result}")
            else:
                self.logger.error(f"Order placement failed: {result}")
            
            return result
        except Exception as e:
            self.logger.error(f"Error executing order: {e}")
            return {'success': False, 'error': str(e)}

    async def process_execution_report(self, report: dict) -> None:
        """Process execution report updates"""
        try:
            client_order_id = report.get('clientOrderId')
            if not client_order_id:
                return
                
            self.order_manager.update_order(client_order_id, {
                'status': report.get('status'),
                'executed_quantity': report.get('executedQuantity'),
                'fill_info': {
                    'price': report.get('price'),
                    'quantity': report.get('executedQuantity'),
                    'time': datetime.now().isoformat()
                }
            })
            
            # Update risk tracking if order is filled
            if report.get('status') == 'FILLED':
                await self.risk_manager.update_position(report)
                
        except Exception as e:
            self.logger.error(f"Error processing execution report: {e}")

    async def listen_for_signals(self, pubsub: aioredis.client.PubSub) -> None:
        """Listen for trading signals"""
        try:
            self.logger.info("Started listening for trading signals")
            async with aiohttp.ClientSession() as session:
                async for message in pubsub.listen():
                    if not self.is_running:
                        break
                        
                    if message["type"] == "message":
                        signal = json.loads(message["data"])
                        self.logger.info(f"Received signal: {signal}")
                        
                        result = await self.execute_order(signal, session)
                        self.logger.info(f"Order execution result: {result}")
                        
        except asyncio.CancelledError:
            self.logger.info("Signal listener cancelled")
        except Exception as e:
            self.logger.error(f"Error in signal listener: {e}")

    async def listen_for_execution_reports(self, pubsub: aioredis.client.PubSub) -> None:
        """Listen for execution reports"""
        try:
            self.logger.info("Started listening for execution reports")
            async for message in pubsub.listen():
                if not self.is_running:
                    break
                    
                if message["type"] == "message":
                    report = json.loads(message["data"])
                    await self.process_execution_report(report)
                    
        except asyncio.CancelledError:
            self.logger.info("Execution report listener cancelled")
        except Exception as e:
            self.logger.error(f"Error in execution report listener: {e}")

    async def start(self, signal_channel: str, execution_channel: str) -> None:
        """Start the order executor"""
        try:
            if not self.redis_client:
                await self.connect_redis()
            
            self.is_running = True
            
            # Subscribe to channels
            signal_pubsub, execution_pubsub = await self.subscribe_to_channels(
                signal_channel, execution_channel
            )
            
            # Create listener tasks
            tasks = [
                asyncio.create_task(self.listen_for_signals(signal_pubsub)),
                asyncio.create_task(self.listen_for_execution_reports(execution_pubsub))
            ]
            
            # Run all tasks
            await asyncio.gather(*tasks)
            
        except Exception as e:
            self.logger.error(f"Error starting order executor: {e}")
            raise
        finally:
            await self.cleanup()

    async def cleanup(self) -> None:
        """Cleanup resources"""
        self.is_running = False
        
        # Close Redis connection
        if self.redis_client:
            await self.redis_client.aclose()
            
        # Clean up old orders
        self.order_manager.cleanup_old_orders()
        
        self.logger.info("Order executor cleaned up")

    async def stop(self) -> None:
        """Stop the order executor"""
        self.is_running = False
        await self.cleanup()
        self.logger.info("Order executor stopped")

# async def main():
#     """Main function for testing"""
#     try:
#         # Initialize with your API credentials
#         app_id = "460c97db-f51d-451c-a23e-3cce56d4c932"
#         api_key = 'sdFgbf5mnyDD/wahfC58Kw=='
#         api_secret = 'FWQGXZCW4P3V4D4EN4EIBL6KLTDA'
#         redis_url = "redis://localhost:6379"
        
#         # Create order executor instance
#         executor = OrderExecutor(
#             api_key=api_key,
#             api_secret=api_secret,
#             redis_url=redis_url
#         )
        
#         # Connect to Redis
#         await executor.connect_redis()
        
#         # Start the executor
#         await executor.start(
#             signal_channel="trading_signals",
#             execution_channel="execution_reports"
#         )
        
#     except KeyboardInterrupt:
#         logging.info("Shutting down...")
#     except Exception as e:
#         logging.error(f"Error in main: {e}")
#     finally:
#         if 'executor' in locals():
#             await executor.stop()

# if __name__ == "__main__":
#     # Setup logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
    
#     # Run the main function
#     asyncio.run(main())