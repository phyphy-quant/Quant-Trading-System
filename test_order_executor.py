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
    """Order execution manager that handles trading signals and executes orders 
       with local_id <-> exchange_id mapping (做法B)"""

    def __init__(self, 
                 api_key: str, 
                 api_secret: str, 
                 redis_url: str = "redis://localhost:6379",
                 hedge_mode: bool = False):
        self.redis_url = redis_url
        self.redis_client: Optional[aioredis.Redis] = None
        
        # Initialize API client
        self.api = WooX_REST_API_Client(api_key, api_secret)
        
        # Initialize managers
        self.order_manager = OrderManager()
        self.risk_manager = RiskManager({})
        
        # Setup logging
        self.logger = logging.getLogger("OrderExecutor")
        self.logger.setLevel(logging.INFO)
        
        self.is_running = False
        self.hedge_mode = hedge_mode

        # Default channels
        self.signal_channel = "cta_signals"
        self.execution_report_channel = "cta_executionreports"
        self.private_report_channel = "[PD]executionreport"

    async def connect_redis(self) -> None:
        try:
            self.redis_client = await aioredis.from_url(self.redis_url)
            self.logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def start(self, 
                    signal_channel: str, 
                    execution_report_channel: str,
                    private_report_channel: str = "[PD]executionreport") -> None:
        self.signal_channel = signal_channel
        self.execution_report_channel = execution_report_channel
        self.private_report_channel = private_report_channel

        if not self.redis_client:
            await self.connect_redis()
        self.is_running = True
        
        # 1) Subscribe to signals
        signal_pubsub = self.redis_client.pubsub()
        await signal_pubsub.subscribe(self.signal_channel)
        
        # 2) Subscribe to private channel
        private_pubsub = self.redis_client.pubsub()
        await private_pubsub.subscribe(self.private_report_channel)
        
        tasks = [
            asyncio.create_task(self.listen_for_signals(signal_pubsub)),
            asyncio.create_task(self.listen_for_private_reports(private_pubsub))
        ]
        self.logger.info(f"OrderExecutor started => signals={signal_channel}, private={private_report_channel}")
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"Error in OrderExecutor => {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        self.is_running = False
        if self.redis_client:
            await self.redis_client.aclose()
        self.order_manager.cleanup_old_orders()
        self.logger.info("Order Executor cleaned up")

    async def stop(self):
        self.is_running = False
        await self.cleanup()
        self.logger.info("Order Executor stopped")

    #------------------------------------------------------------------
    # 1) cta_signals => create local order => exchange_id => send to exchange
    #------------------------------------------------------------------
    async def listen_for_signals(self, pubsub: aioredis.client.PubSub):
        self.logger.info(f"Started listening for signals => {self.signal_channel}")
        async with aiohttp.ClientSession() as session:
            async for message in pubsub.listen():
                if not self.is_running:
                    break
                if message["type"] == "message":
                    data_str = message["data"]
                    if isinstance(data_str, bytes):
                        data_str = data_str.decode("utf-8")
                    signal = json.loads(data_str)
                    self.logger.info(f"[OrderExecutor] Received signal => {signal}")
                    
                    result = await self.execute_order(signal, session)
                    self.logger.info(f"[OrderExecutor] Order execution result => {result}")

    async def execute_order(self, signal: dict, session: aiohttp.ClientSession) -> dict:
        """
        1) local_id = signal["client_order_id"] (ex: "LONG_LIMIT_1735624500111")
        2) exchange_id = int(time.time()*1000)  # or any integer
        3) Add to OrderManager => self.order_manager.add_order(order_info, exchange_id=exchange_id)
        4) Send to exchange => "client_order_id"=exchange_id
        """
        try:
            action = signal.get('action')  # 'OPEN'/'CLOSE'/'CANCEL_ORDER'
            if action == 'CANCEL_ORDER':
                return await self.cancel_order(signal, session)

            if self.hedge_mode:
                pos_side = signal.get('position_side')
                action_side_map = {
                    ('OPEN','LONG'): 'BUY',
                    ('CLOSE','LONG'):'SELL',
                    ('OPEN','SHORT'):'SELL',
                    ('CLOSE','SHORT'):'BUY'
                }
                side = action_side_map.get((action,pos_side),'BUY')
            else:
                side = signal.get('side')  # 'BUY'/'SELL'
            
            order_type = signal.get('order_type','MARKET').upper()
            quantity   = float(signal.get('quantity',0))
            symbol     = signal.get('symbol','PERP_BTC_USDT')
            reduce_only= signal.get('reduce_only',False)
            margin_mode= signal.get('margin_mode','CROSS')
            price      = float(signal.get('price',0.0)) if order_type=='LIMIT' else 0.0

            # local_id => ex: "LONG_LIMIT_1735624500111"
            local_id = signal.get('client_order_id', f"LOCAL_{int(time.time()*1000)}")

            # exchange_id => 純整數
            exchange_id = int(time.time() * 1000)

            # 建立 local order
            order_info = OrderInfo(
                order_id=local_id,
                client_order_id=local_id,
                symbol=symbol,
                side=side,
                order_type=order_type,
                price=price,
                quantity=quantity,
                status=OrderStatus.PENDING,
                create_time=datetime.now()
            )
            # 加入 manager (含 local<->exchange map)
            self.order_manager.add_order(order_info, exchange_id=exchange_id)

            # 先 publish NEW 狀態
            await self.publish_execution_report(local_id, status=OrderStatus.NEW)

            # 準備送到交易所
            order_params = {
                'client_order_id': exchange_id,  # 這裡用整數
                'symbol': symbol,
                'side': side,
                'order_type': order_type,
                'order_quantity': quantity,
                'reduce_only': reduce_only
            }
            if self.hedge_mode:
                order_params['position_side'] = signal.get('position_side')
                order_params['margin_mode']   = margin_mode
            if order_type=='LIMIT':
                order_params['order_price'] = price

            self.logger.info(f"[OrderExecutor] Sending to exchange => {order_params}")
            result = await self.api.send_order(session, order_params)
            if result.get('success'):
                self.logger.info(f"Order placed => {result}")
            else:
                self.logger.error(f"Order placement fail => {result}")
            
            return result
        except Exception as e:
            self.logger.error(f"execute_order error => {e}")
            return {"success": False, "error": str(e)}

    async def cancel_order(self, signal: dict, session: aiohttp.ClientSession) -> dict:
        """
        CANCEL_ORDER => local_id
        1) 查 exchange_id = self.order_manager.get_exchange_id_by_local_id(local_id)
        2) 送給交易所時用 exchange_id
        """
        local_id = signal.get('client_order_id')
        if not local_id:
            return {"success":False,"error":"No local_id to cancel"}

        exchange_id = self.order_manager.get_exchange_id_by_local_id(local_id)
        if exchange_id is None:
            self.logger.warning(f"No exchange_id mapped for local_id={local_id}")
            return {"success":False,"error":"No mapped exchange_id => not found locally"}

        local_info = self.order_manager.get_order_info(local_id)
        if not local_info:
            self.logger.warning(f"No local record => {local_id}")
            return {"success":False,"error":"Order not found locally"}

        # 交易所的cancel => 需看你WooX API => e.g. /v1/client/order => client_order_id=exchange_id
        params = {"client_order_id": exchange_id}
        self.logger.info(f"[OrderExecutor] Cancelling => {params}")
        result = await self.api.cancel_order_by_client_order_id(session, params)
        if result.get('error'):
            self.logger.error(f"Cancel fail => {result}")
        else:
            self.logger.info(f"Cancel success => {result}")

        # update local
        local_info.status = OrderStatus.CANCELLED
        self.order_manager.update_order(local_id, {
            'status': OrderStatus.CANCELLED.value,
            'executed_quantity': local_info.executed_quantity
        })
        await self.publish_execution_report(local_id, status=OrderStatus.CANCELLED)
        return result

    async def publish_execution_report(self, local_id: str, status: OrderStatus, executed_qty: float=0.0, price: float=0.0):
        """Publish execution report => cta_executionreports, using local_id for strategy"""
        # 先拿 local_info
        local_info = self.order_manager.get_order_info(local_id)
        if not local_info:
            # fallback => 只用 minimal data
            data = {
                "clientOrderId": local_id,
                "symbol": "UNKNOWN",
                "status": status.value,
                "executedQuantity": executed_qty,
                "price": price,
                "timestamp": int(time.time()*1000)
            }
        else:
            data = {
                "clientOrderId": local_info.order_id,
                "symbol": local_info.symbol,
                "status": status.value,
                "executedQuantity": executed_qty if executed_qty>0 else local_info.executed_quantity,
                "price": price if price>0 else local_info.price,
                "timestamp": int(time.time()*1000)
            }

        if self.redis_client:
            await self.redis_client.publish(self.execution_report_channel, json.dumps(data))
            self.logger.info(f"[OrderExecutor] Published => {data}")

    #------------------------------------------------------------------
    # 2) listen_for_private_reports => [PD]executionreport => 只有 exchange_id => 反查 local_id
    #------------------------------------------------------------------
    async def listen_for_private_reports(self, pubsub: aioredis.client.PubSub):
        self.logger.info(f"Started listening for private => {self.private_report_channel}")
        async for message in pubsub.listen():
            if not self.is_running:
                break
            if message["type"] == "message":
                raw_msg = message["data"]
                if isinstance(raw_msg, bytes):
                    raw_msg = raw_msg.decode("utf-8")
                try:
                    data = json.loads(raw_msg)
                    topic = data.get("topic")  # "executionreport"
                    inner = data.get("data", {})
                    
                    if topic == "executionreport":
                        # 交易所真正帶的 => exchange_id
                        exchange_id_str = inner.get("clientOrderId","")
                        if not exchange_id_str:
                            continue
                        
                        # convert str->int if needed
                        try:
                            exchange_id = int(exchange_id_str)
                        except:
                            # 如果交易所回報也是字串 => 可能不需轉 int
                            exchange_id = exchange_id_str

                        # 反查 local_id
                        if isinstance(exchange_id, int):
                            local_id = self.order_manager.get_local_id_by_exchange_id(exchange_id)
                        else:
                            # fallback => maybe local_id=exchange_id_str
                            local_id = exchange_id
                        
                        if not local_id:
                            self.logger.debug(f"Ignored exec => exchange_id={exchange_id} not in local map")
                            continue

                        # 解析回報 => e.g. status, execQty, price
                        new_status = inner.get("status","")
                        exec_qty   = float(inner.get("executedQuantity",0.0))
                        trade_px   = float(inner.get("tradePrice",0.0))

                        # Update local
                        updates = {
                            "status": new_status,
                            "executed_quantity": exec_qty
                        }
                        self.order_manager.update_order(local_id, updates)

                        # Publish => cta_executionreports
                        await self.publish_execution_report(
                            local_id, 
                            status=OrderStatus(new_status), 
                            executed_qty=exec_qty,
                            price=trade_px
                        )
                    else:
                        pass
                except Exception as e:
                    self.logger.error(f"Error parse private => {e}")