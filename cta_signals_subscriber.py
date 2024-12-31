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

    async def subscribe_cta_channels(self):
        """
        訂閱 CTA 策略會用到的 channels:
         - cta_signals (策略可能會publish OPEN/CLOSE等signal)
         - cta_executionreports (策略可能publish的成交回報、或是下單執行情況)
        
        之後持續接收訊息並印出。
        """
        if not self.redis:
            await self.connect_to_redis()

        # 你在 cta_strategy.yaml 裡設定:
        #  channels:
        #    signal_channel: "cta_signals"
        #    execution_report_channel: "cta_executionreports"
        # 所以我們就訂閱以下兩個:
        cta_channels = ["cta_signals", "cta_executionreports"]
        
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(*cta_channels)
        print(f"Subscribed to CTA channels: {cta_channels}")

        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    channel = message["channel"]
                    data = message["data"]

                    # 如果只是 test ping / subscribe 事件，可以忽略
                    if message["type"] != "message":
                        continue

                    # 印出我們收到的訊息
                    print(f"\n=== [CTA Subscriber] 收到訊息 ===")
                    print(f"Channel: {channel}")
                    print(f"Raw Data: {data}")
                    print("----------------------------------")

                    # 假設 CTA Strategy publish 出的 signal 可能是 JSON
                    # 我們可以嘗試解析並印出更細節
                    try:
                        parsed = json.loads(data)
                        if channel == "cta_signals":
                            await self.process_cta_signal(parsed)
                        elif channel == "cta_executionreports":
                            await self.process_cta_execution_report(parsed)
                    except json.JSONDecodeError:
                        print("無法 JSON 解析，原始資料：", data)

                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            print("\n[CTA Subscriber] Unsubscribing from CTA channels...")
            await pubsub.unsubscribe()
            await self.redis.aclose()
        except Exception as e:
            print(f"Error in CTA subscription: {e}")

    async def process_cta_signal(self, signal_dict: dict):
        """
        針對 CTA Strategy 發出來的 signal (action=OPEN/CLOSE) 做處理或印出
        signal 內容範例:
          {
            "timestamp": 1697522220000,
            "action": "OPEN",
            "side": "BUY",               # 單向模式
            "order_type": "MARKET",
            "symbol": "PERP_BTC_USDT",
            "quantity": 0.01,
            ...
          }
          或者 (Hedge 模式)
          {
            "timestamp": 1697522220000,
            "action": "OPEN",
            "position_side": "LONG",
            ...
          }
        """
        print("[CTA Signal] 解析後資料:")
        for k, v in signal_dict.items():
            print(f"  {k}: {v}")

    async def process_cta_execution_report(self, report_dict: dict):
        """
        針對 CTA Strategy 發出來的 execution report (若有) 做處理或印出
        內容範例:
          {
            "timestamp": 1697522221000,
            "action": "EXEC_REPORT",
            "order_id": "...",
            "status": "FILLED",
            ...
          }
        """
        print("[CTA ExecReport] 解析後資料:")
        for k, v in report_dict.items():
            print(f"  {k}: {v}")

# 如果需要，也可以保留你原本 DataSubscriber 其它方法 (process_kline_data, process_trade_data...) 在這裏

async def main():
    # 初始化訂閱器
    subscriber = DataSubscriber(redis_host="localhost", redis_port=6379)
    # 直接訂閱 CTA channels (cta_signals / cta_executionreports)
    await subscriber.subscribe_cta_channels()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        print(f"Program error: {str(e)}")