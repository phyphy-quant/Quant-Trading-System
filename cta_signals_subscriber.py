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
        
        # 透過 cta_strategy.yaml 設定或直接硬編
        self.cta_symbol = "PERP_BTC_USDT"

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
        訂閱:
          1) cta_signals
          2) cta_executionreports
          3) [PD]position
        """
        if not self.redis:
            await self.connect_to_redis()

        cta_channels = [
            "cta_signals",
            "cta_executionreports",
            "[PD]position"   # 私有頻道 position
        ]
        
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(*cta_channels)
        print(f"Subscribed to CTA channels: {cta_channels}")

        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message is not None and message["type"] == "message":
                    channel = message["channel"]
                    data = message["data"]

                    # 首先，只在「我們關心的channel」才做進一步處理
                    # cta_signals / cta_executionreports / [PD]position
                    if channel == "cta_signals":
                        # 先印channel & raw data
                        print(f"\n=== [CTA Subscriber] 收到訊息 ===")
                        print(f"Channel: {channel}")
                        print(f"Raw Data: {data}")
                        print("----------------------------------")

                        try:
                            parsed = json.loads(data)
                            await self.process_cta_signal(parsed)
                        except json.JSONDecodeError:
                            print("無法 JSON 解析，原始資料：", data)

                    elif channel == "cta_executionreports":
                        # 同樣處理
                        print(f"\n=== [CTA Subscriber] 收到訊息 ===")
                        print(f"Channel: {channel}")
                        print(f"Raw Data: {data}")
                        print("----------------------------------")

                        try:
                            parsed = json.loads(data)
                            await self.process_cta_execution_report(parsed)
                        except json.JSONDecodeError:
                            print("無法 JSON 解析，原始資料：", data)

                    elif channel == "[PD]position":
                        # 改良: 先解析 => 若有包含 cta_symbol 才印
                        try:
                            parsed = json.loads(data)
                            if self.should_print_position(parsed):
                                # 符合 symbol => 再顯示
                                print(f"\n=== [CTA Subscriber] 收到訊息 ===")
                                print(f"Channel: {channel}")
                                print(f"Raw Data: {data}")
                                print("----------------------------------")
                                await self.process_position_update(parsed)
                            else:
                                # 不印任何東西
                                pass
                        except json.JSONDecodeError:
                            # 如果 JSON 解析失敗，也不印原始資料
                            pass

                await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            print("\n[CTA Subscriber] Unsubscribing from CTA channels...")
            await pubsub.unsubscribe()
            await self.redis.aclose()
        except Exception as e:
            print(f"Error in CTA subscription: {e}")

    # -----------------------------
    #  Signals / ExecReport handlers
    # -----------------------------
    async def process_cta_signal(self, signal_dict: dict):
        print("[CTA Signal] 解析後資料:")
        for k, v in signal_dict.items():
            print(f"  {k}: {v}")

    async def process_cta_execution_report(self, report_dict: dict):
        print("[CTA ExecReport] 解析後資料:")
        for k, v in report_dict.items():
            print(f"  {k}: {v}")

    # -----------------------------
    #  Position handler
    # -----------------------------
    def should_print_position(self, position_dict: dict) -> bool:
        """
        檢查 position 資訊中是否包含 self.cta_symbol
        position_dict 結構可能是：
          {
            "topic": "position",
            "ts": 1735632720684,
            "data": {
              "positions": {
                 "PERP_BTC_USDT": {...},
                 "PERP_ETH_USDT": {...}
              }
            }
          }
        如果 positions 裡沒有 self.cta_symbol，就返回 False
        """
        data = position_dict.get("data", {})
        positions = data.get("positions", {})
        # 如果 symbol not in positions => False
        return self.cta_symbol in positions

    async def process_position_update(self, position_dict: dict):
        """
        只針對包含 self.cta_symbol 的 position 資訊做印出
        """
        data = position_dict.get("data", {})
        positions_dict = data.get("positions", {})
        # 取出 symbol
        sym_position = positions_dict.get(self.cta_symbol, {})

        print(f"[CTA Subscriber] position update for {self.cta_symbol}")
        for k, v in sym_position.items():
            print(f"  {k}: {v}")

        holding = float(sym_position.get("holding", 0.0))
        if holding > 0:
            print(f"  => LONG position={holding}")
        elif holding < 0:
            print(f"  => SHORT position={holding}")
        else:
            print("  => FLAT / no position")

async def main():
    subscriber = DataSubscriber(redis_host="localhost", redis_port=6379)
    await subscriber.subscribe_cta_channels()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        print(f"Program error: {str(e)}")