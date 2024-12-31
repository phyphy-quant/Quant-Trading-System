import datetime
import hmac, hashlib, base64
import aiohttp
import json
import asyncio
import time

class WooX_REST_API_Client:
    def __init__(self, api_key, api_secret, base_url='https://api.staging.woox.io'):
        print(api_key, api_secret)
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url

    async def make_request(self, session, endpoint, params=None, requests_type="get", version="v1", signature=True):
        def _generate_signature(sig_body):
            key_bytes = bytes(self.api_secret, 'utf-8')
            body_bytes = bytes(sig_body, 'utf-8')
            return hmac.new(key_bytes, body_bytes, hashlib.sha256).hexdigest()

        milliseconds_since_epoch = round(datetime.datetime.now().timestamp() * 1000)
        url = self.base_url + endpoint
        params = params or {}

        if version == "v1":
            # 格式化布林值為小寫true/false，確保side、order_type為大寫
            for k, v in params.items():
                if isinstance(v, bool):
                    params[k] = str(v).lower()  # 'true'或'false'
            
            # 假設你的side、order_type來自策略，需要大寫
            if 'side' in params:
                params['side'] = params['side'].upper()  # 'BUY'或'SELL'
            if 'order_type' in params:
                params['order_type'] = params['order_type'].upper()  # 'MARKET'或'LIMIT'

            # 轉成字串並排序
            params = {k: str(v) for k, v in params.items()}
            sorted_params = sorted(params.items(), key=lambda x: x[0])
            body = "&".join(f"{key}={value}" for key, value in sorted_params)
            signature_body = body + f"|{str(milliseconds_since_epoch)}"
        else:
            body = json.dumps(params)
            signature_body = f"{milliseconds_since_epoch}{requests_type.upper()}{endpoint}{body}"

        headers = {
            'x-api-timestamp': str(milliseconds_since_epoch),
            'x-api-key': self.api_key,
            'x-api-signature': _generate_signature(signature_body),
            'Cache-Control': 'no-cache'
        }

        if version == "v3":
            headers['Content-Type'] = 'application/json'
        else:
            headers['Content-Type'] = 'application/x-www-form-urlencoded'

        print("\nURL:", url)
        # 清楚分開印出實際要送出去的body和簽名用body
        print("Actual Request Data (body):", body)  # 真正送出的資料
        print("Signature Body (for sign):", signature_body) # 計算簽名的字串

        try:
            if requests_type == "get":
                async with session.get(url, headers=headers if signature else None, params=params) as response:
                    response.raise_for_status()
                    return await response.json()

            elif requests_type == "post":
                if version == "v1":
                    async with session.post(url, headers=headers, data=body) as response:
                        response.raise_for_status()
                        return await response.json()
                else:
                    async with session.post(url, headers=headers, json=params) as response:
                        response.raise_for_status()
                        return await response.json()

            elif requests_type == "put":
                if version == "v1":
                    async with session.put(url, headers=headers, data=body) as response:
                        response.raise_for_status()
                        return await response.json()
                else:
                    async with session.put(url, headers=headers, json=params) as response:
                        response.raise_for_status()
                        return await response.json()

            elif requests_type == "delete":
                if version == "v1":
                    async with session.delete(url, headers=headers, data=body) as response:
                        response.raise_for_status()
                        return await response.json()
                else:
                    async with session.delete(url, headers=headers, json=params) as response:
                        response.raise_for_status()
                        return await response.json()

            else:
                raise ValueError(f"Unsupported request type: {requests_type}")

        except aiohttp.ClientResponseError as e:
            print(f"[HTTP Error] Status: {e.status}, Message: {e.message}")
            return {"error": f"HTTP {e.status}: {e.message}"}
        except Exception as e:
            print(f"[Error] {e}")
            return {"error": str(e)}

    async def get_orderbook(self, session, symbol, max_level=100):
        endpoint = f'/v1/public/orderbook/{symbol}'
        params = {"max_level": max_level}
        return await self.make_request(session, endpoint, params)

    async def get_trades(self, session, symbol, limit=100):
        endpoint = f'/v1/public/market_trades'
        params = {"symbol": symbol, "limit": limit}
        return await self.make_request(session, endpoint, params)

    async def get_kline(self, session, symbol, interval="1m", limit=100):
        endpoint = f'/v1/public/kline'
        params = {"symbol": symbol, "type": interval, 'limit': limit}
        return await self.make_request(session, endpoint, params)

    async def send_order(self, session, params):
        endpoint = '/v1/order'
        return await self.make_request(session, endpoint, params, requests_type="post")

    async def send_algo_order(self, session, params):
        endpoint = '/v3/algo/order'
        return await self.make_request(session, endpoint, params, requests_type="post", version="v3")

    async def edit_order_by_client_order_id(self, session, params, client_order_id):
        endpoint = f'/v3/order/client/{client_order_id}'
        return await self.make_request(session, endpoint, params, requests_type="put", version="v3", signature=True)

    async def cancel_order_by_client_order_id(self, session, params):
        endpoint = '/v1/client/order'
        return await self.make_request(session, endpoint, params, requests_type="delete", version="v1")

    async def cancel_all_pending_orders(self, session):
        endpoint = '/v3/orders/pending'
        return await self.make_request(session, endpoint, params={}, requests_type="delete", version="v3")

async def busy_loop(api, session, symbol):
    tasks = [
        asyncio.create_task(api.get_orderbook(session, symbol)),
        asyncio.create_task(api.get_trades(session, symbol)),
        asyncio.create_task(api.get_kline(session, symbol))
    ]

    order_tasks = []
    task_names = ["Orderbook", "Trades", "Kline"]
    last_order_time = time.time()
    count =0

    while True:
        print(f"\nPolling for {symbol} data...\n")
        for i, task in enumerate(tasks):
            if task.done():
                try:
                    result = await task
                    print(f"[{task_names[i]}] done.")
                    # print(f"[{task_names[i]}] result={result}")
                    if task_names[i] == "Orderbook":
                        tasks[i] = asyncio.create_task(api.get_orderbook(session, symbol))
                    elif task_names[i] == "Trades":
                        tasks[i] = asyncio.create_task(api.get_trades(session, symbol))
                    elif task_names[i] == "Kline":
                        tasks[i] = asyncio.create_task(api.get_kline(session, symbol))

                except Exception as e:
                    print(f"[{task_names[i]} Error] {e}")

        current_time = time.time()
        if current_time - last_order_time >= 2:  # 2 seconds have passed
            count += 1
            print("[Condition Met] Sending an order...")
            params = {
                'client_order_id': count,
                'order_price': 3190,
                'order_quantity': 0.001,
                'order_type': 'MARKET',
                'side':'BUY',
                'symbol': 'SPOT_ETH_USDT'
            }
            order_tasks.append(
                asyncio.create_task(api.send_order(session, params))
            )
            last_order_time = current_time  # Update the last order time
        
        # Process order tasks
        for order_task in list(order_tasks):  # Iterate over a copy of the list
            if order_task.done():
                try:
                    result = await order_task
                    print("[Order Task Completed] Result:", result)
                except Exception as e:
                    print("[Order Task Error]:", e)
                finally:
                    order_tasks.remove(order_task)  # Clean up completed task

        # await background_processing_task()
        await asyncio.sleep(0.1)

async def main():
    api_key = 'sdFgbf5mnyDD/wahfC58Kw=='
    api_secret = 'FWQGXZCW4P3V4D4EN4EIBL6KLTDA'
    woox_api = WooX_REST_API_Client(api_key, api_secret)

    symbol = 'SPOT_BTC_USDT'

    async with aiohttp.ClientSession() as session:
        await busy_loop(woox_api, session, symbol)

# if __name__ == "__main__":
#     asyncio.run(main())