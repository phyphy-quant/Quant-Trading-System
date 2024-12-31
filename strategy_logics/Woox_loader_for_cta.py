import pandas as pd
import requests
import pytz
from datetime import datetime, timedelta

class WooXPublicAPI:
    def __init__(self, api_key, base_url='https://api-pub.woo.org'):
        self.api_key = api_key
        self.base_url = base_url
    
    def make_request(self, endpoint, params={}):
        headers = {
            'Authorization': f'Bearer {self.api_key}'
        }
        response = requests.get(self.base_url + endpoint, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    
    def get_kline_data(self, symbol, interval, start_time, end_time):
        """
        從 [start_time, end_time] 抓 K 線資料 (若有返回).
        start_time, end_time: Python datetime物件, 會轉成毫秒傳給API.
        回傳 DataFrame或 None, 其欄位: ['open','close','low','high','volume','amount','symbol','type','start_timestamp','end_timestamp']
        """
        endpoint = '/v1/hist/kline'
        params = {
            'symbol': symbol,
            'type': interval,
            'start_time': int(start_time.timestamp()*1000),
            'end_time':   int(end_time.timestamp()*1000)
        }
        data = self.make_request(endpoint, params)
        if data.get('success') and data.get('data') and 'rows' in data['data']:
            df = pd.DataFrame(
                data['data']['rows'], 
                columns=['open','close','low','high','volume','amount','symbol','type','start_timestamp','end_timestamp']
            )
            return df
        return None

def fetch_recent_kline_woo(api: WooXPublicAPI, symbol: str, interval: str = "1m", warmup_bars: int = 14):
    """
    從 "現在(本地 UTC+8)時間" 往前 warmup_bars (多+3根緩衝) 
    取得 "已完成" 的 K 線(避免拿到尚未收盤的bar)。
    回傳 DataFrame => 欄位: ['date','open','high','low','close','volume']，
    其中 'date' 取 start_timestamp (並轉成本地UTC+8)。
    """
    local_tz = pytz.timezone("Asia/Taipei")
    now_local = datetime.now(tz=local_tz)   # 當前本地時間(UTC+8)
    end_utc = now_local.astimezone(pytz.UTC)  # 轉成UTC

    # 計算 interval => 幾分鐘
    minutes_per_bar = 1
    if interval.endswith('m'):
        try:
            minutes_per_bar = int(interval[:-1])
        except:
            minutes_per_bar = 1
    elif interval.endswith('h'):
        hours = int(interval[:-1])
        minutes_per_bar = hours * 60

    # 多加3根緩衝
    total_back = (warmup_bars + 3) * minutes_per_bar
    start_utc = end_utc - timedelta(minutes=total_back)

    # 呼叫 get_kline_data
    df_raw = api.get_kline_data(symbol, interval, start_utc, end_utc)
    if df_raw is None or df_raw.empty:
        print(f"[fetch_recent_kline_woo] No data for {symbol} from {start_utc} to {end_utc}")
        return pd.DataFrame()

    # 將 end_timestamp 轉成UTC, 以判斷 bar 是否已完成
    df_raw['end_timestamp'] = pd.to_datetime(df_raw['end_timestamp'], unit='ms', utc=True)

    # 排除尚未收盤( end_timestamp > end_utc ) 的 bar
    # 例如現在是 01:13，但 bar 01:10~01:15 end_timestamp=01:15 => 這筆 bar 還沒走完 => 排除
    df_raw = df_raw[df_raw['end_timestamp'] <= end_utc].copy()
    df_raw.sort_values('start_timestamp', inplace=True)

    if df_raw.empty:
        print(f"[fetch_recent_kline_woo] All bars are incomplete or no data in range => returning empty df.")
        return pd.DataFrame()

    # 轉成 CTA columns
    df_raw['start_timestamp'] = pd.to_datetime(df_raw['start_timestamp'], unit='ms', utc=True)
    df_raw.reset_index(drop=True, inplace=True)

    # 只取需要欄位: date => start_timestamp, open, high, low, close, volume
    # date轉成本地(UTC+8)顯示
    df_out = pd.DataFrame({
        'date':   df_raw['start_timestamp'],
        'open':   df_raw['open'].astype(float),
        'high':   df_raw['high'].astype(float),
        'low':    df_raw['low'].astype(float),
        'close':  df_raw['close'].astype(float),
        'volume': df_raw['volume'].astype(float),
    })
    df_out['date'] = df_out['date'].dt.tz_convert(local_tz)

    # 若最終還是拿不到 enough bars(>14?), 就只好回傳可用資料
    return df_out
