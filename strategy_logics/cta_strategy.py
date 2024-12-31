# ==== strategy_logics/cta_strategy.py ====

import json
import asyncio
import logging
import time
import pytz
import pandas as pd
import numpy as np
import io
import os
from datetime import datetime
from typing import Optional, Dict
from redis import asyncio as aioredis

from strategy_logics.Woox_loader_for_cta import WooXPublicAPI, fetch_recent_kline_woo
from strategy_logics.strategy_init import (
    Strategy,
    SignalData,
    OrderType,
    PositionSide,
    OrderAction
)

class CTAStrategy(Strategy):
    """
    使用 DataFrame 的上一筆 position 判斷現在是否有倉位:
      - position=0 => 無倉
      - position=1 => 多單
      - position=-1 => 空單

    當 direction 出現翻轉 (up->down 或 down->up)，
    若上筆position=1 => 先 CLOSE LONG, 再 OPEN SHORT
    若上筆position=-1 => 先 CLOSE SHORT, 再 OPEN LONG
    """

    def __init__(self, signal_channel: str, config: Dict = None):
        super().__init__(signal_channel, config)

        # === Logger ===
        self.logger = logging.getLogger("CTAStrategy")
        self.logger.setLevel(logging.INFO)
        os.makedirs("logs", exist_ok=True)

        # 將交易動作詳細記錄到檔案 cta_strategy_trades.log
        fh = logging.FileHandler("logs/cta_strategy_trades.log")
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - CTAStrategy - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        # === 讀取 config ===
        self.config = config or {}
        trading_params = self.config.get('trading_params', {})

        self.max_records          = trading_params.get('max_records', 500)
        self.trading_symbol       = trading_params.get('symbol', "PERP_BTC_USDT")
        self.position_size        = trading_params.get('position_size', 0.01)
        self.hedge_mode           = trading_params.get('hedge_mode', False)
        self.atr_period           = trading_params.get('atr_period', 14)
        self.threshold_multiplier = trading_params.get('threshold_multiplier', 1.0)
        self.take_profit_atr      = trading_params.get('take_profit_pct', 9.0)
        self.stop_loss_atr        = trading_params.get('stop_loss_pct', 3.0)

        # DataFrame (存行情 + 策略訊號 + 持倉)
        self.df = pd.DataFrame()

        # 因為我們現在只看 df 裡上一筆 position，不再需要 self.current_position
        # self.current_position = None

        self.last_processed_bar_time: Optional[int] = None
        self.redis_client = None
        self.local_tz = pytz.timezone("Asia/Taipei")

        self.logger.info("CTA Strategy initialization start")

        # === Warmup: 讀取 K線 + 初始化 df ===
        try:
            api_key = "sdFgbf5mnyDD/wahfC58Kw"
            woo_api = WooXPublicAPI(api_key=api_key, base_url="https://api-pub.woo.org")
            interval = trading_params.get('timeframe', '1m')

            warmup_df = fetch_recent_kline_woo(
                api=woo_api,
                symbol=self.trading_symbol,
                interval=interval,
                warmup_bars=self.atr_period
            )
            if not warmup_df.empty:
                self.df = warmup_df.copy()
                # 計算 ATR
                self.df['atr'] = self.calculate_atr(self.df, self.atr_period)
                # 取得 direction
                self.df = self.get_direction(self.df)
                # 新增 'signal' 欄位
                self.df['signal'] = 0
                # 新增 'position' 欄位 => 0 (起始無倉)
                self.df['position'] = 0

                self.logger.info(
                    f"Warmup loaded {len(self.df)} bars for {self.trading_symbol}, df shape={self.df.shape}"
                )
            else:
                self.logger.warning("Warmup DF is empty => no historical bars fetched.")

        except Exception as e:
            self.logger.error(f"Error fetching warmup bars: {e}")

        self.logger.info("CTA Strategy init done.")

    def start(self):
        self.logger.info("Strategy started: CTAStrategy")

    def stop(self):
        self.logger.info("Strategy stopped: CTAStrategy")

    # ------------------------------------------------------------------
    #  TR / ATR / direction
    # ------------------------------------------------------------------
    def calculate_tr(self, df: pd.DataFrame) -> pd.Series:
        tr_list = []
        for i in range(len(df)):
            if i == 0:
                tr_val = df['high'].iloc[i] - df['low'].iloc[i]
            else:
                prev_close = df['close'].iloc[i-1]
                hi = df['high'].iloc[i]
                lo = df['low'].iloc[i]
                tr_val = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
            tr_list.append(tr_val)
        return pd.Series(tr_list, index=df.index, dtype=float)

    def calculate_atr(self, df: pd.DataFrame, period: int=14) -> pd.Series:
        if df.empty:
            return pd.Series([0]*len(df), index=df.index)
        tr = self.calculate_tr(df)
        return tr.rolling(period).mean()

    def get_direction(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            df['direction'] = []
            return df

        up_trend = True
        last_high = df['high'].iloc[0]
        last_low  = df['low'].iloc[0]
        directions = []
        for i in range(len(df)):
            threshold = self.threshold_multiplier * df.loc[i, 'atr']
            if up_trend:
                if df.loc[i, 'high'] > last_high:
                    last_high = df.loc[i, 'high']
                elif df.loc[i, 'close'] < (last_high - threshold):
                    up_trend = False
                    last_low = df.loc[i, 'low']
            else:
                if df.loc[i, 'low'] < last_low:
                    last_low = df.loc[i, 'low']
                elif df.loc[i, 'close'] > (last_low + threshold):
                    up_trend = True
                    last_high = df.loc[i, 'high']
            directions.append('up' if up_trend else 'down')
        df['direction'] = directions
        return df

    # ------------------------------------------------------------------
    #  不再用私有頻道 position 來更新，但保留函式避免程式報錯
    # ------------------------------------------------------------------
    def _update_position_data(self, data: dict):
        pass

    # ------------------------------------------------------------------
    #  open_long / open_short / close_position => 全市價單(taker)
    # ------------------------------------------------------------------
    async def open_long(self):
        ts_ms = int(time.time() * 1000)
        client_id = f"OPEN_LONG_{ts_ms}"

        if self.hedge_mode:
            sig = SignalData(
                timestamp=ts_ms,
                action=OrderAction.OPEN,
                position_side=PositionSide.LONG,
                order_type=OrderType.MARKET,
                symbol=self.trading_symbol,
                quantity=self.position_size
            )
            sig_dict = sig.to_dict()
            sig_dict["client_order_id"] = client_id
            self.logger.info(f"[CTA] => Publish OPEN LONG => {sig_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(sig_dict))
        else:
            signal_dict = {
                "timestamp": ts_ms,
                "action": "OPEN",
                "side": "BUY",
                "order_type": "MARKET",
                "symbol": self.trading_symbol,
                "quantity": self.position_size,
                "client_order_id": client_id
            }
            self.logger.info(f"[CTA] => Publish single-side OPEN LONG => {signal_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(signal_dict))

        self.logger.info(f"[CTA] => Called open_long => client_id={client_id}")

    async def open_short(self):
        ts_ms = int(time.time() * 1000)
        client_id = f"OPEN_SHORT_{ts_ms}"

        if self.hedge_mode:
            sig = SignalData(
                timestamp=ts_ms,
                action=OrderAction.OPEN,
                position_side=PositionSide.SHORT,
                order_type=OrderType.MARKET,
                symbol=self.trading_symbol,
                quantity=self.position_size
            )
            sig_dict = sig.to_dict()
            sig_dict["client_order_id"] = client_id
            self.logger.info(f"[CTA] => Publish OPEN SHORT => {sig_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(sig_dict))
        else:
            signal_dict = {
                "timestamp": ts_ms,
                "action": "OPEN",
                "side": "SELL",
                "order_type": "MARKET",
                "symbol": self.trading_symbol,
                "quantity": self.position_size,
                "client_order_id": client_id
            }
            self.logger.info(f"[CTA] => Publish single-side OPEN SHORT => {signal_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(signal_dict))

        self.logger.info(f"[CTA] => Called open_short => client_id={client_id}")

    async def close_position(self, pos_side: PositionSide, reason: str=""):
        """
        關閉既有部位:
          - 如果 pos_side=LONG => side=SELL
          - 如果 pos_side=SHORT => side=BUY
        """
        ts_ms = int(time.time() * 1000)
        client_id = f"CLOSE_{pos_side.value}_{ts_ms}"

        if self.hedge_mode:
            close_signal = SignalData(
                timestamp=ts_ms,
                action=OrderAction.CLOSE,
                position_side=pos_side,
                order_type=OrderType.MARKET,
                symbol=self.trading_symbol,
                quantity=self.position_size,
                reduce_only=True
            )
            sig_dict = close_signal.to_dict()
            sig_dict["client_order_id"] = client_id
            self.logger.info(f"[CTA] => Publish CLOSE {pos_side.value} => {sig_dict}, reason={reason}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(sig_dict))
        else:
            # single-side模式: CLOSE LONG => side=SELL, CLOSE SHORT => side=BUY
            side = "SELL" if pos_side == PositionSide.LONG else "BUY"
            close_dict = {
                "timestamp": ts_ms,
                "action": "CLOSE",
                "side": side,
                "order_type": "MARKET",
                "symbol": self.trading_symbol,
                "quantity": self.position_size,
                "reduce_only": True,
                "client_order_id": client_id
            }
            self.logger.info(f"[CTA] => Publish single-side CLOSE {pos_side.value} => {close_dict}, reason={reason}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(close_dict))

        self.logger.info(f"[CTA] => Called close_position => {pos_side.value}, reason={reason}, client_id={client_id}")

    # ------------------------------------------------------------------
    #  execute() => 接收 [MD] 行情 => 產生訊號
    # ------------------------------------------------------------------
    async def execute(self, channel: str, data: dict, redis_client: aioredis.Redis) -> None:
        try:
            self.redis_client = redis_client
            if isinstance(data, str):
                data = json.loads(data)

            # (a) 私有頻道 position => 不再使用
            if "position" in channel.lower():
                self._update_position_data(data)
                return

            # (b) 行情 => signal
            if "processed-kline" in channel:
                self.logger.info(f"execute => channel={channel}, new kline => generating signals")

            cdata = data.get('data', {}).get('kline_data', {})
            if not cdata:
                return

            new_start = cdata.get('startTime')
            if not new_start:
                return
            if new_start == self.last_processed_bar_time:
                self.logger.debug("This bar was already processed => skip.")
                return
            self.last_processed_bar_time = new_start

            dt_utc = pd.to_datetime(new_start, unit='ms', utc=True)
            dt_local = dt_utc.tz_convert(self.local_tz)
            new_row = {
                "date":   dt_local,
                "open":   float(cdata.get('open', 0)),
                "high":   float(cdata.get('high', 0)),
                "low":    float(cdata.get('low', 0)),
                "close":  float(cdata.get('close', 0)),
                "volume": float(cdata.get('volume', 0))
            }
            self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
            if len(self.df) > self.max_records:
                self.df = self.df.iloc[-self.max_records:].reset_index(drop=True)

            # Recompute ATR / direction
            self.df['atr'] = self.calculate_atr(self.df, self.atr_period)
            self.df = self.get_direction(self.df)

            # 若尚未有 'signal' 欄位 => 新增
            if 'signal' not in self.df.columns:
                self.df['signal'] = 0
            else:
                self.df['signal'] = self.df['signal'].fillna(0)

            # 若尚未有 'position' 欄位 => 新增
            if 'position' not in self.df.columns:
                self.df['position'] = 0
            else:
                self.df['position'] = self.df['position'].fillna(0)

            # 產生 signal + 更新 df
            self.df = await self.generate_signals_and_publish(self.df)

            # 存到 Redis => 方便查閱
            buf = io.BytesIO()
            self.df.to_pickle(buf)
            buf.seek(0)
            await self.redis_client.set('strategy_df', buf.read())

        except Exception as e:
            self.logger.error(f"Error in strategy execution: {e}")
            raise

    # ------------------------------------------------------------------
    #  generate_signals_and_publish()
    # ------------------------------------------------------------------
    async def generate_signals_and_publish(self, df: pd.DataFrame) -> pd.DataFrame:
        """根據前一筆 position + 當前方向，決定是否翻倉 (先關再開) 或直接開。"""
        if df.empty or len(df) < 2:
            return df

        i = len(df) - 1
        prev_dir = df.loc[i-1, 'direction']  # e.g. 'up' or 'down'
        curr_dir = df.loc[i, 'direction']
        old_signal = df.loc[i-1, 'signal'] if 'signal' in df.columns else 0
        old_pos = df.loc[i-1, 'position'] if 'position' in df.columns else 0

        new_signal = 0

        # === 翻倉判斷 ===
        if old_pos == 0:
            # 無倉 -> 開倉
            if prev_dir == 'up' and curr_dir == 'down':
                # flip to SHORT
                new_signal = -2
                self.logger.info("[CTA] => old_pos=0 => up->down => OPEN SHORT")
                await self.open_short()
            elif prev_dir == 'down' and curr_dir == 'up':
                new_signal = 2
                self.logger.info("[CTA] => old_pos=0 => down->up => OPEN LONG")
                await self.open_long()

        elif old_pos == 1:
            # 有多單
            if prev_dir == 'up' and curr_dir == 'down':
                # 先關多，再開空
                new_signal = -2
                self.logger.info("[CTA] => old_pos=1 => direction up->down => FLIP to SHORT")
                await self.close_position(PositionSide.LONG, reason="Flip from LONG to SHORT")
                await self.open_short()
            else:
                # 若要檢查停利停損 => 加入 ATR 計算
                pass

        elif old_pos == -1:
            # 有空單
            if prev_dir == 'down' and curr_dir == 'up':
                new_signal = 2
                self.logger.info("[CTA] => old_pos=-1 => direction down->up => FLIP to LONG")
                await self.close_position(PositionSide.SHORT, reason="Flip from SHORT to LONG")
                await self.open_long()
            else:
                # 同樣可加停利停損
                pass

        # === Step2: 記錄 signal 到當前 row ===
        old_sig_thisrow = df.loc[i, 'signal']
        if new_signal == 0:
            if old_sig_thisrow == 0:
                # 沒新訊號 => 沿用上一筆
                df.loc[i, 'signal'] = old_signal
        else:
            df.loc[i, 'signal'] = new_signal

        # === Step3: 更新 position 欄位 ===
        self._update_position_column(df, i, new_signal)

        return df

    def _update_position_column(self, df: pd.DataFrame, i: int, new_signal: int):
        """
        依 new_signal 決定本 row 的 position:
          new_signal= 2 => position=1
          new_signal=-2 => position=-1
          (若有想做 9/-3 => position=0，也可自行加)
          否則 => 沿用上一筆
        """
        if 'position' not in df.columns:
            df['position'] = 0
        if i == 0:
            df.loc[i, 'position'] = 0
            return

        old_pos = df.loc[i-1, 'position']
        if new_signal == 2:
            df.loc[i, 'position'] = 1
        elif new_signal == -2:
            df.loc[i, 'position'] = -1
        else:
            # 沒新訊號 => 沿用上一筆
            df.loc[i, 'position'] = old_pos