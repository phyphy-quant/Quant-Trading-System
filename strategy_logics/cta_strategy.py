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
    真實系統版本：
      1) 發出限價單時，帶上 client_order_id
      2) 等待 cta_executionreports 回報訂單填單(FILLED)或到期(15秒)無法全部成交 => 取消限價單 => 改下市價單
      3) 收到所有回報時，都寫進 cta_strategy_trades.log，也可在這裡更新策略內部狀態
    """

    def __init__(self, signal_channel: str, config: Dict = None):
        super().__init__(signal_channel, config)

        # === Logger ===
        self.logger = logging.getLogger("CTAStrategy")
        self.logger.setLevel(logging.INFO)
        os.makedirs("logs", exist_ok=True)
        fh = logging.FileHandler("logs/cta_strategy_trades.log")
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - CTAStrategy - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        # === 讀取策略配置 ===
        trading_params = self.config.get('trading_params', {})
        self.max_records= trading_params.get('max_records', 500)
        self.trading_symbol = trading_params.get('symbol', "PERP_ETH_USDT")
        self.position_size  = trading_params.get('position_size', 0.01)
        self.hedge_mode     = trading_params.get('hedge_mode', False)
        self.atr_period     = 14
        self.threshold_multiplier= 3.0
        self.take_profit_atr     = 9.0
        self.stop_loss_atr       = 3.0

        # data frame for Kline
        self.df = pd.DataFrame()

        # === 策略狀態紀錄 ===
        self.current_position: Optional[PositionSide] = None
        self.entry_price: Optional[float] = None
        self.last_processed_bar_time: Optional[int] = None
        self.redis_client = None

        # === 為了追蹤訂單回報 ===
        # order_status 紀錄訂單目前的狀態; order_fill_futures 用來等待該訂單的最終狀態。
        self.order_status: Dict[str, str] = {}         # client_order_id -> OrderStatus (字串)
        self.order_fill_futures: Dict[str, asyncio.Future] = {}

        # 頻道名稱(含 execution_report_channel)
        self.execution_report_channel = self.config.get('channels', {}).get('execution_report_channel', 'cta_executionreports')

        # timezone
        self.local_tz = pytz.timezone("Asia/Taipei")

        self.logger.info("CTA Strategy initialization start")

        # === Warmup: 讀取前 N 根 K線 ===
        try:
            api_key = "sdFgbf5mnyDD/wahfC58Kw"
            woo_api = WooXPublicAPI(api_key=api_key, base_url="https://api-pub.woo.org")
            interval = trading_params.get('timeframe', '5m')

            warmup_df = fetch_recent_kline_woo(
                api=woo_api,
                symbol=self.trading_symbol,
                interval=interval,
                warmup_bars=self.atr_period
            )
            if not warmup_df.empty:
                self.df = warmup_df.copy()
                self.df['atr'] = self.calculate_atr(self.df, self.atr_period)
                self.df = self.get_direction(self.df)
                self.df['signal'] = 0
                self.logger.info(
                    f"Warmup loaded {len(self.df)} bars for {self.trading_symbol}, df shape={self.df.shape}"
                )
            else:
                self.logger.warning("Warmup DF is empty => no historical bars fetched.")
        except Exception as e:
            self.logger.error(f"Error fetching warmup bars: {e}")

        self.logger.info("CTA Strategy init done, old data not loaded from Redis.")

    def start(self):
        self.logger.info("Strategy started: CTAStrategy")

    def stop(self):
        self.logger.info("Strategy stopped: CTAStrategy")

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

    #----------------------------------------------------------------------
    #  監聽並更新訂單狀態；若填單 / 取消 / 拒絕，就將 futures 設為完成
    #----------------------------------------------------------------------
    def _update_order_status(self, report_data: dict):
        """
        在 self.execute() 收到 cta_executionreports 時呼叫，更新 self.order_status 與 future 狀態
        report_data 結構(範例):
        {
          "clientOrderId": "xxx",
          "status": "FILLED" / "PARTIALLY_FILLED" / "NEW" / "CANCELLED" / "REJECTED",
          "executedQuantity": ...,
          "price": ...,
          ...
        }
        """
        c_id = str(report_data.get('clientOrderId', ''))
        if not c_id:
            return

        new_status = report_data.get('status', '')
        self.logger.info(f"[CTA] Received order report => clientOrderId={c_id}, status={new_status}")
        self.order_status[c_id] = new_status

        # 將「成功下單」或部分成交資訊寫入 log
        # (若需要紀錄 fills，可進一步看 report_data.get('fill_info') 等)
        if new_status == "FILLED":
            self.logger.info(f"[CTA] Order {c_id} FILLED => qty={report_data.get('executedQuantity')}, price={report_data.get('price')}")
        elif new_status == "PARTIALLY_FILLED":
            self.logger.info(f"[CTA] Order {c_id} PARTIALLY_FILLED => executedQty={report_data.get('executedQuantity')}")
        elif new_status in ["CANCELLED", "REJECTED"]:
            self.logger.info(f"[CTA] Order {c_id} {new_status} => reason=?")

        # 若已最終結束(包含 FILLED / CANCELLED / REJECTED)，可以把 future set_result
        # PARTIALLY_FILLED 不結束，繼續等
        if new_status in ["FILLED", "CANCELLED", "REJECTED"]:
            fut = self.order_fill_futures.get(c_id)
            if fut and not fut.done():
                fut.set_result(new_status)

    async def _wait_for_order_fill(self, client_order_id: str, timeout: int = 15) -> str:
        """
        等待某張訂單最終結果 (FILLED/CANCELLED/REJECTED)，或超時
        回傳最終狀態 "FILLED"/"CANCELLED"/"REJECTED"/"TIMEOUT"
        """
        if client_order_id not in self.order_fill_futures:
            # 建一個 future
            self.order_fill_futures[client_order_id] = asyncio.get_event_loop().create_future()

        fut = self.order_fill_futures[client_order_id]
        try:
            status = await asyncio.wait_for(fut, timeout=timeout)
            return status
        except asyncio.TimeoutError:
            self.logger.warning(f"[CTA] Wait for order {client_order_id} fill TIMEOUT after {timeout} seconds.")
            return "TIMEOUT"

    def _generate_client_order_id(self, prefix: str = "CTA") -> str:
        """產生獨特的 client_order_id"""
        return f"{prefix}_{int(time.time() * 1000)}"

    #----------------------------------------------------------------------
    #  核心：先用限價單，若 15秒 內未 completely fill => 取消再下市價
    #----------------------------------------------------------------------
    async def open_long(self, current_close: float, dt_local: datetime):
        ts_ms = int(dt_local.timestamp() * 1000)
        # 取上一根K線的收盤價做限價
        if len(self.df) >= 2:
            limit_price = float(self.df.iloc[-2]['close'])
        else:
            limit_price = current_close

        # (1) 先送限價單
        limit_client_id = self._generate_client_order_id(prefix="LONG_LIMIT")
        if self.hedge_mode:
            limit_order_signal = SignalData(
                timestamp=ts_ms,
                action=OrderAction.OPEN,
                position_side=PositionSide.LONG,
                order_type=OrderType.LIMIT,
                symbol=self.trading_symbol,
                quantity=self.position_size,
                price=limit_price,
                margin_mode='CROSS'
            )
            # 強制覆蓋clientOrderId (下方執行端會用 client_order_id)
            # 如果你在 test_order_executor.py 裡用 signal.get('client_order_id') 取值，可額外欄位帶上
            signal_dict = limit_order_signal.to_dict()
            signal_dict["client_order_id"] = limit_client_id

            self.logger.info(f"[CTA] Publish OPEN LONG LIMIT => {signal_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(signal_dict))
        else:
            limit_order_dict = {
                "timestamp": ts_ms,
                "action": "OPEN",
                "side": "BUY",
                "order_type": "LIMIT",
                "symbol": self.trading_symbol,
                "quantity": self.position_size,
                "price": limit_price,
                "client_order_id": limit_client_id
            }
            self.logger.info(f"[CTA] Publish single-side OPEN LONG LIMIT => {limit_order_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(limit_order_dict))

        self.logger.info(f"[CTA] => Limit LONG order sent, limit_price={limit_price}. Will wait up to 15s for fill...")

        # (2) 等待限價單結果
        final_status = await self._wait_for_order_fill(limit_client_id, timeout=15)

        if final_status == "FILLED":
            # 已全部成交
            self.logger.info(f"[CTA] => Limit LONG {limit_client_id} fully filled. No need for market order.")
            self.current_position = PositionSide.LONG
            self.entry_price = current_close
            return

        # 如果是 CANCELLED / REJECTED，就表示該單不成立；也要用市價單補單嗎 => 視需求
        # 這裡統一用市價彌補 (也可能是TIMEOUT)
        if final_status in ["CANCELLED", "REJECTED", "TIMEOUT"]:
            self.logger.info(f"[CTA] => Limit LONG {limit_client_id} did not fill -> proceed to Market order.")
            # 先嘗試 cancel (若還沒真被取消)
            cancel_dict = {
                "timestamp": int(time.time() * 1000),
                "action": "CANCEL_ORDER",
                "symbol": self.trading_symbol,
                "client_order_id": limit_client_id
            }
            self.logger.info(f"[CTA] => Cancel LIMIT LONG signal: {cancel_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(cancel_dict))

        # (3) 改用市價單
        ts_ms = int(time.time() * 1000)
        market_client_id = self._generate_client_order_id(prefix="LONG_MKT")
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
            sig_dict["client_order_id"] = market_client_id
            self.logger.info(f"[CTA] => Publish OPEN LONG MARKET => {sig_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(sig_dict))
        else:
            market_order_dict = {
                "timestamp": ts_ms,
                "action": "OPEN",
                "side": "BUY",
                "order_type": "MARKET",
                "symbol": self.trading_symbol,
                "quantity": self.position_size,
                "client_order_id": market_client_id
            }
            self.logger.info(f"[CTA] => Publish single-side OPEN LONG MARKET => {market_order_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(market_order_dict))

        # 再等市價單填單(理論上應該很快 FILLED)
        final_status_2 = await self._wait_for_order_fill(market_client_id, timeout=10)
        if final_status_2 == "FILLED":
            self.logger.info(f"[CTA] => Market LONG {market_client_id} filled successfully.")
            self.current_position = PositionSide.LONG
            self.entry_price = current_close
        else:
            self.logger.warning(f"[CTA] => Market LONG {market_client_id} not filled or cancelled => {final_status_2}. Check exchange or risk logic.")

    async def open_short(self, current_close: float, dt_local: datetime):
        ts_ms = int(dt_local.timestamp() * 1000)
        if len(self.df) >= 2:
            limit_price = float(self.df.iloc[-2]['close'])
        else:
            limit_price = current_close

        # (1) 送限價單
        limit_client_id = self._generate_client_order_id(prefix="SHORT_LIMIT")
        if self.hedge_mode:
            limit_order_signal = SignalData(
                timestamp=ts_ms,
                action=OrderAction.OPEN,
                position_side=PositionSide.SHORT,
                order_type=OrderType.LIMIT,
                symbol=self.trading_symbol,
                quantity=self.position_size,
                price=limit_price,
                margin_mode='CROSS'
            )
            sig_dict = limit_order_signal.to_dict()
            sig_dict["client_order_id"] = limit_client_id
            self.logger.info(f"[CTA] Publish OPEN SHORT LIMIT => {sig_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(sig_dict))
        else:
            limit_order_dict = {
                "timestamp": ts_ms,
                "action": "OPEN",
                "side": "SELL",
                "order_type": "LIMIT",
                "symbol": self.trading_symbol,
                "quantity": self.position_size,
                "price": limit_price,
                "client_order_id": limit_client_id
            }
            self.logger.info(f"[CTA] Publish single-side OPEN SHORT LIMIT => {limit_order_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(limit_order_dict))

        self.logger.info(f"[CTA] => Limit SHORT order sent, limit_price={limit_price}. Will wait up to 15s for fill...")

        # (2) 等待回報
        final_status = await self._wait_for_order_fill(limit_client_id, timeout=15)
        if final_status == "FILLED":
            self.logger.info(f"[CTA] => Limit SHORT {limit_client_id} fully filled. No need for market order.")
            self.current_position = PositionSide.SHORT
            self.entry_price = current_close
            return

        if final_status in ["CANCELLED", "REJECTED", "TIMEOUT"]:
            self.logger.info(f"[CTA] => Limit SHORT {limit_client_id} did not fill -> proceed to Market order.")
            cancel_dict = {
                "timestamp": int(time.time() * 1000),
                "action": "CANCEL_ORDER",
                "symbol": self.trading_symbol,
                "client_order_id": limit_client_id
            }
            self.logger.info(f"[CTA] => Cancel LIMIT SHORT signal => {cancel_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(cancel_dict))

        # (3) 下市價單
        ts_ms = int(time.time() * 1000)
        market_client_id = self._generate_client_order_id(prefix="SHORT_MKT")
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
            sig_dict["client_order_id"] = market_client_id
            self.logger.info(f"[CTA] => Publish OPEN SHORT MARKET => {sig_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(sig_dict))
        else:
            market_order_dict = {
                "timestamp": ts_ms,
                "action": "OPEN",
                "side": "SELL",
                "order_type": "MARKET",
                "symbol": self.trading_symbol,
                "quantity": self.position_size,
                "client_order_id": market_client_id
            }
            self.logger.info(f"[CTA] => Publish single-side OPEN SHORT MARKET => {market_order_dict}")
            if self.redis_client:
                await self.redis_client.publish(self.signal_channel, json.dumps(market_order_dict))

        final_status_2 = await self._wait_for_order_fill(market_client_id, timeout=10)
        if final_status_2 == "FILLED":
            self.logger.info(f"[CTA] => Market SHORT {market_client_id} filled successfully.")
            self.current_position = PositionSide.SHORT
            self.entry_price = current_close
        else:
            self.logger.warning(f"[CTA] => Market SHORT {market_client_id} not filled or cancelled => {final_status_2}.")

    async def close_position(self, pos_side: PositionSide, reason: str=""):
        """
        平倉邏輯(此處示範直接市價單)，你也可以做跟開倉一樣的限價->timeout->市價流程
        """
        if self.current_position == pos_side:
            ts_ms = int(time.time() * 1000)
            market_client_id = self._generate_client_order_id(prefix=f"CLOSE_{pos_side.value}")
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
                sig_dict["client_order_id"] = market_client_id
                self.logger.info(f"[CTA] => Publish CLOSE {pos_side.value} MARKET => {sig_dict}, reason={reason}")
                if self.redis_client:
                    await self.redis_client.publish(self.signal_channel, json.dumps(sig_dict))
            else:
                side = "SELL" if pos_side == PositionSide.LONG else "BUY"
                signal_dict = {
                    "timestamp": ts_ms,
                    "action": "CLOSE",
                    "side": side,
                    "order_type": "MARKET",
                    "symbol": self.trading_symbol,
                    "quantity": self.position_size,
                    "reduce_only": True,
                    "client_order_id": market_client_id
                }
                self.logger.info(f"[CTA] => Publish single-side CLOSE {pos_side.value} => {signal_dict}, reason={reason}")
                if self.redis_client:
                    await self.redis_client.publish(self.signal_channel, json.dumps(signal_dict))

            # 等市價單真正 FILLED
            final_status = await self._wait_for_order_fill(market_client_id, timeout=10)
            if final_status == "FILLED":
                self.logger.info(f"[CTA] => Position {pos_side.value} closed => {reason}")
            else:
                self.logger.warning(f"[CTA] => Close {pos_side.value} order not filled => {final_status}")

            self.current_position = None
            self.entry_price = None

    #----------------------------------------------------------------------
    #  收到任意 channel 資料都會到這裡: 分「行情資料」和「訂單回報」來處理
    #----------------------------------------------------------------------
    async def execute(self, channel: str, data: dict, redis_client: aioredis.Redis) -> None:
        """
        此函式同時接收:
          - [MD]xxx-processed-kline_xxx: 處理 kline => 產生新 bar => generate_signals_and_publish
          - [PD] 或 cta_executionreports (自定) => 更新訂單狀態 => 改動 order_fill_futures
        """
        try:
            self.redis_client = redis_client
            if isinstance(data, str):
                data = json.loads(data)

            # 1) 若是來自「訂單回報」頻道，更新訂單狀態
            #    這裡假設 channel.endswith(self.execution_report_channel) 或你自定
            if channel.endswith(self.execution_report_channel):
                # data 裡應該包含 clientOrderId, status, fills 等
                report = data
                self._update_order_status(report)
                return  # 回報處理完後就結束，不要往下走

            # 2) 若是市場數據 (kline)，才進行策略邏輯
            #    (可依你實際[MD] 前綴或 channel 格式來分)
            if "processed-kline" in channel:
                self.logger.info(f"execute => channel={channel}, new kline data incoming")
            else:
                self.logger.debug(f"execute => channel={channel}")

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

            self.df['atr'] = self.calculate_atr(self.df, self.atr_period)
            self.df = self.get_direction(self.df)
            if 'signal' not in self.df.columns:
                self.df['signal'] = 0
            else:
                self.df['signal'] = self.df['signal'].fillna(0)

            self.df = await self.generate_signals_and_publish(self.df)

            # 存到 Redis => 方便前端或其他程式讀取
            buf = io.BytesIO()
            self.df.to_pickle(buf)
            buf.seek(0)
            await self.redis_client.set('strategy_df', buf.read())

            # self.logger.info(
            #     f"Appended new bar => df.shape={self.df.shape}, last_row={self.df.iloc[-1].to_dict()}"
            # )

        except Exception as e:
            self.logger.error(f"Error in strategy execution: {e}")
            raise

    #----------------------------------------------------------------------
    #  generate_signals_and_publish: 策略核心邏輯 (判斷多空、平倉)
    #----------------------------------------------------------------------
    async def generate_signals_and_publish(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or len(df) < 2:
            return df

        i = len(df) - 1
        prev_dir = df.loc[i-1, 'direction']
        curr_dir = df.loc[i, 'direction']
        current_close = df.loc[i, 'close']
        current_atr   = df.loc[i, 'atr']
        old_signal    = df.loc[i, 'signal'] if 'signal' in df.columns else 0
        new_signal    = 0  # 預設

        # 無持倉 => 若出現多翻空 / 空翻多 => 開倉
        if self.current_position is None:
            if prev_dir == 'up' and curr_dir == 'down':
                new_signal = -2
                await self.open_short(current_close, df.loc[i, 'date'])
            elif prev_dir == 'down' and curr_dir == 'up':
                new_signal = 2
                await self.open_long(current_close, df.loc[i, 'date'])

        # 有持倉 => 停利 / 停損 / 翻倉
        else:
            if self.current_position == PositionSide.LONG:
                dist = current_close - (self.entry_price or 0)
                if dist >= self.take_profit_atr * current_atr:
                    new_signal = 9
                    await self.close_position(PositionSide.LONG, reason="TP by 9*ATR")
                elif dist <= -self.stop_loss_atr * current_atr:
                    new_signal = -3
                    await self.close_position(PositionSide.LONG, reason="SL by 3*ATR")
                elif prev_dir == 'up' and curr_dir == 'down':
                    new_signal = -2
                    await self.close_position(PositionSide.LONG, reason="direction up->down")
                    await self.open_short(current_close, df.loc[i, 'date'])

            elif self.current_position == PositionSide.SHORT:
                dist = (self.entry_price or 0) - current_close
                if dist >= self.take_profit_atr * current_atr:
                    new_signal = 9
                    await self.close_position(PositionSide.SHORT, reason="TP by 9*ATR")
                elif dist <= -self.stop_loss_atr * current_atr:
                    new_signal = -3
                    await self.close_position(PositionSide.SHORT, reason="SL by 3*ATR")
                elif prev_dir == 'down' and curr_dir == 'up':
                    new_signal = 2
                    await self.close_position(PositionSide.SHORT, reason="direction down->up")
                    await self.open_long(current_close, df.loc[i, 'date'])

        # 更新 signal 欄位
        if new_signal == 0:
            if old_signal != 0:
                df.loc[i, 'signal'] = old_signal
            else:
                df.loc[i, 'signal'] = 0
        else:
            df.loc[i, 'signal'] = new_signal

        return df