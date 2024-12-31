# run.py

import asyncio
import logging
from pathlib import Path

from manager.config_manager import ConfigManager
from strategy_executor import StrategyExecutor
from test_order_executor import OrderExecutor
from strategy_logics.cta_strategy import CTAStrategy
from data_publisher_new import WooXStagingAPI

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("TradingSystem")

    try:
        # 1) 初始化配置
        config_manager = ConfigManager()
        main_config = config_manager.get_main_config()
        if not main_config:
            raise ValueError("Failed to load main configuration")

        exchange_config = main_config.get('exchange', {})
        redis_config    = main_config.get('redis', {})
        
        cta_config = config_manager.get_strategy_config('cta_strategy')
        if not cta_config:
            raise ValueError("No cta_strategy config")

        symbol = cta_config['subscription_params']['symbol']
        market_config  = cta_config['subscription_params']['market_config']
        private_config = cta_config['subscription_params']['private_config']
        timeframe = cta_config['trading_params']['timeframe']
        hedge_mode= cta_config['trading_params'].get('hedge_mode', False)

        # Channels
        cta_signal_channel = cta_config['channels']['signal_channel']            # e.g. "cta_signals"
        cta_exec_channel   = cta_config['channels']['execution_report_channel']  # e.g. "cta_executionreports"
        
        # 2) data_publisher => 負責「市場/私人資料 => Redis」
        data_publisher = WooXStagingAPI(
            app_id=exchange_config['app_id'],
            api_key=exchange_config['api_key'],
            api_secret=exchange_config['api_secret'],
            redis_host=redis_config.get('host', 'localhost'),
            redis_port=redis_config.get('port', 6379)
        )

        # 3) Strategy Executor => 把 [MD] & [PD] 分發給 CTA Strategy
        strategy_executor = StrategyExecutor(
            redis_url=f"redis://{redis_config['host']}:{redis_config['port']}"
        )
        await strategy_executor.add_strategy(
            strategy_class=CTAStrategy,
            config=cta_config
        )

        # 4) OrderExecutor => 訂閱 cta_signals, publish cta_executionreports, 
        #    並訂閱 [PD]executionreport => 轉給 cta_executionreports
        cta_order_executor = OrderExecutor(
            api_key=exchange_config['api_key'],
            api_secret=exchange_config['api_secret'],
            redis_url=f"redis://{redis_config['host']}:{redis_config['port']}",
            hedge_mode=hedge_mode
        )

        logger.info("Starting CTA system...")

        # 要訂閱的 Market channels
        market_channels = [
            f"{symbol}-kline_{timeframe}", 
            f"{symbol}-processed-kline_{timeframe}"
        ]
        private_channels= []
        if private_config.get('executionreport'):
            private_channels.append("executionreport")
        if private_config.get('position'):
            private_channels.append("position")
        # 你可依需求再加更多頻道

        # 同時啟動以下三個功能：
        tasks = [
            # a) 啟動 data_publisher => 訂閱 WooX, 再 publish 到 Redis
            asyncio.create_task(
                data_publisher.start(
                    symbol=symbol,
                    market_config=market_config,
                    private_config=private_config,
                    interval=timeframe
                )
            ),
            # b) Strategy Executor => 連接 Redis => 訂閱 [MD], [PD]，再 dispatch 給 CTA Strategy
            asyncio.create_task(
                strategy_executor.start(
                    market_channels=market_channels,
                    private_channels=private_channels
                )
            ),
            # c) Order Executor => 訂閱 CTA signals => 送單 => 擷取回報 => 發布到 cta_exec_channel
            asyncio.create_task(
                cta_order_executor.start(
                    signal_channel=cta_signal_channel,
                    execution_report_channel=cta_exec_channel,
                    private_report_channel="[PD]executionreport"  # 與 data_publisher_new.py 對應
                )
            )
        ]
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"Error in main => {e}")
    finally:
        logger.info("System shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        print(f"Program error => {str(e)}")