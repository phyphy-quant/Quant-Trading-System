import json
import logging
import asyncio
from typing import Dict, List, Optional
from redis import asyncio as aioredis
from strategy_logics.cta_strategy import CTAStrategy
from manager.config_manager import ConfigManager
from manager.risk_manager import RiskManager

class StrategyExecutor:
    """Strategy execution manager that handles multiple trading strategies"""
    
    def __init__(self, redis_url: str, config: Optional[Dict] = None):
        self.redis_url = redis_url
        self.redis_client = None
        self.strategies = {}
        self.config = config or {}
        
        # Initialize managers
        self.config_manager = ConfigManager()
        self.risk_manager = RiskManager(self.config.get('risk_management', {}))
        
        # Setup logging
        self.logger = logging.getLogger("StrategyExecutor")
        # 避免重複新增 handler
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

        # Track connection status
        self.is_running = False

    async def connect_redis(self) -> None:
        """Connect to Redis server"""
        try:
            self.redis_client = await aioredis.from_url(self.redis_url)
            self.logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def add_strategy(self, strategy_class: type, config: Dict = None) -> None:
        """
        Add a new strategy to the executor
        Args:
            strategy_class: Strategy class to instantiate
            config: Strategy configuration
        """
        try:
            strategy_name = strategy_class.__name__
            if strategy_name not in self.strategies:
                strategy_config = config or self.config_manager.get_strategy_config(strategy_name)
                
                # 從 strategy_config 讀取 channels
                channel_cfg = strategy_config.get('channels', {})
                assigned_signal_channel = channel_cfg.get('signal_channel', 'trading_signals')

                strategy_instance = strategy_class(
                    signal_channel=assigned_signal_channel,
                    config=strategy_config
                )
                self.strategies[strategy_name] = strategy_instance
                self.logger.info(f"Added strategy: {strategy_name}, signal_channel={assigned_signal_channel}")
            else:
                self.logger.warning(f"Strategy {strategy_name} already exists")
        except Exception as e:
            self.logger.error(f"Error adding strategy: {e}")
            raise

    def remove_strategy(self, strategy_name: str) -> None:
        """Remove a strategy from the executor"""
        if strategy_name in self.strategies:
            strategy = self.strategies.pop(strategy_name)
            strategy.stop()
            self.logger.info(f"Removed strategy: {strategy_name}")

    async def subscribe_to_channels(self, market_channels: List[str],
                                  private_channels: List[str]) -> tuple:
        """Subscribe to market and private data channels"""
        try:
            market_pubsub = self.redis_client.pubsub()
            private_pubsub = self.redis_client.pubsub()
            
            # Add channel prefixes
            prefixed_market = [f"[MD]{ch}" for ch in market_channels]
            prefixed_private = [f"[PD]{ch}" for ch in private_channels]
            
            # Subscribe to channels
            if prefixed_market:
                await market_pubsub.subscribe(*prefixed_market)
                self.logger.info(f"Subscribed to market channels: {prefixed_market}")
                
            if prefixed_private:
                await private_pubsub.subscribe(*prefixed_private)
                self.logger.info(f"Subscribed to private channels: {prefixed_private}")
                
            return market_pubsub, private_pubsub
            
        except Exception as e:
            self.logger.error(f"Error subscribing to channels: {e}")
            raise

    async def dispatch_to_strategies(self, channel: str, data: dict) -> None:
        """Dispatch data to all registered strategies"""
        try:
            if not self.is_running:
                return

            tasks = []
            for strategy in self.strategies.values():
                # 直接分派給策略，不檢查 can_trade
                task = asyncio.create_task(
                    strategy.execute(channel, data, self.redis_client)
                )
                tasks.append(task)
            
            if tasks:
                await asyncio.gather(*tasks)
                    
        except Exception as e:
            self.logger.error(f"Error dispatching to strategies: {e}")

    async def process_market_data(self, pubsub: aioredis.client.PubSub) -> None:
        """Process market data stream"""
        try:
            async for message in pubsub.listen():
                if not self.is_running:
                    break
                    
                if message["type"] == "message":
                    channel = message["channel"].decode("utf-8")
                    data = json.loads(message["data"])
                    await self.dispatch_to_strategies(channel, data)
                    
        except asyncio.CancelledError:
            self.logger.info("Market data processing cancelled")
        except Exception as e:
            self.logger.error(f"Error processing market data: {e}")

    async def process_private_data(self, pubsub: aioredis.client.PubSub) -> None:
        """Process private data stream"""
        try:
            async for message in pubsub.listen():
                if not self.is_running:
                    break
                    
                if message["type"] == "message":
                    channel = message["channel"].decode("utf-8")
                    data = json.loads(message["data"])
                    await self.dispatch_to_strategies(channel, data)
                    
        except asyncio.CancelledError:
            self.logger.info("Private data processing cancelled")
        except Exception as e:
            self.logger.error(f"Error processing private data: {e}")

    async def start(self, market_channels: List[str],
                   private_channels: List[str]) -> None:
        """Start the strategy executor"""
        try:
            if not self.redis_client:
                await self.connect_redis()
            
            self.is_running = True
            
            # Subscribe to channels
            market_pubsub, private_pubsub = await self.subscribe_to_channels(
                market_channels, private_channels
            )
            
            # Start all strategies
            for strategy in self.strategies.values():
                strategy.start()
            
            # Create processing tasks
            tasks = []
            if market_channels:
                tasks.append(self.process_market_data(market_pubsub))
            if private_channels:
                tasks.append(self.process_private_data(private_pubsub))
            
            # Run all tasks
            await asyncio.gather(*tasks)
            
        except Exception as e:
            self.logger.error(f"Error starting strategy executor: {e}")
            raise
        finally:
            await self.cleanup()

    async def cleanup(self) -> None:
        """Cleanup resources"""
        self.is_running = False
        
        # Stop all strategies
        for strategy in self.strategies.values():
            strategy.stop()
        
        # Close Redis connection
        if self.redis_client:
            await self.redis_client.close()
            
        self.logger.info("Strategy executor cleaned up")

async def main():
    """Main function for testing"""
    try:
        config = {
            'redis': {'url': 'redis://localhost:6379'},
            'signal_channel': 'trading_signals'
        }
        
        executor = StrategyExecutor(redis_url=config['redis']['url'], config=config)
        
        symbol = 'PERP_ETH_USDT'
        
        # 添加策略實例
        await executor.add_strategy(
            strategy_class=CTAStrategy,
            config={
                'max_records': 500,
                'trading_params': {
                    'symbol': symbol,
                    'timeframe': '1m'
                },
                'risk_params': {
                    'max_position_size': 1.0,
                    'stop_loss_pct': 0.25
                }
            }
        )
        
        # 修改訂閱的頻道，使用完整的頻道名稱
        market_channels = [
            f'{symbol}-kline_5m',
            f'{symbol}-processed-kline_5m'  # 添加處理過的K線頻道
        ]
        private_channels = ['executionreport', 'position']
        
        await executor.start(market_channels, private_channels)
        
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    except Exception as e:
        logging.error(f"Error in main: {e}")

# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     asyncio.run(main())