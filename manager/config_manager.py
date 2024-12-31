import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional

class ConfigManager:
    def __init__(self, config_path: str = "configs"):
        self.config_path = Path(config_path)
        self.configs: Dict[str, Any] = {}
        self.logger = logging.getLogger("ConfigManager")
        self.load_configs()

    def load_configs(self) -> None:
        """Load all configuration files"""
        try:
            # Load main config
            main_config_path = self.config_path / "main.yaml"
            if main_config_path.exists():
                with open(main_config_path, 'r') as f:
                    self.configs['main'] = yaml.safe_load(f)
                self.logger.info("Main configuration loaded successfully")
            else:
                self.logger.warning(f"Main config file not found at {main_config_path}, using defaults")
                # 設置默認配置
                self.configs['main'] = {
                    'exchange': {
                        'app_id': "460c97db-f51d-451c-a23e-3cce56d4c932",
                        'api_key': "sdFgbf5mnyDD/wahfC58Kw==",
                        'api_secret': "FWQGXZCW4P3V4D4EN4EIBL6KLTDA",
                        'market_url': "wss://wss.staging.woox.io/ws/stream"
                    },
                    'redis': {
                        'host': "localhost",
                        'port': 6379
                    },
                    'risk_management': {
                        'max_position_size': None,
                        'max_drawdown': 0.25,
                        'max_daily_loss': 125,
                        'max_leverage': 10
                    }
                }

            # Load strategy configs
            strategy_config_path = self.config_path / "strategies"
            self.configs['strategies'] = {}
            
            if strategy_config_path.exists():
                for config_file in strategy_config_path.glob("*.yaml"):
                    with open(config_file, 'r') as f:
                        strategy_name = config_file.stem
                        self.configs['strategies'][strategy_name] = yaml.safe_load(f)
                        self.logger.info(f"Strategy configuration loaded: {strategy_name}")
            else:
                self.logger.warning("Strategy config directory not found, using defaults")
                # 設置默認策略配置
                self.configs['strategies']['example_strategy'] = {
                    'trading_params': {
                        'symbol': "PERP_BTC_USDT",
                        'timeframe': "1m",
                        'max_records': 500
                    },
                    'risk_params': {
                        'max_position_size': 0.001,
                        'stop_loss_pct': 0.02,
                        'take_profit_pct': 0.04
                    }
                }

        except Exception as e:
            self.logger.error(f"Error loading configurations: {e}")
            raise

    def get_main_config(self) -> Dict[str, Any]:
        """Get main configuration"""
        if 'main' not in self.configs:
            self.logger.error("Main configuration not found")
            return {}
        return self.configs['main']

    def get_strategy_config(self, strategy_name: str) -> Dict[str, Any]:
        """Get configuration for a specific strategy"""
        if 'strategies' not in self.configs or strategy_name not in self.configs['strategies']:
            self.logger.warning(f"Strategy configuration not found for: {strategy_name}")
            return {}
        return self.configs['strategies'][strategy_name]

    def save_configs(self) -> None:
        """Save current configurations to files"""
        try:
            # Create config directories if they don't exist
            self.config_path.mkdir(exist_ok=True)
            (self.config_path / "strategies").mkdir(exist_ok=True)

            # Save main config
            main_config_path = self.config_path / "main.yaml"
            with open(main_config_path, 'w') as f:
                yaml.dump(self.configs['main'], f)

            # Save strategy configs
            for strategy_name, config in self.configs['strategies'].items():
                config_path = self.config_path / "strategies" / f"{strategy_name}.yaml"
                with open(config_path, 'w') as f:
                    yaml.dump(config, f)

            self.logger.info("Configurations saved successfully")
        except Exception as e:
            self.logger.error(f"Error saving configurations: {e}")
            raise