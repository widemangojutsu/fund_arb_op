import os
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Centralized configuration from environment variables"""

    # Exchange Credentials
    EXTENDED_API_KEY = os.getenv("EXTENDED_API_KEY")
    EXTENDED_PUBLIC_KEY = os.getenv("EXTENDED_PUBLIC_KEY")
    EXTENDED_PRIVATE_KEY = os.getenv("EXTENDED_PRIVATE_KEY")
    EXTENDED_VAULT = int(os.getenv("EXTENDED_VAULT", "0"))

    ETH_PRIVATE_KEY = os.getenv("ETH_PRIVATE_KEY")
    LIGHTER_API_PRIVATE_KEY = os.getenv("LIGHTER_API_PRIVATE_KEY", "")
    LIGHTER_ACCOUNT_INDEX = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
    LIGHTER_API_KEY_INDEX = int(os.getenv("LIGHTER_API_KEY_INDEX", "1"))

    # URLs
    EXTENDED_BASE_URL = os.getenv("EXTENDED_BASE_URL")
    EXTENDED_STREAM_URL = os.getenv("EXTENDED_STREAM_URL")
    EXTENDED_MAINNET_URL = os.getenv("EXTENDED_MAINNET_URL", os.getenv("EXTENDED_BASE_URL"))
    EXTENDED_MAINNET_STREAM_URL = os.getenv("EXTENDED_MAINNET_STREAM_URL", os.getenv("EXTENDED_STREAM_URL"))
    LIGHTER_BASE_URL = os.getenv("LIGHTER_BASE_URL")
    LIGHTER_WS_URL = os.getenv("LIGHTER_WS_URL")

    # Trading Configuration
    TRADING_MODE = os.getenv("TRADING_MODE", "dry_run")
    AUTO_EXECUTION_ENABLED = os.getenv("AUTO_EXECUTION_ENABLED", "false").lower() == "true"
    CONSERVATIVE_MODE = os.getenv("CONSERVATIVE_MODE", "true").lower() == "true"

    # Risk Parameters
    MAX_POSITION_SIZE_USD = Decimal(os.getenv("MAX_POSITION_SIZE_USD", "500"))
    MAX_TOTAL_EXPOSURE_USD = Decimal(os.getenv("MAX_TOTAL_EXPOSURE_USD", "2000"))
    MIN_ACCOUNT_BALANCE_USD = Decimal(os.getenv("MIN_ACCOUNT_BALANCE_USD", "100"))
    MAX_SLIPPAGE_BPS = Decimal(os.getenv("MAX_SLIPPAGE_BPS", "50"))
    MAX_ORDERS_PER_SYMBOL = int(os.getenv("MAX_ORDERS_PER_SYMBOL", "10"))
    EMERGENCY_STOP_LOSS_PERCENT = Decimal(os.getenv("EMERGENCY_STOP_LOSS_PERCENT", "2"))
    REBALANCE_THRESHOLD_PERCENT = Decimal(os.getenv("REBALANCE_THRESHOLD_PERCENT", "15"))

    # Execution Thresholds
    MIN_EXECUTION_SPREAD_BPS = Decimal(os.getenv("MIN_EXECUTION_SPREAD_BPS", "5"))
    AGGRESSIVE_EXECUTION_SPREAD_BPS = Decimal(os.getenv("AGGRESSIVE_EXECUTION_SPREAD_BPS", "10"))
    MIN_VIABLE_SPREAD_BPS = Decimal(os.getenv("MIN_VIABLE_SPREAD_BPS", "1"))
    MIN_ORDER_SIZE_USD = Decimal(os.getenv("MIN_ORDER_SIZE_USD", "10"))
    MAX_ORDER_SIZE_USD = Decimal(os.getenv("MAX_ORDER_SIZE_USD", "1000"))
    # Additional thresholds
    MIN_MARGIN_RATIO = Decimal(os.getenv("MIN_MARGIN_RATIO", "0.10"))
    EMERGENCY_MARGIN_RATIO = Decimal(os.getenv("EMERGENCY_MARGIN_RATIO", "0.05"))
    MIN_REQUIRED_AVAILABLE_USD = Decimal(os.getenv("MIN_REQUIRED_AVAILABLE_USD", "20"))

    # Intervals
    OPPORTUNITY_SCAN_INTERVAL = int(os.getenv("OPPORTUNITY_SCAN_INTERVAL", "60"))
    BALANCE_CHECK_INTERVAL = int(os.getenv("BALANCE_CHECK_INTERVAL", "30"))
    PERFORMANCE_LOG_INTERVAL = int(os.getenv("PERFORMANCE_LOG_INTERVAL", "120"))
    RISK_CHECK_INTERVAL = int(os.getenv("RISK_CHECK_INTERVAL", "30"))
    SYNC_STATUS_LOG_INTERVAL = int(os.getenv("SYNC_STATUS_LOG_INTERVAL", "10"))

    # Timeouts and limits
    ORDER_TIMEOUT_SECONDS = int(os.getenv("ORDER_TIMEOUT_SECONDS", "300"))
    FILL_TIMEOUT_SECONDS = int(os.getenv("FILL_TIMEOUT_SECONDS", "60"))
    WS_RECONNECT_DELAY = int(os.getenv("WS_RECONNECT_DELAY", "2"))
    API_REQUEST_TIMEOUT = int(os.getenv("API_REQUEST_TIMEOUT", "15"))

    TOP_OPPORTUNITIES_COUNT = int(os.getenv("TOP_OPPORTUNITIES_COUNT", "3"))
    MAX_CONCURRENT_ARBITRAGE_GROUPS = int(os.getenv("MAX_CONCURRENT_ARBITRAGE_GROUPS", "5"))
    MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
    ORDERBOOK_DEPTH_LEVELS = int(os.getenv("ORDERBOOK_DEPTH_LEVELS", "5"))
    # Misc runtime knobs
    ORDERBOOK_READY_COOLDOWN_SECONDS = int(os.getenv("ORDERBOOK_READY_COOLDOWN_SECONDS", "2"))

    @classmethod
    def validate(cls):
        required = ["EXTENDED_API_KEY", "EXTENDED_PRIVATE_KEY", "EXTENDED_VAULT"]
        missing = [k for k in required if not getattr(cls, k)]
        if missing:
            raise ValueError(f"Missing required config: {', '.join(missing)}")
        return True

config = Config()