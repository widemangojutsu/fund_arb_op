# Fund Arbitrage Operations Framework

A high-frequency trading (HFT) arbitrage system for cryptocurrency markets with advanced liquidity validation.

## Overview

This system monitors price differences between Extended and Lighter exchanges, identifying and automatically executing arbitrage opportunities with comprehensive risk management and liquidity validation.

## Key Features

- **Real-time Opportunity Discovery**: Continuously scans and ranks arbitrage opportunities
- **Enhanced Liquidity Validation**: Ensures trades don't exceed market depth
- **Dual Orderbook Monitoring**: Synchronized orderbook streams from multiple exchanges
- **Risk Management**: Built-in position sizing, margin monitoring, and stop-loss mechanisms
- **Balance Management**: Automated balance tracking and rebalancing
- **API Key Management**: Persistent and secure storage of exchange API keys

## System Components

- `main.py`: Primary system orchestrator
- `fund_test.py`: Arbitrage opportunity scanning engine
- `orderbooks.py`: Dual orderbook manager with liquidity validation
- `orders.py`: Enhanced order execution module
- `balance.py`: Balance and margin management
- `config.py`: Centralized configuration
- `lightersetup.py`: Lighter API key bootstrap utilities
- `utils/`: Support utilities

## Configuration

The system is configured via environment variables in the `.env` file:

```
# Exchange Credentials
EXTENDED_API_KEY=your_api_key
EXTENDED_PRIVATE_KEY=your_private_key
EXTENDED_VAULT=0
ETH_PRIVATE_KEY=your_eth_key

# Exchange URLs
EXTENDED_BASE_URL=https://api.extended.example
EXTENDED_STREAM_URL=wss://stream.extended.example
LIGHTER_BASE_URL=https://api.lighter.example
LIGHTER_WS_URL=wss://ws.lighter.example

# Trading Configuration
TRADING_MODE=dry_run # or 'live'
AUTO_EXECUTION_ENABLED=false
CONSERVATIVE_MODE=true

# Risk Parameters
MAX_POSITION_SIZE_USD=500
MIN_EXECUTION_SPREAD_BPS=5
```

## Usage

1. Configure your environment variables in `.env`
2. Run the system:

```bash
python main.py
```

## Risk Controls

The system includes multiple safety mechanisms:

- Minimum spread requirements for execution
- Maximum position size limits
- Liquidity impact scoring
- Emergency stop-loss thresholds
- Balance to liquidity ratio constraints
- Margin monitoring

## Requirements

See `requirements.txt` for dependencies.
