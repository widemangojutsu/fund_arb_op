# Fund Arbitrage Operations Framework

Arbitrage system for dex's.

## Overview

This system monitors price differences between Extended and Lighter exchanges, identifying and automatically executing arbitrage opportunities with comprehensive risk management and liquidity validation.


## ===== WORK IN PROGRESS =====
-needs proper margin/risk and auto execution

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

## Installation

1. Clone the repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

Note: The requirements.txt includes the Lighter package directly from GitHub rather than PyPI.

## Requirements

Dependencies (installed via requirements.txt):
- aiohttp: HTTP client for API requests
- requests: HTTP client for REST API requests
- python-dotenv: Environment variable management
- eth-account: Ethereum account management
- lighter: Direct installation from GitHub repo
- x10-perpetual-sdk: SDK for X10 perpetual contracts
- websockets: WebSocket client for real-time data


## Configuration

The system is configured via environment variables in the `.env` file:
```
  update the .env.example with your own credentials and rename .env.example to .env
```

## Usage

1. Configure your environment variables in `.env`
2. Run the system:
create extended account, get the api keys from extended through the 'more' tab, under api
create lighter sub account, run lightersetup.py, get the subaccount index
```bash
python lightersetup.py
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
