#!/usr/bin/env python3
"""
Standalone Balance Manager for Cross-Exchange Trading - ENHANCED WITH MARGIN FIX
================================================================================

Real-time balance monitoring and management across Extended and Lighter exchanges
with comprehensive risk management and account state tracking via WebSocket connections.

*** ENHANCED: Fixed margin ratio parsing that was causing false emergency halts ***
"""

import asyncio
import aiohttp
import logging
import time
import json
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, List
from enum import Enum
from dataclasses import dataclass

from x10.perpetual.accounts import StarkPerpetualAccount
from x10.perpetual.stream_client import PerpetualStreamClient
from x10.perpetual.configuration import STARKNET_MAINNET_CONFIG
from utils.types import Exchange
from config import config as app_config

# Import Lighter functionality
try:
    from lightersetup import ETH_PRIVATE_KEY, BASE_URL, API_KEY_INDEX
    from lighter.ws_client import WsClient
    import lighter
    import eth_account
    LIGHTER_AVAILABLE = True
except ImportError:
    LIGHTER_AVAILABLE = False


@dataclass
class BalanceData:
    """Balance information for a single exchange - ENHANCED with margin diagnostics"""
    total_balance: Decimal
    available_balance: Decimal
    used_margin: Decimal
    unrealized_pnl: Decimal
    equity: Decimal
    margin_ratio: Decimal
    timestamp: int
    
    # Enhanced diagnostic fields
    raw_margin_ratio: Optional[str] = None
    balance_source: str = "unknown"
    parsing_method: str = "unknown"
    
    @property
    def effective_margin_ratio(self) -> Decimal:
        """Calculate effective margin ratio with fallbacks - FIXES THE 0.0% ISSUE"""
        # If margin ratio is exactly 0, try to calculate from available data
        if self.margin_ratio == Decimal("0") and self.equity > 0 and self.used_margin >= 0:
            # Method 1: Calculate margin ratio as: available_balance / total_balance
            if self.total_balance > 0:
                calculated_ratio = self.available_balance / self.total_balance
                return calculated_ratio
            
            # Method 2: Alternative calculation: 1 - (used_margin / equity)
            if self.equity > 0:
                utilized_ratio = self.used_margin / self.equity
                return max(Decimal("0"), Decimal("1") - utilized_ratio)
        
        return self.margin_ratio
    
    @property
    def is_margin_healthy(self) -> bool:
        """Check if margin is healthy using multiple criteria - PREVENTS FALSE ALARMS"""
        effective_ratio = self.effective_margin_ratio
        
        # Primary check: effective margin ratio > 10%
        if effective_ratio >= Decimal("0.10"):
            return True
        
        # Secondary check: high available balance relative to used margin
        if self.used_margin > 0:
            availability_ratio = self.available_balance / self.used_margin
            if availability_ratio >= Decimal("0.5"):  # 50% availability
                return True
        
        # Tertiary check: sufficient absolute available balance
        if self.available_balance >= Decimal("100"):  # $100 minimum
            return True
        
        return False


@dataclass
class PositionData:
    """Position information"""
    symbol: str
    side: str  # "LONG", "SHORT", ""
    size: Decimal
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal
    timestamp: int

@dataclass
class OrderData:
    """Order information"""
    order_id: str
    symbol: str
    side: str
    price: Decimal
    size: Decimal
    filled: Decimal
    status: str  # e.g., NEW, PARTIALLY_FILLED, FILLED, CANCELED
    timestamp: int

@dataclass
class TradeData:
    """Trade/Fill information"""
    trade_id: str
    order_id: Optional[str]
    symbol: str
    side: str
    price: Decimal
    size: Decimal
    fee: Decimal
    timestamp: int


class BalanceManager:
    """
    Standalone balance manager for cross-exchange arbitrage trading.
    
    *** ENHANCED: Fixed margin ratio parsing that was causing emergency halts ***
    
    Monitors account balances, positions, and risk metrics across
    Extended and Lighter exchanges with real-time WebSocket updates.
    """
    
    def __init__(self, stark_account: StarkPerpetualAccount):
        self.stark_account = stark_account
        self.logger = logging.getLogger("BalanceManager")
        
        # Balance data storage
        self.extended_balance: Optional[BalanceData] = None
        self.lighter_balance: Optional[BalanceData] = None
        
        # Position tracking
        self.extended_positions: Dict[str, PositionData] = {}
        self.lighter_positions: Dict[str, PositionData] = {}
        # Order/Trade tracking (Extended)
        self.extended_orders: Dict[str, OrderData] = {}
        self.extended_trades: List[TradeData] = []
        
        # Connection state
        self.is_monitoring = False
        self.extended_connected = False
        self.lighter_connected = False
        self._lighter_ws_client = None
        # Background tasks
        self._user_stats_task = None
        self._bg_tasks: List[asyncio.Task] = []
        # In-memory caches
        self.balance_updates = 0
        self.position_updates = 0
        self.last_extended_update_ts = 0
        # Extended diagnostics: limit unknown-type logs to avoid spam
        self._ext_unknown_seen = 0
        self._ext_any_seen = 0
        self.start_time = None
        
        # *** ENHANCED: More intelligent risk parameters ***
        self.risk_config = {
            "max_total_exposure_usd": 5000.0,
            "min_margin_ratio": 0.05,  # Lowered from 0.20 to 0.05 (5%)
            "max_position_size_usd": 1000.0,
            "emergency_margin_ratio": 0.02,  # Lowered from 0.10 to 0.02 (2%)
            "use_effective_margin_ratio": True,  # Use calculated margin ratios
            "balance_health_check_enabled": True,  # Multi-criteria health checks
            "margin_calculation_fallback": True,  # Enable fallback calculations
            "min_absolute_available_usd": 50.0  # Minimum $50 available
        }
        
        # *** ENHANCED: Parsing diagnostics to track the fix ***
        self.parsing_stats = {
            "balance_parse_attempts": 0,
            "balance_parse_successes": 0,
            "margin_ratio_calculation_fallbacks": 0,
            "field_mapping_attempts": {},
            "last_successful_parsing_method": None
        }
        
        # Thread safety
        self.balance_lock = asyncio.Lock()
        
        self.logger.info("✅ Enhanced Balance Manager initialized (WebSocket-based) with margin parsing fix")
    
    async def start_monitoring(self):
        """Start real-time balance monitoring via WebSocket on both exchanges"""
        self.logger.info("🚀 Starting real-time WebSocket balance monitoring with enhanced parsing")
        self.is_monitoring = True
        self.start_time = time.time()
        
        # Start monitoring tasks for both exchanges
        tasks = [
            asyncio.create_task(self._monitor_extended_balance(), name="extended_balance"),
            asyncio.create_task(self._monitor_lighter_balance(), name="lighter_balance"),
            asyncio.create_task(self._periodic_risk_check(), name="risk_monitor")
        ]
        # Track for cancellation on shutdown
        self._bg_tasks = tasks
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"Balance monitoring error: {e}")
            raise
        finally:
            self.is_monitoring = False
            # Ensure tasks list cleared after exit
            self._bg_tasks = []
    
    async def stop_monitoring(self):
        """Stop balance monitoring"""
        self.logger.info("🛑 Stopping balance monitoring")
        self.is_monitoring = False
        # Cancel user_stats consumer first (may be blocked on WS read)
        try:
            if getattr(self, '_user_stats_task', None):
                self._user_stats_task.cancel()
                try:
                    await asyncio.wait_for(self._user_stats_task, timeout=2.0)
                except Exception:
                    pass
                self._user_stats_task = None
        except Exception:
            pass

        # Try to stop/close the Lighter WsClient if present to unblock its run loop
        try:
            client = getattr(self, '_lighter_ws_client', None)
            if client is not None:
                for mname in ("stop", "close", "disconnect", "shutdown"):
                    m = getattr(client, mname, None)
                    if callable(m):
                        try:
                            res = m()
                            if asyncio.iscoroutine(res):
                                await asyncio.wait_for(res, timeout=2.0)
                        except Exception:
                            continue
        except Exception:
            pass

        # Cancel background monitor tasks we created in start_monitoring
        try:
            if self._bg_tasks:
                for t in self._bg_tasks:
                    if t and not t.done():
                        t.cancel()
                try:
                    await asyncio.wait(self._bg_tasks, timeout=3.0)
                except Exception:
                    pass
                self._bg_tasks = []
        except Exception:
            pass
    
    async def _monitor_extended_balance(self):
        """Monitor Extended account balance via WebSocket"""
        self.logger.info("📡 Starting Extended balance monitoring")
        
        while self.is_monitoring:
            try:
                # Create stream client for account updates
                stream_client = PerpetualStreamClient(api_url=STARKNET_MAINNET_CONFIG.stream_url)
                
                # Get API key from stark account
                api_key = self.stark_account.api_key
                
                async with stream_client.subscribe_to_account_updates(api_key) as stream:
                    self.extended_connected = True
                    self.logger.info("✅ Extended balance stream connected")
                    
                    async for update in stream:
                        if not self.is_monitoring:
                            break
                        
                        await self._process_extended_update_enhanced(update)
                        
            except Exception as e:
                self.logger.error(f"Extended balance monitoring error: {e}")
                self.extended_connected = False
                
                if self.is_monitoring:
                    self.logger.info("🔄 Reconnecting to Extended balance stream in 5 seconds...")
                    await asyncio.sleep(5.0)
        
        self.logger.info("📡 Extended balance monitoring stopped")
    
    async def _process_extended_update_enhanced(self, update):
        """*** ENHANCED: Process Extended account updates with comprehensive margin parsing ***"""
        try:
            # Normalize update to a dict with 'type' and 'data' when possible
            if not isinstance(update, dict):
                try:
                    msg_type = getattr(update, 'type', None)
                    msg_data = getattr(update, 'data', None)
                    if msg_data is None and hasattr(update, '__dict__'):
                        d = dict(getattr(update, '__dict__', {}))
                        if 'data' in d:
                            msg_data = d.get('data')
                        else:
                            # fallback: consider whole object dict as data
                            msg_data = d
                        if msg_type is None:
                            msg_type = d.get('type')
                    update = {"type": msg_type, "data": msg_data}
                except Exception:
                    self.logger.info(f"EXT RAW | unhandled update object type={type(update).__name__} | sample={str(update)[:200]}")
                    return
            
            message_type = update.get("type")
            data = update.get("data", {})
            timestamp = int(time.time() * 1000)
            self.logger.debug(f"Extended update type={message_type}")
            # General diagnostics for first few frames regardless of type
            self._ext_any_seen += 1
            if self._ext_any_seen <= 5:
                try:
                    top_keys = list(update.keys())
                    data_keys = list(data.keys()) if isinstance(data, dict) else type(data).__name__
                    self.logger.info(
                        f"EXT FRAME | type={message_type} | top_keys={top_keys} | data_keys={data_keys}"
                    )
                except Exception:
                    pass
            # Log a few unknown message types to discover schema
            known = {"BALANCE", "POSITION", "ORDER", "ORDERS", "TRADE", "TRADES", "FILL"}
            if message_type not in known:
                self._ext_unknown_seen += 1
                if self._ext_unknown_seen <= 5:
                    try:
                        top_keys = list(update.keys())
                        data_keys = list(data.keys()) if isinstance(data, dict) else type(data).__name__
                        self.logger.info(
                            f"EXT RAW | unknown type={message_type} | top_keys={top_keys} | data_keys={data_keys}"
                        )
                        # Shallow sample of values without huge dumps
                        sample_items = list(update.items())[:10]
                        sample = {}
                        for k, v in sample_items:
                            if isinstance(v, (dict, list)):
                                sample[k] = type(v).__name__
                            else:
                                sample[k] = str(v)[:200]
                        self.logger.info(f"EXT RAW | sample={sample}")
                    except Exception:
                        pass

            if message_type == "BALANCE":
                await self._process_enhanced_extended_balance(data, timestamp)
            elif message_type == "POSITION":
                await self._process_extended_positions(data, timestamp)
            elif message_type in ("ORDER", "ORDERS"):
                await self._process_extended_order(data, timestamp)
            elif message_type in ("TRADE", "FILL", "TRADES"):
                await self._process_extended_trade(data, timestamp)
        
        except Exception as e:
            self.logger.error(f"Error processing Extended update: {e}")
    
    async def _process_enhanced_extended_balance(self, data, timestamp):
        """*** ENHANCED: Process Extended balance with comprehensive margin ratio parsing ***"""
        try:
            self.parsing_stats["balance_parse_attempts"] += 1
            
            # Extract balance payload either from dict or attribute-based model
            balance_info = None
            parsing_method = "unknown"
            
            if isinstance(data, dict):
                balance_info = data.get("balance")
                parsing_method = "dict_balance"
            else:
                balance_info = getattr(data, 'balance', None)
                parsing_method = "attr_balance"

            # If still nothing, try alternative field names
            if balance_info is None:
                for alt_field in ['accountBalance', 'account_balance', 'balanceData', 'balances']:
                    if isinstance(data, dict):
                        balance_info = data.get(alt_field)
                        if balance_info:
                            parsing_method = f"dict_{alt_field}"
                            break
                    else:
                        balance_info = getattr(data, alt_field, None)
                        if balance_info:
                            parsing_method = f"attr_{alt_field}"
                            break

            # If still nothing, use the data itself
            if balance_info is None:
                balance_info = data
                parsing_method = "direct_data"

            # One-time deep structural diagnostic for BALANCE payload
            if not hasattr(self, "_ext_balance_struct_logged"):
                try:
                    if isinstance(data, dict):
                        sdata_keys = list(data.keys())
                        sample = {}
                        for k, v in list(data.items())[:10]:
                            if isinstance(v, (str, int, float, bool)):
                                sample[k] = v
                            else:
                                sample[k] = type(v).__name__
                        self.logger.info(
                            f"EXT BAL STRUCT | dict keys={sdata_keys} | sample={sample}"
                        )
                    else:
                        is_snapshot = getattr(data, 'isSnapshot', getattr(data, 'is_snapshot', None))
                        bal = getattr(data, 'balance', None)
                        bal_type = type(bal).__name__ if bal is not None else None
                        # Curated fields to probe with both snake/camel aliases
                        probe = [
                            'balance', 'equity', 'available_for_trade', 'availableForTrade',
                            'used_margin', 'usedMargin', 'unrealisedPnl', 'unrealized_pnl',
                            'margin_ratio', 'marginRatio', 'currency'
                        ]
                        bal_summary = {}
                        if bal is not None:
                            for name in probe:
                                if hasattr(bal, name):
                                    try:
                                        val = getattr(bal, name)
                                        bal_summary[name] = str(val)
                                    except Exception:
                                        bal_summary[name] = '<err>'
                        self.logger.info(
                            f"EXT BAL STRUCT | data_type={type(data).__name__} | isSnapshot={is_snapshot} | balance_type={bal_type} | balance_fields={list(bal_summary.keys())} | values={bal_summary}"
                        )
                except Exception:
                    pass
                self._ext_balance_struct_logged = True

            # *** ENHANCED: Comprehensive field mapping with multiple fallbacks ***
            def enhanced_field_extract(obj, *field_names, default=None):
                """Enhanced field extraction with comprehensive fallback logic"""
                for field_name in field_names:
                    # Track attempts for diagnostics
                    if field_name not in self.parsing_stats["field_mapping_attempts"]:
                        self.parsing_stats["field_mapping_attempts"][field_name] = 0
                    self.parsing_stats["field_mapping_attempts"][field_name] += 1
                    
                    # Try dict access
                    if isinstance(obj, dict) and field_name in obj:
                        value = obj[field_name]
                        if value is not None:
                            return value
                    
                    # Try attribute access
                    if hasattr(obj, field_name):
                        value = getattr(obj, field_name)
                        if value is not None:
                            return value
                
                return default

            def safe_decimal(x, default="0"):
                try:
                    if x is None:
                        return Decimal(default)
                    return Decimal(str(x))
                except Exception:
                    return Decimal(default)

            # Extract fields with comprehensive fallback mapping
            total_balance = safe_decimal(enhanced_field_extract(
                balance_info, 
                'totalBalance', 'balance', 'total_balance', 'equity', 'total'
            ))
            
            available_balance = safe_decimal(enhanced_field_extract(
                balance_info,
                'availableForTrade', 'available_for_trade', 'available', 'availableBalance', 'freeBalance'
            ))
            
            used_margin = safe_decimal(enhanced_field_extract(
                balance_info,
                'usedMargin', 'initialMargin', 'used_margin', 'marginUsed', 'lockedMargin'
            ))
            
            unrealized_pnl = safe_decimal(enhanced_field_extract(
                balance_info,
                'unrealisedPnl', 'unrealized_pnl', 'unrealizedPnl', 'pnl', 'unrealizedProfit'
            ))
            
            equity = safe_decimal(enhanced_field_extract(
                balance_info,
                'equity', 'totalEquity', 'netEquity', 'accountEquity'
            ))
            
            # *** ENHANCED: Margin ratio extraction with fallbacks ***
            raw_margin_ratio_str = enhanced_field_extract(
                balance_info,
                'marginRatio', 'margin_ratio', 'marginLevel', 'healthRatio', 'collateralRatio'
            )
            
            margin_ratio = safe_decimal(raw_margin_ratio_str)
            
            # *** CRITICAL FIX: Calculate margin ratio if not provided or zero ***
            calculated_margin_ratio = None
            if margin_ratio == Decimal("0") or raw_margin_ratio_str is None:
                self.parsing_stats["margin_ratio_calculation_fallbacks"] += 1
                
                # Method 1: available / total
                if total_balance > 0:
                    calculated_margin_ratio = available_balance / total_balance
                    self.logger.debug(f"Calculated margin ratio (method 1): {calculated_margin_ratio:.4f}")
                
                # Method 2: 1 - (used / equity)
                elif equity > 0 and calculated_margin_ratio is None:
                    utilization = used_margin / equity
                    calculated_margin_ratio = max(Decimal("0"), Decimal("1") - utilization)
                    self.logger.debug(f"Calculated margin ratio (method 2): {calculated_margin_ratio:.4f}")
                
                # Method 3: Conservative fallback if we have positive available balance
                elif available_balance > Decimal("10") and calculated_margin_ratio is None:
                    calculated_margin_ratio = Decimal("0.50")  # Conservative 50%
                    self.logger.debug(f"Calculated margin ratio (method 3 - conservative): {calculated_margin_ratio:.4f}")
                
                # Use calculated ratio if available
                if calculated_margin_ratio is not None:
                    margin_ratio = calculated_margin_ratio

            # If equity is not provided, calculate it
            if equity == Decimal("0") and total_balance > 0:
                equity = total_balance + unrealized_pnl

            # Thread-safe assignment only
            async with self.balance_lock:
                self.extended_balance = BalanceData(
                    total_balance=total_balance,
                    available_balance=available_balance,
                    used_margin=used_margin,
                    unrealized_pnl=unrealized_pnl,
                    equity=equity,
                    margin_ratio=margin_ratio,
                    timestamp=timestamp,
                    raw_margin_ratio=str(raw_margin_ratio_str) if raw_margin_ratio_str else None,
                    balance_source="extended_websocket",
                    parsing_method=parsing_method
                )
                self.balance_updates += 1
                self.parsing_stats["balance_parse_successes"] += 1
                self.parsing_stats["last_successful_parsing_method"] = parsing_method

            # *** ENHANCED: Logging with diagnostic info ***
            eb = self.extended_balance
            effective_ratio = eb.effective_margin_ratio
            is_healthy = eb.is_margin_healthy
            
            # Log detailed balance information 
            try:
                self.logger.info(
                    f"EXT STATS | equity={float(eb.equity):.4f} | avail={float(eb.available_balance):.4f} "
                    f"| used={float(eb.used_margin):.4f} | upnl={float(eb.unrealized_pnl):.4f} | mr={float(eb.margin_ratio):.4f}"
                )
            except Exception:
                pass

            # *** ENHANCED: Show the fix working ***
            if eb.margin_ratio == Decimal("0") and calculated_margin_ratio is not None:
                self.logger.info(f"MARGIN FIX | raw=0.0000 | calculated={float(calculated_margin_ratio):.4f} | healthy={'✅' if is_healthy else '❌'} | method={parsing_method}")
            else:
                self.logger.debug(f"MARGIN OK | raw={float(eb.margin_ratio):.4f} | effective={float(effective_ratio):.4f} | healthy={'✅' if is_healthy else '❌'}")
        
        except Exception as e:
            self.logger.error(f"Error processing Enhanced Extended balance: {e}")

    async def _process_extended_order(self, data, timestamp):
        """Process Extended order updates (single or list)."""
        try:
            # Extract orders from dict or attribute-based model
            orders = []
            container = None
            if isinstance(data, dict):
                container = data.get('orders', data)
            else:
                container = getattr(data, 'orders', data)

            if isinstance(container, list):
                orders = container
            else:
                orders = [container]

            if not orders or orders == [None]:
                return

            async with self.balance_lock:
                for od in orders:
                    # field accessor supporting dicts and models
                    def f(o, *names):
                        for n in names:
                            if isinstance(o, dict) and n in o:
                                return o[n]
                            if hasattr(o, n):
                                return getattr(o, n)
                        return None
                    oid = str(f(od, 'orderId', 'id', 'clientOrderId') or '')
                    if not oid:
                        # skip if no identifiable id
                        continue
                    sym = str(f(od, 'market', 'symbol') or '')
                    side = str(f(od, 'side') or '').upper()
                    price = Decimal(str(f(od, 'price') or '0'))
                    size = Decimal(str(f(od, 'size', 'quantity') or '0'))
                    filled = Decimal(str(f(od, 'filled', 'filledSize', 'filled_size') or '0'))
                    status = str(f(od, 'status') or '').upper()
                    # prefer on-chain/server timestamp fields when present
                    ts = int(f(od, 'timestamp', 'ts') or timestamp)
                    self.extended_orders[oid] = OrderData(
                        order_id=oid,
                        symbol=sym,
                        side=side,
                        price=price,
                        size=size,
                        filled=filled,
                        status=status,
                        timestamp=ts,
                    )
                    self.logger.info(
                        f"EXT ORDER | id={oid} | {sym} {side} | px={price} | qty={size} | filled={filled} | {status}"
                    )
        except Exception as e:
            self.logger.error(f"Error processing Extended order update: {e}")

    async def _process_extended_trade(self, data, timestamp):
        """Process Extended trade/fill updates (single or list)."""
        try:
            # Extract trades from dict or attribute-based model
            trades = []
            container = None
            if isinstance(data, dict):
                container = data.get('trades', data)
            else:
                container = getattr(data, 'trades', data)

            if isinstance(container, list):
                trades = container
            else:
                trades = [container]

            if not trades or trades == [None]:
                return

            async with self.balance_lock:
                for td in trades:
                    def f(o, *names):
                        for n in names:
                            if isinstance(o, dict) and n in o:
                                return o[n]
                            if hasattr(o, n):
                                return getattr(o, n)
                        return None
                    tid = str(f(td, 'tradeId', 'id') or '')
                    oid = f(td, 'orderId', 'order_id')
                    sym = str(f(td, 'market', 'symbol') or '')
                    side = str(f(td, 'side') or '').upper()
                    price = Decimal(str(f(td, 'price') or '0'))
                    size = Decimal(str(f(td, 'size', 'quantity') or '0'))
                    fee = Decimal(str(f(td, 'fee', 'feeAmount') or '0'))
                    ts = int(f(td, 'timestamp', 'ts') or timestamp)
                    self.extended_trades.append(TradeData(
                        trade_id=tid,
                        order_id=str(oid) if oid is not None else None,
                        symbol=sym,
                        side=side,
                        price=price,
                        size=size,
                        fee=fee,
                        timestamp=ts,
                    ))
                    # cap memory
                    if len(self.extended_trades) > 1000:
                        self.extended_trades = self.extended_trades[-800:]
                    self.logger.info(
                        f"EXT TRADE | id={tid} | order={oid} | {sym} {side} | px={price} | qty={size} | fee={fee}"
                    )
        except Exception as e:
            self.logger.error(f"Error processing Extended trade update: {e}")
    
    async def _process_extended_positions(self, data, timestamp):
        """Process Extended position updates"""
        try:
            # Extract positions list from dict or attribute-based model
            positions_data = []
            if isinstance(data, dict):
                positions_data = data.get("positions", [])
            else:
                positions_data = getattr(data, 'positions', []) or []
            
            async with self.balance_lock:
                for pos in positions_data:
                    def f(o, *names, default=None):
                        for n in names:
                            if isinstance(o, dict) and n in o:
                                return o[n]
                            if hasattr(o, n):
                                return getattr(o, n)
                        return default
                    symbol = str(f(pos, "market", "symbol", default=""))
                    side = str(f(pos, "side", default="")).upper()
                    size = Decimal(str(f(pos, "size", "position_size", default="0")))
                    entry_price = Decimal(str(f(pos, "openPrice", "entry_price", "avg_entry_price", default="0")))
                    mark_price = Decimal(str(f(pos, "markPrice", "mark_price", default="0")))
                    upnl = Decimal(str(f(pos, "unrealisedPnl", "unrealized_pnl", default="0")))
                    position = PositionData(
                        symbol=symbol,
                        side=side,
                        size=size,
                        entry_price=entry_price,
                        mark_price=mark_price,
                        unrealized_pnl=upnl,
                        timestamp=timestamp
                    )
                    if symbol:
                        self.extended_positions[symbol] = position

            # Log a concise snapshot of open positions after update
            await self._log_open_positions()
        
        except Exception as e:
            self.logger.error(f"Error processing Extended positions: {e}")
    
    # --- Lighter helpers (unchanged from original) ---
    def _extract_lighter_payload(self, message: Dict) -> Dict:
        """Lighter frames often nest useful fields under 'data'.
        Return that when present, otherwise the message itself."""
        if isinstance(message, dict) and "data" in message and isinstance(message["data"], (dict, list)):
            return message["data"]
        return message

    def _to_dict(self, obj) -> Optional[Dict]:
        """Convert obj to dict when possible (supports JSON string)."""
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, str):
            try:
                parsed = json.loads(obj)
                return parsed if isinstance(parsed, dict) else None
            except Exception:
                return None
        return None

    def _to_list_of_dicts(self, obj) -> List[Dict]:
        """Normalize obj into a list[dict]. Accepts:
        - list of dicts
        - list of JSON strings
        - dict-of-dicts (returns values())
        - JSON string encoding any of the above
        Otherwise returns []."""
        # If it's a JSON string, parse first
        if isinstance(obj, str):
            try:
                obj = json.loads(obj)
            except Exception:
                return []

        if isinstance(obj, list):
            out: List[Dict] = []
            for item in obj:
                if isinstance(item, dict):
                    out.append(item)
                elif isinstance(item, str):
                    try:
                        parsed = json.loads(item)
                        if isinstance(parsed, dict):
                            out.append(parsed)
                    except Exception:
                        continue
            return out

        if isinstance(obj, dict):
            # Some feeds send map keyed by id/token -> position/balance dict
            # Only keep dict values
            return [v for v in obj.values() if isinstance(v, dict)]

        return []

    def _handle_lighter_account_update(self, account_id, message):
        """Parse Lighter WebSocket message (string -> dict) and dispatch to async handler.
        Note: lighter WsClient handles ping/pong internally; no manual ping handling here."""
        try:
            if isinstance(message, str):
                message_dict = json.loads(message)
            elif isinstance(message, dict):
                message_dict = message
            else:
                self.logger.warning(f"Unexpected message type: {type(message)}")
                return
            
            # If this is a ping frame, ignore (WsClient handles ping/pong internally)
            if self._is_ping_message(message_dict):
                return
            # Schedule async handler without blocking the callback
            asyncio.create_task(self._process_lighter_account_update(account_id, message_dict))
        except Exception as e:
            self.logger.error(f"Error in _handle_lighter_account_update: {e}")

    def _is_ping_message(self, msg: Dict) -> bool:
        """Detect Lighter ping frames to avoid logging errors and unnecessary handling.
        Accept variants like type/op/event=='ping' and nested data.type."""
        try:
            if not isinstance(msg, dict):
                return False
            keys = ('type', 'op', 'event')
            for k in keys:
                v = str(msg.get(k, '')).lower()
                if v == 'ping':
                    return True
            data = msg.get('data')
            if isinstance(data, dict):
                for k in keys:
                    v = str(data.get(k, '')).lower()
                    if v == 'ping':
                        return True
            # Some servers send bare {"ping": ...}
            if 'ping' in msg and not any(k in msg for k in ('balances', 'positions', 'data', 'orders', 'channel')):
                return True
            return False
        except Exception:
            return False

    async def _send_lighter_pong(self, msg: Dict):
        """Try to respond to a ping with a pong using available WsClient methods.
        If WsClient handles ping/pong internally, this becomes a no-op."""
        try:
            client = getattr(self, '_lighter_ws_client', None)
            if client is None:
                return

            payload = {'op': 'pong'}
            # Echo correlation if present
            if isinstance(msg, dict):
                if 'id' in msg:
                    payload['id'] = msg['id']
                if 'ts' in msg:
                    payload['ts'] = msg['ts']

            import inspect
            for method_name in ('send_pong', 'pong', 'send_json', 'send'):
                m = getattr(client, method_name, None)
                if callable(m):
                    try:
                        res = m(payload)
                        if inspect.iscoroutine(res):
                            await res
                        self.logger.debug("Sent Lighter WS pong via %s", method_name)
                        return
                    except Exception:
                        continue
            # As a last resort, just log; likely the client auto-handles ping/pong
            self.logger.debug("WsClient does not expose explicit pong; assuming internal handling")
        except Exception as e:
            self.logger.debug(f"Unable to send Lighter pong: {e}")
    
    # [Rest of Lighter methods unchanged from original for brevity - they work fine]
    async def _monitor_lighter_balance(self):
        """Monitor Lighter account balance via WebSocket (unchanged from original)"""
        self.logger.info("📡 Starting Lighter WebSocket balance monitoring")
        
        if not LIGHTER_AVAILABLE:
            self.logger.warning("⚠️ Lighter libraries not available")
            return
        
        while self.is_monitoring:
            try:
                # Get account information
                eth_acc = eth_account.Account.from_key(ETH_PRIVATE_KEY)
                eth_address = eth_acc.address
                
                # Get account index from API
                api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
                try:
                    account_response = await lighter.AccountApi(api_client).accounts_by_l1_address(l1_address=eth_address)
                finally:
                    # Ensure underlying aiohttp session is closed to prevent leaks
                    try:
                        close_coro = getattr(api_client, "close", None)
                        if callable(close_coro):
                            res = close_coro()
                            if asyncio.iscoroutine(res):
                                await res
                    except Exception:
                        pass
                
                if not account_response.sub_accounts:
                    self.logger.error("No Lighter accounts found")
                    await asyncio.sleep(60)
                    continue
                
                # Collect all sub-account indices
                account_indices = [str(sa.index) for sa in account_response.sub_accounts]
                self.logger.info(f"Monitoring Lighter accounts: {', '.join(account_indices)}")
                
                # Create WebSocket client for account updates (WsClient handles ping/pong internally)
                ws_client = WsClient(
                    host=BASE_URL.replace("https://", ""),
                    path="/stream",
                    account_ids=account_indices,
                    on_account_update=self._handle_lighter_account_update
                )

                # Keep a reference for lifecycle management
                self._lighter_ws_client = ws_client
                # Patch unhandled message behavior to avoid exceptions on ping frames
                try:
                    if hasattr(ws_client, 'handle_unhandled_message'):
                        original = getattr(ws_client, 'handle_unhandled_message')
                        def _safe_unhandled(msg):
                            try:
                                # Normalize to dict
                                md = None
                                if isinstance(msg, str):
                                    try:
                                        md = json.loads(msg)
                                    except Exception:
                                        md = None
                                elif isinstance(msg, dict):
                                    md = msg
                                # Ignore common ping shapes
                                if isinstance(md, dict):
                                    if self._is_ping_message(md):
                                        return
                                # Fall back to original handler if available, but do not raise if it throws
                                try:
                                    return original(msg)
                                except Exception:
                                    self.logger.debug("Ignoring unhandled WS message without failing")
                                    return
                            except Exception:
                                return
                        setattr(ws_client, 'handle_unhandled_message', _safe_unhandled)
                except Exception:
                    pass
                # Ensure stats map exists
                if not hasattr(self, 'lighter_user_stats'):
                    self.lighter_user_stats: Dict[str, Dict] = {}

                self.lighter_connected = True
                self.logger.info("✅ Lighter WebSocket connected")
                
                # Start dedicated user_stats WS consumer (idempotent)
                try:
                    t = getattr(self, '_user_stats_task', None)
                    if not t or t.done() or t.cancelled():
                        # Name the task for easier debugging when supported
                        try:
                            self._user_stats_task = asyncio.create_task(self._run_user_stats_ws(account_indices), name="user_stats_task")
                        except TypeError:
                            self._user_stats_task = asyncio.create_task(self._run_user_stats_ws(account_indices))
                        self.logger.info(f"▶️ Started user_stats_task; done={self._user_stats_task.done()} cancelled={self._user_stats_task.cancelled()}")
                except Exception as e:
                    self.logger.warning(f"Unable to start user_stats consumer: {e}")

                # Run WebSocket client
                await ws_client.run_async() if hasattr(ws_client, 'run_async') else ws_client.run()
                
            except Exception as e:
                self.logger.error(f"Lighter WebSocket error: {e}")
                self.lighter_connected = False
                # Do not cancel user_stats consumer; it has its own reconnect loop
                
                if self.is_monitoring:
                    self.logger.info("🔄 Reconnecting to Lighter WebSocket in 5 seconds...")
                    await asyncio.sleep(5.0)
        
        self.logger.info("📡 Lighter WebSocket monitoring stopped")

    async def _run_user_stats_ws(self, account_ids: List[str]):
        """Dedicated WS consumer for user_stats channels using aiohttp."""
        ws_url = BASE_URL.replace("https://", "wss://") + "/stream"
        backoff = 2.0
        last_stats_ts = 0.0
        while self.is_monitoring:
            try:
                timeout = aiohttp.ClientTimeout(total=None, sock_read=None, sock_connect=20)
                headers = {}
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(ws_url, headers=headers, heartbeat=30) as ws:
                        self.logger.info("📡 UserStats WS connected")
                        # subscribe all accounts
                        for aid in account_ids:
                            sub = {"type": "subscribe", "channel": f"user_stats/{aid}"}
                            await ws.send_json(sub)
                        self.logger.info(f"Subscribed to user_stats via direct WS: {', '.join(account_ids)}")

                        # read loop
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                except Exception:
                                    continue
                                # Ignore application-level pings
                                if isinstance(data, dict) and self._is_ping_message(data):
                                    continue
                                ch = str(data.get('channel', '')).lower()
                                if ch.startswith('user_stats'):
                                    # account id from channel: user_stats:{ACCOUNT_ID}
                                    acc_from_ch = None
                                    if ':' in ch:
                                        try:
                                            acc_from_ch = ch.split(':', 1)[1]
                                        except Exception:
                                            acc_from_ch = None
                                    stats_payload = data.get('stats')
                                    if isinstance(stats_payload, dict):
                                        await self._process_lighter_user_stats_ws(stats_payload, account_id=acc_from_ch)
                                        self.logger.debug(f"Received user_stats for acct={acc_from_ch}")
                                        last_stats_ts = time.time()
                                    else:
                                        # some variants nest under 'data'
                                        stats2 = data.get('data')
                                        if isinstance(stats2, dict):
                                            await self._process_lighter_user_stats_ws(stats2, account_id=acc_from_ch)
                                            self.logger.debug(f"Received user_stats (data) for acct={acc_from_ch}")
                                            last_stats_ts = time.time()
                                # Watchdog: if no stats for 30s, re-send subscribe messages
                                if last_stats_ts and (time.time() - last_stats_ts) > 30.0:
                                    try:
                                        for aid in account_ids:
                                            sub = {"type": "subscribe", "channel": f"user_stats/{aid}"}
                                            await ws.send_json(sub)
                                        self.logger.debug("Resent user_stats subscriptions due to inactivity")
                                        last_stats_ts = time.time()
                                    except Exception:
                                        pass
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                break
                            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                                break
            except asyncio.CancelledError:
                self.logger.info("🛑 UserStats WS consumer cancelled")
                return
            except Exception as e:
                self.logger.warning(f"UserStats WS error: {e}")
            # reconnect backoff
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)

    def user_stats_task_status(self) -> str:
        """Return a concise status string for the user_stats_task for debugging."""
        t = getattr(self, '_user_stats_task', None)
        if not t:
            return "user_stats_task: not created"
        try:
            return f"user_stats_task: done={t.done()} cancelled={t.cancelled()}"
        except Exception:
            return "user_stats_task: unknown"
    
    async def _process_lighter_account_update(self, account_id, message):
        """Process Lighter WebSocket account update messages"""
        try:
            if not isinstance(message, dict):
                self.logger.warning(f"Invalid Lighter WebSocket message format: {type(message)}")
                return

            timestamp = int(time.time() * 1000)
            self.logger.debug(f"Lighter account update for account_id={account_id}")

            # Extract nested payload when present
            payload = self._extract_lighter_payload(message)

            # Handle user_stats routed via channel name: {"channel":"user_stats:{ACCOUNT_ID}", "stats":{...}}
            try:
                ch = str(message.get('channel', '')).lower()
                if ch.startswith('user_stats'):
                    # Extract account id from channel suffix if present
                    acc_from_ch = None
                    if ':' in ch:
                        try:
                            acc_from_ch = ch.split(':', 1)[1]
                        except Exception:
                            acc_from_ch = None
                    stats_payload = message.get('stats')
                    if isinstance(stats_payload, dict):
                        await self._process_lighter_user_stats_ws(stats_payload, account_id=acc_from_ch)
                        return
            except Exception:
                pass

            # Extract different data types from unified message
            if isinstance(payload, dict) and 'balances' in payload:
                await self._process_lighter_balance_ws(payload['balances'], timestamp)
            elif 'balances' in message:  # fallback
                await self._process_lighter_balance_ws(message['balances'], timestamp)
            
            if isinstance(payload, dict) and 'positions' in payload:
                await self._process_lighter_positions_ws(payload['positions'], timestamp, account_id=account_id)
            elif 'positions' in message:  # fallback
                await self._process_lighter_positions_ws(message['positions'], timestamp, account_id=account_id)
            
            # Trigger risk check after each update
            await self._check_lighter_risk_limits()
            
        except Exception as e:
            self.logger.error(f"Error processing Lighter WebSocket message: {e}")
    
            # Attempt to detect user_stats updates if embedded differently
            try:
                t = str(message.get('type', '')).lower() if isinstance(message, dict) else ''
                if t == 'update/user_stats' and isinstance(message.get('stats'), dict):
                    await self._process_lighter_user_stats_ws(message['stats'], account_id)
            except Exception:
                pass

    async def _process_lighter_balance_ws(self, balances_data, timestamp):
        """Process Lighter balance updates from WebSocket"""
        try:
            if not balances_data:
                return
                
            # Thread-safe balance update
            async with self.balance_lock:
                # Normalize to list of balance dicts
                balances_list: List[Dict] = self._to_list_of_dicts(balances_data)

                # If still empty and it's a dict with a key like 'balances', unwrap
                if not balances_list and isinstance(balances_data, dict) and 'balances' in balances_data:
                    balances_list = self._to_list_of_dicts(balances_data['balances'])

                # Find USDC balance (main collateral)
                usdc_balance = None
                total_equity = Decimal("0")

                for balance in balances_list:
                    token = str(balance.get('token', '')).upper()
                    available = Decimal(str(balance.get('available', '0')))
                    total = Decimal(str(balance.get('total', '0')))

                    if token == 'USDC':
                        usdc_balance = {
                            'available': available,
                            'total': total,
                            'reserved': total - available
                        }

                    total_equity += total

                if usdc_balance:
                    self.lighter_balance = BalanceData(
                        total_balance=usdc_balance['total'],
                        available_balance=usdc_balance['available'],
                        used_margin=usdc_balance['reserved'],
                        unrealized_pnl=Decimal("0"),  # Will be calculated from positions
                        equity=total_equity,
                        margin_ratio=Decimal("1.0"),  # Lighter uses different risk model
                        timestamp=timestamp,
                        balance_source="lighter_websocket",
                        parsing_method="websocket_balances"
                    )

                    self.balance_updates += 1

                    self.logger.debug(
                        f"Lighter balance updated via WebSocket - Available: ${self.lighter_balance.available_balance} USDC"
                    )
                
        except Exception as e:
            self.logger.error(f"Error processing Lighter WebSocket balance: {e}")
    
    async def _process_lighter_positions_ws(self, positions_data, timestamp, account_id=None):
        """Process Lighter position updates from WebSocket"""
        try:
            if not positions_data:
                return
                
            async with self.balance_lock:
                # Normalize to list of position dicts
                positions_list: List[Dict] = self._to_list_of_dicts(positions_data)

                # If still empty and it's a dict with a key like 'positions', unwrap
                if not positions_list and isinstance(positions_data, dict) and 'positions' in positions_data:
                    positions_list = self._to_list_of_dicts(positions_data['positions'])

                for position_data in positions_list:
                    market_id = str(position_data.get('market_id') or position_data.get('market') or position_data.get('symbol') or '')
                    size_str = str(position_data.get('size', '0'))
                    size = Decimal(size_str)

                    # Determine side from size sign
                    if size > 0:
                        side = "LONG"
                    elif size < 0:
                        side = "SHORT"
                        size = abs(size)  # Store as positive
                    else:
                        side = ""

                    entry_price = Decimal(str(position_data.get('entry_price', position_data.get('avg_entry_price', '0'))))
                    mark_price = Decimal(str(position_data.get('mark_price', position_data.get('mark', '0'))))
                    unrealized = Decimal(str(position_data.get('unrealized_pnl', position_data.get('unrealised_pnl', '0'))))

                    position = PositionData(
                        symbol=market_id,
                        side=side,
                        size=size,
                        entry_price=entry_price,
                        mark_price=mark_price,
                        unrealized_pnl=unrealized,
                        timestamp=timestamp
                    )

                    if market_id:
                        key = f"{account_id}:{market_id}" if account_id is not None else market_id
                        self.lighter_positions[key] = position

                    self.logger.debug(f"Lighter position updated via WebSocket: account_id={account_id} market={market_id} {side} {size}")

            # Log a concise snapshot of open positions after update
            await self._log_open_positions()
            
        except Exception as e:
            self.logger.error(f"Error processing Lighter WebSocket positions: {e}")

    async def _process_lighter_user_stats_ws(self, stats: Dict, account_id=None):
        """Process Lighter user_stats updates: store and log available balance and related fields."""
        try:
            if not isinstance(stats, dict):
                return
            if not hasattr(self, 'lighter_user_stats'):
                self.lighter_user_stats = {}

            # Normalize numbers from strings
            def f(x):
                try:
                    return float(x)
                except Exception:
                    return x

            normalized = {k: (f(v) if not isinstance(v, dict) else {ik: f(iv) for ik, iv in v.items()}) for k, v in stats.items()}
            key = str(account_id) if account_id is not None else 'unknown'
            self.lighter_user_stats[key] = normalized

            # Log concise stats snapshot
            ab = normalized.get('available_balance')
            coll = normalized.get('collateral')
            pv = normalized.get('portfolio_value')
            lev = normalized.get('leverage')
            bp = normalized.get('buying_power')
            self.logger.info(f"LGT STATS | acct={key} | avail={ab} | collat={coll} | pv={pv} | lev={lev} | bp={bp}")

            # Print together with positions for context
            await self._log_open_positions()
        except Exception as e:
            self.logger.error(f"Error processing Lighter user_stats: {e}")

    async def _log_open_positions(self):
        """Print a concise list of open positions across exchanges: symbol, entry, PnL."""
        try:
            async with self.balance_lock:
                lines: List[str] = []
                # Group lighter positions by account
                from collections import defaultdict
                grouped_positions: Dict[str, List[tuple]] = defaultdict(list)
                for key, pos in self.lighter_positions.items():
                    if ':' in key:
                        acct_id, sym = key.split(':', 1)
                    else:
                        acct_id, sym = 'unknown', key
                    grouped_positions[acct_id].append((sym, pos))

                # Per-account sections
                acct_ids = sorted(set(list(grouped_positions.keys()) + list(getattr(self, 'lighter_user_stats', {}).keys() if hasattr(self, 'lighter_user_stats') else [])))
                for acct_id in acct_ids:
                    lines.append(f"--- Lighter Account {acct_id} ---")
                    # Stats first
                    st = getattr(self, 'lighter_user_stats', {}).get(acct_id) if hasattr(self, 'lighter_user_stats') else None
                    if isinstance(st, dict):
                        lines.append(
                            f"LGT STATS | acct={acct_id} | avail={st.get('available_balance')} | collat={st.get('collateral')} | pv={st.get('portfolio_value')} | lev={st.get('leverage')} | bp={st.get('buying_power')}"
                        )
                    # Positions for this account
                    for sym, pos in sorted(grouped_positions.get(acct_id, [])):
                        lines.append(
                            f"LGT | {sym} | side={pos.side} size={pos.size} | entry={pos.entry_price} | pnl={pos.unrealized_pnl}"
                        )

                # Extended (ungrouped)
                if self.extended_positions:
                    lines.append("--- Extended ---")
                    for sym, pos in self.extended_positions.items():
                        if pos.size != 0 and (pos.side or pos.size):
                            lines.append(
                                f"EXT | {sym} | entry={pos.entry_price} | pnl={pos.unrealized_pnl}"
                            )

                if lines:
                    self.logger.info("Open positions:")
                for ln in lines[:80]:  # slightly higher cap
                    self.logger.info(f"  {ln}")
        except Exception as e:
            self.logger.error(f"Error logging open positions: {e}")

    
    async def _check_lighter_risk_limits(self):
        """Check Lighter-specific risk limits"""
        try:
            # Add any Lighter-specific risk checks here
            pass
        except Exception as e:
            self.logger.error(f"Error checking Lighter risk limits: {e}")
    
    async def _periodic_risk_check(self):
        """*** ENHANCED: Periodic risk monitoring with intelligent margin checking ***"""
        while self.is_monitoring:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                risk_status = await self.check_risk_limits()
                if not risk_status["within_limits"]:
                    self.logger.warning("⚠️ Risk limit violations detected:")
                    for violation in risk_status["violations"]:
                        self.logger.warning(f"   • {violation}")
                
            except Exception as e:
                self.logger.error(f"Risk check error: {e}")
    
    async def get_account_summary(self) -> Dict:
        """*** ENHANCED: Get comprehensive account summary with margin diagnostics ***"""
        async with self.balance_lock:
            # Calculate totals
            total_equity = Decimal("0")
            total_available = Decimal("0")
            total_used_margin = Decimal("0")
            total_unrealized_pnl = Decimal("0")
            
            if self.extended_balance:
                total_equity += self.extended_balance.equity
                total_available += self.extended_balance.available_balance
                total_used_margin += self.extended_balance.used_margin
                total_unrealized_pnl += self.extended_balance.unrealized_pnl
            
            if self.lighter_balance:
                total_equity += self.lighter_balance.equity
                total_available += self.lighter_balance.available_balance
                total_used_margin += self.lighter_balance.used_margin
                total_unrealized_pnl += self.lighter_balance.unrealized_pnl
            
            return {
                "timestamp": int(time.time() * 1000),
                "total_equity": total_equity,
                "total_available": total_available,
                "total_used_margin": total_used_margin,
                "total_unrealized_pnl": total_unrealized_pnl,
                "exchanges": {
                    "extended": {
                        "connected": self.extended_connected,
                        "equity": self.extended_balance.equity if self.extended_balance else Decimal("0"),
                        "available": self.extended_balance.available_balance if self.extended_balance else Decimal("0"),
                        "balance": {
                            "equity": float(self.extended_balance.equity) if self.extended_balance else 0,
                            "available": float(self.extended_balance.available_balance) if self.extended_balance else 0,
                            "used_margin": float(self.extended_balance.used_margin) if self.extended_balance else 0,
                            "margin_ratio": float(self.extended_balance.margin_ratio) if self.extended_balance else 0,
                            # *** ENHANCED: Include fixed margin ratio calculations ***
                            "effective_margin_ratio": float(self.extended_balance.effective_margin_ratio) if self.extended_balance else 0,
                            "margin_healthy": self.extended_balance.is_margin_healthy if self.extended_balance else False,
                            "parsing_method": self.extended_balance.parsing_method if self.extended_balance else None
                        }
                    },
                    "lighter": {
                        "connected": self.lighter_connected,
                        "balances": {
                            "USDC": float(self.lighter_balance.equity) if self.lighter_balance else 0
                        }
                    }
                },
                "performance": {
                    "uptime": time.time() - self.start_time if self.start_time else 0,
                    "updates": self.balance_updates
                },
                # *** ENHANCED: Include parsing diagnostics ***
                "parsing_diagnostics": self.parsing_stats,
                "risk_status": "healthy"
            }
    
    async def check_risk_limits(self) -> Dict:
        """*** ENHANCED: Check all risk limits with intelligent margin checking ***"""
        violations = []
        
        try:
            async with self.balance_lock:
                # *** ENHANCED: Extended margin checks with intelligence ***
                if self.extended_balance:
                    eb = self.extended_balance
                    effective_margin = eb.effective_margin_ratio
                    is_healthy = eb.is_margin_healthy
                    
                    # Use intelligent health check instead of raw margin ratio
                    if not is_healthy:
                        # Only trigger violation if multiple criteria fail
                        criteria_failed = []
                        
                        if effective_margin < self.risk_config["min_margin_ratio"]:
                            criteria_failed.append(f"effective margin ratio {effective_margin:.3f}")
                        
                        if eb.available_balance < self.risk_config["min_absolute_available_usd"]:
                            criteria_failed.append(f"low available balance ${eb.available_balance:.2f}")
                        
                        if eb.used_margin > 0 and eb.available_balance / eb.used_margin < Decimal("0.2"):
                            criteria_failed.append("high margin utilization")
                        
                        # *** ENHANCED: Require at least 2 failing criteria to trigger violation ***
                        if len(criteria_failed) >= 2:
                            violations.append(f"Extended margin health: {', '.join(criteria_failed)}")
                    
                    # Emergency check - even more stringent
                    if effective_margin < self.risk_config["emergency_margin_ratio"] and eb.available_balance < Decimal("20"):
                        violations.append(f"EMERGENCY: Extended critical margin - ratio {effective_margin:.3f}, available ${eb.available_balance:.2f}")
                
                # Check total exposure
                total_equity = Decimal("0")
                if self.extended_balance:
                    total_equity += self.extended_balance.equity
                if self.lighter_balance:
                    total_equity += self.lighter_balance.equity
                
                if float(total_equity) > self.risk_config["max_total_exposure_usd"]:
                    violations.append(f"Total exposure too high: ${float(total_equity):,.2f}")
                
                # Check individual position sizes
                for symbol, position in self.extended_positions.items():
                    position_value = float(position.size * position.mark_price)
                    if position_value > self.risk_config["max_position_size_usd"]:
                        violations.append(f"Position {symbol} too large: ${position_value:,.2f}")
                
                return {
                    "within_limits": len(violations) == 0,
                    "violations": violations,
                    "timestamp": int(time.time() * 1000),
                    "margin_ratio": float(self.extended_balance.margin_ratio) if self.extended_balance else 0,
                    # *** ENHANCED: Include additional diagnostic info ***
                    "extended_effective_margin": float(self.extended_balance.effective_margin_ratio) if self.extended_balance else 0,
                    "extended_margin_healthy": self.extended_balance.is_margin_healthy if self.extended_balance else False,
                    "total_equity": float(total_equity),
                    "parsing_stats": self.parsing_stats
                }
        
        except Exception as e:
            self.logger.error(f"Error checking risk limits: {e}")
            return {
                "within_limits": False,
                "violations": [f"Risk check error: {e}"],
                "timestamp": int(time.time() * 1000)
            }
    
    # Additional methods for compatibility (unchanged from original)
    
    async def get_balances(self) -> Dict:
        """Get raw balance data"""
        account_summary = await self.get_account_summary()
        return {
            "extended": account_summary["exchanges"]["extended"],
            "lighter": account_summary["exchanges"]["lighter"]
        }
    
    async def get_positions(self) -> Dict:
        """Get position data"""
        async with self.balance_lock:
            return {
                "extended": {symbol: {
                    "symbol": pos.symbol,
                    "side": pos.side,
                    "size": float(pos.size),
                    "entry_price": float(pos.entry_price),
                    "mark_price": float(pos.mark_price),
                    "unrealized_pnl": float(pos.unrealized_pnl),
                    "timestamp": pos.timestamp
                } for symbol, pos in self.extended_positions.items()},
                "lighter": {}  # Lighter positions would be implemented similarly
            }
    
    async def get_orders(self) -> Dict:
        """Get order data - placeholder for order tracking"""
        async with self.balance_lock:
            return {
                "extended": {
                    oid: {
                        "order_id": o.order_id,
                        "symbol": o.symbol,
                        "side": o.side,
                        "price": float(o.price),
                        "size": float(o.size),
                        "filled": float(o.filled),
                        "status": o.status,
                        "timestamp": o.timestamp,
                    }
                    for oid, o in self.extended_orders.items()
                },
                "lighter": {}
            }
    

# Factory function for backward compatibility
def create_balance_manager(stark_account: StarkPerpetualAccount) -> BalanceManager:
    """Factory function to create a balance manager"""
    return BalanceManager(stark_account)


# Testing function
async def test_balance_manager():
    """Test the enhanced balance manager"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s'
    )
    
    logger = logging.getLogger("BalanceTest")
    logger.info("🧪 Testing Enhanced Balance Manager with Margin Fix")
    
    # Extended credentials from .env via config.Config
    api_key = app_config.EXTENDED_API_KEY
    public_key = app_config.EXTENDED_PUBLIC_KEY
    private_key = app_config.EXTENDED_PRIVATE_KEY
    vault = app_config.EXTENDED_VAULT
    if not api_key or not public_key or not private_key or not vault:
        logger.error("Missing EXTENDED_* configuration in .env (EXTENDED_API_KEY, EXTENDED_PUBLIC_KEY, EXTENDED_PRIVATE_KEY, EXTENDED_VAULT)")
        return
    
    stark_account = StarkPerpetualAccount(
        vault=vault,
        private_key=private_key,
        public_key=public_key,
        api_key=api_key,
    )
    
    # Create balance manager
    balance_manager = BalanceManager(stark_account)
    logger.info("✅ Enhanced balance manager created")
    
    try:
        # Test 1: Get initial account summary
        logger.info("📊 Test 1: Initial account summary")
        summary = await balance_manager.get_account_summary()
        logger.info(f"   Total Equity: ${summary['total_equity']}")
        logger.info(f"   Total Available: ${summary['total_available']}")
        logger.info(f"   Extended Connected: {summary['exchanges']['extended']['connected']}")
        logger.info(f"   Lighter Connected: {summary['exchanges']['lighter']['connected']}")
        
        # Test 2: Enhanced risk limits check
        logger.info("⚠️ Test 2: Enhanced risk limits check")
        risk_status = await balance_manager.check_risk_limits()
        logger.info(f"   Within Limits: {risk_status['within_limits']}")
        logger.info(f"   Violations: {len(risk_status['violations'])}")
        logger.info(f"   Extended Margin Healthy: {risk_status.get('extended_margin_healthy', 'N/A')}")
        
        # Test 3: Brief monitoring test
        logger.info("🚀 Test 3: Brief enhanced monitoring test (10 seconds)")
        monitor_task = asyncio.create_task(balance_manager.start_monitoring())
        
        # Let it run for 10 seconds
        await asyncio.sleep(10)
        
        # Stop monitoring
        await balance_manager.stop_monitoring()
        monitor_task.cancel()
        
        # Final summary
        final_summary = await balance_manager.get_account_summary()
        logger.info("📈 Final Enhanced Summary:")
        logger.info(f"   Balance Updates: {final_summary['performance']['updates']}")
        logger.info(f"   Extended Connected: {final_summary['exchanges']['extended']['connected']}")
        logger.info(f"   Lighter Connected: {final_summary['exchanges']['lighter']['connected']}")
        
        # Enhanced margin diagnostics
        if final_summary['exchanges']['extended']['balance']:
            ext_balance = final_summary['exchanges']['extended']['balance']
            logger.info(f"   Extended Margin Ratio: {ext_balance['margin_ratio']:.4f}")
            logger.info(f"   Extended Effective Margin: {ext_balance['effective_margin_ratio']:.4f}")
            logger.info(f"   Extended Margin Healthy: {ext_balance['margin_healthy']}")
            logger.info(f"   Parsing Method: {ext_balance['parsing_method']}")
        
        logger.info("✅ Enhanced balance manager test completed successfully")
        
    except Exception as e:
        logger.error(f"Test error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("Enhanced Balance Manager with Margin Parsing Fix")
    print("=" * 50)
    print("Real-time balance monitoring with fixed margin ratio parsing")
    print("Features:")
    print("• ✅ Fixed Extended margin ratio parsing with fallbacks")
    print("• ✅ Intelligent margin health assessment")
    print("• ✅ Enhanced risk limit checking")
    print("• ✅ Detailed parsing diagnostics")
    print("• ✅ Multi-criteria balance health checks")
    
    input("\nPress Enter to test the enhanced balance manager...")
    asyncio.run(test_balance_manager())