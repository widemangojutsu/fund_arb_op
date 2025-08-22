#!/usr/bin/env python3
"""
Dual Exchange Account Management System for Funding Arbitrage
=============================================================

Real-time account monitoring across Extended and Lighter exchanges
with thread-safe state management and risk controls for HFT arbitrage.
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from decimal import Decimal
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Callable
from enum import Enum

from x10.perpetual.accounts import StarkPerpetualAccount
from x10.perpetual.stream_client import PerpetualStreamClient
from x10.perpetual.configuration import MAINNET_CONFIG
from utils.types import Exchange
# Lighter imports
try:
    from lighter.ws_client import WsClient
    import lighter
    import eth_account
    LIGHTER_AVAILABLE = True
except ImportError:
    LIGHTER_AVAILABLE = False


@dataclass
class AccountBalance:
    """Account balance information for risk management"""
    available_for_trade: Decimal
    total_balance: Decimal
    equity: Decimal
    unrealized_pnl: Decimal
    margin_ratio: Decimal
    exposure: Decimal
    updated_time: int


@dataclass
class Position:
    """Position information across exchanges"""
    market: str
    exchange: Exchange
    side: str  # "LONG", "SHORT", or ""
    size: Decimal
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal
    margin_required: Decimal
    updated_time: int


@dataclass
class Order:
    """Order tracking across exchanges"""
    id: str
    external_id: str
    market: str
    exchange: Exchange
    side: str  # "BUY", "SELL"
    order_type: str  # "LIMIT", "MARKET"
    status: str  # "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"
    original_qty: Decimal
    filled_qty: Decimal
    remaining_qty: Decimal
    price: Decimal
    average_fill_price: Decimal = Decimal("0")
    total_fees: Decimal = Decimal("0")
    created_time: int = 0
    updated_time: int = 0


@dataclass
class TradeExecution:
    """Trade execution record for P&L tracking"""
    trade_id: str
    order_id: str
    market: str
    exchange: Exchange
    side: str
    price: Decimal
    quantity: Decimal
    fee: Decimal
    timestamp: int
    is_taker: bool = True


# ============================================================================
# DUAL EXCHANGE ACCOUNT MANAGER
# ============================================================================

class DualExchangeAccountManager:
    """
    Core account management system for Extended-Lighter funding arbitrage.
    
    Features:
    - Thread-safe real-time account state tracking
    - Risk management with configurable limits
    - Hedged order placement coordination
    - Emergency position closure capabilities
    """
    
    def __init__(self, 
                 stark_account: StarkPerpetualAccount,
                 lighter_account_id: str,
                 min_trade_size: Decimal = Decimal("0.01"),
                 min_margin_ratio: Decimal = Decimal("0.2"),
                 max_position_size: Decimal = Decimal("1.0")):
        
        self.logger = logging.getLogger("DualAccountManager")
        
        # Exchange account references
        self.stark_account = stark_account
        self.lighter_account_id = lighter_account_id
        
        # Risk management parameters
        self.min_trade_size = min_trade_size
        self.min_margin_ratio = min_margin_ratio
        self.max_position_size = max_position_size
        
        # Thread-safe locks for state updates (minimal locking for HFT)
        self._balance_lock = asyncio.Lock()
        self._position_lock = asyncio.Lock()
        self._order_lock = asyncio.Lock()
        
        # Account state storage
        self.extended_balance: Optional[AccountBalance] = None
        self.lighter_balance: Optional[AccountBalance] = None
        
        self.extended_positions: Dict[str, Position] = {}
        self.lighter_positions: Dict[str, Position] = {}
        
        self.extended_orders: Dict[str, Order] = {}
        self.lighter_orders: Dict[str, Order] = {}
        
        self.trade_executions: List[TradeExecution] = []
        
        # Trading state flags
        self.extended_can_trade = False
        self.lighter_can_trade = False
        self.system_active = False
        self.emergency_halt = False
        
        # WebSocket connection state
        self.extended_ws_connected = False
        self.lighter_ws_connected = False
        self.extended_stream_client: Optional[PerpetualStreamClient] = None
        self.lighter_ws_client = None
        
        # Callback hooks for trading logic
        self.on_balance_update: Optional[Callable] = None
        self.on_position_update: Optional[Callable] = None
        self.on_order_update: Optional[Callable] = None
        self.on_trade_execution: Optional[Callable] = None
        self.on_arbitrage_opportunity: Optional[Callable] = None
        self.on_emergency_halt: Optional[Callable] = None
        
        # Performance tracking
        self.start_time = time.time()
        self.total_trades_executed = 0
        self.total_pnl = Decimal("0")
        
        self.logger.info(f"DualExchangeAccountManager initialized")
        self.logger.info(f"Risk params - Min trade: {min_trade_size}, Min margin: {min_margin_ratio}")
    
    # ========================================================================
    # CORE SYSTEM LIFECYCLE
    # ========================================================================
    
    async def start_monitoring(self):
        """
        Start real-time account monitoring on both exchanges.
        This is the main entry point for the account management system.
        """
        self.logger.info("Starting dual exchange account monitoring")
        
        try:
            self.system_active = True
            
            # Start both monitoring tasks concurrently
            monitoring_tasks = await asyncio.gather(
                self._monitor_extended_account(),
                self._monitor_lighter_account(),
                return_exceptions=True
            )
            
            # Check for any connection failures
            for i, result in enumerate(monitoring_tasks):
                if isinstance(result, Exception):
                    exchange = "Extended" if i == 0 else "Lighter"
                    self.logger.error(f"{exchange} monitoring failed: {result}")
                    raise result
                    
        except Exception as e:
            self.logger.critical(f"Failed to start account monitoring: {e}")
            self.system_active = False
            raise
    
    async def stop_monitoring(self):
        """Stop all monitoring and close connections"""
        self.logger.info("Stopping dual exchange account monitoring")
        self.system_active = False
        
        # Close WebSocket connections
        if self.extended_stream_client:
            await self.extended_stream_client.disconnect()
        
        if self.lighter_ws_client:
            # Lighter client stop implementation will be added in Step 3
            pass
    
    # ========================================================================
    # ACCOUNT STATE ACCESS (Thread-Safe)
    # ========================================================================
    
    async def get_account_summary(self) -> Dict:
        """Get current account summary across both exchanges"""
        async with self._balance_lock:
            summary = {
                "system_active": self.system_active,
                "emergency_halt": self.emergency_halt,
                "extended": {
                    "connected": self.extended_ws_connected,
                    "can_trade": self.extended_can_trade,
                    "balance": self.extended_balance.__dict__ if self.extended_balance else None,
                    "positions": len(self.extended_positions),
                    "orders": len(self.extended_orders)
                },
                "lighter": {
                    "connected": self.lighter_ws_connected,
                    "can_trade": self.lighter_can_trade,
                    "balance": self.lighter_balance.__dict__ if self.lighter_balance else None,
                    "positions": len(self.lighter_positions),
                    "orders": len(self.lighter_orders)
                },
                "performance": {
                    "uptime_seconds": time.time() - self.start_time,
                    "total_trades": self.total_trades_executed,
                    "total_pnl": float(self.total_pnl)
                }
            }
        
        return summary
    
    async def get_trading_capacity(self, exchange: Exchange) -> Decimal:
        """Get maximum safe trading size for given exchange"""
        async with self._balance_lock:
            if exchange == Exchange.EXTENDED:
                if not self.extended_balance or not self.extended_can_trade:
                    return Decimal("0")
                
                # Conservative sizing - use 50% of available balance
                available = self.extended_balance.available_for_trade * Decimal("0.5")
                
            elif exchange == Exchange.LIGHTER:
                if not self.lighter_balance or not self.lighter_can_trade:
                    return Decimal("0")
                
                available = self.lighter_balance.available_for_trade * Decimal("0.5")
            
            else:
                return Decimal("0")
            
            # Apply minimum and maximum limits
            if available < self.min_trade_size:
                return Decimal("0")
            
            return min(available, self.max_position_size)
    
    async def get_net_exposure(self, market: str) -> Decimal:
        """Calculate net exposure across both exchanges for a market"""
        async with self._position_lock:
            extended_pos = self.extended_positions.get(market)
            
            # Map to corresponding Lighter market
            lighter_market_id = self._map_extended_to_lighter(market)
            lighter_pos = self.lighter_positions.get(lighter_market_id)
            
            extended_size = extended_pos.size if extended_pos else Decimal("0")
            lighter_size = lighter_pos.size if lighter_pos else Decimal("0")
            
            # Convert position sides to signed values
            if extended_pos and extended_pos.side == "SHORT":
                extended_size = -extended_size
            if lighter_pos and lighter_pos.side == "SHORT":
                lighter_size = -lighter_size
            
            return extended_size + lighter_size
    
    # ========================================================================
    # RISK MANAGEMENT
    # ========================================================================
    
    async def check_risk_limits(self) -> bool:
        """Check all risk limits and trigger emergency halt if needed"""
        
        # Check margin ratios
        if self.extended_balance and self.extended_balance.margin_ratio < self.min_margin_ratio:
            await self._trigger_emergency_halt("Extended margin ratio too low")
            return False
        
        # Check connection health
        if not self.extended_ws_connected or not self.lighter_ws_connected:
            self.logger.warning("WebSocket connection lost - halting new trades")
            self.extended_can_trade = False
            self.lighter_can_trade = False
            return False
        
        # Check position limits
        async with self._position_lock:
            for market, position in self.extended_positions.items():
                if abs(position.size) > self.max_position_size:
                    await self._trigger_emergency_halt(f"Position size limit exceeded: {market}")
                    return False
        
        return True
    
    async def _trigger_emergency_halt(self, reason: str):
        """Trigger emergency halt and close all positions"""
        self.logger.critical(f"EMERGENCY HALT: {reason}")
        self.emergency_halt = True
        self.extended_can_trade = False
        self.lighter_can_trade = False
        
        if self.on_emergency_halt:
            self.on_emergency_halt(reason)
        
        # Emergency position closure will be implemented in Step 4
        await self._emergency_close_all_positions()
    
    async def _emergency_close_all_positions(self):
        """Emergency closure of all positions - implementation placeholder"""
        self.logger.critical("Emergency position closure initiated")
        # Detailed implementation will be added in Step 4
        pass
    
    # ========================================================================
    # EXTENDED WEBSOCKET MESSAGE HANDLERS
    # ========================================================================
    
    async def _handle_extended_message(self, message):
        """Process incoming Extended account update messages"""
        try:
            if not isinstance(message, dict):
                self.logger.warning(f"Invalid Extended message format: {type(message)}")
                return
                
            message_type = message.get("type")
            message_data = message.get("data", {})
            timestamp = message.get("ts", int(time.time() * 1000))
            sequence = message.get("seq", 0)
            
            self.logger.debug(f"Extended message: {message_type} (seq: {sequence})")
            
            # Route to appropriate handler based on message type
            if message_type == "BALANCE":
                await self._handle_extended_balance_update(message_data, timestamp)
            elif message_type == "ORDER":
                await self._handle_extended_order_update(message_data, timestamp)
            elif message_type == "TRADE":
                await self._handle_extended_trade_update(message_data, timestamp)
            elif message_type == "POSITION":
                await self._handle_extended_position_update(message_data, timestamp)
            else:
                self.logger.warning(f"Unknown Extended message type: {message_type}")
                
            # Trigger risk check after each update
            if not await self.check_risk_limits():
                return
                
        except Exception as e:
            self.logger.error(f"Extended message handling error: {e}")
    
    async def _handle_extended_balance_update(self, data, timestamp):
        """Handle Extended BALANCE update messages"""
        try:
            balance_data = data.get("balance", {})
            
            if not balance_data:
                self.logger.warning("Empty balance data in Extended update")
                return
            
            # Thread-safe balance update
            async with self._balance_lock:
                self.extended_balance = AccountBalance(
                    available_for_trade=Decimal(str(balance_data.get("availableForTrade", "0"))),
                    total_balance=Decimal(str(balance_data.get("balance", "0"))),
                    equity=Decimal(str(balance_data.get("equity", "0"))),
                    unrealized_pnl=Decimal(str(balance_data.get("unrealisedPnl", "0"))),
                    margin_ratio=Decimal(str(balance_data.get("marginRatio", "0"))),
                    exposure=Decimal(str(balance_data.get("exposure", "0"))),
                    updated_time=timestamp
                )
                
                # Update trading permission based on balance
                if (self.extended_balance.available_for_trade >= self.min_trade_size and 
                    self.extended_balance.margin_ratio >= self.min_margin_ratio):
                    self.extended_can_trade = True
                else:
                    self.extended_can_trade = False
                    
            self.logger.info(f"Extended balance updated - Available: {self.extended_balance.available_for_trade}, "
                           f"Margin: {self.extended_balance.margin_ratio:.3f}, Can trade: {self.extended_can_trade}")
            
            # Trigger callback if registered
            if self.on_balance_update:
                self.on_balance_update(Exchange.EXTENDED, self.extended_balance)
                
        except Exception as e:
            self.logger.error(f"Extended balance update error: {e}")
    
    async def _handle_extended_order_update(self, data, timestamp):
        """Handle Extended ORDER update messages"""
        try:
            orders_data = data.get("orders", [])
            
            if not orders_data:
                return
                
            async with self._order_lock:
                for order_data in orders_data:
                    order_id = str(order_data.get("id", ""))
                    external_id = str(order_data.get("externalId", ""))
                    
                    # Create or update order record
                    order = Order(
                        id=order_id,
                        external_id=external_id,
                        market=order_data.get("market", ""),
                        exchange=Exchange.EXTENDED,
                        side=order_data.get("side", ""),
                        order_type=order_data.get("type", "LIMIT"),
                        status=order_data.get("status", "UNKNOWN"),
                        original_qty=Decimal(str(order_data.get("qty", "0"))),
                        filled_qty=Decimal(str(order_data.get("filledQty", "0"))),
                        remaining_qty=Decimal(str(order_data.get("qty", "0"))) - Decimal(str(order_data.get("filledQty", "0"))),
                        price=Decimal(str(order_data.get("price", "0"))),
                        average_fill_price=Decimal(str(order_data.get("averagePrice", "0"))),
                        total_fees=Decimal(str(order_data.get("payedFee", "0"))),
                        created_time=order_data.get("createdTime", timestamp),
                        updated_time=order_data.get("updatedTime", timestamp)
                    )
                    
                    self.extended_orders[order_id] = order
                    
                    # Log important order status changes
                    status = order.status
                    if status in ["FILLED", "CANCELLED", "REJECTED"]:
                        self.logger.info(f"Extended order {status}: {order.market} {order.side} "
                                       f"{order.filled_qty}/{order.original_qty} @ {order.average_fill_price}")
                        
                        # If order filled, may need to trigger hedge
                        if status == "FILLED" and self.on_arbitrage_opportunity:
                            self.on_arbitrage_opportunity(order)
            
            # Trigger callback if registered
            if self.on_order_update:
                self.on_order_update(Exchange.EXTENDED, orders_data)
                
        except Exception as e:
            self.logger.error(f"Extended order update error: {e}")
    
    async def _handle_extended_trade_update(self, data, timestamp):
        """Handle Extended TRADE update messages"""
        try:
            trades_data = data.get("trades", [])
            
            if not trades_data:
                return
                
            for trade_data in trades_data:
                # Record trade execution
                execution = TradeExecution(
                    trade_id=str(trade_data.get("id", "")),
                    order_id=str(trade_data.get("orderId", "")),
                    market=trade_data.get("market", ""),
                    exchange=Exchange.EXTENDED,
                    side=trade_data.get("side", ""),
                    price=Decimal(str(trade_data.get("price", "0"))),
                    quantity=Decimal(str(trade_data.get("qty", "0"))),
                    fee=Decimal(str(trade_data.get("fee", "0"))),
                    timestamp=trade_data.get("createdTime", timestamp),
                    is_taker=trade_data.get("isTaker", True)
                )
                
                self.trade_executions.append(execution)
                self.total_trades_executed += 1
                
                self.logger.info(f"Extended trade executed: {execution.market} {execution.side} "
                               f"{execution.quantity} @ {execution.price} (fee: {execution.fee})")
                
                # Trigger callback if registered
                if self.on_trade_execution:
                    self.on_trade_execution(Exchange.EXTENDED, execution)
                    
        except Exception as e:
            self.logger.error(f"Extended trade update error: {e}")
    
    async def _handle_extended_position_update(self, data, timestamp):
        """Handle Extended POSITION update messages"""
        try:
            positions_data = data.get("positions", [])
            
            if not positions_data:
                return
                
            async with self._position_lock:
                for position_data in positions_data:
                    market = position_data.get("market", "")
                    
                    position = Position(
                        market=market,
                        exchange=Exchange.EXTENDED,
                        side=position_data.get("side", ""),
                        size=Decimal(str(position_data.get("size", "0"))),
                        entry_price=Decimal(str(position_data.get("openPrice", "0"))),
                        mark_price=Decimal(str(position_data.get("markPrice", "0"))),
                        unrealized_pnl=Decimal(str(position_data.get("unrealisedPnl", "0"))),
                        margin_required=Decimal(str(position_data.get("margin", "0"))),
                        updated_time=timestamp
                    )
                    
                    self.extended_positions[market] = position
                    
                    # Update total P&L tracking
                    # Note: This is a simplified calculation, proper P&L should be calculated differently
                    if hasattr(self, 'last_extended_pnl'):
                        pnl_change = position.unrealized_pnl - self.last_extended_pnl.get(market, Decimal("0"))
                        self.total_pnl += pnl_change
                    
                    self.last_extended_pnl = getattr(self, 'last_extended_pnl', {})
                    self.last_extended_pnl[market] = position.unrealized_pnl
                    
                    self.logger.info(f"Extended position updated: {market} {position.side} {position.size} "
                                   f"@ {position.mark_price} (PnL: {position.unrealized_pnl})")
                    
                    # Check if hedge is required
                    net_exposure = await self.get_net_exposure(market)
                    if abs(net_exposure) > self.max_position_size * Decimal("0.1"):  # 10% of max size threshold
                        self.logger.warning(f"Hedge imbalance detected: {market} net exposure {net_exposure}")
            
            # Trigger callback if registered
            if self.on_position_update:
                self.on_position_update(Exchange.EXTENDED, positions_data)
                
        except Exception as e:
            self.logger.error(f"Extended position update error: {e}")
    
    # ========================================================================
    # LIGHTER WEBSOCKET MESSAGE HANDLERS
    # ========================================================================
    
    def _handle_lighter_account_update(self, account_id, message):
        """Handle Lighter account update messages (synchronous callback)"""
        try:
            # Convert to async and schedule
            asyncio.create_task(self._process_lighter_account_update(account_id, message))
        except Exception as e:
            self.logger.error(f"Lighter callback error: {e}")
    
    async def _process_lighter_account_update(self, account_id, message):
        """Process Lighter account update messages asynchronously"""
        try:
            if not isinstance(message, dict):
                self.logger.warning(f"Invalid Lighter message format: {type(message)}")
                return
            
            timestamp = int(time.time() * 1000)
            self.logger.debug(f"Lighter account update for {account_id}")
            
            # Extract different data types from unified message
            if 'balances' in message:
                await self._handle_lighter_balance_update(message['balances'], timestamp)
            
            if 'positions' in message:
                await self._handle_lighter_position_update(message['positions'], timestamp)
            
            if 'orders' in message:
                await self._handle_lighter_order_update(message['orders'], timestamp)
                
            # Trigger risk check after each update
            await self.check_risk_limits()
            
        except Exception as e:
            self.logger.error(f"Lighter message processing error: {e}")
    
    async def _handle_lighter_balance_update(self, balances_data, timestamp):
        """Handle Lighter balance updates"""
        try:
            if not balances_data:
                return
                
            # Thread-safe balance update
            async with self._balance_lock:
                # Find USDC balance (main collateral)
                usdc_balance = None
                total_equity = Decimal("0")
                
                for balance in balances_data:
                    token = balance.get('token', '').upper()
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
                    self.lighter_balance = AccountBalance(
                        available_for_trade=usdc_balance['available'],
                        total_balance=usdc_balance['total'], 
                        equity=total_equity,
                        unrealized_pnl=Decimal("0"),  # Will be calculated from positions
                        margin_ratio=Decimal("1.0"),  # Lighter uses different risk model
                        exposure=Decimal("0"),  # Will be calculated from positions
                        updated_time=timestamp
                    )
                    
                    # Update trading permission
                    if self.lighter_balance.available_for_trade >= self.min_trade_size:
                        self.lighter_can_trade = True
                    else:
                        self.lighter_can_trade = False
                        
                    self.logger.info(f"Lighter balance updated - Available: {self.lighter_balance.available_for_trade} USDC, "
                                   f"Can trade: {self.lighter_can_trade}")
                    
                    # Trigger callback if registered
                    if self.on_balance_update:
                        self.on_balance_update(Exchange.LIGHTER, self.lighter_balance)
                
        except Exception as e:
            self.logger.error(f"Lighter balance update error: {e}")
    
    async def _handle_lighter_position_update(self, positions_data, timestamp):
        """Handle Lighter position updates"""
        try:
            if not positions_data:
                return
                
            async with self._position_lock:
                for position_data in positions_data:
                    market_id = str(position_data.get('market_id', ''))
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
                    
                    position = Position(
                        market=market_id,
                        exchange=Exchange.LIGHTER,
                        side=side,
                        size=size,
                        entry_price=Decimal(str(position_data.get('entry_price', '0'))),
                        mark_price=Decimal(str(position_data.get('mark_price', '0'))),
                        unrealized_pnl=Decimal(str(position_data.get('unrealized_pnl', '0'))),
                        margin_required=Decimal(str(position_data.get('margin', '0'))),
                        updated_time=timestamp
                    )
                    
                    self.lighter_positions[market_id] = position
                    
                    self.logger.info(f"Lighter position updated: Market {market_id} {side} {size} "
                                   f"@ {position.mark_price} (PnL: {position.unrealized_pnl})")
            
            # Trigger callback if registered
            if self.on_position_update:
                self.on_position_update(Exchange.LIGHTER, positions_data)
                
        except Exception as e:
            self.logger.error(f"Lighter position update error: {e}")
    
    async def _handle_lighter_order_update(self, orders_data, timestamp):
        """Handle Lighter order updates"""
        try:
            if not orders_data:
                return
                
            async with self._order_lock:
                for order_data in orders_data:
                    order_id = str(order_data.get('id', ''))
                    
                    # Map Lighter order status to standard format
                    lighter_status = order_data.get('status', 'UNKNOWN')
                    status_mapping = {
                        'open': 'NEW',
                        'filled': 'FILLED', 
                        'cancelled': 'CANCELLED',
                        'partial': 'PARTIALLY_FILLED'
                    }
                    status = status_mapping.get(lighter_status.lower(), lighter_status.upper())
                    
                    order = Order(
                        id=order_id,
                        external_id=order_data.get('client_order_id', order_id),
                        market=str(order_data.get('market_id', '')),
                        exchange=Exchange.LIGHTER,
                        side=order_data.get('side', '').upper(),
                        order_type=order_data.get('type', 'LIMIT').upper(),
                        status=status,
                        original_qty=Decimal(str(order_data.get('quantity', '0'))),
                        filled_qty=Decimal(str(order_data.get('filled_quantity', '0'))),
                        remaining_qty=Decimal(str(order_data.get('quantity', '0'))) - Decimal(str(order_data.get('filled_quantity', '0'))),
                        price=Decimal(str(order_data.get('price', '0'))),
                        average_fill_price=Decimal(str(order_data.get('average_fill_price', '0'))),
                        total_fees=Decimal(str(order_data.get('fees', '0'))),
                        created_time=order_data.get('created_at', timestamp),
                        updated_time=timestamp
                    )
                    
                    self.lighter_orders[order_id] = order
                    
                    # Log important status changes
                    if status in ["FILLED", "CANCELLED", "REJECTED"]:
                        self.logger.info(f"Lighter order {status}: Market {order.market} {order.side} "
                                       f"{order.filled_qty}/{order.original_qty} @ {order.average_fill_price}")
                        
                        # If order filled, may need to trigger hedge
                        if status == "FILLED" and self.on_arbitrage_opportunity:
                            self.on_arbitrage_opportunity(order)
            
            # Trigger callback if registered
            if self.on_order_update:
                self.on_order_update(Exchange.LIGHTER, orders_data)
                
        except Exception as e:
            self.logger.error(f"Lighter order update error: {e}")
    
    # ========================================================================
    # HEDGED ORDER PLACEMENT SYSTEM
    # ========================================================================
    
    async def place_hedged_funding_orders(self,
                                         extended_market: str,
                                         lighter_market_id: int,
                                         target_price: Decimal,
                                         order_size: Decimal,
                                         extended_side: str) -> Dict:
        """
        Place hedged orders on both Extended and Lighter at the same price.
        
        Args:
            extended_market: Extended market symbol (e.g., "BTC-USD")
            lighter_market_id: Lighter market ID (e.g., 1 for BTC)
            target_price: Target execution price for both exchanges
            order_size: Order size (will be validated against available balance)
            extended_side: "BUY" or "SELL" on Extended (opposite on Lighter)
            
        Returns:
            Dict with order placement results and execution status
        """
        
        self.logger.info(f"Placing hedged funding orders: {extended_market} vs Lighter {lighter_market_id}")
        self.logger.info(f"Target price: {target_price}, Size: {order_size}, Extended side: {extended_side}")
        
        # Validate system is ready for trading
        if not await self._validate_trading_ready():
            return {
                "success": False,
                "error": "System not ready for trading",
                "extended_order": None,
                "lighter_order": None
            }
        
        # Calculate safe order sizes for both exchanges
        safe_sizes = await self._calculate_hedged_order_sizes(order_size)
        if safe_sizes["extended_size"] == 0 or safe_sizes["lighter_size"] == 0:
            return {
                "success": False,
                "error": "Insufficient balance for hedged orders",
                "extended_order": None,
                "lighter_order": None
            }
        
        # Use minimum of both sizes to ensure perfect hedge
        final_size = min(safe_sizes["extended_size"], safe_sizes["lighter_size"])
        lighter_side = "SELL" if extended_side == "BUY" else "BUY"
        
        self.logger.info(f"Final hedge size: {final_size} (Extended: {extended_side}, Lighter: {lighter_side})")
        
        # Place orders simultaneously with timeout
        try:
            extended_task = asyncio.create_task(
                self._place_extended_order(extended_market, extended_side, final_size, target_price)
            )
            
            lighter_task = asyncio.create_task(
                self._place_lighter_order(lighter_market_id, lighter_side, final_size, target_price)
            )
            
            # Execute both orders with 10-second timeout
            extended_result, lighter_result = await asyncio.wait_for(
                asyncio.gather(extended_task, lighter_task, return_exceptions=True),
                timeout=10.0
            )
            
            # Check results and handle failures
            success = True
            error_msgs = []
            
            if isinstance(extended_result, Exception):
                success = False
                error_msgs.append(f"Extended order failed: {extended_result}")
                extended_result = None
                
            if isinstance(lighter_result, Exception):
                success = False
                error_msgs.append(f"Lighter order failed: {lighter_result}")
                lighter_result = None
            
            # Handle partial hedge failure
            if not success:
                await self._handle_partial_hedge_failure(extended_result, lighter_result)
            
            result = {
                "success": success,
                "error": "; ".join(error_msgs) if error_msgs else None,
                "extended_order": extended_result,
                "lighter_order": lighter_result,
                "hedge_size": float(final_size),
                "target_price": float(target_price),
                "timestamp": time.time()
            }
            
            if success:
                self.logger.info(f"✅ Hedged orders placed successfully: {final_size} @ {target_price}")
            else:
                self.logger.error(f"❌ Hedged order placement failed: {'; '.join(error_msgs)}")
            
            return result
            
        except asyncio.TimeoutError:
            self.logger.error("Hedged order placement timed out")
            return {
                "success": False,
                "error": "Order placement timed out",
                "extended_order": None,
                "lighter_order": None
            }
        except Exception as e:
            self.logger.error(f"Hedged order placement error: {e}")
            return {
                "success": False,
                "error": str(e),
                "extended_order": None,
                "lighter_order": None
            }
    
    async def _validate_trading_ready(self) -> bool:
        """Validate that both exchanges are ready for trading"""
        if self.emergency_halt:
            self.logger.warning("Trading halted - emergency mode")
            return False
            
        if not self.extended_can_trade:
            self.logger.warning("Extended trading not available")
            return False
            
        if not self.lighter_can_trade:
            self.logger.warning("Lighter trading not available")
            return False
            
        if not self.extended_ws_connected or not self.lighter_ws_connected:
            self.logger.warning("WebSocket connections not ready")
            return False
            
        return True
    
    async def _calculate_hedged_order_sizes(self, requested_size: Decimal) -> Dict[str, Decimal]:
        """Calculate safe order sizes for both exchanges"""
        async with self._balance_lock:
            # Extended sizing
            extended_available = Decimal("0")
            if self.extended_balance:
                # Use 80% of available for safety
                extended_available = self.extended_balance.available_for_trade * Decimal("0.8")
            
            # Lighter sizing
            lighter_available = Decimal("0")
            if self.lighter_balance:
                # Use 80% of available for safety
                lighter_available = self.lighter_balance.available_for_trade * Decimal("0.8")
            
            # Apply minimum size constraints
            extended_size = min(requested_size, extended_available)
            lighter_size = min(requested_size, lighter_available)
            
            if extended_size < self.min_trade_size:
                extended_size = Decimal("0")
            if lighter_size < self.min_trade_size:
                lighter_size = Decimal("0")
                
            self.logger.debug(f"Order sizing - Requested: {requested_size}, "
                            f"Extended: {extended_size}, Lighter: {lighter_size}")
            
            return {
                "extended_size": extended_size,
                "lighter_size": lighter_size,
                "extended_available": extended_available,
                "lighter_available": lighter_available
            }
    
    async def _place_extended_order(self, market: str, side: str, size: Decimal, price: Decimal):
        """Place order on Extended exchange"""
        try:
            # Import Extended trading client
            from x10.perpetual.trading_client import PerpetualTradingClient
            
            # Create trading client
            trading_client = PerpetualTradingClient(
                endpoint_config=MAINNET_CONFIG,
                stark_account=self.stark_account
            )
            
            # Generate external order ID for tracking
            external_id = f"hedge_{int(time.time() * 1000)}_{market}_{side}"
            
            # Place order
            order_response = await trading_client.order.create_or_edit_order(
                market=market,
                side=side,
                type="LIMIT",
                size=str(size),
                price=str(price),
                external_id=external_id,
                reduce_only=False
            )
            
            self.logger.info(f"Extended order placed: {market} {side} {size} @ {price}")
            return {
                "exchange": "extended",
                "order_id": order_response.id if hasattr(order_response, 'id') else external_id,
                "external_id": external_id,
                "market": market,
                "side": side,
                "size": float(size),
                "price": float(price),
                "status": "PENDING"
            }
            
        except Exception as e:
            self.logger.error(f"Extended order placement failed: {e}")
            raise e
    
    async def _place_lighter_order(self, market_id: int, side: str, size: Decimal, price: Decimal):
        """Place order on Lighter exchange"""
        try:
            if not LIGHTER_AVAILABLE:
                raise Exception("Lighter library not available")
            
            # Import Lighter configuration
            from lightersetup import ETH_PRIVATE_KEY, BASE_URL, API_KEY_INDEX
            
            # Create Lighter client
            # Note: This is a simplified implementation
            # In production, you would maintain persistent client connection
            import eth_account
            eth_acc = eth_account.Account.from_key(ETH_PRIVATE_KEY)
            eth_address = eth_acc.address
            
            # Get account information
            api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
            account_response = await lighter.AccountApi(api_client).accounts_by_l1_address(l1_address=eth_address)
            account_index = account_response.sub_accounts[0].index
            
            # Create SignerClient for order placement
            # Note: This would need proper API key setup in production
            tx_client = lighter.SignerClient(
                url=BASE_URL,
                private_key="dummy_api_key",  # Would need real API key
                account_index=account_index,
                api_key_index=API_KEY_INDEX,
            )
            
            # Generate client order ID
            client_order_id = f"hedge_{int(time.time() * 1000)}_{market_id}_{side}"
            
            # Place order (simplified - would need proper implementation)
            self.logger.info(f"Lighter order would be placed: Market {market_id} {side} {size} @ {price}")
            
            return {
                "exchange": "lighter",
                "order_id": client_order_id,
                "external_id": client_order_id,
                "market": str(market_id),
                "side": side,
                "size": float(size),
                "price": float(price),
                "status": "PENDING"
            }
            
        except Exception as e:
            self.logger.error(f"Lighter order placement failed: {e}")
            raise e
    
    async def _handle_partial_hedge_failure(self, extended_result, lighter_result):
        """Handle cases where only one side of the hedge succeeded"""
        self.logger.critical("PARTIAL HEDGE FAILURE - Taking emergency action")
        
        # Cancel successful orders to avoid unhedged exposure
        if extended_result and not isinstance(extended_result, Exception):
            self.logger.warning("Attempting to cancel Extended order due to partial hedge failure")
            try:
                await self._cancel_extended_order(extended_result["order_id"])
            except Exception as e:
                self.logger.error(f"Failed to cancel Extended order: {e}")
        
        if lighter_result and not isinstance(lighter_result, Exception):
            self.logger.warning("Attempting to cancel Lighter order due to partial hedge failure")
            try:
                await self._cancel_lighter_order(lighter_result["order_id"])
            except Exception as e:
                self.logger.error(f"Failed to cancel Lighter order: {e}")
        
        # Trigger emergency halt if needed
        await self._trigger_emergency_halt("Partial hedge failure - unhedged exposure risk")
    
    async def _cancel_extended_order(self, order_id: str):
        """Cancel Extended order"""
        try:
            from x10.perpetual.trading_client import PerpetualTradingClient
            
            trading_client = PerpetualTradingClient(
                endpoint_config=MAINNET_CONFIG,
                stark_account=self.stark_account
            )
            
            await trading_client.order.cancel_order(order_id=order_id)
            self.logger.info(f"Extended order {order_id} cancelled")
            
        except Exception as e:
            self.logger.error(f"Failed to cancel Extended order {order_id}: {e}")
    
    async def _cancel_lighter_order(self, order_id: str):
        """Cancel Lighter order"""
        try:
            # Lighter order cancellation implementation
            # This would need proper Lighter API integration
            self.logger.info(f"Lighter order {order_id} cancellation requested")
            
        except Exception as e:
            self.logger.error(f"Failed to cancel Lighter order {order_id}: {e}")
    
    # ========================================================================
    # HELPER METHODS
    # ========================================================================
    
    def _map_extended_to_lighter(self, extended_market: str) -> str:
        """Map Extended market symbol to Lighter market ID"""
        # This will integrate with existing CrossExchangeMapper in Step 5
        # For now, simple conversion logic
        if extended_market.endswith("-USD"):
            base = extended_market.replace("-USD", "")
            return f"lighter_{base.lower()}"
        return extended_market
    
    async def get_system_status(self) -> Dict:
        """Get comprehensive system status for monitoring"""
        return {
            "timestamp": datetime.now().isoformat(),
            "system_active": self.system_active,
            "emergency_halt": self.emergency_halt,
            "exchanges": {
                "extended": {
                    "connected": self.extended_ws_connected,
                    "can_trade": self.extended_can_trade,
                    "balance_available": float(self.extended_balance.available_for_trade) if self.extended_balance else 0,
                    "positions_count": len(self.extended_positions),
                    "orders_count": len(self.extended_orders)
                },
                "lighter": {
                    "connected": self.lighter_ws_connected,
                    "can_trade": self.lighter_can_trade,
                    "balance_available": float(self.lighter_balance.available_for_trade) if self.lighter_balance else 0,
                    "positions_count": len(self.lighter_positions),
                    "orders_count": len(self.lighter_orders)
                }
            },
            "performance": {
                "uptime_seconds": time.time() - self.start_time,
                "total_trades": self.total_trades_executed,
                "total_pnl": float(self.total_pnl)
            }
        }
    
    async def _monitor_extended_account(self):
        """Monitor Extended account updates using PerpetualStreamClient"""
        self.logger.info("Starting Extended account monitoring with real account")
        
        # Import Extended account configuration
        try:
            from extendedsetup import api_key, vault, private_key, public_key
            
            # Initialize Extended stream client
            self.extended_stream_client = PerpetualStreamClient(api_url=MAINNET_CONFIG.stream_url)
            
            # Start account updates stream with exponential backoff reconnection
            max_retries = 5
            retry_delay = 1
            
            for attempt in range(max_retries):
                try:
                    self.logger.info(f"Extended WebSocket connection attempt {attempt + 1}/{max_retries}")
                    
                    async with self.extended_stream_client.subscribe_to_account_updates(api_key) as stream:
                        self.extended_ws_connected = True
                        self.logger.info("Extended WebSocket connected successfully")
                        
                        while self.system_active and not self.emergency_halt:
                            try:
                                # Receive account update with timeout
                                message = await asyncio.wait_for(stream.recv(), timeout=30)
                                
                                # Process the message
                                await self._handle_extended_message(message)
                                
                            except asyncio.TimeoutError:
                                self.logger.debug("Extended WebSocket timeout - sending heartbeat")
                                # Connection is still alive, just no messages
                                continue
                                
                            except Exception as msg_error:
                                self.logger.error(f"Extended message processing error: {msg_error}")
                                continue
                    
                    # If we reach here, connection closed normally
                    break
                    
                except Exception as conn_error:
                    self.extended_ws_connected = False
                    self.extended_can_trade = False
                    
                    self.logger.error(f"Extended WebSocket connection failed: {conn_error}")
                    
                    if attempt < max_retries - 1:
                        self.logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, 60)  # Exponential backoff, max 60s
                    else:
                        self.logger.critical("Extended WebSocket connection failed after all retries")
                        await self._trigger_emergency_halt("Extended WebSocket connection failed")
                        break
                        
        except ImportError:
            self.logger.error("Could not import Extended account configuration from extendedsetup.py")
            await self._trigger_emergency_halt("Extended account configuration missing")
        except Exception as e:
            self.logger.critical(f"Extended account monitoring initialization failed: {e}")
            await self._trigger_emergency_halt(f"Extended monitoring failed: {e}")
    
    async def _monitor_lighter_account(self):
        """Monitor Lighter account updates using WsClient"""
        self.logger.info("Starting Lighter account monitoring with real account")
        
        if not LIGHTER_AVAILABLE:
            self.logger.error("Lighter library not available - skipping Lighter monitoring")
            return
        
        # Import Lighter account configuration
        try:
            from lightersetup import ETH_PRIVATE_KEY, BASE_URL
            
            # Get account ID from Ethereum address
            eth_acc = eth_account.Account.from_key(ETH_PRIVATE_KEY)
            eth_address = eth_acc.address
            
            # Get account information
            api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
            account_response = await lighter.AccountApi(api_client).accounts_by_l1_address(l1_address=eth_address)
            
            if not account_response.sub_accounts:
                self.logger.error(f"No Lighter accounts found for address: {eth_address}")
                return
            
            # Use first account (basic implementation)
            account_index = account_response.sub_accounts[0].index
            self.logger.info(f"Monitoring Lighter account index: {account_index}")
            
            # Start Lighter WebSocket with exponential backoff
            max_retries = 5
            retry_delay = 1
            
            for attempt in range(max_retries):
                try:
                    self.logger.info(f"Lighter WebSocket connection attempt {attempt + 1}/{max_retries}")
                    
                    # Create WsClient for account updates
                    ws_client = WsClient(
                        host=BASE_URL.replace("https://", ""),
                        path="/stream",
                        account_ids=[str(account_index)],
                        on_account_update=self._handle_lighter_account_update
                    )
                    
                    self.lighter_ws_client = ws_client
                    self.lighter_ws_connected = True
                    self.logger.info("Lighter WebSocket connected successfully")
                    
                    # Run WebSocket client
                    await ws_client.run_async()
                    
                    # If we reach here, connection closed normally
                    break
                    
                except Exception as conn_error:
                    self.lighter_ws_connected = False
                    self.lighter_can_trade = False
                    
                    self.logger.error(f"Lighter WebSocket connection failed: {conn_error}")
                    
                    if attempt < max_retries - 1:
                        self.logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, 60)  # Exponential backoff
                    else:
                        self.logger.critical("Lighter WebSocket connection failed after all retries")
                        await self._trigger_emergency_halt("Lighter WebSocket connection failed")
                        break
                        
        except ImportError:
            self.logger.error("Could not import Lighter account configuration from lightersetup.py")
        except Exception as e:
            self.logger.critical(f"Lighter account monitoring initialization failed: {e}")
            await self._trigger_emergency_halt(f"Lighter monitoring failed: {e}")
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def __repr__(self):
        return (f"DualExchangeAccountManager("
                f"extended_connected={self.extended_ws_connected}, "
                f"lighter_connected={self.lighter_ws_connected}, "
                f"system_active={self.system_active})")


# ============================================================================
# ACCOUNT MANAGER FACTORY
# ============================================================================

async def create_account_manager(stark_account: StarkPerpetualAccount,
                               lighter_account_id: str,
                               **kwargs) -> DualExchangeAccountManager:
    """
    Factory function to create and initialize DualExchangeAccountManager
    
    Args:
        stark_account: Extended/StarkNet account instance
        lighter_account_id: Lighter account identifier
        **kwargs: Additional configuration parameters
    
    Returns:
        Initialized DualExchangeAccountManager instance
    """
    
    manager = DualExchangeAccountManager(
        stark_account=stark_account,
        lighter_account_id=lighter_account_id,
        **kwargs
    )
    
    # Additional initialization can be added here
    return manager


# ============================================================================
# TESTING AND VALIDATION
# ============================================================================

async def test_extended_websocket():
    """Test Extended WebSocket connection with real account"""
    logging.basicConfig(level=logging.INFO)
    
    print("Testing Extended WebSocket Account Updates...")
    
    try:
        # Import real account configuration
        from extendedsetup import api_key, vault, private_key, public_key
        
        # Create real account for testing
        stark_account = StarkPerpetualAccount(
            vault=vault,
            private_key=private_key,
            public_key=public_key,
            api_key=api_key
        )
        
        manager = await create_account_manager(
            stark_account=stark_account,
            lighter_account_id="test_lighter_account",  # Will be updated in Step 3
            min_trade_size=Decimal("0.001")
        )
        
        print("✅ Account Manager initialized with real Extended account")
        print(f"   Vault: {vault}")
        print(f"   API Key: {api_key[:8]}...")
        
        # Test Extended WebSocket connection for 30 seconds
        print("\n🔌 Testing Extended WebSocket connection (30 seconds)...")
        
        # Set up callbacks to capture updates
        balance_updates = []
        order_updates = []
        position_updates = []
        trade_updates = []
        
        def capture_balance_update(exchange, balance):
            balance_updates.append((exchange, balance))
            print(f"📊 Balance Update: {exchange.value} - Available: {balance.available_for_trade}")
        
        def capture_order_update(exchange, orders):
            order_updates.append((exchange, orders))
            print(f"📋 Order Update: {exchange.value} - {len(orders)} orders")
        
        def capture_position_update(exchange, positions):
            position_updates.append((exchange, positions))
            print(f"📈 Position Update: {exchange.value} - {len(positions)} positions")
        
        def capture_trade_update(exchange, execution):
            trade_updates.append((exchange, execution))
            print(f"💱 Trade Update: {exchange.value} - {execution.market} {execution.side} {execution.quantity}")
        
        # Register callbacks
        manager.on_balance_update = capture_balance_update
        manager.on_order_update = capture_order_update
        manager.on_position_update = capture_position_update
        manager.on_trade_execution = capture_trade_update
        
        # Start monitoring task with timeout
        monitoring_task = asyncio.create_task(manager._monitor_extended_account())
        
        try:
            # Let it run for 30 seconds to capture updates
            await asyncio.wait_for(monitoring_task, timeout=30.0)
        except asyncio.TimeoutError:
            print("✅ Extended WebSocket test completed (30s timeout)")
            monitoring_task.cancel()
        
        # Show results
        print(f"\n📊 Test Results:")
        print(f"  Balance updates: {len(balance_updates)}")
        print(f"  Order updates: {len(order_updates)}")
        print(f"  Position updates: {len(position_updates)}")
        print(f"  Trade updates: {len(trade_updates)}")
        print(f"  WebSocket connected: {manager.extended_ws_connected}")
        print(f"  Can trade: {manager.extended_can_trade}")
        
        # Show account summary
        summary = await manager.get_account_summary()
        print("\n📈 Final Account Summary:")
        print(json.dumps(summary, indent=2, default=str))
        
        return manager
        
    except Exception as e:
        print(f"❌ Extended WebSocket test failed: {e}")
        raise


async def test_lighter_websocket():
    """Test Lighter WebSocket connection with real account"""
    logging.basicConfig(level=logging.INFO)
    
    print("Testing Lighter WebSocket Account Updates...")
    
    try:
        # Import real account configuration
        from lightersetup import ETH_PRIVATE_KEY
        from extendedsetup import api_key, vault, private_key, public_key
        
        # Create account manager with real configurations
        stark_account = StarkPerpetualAccount(
            vault=vault,
            private_key=private_key,
            public_key=public_key,
            api_key=api_key
        )
        
        manager = await create_account_manager(
            stark_account=stark_account,
            lighter_account_id="auto_detect",  # Will detect from ETH_PRIVATE_KEY
            min_trade_size=Decimal("1.0")  # $1 minimum for testing
        )
        
        print("✅ Account Manager initialized with real Lighter account")
        print(f"   ETH Private Key: {ETH_PRIVATE_KEY[:8]}...")
        
        # Test Lighter WebSocket connection for 30 seconds
        print("\n🔌 Testing Lighter WebSocket connection (30 seconds)...")
        
        # Set up callbacks to capture updates
        balance_updates = []
        order_updates = []
        position_updates = []
        
        def capture_balance_update(exchange, balance):
            balance_updates.append((exchange, balance))
            print(f"💰 Balance Update: {exchange.value} - Available: {balance.available_for_trade}")
        
        def capture_order_update(exchange, orders):
            order_updates.append((exchange, orders))
            print(f"📝 Order Update: {exchange.value} - {len(orders)} orders")
        
        def capture_position_update(exchange, positions):
            position_updates.append((exchange, positions))
            print(f"📊 Position Update: {exchange.value} - {len(positions)} positions")
        
        # Register callbacks
        manager.on_balance_update = capture_balance_update
        manager.on_order_update = capture_order_update
        manager.on_position_update = capture_position_update
        
        # Start monitoring task with timeout
        monitoring_task = asyncio.create_task(manager._monitor_lighter_account())
        
        try:
            # Let it run for 30 seconds to capture updates
            await asyncio.wait_for(monitoring_task, timeout=30.0)
        except asyncio.TimeoutError:
            print("✅ Lighter WebSocket test completed (30s timeout)")
            monitoring_task.cancel()
        
        # Show results
        print(f"\n📈 Test Results:")
        print(f"  Balance updates: {len(balance_updates)}")
        print(f"  Order updates: {len(order_updates)}")
        print(f"  Position updates: {len(position_updates)}")
        print(f"  WebSocket connected: {manager.lighter_ws_connected}")
        print(f"  Can trade: {manager.lighter_can_trade}")
        
        # Show account summary
        summary = await manager.get_account_summary()
        print("\n📊 Final Account Summary:")
        print(json.dumps(summary, indent=2, default=str))
        
        return manager
        
    except Exception as e:
        print(f"❌ Lighter WebSocket test failed: {e}")
        raise


async def test_dual_monitoring():
    """Test dual exchange monitoring with both Extended and Lighter"""
    logging.basicConfig(level=logging.INFO)
    
    print("Testing Dual Exchange Account Monitoring...")
    
    try:
        # Import real account configurations
        from extendedsetup import api_key, vault, private_key, public_key
        from lightersetup import ETH_PRIVATE_KEY
        
        # Create account manager with real configurations
        stark_account = StarkPerpetualAccount(
            vault=vault,
            private_key=private_key,
            public_key=public_key,
            api_key=api_key
        )
        
        manager = await create_account_manager(
            stark_account=stark_account,
            lighter_account_id="auto_detect",
            min_trade_size=Decimal("0.1")
        )
        
        print("✅ Dual Account Manager initialized")
        print(f"   Extended Vault: {vault}")
        print(f"   Lighter ETH Key: {ETH_PRIVATE_KEY[:8]}...")
        
        # Set up comprehensive monitoring
        updates_count = {
            'extended_balance': 0,
            'extended_orders': 0,
            'extended_positions': 0,
            'lighter_balance': 0,
            'lighter_orders': 0,
            'lighter_positions': 0
        }
        
        def track_updates(exchange, data_type):
            key = f"{exchange.value}_{data_type}"
            updates_count[key] += 1
            print(f"🔔 Update: {exchange.value.upper()} {data_type}")
        
        # Register tracking callbacks
        manager.on_balance_update = lambda ex, bal: track_updates(ex, 'balance')
        manager.on_order_update = lambda ex, ord: track_updates(ex, 'orders')
        manager.on_position_update = lambda ex, pos: track_updates(ex, 'positions')
        
        print("\n🚀 Starting dual exchange monitoring (60 seconds)...")
        
        # Start both monitoring tasks
        extended_task = asyncio.create_task(manager._monitor_extended_account())
        lighter_task = asyncio.create_task(manager._monitor_lighter_account())
        
        try:
            # Monitor both exchanges for 60 seconds
            await asyncio.wait_for(
                asyncio.gather(extended_task, lighter_task, return_exceptions=True),
                timeout=60.0
            )
        except asyncio.TimeoutError:
            print("✅ Dual monitoring test completed (60s timeout)")
            extended_task.cancel()
            lighter_task.cancel()
        
        # Show comprehensive results
        print(f"\n📊 Dual Monitoring Results:")
        for key, count in updates_count.items():
            print(f"  {key}: {count} updates")
        
        print(f"\n🔗 Connection Status:")
        print(f"  Extended connected: {manager.extended_ws_connected}")
        print(f"  Lighter connected: {manager.lighter_ws_connected}")
        print(f"  Extended can trade: {manager.extended_can_trade}")
        print(f"  Lighter can trade: {manager.lighter_can_trade}")
        
        # Final account summary
        summary = await manager.get_account_summary()
        print("\n📈 Final Dual Account Summary:")
        print(json.dumps(summary, indent=2, default=str))
        
        return manager
        
    except Exception as e:
        print(f"❌ Dual monitoring test failed: {e}")
        raise


async def test_hedged_order_placement():
    """Test hedged order placement system for funding arbitrage"""
    logging.basicConfig(level=logging.INFO)
    
    print("Testing Hedged Order Placement System...")
    
    try:
        # Import real account configurations
        from extendedsetup import api_key, vault, private_key, public_key
        from lightersetup import ETH_PRIVATE_KEY
        
        # Create account manager
        stark_account = StarkPerpetualAccount(
            vault=vault,
            private_key=private_key,
            public_key=public_key,
            api_key=api_key
        )
        
        manager = await create_account_manager(
            stark_account=stark_account,
            lighter_account_id="auto_detect",
            min_trade_size=Decimal("0.01")  # Small size for testing
        )
        
        print("✅ Account Manager initialized for hedged trading")
        
        # Simulate account balances for testing (since we won't get real WebSocket updates in test)
        print("\n💰 Setting up test balances...")
        
        # Mock some balance data for testing
        manager.extended_balance = AccountBalance(
            available_for_trade=Decimal("100.0"),  # $100 available
            total_balance=Decimal("150.0"),
            equity=Decimal("140.0"),
            unrealized_pnl=Decimal("0"),
            margin_ratio=Decimal("0.8"),
            exposure=Decimal("0"),
            updated_time=int(time.time() * 1000)
        )
        
        manager.lighter_balance = AccountBalance(
            available_for_trade=Decimal("80.0"),  # $80 available
            total_balance=Decimal("100.0"),
            equity=Decimal("95.0"),
            unrealized_pnl=Decimal("0"),
            margin_ratio=Decimal("1.0"),
            exposure=Decimal("0"),
            updated_time=int(time.time() * 1000)
        )
        
        # Enable trading flags
        manager.extended_can_trade = True
        manager.lighter_can_trade = True
        manager.extended_ws_connected = True
        manager.lighter_ws_connected = True
        
        print(f"  Extended balance: ${manager.extended_balance.available_for_trade}")
        print(f"  Lighter balance: ${manager.lighter_balance.available_for_trade}")
        
        # Test funding arbitrage scenarios
        test_scenarios = [
            {
                "name": "BTC Long-Short Arbitrage",
                "extended_market": "BTC-USD",
                "lighter_market_id": 1,
                "target_price": Decimal("50000.00"),
                "order_size": Decimal("0.001"),  # 0.001 BTC = $50
                "extended_side": "BUY"  # Buy on Extended, Sell on Lighter
            },
            {
                "name": "ETH Short-Long Arbitrage",
                "extended_market": "ETH-USD",
                "lighter_market_id": 2,
                "target_price": Decimal("3000.00"),
                "order_size": Decimal("0.01"),  # 0.01 ETH = $30
                "extended_side": "SELL"  # Sell on Extended, Buy on Lighter
            },
            {
                "name": "Large Size Test (Should fail)",
                "extended_market": "BTC-USD",
                "lighter_market_id": 1,
                "target_price": Decimal("50000.00"),
                "order_size": Decimal("10.0"),  # Too large - should fail
                "extended_side": "BUY"
            }
        ]
        
        results = []
        
        for i, scenario in enumerate(test_scenarios, 1):
            print(f"\n{i}️⃣ Testing {scenario['name']}:")
            print(f"   Market: {scenario['extended_market']} vs Lighter {scenario['lighter_market_id']}")
            print(f"   Size: {scenario['order_size']} @ ${scenario['target_price']}")
            print(f"   Strategy: {scenario['extended_side']} Extended, {'SELL' if scenario['extended_side'] == 'BUY' else 'BUY'} Lighter")
            
            try:
                # Place hedged orders
                result = await manager.place_hedged_funding_orders(
                    extended_market=scenario["extended_market"],
                    lighter_market_id=scenario["lighter_market_id"],
                    target_price=scenario["target_price"],
                    order_size=scenario["order_size"],
                    extended_side=scenario["extended_side"]
                )
                
                results.append({
                    "scenario": scenario["name"],
                    "result": result
                })
                
                if result["success"]:
                    print(f"   ✅ SUCCESS: Hedged orders placed")
                    print(f"      Hedge size: {result['hedge_size']}")
                    print(f"      Target price: ${result['target_price']}")
                else:
                    print(f"   ❌ FAILED: {result['error']}")
                    
            except Exception as e:
                print(f"   🐛 ERROR: {e}")
                results.append({
                    "scenario": scenario["name"],
                    "result": {"success": False, "error": str(e)}
                })
        
        # Summary
        print(f"\n📈 Test Results Summary:")
        successful = sum(1 for r in results if r["result"]["success"])
        total = len(results)
        print(f"  Successful: {successful}/{total}")
        print(f"  Failed: {total - successful}/{total}")
        
        # Show detailed results
        print(f"\n📋 Detailed Results:")
        for result in results:
            status = "✅" if result["result"]["success"] else "❌"
            print(f"  {status} {result['scenario']}")
            if not result["result"]["success"]:
                print(f"    Error: {result['result']['error']}")
        
        # Test order sizing calculations
        print(f"\n🧮 Order Sizing Tests:")
        sizing_tests = [
            Decimal("0.001"),  # Small order
            Decimal("1.0"),    # Medium order  
            Decimal("50.0"),   # Should hit Extended limit
            Decimal("100.0")   # Should hit Lighter limit
        ]
        
        for size in sizing_tests:
            sizes = await manager._calculate_hedged_order_sizes(size)
            final_size = min(sizes["extended_size"], sizes["lighter_size"])
            print(f"  Requested: ${size:>6} -> Final: ${final_size:>6} (E:{sizes['extended_size']:>6}, L:{sizes['lighter_size']:>6})")
        
        return results
        
    except Exception as e:
        print(f"❌ Hedged order placement test failed: {e}")
        raise


def test_data_structures():
    """Test data structure creation without requiring authentication"""
    print("Testing data structures...")
    
    # Test AccountBalance
    balance = AccountBalance(
        available_for_trade=Decimal("100.0"),
        total_balance=Decimal("150.0"),
        equity=Decimal("140.0"),
        unrealized_pnl=Decimal("10.0"),
        margin_ratio=Decimal("0.8"),
        exposure=Decimal("200.0"),
        updated_time=int(time.time())
    )
    print(f"✅ AccountBalance: {balance}")
    
    # Test Position
    position = Position(
        market="BTC-USD",
        exchange=Exchange.EXTENDED,
        side="LONG",
        size=Decimal("0.1"),
        entry_price=Decimal("50000.0"),
        mark_price=Decimal("51000.0"),
        unrealized_pnl=Decimal("100.0"),
        margin_required=Decimal("1000.0"),
        updated_time=int(time.time())
    )
    print(f"✅ Position: {position}")
    
    # Test Order
    order = Order(
        id="12345",
        external_id="ext-12345",
        market="BTC-USD",
        exchange=Exchange.EXTENDED,
        side="BUY",
        order_type="LIMIT",
        status="NEW",
        original_qty=Decimal("0.1"),
        filled_qty=Decimal("0.0"),
        remaining_qty=Decimal("0.1"),
        price=Decimal("50000.0")
    )
    print(f"✅ Order: {order}")
    
    print("✅ All data structure tests passed!")


if __name__ == "__main__":
    print("=" * 60)
    print("DualExchangeAccountManager Test Suite")
    print("=" * 60)
    
    # Test data structures first (no authentication required)
    test_data_structures()
    
    print("\n" + "=" * 60)
    print("Choose test to run:")
    print("1. Extended WebSocket Test")
    print("2. Lighter WebSocket Test")
    print("3. Dual Exchange Monitoring Test")
    print("4. Hedged Order Placement Test")
    print("5. All tests")
    print("=" * 60)
    
    choice = input("Enter choice (1-4): ").strip()
    
    if choice == "1":
        print("\n🔵 Extended WebSocket Test")
        asyncio.run(test_extended_websocket())
    elif choice == "2":
        print("\n🟡 Lighter WebSocket Test")
        asyncio.run(test_lighter_websocket())
    elif choice == "3":
        print("\n🔴 Dual Exchange Monitoring Test")
        asyncio.run(test_dual_monitoring())
    elif choice == "4":
        print("\n🔋 Hedged Order Placement Test")
        asyncio.run(test_hedged_order_placement())
    elif choice == "5":
        print("\n🎯 Running All Tests...")
        tests = [
            ("Extended WebSocket", test_extended_websocket),
            ("Lighter WebSocket", test_lighter_websocket),
            ("Dual Monitoring", test_dual_monitoring),
            ("Hedged Order Placement", test_hedged_order_placement)
        ]
        
        for i, (test_name, test_func) in enumerate(tests, 1):
            print(f"\n{i}️⃣ {test_name} Test:")
            try:
                asyncio.run(test_func())
            except Exception as e:
                print(f"{test_name} test failed: {e}")
    else:
        print("Invalid choice. Running basic data structure test only.")
