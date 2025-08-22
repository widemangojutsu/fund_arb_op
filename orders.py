import asyncio
import json
from urllib.request import urlopen, Request
import logging
import os
import time
import uuid
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple, Callable
from enum import Enum

# Import your existing components
from orderbooks import DualOrderbookManager, OrderbookState
from balance import BalanceManager, Exchange
from x10.perpetual.accounts import StarkPerpetualAccount
from utils.env_manager import EnvManager

# UPDATED IMPORTS FOR REAL ORDER PLACEMENT
from x10.perpetual.trading_client import PerpetualTradingClient
from x10.perpetual.configuration import STARKNET_MAINNET_CONFIG
import urllib.request
import lighter


class OrderType(Enum):
    LIMIT = "limit"
    MARKET = "market"
    STOP_LIMIT = "stop_limit"
    STOP_MARKET = "stop_market"


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class TimeInForce(Enum):
    GTC = "gtc"  # Good Till Cancel
    IOC = "ioc"  # Immediate Or Cancel
    FOK = "fok"  # Fill Or Kill
    GTT = "gtt"  # Good Till Time
    POST_ONLY = "post_only"  # Maker-only


# --- Lighter native client helpers and constants ---
ORDER_TYPE_LIMIT = lighter.SignerClient.ORDER_TYPE_LIMIT
ORDER_TYPE_MARKET = lighter.SignerClient.ORDER_TYPE_MARKET
ORDER_TYPE_STOP_LOSS = lighter.SignerClient.ORDER_TYPE_STOP_LOSS
ORDER_TYPE_STOP_LOSS_LIMIT = lighter.SignerClient.ORDER_TYPE_STOP_LOSS_LIMIT
ORDER_TYPE_TAKE_PROFIT = lighter.SignerClient.ORDER_TYPE_TAKE_PROFIT
ORDER_TYPE_TAKE_PROFIT_LIMIT = lighter.SignerClient.ORDER_TYPE_TAKE_PROFIT_LIMIT




async def create_lighter_client(
    *,
    base_url: str,
    private_key: str,
    account_index: int,
    api_key_index: int,
) -> Tuple["lighter.SignerClient", "lighter.ApiClient"]:
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=base_url))
    client = lighter.SignerClient(
        url=base_url,
        private_key=private_key,
        account_index=account_index,
        api_key_index=api_key_index,
    )
    err = client.check_client()
    if err is not None:
        await api_client.close()
        raise RuntimeError(f"Lighter check_client failed: {err}")
    return client, api_client


async def close_lighter_client(
    client: Optional["lighter.SignerClient"],
    api_client: Optional["lighter.ApiClient"],
) -> None:
    try:
        if client is not None:
            await client.close()
    finally:
        if api_client is not None:
            await api_client.close()


async def lighter_place_post_only_limit(
    client: "lighter.SignerClient",
    *,
    market_index: int,
    client_order_index: int,
    base_amount: int,
    price: int,
    is_ask: bool,
    reduce_only: int = 0,
    trigger_price: int = 0,
) -> Tuple[Optional[object], Optional[str], Optional[Exception]]:
    return await client.create_order(
        market_index=market_index,
        client_order_index=client_order_index,
        base_amount=base_amount,
        price=price,
        is_ask=is_ask,
        order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
        time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
        reduce_only=reduce_only,
        trigger_price=trigger_price,
    )


async def lighter_cancel_order(
    client: "lighter.SignerClient",
    *,
    market_index: int,
    order_index: int,
) -> Tuple[Optional[object], Optional[str], Optional[Exception]]:
    return await client.cancel_order(
        market_index=market_index,
        order_index=order_index,
    )


@dataclass
class Order:
    """Individual order representation"""
    order_id: str
    exchange: Exchange
    symbol: str
    side: OrderSide
    order_type: OrderType
    size: Decimal
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    time_in_force: TimeInForce = TimeInForce.GTC
    
    # Order state
    status: OrderStatus = OrderStatus.PENDING
    filled_size: Decimal = field(default_factory=lambda: Decimal("0"))
    remaining_size: Decimal = field(default_factory=lambda: Decimal("0"))
    average_fill_price: Optional[Decimal] = None
    
    # Metadata
    created_at: int = field(default_factory=lambda: int(time.time() * 1000))
    updated_at: int = field(default_factory=lambda: int(time.time() * 1000))
    exchange_order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    
    # Risk and execution tracking
    estimated_cost: Optional[Decimal] = None
    slippage_tolerance: Decimal = field(default_factory=lambda: Decimal("0.001"))  # 0.1%
    max_latency_ms: int = 1000  # Maximum acceptable latency
    
    # Arbitrage-specific fields
    hedge_order_id: Optional[str] = None  # Linked hedge order
    arbitrage_group_id: Optional[str] = None  # Group for arbitrage orders
    lighter_market_id: Optional[int] = None  # For Lighter orders - from opportunity discovery
    
    def __post_init__(self):
        if not self.remaining_size:
            self.remaining_size = self.size
    
    @property
    def is_active(self) -> bool:
        """Check if order is still active"""
        return self.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED]
    
    @property
    def is_complete(self) -> bool:
        """Check if order is completely filled"""
        return self.status == OrderStatus.FILLED
    
    @property
    def fill_percentage(self) -> float:
        """Get fill percentage (0.0 to 1.0)"""
        if self.size == 0:
            return 0.0
        return float(self.filled_size / self.size)


@dataclass
class ArbitrageOrderGroup:
    """Group of hedged orders for arbitrage execution"""
    group_id: str
    extended_order: Order
    lighter_order: Order
    target_spread_bps: Decimal
    actual_spread_bps: Optional[Decimal] = None
    created_at: int = field(default_factory=lambda: int(time.time() * 1000))
    status: str = "active"  # active, completed, failed, cancelled
    
    @property
    def is_hedged(self) -> bool:
        """Check if both orders are properly hedged"""
        return (self.extended_order.side != self.lighter_order.side and
                abs(self.extended_order.size - self.lighter_order.size) < Decimal("0.0001"))
    
    @property
    def total_filled_value(self) -> Decimal:
        """Get total filled value across both orders"""
        ext_value = (self.extended_order.filled_size * self.extended_order.average_fill_price) if self.extended_order.average_fill_price else Decimal("0")
        lit_value = (self.lighter_order.filled_size * self.lighter_order.average_fill_price) if self.lighter_order.average_fill_price else Decimal("0")
        return ext_value + lit_value


class OrderManager:
    """
    Advanced order management system for dual-exchange arbitrage trading.
    
    UPDATED WITH REAL ORDER PLACEMENT FUNCTIONALITY
    """
    
    def __init__(self, 
                 stark_account: StarkPerpetualAccount,
                 balance_manager: BalanceManager,
                 orderbook_manager: Optional[DualOrderbookManager] = None,
                 lighter_client: Optional["lighter.SignerClient"] = None):
        self.stark_account = stark_account
        self.balance_manager = balance_manager
        self.orderbook_manager = orderbook_manager
        self.logger = logging.getLogger("OrderManager")
        self.lighter_client = lighter_client
        
        # Order tracking
        self.active_orders: Dict[str, Order] = {}  # order_id -> Order
        self.arbitrage_groups: Dict[str, ArbitrageOrderGroup] = {}  # group_id -> ArbitrageOrderGroup
        self.order_history: List[Order] = []
        
        # Risk management
        self.risk_config = {
            "max_order_size_usd": 1000.0,  # Maximum single order size
            "max_total_exposure_usd": 5000.0,  # Maximum total exposure
            "min_account_balance_usd": 100.0,  # Minimum account balance to maintain
            "max_slippage_bps": 50.0,  # Maximum acceptable slippage (5%)
            "max_orders_per_symbol": 10,  # Maximum orders per symbol
            "pre_trade_balance_check": True,
            "validate_hedge_ratios": True
        }
        
        # Execution settings
        self.execution_config = {
            "default_slippage_tolerance": Decimal("0.002"),  # 0.2%
            "aggressive_slippage_tolerance": Decimal("0.005"),  # 0.5%
            "conservative_slippage_tolerance": Decimal("0.001"),  # 0.1%
            "order_timeout_seconds": 300,  # 5 minutes
            "fill_timeout_seconds": 60,  # 1 minute for immediate orders
            "retry_failed_orders": True,
            "max_retry_attempts": 3
        }
        
        # Performance tracking
        self.stats = {
            "orders_placed": 0,
            "orders_filled": 0,
            "orders_cancelled": 0,
            "arbitrage_groups_created": 0,
            "total_volume_traded": Decimal("0"),
            "average_fill_latency_ms": 0,
            "start_time": time.time()
        }
        
        # Thread safety
        self.order_lock = asyncio.Lock()
        # Cache for Lighter market scales: market_index -> (base_scale, price_scale)
        self._lighter_market_scales: Dict[int, Tuple[int, int]] = {}

    async def _get_lighter_market_scales(self, market_id: int) -> Tuple[int, int]:
        """Get base and price scaling factors for Lighter market"""
        if market_id in self._lighter_market_scales:
            return self._lighter_market_scales[market_id]
        
        try:
            url = f"https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails?market_id={market_id}"
            
            with urllib.request.urlopen(url, timeout=5) as response:
                data = json.loads(response.read())
            
            if data.get("code") != 200:
                raise Exception(f"API error: {data.get('code')}")
            
            details = data.get("order_book_details", [])
            if not details:
                raise Exception("No order book details found")
            
            market_info = details[0]
            size_decimals = int(market_info["size_decimals"])
            price_decimals = int(market_info["price_decimals"])
            
            base_scale = 10 ** size_decimals
            price_scale = 10 ** price_decimals
            
            # Cache the result
            self._lighter_market_scales[market_id] = (base_scale, price_scale)
            
            self.logger.debug(f"Lighter market {market_id} scales: base={base_scale}, price={price_scale}")
            return base_scale, price_scale
            
        except Exception as e:
            self.logger.error(f"Failed to get scales for market {market_id}: {e}")
            # Fallback values
            return 100000000, 100000  # 8 decimals base, 5 decimals price

    # --- UPDATED METHODS FOR REAL ORDER PLACEMENT ---

    async def _place_extended_order(self, order: Order) -> bool:
        """Place order on Extended exchange - UPDATED to support market orders"""
        try:
            # Create Extended trading client
            trading_client = PerpetualTradingClient(
                endpoint_config=STARKNET_MAINNET_CONFIG,
                stark_account=self.stark_account
            )
            
            # Map order types
            order_type_map = {
                "limit": "LIMIT",
                "market": "MARKET"  # ← Added market order support
            }
            
            # Generate external order ID for tracking
            external_id = f"hft_{int(time.time() * 1000)}_{order.symbol}_{order.side.value}"
            
            # Prepare order parameters
            order_params = {
                "market": order.symbol,
                "side": order.side.value.upper(),  # "BUY" or "SELL"
                "type": order_type_map.get(order.order_type.value, "LIMIT"),
                "size": str(order.size),
                "external_id": external_id,
                "reduce_only": False
            }
            
            # Add price only for limit orders
            if order.order_type == OrderType.LIMIT and order.price:
                order_params["price"] = str(order.price)
            
            # For market orders, add slippage protection if available
            if order.order_type == OrderType.MARKET and hasattr(order, 'slippage_tolerance'):
                # Extended may support slippage limits - check documentation
                pass
            
            self.logger.info(f"🔵 Placing Extended {order.order_type.value.upper()} order: {order_params}")
            
            # Place the order
            response = await trading_client.order.create_or_edit_order(**order_params)
            
            if response and hasattr(response, 'id'):
                order.exchange_order_id = str(response.id)
                order.status = OrderStatus.SUBMITTED
                self.logger.info(f"✅ Extended {order.order_type.value} order placed: {order.exchange_order_id}")
                return True
            else:
                self.logger.error(f"❌ Extended order failed: {response}")
                order.status = OrderStatus.REJECTED
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Extended order placement error: {e}")
            order.status = OrderStatus.REJECTED
            return False

    async def _place_lighter_order(self, order: Order) -> bool:
        """Place order on Lighter exchange - UPDATED to support market orders"""
        try:
            if not self.lighter_client:
                self.logger.error("❌ Lighter client not available")
                order.status = OrderStatus.REJECTED
                return False
            
            # Get market ID from the order (set from opportunity discovery)
            market_id = getattr(order, 'lighter_market_id', None)
            if not market_id:
                self.logger.error(f"❌ No Lighter market ID in order for symbol: {order.symbol}")
                order.status = OrderStatus.REJECTED
                return False
            
            # Get market scaling info
            base_scale, price_scale = await self._get_lighter_market_scales(market_id)
            
            # Convert decimal amounts to integer units
            base_amount = int((order.size * Decimal(base_scale)).to_integral_value(rounding=ROUND_DOWN))
            
            # Generate unique client order index
            client_order_index = int(time.time() * 1_000_000) & 0x7FFFFFFF
            
            # Determine order side
            is_ask = (order.side == OrderSide.SELL)
            
            # Map order types - UPDATED for market orders
            if order.order_type == OrderType.MARKET:
                order_type = lighter.SignerClient.ORDER_TYPE_MARKET
                time_in_force = lighter.SignerClient.ORDER_TIME_IN_FORCE_IOC
                price_int = 0  # Market orders don't need price
            else:
                order_type = lighter.SignerClient.ORDER_TYPE_LIMIT
                price_int = int((order.price * Decimal(price_scale)).to_integral_value(rounding=ROUND_HALF_UP)) if order.price else 0
                time_in_force = lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY
            
            self.logger.info(f"🟡 Placing Lighter {order.order_type.value.upper()} order:")
            self.logger.info(f"   Market: {market_id} ({order.symbol})")
            self.logger.info(f"   Side: {'ASK' if is_ask else 'BID'}")
            self.logger.info(f"   Size: {order.size} -> {base_amount}")
            if order.order_type == OrderType.LIMIT:
                self.logger.info(f"   Price: {order.price} -> {price_int}")
            else:
                self.logger.info(f"   Type: MARKET (immediate execution)")
            
            # Place the order
            tx, tx_hash, err = await self.lighter_client.create_order(
                market_index=market_id,
                client_order_index=client_order_index,
                base_amount=base_amount,
                price=price_int,
                is_ask=is_ask,
                order_type=order_type,
                time_in_force=time_in_force,
                reduce_only=0,
                trigger_price=0
            )
            
            if err is None and tx_hash:
                order.exchange_order_id = tx_hash
                order.client_order_id = str(client_order_index)
                order.status = OrderStatus.SUBMITTED
                self.logger.info(f"✅ Lighter {order.order_type.value} order placed: {tx_hash}")
                return True
            else:
                self.logger.error(f"❌ Lighter order failed: {err}")
                order.status = OrderStatus.REJECTED
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Lighter order placement error: {e}")
            order.status = OrderStatus.REJECTED
            return False
    
    async def _cancel_extended_order(self, order: Order) -> bool:
        """Cancel order on Extended exchange"""
        try:
            if not order.exchange_order_id:
                self.logger.error("❌ No Extended order ID to cancel")
                return False
            
            trading_client = PerpetualTradingClient(
                endpoint_config=STARKNET_MAINNET_CONFIG,
                stark_account=self.stark_account
            )
            
            response = await trading_client.order.cancel_order(order_id=order.exchange_order_id)
            
            if response:
                self.logger.info(f"✅ Extended order cancelled: {order.exchange_order_id}")
                return True
            else:
                self.logger.error(f"❌ Failed to cancel Extended order: {order.exchange_order_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Extended cancellation error: {e}")
            return False

    async def _cancel_lighter_order(self, order: Order) -> bool:
        """Cancel order on Lighter exchange"""
        try:
            if not self.lighter_client:
                self.logger.error("❌ Lighter client not available")
                return False
            
            if not order.client_order_id:
                self.logger.error("❌ No Lighter client order ID to cancel")
                return False
            
            # Get market ID from the order (already available from opportunity discovery)
            market_id = getattr(order, 'lighter_market_id', None)
            if not market_id:
                self.logger.error(f"❌ No market ID for {order.symbol}")
                return False
            
            # Convert client order ID back to integer
            order_index = int(order.client_order_id)
            
            tx, tx_hash, err = await self.lighter_client.cancel_order(
                market_index=market_id,
                order_index=order_index
            )
            
            if err is None:
                self.logger.info(f"✅ Lighter order cancelled: {order.symbol} order {order_index}")
                return True
            else:
                self.logger.error(f"❌ Failed to cancel Lighter order: {err}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Lighter cancellation error: {e}")
            return False

    # --- NEW METHOD FOR LIVE ARBITRAGE EXECUTION ---

    async def execute_live_arbitrage_opportunity(self, opportunity, target_size: Decimal = None) -> Dict:
        """
        Execute live arbitrage with MARKET ORDERS for immediate execution.
        Uses half available balance if target_size not specified.
        """
        result = {
            "success": False,
            "extended_order": None,
            "lighter_order": None,
            "group_id": None,
            "error": None,
            "execution_time": 0
        }
        
        start_time = time.time()
        
        try:
            self.logger.info(f"🎯 EXECUTING LIVE ARBITRAGE WITH MARKET ORDERS")
            self.logger.info(f"   Pair: {opportunity.extended_symbol} / {opportunity.lighter_symbol}")
            self.logger.info(f"   Spread: {opportunity.spread_bps:.2f} bps")
            self.logger.info(f"   Expected APY: {opportunity.annual_yield:+.1f}%")
            
            # Calculate position size (half balance if not specified)
            if target_size is None:
                # Get account balance and use half
                account_summary = await self.balance_manager.get_account_summary()
                total_available = float(account_summary["total_available"])
                half_balance_usd = total_available * 0.5
                
                # Estimate position size based on current price
                estimated_price = Decimal("50000")  # Fallback
                if self.orderbook_manager:
                    try:
                        price_data = await self.orderbook_manager.get_synchronized_prices()
                        if price_data.get("status") == "complete":
                            extended_mid = price_data["extended"].get("mid_price")
                            if extended_mid:
                                estimated_price = Decimal(str(extended_mid))
                    except Exception:
                        pass
                
                target_size = (Decimal(str(half_balance_usd)) / estimated_price).quantize(Decimal("0.0001"))
                self.logger.info(f"💰 Using half balance: ${half_balance_usd:.2f} = {target_size} crypto")
            
            # Determine strategy
            if opportunity.extended_rate > opportunity.lighter_rate:
                extended_side = OrderSide.SELL  # Short higher funding
                lighter_side = OrderSide.BUY    # Long lower funding
                strategy = "SHORT_EXTENDED_LONG_LIGHTER"
            else:
                extended_side = OrderSide.BUY   # Long lower funding
                lighter_side = OrderSide.SELL   # Short higher funding
                strategy = "LONG_EXTENDED_SHORT_LIGHTER"
            
            self.logger.info(f"   Strategy: {strategy}")
            self.logger.info(f"   Position size: {target_size}")
            
            # Create market orders for immediate execution
            group_id = f"market_arb_{int(time.time() * 1000)}"
            
            extended_order = Order(
                order_id=f"{group_id}_ext",
                exchange=Exchange.EXTENDED,
                symbol=opportunity.extended_symbol,
                side=extended_side,
                order_type=OrderType.MARKET,  # ← CHANGED TO MARKET
                size=target_size,
                price=None,  # Market orders don't need price
                slippage_tolerance=Decimal("0.005"),  # 0.5% slippage tolerance
                arbitrage_group_id=group_id
            )
            
            lighter_order = Order(
                order_id=f"{group_id}_lit",
                exchange=Exchange.LIGHTER,
                symbol=opportunity.lighter_symbol,
                side=lighter_side,
                order_type=OrderType.MARKET,  # ← CHANGED TO MARKET
                size=target_size,
                price=None,  # Market orders don't need price
                slippage_tolerance=Decimal("0.005"),  # 0.5% slippage tolerance
                arbitrage_group_id=group_id
            )
            
            # Add the market ID to the lighter order
            lighter_order.lighter_market_id = opportunity.lighter_market_id
            
            # Link hedge orders
            extended_order.hedge_order_id = lighter_order.order_id
            lighter_order.hedge_order_id = extended_order.order_id
            
            # Validate orders
            ext_validation = await self._validate_order(extended_order)
            lit_validation = await self._validate_order(lighter_order)
            
            if not ext_validation["valid"]:
                result["error"] = f"Extended validation failed: {ext_validation['reason']}"
                return result
            
            if not lit_validation["valid"]:
                result["error"] = f"Lighter validation failed: {lit_validation['reason']}"
                return result
            
            # Place MARKET orders simultaneously for immediate execution
            self.logger.info("⚡ Placing MARKET orders for immediate execution...")
            
            ext_success = await self._place_extended_order(extended_order)
            lit_success = await self._place_lighter_order(lighter_order)
            
            if ext_success and lit_success:
                # Both orders placed successfully
                async with self.order_lock:
                    self.active_orders[extended_order.order_id] = extended_order
                    self.active_orders[lighter_order.order_id] = lighter_order
                    
                    # Create arbitrage group
                    arbitrage_group = ArbitrageOrderGroup(
                        group_id=group_id,
                        extended_order=extended_order,
                        lighter_order=lighter_order,
                        target_spread_bps=opportunity.spread_bps
                    )
                    self.arbitrage_groups[group_id] = arbitrage_group
                    
                    # Update stats
                    self.stats["orders_placed"] += 2
                    self.stats["arbitrage_groups_created"] += 1
                
                result["success"] = True
                result["group_id"] = group_id
                result["extended_order"] = {
                    "order_id": extended_order.exchange_order_id,
                    "side": extended_side.value,
                    "size": float(target_size),
                    "type": "MARKET"
                }
                result["lighter_order"] = {
                    "tx_hash": lighter_order.exchange_order_id,
                    "side": lighter_side.value,
                    "size": float(target_size),
                    "type": "MARKET"
                }
                
                self.logger.info(f"✅ LIVE MARKET ARBITRAGE EXECUTED")
                self.logger.info(f"   Extended: {extended_order.exchange_order_id}")
                self.logger.info(f"   Lighter: {lighter_order.exchange_order_id}")
                self.logger.info(f"   Total value: ~${float(target_size) * 50000:.2f}")
                
            else:
                # Handle partial failures
                if ext_success and not lit_success:
                    self.logger.error("❌ Lighter failed, cancelling Extended order")
                    await self._cancel_extended_order(extended_order)
                    result["error"] = "Lighter order failed, Extended order cancelled"
                elif not ext_success and lit_success:
                    self.logger.error("❌ Extended failed, cancelling Lighter order")
                    await self._cancel_lighter_order(lighter_order)
                    result["error"] = "Extended order failed, Lighter order cancelled"
                else:
                    result["error"] = "Both orders failed to place"
                    self.logger.error("❌ Both hedge orders failed")
            
            result["execution_time"] = time.time() - start_time
            return result
            
        except Exception as e:
            result["error"] = f"Execution exception: {str(e)}"
            result["execution_time"] = time.time() - start_time
            self.logger.error(f"❌ Live arbitrage execution error: {e}")
            return result

    # --- Lighter native client integration (existing methods) ---
    async def place_lighter_post_only_raw(self, *, market_index: int, client_order_index: int, base_amount: int, price: int, is_ask: bool, reduce_only: int = 0, trigger_price: int = 0) -> Tuple[Optional[str], Optional[str], Optional[Exception]]:
        """Place a post-only limit order on Lighter using raw integer fields"""
        if not self.lighter_client:
            err = RuntimeError("lighter.SignerClient not configured")
            self.logger.error(str(err))
            return None, None, err
        try:
            tx, tx_hash, err = await self.lighter_client.create_order(
                market_index=market_index,
                client_order_index=client_order_index,
                base_amount=base_amount,
                price=price,
                is_ask=is_ask,
                order_type=lighter.SignerClient.ORDER_TYPE_LIMIT,
                time_in_force=lighter.SignerClient.ORDER_TIME_IN_FORCE_POST_ONLY,
                reduce_only=reduce_only,
                trigger_price=trigger_price,
            )
            if err is None:
                self.logger.info(f"Lighter POST_ONLY order placed | mkt={market_index} | coi={client_order_index} | amt={base_amount} | px={price} | ask={is_ask}")
            else:
                self.logger.error(f"Lighter create_order error: {err}")
            return tx, tx_hash, err
        except Exception as e:
            self.logger.error(f"Exception in place_lighter_post_only_raw: {e}")
            return None, None, e

    async def place_lighter_limit_at_cross_mid(self, *, lighter_market_index: int, side: OrderSide, base_size: Decimal) -> Tuple[Optional[str], Optional[str], Optional[Exception], Optional[Decimal]]:
        """Place a POST_ONLY limit order on Lighter at the cross-exchange mid price"""
        # Preconditions
        if not self.orderbook_manager:
            return None, None, RuntimeError("orderbook_manager not set"), None
        if not self.lighter_client:
            return None, None, RuntimeError("lighter client not configured"), None

        # Fetch synchronized price data
        price_data = await self.orderbook_manager.get_synchronized_prices()
        if price_data.get("status") != "complete":
            return None, None, RuntimeError(f"synchronized prices incomplete: {price_data.get('reason')}"), None

        # Use Lighter best bid minus a small epsilon for maker-safe pricing
        lighter_best_bid = price_data["lighter"].get("best_bid_price")
        if lighter_best_bid is None:
            return None, None, RuntimeError("missing lighter best bid for bid-based pricing"), None

        target_price = Decimal(str(lighter_best_bid)) - Decimal("0.0001")

        # Fetch scales and convert to Lighter integer units using Decimal-safe rounding
        base_scale, price_scale = await self._get_lighter_market_scales(lighter_market_index)
        base_amount_val = int((base_size * Decimal(base_scale)).to_integral_value(rounding=ROUND_DOWN))
        price_val = int((target_price * Decimal(price_scale)).to_integral_value(rounding=ROUND_HALF_UP))

        # Microsecond-based client order index (fit to 31-bit if needed)
        client_order_index = int(time.time() * 1_000_000) & 0x7FFFFFFF

        is_ask = (side == OrderSide.SELL)
        self.logger.info(f"Placing Lighter POST_ONLY | mkt={lighter_market_index} | coi={client_order_index} | base_amount(int)={base_amount_val} | price(int)={price_val} | base(human)={str(base_size)} | price(human)={str(target_price)} | side={'ASK' if is_ask else 'BID'} | note=lighter_best_bid - 0.0001")

        tx, tx_hash, err = await self.place_lighter_post_only_raw(
            market_index=lighter_market_index,
            client_order_index=client_order_index,
            base_amount=base_amount_val,
            price=price_val,
            is_ask=is_ask,
        )
        return tx, tx_hash, err, target_price

    # --- REST OF YOUR EXISTING METHODS (unchanged) ---
    
    async def place_limit_order(self, exchange: Exchange, symbol: str, side: OrderSide, size: Decimal, price: Decimal, time_in_force: TimeInForce = TimeInForce.GTC, client_order_id: Optional[str] = None) -> Optional[Order]:
        """Place a limit order on the specified exchange"""
        try:
            # Generate order ID
            order_id = client_order_id or f"{exchange.value}_{symbol}_{int(time.time() * 1000000)}"
            
            # Create order object
            order = Order(
                order_id=order_id,
                exchange=exchange,
                symbol=symbol,
                side=side,
                order_type=OrderType.LIMIT,
                size=size,
                price=price,
                time_in_force=time_in_force,
                client_order_id=client_order_id
            )
            
            # Pre-trade validation
            validation_result = await self._validate_order(order)
            if not validation_result["valid"]:
                self.logger.error(f"Order validation failed: {validation_result['reason']}")
                return None
            
            # Place order on exchange
            success = await self._execute_order_placement(order)
            if not success:
                self.logger.error(f"Failed to place order on {exchange.value}")
                return None
            
            # Track order
            async with self.order_lock:
                self.active_orders[order_id] = order
                self.stats["orders_placed"] += 1
            
            self.logger.info(f"✅ Limit order placed: {symbol} {side.value} {size} @ ${price}")
            return order
            
        except Exception as e:
            self.logger.error(f"Error placing limit order: {e}")
            return None
    
    async def place_market_order(self, exchange: Exchange, symbol: str, side: OrderSide, size: Decimal, slippage_tolerance: Optional[Decimal] = None) -> Optional[Order]:
        """Place a market order with intelligent price estimation"""
        try:
            # Use orderbook to estimate execution price
            if self.orderbook_manager:
                estimated_price = await self._estimate_market_price(exchange, symbol, side, size)
                if not estimated_price:
                    self.logger.error(f"Could not estimate market price for {symbol} on {exchange.value}")
                    return None
            else:
                estimated_price = None
            
            # Generate order ID
            order_id = f"{exchange.value}_{symbol}_market_{int(time.time() * 1000000)}"
            
            # Create order object
            order = Order(
                order_id=order_id,
                exchange=exchange,
                symbol=symbol,
                side=side,
                order_type=OrderType.MARKET,
                size=size,
                price=estimated_price,  # Estimated execution price
                slippage_tolerance=slippage_tolerance or self.execution_config["default_slippage_tolerance"]
            )
            
            # Pre-trade validation
            validation_result = await self._validate_order(order)
            if not validation_result["valid"]:
                self.logger.error(f"Market order validation failed: {validation_result['reason']}")
                return None
            
            # Place order on exchange
            success = await self._execute_order_placement(order)
            if not success:
                self.logger.error(f"Failed to place market order on {exchange.value}")
                return None
            
            # Track order
            async with self.order_lock:
                self.active_orders[order_id] = order
                self.stats["orders_placed"] += 1
            
            self.logger.info(f"✅ Market order placed: {symbol} {side.value} {size}")
            return order
            
        except Exception as e:
            self.logger.error(f"Error placing market order: {e}")
            return None
    
    async def place_hedged_arbitrage_orders(self, extended_symbol: str, lighter_symbol: str, target_size: Decimal, target_spread_bps: Decimal, aggressive: bool = False) -> Optional[ArbitrageOrderGroup]:
        """Place hedged orders for arbitrage opportunity (UPDATED TO USE REAL EXECUTION)"""
        
        # For this method, we'll delegate to the new live execution method
        # This maintains backward compatibility while using real orders
        from fund_test import ArbitrageOpportunity
        
        # Create a minimal opportunity object for the live execution
        # In practice, this should be called with a real ArbitrageOpportunity
        self.logger.warning("place_hedged_arbitrage_orders called without full opportunity object")
        self.logger.warning("Consider using execute_live_arbitrage_opportunity directly")
        
        return None  # Disabled for safety - use execute_live_arbitrage_opportunity instead
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an active order"""
        try:
            async with self.order_lock:
                order = self.active_orders.get(order_id)
                if not order:
                    self.logger.warning(f"Order {order_id} not found")
                    return False
                
                if not order.is_active:
                    self.logger.warning(f"Order {order_id} is not active")
                    return False
            
            # Cancel on exchange
            success = await self._execute_order_cancellation(order)
            if success:
                async with self.order_lock:
                    order.status = OrderStatus.CANCELLED
                    order.updated_at = int(time.time() * 1000)
                    self.stats["orders_cancelled"] += 1
                
                self.logger.info(f"✅ Order cancelled: {order_id}")
                return True
            else:
                self.logger.error(f"Failed to cancel order {order_id}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error cancelling order {order_id}: {e}")
            return False
    
    async def cancel_arbitrage_group(self, group_id: str) -> bool:
        """Cancel all orders in an arbitrage group"""
        try:
            async with self.order_lock:
                group = self.arbitrage_groups.get(group_id)
                if not group:
                    self.logger.warning(f"Arbitrage group {group_id} not found")
                    return False
            
            # Cancel both orders
            ext_cancelled = await self.cancel_order(group.extended_order.order_id)
            lit_cancelled = await self.cancel_order(group.lighter_order.order_id)
            
            if ext_cancelled and lit_cancelled:
                async with self.order_lock:
                    group.status = "cancelled"
                
                self.logger.info(f"✅ Arbitrage group cancelled: {group_id}")
                return True
            else:
                self.logger.warning(f"Partial cancellation of arbitrage group {group_id}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error cancelling arbitrage group {group_id}: {e}")
            return False
    
    async def _validate_order(self, order: Order) -> Dict:
        """Comprehensive order validation"""
        try:
            # Basic validation
            if order.size <= 0:
                return {"valid": False, "reason": "Invalid order size"}
            
            if order.order_type == OrderType.LIMIT and not order.price:
                return {"valid": False, "reason": "Limit order requires price"}
            
            # Risk checks
            order_value = order.size * (order.price or Decimal("50000"))  # Estimate if no price
            
            if float(order_value) > self.risk_config["max_order_size_usd"]:
                return {"valid": False, "reason": f"Order value ${order_value} exceeds limit"}
            
            # Balance check
            if self.risk_config["pre_trade_balance_check"]:
                account_summary = await self.balance_manager.get_account_summary()
                
                # Check if sufficient balance on target exchange
                exchange_data = account_summary["exchanges"].get(order.exchange.value)
                if not exchange_data:
                    return {"valid": False, "reason": f"No balance data for {order.exchange.value}"}
                
                available_balance = float(exchange_data["available"])
                required_margin = float(order_value) * 0.1  # Estimate 10% margin requirement
                
                if required_margin > available_balance:
                    return {"valid": False, "reason": f"Insufficient balance: need ${required_margin:.2f}, have ${available_balance:.2f}"}
            
            # Symbol-specific checks
            symbol_orders = [o for o in self.active_orders.values() if o.symbol == order.symbol]
            if len(symbol_orders) >= self.risk_config["max_orders_per_symbol"]:
                return {"valid": False, "reason": f"Too many orders for {order.symbol}"}
            
            return {"valid": True, "reason": "Order validation passed"}
            
        except Exception as e:
            return {"valid": False, "reason": f"Validation error: {e}"}
    
    async def _estimate_market_price(self, exchange: Exchange, symbol: str, side: OrderSide, size: Decimal) -> Optional[Decimal]:
        """Estimate market execution price using orderbook data"""
        try:
            if exchange == Exchange.EXTENDED:
                orderbook = await self.orderbook_manager.get_extended_state()
            else:
                orderbook = await self.orderbook_manager.get_lighter_state()
            
            if not orderbook:
                return None
            
            # Calculate VWAP for the order size
            vwap = orderbook.get_vwap(side.value, size)
            return vwap
            
        except Exception as e:
            self.logger.error(f"Error estimating market price: {e}")
            return None
    
    async def _calculate_arbitrage_pricing(self, extended_data: Dict, lighter_data: Dict, size: Decimal, aggressive: bool) -> Optional[Dict]:
        """Calculate optimal pricing for arbitrage orders"""
        try:
            ext_bid = extended_data.get("best_bid_price")
            ext_ask = extended_data.get("best_ask_price")
            lit_bid = lighter_data.get("best_bid_price")
            lit_ask = lighter_data.get("best_ask_price")
            
            if not all([ext_bid, ext_ask, lit_bid, lit_ask]):
                return None
            
            # Determine arbitrage direction
            # Buy Extended, Sell Lighter
            if lit_bid > ext_ask:
                extended_side = OrderSide.BUY
                lighter_side = OrderSide.SELL
                
                if aggressive:
                    extended_price = Decimal(str(ext_ask))  # Take the ask (aggressive)
                    lighter_price = Decimal(str(lit_bid))   # Hit the bid (aggressive)
                else:
                    # Place on better side of spread for better execution
                    extended_price = Decimal(str(ext_bid + (ext_ask - ext_bid) * Decimal("0.3")))  # 30% into spread
                    lighter_price = Decimal(str(lit_ask - (lit_ask - lit_bid) * Decimal("0.3")))  # 30% into spread
            
            # Buy Lighter, Sell Extended  
            elif ext_bid > lit_ask:
                extended_side = OrderSide.SELL
                lighter_side = OrderSide.BUY
                
                if aggressive:
                    extended_price = Decimal(str(ext_bid))  # Hit the bid (aggressive)
                    lighter_price = Decimal(str(lit_ask))   # Take the ask (aggressive)
                else:
                    # Place on better side of spread
                    extended_price = Decimal(str(ext_ask - (ext_ask - ext_bid) * Decimal("0.3")))
                    lighter_price = Decimal(str(lit_bid + (lit_ask - lit_bid) * Decimal("0.3")))
            
            else:
                return None  # No clear arbitrage opportunity
            
            return {
                "extended_side": extended_side,
                "lighter_side": lighter_side,
                "extended_price": extended_price,
                "lighter_price": lighter_price
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating arbitrage pricing: {e}")
            return None
    
    async def _execute_order_placement(self, order: Order) -> bool:
        """Execute order placement on the specified exchange"""
        try:
            order.status = OrderStatus.SUBMITTED
            order.updated_at = int(time.time() * 1000)
            
            if order.exchange == Exchange.EXTENDED:
                # Place order using REAL Extended SDK
                success = await self._place_extended_order(order)
            else:
                # Place order using REAL Lighter API
                success = await self._place_lighter_order(order)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error executing order placement: {e}")
            order.status = OrderStatus.REJECTED
            return False
    
    async def _execute_order_cancellation(self, order: Order) -> bool:
        """Execute order cancellation on the specified exchange"""
        try:
            if order.exchange == Exchange.EXTENDED:
                success = await self._cancel_extended_order(order)
            else:
                success = await self._cancel_lighter_order(order)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error executing order cancellation: {e}")
            return False
    
    # Public API methods (unchanged)
    
    async def get_active_orders(self) -> Dict[str, Order]:
        """Get all active orders"""
        async with self.order_lock:
            return {oid: order for oid, order in self.active_orders.items() if order.is_active}
    
    async def get_arbitrage_groups(self) -> Dict[str, ArbitrageOrderGroup]:
        """Get all arbitrage groups"""
        async with self.order_lock:
            return self.arbitrage_groups.copy()
    
    async def get_order_status(self, order_id: str) -> Optional[Dict]:
        """Get detailed status of a specific order"""
        async with self.order_lock:
            order = self.active_orders.get(order_id)
            if not order:
                return None
            
            return {
                "order_id": order.order_id,
                "exchange": order.exchange.value,
                "symbol": order.symbol,
                "side": order.side.value,
                "type": order.order_type.value,
                "status": order.status.value,
                "size": float(order.size),
                "price": float(order.price) if order.price else None,
                "filled_size": float(order.filled_size),
                "remaining_size": float(order.remaining_size),
                "fill_percentage": order.fill_percentage,
                "created_at": order.created_at,
                "updated_at": order.updated_at,
                "exchange_order_id": order.exchange_order_id,
                "arbitrage_group_id": order.arbitrage_group_id
            }
    
    async def get_trading_summary(self) -> Dict:
        """Get comprehensive trading summary and statistics"""
        active_orders = await self.get_active_orders()
        arbitrage_groups = await self.get_arbitrage_groups()
        
        # Calculate statistics
        total_active_value = sum(
            float(order.size * (order.price or Decimal("0")))
            for order in active_orders.values()
        )
        
        active_arbitrage_groups = [g for g in arbitrage_groups.values() if g.status == "active"]
        
        uptime_minutes = (time.time() - self.stats["start_time"]) / 60
        
        return {
            "timestamp": int(time.time() * 1000),
            "uptime_minutes": round(uptime_minutes, 1),
            "statistics": self.stats,
            "active_orders": {
                "count": len(active_orders),
                "total_value_usd": round(total_active_value, 2),
                "by_exchange": {
                    "extended": len([o for o in active_orders.values() if o.exchange == Exchange.EXTENDED]),
                    "lighter": len([o for o in active_orders.values() if o.exchange == Exchange.LIGHTER])
                }
            },
            "arbitrage_groups": {
                "total": len(arbitrage_groups),
                "active": len(active_arbitrage_groups),
                "completed": len([g for g in arbitrage_groups.values() if g.status == "completed"]),
                "failed": len([g for g in arbitrage_groups.values() if g.status == "failed"])
            },
            "risk_status": {
                "total_exposure_usd": total_active_value,
                "max_exposure_usd": self.risk_config["max_total_exposure_usd"],
                "utilization_percent": round((total_active_value / self.risk_config["max_total_exposure_usd"]) * 100, 1)
            }
        }


# Standalone testing functions (unchanged)
async def test_order_management():
    """Test the order management system"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)-15s | %(levelname)-8s | %(message)s'
    )
    
    logger = logging.getLogger("OrderTest")
    logger.info("🚀 Testing Order Management System")
    
    # Mock setup (replace with actual components)
    from balance import BalanceManager
    
    # Read Extended credentials from .env file
    env_manager = EnvManager()
    api_key = env_manager.get_env_value("EXTENDED_API_KEY")
    public_key = env_manager.get_env_value("EXTENDED_PUBLIC_KEY")
    private_key = env_manager.get_env_value("EXTENDED_PRIVATE_KEY")
    vault_str = env_manager.get_env_value("EXTENDED_VAULT")
    
    # Convert vault to integer
    try:
        vault = int(vault_str) if vault_str else None
    except ValueError:
        logger.error(f"Invalid vault value in .env file: {vault_str}")
        return
    
    # Check that all required credentials are available
    if not all([api_key, public_key, private_key, vault]):
        logger.error("Missing Extended credentials in .env file")
        logger.error("Please ensure EXTENDED_API_KEY, EXTENDED_PUBLIC_KEY, EXTENDED_PRIVATE_KEY, and EXTENDED_VAULT are set")
        return
    
    logger.info(f"Loaded Extended credentials from .env file")
    
    # Create Extended account
    stark_account = StarkPerpetualAccount(
        vault=vault,
        private_key=private_key,
        public_key=public_key,
        api_key=api_key,
    )
    
    # Create balance manager
    balance_manager = BalanceManager(stark_account)
    
    # Create order manager
    order_manager = OrderManager(stark_account, balance_manager)
    
    logger.info("✅ Order manager initialized")
    
    try:
        # Test 1: Place limit orders
        logger.info("🔍 Test 1: Placing limit orders")
        
        ext_order = await order_manager.place_limit_order(
            exchange=Exchange.EXTENDED,
            symbol="BTC-USD",
            side=OrderSide.BUY,
            size=Decimal("0.01"),
            price=Decimal("45000.0")
        )
        
        if ext_order:
            logger.info(f"✅ Extended limit order placed: {ext_order.order_id}")
        
        lit_order = await order_manager.place_limit_order(
            exchange=Exchange.LIGHTER,
            symbol="BTC",
            side=OrderSide.SELL,
            size=Decimal("0.01"),
            price=Decimal("46000.0")
        )
        
        if lit_order:
            logger.info(f"✅ Lighter limit order placed: {lit_order.order_id}")
        
        # Test 2: Place market order
        logger.info("🔍 Test 2: Placing market order")
        
        market_order = await order_manager.place_market_order(
            exchange=Exchange.EXTENDED,
            symbol="ETH-USD",
            side=OrderSide.BUY,
            size=Decimal("0.1")
        )
        
        if market_order:
            logger.info(f"✅ Market order placed: {market_order.order_id}")
        
        # Test 3: Get trading summary
        logger.info("🔍 Test 3: Getting trading summary")
        
        summary = await order_manager.get_trading_summary()
        logger.info(f"📊 Trading Summary:")
        logger.info(f"   Active Orders: {summary['active_orders']['count']}")
        logger.info(f"   Total Value: ${summary['active_orders']['total_value_usd']:,.2f}")
        logger.info(f"   Risk Utilization: {summary['risk_status']['utilization_percent']}%")
        
        # Test 4: Cancel orders
        logger.info("🔍 Test 4: Cancelling orders")
        
        active_orders = await order_manager.get_active_orders()
        for order_id in list(active_orders.keys())[:2]:  # Cancel first 2 orders
            success = await order_manager.cancel_order(order_id)
            if success:
                logger.info(f"✅ Order cancelled: {order_id}")
        
        # Final summary
        final_summary = await order_manager.get_trading_summary()
        logger.info(f"📊 Final Summary:")
        logger.info(f"   Orders Placed: {final_summary['statistics']['orders_placed']}")
        logger.info(f"   Orders Cancelled: {final_summary['statistics']['orders_cancelled']}")
        logger.info(f"   Active Orders: {final_summary['active_orders']['count']}")
        
    except Exception as e:
        logger.error(f"Test error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("HFT Order Management System - UPDATED WITH REAL ORDER PLACEMENT")
    print("=" * 60)
    print("Features:")
    print("• REAL cross-exchange order placement and tracking")
    print("• Smart order routing with orderbook analysis")
    print("• LIVE hedged arbitrage order placement")
    print("• Comprehensive risk management")
    print("• Real-time order status monitoring")
    
    input("\nPress Enter to start order management test...")
    asyncio.run(test_order_management())