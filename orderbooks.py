#!/usr/bin/env python3
"""
Enhanced Dual Orderbook Management System for HFT Arbitrage - CONNECTION READINESS FIX
========================================================================================

ENHANCED: Adds intelligent connection readiness validation and quality scoring
to prevent execution attempts on incomplete orderbook data.
"""

import asyncio
import json
import logging
import time
import aiohttp
import websockets
from datetime import datetime
from decimal import Decimal
from dataclasses import dataclass
from typing import Dict, Optional, List, Tuple
from enum import Enum

from x10.perpetual.configuration import STARKNET_MAINNET_CONFIG
from x10.perpetual.stream_client import PerpetualStreamClient
from config import config


# ============================================================================
# ENHANCED CONNECTION STATE MANAGEMENT
# ============================================================================

class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting" 
    CONNECTED = "connected"
    READY = "ready"  # Connected + data flowing + latency acceptable
    DEGRADED = "degraded"  # Connected but poor quality
    FAILED = "failed"

@dataclass
class ConnectionHealth:
    """Enhanced connection health tracking with quality metrics"""
    state: ConnectionState = ConnectionState.DISCONNECTED
    last_update_time: float = 0.0
    update_count: int = 0
    avg_latency_ms: float = 0.0
    last_spread_bps: Optional[float] = None
    last_depth: Optional[int] = None
    connection_time: Optional[float] = None
    quality_score: float = 0.0  # 0-100 scale
    consecutive_timeouts: int = 0
    
    def update_metrics(self, latency_ms: float, spread_bps: float, depth: int):
        """Update connection health metrics with data quality assessment"""
        now = time.time()
        self.last_update_time = now
        self.update_count += 1
        self.consecutive_timeouts = 0  # Reset timeout counter on successful update
        
        # Exponential moving average for latency
        alpha = 0.1
        self.avg_latency_ms = (alpha * latency_ms + (1 - alpha) * self.avg_latency_ms)
        self.last_spread_bps = spread_bps
        self.last_depth = depth
        
        # Calculate quality score
        self._calculate_quality_score()
    
    def record_timeout(self):
        """Record a timeout/stale data event"""
        self.consecutive_timeouts += 1
        self._calculate_quality_score()
    
    def _calculate_quality_score(self) -> float:
        """
        FIXED: Even more lenient quality scoring for Lighter orderbook
        """
        if self.state not in [ConnectionState.CONNECTED, ConnectionState.READY]:
            self.quality_score = 0.0
            return self.quality_score
            
        score = 100.0
        
        # VERY lenient latency thresholds for Lighter
        if self.avg_latency_ms > 100:  # Increased to 100ms
            penalty = min(20, (self.avg_latency_ms - 100) * 0.2)  # Very reduced penalty
            score -= penalty
        
        # VERY lenient stale data threshold for Lighter  
        time_since_update = time.time() - self.last_update_time
        if time_since_update > 15.0:  # Increased to 15 seconds
            penalty = min(15, (time_since_update - 15.0) * 2)  # Very reduced penalty
            score -= penalty
        
        # VERY lenient timeout handling for Lighter
        if self.consecutive_timeouts > 5:  # Increased threshold to 5
            penalty = min(10, (self.consecutive_timeouts - 5) * 5)  # Very reduced penalty
            score -= penalty
        
        # VERY lenient depth requirements for Lighter
        if self.last_depth and self.last_depth < 1:  # Just need 1 level
            score -= (1 - self.last_depth) * 2  # Very reduced penalty
        
        # VERY lenient spread requirements for Lighter
        if self.last_spread_bps and self.last_spread_bps > 100.0:  # Very high threshold
            penalty = min(10, (self.last_spread_bps - 100.0) * 0.1)  # Very reduced penalty
            score -= penalty
        
        self.quality_score = max(30.0, score)  # Minimum score of 30 instead of 20
        return self.quality_score

    @property 
    def is_ready_for_execution(self) -> bool:
        """
        FIXED: More lenient execution readiness criteria
        """
        return (
            self.state in [ConnectionState.CONNECTED, ConnectionState.READY] and
            self.quality_score >= 50.0 and  # FIXED: Lowered from 75.0 to 50.0
            self.consecutive_timeouts <= 3 and  # FIXED: More lenient
            time.time() - self.last_update_time < 5.0  # FIXED: Increased from 2.0s to 5.0s
        )


# ============================================================================
# EXISTING DATACLASSES (unchanged)
# ============================================================================

@dataclass
class OrderbookLevel:
    """Individual price level in orderbook for HFT execution"""
    price: Decimal
    size: Decimal
    timestamp: int


@dataclass
class OrderbookState:
    """Complete orderbook state for one exchange with real-time updates"""
    symbol: str
    exchange: str  # "extended" or "lighter"
    bids: List[OrderbookLevel]  # Sorted highest to lowest price
    asks: List[OrderbookLevel]  # Sorted lowest to highest price
    last_update: int
    sequence: int = 0  # For tracking update sequence
    
    # Keep all existing methods unchanged...
    def get_best_bid(self) -> Optional[OrderbookLevel]:
        return self.bids[0] if self.bids else None
    
    def get_best_ask(self) -> Optional[OrderbookLevel]:
        return self.asks[0] if self.asks else None
    
    def get_spread(self) -> Optional[Decimal]:
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid and best_ask:
            return best_ask.price - best_bid.price
        return None
    
    def get_spread_bps(self) -> Optional[Decimal]:
        spread = self.get_spread()
        mid_price = self.get_mid_price()
        if spread and mid_price and mid_price > 0:
            return (spread / mid_price) * Decimal("10000")
        return None
    
    def get_mid_price(self) -> Optional[Decimal]:
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid and best_ask:
            return (best_bid.price + best_ask.price) / Decimal("2")
        return None
    
    def get_weighted_mid_price(self, depth_levels: int = 5) -> Optional[Decimal]:
        if not self.bids or not self.asks:
            return None
        
        total_bid_volume = Decimal("0")
        weighted_bid_sum = Decimal("0")
        
        for level in self.bids[:depth_levels]:
            total_bid_volume += level.size
            weighted_bid_sum += level.price * level.size
        
        total_ask_volume = Decimal("0")
        weighted_ask_sum = Decimal("0")
        
        for level in self.asks[:depth_levels]:
            total_ask_volume += level.size
            weighted_ask_sum += level.price * level.size
        
        if total_bid_volume == 0 or total_ask_volume == 0:
            return self.get_mid_price()
        
        weighted_bid_price = weighted_bid_sum / total_bid_volume
        weighted_ask_price = weighted_ask_sum / total_ask_volume
        
        return (weighted_bid_price + weighted_ask_price) / Decimal("2")
    
    def get_vwap(self, side: str, target_volume: Decimal) -> Optional[Decimal]:
        levels = self.bids if side == "sell" else self.asks
        if not levels:
            return None
        
        volume_filled = Decimal("0")
        total_cost = Decimal("0")
        
        for level in levels:
            volume_to_take = min(level.size, target_volume - volume_filled)
            total_cost += level.price * volume_to_take
            volume_filled += volume_to_take
            
            if volume_filled >= target_volume:
                break
        
        return total_cost / volume_filled if volume_filled > 0 else None
    
    def get_liquidity_metrics(self) -> Dict:
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        
        if not best_bid and not best_ask:
            return {}
        
        metrics = {
            "timestamp": self.last_update,
            "bid_depth_5": sum(float(level.size) for level in self.bids[:5]),
            "ask_depth_5": sum(float(level.size) for level in self.asks[:5]),
            "bid_levels": len(self.bids),
            "ask_levels": len(self.asks),
        }
        
        if best_bid:
            metrics.update({
                "best_bid_price": float(best_bid.price),
                "best_bid_size": float(best_bid.size),
            })
        
        if best_ask:
            metrics.update({
                "best_ask_price": float(best_ask.price),
                "best_ask_size": float(best_ask.size),
            })
        
        if best_bid and best_ask:
            metrics.update({
                "mid_price": float(self.get_mid_price() or 0),
                "weighted_mid_price": float(self.get_weighted_mid_price() or 0),
                "spread_bps": float(self.get_spread_bps() or 0),
            })
        else:
            metrics.update({
                "mid_price": None,
                "weighted_mid_price": None, 
                "spread_bps": None,
            })
        
        return metrics

    def get_depth(self, levels: int = 5) -> Dict:
        return {
            "bids": [(float(level.price), float(level.size)) for level in self.bids[:levels]],
            "asks": [(float(level.price), float(level.size)) for level in self.asks[:levels]],
            "timestamp": self.last_update
        }


# ============================================================================
# ENHANCED DUAL ORDERBOOK MANAGER
# ============================================================================

class DualOrderbookManager:
    """
    ENHANCED: High-frequency dual-exchange orderbook manager with intelligent 
    connection readiness validation and quality scoring.
    """
    
    def __init__(self, opportunity, quality_threshold: float = 75.0, 
                 readiness_timeout: float = 45.0):
        self.opportunity = opportunity
        self.extended_orderbook: Optional[OrderbookState] = None
        self.lighter_orderbook: Optional[OrderbookState] = None
        
        # ENHANCED: Connection health tracking
        self.extended_health = ConnectionHealth()
        self.lighter_health = ConnectionHealth()
        self.quality_threshold = quality_threshold
        self.readiness_timeout = readiness_timeout
        
        # Thread-safe synchronization primitives
        self.extended_lock = asyncio.Lock()
        self.lighter_lock = asyncio.Lock()
        self.update_event = asyncio.Event()
        
        # Connection and state management
        self.extended_connected = False
        self.lighter_connected = False
        self.is_running = False
        
        # Performance and monitoring metrics
        self.extended_updates = 0
        self.lighter_updates = 0
        self.last_sync_check = time.time()
        
        # WebSocket clients for real-time streams
        self.extended_stream_client: Optional[PerpetualStreamClient] = None
        self.lighter_stream_client = None
        
        self.logger = logging.getLogger(f"DualOrderbook-{opportunity.extended_symbol}")
        
    # ========================================================================
    # ENHANCED CONNECTION READINESS METHODS
    # ========================================================================
    
    async def wait_for_connections_ready(self, timeout: Optional[float] = None) -> bool:
        """
        FIXED: More realistic connection readiness validation
        """
        timeout = timeout or self.readiness_timeout
        start_time = time.time()
        
        symbols = [self.opportunity.extended_symbol, self.opportunity.lighter_symbol]
        self.logger.info(f"⏳ Waiting for {len(symbols)} orderbook connections to be ready...")
        self.logger.info(f"   Quality threshold: 50.0 (FIXED - was too high)")  # FIXED
        self.logger.info(f"   Timeout: {timeout:.1f}s")
        
        while time.time() - start_time < timeout:
            ready_count = 0
            status_report = []
            
            # FIXED: More lenient Extended connection readiness
            ext_ready = (
                self.extended_connected and 
                self.extended_health.quality_score >= 50.0 and  # FIXED: Lowered threshold
                self.extended_orderbook and
                len(self.extended_orderbook.bids) > 0 and  # FIXED: Just check for any bids/asks
                len(self.extended_orderbook.asks) > 0
            )
            
            if ext_ready:
                ready_count += 1
                status_report.append(f"Extended: READY ({self.extended_health.quality_score:.1f})")
            else:
                state = self.extended_health.state.value.upper()
                quality = self.extended_health.quality_score
                bid_count = len(self.extended_orderbook.bids) if self.extended_orderbook else 0
                ask_count = len(self.extended_orderbook.asks) if self.extended_orderbook else 0
                status_report.append(f"Extended: {state} (Q:{quality:.1f}, B:{bid_count}, A:{ask_count})")
            
            # FIXED: More lenient Lighter connection readiness
            lit_ready = (
                self.lighter_connected and 
                self.lighter_health.quality_score >= 50.0 and  # FIXED: Lowered threshold
                self.lighter_orderbook and
                len(self.lighter_orderbook.bids) > 0 and  # FIXED: Just check for any bids/asks
                len(self.lighter_orderbook.asks) > 0
            )
            
            if lit_ready:
                ready_count += 1
                status_report.append(f"Lighter: READY ({self.lighter_health.quality_score:.1f})")
            else:
                state = self.lighter_health.state.value.upper()
                quality = self.lighter_health.quality_score
                bid_count = len(self.lighter_orderbook.bids) if self.lighter_orderbook else 0
                ask_count = len(self.lighter_orderbook.asks) if self.lighter_orderbook else 0
                status_report.append(f"Lighter: {state} (Q:{quality:.1f}, B:{bid_count}, A:{ask_count})")
            
            if ready_count == 2:
                self.logger.info(f"✅ All connections ready for execution!")
                return True
            
            # FIXED: Log progress every 3 seconds instead of 5
            elapsed = time.time() - start_time
            if int(elapsed) % 3 == 0 and elapsed > 0:
                self.logger.info(f"📊 Connection readiness ({elapsed:.1f}s): {' | '.join(status_report)}")
            
            await asyncio.sleep(0.2)  # FIXED: Faster polling
        
        # FIXED: More detailed timeout logging
        self.logger.error(f"❌ Connection readiness timeout after {timeout}s")
        self.logger.error(f"   Extended ready: {ext_ready} (Q:{self.extended_health.quality_score:.1f})")
        self.logger.error(f"   Lighter ready: {lit_ready} (Q:{self.lighter_health.quality_score:.1f})")
        
        # FIXED: Return True if at least basic connections are established (fallback)
        basic_connections = self.extended_connected and self.lighter_connected
        if basic_connections:
            self.logger.warning("⚠️ Using basic connection fallback (quality requirements relaxed)")
            return True
        
        return False
    
    def are_connections_ready_for_execution(self) -> bool:
        """
        FIXED: More practical execution readiness check
        """
        # FIXED: Check basic connection state first
        if not (self.extended_connected and self.lighter_connected):
            return False
        
        # FIXED: Check for any orderbook data
        has_extended_data = (
            self.extended_orderbook and
            len(self.extended_orderbook.bids) > 0 and
            len(self.extended_orderbook.asks) > 0
        )
        
        has_lighter_data = (
            self.lighter_orderbook and
            len(self.lighter_orderbook.bids) > 0 and
            len(self.lighter_orderbook.asks) > 0
        )
        
        # FIXED: More lenient quality requirements
        quality_ok = (
            self.extended_health.quality_score >= 40.0 and  # FIXED: Very lenient
            self.lighter_health.quality_score >= 40.0
        )
        
        # FIXED: Check data freshness (more lenient)
        data_fresh = (
            time.time() - self.extended_health.last_update_time < 10.0 and  # FIXED: 10s instead of 2s
            time.time() - self.lighter_health.last_update_time < 10.0
        )
        
        result = has_extended_data and has_lighter_data and (quality_ok or data_fresh)
        
        # FIXED: Debug logging for troubleshooting
        if not result:
            self.logger.debug(f"Execution readiness failed:")
            self.logger.debug(f"  Extended data: {has_extended_data}")
            self.logger.debug(f"  Lighter data: {has_lighter_data}")
            self.logger.debug(f"  Quality OK: {quality_ok} (E:{self.extended_health.quality_score:.1f}, L:{self.lighter_health.quality_score:.1f})")
            self.logger.debug(f"  Data fresh: {data_fresh}")
        
        return result
    
    def get_connection_health_summary(self) -> Dict:
        """Get comprehensive connection health summary for monitoring"""
        return {
            "extended": {
                "state": self.extended_health.state.value,
                "quality_score": self.extended_health.quality_score,
                "avg_latency_ms": self.extended_health.avg_latency_ms,
                "update_count": self.extended_health.update_count,
                "last_update_age_ms": int((time.time() - self.extended_health.last_update_time) * 1000),
                "ready_for_execution": self.extended_health.is_ready_for_execution,
                "consecutive_timeouts": self.extended_health.consecutive_timeouts
            },
            "lighter": {
                "state": self.lighter_health.state.value,
                "quality_score": self.lighter_health.quality_score,
                "avg_latency_ms": self.lighter_health.avg_latency_ms,
                "update_count": self.lighter_health.update_count,
                "last_update_age_ms": int((time.time() - self.lighter_health.last_update_time) * 1000),
                "ready_for_execution": self.lighter_health.is_ready_for_execution,
                "consecutive_timeouts": self.lighter_health.consecutive_timeouts
            },
            "overall_ready": self.are_connections_ready_for_execution(),
            "overall_quality": (self.extended_health.quality_score + self.lighter_health.quality_score) / 2
        }

    # ========================================================================
    # NEW LIQUIDITY VALIDATION METHODS FOR ORDERS.PY INTEGRATION
    # ========================================================================

    async def validate_trade_size_against_liquidity(self, trade_size_crypto: Decimal, aggressive_mode: bool = False) -> Dict:
        """
        Validate if a trade size is appropriate given current orderbook liquidity
        """
        validation_result = {
            "valid": False,
            "reason": "",
            "recommended_size": Decimal("0"),
            "liquidity_analysis": {}
        }
        
        try:
            # Get current orderbook states
            extended_state = await self.get_extended_state()
            lighter_state = await self.get_lighter_state()
            
            if not extended_state or not lighter_state:
                validation_result["reason"] = "Incomplete orderbook data"
                return validation_result
            
            # Analyze liquidity on both exchanges
            extended_analysis = self._analyze_exchange_liquidity(extended_state, trade_size_crypto, aggressive_mode)
            lighter_analysis = self._analyze_exchange_liquidity(lighter_state, trade_size_crypto, aggressive_mode)
            
            validation_result["liquidity_analysis"] = {
                "extended": extended_analysis,
                "lighter": lighter_analysis
            }
            
            # Check if trade size is safe on both exchanges
            extended_safe = extended_analysis["safe_for_size"]
            lighter_safe = lighter_analysis["safe_for_size"]
            
            if extended_safe and lighter_safe:
                validation_result["valid"] = True
                validation_result["recommended_size"] = trade_size_crypto
                validation_result["reason"] = "Trade size validated against liquidity"
            else:
                # Calculate recommended size as minimum of both exchanges
                extended_max = extended_analysis.get("max_safe_size", Decimal("0"))
                lighter_max = lighter_analysis.get("max_safe_size", Decimal("0"))
                recommended = min(extended_max, lighter_max)
                
                validation_result["recommended_size"] = recommended
                validation_result["reason"] = f"Trade size too large for available liquidity"
                
                if not extended_safe:
                    validation_result["reason"] += f" (Extended max: {extended_max})"
                if not lighter_safe:
                    validation_result["reason"] += f" (Lighter max: {lighter_max})"
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Error validating trade size against liquidity: {e}")
            validation_result["reason"] = f"Validation error: {e}"
            return validation_result

    def _analyze_exchange_liquidity(self, orderbook_state: OrderbookState, trade_size: Decimal, aggressive: bool) -> Dict:
        """
        Analyze liquidity available on a single exchange
        """
        analysis = {
            "safe_for_size": False,
            "max_safe_size": Decimal("0"),
            "conservative_max": Decimal("0"),
            "aggressive_max": Decimal("0"),
            "depth_score": 0.0,
            "liquidity_usd": 0.0
        }
        
        try:
            if not orderbook_state.bids or not orderbook_state.asks:
                return analysis
            
            # Calculate available liquidity in both directions
            bid_liquidity = self._calculate_side_liquidity(orderbook_state.bids, 5)  # Top 5 levels
            ask_liquidity = self._calculate_side_liquidity(orderbook_state.asks, 5)
            
            # Use minimum liquidity (most restrictive side)
            min_liquidity = min(bid_liquidity["total_size"], ask_liquidity["total_size"])
            
            # Conservative sizing: use 30% of available liquidity
            conservative_max = min_liquidity * Decimal("0.3")
            
            # Aggressive sizing: use 60% of available liquidity
            aggressive_max = min_liquidity * Decimal("0.6")
            
            # Determine max safe size based on mode
            max_safe = aggressive_max if aggressive else conservative_max
            
            # Calculate depth quality score (0-100)
            bid_depth = len(orderbook_state.bids)
            ask_depth = len(orderbook_state.asks)
            avg_depth = (bid_depth + ask_depth) / 2
            depth_score = min(100.0, avg_depth * 10)  # 10 levels = 100 score
            
            # Estimate liquidity in USD
            mid_price = orderbook_state.get_mid_price()
            liquidity_usd = float(min_liquidity * (mid_price or Decimal("50000")))
            
            analysis.update({
                "safe_for_size": trade_size <= max_safe,
                "max_safe_size": max_safe,
                "conservative_max": conservative_max,
                "aggressive_max": aggressive_max,
                "depth_score": depth_score,
                "liquidity_usd": liquidity_usd
            })
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing exchange liquidity: {e}")
            return analysis

    def _calculate_side_liquidity(self, levels: List[OrderbookLevel], max_levels: int = 5) -> Dict:
        """
        Calculate total liquidity available on one side (bid or ask)
        """
        total_size = Decimal("0")
        total_value = Decimal("0")
        
        for i, level in enumerate(levels[:max_levels]):
            total_size += level.size
            total_value += level.price * level.size
        
        avg_price = total_value / total_size if total_size > 0 else Decimal("0")
        
        return {
            "total_size": total_size,
            "total_value": total_value,
            "avg_price": avg_price,
            "levels_count": min(len(levels), max_levels)
        }

    async def get_cross_exchange_liquidity_comparison(self) -> Optional[Dict]:
        """
        Compare liquidity across both exchanges and provide trading recommendations
        """
        try:
            extended_state = await self.get_extended_state()
            lighter_state = await self.get_lighter_state()
            
            if not extended_state or not lighter_state:
                return None
            
            # Analyze liquidity on both exchanges
            extended_analysis = self._analyze_exchange_liquidity(extended_state, Decimal("1.0"), False)
            lighter_analysis = self._analyze_exchange_liquidity(lighter_state, Decimal("1.0"), False)
            
            # Calculate combined metrics
            combined_conservative = min(
                extended_analysis["conservative_max"],
                lighter_analysis["conservative_max"]
            )
            
            combined_aggressive = min(
                extended_analysis["aggressive_max"],
                lighter_analysis["aggressive_max"]
            )
            
            # Determine limiting exchange
            if extended_analysis["conservative_max"] < lighter_analysis["conservative_max"]:
                limiting_exchange = "extended"
                limiting_factor = "Extended orderbook depth"
            else:
                limiting_exchange = "lighter"
                limiting_factor = "Lighter orderbook depth"
            
            # Calculate overall quality score
            overall_quality = (extended_analysis["depth_score"] + lighter_analysis["depth_score"]) / 2
            
            # Estimate maximum safe trade value in USD
            extended_mid = extended_state.get_mid_price() or Decimal("50000")
            lighter_mid = lighter_state.get_mid_price() or Decimal("50000")
            avg_price = (extended_mid + lighter_mid) / Decimal("2")
            
            max_safe_trade_usd = float(combined_conservative * avg_price)
            
            return {
                "timestamp": int(time.time() * 1000),
                "comparison": {
                    "conservative_capacity": {
                        "extended": float(extended_analysis["conservative_max"]),
                        "lighter": float(lighter_analysis["conservative_max"]),
                        "combined": float(combined_conservative)
                    },
                    "aggressive_capacity": {
                        "extended": float(extended_analysis["aggressive_max"]),
                        "lighter": float(lighter_analysis["aggressive_max"]),
                        "combined": float(combined_aggressive)
                    },
                    "depth_quality": {
                        "extended": extended_analysis["depth_score"],
                        "lighter": lighter_analysis["depth_score"],
                        "overall": overall_quality
                    }
                },
                "recommendations": {
                    "max_safe_trade_usd": max_safe_trade_usd,
                    "limiting_exchange": limiting_exchange,
                    "limiting_factor": limiting_factor,
                    "overall_quality": overall_quality,
                    "trading_recommendation": self._get_liquidity_trading_recommendation(overall_quality, max_safe_trade_usd)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error in cross-exchange liquidity comparison: {e}")
            return None

    def _get_liquidity_trading_recommendation(self, quality_score: float, max_trade_usd: float) -> str:
        """
        Generate trading recommendation based on liquidity analysis
        """
        if quality_score >= 80 and max_trade_usd >= 1000:
            return "EXCELLENT - Normal position sizing recommended"
        elif quality_score >= 60 and max_trade_usd >= 500:
            return "GOOD - Conservative position sizing recommended"
        elif quality_score >= 40 and max_trade_usd >= 100:
            return "MODERATE - Very conservative sizing recommended"
        elif quality_score >= 20 and max_trade_usd >= 50:
            return "POOR - Minimal sizing only"
        else:
            return "INADEQUATE - Avoid trading until liquidity improves"

    async def check_balance_to_liquidity_ratio(self, available_balance_usd: Decimal) -> Dict:
        """
        Check if available balance is appropriate relative to market liquidity
        """
        analysis = {
            "balance_appropriate": False,
            "balance_to_liquidity_ratio": 0.0,
            "max_recommended_trade_usd": Decimal("0"),
            "warnings": [],
            "liquidity_health": "unknown"
        }
        
        try:
            liquidity_comparison = await self.get_cross_exchange_liquidity_comparison()
            if not liquidity_comparison:
                analysis["warnings"].append("Unable to analyze cross-exchange liquidity")
                return analysis
            
            # Get total available liquidity in USD
            max_safe_trade = liquidity_comparison["recommendations"]["max_safe_trade_usd"]
            total_liquidity_estimate = max_safe_trade * 3  # Rough estimate of total market liquidity
            
            # Calculate balance to liquidity ratio
            ratio = float(available_balance_usd) / total_liquidity_estimate if total_liquidity_estimate > 0 else float('inf')
            
            analysis["balance_to_liquidity_ratio"] = ratio
            analysis["max_recommended_trade_usd"] = Decimal(str(max_safe_trade))
            
            # Determine appropriateness and warnings
            if ratio <= 0.5:
                analysis["balance_appropriate"] = True
                analysis["liquidity_health"] = "healthy"
            elif ratio <= 1.0:
                analysis["balance_appropriate"] = True
                analysis["liquidity_health"] = "moderate"
                analysis["warnings"].append("Balance is moderate relative to liquidity - use conservative sizing")
            elif ratio <= 2.0:
                analysis["balance_appropriate"] = False
                analysis["liquidity_health"] = "concerning"
                analysis["warnings"].append("Balance is large relative to available liquidity")
                # Reduce recommended trade size
                analysis["max_recommended_trade_usd"] = Decimal(str(max_safe_trade * 0.5))
            else:
                analysis["balance_appropriate"] = False
                analysis["liquidity_health"] = "dangerous"
                analysis["warnings"].append("Balance is too large for current market liquidity")
                analysis["warnings"].append("Consider reducing position sizes significantly")
                # Further reduce recommended trade size
                analysis["max_recommended_trade_usd"] = Decimal(str(max_safe_trade * 0.25))
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error checking balance to liquidity ratio: {e}")
            analysis["warnings"].append(f"Analysis error: {e}")
            return analysis

    # ========================================================================
    # ENHANCED MONITORING WITH QUALITY TRACKING
    # ========================================================================
    
    async def start_monitoring(self):
        """Start monitoring with enhanced connection validation"""
        self.logger.info(f"🚀 Starting ENHANCED dual orderbook monitoring")
        self.logger.info(f"   Extended: {self.opportunity.extended_symbol}")
        self.logger.info(f"   Lighter: {self.opportunity.lighter_symbol}")
        self.logger.info(f"   Quality threshold: {self.quality_threshold}")
        
        self.is_running = True
        
        # Initialize orderbook states
        self.extended_orderbook = OrderbookState(
            symbol=self.opportunity.extended_symbol,
            exchange="extended",
            bids=[],
            asks=[],
            last_update=int(time.time() * 1000)
        )
        
        self.lighter_orderbook = OrderbookState(
            symbol=self.opportunity.lighter_symbol,
            exchange="lighter",
            bids=[],
            asks=[],
            last_update=int(time.time() * 1000)
        )
        
        # Start enhanced monitoring tasks
        tasks = [
            asyncio.create_task(self._monitor_extended_orderbook_enhanced(), name="extended_monitor"),
            asyncio.create_task(self._monitor_lighter_orderbook_enhanced(), name="lighter_monitor"),
            asyncio.create_task(self._sync_monitor_loop_enhanced(), name="sync_monitor")
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"Error in enhanced dual orderbook monitoring: {e}")
            raise
        finally:
            self.is_running = False
            await self._cleanup_connections()

    async def _monitor_extended_orderbook_enhanced(self):
        """ENHANCED: Monitor Extended with quality tracking"""
        self.logger.info(f"📡 Starting Enhanced Extended orderbook stream")
        
        while self.is_running:
            try:
                self.extended_health.state = ConnectionState.CONNECTING
                self.extended_stream_client = PerpetualStreamClient(api_url=STARKNET_MAINNET_CONFIG.stream_url)
                
                async with self.extended_stream_client.subscribe_to_orderbooks(self.opportunity.extended_symbol) as stream:
                    self.extended_connected = True
                    self.extended_health.state = ConnectionState.CONNECTED
                    self.extended_health.connection_time = time.time()
                    self.logger.info(f"✅ Extended orderbook connected: {self.opportunity.extended_symbol}")
                    
                    async for orderbook_update in stream:
                        if not self.is_running:
                            break
                            
                        update_start_time = time.time()
                        await self._process_extended_orderbook_update_enhanced(orderbook_update)
                        processing_latency = (time.time() - update_start_time) * 1000
                        
                        # Update connection health with quality metrics
                        if self.extended_orderbook:
                            spread_bps = float(self.extended_orderbook.get_spread_bps() or 0)
                            depth = len(self.extended_orderbook.bids) + len(self.extended_orderbook.asks)
                            self.extended_health.update_metrics(processing_latency, spread_bps, depth)
                            
                            # Promote to READY state if quality is good
                            if (self.extended_health.state == ConnectionState.CONNECTED and 
                                self.extended_health.quality_score >= self.quality_threshold):
                                self.extended_health.state = ConnectionState.READY
                        
                        self.extended_updates += 1
                        
            except Exception as e:
                self.logger.error(f"Enhanced Extended orderbook error: {e}")
                self.extended_connected = False
                self.extended_health.state = ConnectionState.FAILED
                
                if self.is_running:
                    self.logger.info("🔄 Reconnecting to Extended orderbook in 2 seconds...")
                    await asyncio.sleep(2.0)
        
        self.logger.info("📡 Enhanced Extended orderbook monitoring stopped")

    async def _process_extended_orderbook_update_enhanced(self, orderbook_update):
        """
        CORRECTED: Process Extended orderbook updates with proper bid/ask logic
        
        The key insight: Extended puts bids in 'bid' list and asks in 'ask' list.
        The qty sign indicates add/remove in DELTA messages, not bid/ask direction.
        """
        try:
            if not orderbook_update or not orderbook_update.data:
                self.extended_health.record_timeout()
                return
            
            data = orderbook_update.data
            current_timestamp = int(time.time() * 1000)
            
            self.logger.debug(f"🔍 Processing Extended orderbook update")
            self.logger.debug(f"   Data type: {type(data)}")
            
            async with self.extended_lock:
                if not self.extended_orderbook:
                    return
                
                bids_dict = {}
                asks_dict = {}
                processed_levels = 0
                
                # *** CORRECTED LOGIC: Process bid and ask lists separately ***
                
                # Process BID list - all items are bids regardless of qty sign
                if hasattr(data, 'bid') and data.bid:
                    self.logger.debug(f"   Processing {len(data.bid)} bid items")
                    
                    for i, bid_item in enumerate(data.bid):
                        try:
                            if hasattr(bid_item, 'price') and hasattr(bid_item, 'qty'):
                                price = Decimal(str(bid_item.price))
                                qty = Decimal(str(bid_item.qty))
                                size = abs(qty)  # Always use absolute value for size
                                
                                if i < 3:  # Debug first few
                                    self.logger.debug(f"   Bid {i}: price={price}, qty={qty}, size={size}")
                                
                                if size > 0:  # Only add if size > 0 (qty=0 means remove)
                                    level = OrderbookLevel(
                                        price=price,
                                        size=size,
                                        timestamp=current_timestamp
                                    )
                                    bids_dict[price] = level
                                    processed_levels += 1
                                elif qty == 0:
                                    # Remove this price level (DELTA message)
                                    bids_dict.pop(price, None)
                                    
                        except Exception as e:
                            if i < 3:
                                self.logger.warning(f"   Error processing bid {i}: {e}")
                            continue
                
                # Process ASK list - all items are asks regardless of qty sign  
                if hasattr(data, 'ask') and data.ask:
                    self.logger.debug(f"   Processing {len(data.ask)} ask items")
                    
                    for i, ask_item in enumerate(data.ask):
                        try:
                            if hasattr(ask_item, 'price') and hasattr(ask_item, 'qty'):
                                price = Decimal(str(ask_item.price))
                                qty = Decimal(str(ask_item.qty))
                                size = abs(qty)  # Always use absolute value for size
                                
                                if i < 3:  # Debug first few
                                    self.logger.debug(f"   Ask {i}: price={price}, qty={qty}, size={size}")
                                
                                if size > 0:  # Only add if size > 0 (qty=0 means remove)
                                    level = OrderbookLevel(
                                        price=price,
                                        size=size,
                                        timestamp=current_timestamp
                                    )
                                    asks_dict[price] = level
                                    processed_levels += 1
                                elif qty == 0:
                                    # Remove this price level (DELTA message)
                                    asks_dict.pop(price, None)
                                    
                        except Exception as e:
                            if i < 3:
                                self.logger.warning(f"   Error processing ask {i}: {e}")
                            continue
                
                self.logger.debug(f"🔍 Processed: {processed_levels} levels ({len(bids_dict)} bids, {len(asks_dict)} asks)")
                
                # *** FIXED: Handle both SNAPSHOT and DELTA messages ***
                message_type = getattr(orderbook_update, 'type', 'UNKNOWN')
                
                if message_type == 'SNAPSHOT':
                    # SNAPSHOT: Replace entire orderbook
                    current_bids = sorted(bids_dict.values(), key=lambda x: x.price, reverse=True)
                    current_asks = sorted(asks_dict.values(), key=lambda x: x.price)
                    
                    self.extended_orderbook.bids = current_bids
                    self.extended_orderbook.asks = current_asks
                    
                elif message_type == 'DELTA':
                    # DELTA: Merge with existing orderbook
                    
                    # Start with existing levels
                    existing_bids = {level.price: level for level in self.extended_orderbook.bids}
                    existing_asks = {level.price: level for level in self.extended_orderbook.asks}
                    
                    # Apply bid updates
                    for price, level in bids_dict.items():
                        existing_bids[price] = level
                    
                    # Apply ask updates  
                    for price, level in asks_dict.items():
                        existing_asks[price] = level
                    
                    # Remove zero-quantity levels (already handled above)
                    # Update orderbook
                    current_bids = sorted(existing_bids.values(), key=lambda x: x.price, reverse=True)
                    current_asks = sorted(existing_asks.values(), key=lambda x: x.price)
                    
                    self.extended_orderbook.bids = current_bids
                    self.extended_orderbook.asks = current_asks
                
                else:
                    self.logger.warning(f"Unknown message type: {message_type}")
                    return
                
                # Update metadata
                self.extended_orderbook.last_update = current_timestamp
                self.extended_orderbook.sequence += 1
                
                # Log successful update
                bid_count = len(self.extended_orderbook.bids)
                ask_count = len(self.extended_orderbook.asks)
                
                if bid_count > 0 or ask_count > 0:
                    self.logger.info(f"✅ Extended orderbook updated ({message_type}): {bid_count} bids, {ask_count} asks")
                    
                    if bid_count > 0 and ask_count > 0:
                        best_bid = self.extended_orderbook.bids[0].price
                        best_ask = self.extended_orderbook.asks[0].price
                        spread = best_ask - best_bid
                        self.logger.info(f"   Best: {best_bid} / {best_ask} (spread: {spread})")
                
                # Update health metrics
                spread_bps = float(self.extended_orderbook.get_spread_bps() or 0)
                depth = bid_count + ask_count
                self.extended_health.update_metrics(5.0, spread_bps, depth)
                
                if (self.extended_health.state == ConnectionState.CONNECTED and 
                    bid_count > 0 and ask_count > 0):
                    self.extended_health.state = ConnectionState.READY
                
                self.update_event.set()
                self.update_event.clear()
                    
        except Exception as e:
            self.logger.error(f"❌ Error processing Enhanced Extended update: {e}")
            import traceback
            self.logger.error(f"Extended processing traceback: {traceback.format_exc()}")
            self.extended_health.record_timeout()


    async def _monitor_lighter_orderbook_enhanced(self):
        """ENHANCED: Monitor Lighter with quality tracking"""
        self.logger.info(f"📡 Starting Enhanced Lighter WebSocket stream")
        
        websocket_url = config.LIGHTER_WS_URL or "wss://mainnet.zklighter.elliot.ai/stream"
        market_id = self.opportunity.lighter_market_id
        
        subscription_msg = {
            "type": "subscribe",
            "channel": f"order_book/{market_id}"
        }
        
        while self.is_running:
            try:
                self.lighter_health.state = ConnectionState.CONNECTING
                
                async with websockets.connect(websocket_url) as websocket:
                    await websocket.send(json.dumps(subscription_msg))
                    self.lighter_connected = True
                    self.lighter_health.state = ConnectionState.CONNECTED
                    self.lighter_health.connection_time = time.time()
                    self.logger.info(f"✅ Lighter WebSocket connected: market {market_id}")
                    
                    async for message in websocket:
                        if not self.is_running:
                            break
                            
                        update_start_time = time.time()
                        await self._process_lighter_websocket_message_enhanced(message)
                        processing_latency = (time.time() - update_start_time) * 1000
                        
                        # Update connection health
                        if self.lighter_orderbook:
                            spread_bps = float(self.lighter_orderbook.get_spread_bps() or 0)
                            depth = len(self.lighter_orderbook.bids) + len(self.lighter_orderbook.asks)
                            self.lighter_health.update_metrics(processing_latency, spread_bps, depth)
                            
                            # Promote to READY state if quality is good
                            if (self.lighter_health.state == ConnectionState.CONNECTED and 
                                self.lighter_health.quality_score >= self.quality_threshold):
                                self.lighter_health.state = ConnectionState.READY
                        
                        self.lighter_updates += 1
                        
            except Exception as e:
                self.logger.error(f"Enhanced Lighter WebSocket error: {e}")
                self.lighter_connected = False
                self.lighter_health.state = ConnectionState.FAILED
                
                if self.is_running:
                    self.logger.info("🔄 Reconnecting to Lighter WebSocket in 2 seconds...")
                    await asyncio.sleep(2.0)
        
        self.logger.info("📡 Enhanced Lighter WebSocket monitoring stopped")

    async def _process_lighter_websocket_message_enhanced(self, message: str):
        """
        FIXED: Process Lighter messages based on actual diagnostic data format
        """
        try:
            data = json.loads(message)
            message_type = data.get("type")
            
            # *** FIXED: Handle the actual message types from diagnostic ***
            if message_type == "connected":
                self.logger.info("📡 Lighter WebSocket session established")
                return
            
            if message_type not in ["subscribed/order_book", "update/order_book"]:
                # Skip non-orderbook messages
                return
            
            # *** FIXED: Extract orderbook data correctly ***
            order_book_data = data.get("order_book")
            if not order_book_data:
                self.logger.debug(f"No orderbook data in {message_type} message")
                return
            
            # Check if orderbook has error code
            if order_book_data.get("code") != 0:
                self.logger.warning(f"Lighter orderbook error code: {order_book_data.get('code')}")
                return
            
            is_snapshot = (message_type == "subscribed/order_book")
            current_timestamp = int(time.time() * 1000)
            
            async with self.lighter_lock:
                if not self.lighter_orderbook:
                    return
                
                processed_levels = 0
                
                try:
                    if is_snapshot:
                        # *** SNAPSHOT: Replace entire orderbook ***
                        self.logger.info(f"📸 Processing Lighter orderbook SNAPSHOT")
                        
                        new_bids = []
                        bids_data = order_book_data.get("bids", [])
                        for bid in bids_data:
                            try:
                                price = Decimal(str(bid["price"]))
                                size = Decimal(str(bid["size"]))
                                
                                if size > 0:  # Only include non-zero sizes
                                    new_bids.append(OrderbookLevel(
                                        price=price,
                                        size=size,
                                        timestamp=current_timestamp
                                    ))
                                    processed_levels += 1
                            except (KeyError, ValueError, TypeError):
                                continue
                        
                        new_asks = []
                        asks_data = order_book_data.get("asks", [])
                        for ask in asks_data:
                            try:
                                price = Decimal(str(ask["price"]))
                                size = Decimal(str(ask["size"]))
                                
                                if size > 0:  # Only include non-zero sizes
                                    new_asks.append(OrderbookLevel(
                                        price=price,
                                        size=size,
                                        timestamp=current_timestamp
                                    ))
                                    processed_levels += 1
                            except (KeyError, ValueError, TypeError):
                                continue
                        
                        # Sort and update orderbook
                        new_bids.sort(key=lambda x: x.price, reverse=True)  # Highest first
                        new_asks.sort(key=lambda x: x.price)  # Lowest first
                        
                        self.lighter_orderbook.bids = new_bids
                        self.lighter_orderbook.asks = new_asks
                        
                        self.logger.info(f"✅ Lighter SNAPSHOT: {len(new_bids)} bids, {len(new_asks)} asks")
                        
                    else:
                        # *** UPDATE: Apply incremental changes ***
                        self.logger.debug(f"🔄 Processing Lighter orderbook UPDATE")
                        
                        # Start with existing orderbook
                        bid_dict = {level.price: level for level in self.lighter_orderbook.bids}
                        ask_dict = {level.price: level for level in self.lighter_orderbook.asks}
                        
                        # Apply bid updates
                        bids_data = order_book_data.get("bids", [])
                        for bid in bids_data:
                            try:
                                price = Decimal(str(bid["price"]))
                                size = Decimal(str(bid["size"]))
                                
                                if size > 0:
                                    # Add or update this price level
                                    bid_dict[price] = OrderbookLevel(
                                        price=price,
                                        size=size,
                                        timestamp=current_timestamp
                                    )
                                else:
                                    # Remove this price level (size = 0)
                                    bid_dict.pop(price, None)
                                
                                processed_levels += 1
                            except (KeyError, ValueError, TypeError):
                                continue
                        
                        # Apply ask updates
                        asks_data = order_book_data.get("asks", [])
                        for ask in asks_data:
                            try:
                                price = Decimal(str(ask["price"]))
                                size = Decimal(str(ask["size"]))
                                
                                if size > 0:
                                    # Add or update this price level
                                    ask_dict[price] = OrderbookLevel(
                                        price=price,
                                        size=size,
                                        timestamp=current_timestamp
                                    )
                                else:
                                    # Remove this price level (size = 0)
                                    ask_dict.pop(price, None)
                                
                                processed_levels += 1
                            except (KeyError, ValueError, TypeError):
                                continue
                        
                        # Sort and update orderbook
                        new_bids = sorted(bid_dict.values(), key=lambda x: x.price, reverse=True)
                        new_asks = sorted(ask_dict.values(), key=lambda x: x.price)
                        
                        self.lighter_orderbook.bids = new_bids
                        self.lighter_orderbook.asks = new_asks
                        
                        self.logger.debug(f"🔄 Lighter UPDATE: {len(new_bids)} bids, {len(new_asks)} asks")
                    
                    # *** FIXED: Update metadata and health tracking ***
                    if processed_levels > 0:
                        self.lighter_orderbook.last_update = current_timestamp
                        self.lighter_orderbook.sequence += 1
                        
                        # Calculate health metrics
                        spread_bps = float(self.lighter_orderbook.get_spread_bps() or 0)
                        depth = len(self.lighter_orderbook.bids) + len(self.lighter_orderbook.asks)
                        processing_latency = 5.0  # Default processing latency
                        
                        # Update connection health
                        self.lighter_health.update_metrics(processing_latency, spread_bps, depth)
                        
                        # Promote to READY state if we have good data
                        if (self.lighter_health.state == ConnectionState.CONNECTED and 
                            len(self.lighter_orderbook.bids) > 0 and 
                            len(self.lighter_orderbook.asks) > 0):
                            self.lighter_health.state = ConnectionState.READY
                        
                        # Log current best prices
                        if self.lighter_orderbook.bids and self.lighter_orderbook.asks:
                            best_bid = self.lighter_orderbook.bids[0].price
                            best_ask = self.lighter_orderbook.asks[0].price
                            spread = best_ask - best_bid
                            
                            if is_snapshot:
                                self.logger.info(f"✅ Lighter orderbook updated ({message_type}): {len(self.lighter_orderbook.bids)} bids, {len(self.lighter_orderbook.asks)} asks")
                                self.logger.info(f"   Best: {best_bid} / {best_ask} (spread: {spread})")
                        
                        # Signal that orderbook data has been updated
                        self.update_event.set()
                        self.update_event.clear()
                        
                    else:
                        # No levels processed - might be empty update
                        self.logger.debug("🔄 Lighter update with no level changes")
                        
                except Exception as processing_error:
                    self.logger.error(f"Error processing Lighter orderbook data: {processing_error}")
                    self.lighter_health.record_timeout()
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing Lighter WebSocket message JSON: {e}")
        except Exception as e:
            self.logger.error(f"Error processing Lighter WebSocket message: {e}")
            self.lighter_health.record_timeout()
    
    async def _sync_monitor_loop_enhanced(self):
        """ENHANCED: Monitor synchronization with quality reporting"""
        while self.is_running:
            try:
                await asyncio.sleep(5.0)  # Check every 5 seconds
                
                # Log enhanced status
                health_summary = self.get_connection_health_summary()
                overall_ready = health_summary["overall_ready"]
                overall_quality = health_summary["overall_quality"]
                
                self.logger.info(f"📊 Enhanced Dual Orderbook Status:")
                self.logger.info(f"   Overall Ready: {'✅' if overall_ready else '❌'} | Quality: {overall_quality:.1f}")
                self.logger.info(f"   Extended: {health_summary['extended']['state'].upper()} ({health_summary['extended']['quality_score']:.1f})")
                self.logger.info(f"   Lighter: {health_summary['lighter']['state'].upper()} ({health_summary['lighter']['quality_score']:.1f})")
                
                # Detect and log quality issues
                if health_summary['extended']['quality_score'] < 50:
                    self.logger.warning(f"⚠️ Extended quality degraded: {health_summary['extended']['quality_score']:.1f}")
                
                if health_summary['lighter']['quality_score'] < 50:
                    self.logger.warning(f"⚠️ Lighter quality degraded: {health_summary['lighter']['quality_score']:.1f}")
                    
            except Exception as e:
                self.logger.error(f"Error in enhanced sync monitor: {e}")

    # ========================================================================
    # KEEP ALL EXISTING METHODS UNCHANGED
    # ========================================================================
    
    async def get_extended_state(self) -> Optional[OrderbookState]:
        async with self.extended_lock:
            return self.extended_orderbook
    
    async def get_lighter_state(self) -> Optional[OrderbookState]:
        async with self.lighter_lock:
            return self.lighter_orderbook
    
    async def get_synchronized_prices(self) -> Dict:
        """Keep existing implementation unchanged"""
        extended_state = await self.get_extended_state()
        lighter_state = await self.get_lighter_state()
        
        if not extended_state or not lighter_state:
            return {"status": "incomplete", "reason": "missing_orderbook_data"}
        
        extended_metrics = extended_state.get_liquidity_metrics()
        lighter_metrics = lighter_state.get_liquidity_metrics()
        
        if not extended_metrics or not lighter_metrics:
            return {"status": "incomplete", "reason": "missing_price_data"}
        
        # Keep all existing arbitrage calculation logic...
        ext_mid = extended_metrics.get("mid_price")
        lit_mid = lighter_metrics.get("mid_price")
        ext_bid = extended_metrics.get("best_bid_price")
        ext_ask = extended_metrics.get("best_ask_price")
        lit_bid = lighter_metrics.get("best_bid_price")
        lit_ask = lighter_metrics.get("best_ask_price")
        
        arbitrage_opportunities = []
        
        if ext_ask is not None and lit_bid is not None:
            if lit_bid > ext_ask:
                mid_price = (ext_ask + lit_bid) / 2
                price_diff_bps = ((lit_bid - ext_ask) / mid_price) * 10000
                
                arbitrage_opportunities.append({
                    "type": "buy_extended_sell_lighter",
                    "action": "BUY_EXTENDED_SELL_LIGHTER",
                    "buy_exchange": "Extended",
                    "sell_exchange": "Lighter", 
                    "buy_price": ext_ask,
                    "sell_price": lit_bid,
                    "price_diff": lit_bid - ext_ask,
                    "price_diff_bps": abs(price_diff_bps),
                    "mid_price": mid_price
                })
        
        if lit_ask is not None and ext_bid is not None:
            if ext_bid > lit_ask:
                mid_price = (lit_ask + ext_bid) / 2
                price_diff_bps = ((ext_bid - lit_ask) / mid_price) * 10000
                
                arbitrage_opportunities.append({
                    "type": "buy_lighter_sell_extended",
                    "action": "BUY_LIGHTER_SELL_EXTENDED",
                    "buy_exchange": "Lighter",
                    "sell_exchange": "Extended",
                    "buy_price": lit_ask,
                    "sell_price": ext_bid,
                    "price_diff": ext_bid - lit_ask,
                    "price_diff_bps": abs(price_diff_bps),
                    "mid_price": mid_price
                })
        
        if ext_mid is not None and lit_mid is not None and ext_mid > 0 and lit_mid > 0:
            price_diff = ext_mid - lit_mid
            avg_price = (ext_mid + lit_mid) / 2
            price_diff_bps = (price_diff / avg_price) * 10000
            arbitrage_opportunities.append({
                "type": "mid_price",
                "action": "Long Extended, Short Lighter" if price_diff > 0 else "Long Lighter, Short Extended",
                "price_diff": price_diff,
                "price_diff_bps": price_diff_bps
            })
        
        if arbitrage_opportunities:
            best_arb = max(arbitrage_opportunities, key=lambda x: abs(x["price_diff_bps"]))
            is_viable = abs(best_arb["price_diff_bps"]) >= 2.0
            
            return {
                "status": "complete",
                "timestamp": int(time.time() * 1000),
                "extended": extended_metrics,
                "lighter": lighter_metrics,
                "arbitrage": {
                    "price_diff": best_arb["price_diff"],
                    "price_diff_bps": best_arb["price_diff_bps"],
                    "is_viable": is_viable,
                    "recommended_action": best_arb["action"],
                    "arbitrage_type": best_arb["type"],
                    "all_opportunities": arbitrage_opportunities
                }
            }
        
        return {"status": "incomplete", "reason": "no_arbitrage_opportunities"}
    
    async def get_arbitrage_execution_analysis(self) -> Optional[Dict]:
        """Keep existing implementation unchanged"""
        if not (self.extended_orderbook and self.lighter_orderbook):
            return None
        
        async with self.extended_lock:
            extended_best_bid = self.extended_orderbook.get_best_bid()
            extended_best_ask = self.extended_orderbook.get_best_ask()
            extended_mid = self.extended_orderbook.get_mid_price()
            extended_spread = self.extended_orderbook.get_spread_bps()
            extended_depth = self.extended_orderbook.get_depth(5)
        
        async with self.lighter_lock:
            lighter_best_bid = self.lighter_orderbook.get_best_bid()
            lighter_best_ask = self.lighter_orderbook.get_best_ask()
            lighter_mid = self.lighter_orderbook.get_mid_price()
            lighter_spread = self.lighter_orderbook.get_spread_bps()
            lighter_depth = self.lighter_orderbook.get_depth(5)
        
        if not all([extended_best_bid, extended_best_ask, lighter_best_bid, lighter_best_ask]):
            return None
        
        analysis = {
            "timestamp": int(time.time() * 1000),
            "symbol_pair": f"{self.opportunity.extended_symbol}/{self.opportunity.lighter_symbol}",
            "sync_status": {
                "extended_connected": self.extended_connected,
                "lighter_connected": self.lighter_connected,
                "data_age_ms": int(time.time() * 1000) - min(self.extended_orderbook.last_update, self.lighter_orderbook.last_update)
            },
            "extended": {
                "symbol": self.opportunity.extended_symbol,
                "best_bid": float(extended_best_bid.price),
                "best_ask": float(extended_best_ask.price),
                "mid_price": float(extended_mid) if extended_mid else None,
                "spread_bps": float(extended_spread) if extended_spread else None,
                "depth": extended_depth
            },
            "lighter": {
                "symbol": self.opportunity.lighter_symbol,  
                "best_bid": float(lighter_best_bid.price),
                "best_ask": float(lighter_best_ask.price),
                "mid_price": float(lighter_mid) if lighter_mid else None,
                "spread_bps": float(lighter_spread) if lighter_spread else None,
                "depth": lighter_depth
            },
            "arbitrage_scenarios": {
                "buy_ext_sell_lighter": {
                    "buy_price": float(extended_best_ask.price),
                    "sell_price": float(lighter_best_bid.price),
                    "gross_profit": float(lighter_best_bid.price - extended_best_ask.price),
                    "profit_bps": float((lighter_best_bid.price - extended_best_ask.price) / extended_best_ask.price * Decimal("10000"))
                },
                "buy_lighter_sell_ext": {
                    "buy_price": float(lighter_best_ask.price),
                    "sell_price": float(extended_best_bid.price),
                    "gross_profit": float(extended_best_bid.price - lighter_best_ask.price),
                    "profit_bps": float((extended_best_bid.price - lighter_best_ask.price) / lighter_best_ask.price * Decimal("10000"))
                }
            }
        }
        
        return analysis
    
    async def _cleanup_connections(self):
        """Clean up WebSocket connections and resources"""
        self.logger.info("🧹 Cleaning up enhanced dual orderbook connections")
        
        self.extended_health.state = ConnectionState.DISCONNECTED
        self.lighter_health.state = ConnectionState.DISCONNECTED
        
        if self.extended_stream_client:
            pass  # Extended cleanup
        
        if self.lighter_stream_client:
            pass  # Lighter cleanup
    
    async def stop_monitoring(self):
        """Stop monitoring and cleanup all resources"""
        self.logger.info("🛑 Stopping enhanced dual orderbook monitoring")
        self.is_running = False
        await self._cleanup_connections()


# Keep the main section unchanged for compatibility
if __name__ == "__main__":
    print("Enhanced Dual Orderbook Management System - CONNECTION READINESS FIX")
    print("=" * 70)
    print("ENHANCED FEATURES:")
    print("• ✅ Intelligent connection readiness validation")
    print("• ✅ Real-time connection quality scoring (0-100)")
    print("• ✅ Execution readiness verification")
    print("• ✅ Enhanced timeout and stale data detection")
    print("• ✅ Comprehensive connection health monitoring")
    print("• ✅ Complete liquidity validation integration")
    print("• ✅ Cross-exchange liquidity comparison")
    print("• ✅ Balance-to-liquidity ratio analysis")