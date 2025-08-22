
import asyncio
import logging
import time
import json
import os

# Configure logging to suppress WebSocket debug messages
logging.basicConfig(level=logging.DEBUG)  # Temporary debug level
logging.getLogger("websockets.client").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
from datetime import datetime
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, List, Tuple
import aiohttp

# Import environment variable manager
from utils.env_manager import EnvManager

from x10.perpetual.configuration import STARKNET_MAINNET_CONFIG
from x10.perpetual.stream_client import PerpetualStreamClient
from x10.perpetual.accounts import StarkPerpetualAccount
from x10.perpetual.funding_rates import FundingRateModel

# Import your working functions
from lightersetup import get_market_data_clean_async

# Import dual orderbook management system
from orderbooks import DualOrderbookManager, OrderbookState, OrderbookLevel
from account import DualExchangeAccountManager, create_account_manager, AccountBalance
from utils.types import Exchange




@dataclass
class ArbitrageOpportunity:
    """Complete arbitrage opportunity with all metrics"""
    extended_symbol: str
    lighter_symbol: str
    lighter_market_id: int  # Required for WebSocket orderbook subscription
    extended_rate: Decimal
    lighter_rate: Decimal
    spread: Decimal
    spread_bps: float
    strategy: str
    annual_yield: float
    daily_yield: float
    breakeven_days: float
    profit_score: float  # Combined profitability score
    volume_score: float  # Liquidity/volume score
    timestamp: int


@dataclass
class FundingRate:
    exchange: Exchange
    symbol: str
    rate: Decimal
    timestamp: int
    volume: Optional[str] = None
    source: str = "unknown"


class CrossExchangeMapper:
    """Dynamic market mapper based on symbol matching"""
    
    @classmethod
    def build_dynamic_mapping(cls, extended_markets: List[str], lighter_markets: List[str]) -> Dict[str, str]:
        """
        Build dynamic mapping between Extended and Lighter markets.
        
        Extended format: "BTC-USD", "ETH-USD", "SOL-USD"
        Lighter format: "BTC", "ETH", "SOL"
        
        Logic: Remove "-USD" from Extended symbols and match with Lighter
        """
        mapping = {}
        
        for extended_symbol in extended_markets:
            # Method 1: Remove "-USD" suffix
            if extended_symbol.endswith("-USD"):
                base_symbol = extended_symbol[:-4]  # Remove "-USD"
                if base_symbol in lighter_markets:
                    mapping[extended_symbol] = base_symbol
                    continue
            
            # Method 2: Direct match (in case formats are identical)
            if extended_symbol in lighter_markets:
                mapping[extended_symbol] = extended_symbol
                continue
            
            # Method 3: Try removing common suffixes
            for suffix in ["-USDT", "-PERP"]:
                if extended_symbol.endswith(suffix):
                    base_symbol = extended_symbol.replace(suffix, "")
                    if base_symbol in lighter_markets:
                        mapping[extended_symbol] = base_symbol
                        break
        
        return mapping
    
    @classmethod
    def get_lighter_symbol(cls, extended_symbol: str, lighter_markets: List[str]) -> Optional[str]:
        """Get corresponding Lighter symbol for Extended market"""
        # Remove "-USD" suffix first
        if extended_symbol.endswith("-USD"):
            base_symbol = extended_symbol[:-4]
            if base_symbol in lighter_markets:
                return base_symbol
        
        # Direct match
        if extended_symbol in lighter_markets:
            return extended_symbol
        
        return None


class TopArbitrageScanner:
    """Scans all markets to find top arbitrage opportunities"""
    
    def __init__(self, api_key: Optional[str] = None, account_manager: Optional[DualExchangeAccountManager] = None):
        self.logger = logging.getLogger("TopArbitrageScanner")
        self.extended_base_url = "https://api.extended.exchange/api/v1"
        self.api_key = api_key
        
        # Account manager for hedged trading
        self.account_manager = account_manager
        
        # Trading parameters for risk management
        self.min_profit_threshold_bps = 5.0  # Minimum 5 basis points profit to trigger trade
        self.max_order_size_usd = 100.0  # Maximum $100 per trade for safety
        self.auto_execute = False  # Disabled by default for safety
        
        # Dynamic market mapping will be built from available markets
        self.market_mapping: Dict[str, str] = {}
        self.reverse_mapping: Dict[str, str] = {}  # Lighter -> Extended
        
    def _get_extended_headers(self):
        headers = {"accept": "application/json"}
        if self.api_key:
            headers["X-Api-Key"] = self.api_key
        return headers
    
    async def get_all_extended_rates(self) -> Dict[str, Dict]:
        """Get all Extended markets with funding rates and volume data"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.extended_base_url}/info/markets",
                    headers=self._get_extended_headers(),
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        
                        if data.get("status") in ["ok", "OK"]:
                            extended_markets = {}
                            
                            for market_data in data["data"]:
                                if (market_data.get("active", False) and 
                                    market_data.get("status") == "ACTIVE"):
                                    
                                    name = market_data["name"]
                                    stats = market_data.get("marketStats", {})
                                    
                                    extended_markets[name] = {
                                        "rate": Decimal(str(stats.get("fundingRate", "0"))),
                                        "volume": stats.get("dailyVolume", "0"),
                                        "last_price": stats.get("lastPrice", "0"),
                                        "open_interest": stats.get("openInterest", "0"),
                                        "next_funding_time": stats.get("nextFundingRate", 0)
                                    }
                            
                            self.logger.info(f"Fetched {len(extended_markets)} Extended active markets")
                            return extended_markets
                        else:
                            error_msg = data.get("error", {}).get("message", "Unknown API error")
                            self.logger.error(f"Extended API error: {error_msg}")
                            return {}
                    else:
                        self.logger.error(f"Extended API HTTP error: {response.status}")
                        return {}
                        
        except Exception as e:
            self.logger.error(f"Failed to fetch Extended markets: {e}")
            return {}
    
    async def get_all_lighter_rates(self) -> Dict[str, Dict]:
        """Get all Lighter markets with funding rates"""
        try:
            response = await get_market_data_clean_async()
            
            if response["success"] and response["data"]:
                lighter_markets = {}
                
                for instrument in response["data"]:
                    symbol = instrument["symbol"]
                    rate = Decimal(str(instrument["rate"]))
                    
                    lighter_markets[symbol] = {
                        "rate": rate,
                        "market_id": instrument.get("market_id"),
                        "exchange": instrument.get("exchange", "lighter")
                    }
                
                self.logger.info(f"Fetched {len(lighter_markets)} Lighter markets")
                return lighter_markets
            else:
                self.logger.error(f"Lighter API error: {response.get('error')}")
                return {}
                
        except Exception as e:
            self.logger.error(f"Failed to fetch Lighter markets: {e}")
            return {}
    
    def build_market_mapping(self, extended_markets: Dict[str, Dict], lighter_markets: Dict[str, Dict]):
        """Build dynamic market mapping based on available markets"""
        
        self.market_mapping.clear()
        self.reverse_mapping.clear()
        
        # Use the dynamic mapper
        extended_symbols = list(extended_markets.keys())
        lighter_symbols = list(lighter_markets.keys())
        
        self.market_mapping = CrossExchangeMapper.build_dynamic_mapping(extended_symbols, lighter_symbols)
        
        # Build reverse mapping
        for ext, ltr in self.market_mapping.items():
            self.reverse_mapping[ltr] = ext
        
        self.logger.info(f"Built dynamic market mapping: {len(self.market_mapping)} arbitrageable pairs")
        
        # Show the mappings we found
        if self.market_mapping:
            self.logger.info(f"Available arbitrage pairs:")
            for ext, ltr in list(self.market_mapping.items())[:15]:  # Show first 15
                self.logger.info(f"   {ext} <-> {ltr}")
        else:
            self.logger.warning("No market mappings found between exchanges")
    
    async def scan_all_opportunities(self) -> List[ArbitrageOpportunity]:
        """Scan all markets and return sorted list of arbitrage opportunities"""
        
        self.logger.info("SCANNING ALL MARKETS FOR ARBITRAGE OPPORTUNITIES")
        
        # Get data from both exchanges
        extended_markets = await self.get_all_extended_rates()
        lighter_markets = await self.get_all_lighter_rates()
        
        if not extended_markets or not lighter_markets:
            self.logger.error("Failed to get market data from one or both exchanges")
            return []
        
        # Build dynamic market mapping
        self.build_market_mapping(extended_markets, lighter_markets)
        
        if not self.market_mapping:
            self.logger.error("No arbitrageable market pairs found")
            return []
        
        opportunities = []
        current_time = int(time.time() * 1000)  # Fix: Define current_time here
        
        # Find markets available on both exchanges
        for extended_symbol, lighter_symbol in self.market_mapping.items():
            extended_data = extended_markets[extended_symbol]
            lighter_data = lighter_markets[lighter_symbol]
            
            extended_rate = extended_data["rate"]
            lighter_rate = lighter_data["rate"]
            
            spread = abs(extended_rate - lighter_rate)
            spread_bps = float(spread * 10000)
            
            # Skip tiny spreads (< 0.1 bps)
            if spread_bps < 0.1:
                continue
            
            # Determine strategy
            if extended_rate > lighter_rate:
                strategy = "Short Extended, Long Lighter"
            else:
                strategy = "Long Extended, Short Lighter"
            
            # Calculate yields
            daily_yield = float(spread * Decimal("3") * Decimal("100"))
            annual_yield = float(spread * Decimal("3") * Decimal("365") * Decimal("100"))

            
            # Break-even analysis
            round_trip_cost_bps = Decimal("8.0")
            if spread_bps > 0:
                breakeven_periods = round_trip_cost_bps / Decimal(str(spread_bps))
                breakeven_days = float(breakeven_periods * Decimal("8") / Decimal("24"))
            else:
                breakeven_days = float('inf')
            
            # Calculate composite profit score
            # Factors: spread size, volume, break-even time
            volume_score = self._calculate_volume_score(extended_data.get("volume", "0"))
            profit_score = float(spread_bps) * volume_score * (1.0 / max(breakeven_days, 0.1))
            
            opportunity = ArbitrageOpportunity(
                extended_symbol=extended_symbol,
                lighter_symbol=lighter_symbol,
                lighter_market_id=lighter_data["market_id"],  # WebSocket orderbook index
                extended_rate=extended_rate,
                lighter_rate=lighter_rate,
                spread=spread,
                spread_bps=spread_bps,
                strategy=strategy,
                annual_yield=annual_yield,
                daily_yield=daily_yield,
                breakeven_days=breakeven_days,
                profit_score=profit_score,
                volume_score=volume_score,
                timestamp=current_time
            )
            
            opportunities.append(opportunity)
        
        # Sort by profit score (best opportunities first)
        opportunities.sort(key=lambda x: x.profit_score, reverse=True)
        
        self.logger.info(f"Found {len(opportunities)} total arbitrage opportunities")
        
        # Auto-execute hedged orders if conditions are met
        if (self.account_manager and 
            self.auto_execute and 
            opportunities and 
            opportunities[0].spread_bps > self.min_profit_threshold_bps):
            
            top_opportunity = opportunities[0]
            self.logger.info(f"🎯 AUTO-EXECUTING top opportunity: {top_opportunity.extended_symbol} "
                            f"spread {top_opportunity.spread_bps:.2f}bps (threshold: {self.min_profit_threshold_bps}bps)")
            
            try:
                result = await self.execute_arbitrage_opportunity(top_opportunity)
                if result["success"]:
                    self.logger.info(f"✅ Hedged orders placed successfully for {top_opportunity.extended_symbol}")
                    self.logger.info(f"   Hedge size: {result.get('hedge_size', 'N/A')}")
                    self.logger.info(f"   Target price: ${result.get('target_price', 'N/A')}")
                else:
                    self.logger.warning(f"❌ Auto-execution failed for {top_opportunity.extended_symbol}: {result['error']}")
            except Exception as e:
                self.logger.error(f"🐛 Auto-execution error for {top_opportunity.extended_symbol}: {e}")
        elif opportunities and opportunities[0].spread_bps > self.min_profit_threshold_bps:
            # Log opportunity but don't execute
            top_opportunity = opportunities[0]
            self.logger.info(f"💡 PROFITABLE opportunity detected (auto-execute disabled): "
                            f"{top_opportunity.extended_symbol} spread {top_opportunity.spread_bps:.2f}bps")
        
        return opportunities
    
    def _calculate_volume_score(self, volume_str: str) -> float:
        """Calculate volume score for liquidity weighting"""
        try:
            volume = float(volume_str)
            # Logarithmic scale: higher volume = better score
            if volume > 1000000:
                return 1.0  # Excellent liquidity
            elif volume > 100000:
                return 0.8  # Good liquidity
            elif volume > 10000:
                return 0.6  # Moderate liquidity
            elif volume > 1000:
                return 0.4  # Low liquidity
            else:
                return 0.2  # Very low liquidity
        except:
            return 0.3  # Unknown volume

    async def execute_arbitrage_opportunity(self, opportunity: ArbitrageOpportunity) -> Dict[str, any]:
        """Execute hedged orders for a profitable arbitrage opportunity"""
        if not self.account_manager:
            return {"success": False, "error": "No account manager available"}
        
        if opportunity.spread_bps < self.min_profit_threshold_bps:
            return {"success": False, "error": f"Spread {opportunity.spread_bps:.2f}bps below minimum {self.min_profit_threshold_bps}bps"}
        
        # Determine Extended side based on which exchange has higher funding rate
        extended_side = "BUY" if opportunity.extended_rate > opportunity.lighter_rate else "SELL"
        
        # Calculate safe order size (smaller of max size or available balance * 0.8)
        max_size_decimal = Decimal(str(self.max_order_size_usd))
        conservative_size = Decimal("50.0") 
        order_size_usd = min(max_size_decimal, conservative_size)
        
        # Execute hedged orders
        return await self.account_manager.place_hedged_funding_orders(
            extended_market=opportunity.extended_symbol,
            lighter_market_id=opportunity.lighter_market_id,
            target_price=Decimal("50000.0"),  # Will need real market price
            order_size=order_size_usd / Decimal("50000.0"),
            extended_side=extended_side
        )


    
    async def get_top_opportunities(self, top_n: int = 10) -> List[ArbitrageOpportunity]:
        """Get top N arbitrage opportunities ranked by profitability"""
        
        opportunities = await self.scan_all_opportunities()
        
        if opportunities:
            top_opportunities = opportunities[:top_n]
            
            self.logger.info(f"TOP {len(top_opportunities)} ARBITRAGE OPPORTUNITIES:")
            self.logger.info(f"   {'Rank':<4} {'Extended':<12} {'Lighter':<8} {'Spread':<8} {'APY':<8} {'B/E Days':<8} {'Strategy'}")
            self.logger.info(f"   {'-'*80}")
            
            for i, opp in enumerate(top_opportunities, 1):
                profitable_indicator = "[PROFIT]" if opp.spread_bps >= 5 else "[GOOD]" if opp.spread_bps >= 1 else "[TRACK]"
                
                self.logger.info(
                    f"   {i:<4} {opp.extended_symbol:<12} {opp.lighter_symbol:<8} "
                    f"{opp.spread_bps:>6.2f}bps {opp.annual_yield:>+6.1f}% {opp.breakeven_days:>6.1f}d "
                    f"{opp.strategy[:20]} {profitable_indicator}"
                )
            
            return top_opportunities
        else:
            self.logger.warning("No arbitrage opportunities found")
            return []


class TopArbitrageMonitor:
    """Monitor top arbitrage opportunities in real-time"""
    
    def __init__(self, stark_account: StarkPerpetualAccount, top_n: int = 10):
        self.logger = logging.getLogger("TopArbitrageMonitor")
        self.stark_account = stark_account
        self.top_n = top_n
        
        # Components
        self.scanner = TopArbitrageScanner()
        
        # Current state
        self.top_opportunities: List[ArbitrageOpportunity] = []
        self.funding_rates: Dict[str, FundingRate] = {}
        
        # Performance tracking
        self.scan_count = 0
        self.total_opportunities_detected = 0
        self.start_time = None
        
        # Refresh intervals
        self.scan_interval = 300  # Re-scan top opportunities every 5 minutes
        self.rate_update_interval = 30  # Update rates every 30 seconds
    
    def get_rate_key(self, exchange: Exchange, symbol: str) -> str:
        return f"{exchange.value}_{symbol}"
    
    async def refresh_top_opportunities(self):
        """Refresh the list of top opportunities"""
        self.scan_count += 1
        self.logger.info(f"REFRESHING TOP OPPORTUNITIES (Scan #{self.scan_count})")
        
        new_opportunities = await self.scanner.get_top_opportunities(self.top_n)
        
        if new_opportunities:
            self.top_opportunities = new_opportunities
            
            # Log changes in top opportunities
            top_symbols = [f"{opp.extended_symbol}/{opp.lighter_symbol}" for opp in new_opportunities[:5]]
            self.logger.info(f"Current top 5: {', '.join(top_symbols)}")
            
            return True
        else:
            self.logger.warning("No opportunities found in refresh")
            return False
    
    async def monitor_extended_websocket_for_top(self, market: str):
        """Monitor Extended WebSocket for top opportunity markets"""
        stream_client = PerpetualStreamClient(api_url=STARKNET_MAINNET_CONFIG.stream_url)
        
        try:
            async with stream_client.subscribe_to_funding_rates(market) as stream:
                self.logger.info(f"Extended WebSocket monitoring: {market}")
                
                async for funding_update in stream:
                    try:
                        # Parse Extended WebSocket data correctly
                        if hasattr(funding_update, 'data') and funding_update.data:
                            
                            # Check if it's FundingRateModel (which it is, based on your debug)
                            if isinstance(funding_update.data, FundingRateModel):
                                market_name = funding_update.data.market
                                rate = funding_update.data.funding_rate  # Already a Decimal
                                timestamp = funding_update.data.timestamp
                                
                                # Update funding rate
                                key = self.get_rate_key(Exchange.EXTENDED, market_name)
                                self.funding_rates[key] = FundingRate(
                                    exchange=Exchange.EXTENDED,
                                    symbol=market_name,
                                    rate=rate,
                                    timestamp=timestamp,
                                    source="websocket"
                                )
                                
                                rate_bps = float(rate * 10000)
                                self.logger.info(f"Extended {market_name} LIVE: {float(rate):>8.6f} ({rate_bps:>+6.2f} bps)")
                                
                                # Check if this affects our top opportunities
                                await self._check_opportunity_impact(market_name)
                                
                            # Fallback: Try direct attribute access (legacy support)
                            elif hasattr(funding_update.data, 'm') and hasattr(funding_update.data, 'f'):
                                market_name = str(funding_update.data.m)
                                rate = Decimal(str(funding_update.data.f))
                                timestamp = getattr(funding_update.data, 'T', int(time.time() * 1000))
                                
                                # Update funding rate
                                key = self.get_rate_key(Exchange.EXTENDED, market_name)
                                self.funding_rates[key] = FundingRate(
                                    exchange=Exchange.EXTENDED,
                                    symbol=market_name,
                                    rate=rate,
                                    timestamp=timestamp,
                                    source="websocket"
                                )
                                
                                rate_bps = float(rate * 10000)
                                self.logger.info(f"Extended {market_name} LIVE: {float(rate):>8.6f} ({rate_bps:>+6.2f} bps)")
                                
                                await self._check_opportunity_impact(market_name)
                                
                            else:
                                self.logger.warning(f"Extended {market}: Unknown data format: {type(funding_update.data)}")
                                self.logger.debug(f"Data repr: {repr(funding_update.data)}")
                                
                    except Exception as e:
                        self.logger.error(f"Error processing Extended data for {market}: {e}")
                        self.logger.debug(f"Raw funding_update: {funding_update}")
                        continue
                        
        except Exception as e:
            self.logger.error(f"Extended WebSocket error for {market}: {e}")
    
    async def monitor_lighter_rates_for_top(self):
        """Monitor Lighter rates for top opportunity markets"""
        while True:
            try:
                response = await get_market_data_clean_async()
                
                if response["success"] and response["data"]:
                    # Update all Lighter rates
                    for instrument in response["data"]:
                        symbol = instrument["symbol"]
                        rate = Decimal(str(instrument["rate"]))
                        timestamp = int(time.time() * 1000)
                        
                        key = self.get_rate_key(Exchange.LIGHTER, symbol)
                        self.funding_rates[key] = FundingRate(
                            exchange=Exchange.LIGHTER,
                            symbol=symbol,
                            rate=rate,
                            timestamp=timestamp,
                            source="rest_api"
                        )
                    
                    self.logger.debug(f"Updated {len(response['data'])} Lighter rates")
                    
                    # Check impact on top opportunities
                    for opp in self.top_opportunities:
                        if opp.lighter_symbol in [inst["symbol"] for inst in response["data"]]:
                            await self._check_opportunity_impact(opp.extended_symbol)
                
            except Exception as e:
                self.logger.error(f"Lighter monitoring error: {e}")
            
            await asyncio.sleep(60)  # Update every minute
    
    async def _check_opportunity_impact(self, extended_symbol: str):
        """Check if rate update impacts existing opportunities"""
        
        # Find the opportunity for this market
        affected_opp = next((opp for opp in self.top_opportunities if opp.extended_symbol == extended_symbol), None)
        
        if not affected_opp:
            return
        
        # Get current rates
        extended_key = self.get_rate_key(Exchange.EXTENDED, extended_symbol)
        lighter_key = self.get_rate_key(Exchange.LIGHTER, affected_opp.lighter_symbol)
        
        extended_rate = self.funding_rates.get(extended_key)
        lighter_rate = self.funding_rates.get(lighter_key)
        
        if extended_rate and lighter_rate:
            new_spread = abs(extended_rate.rate - lighter_rate.rate)
            new_spread_bps = float(new_spread * 10000)
            
            # Log significant changes
            old_spread_bps = affected_opp.spread_bps
            change_bps = new_spread_bps - old_spread_bps
            
            if abs(change_bps) >= 0.5:  # Log changes >= 0.5 bps
                self.logger.info(f"{extended_symbol} SPREAD UPDATE: {old_spread_bps:.2f} -> {new_spread_bps:.2f} bps ({change_bps:+.2f})")
                
                if new_spread_bps >= 5:
                    self.logger.info(f"   NOW HIGHLY PROFITABLE!")
                elif new_spread_bps >= 1:
                    self.logger.info(f"   Profitable opportunity")
    
    async def show_live_top_opportunities(self):
        """Show current top opportunities status"""
        while True:
            await asyncio.sleep(120)  # Every 2 minutes
            
            if not self.top_opportunities:
                continue
            
            elapsed = time.time() - self.start_time if self.start_time else 0
            
            self.logger.info(f"LIVE TOP OPPORTUNITIES STATUS (after {elapsed/60:.1f}m)")
            self.logger.info(f"   Total scans: {self.scan_count}")
            self.logger.info(f"   Opportunities tracked: {len(self.top_opportunities)}")
            
            # Show current status of top 5
            current_profitable = 0
            
            self.logger.info(f"   Current top 5 spreads:")
            
            for i, opp in enumerate(self.top_opportunities[:5], 1):
                # Get latest rates
                extended_key = self.get_rate_key(Exchange.EXTENDED, opp.extended_symbol)
                lighter_key = self.get_rate_key(Exchange.LIGHTER, opp.lighter_symbol)
                
                extended_rate = self.funding_rates.get(extended_key)
                lighter_rate = self.funding_rates.get(lighter_key)
                
                if extended_rate and lighter_rate:
                    current_spread = abs(extended_rate.rate - lighter_rate.rate)
                    current_spread_bps = float(current_spread * 10000)
                    
                    status = "[PROFIT]" if current_spread_bps >= 5 else "[GOOD]" if current_spread_bps >= 1 else "[TRACK]"
                    age_ext = int(time.time() - extended_rate.timestamp/1000)
                    age_ltr = int(time.time() - lighter_rate.timestamp/1000)
                    
                    self.logger.info(
                        f"     {i}. {opp.extended_symbol:>8}: {current_spread_bps:>5.2f}bps "
                        f"(Ext:{age_ext:>2}s, Ltr:{age_ltr:>2}s) {status}"
                    )
                    
                    if current_spread_bps >= 5:
                        current_profitable += 1
                else:
                    self.logger.info(f"     {i}. {opp.extended_symbol:>8}: No current data")
            
            self.logger.info(f"   Currently profitable (>= 5 bps): {current_profitable}/5")
    
    async def start_monitoring_top_opportunities(self):
        """Start monitoring the top arbitrage opportunities"""
        self.start_time = time.time()
        
        self.logger.info(f"STARTING TOP {self.top_n} ARBITRAGE OPPORTUNITIES MONITOR")
        
        # Initial scan to find top opportunities
        await self.refresh_top_opportunities()
        
        if not self.top_opportunities:
            self.logger.error("No opportunities found to monitor")
            return
        
        # Get the markets we need to monitor
        extended_markets_to_monitor = [opp.extended_symbol for opp in self.top_opportunities]
        
        self.logger.info(f"Monitoring top markets: {extended_markets_to_monitor}")
        
        # Create monitoring tasks
        tasks = []
        
        # Extended WebSocket monitoring for top markets
        for market in extended_markets_to_monitor:
            tasks.append(asyncio.create_task(
                self.monitor_extended_websocket_for_top(market),
                name=f"extended_ws_{market}"
            ))
        
        # Lighter REST monitoring
        tasks.append(asyncio.create_task(
            self.monitor_lighter_rates_for_top(),
            name="lighter_monitor"
        ))
        
        # Periodic opportunity refresh
        async def periodic_refresh():
            while True:
                await asyncio.sleep(self.scan_interval)
                await self.refresh_top_opportunities()
        
        tasks.append(asyncio.create_task(periodic_refresh(), name="opportunity_refresh"))
        
        # Live status display
        tasks.append(asyncio.create_task(
            self.show_live_top_opportunities(),
            name="live_status"
        ))
        
        try:
            self.logger.info(f"Launching {len(tasks)} monitoring tasks for top opportunities...")
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"Top opportunities monitoring error: {e}")
        finally:
            self.logger.info("Top arbitrage monitoring stopped")


# Main execution functions
async def scan_and_show_top_opportunities():
    """Scan all markets and show top opportunities"""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s'
    )
    
    logger = logging.getLogger("TopOpportunitiesScan")
    logger.info("SCANNING ALL MARKETS FOR TOP ARBITRAGE OPPORTUNITIES")
    
    try:
        scanner = TopArbitrageScanner()
        top_opportunities = await scanner.get_top_opportunities(20)  # Get top 20
        
        if top_opportunities:
            # Separate into categories
            highly_profitable = [opp for opp in top_opportunities if opp.spread_bps >= 5]
            profitable = [opp for opp in top_opportunities if 1 <= opp.spread_bps < 5]
            marginal = [opp for opp in top_opportunities if opp.spread_bps < 1]
            
            logger.info(f"OPPORTUNITY CATEGORIES:")
            logger.info(f"   HIGHLY PROFITABLE (>= 5 bps): {len(highly_profitable)}")
            logger.info(f"   PROFITABLE (1-5 bps): {len(profitable)}")
            logger.info(f"   MARGINAL (< 1 bps): {len(marginal)}")
            
            # Show detailed analysis for top 10
            logger.info(f"TOP 10 DETAILED ANALYSIS:")
            
            for i, opp in enumerate(top_opportunities[:10], 1):
                logger.info(f"   {i}. {opp.extended_symbol} / {opp.lighter_symbol}")
                logger.info(f"      Extended rate: {float(opp.extended_rate):>8.6f} ({float(opp.extended_rate)*10000:>+6.2f} bps)")
                logger.info(f"      Lighter rate:  {float(opp.lighter_rate):>8.6f} ({float(opp.lighter_rate)*10000:>+6.2f} bps)")
                logger.info(f"      Spread:        {float(opp.spread):>8.6f} ({opp.spread_bps:>6.2f} bps)")
                logger.info(f"      Strategy:      {opp.strategy}")
                logger.info(f"      Break-even:    {opp.breakeven_days:.1f} days")
                logger.info(f"      Daily yield:   {opp.daily_yield:.4f}%")
                logger.info(f"      Annual yield:  {opp.annual_yield:+.2f}%")
                logger.info(f"      Profit score:  {opp.profit_score:.2f}")
                
                if opp.spread_bps >= 5:
                    logger.info(f"      Status:        EXECUTE IMMEDIATELY")
                elif opp.spread_bps >= 1:
                    logger.info(f"      Status:        Consider execution")
                else:
                    logger.info(f"      Status:        Monitor for changes")
            
            # Save top opportunities
            filename = f"top_opportunities_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            opportunities_data = []
            
            for opp in top_opportunities:
                opportunities_data.append({
                    "rank": top_opportunities.index(opp) + 1,
                    "extended_symbol": opp.extended_symbol,
                    "lighter_symbol": opp.lighter_symbol,
                    "extended_rate": float(opp.extended_rate),
                    "lighter_rate": float(opp.lighter_rate),
                    "spread_bps": opp.spread_bps,
                    "strategy": opp.strategy,
                    "annual_yield": opp.annual_yield,
                    "breakeven_days": opp.breakeven_days,
                    "profit_score": opp.profit_score,
                    "timestamp": opp.timestamp
                })
            
            with open(filename, 'w') as f:
                json.dump(opportunities_data, f, indent=2)
            
            logger.info(f"Top opportunities saved to: {filename}")
            
            return top_opportunities
        else:
            logger.warning("No arbitrage opportunities found")
            return []
            
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        import traceback
        traceback.print_exc()
        return []


async def run_top_opportunities_monitor():
    """Run the top opportunities monitoring system"""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'top_arbitrage_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )
    
    logger = logging.getLogger("TopArbitrageMain")
    logger.info("=== TOP ARBITRAGE OPPORTUNITIES MONITORING SYSTEM ===")
    
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
    
    logger.info(f"Extended account ready: vault {vault}")
    
    # Continue with monitor initialization
    # TODO: Initialize the TopArbitrageMonitor with the account and start monitoring


async def test_dual_orderbook_monitoring():
    """
    Test the dual orderbook monitoring system with real-time data from both exchanges.
    
    This function demonstrates the complete pipeline:
    1. Find top arbitrage opportunity
    2. Initialize DualOrderbookManager  
    3. Monitor both Extended and Lighter orderbooks simultaneously
    4. Display real-time arbitrage execution analysis
    """
    print("🔍 Step 1: Finding top arbitrage opportunity...")
    
    # Find the best arbitrage opportunity
    scanner = TopArbitrageScanner()
    opportunities = await scanner.get_top_opportunities(3)
    
    if not opportunities:
        print("❌ No arbitrage opportunities found. Cannot test orderbook monitoring.")
        return
    
    # Use the top opportunity for testing
    top_opportunity = opportunities[0]
    print(f"✅ Top opportunity selected:")
    print(f"   Symbol Pair: {top_opportunity.extended_symbol} / {top_opportunity.lighter_symbol}")
    print(f"   Spread: {top_opportunity.spread_bps:.2f} bps")
    print(f"   Annual Yield: {top_opportunity.annual_yield:+.1f}%")
    
    # Initialize dual orderbook manager
    print(f"\n📊 Step 2: Initializing Dual Orderbook Manager...")
    dual_manager = DualOrderbookManager(top_opportunity)
    
    print(f"🚀 Step 3: Starting dual orderbook monitoring...")
    print(f"   Extended: {top_opportunity.extended_symbol} (WebSocket)")
    print(f"   Lighter: {top_opportunity.lighter_symbol} (REST API)")
    print(f"\n📈 Real-time arbitrage analysis will show every 10 seconds...")
    print(f"   Press Ctrl+C to stop monitoring\n")
    
    # Create enhanced analysis display task
    async def display_arbitrage_analysis():
        """Display enhanced real-time arbitrage analysis with bid/ask prices and mid prices"""
        
        def safe_format_price(value, decimals=6):
            """Safely format price values, handling None"""
            if value is None:
                return "N/A     "
            return f"${value:.{decimals}f}"
        
        def safe_format_number(value, decimals=2):
            """Safely format numeric values, handling None"""
            if value is None:
                return "N/A"
            return f"{value:.{decimals}f}"
        
        while dual_manager.is_running:
            try:
                # Get synchronized price data from both exchanges
                price_data = await dual_manager.get_synchronized_prices()
                
                if price_data["status"] == "complete":
                    extended = price_data["extended"]
                    lighter = price_data["lighter"]
                    arbitrage = price_data["arbitrage"]
                    
                    print("\n" + "="*80)
                    print(f"🚀 DUAL ORDERBOOK ARBITRAGE ANALYSIS - {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
                    print("="*80)
                    
                    # Extended Exchange Data with Diagnostic Info
                    print(f"📈 EXTENDED ({top_opportunity.extended_symbol}):")
                    print(f"   Best Bid:  {safe_format_price(extended.get('best_bid_price'))} ({safe_format_number(extended.get('best_bid_size', 0), 4)})")
                    print(f"   Best Ask:  {safe_format_price(extended.get('best_ask_price'))} ({safe_format_number(extended.get('best_ask_size', 0), 4)})")
                    print(f"   Mid Price: {safe_format_price(extended.get('mid_price'))}")
                    print(f"   W.Mid:     {safe_format_price(extended.get('weighted_mid_price'))}")
                    print(f"   Spread:    {safe_format_number(extended.get('spread_bps', 0))} bps")
                    
                    # DIAGNOSTIC: Show bid/ask counts for debugging
                    bid_count = extended.get('bid_levels', 0)
                    ask_count = extended.get('ask_levels', 0)
                    print(f"   📊 DEBUG: {bid_count} bids, {ask_count} asks")
                    
                    # Lighter Exchange Data  
                    print(f"\n📊 LIGHTER ({top_opportunity.lighter_symbol}):")
                    print(f"   Best Bid:  {safe_format_price(lighter.get('best_bid_price'))} ({safe_format_number(lighter.get('best_bid_size', 0), 4)})")
                    print(f"   Best Ask:  {safe_format_price(lighter.get('best_ask_price'))} ({safe_format_number(lighter.get('best_ask_size', 0), 4)})")
                    print(f"   Mid Price: {safe_format_price(lighter.get('mid_price'))}")
                    print(f"   W.Mid:     {safe_format_price(lighter.get('weighted_mid_price'))}")
                    print(f"   Spread:    {safe_format_number(lighter.get('spread_bps', 0))} bps")
                    
                    # Arbitrage Analysis
                    print(f"\n⚡ ARBITRAGE OPPORTUNITY:")
                    if arbitrage["is_viable"]:
                        print(f"   Status:    🟢 VIABLE")
                        print(f"   Direction: {arbitrage['recommended_action']}")
                        print(f"   Price Diff: ${arbitrage['price_diff']:.8f}")
                        print(f"   Diff (bps): {arbitrage['price_diff_bps']:+.2f}")
                    else:
                        print(f"   Status:    🔴 NOT VIABLE")
                        print(f"   Diff (bps): {arbitrage['price_diff_bps']:+.2f} (< 2.0 threshold)")
                    
                    # Connection Status
                    print(f"\n🔗 CONNECTION STATUS:")
                    extended_status = "🟢 Connected" if dual_manager.extended_connected else "🔴 Disconnected"
                    lighter_status = "🟢 Connected" if dual_manager.lighter_connected else "🔴 Disconnected"
                    print(f"   Extended:  {extended_status} ({dual_manager.extended_updates} updates)")
                    print(f"   Lighter:   {lighter_status} ({dual_manager.lighter_updates} updates)")
                    
                    # Latency Statistics
                    if dual_manager.latency_stats["extended"] and dual_manager.latency_stats["lighter"]:
                        avg_ext_latency = sum(dual_manager.latency_stats["extended"]) / len(dual_manager.latency_stats["extended"])
                        avg_lit_latency = sum(dual_manager.latency_stats["lighter"]) / len(dual_manager.latency_stats["lighter"])
                        print(f"   Avg Latency: Extended {avg_ext_latency:.3f}ms | Lighter {avg_lit_latency:.3f}ms")
                    
                else:
                    print(f"\n⏳ Waiting for complete orderbook data from both exchanges...")
                    print(f"   Reason: {price_data.get('reason', 'unknown')}")
                    extended_state = await dual_manager.get_extended_state()
                    lighter_state = await dual_manager.get_lighter_state()
                    print(f"   Extended: {'✅' if extended_state else '❌'} | Lighter: {'✅' if lighter_state else '❌'}")
                
                await asyncio.sleep(3.0)  # Update every 3 seconds for better readability
                    
            except Exception as e:
                print(f"Error in analysis display: {e}")
                await asyncio.sleep(5.0)
    
    # Start monitoring with analysis display
    try:
        # Run both monitoring and analysis display concurrently
        await asyncio.gather(
            dual_manager.start_monitoring(),
            display_arbitrage_analysis()
        )
    except KeyboardInterrupt:
        print(f"\n\n🛑 Dual orderbook monitoring stopped by user")
        await dual_manager.stop_monitoring()
        logger.info("Top opportunities monitoring stopped by user")
        
        # Show final summary
        logger.info(f"FINAL SUMMARY")
        logger.info(f"   Total scans performed: {monitor.scan_count}")
        logger.info(f"   Total opportunities detected: {monitor.total_opportunities_detected}")
        
        if monitor.top_opportunities:
            logger.info(f"   Final top opportunity: {monitor.top_opportunities[0].extended_symbol} ({monitor.top_opportunities[0].spread_bps:.2f} bps)")


if __name__ == "__main__":
    print("Choose test to run:")
    print("1. Quick Top 5 Scan")
    print("2. Full Top Opportunities Monitor")
    print("3. Dual Orderbook Test")
    print("4. All Systems Test")
    print("5. Integrated Hedged Trading Test")
    print("=" * 60)

    choice = input("\nSelect option (1-5): ").strip()

    if choice == "1":
        print("\nScanning for top opportunities...")
        asyncio.run(scan_and_show_top_opportunities())
        
    elif choice == "2":
        print("\nStarting live monitoring of top 10...")
        asyncio.run(run_top_opportunities_monitor())
        
    elif choice == "3":
        print("\nQuick top 5 scan...")
        
        async def quick_top_scan():
            scanner = TopArbitrageScanner()
            opportunities = await scanner.get_top_opportunities(5)
            
            if opportunities:
                print(f"TOP 5 OPPORTUNITIES RIGHT NOW:")
                for i, opp in enumerate(opportunities, 1):
                    print(f"   {i}. {opp.extended_symbol}/{opp.lighter_symbol}: {opp.spread_bps:.2f} bps ({opp.annual_yield:+.1f}% APY)")
            else:
                print("No opportunities found")
        
        asyncio.run(quick_top_scan())
        
    elif choice == "5":
        print("\n🚀 TESTING INTEGRATED HEDGED TRADING SYSTEM")
        asyncio.run(test_integrated_hedged_trading())
    
    elif choice == "4":
        print("\n🚀 TESTING DUAL ORDERBOOK MONITORING SYSTEM")
        print("=" * 50)
        print("This will:")
        print("• Find the top arbitrage opportunity")
        print("• Monitor Extended orderbook (WebSocket)")
        print("• Monitor Lighter orderbook (REST API)")
        print("• Show real-time bid/ask data from both exchanges")
        print("• Display arbitrage execution analysis")
        
        input("\nPress Enter to start dual orderbook test...")
        asyncio.run(test_dual_orderbook_monitoring())
    
    else:
        print("Invalid choice")