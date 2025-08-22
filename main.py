import asyncio
import logging
import time
import json
import signal
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, List
from dataclasses import dataclass
import os
import lighter
from config import config as app_config

# Import system components (enhanced versions)
from fund_test import TopArbitrageScanner, ArbitrageOpportunity
from orderbooks import DualOrderbookManager  # Enhanced with liquidity validation
from orders import OrderManager, ArbitrageOrderGroup, Exchange  # Enhanced with liquidity validation
from orders import OrderSide
from orders import create_lighter_client, close_lighter_client
from utils.env_manager import save_lighter_api_key, check_lighter_api_key

# Enhanced balance manager with margin parsing fix
from balance import BalanceManager

from x10.perpetual.accounts import StarkPerpetualAccount

from decimal import Decimal, InvalidOperation

def safe_decimal(value, default="0"):
    """Convert any value to Decimal safely"""
    try:
        if isinstance(value, Decimal):
            return value
        elif value is None:
            return Decimal(default)
        else:
            return Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation,):
        return Decimal(default)


# --- Lighter API key bootstrap ---
async def _bootstrap_lighter_api_key(*, base_url: str, eth_private_key: str, account_index: int, api_key_index: int) -> str:
    """Generate and register a Lighter API key using the provided Ethereum L1 key."""
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=base_url))
    try:
        # create a private/public key pair for the new API key
        private_key, public_key, err = lighter.create_api_key()
        if err is not None:
            raise RuntimeError(f"create_api_key error: {err}")

        # use a temporary signer client to perform change_api_key
        tx_client = lighter.SignerClient(
            url=base_url,
            private_key=private_key,
            account_index=account_index,
            api_key_index=api_key_index,
        )
        try:
            # register the new public key on-chain/back-end using ETH key
            _resp, err = await tx_client.change_api_key(
                eth_private_key=eth_private_key,
                new_pubkey=public_key,
            )
            if err is not None:
                raise RuntimeError(f"change_api_key error: {err}")

            # wait for API key propagation
            await asyncio.sleep(10)

            # verify
            err = tx_client.check_client()
            if err is not None:
                raise RuntimeError(f"check_client error after key setup: {err}")
        finally:
            await tx_client.close()

        return private_key
    finally:
        await api_client.close()


async def initialize_lighter_client(system, logger):
    """
    Initialize Lighter client with persistent API key management
    
    This function:
    1. Checks for existing API key in .env
    2. Validates existing key if found
    3. Generates new key if needed
    4. Saves generated key to .env for future use
    5. Returns configured client
    """
    lighter_client = None
    lighter_api_client = None
    
    try:
        # First, check if we already have a saved API key in .env
        saved_api_key = check_lighter_api_key()
        
        if saved_api_key:
            logger.info("📝 Found existing Lighter API key in .env file")
            logger.info(f"   Key (first 16 chars): {saved_api_key[:16]}...")
            
            # Try to use the saved key
            try:
                lighter_client, lighter_api_client = await create_lighter_client(
                    base_url=app_config.LIGHTER_BASE_URL,
                    private_key=saved_api_key,
                    account_index=app_config.LIGHTER_ACCOUNT_INDEX,
                    api_key_index=app_config.LIGHTER_API_KEY_INDEX,
                )
                
                # Test if the saved key works
                err = lighter_client.check_client()
                if err is None:
                    logger.info("✅ Existing Lighter API key validated successfully")
                    system.order_manager.lighter_client = lighter_client
                    return lighter_client, lighter_api_client
                else:
                    logger.warning(f"⚠️ Saved API key validation failed: {err}")
                    logger.info("🔄 Will generate new API key...")
                    # Close failed client
                    await close_lighter_client(lighter_client, lighter_api_client)
                    lighter_client = None
                    lighter_api_client = None
                    
            except Exception as e:
                logger.warning(f"⚠️ Failed to use saved API key: {e}")
                logger.info("🔄 Will generate new API key...")
        else:
            logger.info("📝 No saved Lighter API key found in .env")
        
        # If no saved key or saved key failed, check if one was provided in config
        if not lighter_client and app_config.LIGHTER_API_PRIVATE_KEY and app_config.LIGHTER_API_PRIVATE_KEY != saved_api_key:
            logger.info("📝 Using LIGHTER_API_PRIVATE_KEY from environment/config")
            try:
                lighter_client, lighter_api_client = await create_lighter_client(
                    base_url=app_config.LIGHTER_BASE_URL,
                    private_key=app_config.LIGHTER_API_PRIVATE_KEY,
                    account_index=app_config.LIGHTER_ACCOUNT_INDEX,
                    api_key_index=app_config.LIGHTER_API_KEY_INDEX,
                )
                
                # Test the key
                err = lighter_client.check_client()
                if err is None:
                    # Save this working key to .env for future use
                    if save_lighter_api_key(app_config.LIGHTER_API_PRIVATE_KEY):
                        logger.info("💾 Working API key saved to .env file for future use")
                    
                    system.order_manager.lighter_client = lighter_client
                    logger.info("✅ Lighter client initialized with config key")
                    return lighter_client, lighter_api_client
                else:
                    logger.warning(f"⚠️ Config API key validation failed: {err}")
                    await close_lighter_client(lighter_client, lighter_api_client)
                    lighter_client = None
                    
            except Exception as e:
                logger.warning(f"⚠️ Config API key failed: {e}")
                lighter_client = None
        
        # If still no client, bootstrap new API key
        if not lighter_client:
            if not app_config.ETH_PRIVATE_KEY:
                logger.error("❌ Cannot bootstrap: Missing ETH_PRIVATE_KEY")
                logger.error("   Please provide either:")
                logger.error("   1. LIGHTER_API_PRIVATE_KEY (existing API key)")
                logger.error("   2. ETH_PRIVATE_KEY (to generate new API key)")
                return None, None
            
            logger.info("🔑 Bootstrapping new Lighter API key...")
            logger.info("   This one-time process registers a new API key on-chain")
            
            try:
                # Generate and register new API key
                new_api_key = await _bootstrap_lighter_api_key(
                    base_url=app_config.LIGHTER_BASE_URL,
                    eth_private_key=app_config.ETH_PRIVATE_KEY,
                    account_index=app_config.LIGHTER_ACCOUNT_INDEX,
                    api_key_index=app_config.LIGHTER_API_KEY_INDEX,
                )
                
                logger.info("✅ New Lighter API key generated successfully")
                
                # Save the new key to .env file
                if save_lighter_api_key(new_api_key):
                    logger.info("💾 API key saved to .env file for future use")
                    logger.info("   Future runs will use this saved key automatically")
                    # Update in-memory config
                    app_config.LIGHTER_API_PRIVATE_KEY = new_api_key
                else:
                    logger.warning("⚠️ Could not save API key to .env automatically")
                    logger.warning(f"   Please manually add to .env:")
                    logger.warning(f"   LIGHTER_API_PRIVATE_KEY={new_api_key}")
                
                # Create client with new key
                lighter_client, lighter_api_client = await create_lighter_client(
                    base_url=app_config.LIGHTER_BASE_URL,
                    private_key=new_api_key,
                    account_index=app_config.LIGHTER_ACCOUNT_INDEX,
                    api_key_index=app_config.LIGHTER_API_KEY_INDEX,
                )
                
                system.order_manager.lighter_client = lighter_client
                logger.info("✅ Lighter client initialized with new API key")
                return lighter_client, lighter_api_client
                
            except Exception as be:
                logger.error(f"❌ Failed to bootstrap Lighter API key: {be}")
                return None, None
    
    except Exception as e:
        logger.error(f"❌ Lighter client initialization error: {e}")
        return None, None


@dataclass
class SystemConfig:
    """Enhanced system configuration parameters with liquidity validation settings"""
    # Opportunity scanning
    opportunity_scan_interval: int = 60  # seconds
    top_opportunities_count: int = 3  # Track top N opportunities
    
    # Execution thresholds
    min_execution_spread_bps: Decimal = Decimal("5.0")  # Minimum 5 bps to execute
    aggressive_execution_spread_bps: Decimal = Decimal("10.0")  # 10+ bps = aggressive execution
    max_position_size_usd: Decimal = Decimal("500.0")  # Maximum position size
    min_viable_spread_bps: Decimal = Decimal("1.0")  # Minimum viable spread to consider
    min_order_size_usd: Decimal = Decimal("10")  # Minimum order size constraint
    max_order_size_usd: Decimal = Decimal("1000")  # Maximum order size constraint
    
    # *** ENHANCED: Liquidity validation thresholds ***
    enable_liquidity_validation: bool = True  # Enable comprehensive liquidity validation
    max_liquidity_impact_score: Decimal = Decimal("75.0")  # Max acceptable market impact (0-100)
    min_market_depth_score: Decimal = Decimal("30.0")  # Minimum market depth quality
    max_balance_to_liquidity_ratio: Decimal = Decimal("1.0")  # Max balance relative to liquidity
    liquidity_safety_buffer: Decimal = Decimal("0.8")  # Use 80% of max safe liquidity
    require_cross_exchange_liquidity_check: bool = True  # Check both exchanges
    
    # Orderbook readiness
    orderbook_ready_cooldown_seconds: int = 2  # Cooldown after switching opp before execution
    
    # Risk management
    max_total_exposure_usd: Decimal = Decimal("2000.0")  # Maximum total exposure
    rebalance_threshold_percent: Decimal = Decimal("15.0")  # 15% imbalance triggers rebalance
    emergency_stop_loss_percent: Decimal = Decimal("2.0")  # 2% loss triggers emergency stop
    
    # Risk thresholds (wired from env)
    min_margin_ratio: Decimal = Decimal("0.10")  # 10%
    emergency_margin_ratio: Decimal = Decimal("0.05")  # 5%
    # Pre-exec balance threshold
    min_required_available_usd: Decimal = Decimal("20.0")
    
    # Performance tracking
    performance_log_interval: int = 120  # seconds (2 minutes)
    risk_check_interval: int = 30  # seconds
    
    # System behavior
    auto_execution_enabled: bool = True  # Enable/disable auto-execution
    conservative_mode: bool = True  # Conservative vs aggressive execution
    dry_run_mode: bool = False  # Paper trading mode (DEFAULT TO SAFE)


class HFTArbitrageSystem:
    """
    Enhanced main orchestrator for the HFT arbitrage trading system with 
    comprehensive liquidity validation to prevent market impact and ensure safe execution.
    """
    
    def __init__(self, stark_account: StarkPerpetualAccount, config: SystemConfig):
        self.stark_account = stark_account
        self.config = config
        self.logger = logging.getLogger("HFTArbitrageSystem")
        
        # System components - enhanced versions with liquidity validation
        self.scanner = TopArbitrageScanner()
        self.balance_manager = BalanceManager(stark_account)  # Enhanced balance manager
        self.order_manager = OrderManager(stark_account, self.balance_manager)  # Enhanced order manager
        self.orderbook_manager: Optional[DualOrderbookManager] = None  # Enhanced orderbook manager
        
        # *** ENHANCED: Configure liquidity validation settings ***
        if hasattr(self.balance_manager, 'risk_config'):
            self.balance_manager.risk_config.update({
                "min_margin_ratio": float(config.min_margin_ratio),
                "emergency_margin_ratio": float(config.emergency_margin_ratio),
                "use_effective_margin_ratio": True,
                "balance_health_check_enabled": True,
                "margin_calculation_fallback": True,
                "min_absolute_available_usd": float(config.min_required_available_usd)
            })
            self.logger.info("✅ Enhanced balance manager configured with intelligent margin thresholds")
        
        # Configure enhanced order manager with liquidity validation
        self.order_manager.risk_config.update({
            "require_liquidity_validation": config.enable_liquidity_validation,
            "max_liquidity_impact_score": float(config.max_liquidity_impact_score),
            "min_market_depth_score": float(config.min_market_depth_score),
            "max_balance_to_liquidity_ratio": float(config.max_balance_to_liquidity_ratio),
            "liquidity_safety_buffer": float(config.liquidity_safety_buffer),
        })
        self.logger.info("✅ Enhanced order manager configured with liquidity validation")
        
        # System state
        self.is_running = False
        self.current_opportunity: Optional[ArbitrageOpportunity] = None
        self.system_start_time = time.time()
        
        # Enhanced performance tracking with liquidity metrics
        self.performance_stats = {
            "opportunities_discovered": 0,
            "opportunities_executed": 0,
            "total_trades": 0,
            "total_volume_usd": Decimal("0"),
            "total_pnl_usd": Decimal("0"),
            "best_spread_captured_bps": Decimal("0"),
            "average_execution_latency_ms": 0,
            "risk_violations": 0,
            "emergency_stops": 0,
            # *** NEW: Liquidity-related performance metrics ***
            "liquidity_validations_performed": 0,
            "opportunities_rejected_for_liquidity": 0,
            "average_market_depth_score": 0.0,
            "liquidity_limited_executions": 0,
            "balance_to_liquidity_warnings": 0,
        }
        
        # Active monitoring
        self.active_arbitrage_groups: Dict[str, ArbitrageOrderGroup] = {}
        
        # Graceful shutdown handling
        self.shutdown_event = asyncio.Event()
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"🛑 Received shutdown signal: {signum}")
        asyncio.create_task(self._graceful_shutdown())
    
    async def start(self):
        """Start the complete HFT arbitrage system with enhanced liquidity validation"""
        self.logger.info("🚀 STARTING ENHANCED HFT ARBITRAGE SYSTEM WITH LIQUIDITY VALIDATION")
        self.logger.info("=" * 80)
        self.logger.info(f"   Mode: {'DRY RUN' if self.config.dry_run_mode else 'LIVE TRADING'}")
        self.logger.info(f"   Auto-execution: {'ENABLED' if self.config.auto_execution_enabled else 'DISABLED'}")
        self.logger.info(f"   Max position size: ${self.config.max_position_size_usd}")
        self.logger.info(f"   Min execution spread: {self.config.min_execution_spread_bps} bps")
        self.logger.info(f"   *** ENHANCED: Liquidity validation: {'ENABLED' if self.config.enable_liquidity_validation else 'DISABLED'}")
        self.logger.info(f"   *** ENHANCED: Max liquidity impact: {self.config.max_liquidity_impact_score}%")
        self.logger.info(f"   *** ENHANCED: Min market depth: {self.config.min_market_depth_score}/100")
        self.logger.info(f"   *** ENHANCED: Max balance/liquidity ratio: {self.config.max_balance_to_liquidity_ratio}")
        self.logger.info(f"   *** ENHANCED: Safety buffer: {self.config.liquidity_safety_buffer * 100}%")
        self.logger.info("=" * 80)
        
        self.is_running = True
        
        # Start system components
        tasks = [
            asyncio.create_task(self._run_opportunity_discovery(), name="opportunity_discovery"),
            asyncio.create_task(self._run_balance_monitoring(), name="balance_monitoring"),
            asyncio.create_task(self._run_performance_monitoring(), name="performance_monitoring"),
            asyncio.create_task(self._run_enhanced_risk_monitoring(), name="enhanced_risk_monitoring"),
            asyncio.create_task(self._handle_shutdown(), name="shutdown_handler")
        ]
        
        try:
            self.logger.info("🎯 All enhanced system components starting...")
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"System error: {e}")
            raise
        finally:
            await self._cleanup()
    
    async def _run_opportunity_discovery(self):
        """Continuously discover and update top arbitrage opportunities"""
        self.logger.info("🔍 Starting enhanced opportunity discovery engine")
        
        while self.is_running:
            try:
                start_time = time.time()
                
                # Scan for top opportunities
                opportunities = await self.scanner.get_top_opportunities(self.config.top_opportunities_count)
                
                if opportunities:
                    self.performance_stats["opportunities_discovered"] += len(opportunities)
                    
                    # Select best opportunity
                    best_opportunity = opportunities[0]
                    
                    # Check if we should switch to a new opportunity
                    if (not self.current_opportunity or 
                        safe_decimal(best_opportunity.spread_bps) > safe_decimal(self.current_opportunity.spread_bps) * Decimal("1.2")):  # 20% improvement
                        
                        await self._switch_to_opportunity(best_opportunity)
                    
                    # Log current status
                    self.logger.info(f"💰 TOP OPPORTUNITIES SCAN COMPLETE:")
                    for i, opp in enumerate(opportunities[:3], 1):
                        status = "🎯 ACTIVE" if opp == self.current_opportunity else "📊 TRACKING"
                        self.logger.info(f"   {i}. {opp.extended_symbol}/{opp.lighter_symbol}: "
                                       f"{opp.spread_bps:.2f} bps ({opp.annual_yield:+.1f}% APY) {status}")
                    
                    # *** ENHANCED: Check for execution with liquidity validation ***
                    if (self.config.auto_execution_enabled and 
                        safe_decimal(best_opportunity.spread_bps) >= self.config.min_execution_spread_bps):
                        
                        await self._execute_arbitrage_opportunity_enhanced(best_opportunity)
                
                # Calculate scan time
                scan_duration = time.time() - start_time
                self.logger.debug(f"Opportunity scan completed in {scan_duration:.3f}s")
                
                # Wait for next scan
                await asyncio.sleep(self.config.opportunity_scan_interval)
                
            except Exception as e:
                self.logger.error(f"Error in opportunity discovery: {e}")
                await asyncio.sleep(30)  # Wait longer on error
    
    async def _switch_to_opportunity(self, opportunity: ArbitrageOpportunity):
        """Switch to monitoring a new arbitrage opportunity with enhanced liquidity tracking"""
        self.logger.info(f"🔄 Switching to new opportunity: {opportunity.extended_symbol}/{opportunity.lighter_symbol}")
        self.logger.info(f"   Spread: {opportunity.spread_bps:.2f} bps")
        self.logger.info(f"   Annual yield: {opportunity.annual_yield:+.1f}%")
        
        # Stop current orderbook monitoring
        if self.orderbook_manager:
            await self.orderbook_manager.stop_monitoring()
        
        # Create new enhanced orderbook manager for this opportunity
        self.orderbook_manager = DualOrderbookManager(opportunity)  # Enhanced version
        self.order_manager.orderbook_manager = self.orderbook_manager
        
        # Start monitoring the new opportunity
        asyncio.create_task(self.orderbook_manager.start_monitoring(), name=f"orderbook_{opportunity.extended_symbol}")
        
        self.current_opportunity = opportunity
        
        self.logger.info(f"✅ Now monitoring enhanced orderbooks with liquidity validation for {opportunity.extended_symbol}/{opportunity.lighter_symbol}")
        
        # Allow orderbook streams to warm up before first execution attempt
        try:
            cooldown = int(self.config.orderbook_ready_cooldown_seconds)
            if cooldown > 0:
                self.logger.info(f"⏳ Cooling down {cooldown}s to ensure orderbooks are ready...")
                await asyncio.sleep(cooldown)
        except Exception:
            pass
    
    # ========================================================================
    # ENHANCED ARBITRAGE EXECUTION WITH COMPREHENSIVE LIQUIDITY VALIDATION
    # ========================================================================
    
    async def _execute_arbitrage_opportunity_enhanced(self, opportunity: ArbitrageOpportunity):
        """
        ENHANCED: Execute arbitrage opportunity with comprehensive pre-execution liquidity validation
        """
        try:
            self.logger.info(f"⚡ ENHANCED ARBITRAGE EXECUTION STARTING")
            self.logger.info(f"   Symbol: {opportunity.extended_symbol}/{opportunity.lighter_symbol}")
            self.logger.info(f"   Spread: {opportunity.spread_bps:.2f} bps")
            self.logger.info(f"   Target yield: {opportunity.annual_yield:+.1f}%")
            
            # *** ENHANCED: Comprehensive pre-execution validation ***
            validation_result = await self._validate_execution_conditions_enhanced(opportunity)
            if not validation_result["valid"]:
                self.logger.warning(f"⚠️ Enhanced pre-execution validation failed: {validation_result['reason']}")
                
                # Track rejection reasons for analytics
                if "liquidity" in validation_result["reason"].lower():
                    self.performance_stats["opportunities_rejected_for_liquidity"] += 1
                
                return
            
            # *** ENHANCED: Get liquidity-validated position size ***
            position_size_result = await self._calculate_enhanced_position_size(opportunity)
            if not position_size_result["valid"]:
                self.logger.warning(f"⚠️ Position sizing failed: {position_size_result['reason']}")
                self.performance_stats["liquidity_limited_executions"] += 1
                return
            
            position_size = position_size_result["size"]
            liquidity_analysis = position_size_result["liquidity_analysis"]
            
            # Determine execution mode
            aggressive = opportunity.spread_bps >= self.config.aggressive_execution_spread_bps
            
            self.logger.info(f"📊 Enhanced execution parameters:")
            self.logger.info(f"   Position size: {position_size}")
            self.logger.info(f"   Mode: {'AGGRESSIVE' if aggressive else 'CONSERVATIVE'}")
            self.logger.info(f"   Dry run: {'YES' if self.config.dry_run_mode else 'NO'}")
            self.logger.info(f"   Liquidity validated: {'YES' if liquidity_analysis.get('valid') else 'NO'}")
            
            if liquidity_analysis.get("valid"):
                self.logger.info(f"   Max safe trade USD: ${liquidity_analysis.get('max_safe_trade_usd', 0):,.0f}")
                self.logger.info(f"   Market depth score: {liquidity_analysis.get('depth_score', 0):.1f}/100")
                self.logger.info(f"   Liquidity impact: {liquidity_analysis.get('impact_score', 0):.1f}%")
            
            if self.config.dry_run_mode:
                # Enhanced paper trading simulation
                await self._simulate_arbitrage_execution_enhanced(opportunity, position_size, liquidity_analysis)
            else:
                # REAL order execution with liquidity validation
                await self._execute_live_arbitrage_enhanced(opportunity, position_size, aggressive, liquidity_analysis)
            
        except Exception as e:
            self.logger.error(f"Error executing enhanced arbitrage opportunity: {e}")
            self.performance_stats["risk_violations"] += 1

    async def _validate_execution_conditions_enhanced(self, opportunity: ArbitrageOpportunity) -> Dict:
        """
        ENHANCED: Comprehensive execution validation including liquidity checks
        """
        validation_result = {
            "valid": False,
            "reason": "",
            "checks_passed": [],
            "checks_failed": [],
            "liquidity_analysis": {}
        }
        
        try:
            # 1. System check
            if self.shutdown_event.is_set():
                validation_result["reason"] = "System shutdown in progress"
                validation_result["checks_failed"].append("system_shutdown")
                return validation_result
            
            validation_result["checks_passed"].append("system_active")
            
            # 2. *** ENHANCED: Orderbook connections and liquidity readiness ***
            if not self.orderbook_manager:
                validation_result["reason"] = "No orderbook manager available"
                validation_result["checks_failed"].append("no_orderbook_manager")
                return validation_result
            
            # Check if connections are ready for execution (includes liquidity validation)
            connections_ready = self.orderbook_manager.are_connections_ready_for_execution()
            if not connections_ready:
                validation_result["reason"] = "Orderbook connections not ready for execution"
                validation_result["checks_failed"].append("orderbook_not_ready")
                return validation_result
            
            validation_result["checks_passed"].append("orderbook_ready")
            
            # 3. *** ENHANCED: Cross-exchange liquidity validation ***
            if self.config.require_cross_exchange_liquidity_check:
                liquidity_comparison = await self.orderbook_manager.get_cross_exchange_liquidity_comparison()
                if not liquidity_comparison:
                    validation_result["reason"] = "Unable to analyze cross-exchange liquidity"
                    validation_result["checks_failed"].append("liquidity_analysis_failed")
                    return validation_result
                
                # Check if liquidity quality meets minimum requirements
                overall_quality = liquidity_comparison["recommendations"]["overall_quality"]
                if overall_quality < float(self.config.min_market_depth_score):
                    validation_result["reason"] = f"Market depth quality too low: {overall_quality:.1f} (min: {self.config.min_market_depth_score})"
                    validation_result["checks_failed"].append("insufficient_market_depth")
                    return validation_result
                
                validation_result["liquidity_analysis"] = liquidity_comparison
                validation_result["checks_passed"].append("liquidity_quality_check")
            
            # 4. Balance and margin check (enhanced)
            account_summary = await self.balance_manager.get_account_summary()
            total_available = float(account_summary["total_available"])
            min_required = float(self.config.min_required_available_usd)
            
            if total_available < min_required:
                validation_result["reason"] = f"Insufficient balance: ${total_available:.2f} < ${min_required}"
                validation_result["checks_failed"].append("insufficient_balance")
                return validation_result
            
            validation_result["checks_passed"].append("balance_check")
            
            # 5. *** ENHANCED: Balance to liquidity ratio check ***
            if self.config.enable_liquidity_validation:
                balance_analysis = await self.orderbook_manager.check_balance_to_liquidity_ratio(
                    Decimal(str(total_available))
                )
                
                if balance_analysis["balance_to_liquidity_ratio"] > float(self.config.max_balance_to_liquidity_ratio):
                    validation_result["reason"] = f"Balance too large relative to liquidity: ratio {balance_analysis['balance_to_liquidity_ratio']:.2f}"
                    validation_result["checks_failed"].append("balance_liquidity_ratio_exceeded")
                    self.performance_stats["balance_to_liquidity_warnings"] += 1
                    return validation_result
                
                validation_result["checks_passed"].append("balance_liquidity_ratio")
            
            # 6. Spread check
            if opportunity.spread_bps < self.config.min_execution_spread_bps:
                validation_result["reason"] = f"Spread too small: {opportunity.spread_bps:.2f} < {self.config.min_execution_spread_bps} bps"
                validation_result["checks_failed"].append("spread_too_small")
                return validation_result
            
            validation_result["checks_passed"].append("spread_check")
            
            # All checks passed
            validation_result["valid"] = True
            validation_result["reason"] = "All enhanced validation checks passed"
            
            self.logger.info("✅ ENHANCED VALIDATION PASSED - Ready for liquidity-validated execution!")
            self.logger.info(f"   Checks passed: {', '.join(validation_result['checks_passed'])}")
            
            # Update performance tracking
            self.performance_stats["liquidity_validations_performed"] += 1
            
            if validation_result["liquidity_analysis"]:
                quality = validation_result["liquidity_analysis"]["recommendations"]["overall_quality"]
                self.performance_stats["average_market_depth_score"] = (
                    (self.performance_stats["average_market_depth_score"] * (self.performance_stats["liquidity_validations_performed"] - 1) + quality) /
                    self.performance_stats["liquidity_validations_performed"]
                )
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Error in enhanced validation: {e}")
            validation_result["reason"] = f"Validation error: {e}"
            validation_result["checks_failed"].append("validation_exception")
            return validation_result

    async def _calculate_enhanced_position_size(self, opportunity: ArbitrageOpportunity) -> Dict:
        """
        ENHANCED: Calculate position size with comprehensive liquidity constraints
        """
        result = {
            "valid": False,
            "size": Decimal("0"),
            "reason": "",
            "liquidity_analysis": {}
        }
        
        try:
            # Get account balance
            account_summary = await self.balance_manager.get_account_summary()
            total_available = float(account_summary["total_available"])
            
            self.logger.info(f"💰 Enhanced Position Sizing Analysis:")
            self.logger.info(f"   Total available: ${total_available:.2f}")
            
            # Basic balance-based sizing (conservative)
            balance_based_size_usd = total_available * 0.3  # Use 30% of balance
            
            # *** ENHANCED: Get liquidity-constrained sizing ***
            if not self.orderbook_manager:
                self.logger.warning("No orderbook manager - using balance-only sizing")
                estimated_price = Decimal("50000")  # Fallback
                result["size"] = Decimal(str(balance_based_size_usd)) / estimated_price
                result["valid"] = True
                result["reason"] = "Balance-only sizing (no liquidity data)"
                return result
            
            # Get cross-exchange liquidity analysis
            liquidity_comparison = await self.orderbook_manager.get_cross_exchange_liquidity_comparison()
            if not liquidity_comparison:
                self.logger.warning("No liquidity comparison - using conservative balance sizing")
                estimated_price = Decimal("50000")
                result["size"] = Decimal(str(balance_based_size_usd * 0.5)) / estimated_price  # Extra conservative
                result["valid"] = True
                result["reason"] = "Conservative sizing (no liquidity analysis)"
                return result
            
            result["liquidity_analysis"] = liquidity_comparison
            
            # Determine appropriate liquidity limit
            if opportunity.spread_bps >= self.config.aggressive_execution_spread_bps:
                max_liquidity_usd = liquidity_comparison["comparison"]["aggressive_capacity"]["combined"]
                mode = "aggressive"
            else:
                max_liquidity_usd = liquidity_comparison["comparison"]["conservative_capacity"]["combined"]
                mode = "conservative"
            
            # Apply safety buffer
            safe_liquidity_usd = max_liquidity_usd * float(self.config.liquidity_safety_buffer)
            
            # Use the most restrictive constraint
            final_size_usd = min(
                balance_based_size_usd,
                safe_liquidity_usd,
                float(self.config.max_position_size_usd)
            )
            
            self.logger.info(f"   Balance limit: ${balance_based_size_usd:.2f}")
            self.logger.info(f"   Liquidity limit ({mode}): ${safe_liquidity_usd:.2f}")
            self.logger.info(f"   Config limit: ${self.config.max_position_size_usd}")
            self.logger.info(f"   Final size USD: ${final_size_usd:.2f}")
            
            # Get current market price for sizing
            estimated_price = Decimal("50000")  # Fallback
            try:
                price_data = await self.orderbook_manager.get_synchronized_prices()
                if price_data.get("status") == "complete":
                    extended_mid = price_data["extended"].get("mid_price")
                    lighter_mid = price_data["lighter"].get("mid_price")
                    
                    if extended_mid and lighter_mid:
                        estimated_price = (Decimal(str(extended_mid)) + Decimal(str(lighter_mid))) / Decimal("2")
                        self.logger.info(f"   Using live mid price: ${estimated_price}")
            except Exception:
                self.logger.debug("Using fallback price for sizing")
            
            # Calculate final position size
            position_size = Decimal(str(final_size_usd)) / estimated_price
            position_size = position_size.quantize(Decimal("0.0001"))
            
            # Validate minimum size
            if position_size < Decimal("0.001"):  # Minimum 0.001 crypto
                result["reason"] = f"Position size too small: {position_size} (min: 0.001)"
                return result
            
            result["size"] = position_size
            result["valid"] = True
            result["reason"] = f"Liquidity-validated sizing ({mode} mode)"
            
            # Add detailed analysis to result
            result["liquidity_analysis"].update({
                "mode": mode,
                "max_safe_trade_usd": safe_liquidity_usd,
                "limiting_factor": "balance" if balance_based_size_usd < safe_liquidity_usd else "liquidity",
                "depth_score": liquidity_comparison["recommendations"]["overall_quality"],
                "safety_buffer_applied": float(self.config.liquidity_safety_buffer),
                "estimated_price": float(estimated_price),
                "position_value_usd": float(position_size * estimated_price)
            })
            
            self.logger.info(f"   Position size: {position_size} crypto")
            self.logger.info(f"   Position value: ${float(position_size * estimated_price):.2f}")
            self.logger.info(f"   Mode: {mode}")
            self.logger.info(f"   Limiting factor: {result['liquidity_analysis']['limiting_factor']}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in enhanced position sizing: {e}")
            result["reason"] = f"Sizing error: {e}"
            return result

    async def _execute_live_arbitrage_enhanced(self, opportunity: ArbitrageOpportunity, 
                                            position_size: Decimal, aggressive: bool, 
                                            liquidity_analysis: Dict):
        """
        ENHANCED: Execute live arbitrage with comprehensive liquidity validation
        """
        try:
            self.logger.info(f"🎯 ENHANCED LIVE ARBITRAGE EXECUTION STARTING")
            
            # Execute using the enhanced order manager's liquidity-validated execution method
            result = await self.order_manager.execute_live_arbitrage_opportunity(
                opportunity=opportunity,
                target_size=position_size,
                aggressive_mode=aggressive
            )
            
            if result["success"]:
                self.logger.info(f"✅ ENHANCED ARBITRAGE EXECUTED SUCCESSFULLY")
                self.logger.info(f"   Group ID: {result['group_id']}")
                self.logger.info(f"   Extended Order: {result['extended_order']['order_id']}")
                self.logger.info(f"   Lighter Order: {result['lighter_order']['tx_hash']}")
                self.logger.info(f"   Execution Time: {result['execution_time']:.3f}s")
                
                # Enhanced performance tracking
                self.performance_stats["opportunities_executed"] += 1
                self.performance_stats["total_trades"] += 2
                
                estimated_volume = position_size * Decimal("50000")  # Estimate USD volume
                self.performance_stats["total_volume_usd"] += estimated_volume
                
                # Track liquidity validation success
                if result.get("liquidity_analysis", {}).get("valid"):
                    self.logger.info(f"   Liquidity validation: PASSED")
                    ext_impact = result["extended_order"].get("impact_score", 0)
                    lit_impact = result["lighter_order"].get("impact_score", 0)
                    avg_impact = (ext_impact + lit_impact) / 2
                    self.logger.info(f"   Average market impact: {avg_impact:.1f}%")
                
                # Track as active arbitrage group
                group_id = result["group_id"]
                if group_id:
                    self.logger.info(f"📊 Tracking enhanced arbitrage group: {group_id}")
                
            else:
                self.logger.error(f"❌ ENHANCED ARBITRAGE EXECUTION FAILED")
                self.logger.error(f"   Error: {result['error']}")
                self.logger.error(f"   Execution Time: {result['execution_time']:.3f}s")
                
                # Track failure reasons
                if "liquidity" in result["error"].lower():
                    self.performance_stats["opportunities_rejected_for_liquidity"] += 1
                
                self.performance_stats["risk_violations"] += 1
                
        except Exception as e:
            self.logger.error(f"❌ Enhanced arbitrage execution error: {e}")
            self.performance_stats["risk_violations"] += 1

    async def _simulate_arbitrage_execution_enhanced(self, opportunity: ArbitrageOpportunity, 
                                                   position_size: Decimal, liquidity_analysis: Dict):
        """ENHANCED: Simulate arbitrage execution with liquidity analysis for paper trading"""
        self.logger.info("📝 ENHANCED DRY RUN: Simulating liquidity-validated arbitrage execution...")
        
        # Simulate execution time
        await asyncio.sleep(0.5)
        
        # Update performance stats (simulation)
        self.performance_stats["opportunities_executed"] += 1
        self.performance_stats["total_trades"] += 2
        
        estimated_volume = position_size * Decimal("50000")
        self.performance_stats["total_volume_usd"] += estimated_volume
        
        # Estimate profit with liquidity considerations
        estimated_profit = estimated_volume * (safe_decimal(opportunity.spread_bps) / Decimal("10000"))
        
        # Account for potential slippage due to liquidity constraints
        if liquidity_analysis.get("impact_score", 0) > 50:
            slippage_penalty = estimated_profit * Decimal("0.1")  # 10% penalty for high impact
            estimated_profit -= slippage_penalty
            self.logger.info(f"   Applied slippage penalty for high liquidity impact: ${slippage_penalty:.2f}")
        
        self.performance_stats["total_pnl_usd"] += estimated_profit
        
        if safe_decimal(opportunity.spread_bps) > self.performance_stats["best_spread_captured_bps"]:
            self.performance_stats["best_spread_captured_bps"] = safe_decimal(opportunity.spread_bps)
        
        self.logger.info(f"✅ ENHANCED DRY RUN EXECUTION COMPLETED")
        self.logger.info(f"   Estimated profit: ${estimated_profit:.2f}")
        self.logger.info(f"   Estimated volume: ${estimated_volume:.2f}")
        self.logger.info(f"   Liquidity impact: {liquidity_analysis.get('impact_score', 0):.1f}%")
        self.logger.info(f"   Market depth score: {liquidity_analysis.get('depth_score', 0):.1f}/100")
    
    # ========================================================================
    # ENHANCED MONITORING AND RISK MANAGEMENT
    # ========================================================================
    
    async def _run_balance_monitoring(self):
        """Run enhanced balance monitoring in background"""
        self.logger.info("💰 Starting enhanced balance monitoring")
        
        try:
            await self.balance_manager.start_monitoring()
        except Exception as e:
            self.logger.error(f"Enhanced balance monitoring error: {e}")
    
    async def _run_performance_monitoring(self):
        """Monitor and log enhanced system performance"""
        self.logger.info("📊 Starting enhanced performance monitoring")
        
        while self.is_running:
            try:
                await asyncio.sleep(self.config.performance_log_interval)
                await self._log_enhanced_performance_summary()
                
            except Exception as e:
                self.logger.error(f"Enhanced performance monitoring error: {e}")
    
    async def _run_enhanced_risk_monitoring(self):
        """ENHANCED: Monitor risk metrics with intelligent margin and liquidity checking"""
        self.logger.info("⚠️ Starting enhanced risk monitoring with liquidity validation")
        
        while self.is_running:
            try:
                # Enhanced risk checking
                risk_status = await self.balance_manager.check_risk_limits()
                
                if not risk_status["within_limits"]:
                    self.logger.warning("⚠️ RISK LIMIT VIOLATIONS DETECTED:")
                    for violation in risk_status["violations"]:
                        self.logger.warning(f"   • {violation}")
                    
                    # Only trigger emergency actions for critical violations
                    critical_violations = [v for v in risk_status["violations"] if "EMERGENCY" in v]
                    if critical_violations:
                        self.logger.critical("🚨 CRITICAL RISK VIOLATIONS - TRIGGERING EMERGENCY HALT")
                        await self._emergency_halt()
                    else:
                        self.logger.info("ℹ️ Non-critical violations - continuing with enhanced monitoring")
                
                # *** ENHANCED: Check liquidity health ***
                if self.config.enable_liquidity_validation and self.orderbook_manager:
                    try:
                        liquidity_comparison = await self.orderbook_manager.get_cross_exchange_liquidity_comparison()
                        if liquidity_comparison:
                            overall_quality = liquidity_comparison["recommendations"]["overall_quality"]
                            
                            if overall_quality < float(self.config.min_market_depth_score):
                                self.logger.warning(f"⚠️ LIQUIDITY DEGRADATION: Market depth quality {overall_quality:.1f} below minimum {self.config.min_market_depth_score}")
                                # Could implement liquidity-based trading halt here
                            
                            max_safe_trade = liquidity_comparison["recommendations"]["max_safe_trade_usd"]
                            if max_safe_trade < float(self.config.min_order_size_usd):
                                self.logger.warning(f"⚠️ LIQUIDITY WARNING: Max safe trade ${max_safe_trade:.0f} below minimum order size")
                    
                    except Exception as e:
                        self.logger.debug(f"Liquidity monitoring error: {e}")
                
                # Enhanced diagnostic logging
                if risk_status.get("extended_margin_healthy") is not None:
                    margin_status = "✅ HEALTHY" if risk_status["extended_margin_healthy"] else "⚠️ ATTENTION"
                    self.logger.debug(f"Margin Status: {margin_status} | "
                                    f"Raw: {risk_status.get('margin_ratio', 0):.4f} | "
                                    f"Effective: {risk_status.get('extended_effective_margin', 0):.4f}")
                
                # Check active arbitrage groups
                await self._monitor_arbitrage_groups()
                
                await asyncio.sleep(self.config.risk_check_interval)
                
            except Exception as e:
                self.logger.error(f"Enhanced risk monitoring error: {e}")
    
    async def _emergency_halt(self):
        """Enhanced emergency halt with liquidity considerations"""
        self.logger.critical("🚨 ENHANCED EMERGENCY HALT INITIATED")
        
        try:
            # Stop auto-execution immediately
            self.config.auto_execution_enabled = False
            
            # Cancel all active orders
            active_orders = await self.order_manager.get_active_orders()
            if active_orders:
                self.logger.critical(f"🚨 Emergency cancelling {len(active_orders)} active orders")
                for order_id in active_orders.keys():
                    try:
                        await self.order_manager.cancel_order(order_id)
                        self.logger.info(f"🚨 Emergency cancelled order: {order_id}")
                    except Exception as e:
                        self.logger.error(f"🚨 Failed to cancel order {order_id}: {e}")
            
            # Update stats
            self.performance_stats["emergency_stops"] += 1
            
            self.logger.critical("🚨 ENHANCED EMERGENCY HALT COMPLETED")
            
        except Exception as e:
            self.logger.critical(f"🚨 Error during enhanced emergency halt: {e}")
    
    async def _monitor_arbitrage_groups(self):
        """Enhanced arbitrage group monitoring with liquidity tracking"""
        try:
            current_groups = await self.order_manager.get_arbitrage_groups()
            
            for group_id, group in current_groups.items():
                if group.status == "active":
                    # Check fill status of both orders
                    ext_order_status = await self.order_manager.get_order_status(group.extended_order.order_id)
                    lit_order_status = await self.order_manager.get_order_status(group.lighter_order.order_id)
                    
                    if ext_order_status and lit_order_status:
                        ext_filled = ext_order_status["status"] == "filled"
                        lit_filled = lit_order_status["status"] == "filled"
                        
                        if ext_filled and lit_filled:
                            # Both orders completed
                            profit = await self._calculate_realized_arbitrage_profit(group, ext_order_status, lit_order_status)
                            
                            self.logger.info(f"✅ ENHANCED ARBITRAGE GROUP COMPLETED: {group_id}")
                            self.logger.info(f"   Extended: {ext_order_status['filled_size']}/{ext_order_status['size']} @ ${ext_order_status['price']}")
                            self.logger.info(f"   Lighter: {lit_order_status['filled_size']}/{lit_order_status['size']} @ ${lit_order_status['price']}")
                            self.logger.info(f"   Realized Profit: ${profit:.2f}")
                            self.logger.info(f"   Target Spread: {group.target_spread_bps:.2f} bps")
                            
                            # Enhanced tracking with liquidity metrics
                            if hasattr(group, 'liquidity_validated') and group.liquidity_validated:
                                self.logger.info(f"   Liquidity validated: ✅")
                                if hasattr(group, 'combined_liquidity_score'):
                                    self.logger.info(f"   Liquidity score: {group.combined_liquidity_score:.1f}/100")
                            
                            # Update performance
                            self.performance_stats["total_pnl_usd"] += profit
                            
                            # Mark as completed
                            group.status = "completed"
                            
                        elif time.time() - group.created_at/1000 > 300:  # 5 minutes timeout
                            # Orders taking too long - consider cancelling
                            self.logger.warning(f"⚠️ Arbitrage group {group_id} timeout - considering cancellation")
                            
                            # Cancel unfilled orders
                            if not ext_filled:
                                await self.order_manager.cancel_order(group.extended_order.order_id)
                                self.logger.info(f"🗑️ Cancelled Extended order: {group.extended_order.order_id}")
                            
                            if not lit_filled:
                                await self.order_manager.cancel_order(group.lighter_order.order_id)
                                self.logger.info(f"🗑️ Cancelled Lighter order: {group.lighter_order.order_id}")
                            
                            group.status = "timeout_cancelled"
                            
        except Exception as e:
            self.logger.error(f"Error monitoring enhanced arbitrage groups: {e}")

    async def _calculate_realized_arbitrage_profit(self, group: ArbitrageOrderGroup, ext_status: Dict, lit_status: Dict) -> Decimal:
        """Calculate realized profit from completed arbitrage orders"""
        try:
            ext_value = Decimal(str(ext_status["filled_size"])) * Decimal(str(ext_status["price"]))
            lit_value = Decimal(str(lit_status["filled_size"])) * Decimal(str(lit_status["price"]))
            
            # Calculate profit based on order sides
            if group.extended_order.side.value == "buy":
                # Bought Extended, Sold Lighter
                profit = lit_value - ext_value
            else:
                # Sold Extended, Bought Lighter
                profit = ext_value - lit_value
            
            # Subtract estimated fees (approximate)
            total_value = ext_value + lit_value
            estimated_fees = total_value * Decimal("0.001")  # 0.1% total fees
            
            net_profit = profit - estimated_fees
            
            return net_profit
            
        except Exception as e:
            self.logger.error(f"Error calculating arbitrage profit: {e}")
            return Decimal("0")
    
    async def _log_enhanced_performance_summary(self):
        """ENHANCED: Log comprehensive performance summary with liquidity diagnostics"""
        uptime_hours = (time.time() - self.system_start_time) / 3600
        
        # Get current system status
        account_summary = await self.balance_manager.get_account_summary()
        trading_summary = await self.order_manager.get_trading_summary()
        
        self.logger.info("=" * 90)
        self.logger.info("📊 ENHANCED SYSTEM PERFORMANCE SUMMARY WITH LIQUIDITY METRICS")
        self.logger.info("=" * 90)
        self.logger.info(f"⏰ Uptime: {uptime_hours:.1f} hours")
        self.logger.info(f"🎯 Current opportunity: {self.current_opportunity.extended_symbol if self.current_opportunity else 'None'}")
        
        if self.current_opportunity:
            self.logger.info(f"   Spread: {self.current_opportunity.spread_bps:.2f} bps")
            self.logger.info(f"   Annual yield: {self.current_opportunity.annual_yield:+.1f}%")
        
        self.logger.info(f"💰 Account Status:")
        self.logger.info(f"   Total equity: ${float(account_summary['total_equity']):,.2f}")
        self.logger.info(f"   Available margin: ${float(account_summary['total_available']):,.2f}")
        self.logger.info(f"   Used margin: ${float(account_summary['total_used_margin']):,.2f}")
        self.logger.info(f"   Unrealized PnL: ${float(account_summary['total_unrealized_pnl']):+,.2f}")
        
        # Enhanced margin diagnostics
        extended_balance_info = account_summary["exchanges"]["extended"]["balance"]
        if extended_balance_info and "margin_healthy" in extended_balance_info:
            self.logger.info(f"🔧 Enhanced Margin Analysis:")
            self.logger.info(f"   Raw margin ratio: {extended_balance_info['margin_ratio']:.4f}")
            self.logger.info(f"   Effective margin ratio: {extended_balance_info['effective_margin_ratio']:.4f}")
            self.logger.info(f"   Margin healthy: {'✅' if extended_balance_info['margin_healthy'] else '⚠️'}")
            self.logger.info(f"   Parsing method: {extended_balance_info.get('parsing_method', 'unknown')}")
        
        self.logger.info(f"📈 Trading Performance:")
        self.logger.info(f"   Opportunities discovered: {self.performance_stats['opportunities_discovered']}")
        self.logger.info(f"   Opportunities executed: {self.performance_stats['opportunities_executed']}")
        self.logger.info(f"   Total trades: {self.performance_stats['total_trades']}")
        self.logger.info(f"   Total volume: ${float(self.performance_stats['total_volume_usd']):,.2f}")
        self.logger.info(f"   Total PnL: ${float(self.performance_stats['total_pnl_usd']):+,.2f}")
        self.logger.info(f"   Best spread captured: {float(self.performance_stats['best_spread_captured_bps']):.2f} bps")
        
        # *** ENHANCED: Liquidity validation metrics ***
        self.logger.info(f"💧 Liquidity Validation Metrics:")
        self.logger.info(f"   Validations performed: {self.performance_stats['liquidity_validations_performed']}")
        self.logger.info(f"   Opportunities rejected for liquidity: {self.performance_stats['opportunities_rejected_for_liquidity']}")
        self.logger.info(f"   Average market depth score: {self.performance_stats['average_market_depth_score']:.1f}/100")
        self.logger.info(f"   Liquidity-limited executions: {self.performance_stats['liquidity_limited_executions']}")
        self.logger.info(f"   Balance/liquidity warnings: {self.performance_stats['balance_to_liquidity_warnings']}")
        
        # Enhanced liquidity analysis if available
        if self.orderbook_manager:
            try:
                liquidity_comparison = await self.orderbook_manager.get_cross_exchange_liquidity_comparison()
                if liquidity_comparison:
                    rec = liquidity_comparison["recommendations"]
                    self.logger.info(f"   Current max safe trade: ${rec['max_safe_trade_usd']:,.0f}")
                    self.logger.info(f"   Limiting exchange: {rec['limiting_exchange']}")
                    self.logger.info(f"   Overall liquidity quality: {rec['overall_quality']:.1f}/100")
            except Exception:
                pass
        
        self.logger.info(f"⚠️ Risk Metrics:")
        self.logger.info(f"   Active orders: {trading_summary['active_orders']['count']}")
        self.logger.info(f"   Active exposure: ${trading_summary['active_orders']['total_value_usd']:,.2f}")
        self.logger.info(f"   Risk utilization: {trading_summary['risk_status']['utilization_percent']:.1f}%")
        self.logger.info(f"   Risk violations: {self.performance_stats['risk_violations']}")
        self.logger.info(f"   Emergency stops: {self.performance_stats['emergency_stops']}")
        
        # Enhanced order tracking
        if 'liquidity_metrics' in trading_summary:
            lm = trading_summary['liquidity_metrics']
            self.logger.info(f"   Liquidity validation enabled: {lm['liquidity_validation_enabled']}")
            self.logger.info(f"   Orders liquidity-validated: {trading_summary['active_orders']['liquidity_validated']}")
            self.logger.info(f"   Orders liquidity-safe: {trading_summary['active_orders']['liquidity_safe']}")
            self.logger.info(f"   Average impact score: {trading_summary['active_orders']['avg_impact_score']:.1f}%")
        
        self.logger.info("=" * 90)
    
    # ========================================================================
    # KEEP ALL EXISTING METHODS (unchanged)
    # ========================================================================
    
    async def _handle_shutdown(self):
        """Handle graceful shutdown"""
        await self.shutdown_event.wait()
        await self._graceful_shutdown()
    
    async def _graceful_shutdown(self):
        """Perform graceful shutdown of all system components"""
        self.logger.info("🛑 INITIATING ENHANCED GRACEFUL SHUTDOWN")
        
        self.is_running = False
        
        try:
            # Cancel all active orders
            active_orders = await self.order_manager.get_active_orders()
            if active_orders:
                self.logger.info(f"📋 Cancelling {len(active_orders)} active orders...")
                for order_id in active_orders.keys():
                    await self.order_manager.cancel_order(order_id)
            
            # Stop orderbook monitoring
            if self.orderbook_manager:
                await self.orderbook_manager.stop_monitoring()
            
            # Stop balance monitoring
            await self.balance_manager.stop_monitoring()
            
            # Log final enhanced performance summary
            await self._log_enhanced_performance_summary()
            
            self.logger.info("✅ Enhanced graceful shutdown completed")
            
        except Exception as e:
            self.logger.error(f"Error during enhanced shutdown: {e}")
        
        # Trigger shutdown event
        self.shutdown_event.set()
    
    async def _cleanup(self):
        """Enhanced final cleanup of resources"""
        self.logger.info("🧹 Performing enhanced final cleanup")
        
        # Save enhanced performance data
        performance_file = f"enhanced_hft_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            performance_data = {
                "session_stats": self.performance_stats,
                "config": {
                    "dry_run_mode": self.config.dry_run_mode,
                    "auto_execution_enabled": self.config.auto_execution_enabled,
                    "min_execution_spread_bps": float(self.config.min_execution_spread_bps),
                    "max_position_size_usd": float(self.config.max_position_size_usd),
                    # Enhanced config tracking
                    "liquidity_validation_enabled": self.config.enable_liquidity_validation,
                    "max_liquidity_impact_score": float(self.config.max_liquidity_impact_score),
                    "min_market_depth_score": float(self.config.min_market_depth_score),
                    "max_balance_to_liquidity_ratio": float(self.config.max_balance_to_liquidity_ratio),
                    "liquidity_safety_buffer": float(self.config.liquidity_safety_buffer),
                },
                "uptime_hours": (time.time() - self.system_start_time) / 3600,
                "final_timestamp": int(time.time() * 1000),
                "enhanced_features": {
                    "liquidity_validation": True,
                    "margin_parsing_fix": True,
                    "cross_exchange_analysis": True,
                    "dynamic_position_sizing": True
                }
            }
            
            # Convert Decimal objects to float for JSON serialization
            for key, value in performance_data["session_stats"].items():
                if isinstance(value, Decimal):
                    performance_data["session_stats"][key] = float(value)
            
            with open(performance_file, 'w') as f:
                json.dump(performance_data, f, indent=2)
            
            self.logger.info(f"📊 Enhanced performance data saved to: {performance_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving enhanced performance data: {e}")


async def main():
    """Main entry point with Enhanced Liquidity Validation and Persistent API Key Management"""
    
    # Configure logging with enhanced detail
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'enhanced_hft_arbitrage_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )
    
    logger = logging.getLogger("EnhancedMainSystem")
    
    print("🚀 ENHANCED HFT ARBITRAGE SYSTEM WITH COMPREHENSIVE LIQUIDITY VALIDATION")
    print("=" * 80)
    print("Advanced cross-exchange arbitrage trading system")
    print("*** ENHANCED FEATURES ***")
    print("• ✅ Comprehensive liquidity validation before execution")
    print("• ✅ Balance-to-liquidity ratio analysis and safety checks")
    print("• ✅ Dynamic position sizing based on market depth")
    print("• ✅ Cross-exchange liquidity comparison and monitoring")
    print("• ✅ Market impact assessment and protection")
    print("• ✅ Enhanced risk management with intelligent thresholds")
    print("• ✅ REAL automated hedged arbitrage execution")
    print("• ✅ Performance tracking with liquidity metrics")
    print("• 💾 Persistent Lighter API key management")
    print("=" * 80)
    
    # Enhanced system configuration from centralized .env
    config = SystemConfig(
        opportunity_scan_interval=int(app_config.OPPORTUNITY_SCAN_INTERVAL),
        min_execution_spread_bps=Decimal(str(app_config.MIN_EXECUTION_SPREAD_BPS)),
        aggressive_execution_spread_bps=Decimal(str(app_config.AGGRESSIVE_EXECUTION_SPREAD_BPS)),
        max_position_size_usd=Decimal(str(app_config.MAX_POSITION_SIZE_USD)),
        performance_log_interval=int(app_config.PERFORMANCE_LOG_INTERVAL),
        auto_execution_enabled=bool(app_config.AUTO_EXECUTION_ENABLED),
        conservative_mode=bool(app_config.CONSERVATIVE_MODE),
        dry_run_mode=(str(app_config.TRADING_MODE).lower() != "live_trading"),
        risk_check_interval=int(app_config.RISK_CHECK_INTERVAL),
        # Optional thresholds exposed for broader use
        min_viable_spread_bps=Decimal(str(app_config.MIN_VIABLE_SPREAD_BPS)),
        min_order_size_usd=Decimal(str(app_config.MIN_ORDER_SIZE_USD)),
        max_order_size_usd=Decimal(str(app_config.MAX_ORDER_SIZE_USD)),
        # Enhanced margin thresholds from .env
        min_margin_ratio=Decimal(str(getattr(app_config, 'MIN_MARGIN_RATIO', Decimal('0.10')))),
        emergency_margin_ratio=Decimal(str(getattr(app_config, 'EMERGENCY_MARGIN_RATIO', Decimal('0.05')))),
        min_required_available_usd=Decimal(str(getattr(app_config, 'MIN_REQUIRED_AVAILABLE_USD', Decimal('20')))),
        orderbook_ready_cooldown_seconds=int(getattr(app_config, 'ORDERBOOK_READY_COOLDOWN_SECONDS', 2)),
        
        # *** NEW: Enhanced liquidity validation settings ***
        enable_liquidity_validation=True,  # Always enabled for safety
        max_liquidity_impact_score=Decimal("75.0"),  # Allow up to 75% market impact
        min_market_depth_score=Decimal("30.0"),  # Require minimum 30/100 depth quality
        max_balance_to_liquidity_ratio=Decimal("1.0"),  # Balance <= 100% of available liquidity
        liquidity_safety_buffer=Decimal("0.8"),  # Use 80% of max safe liquidity
        require_cross_exchange_liquidity_check=True,  # Always check both exchanges
    )

    # Log enhanced configuration
    valid_modes = {"dry_run", "paper_trading", "live_trading"}
    if str(app_config.TRADING_MODE).lower() not in valid_modes:
        logger.warning(f"Unknown TRADING_MODE='{app_config.TRADING_MODE}', defaulting to DRY RUN")
        config.dry_run_mode = True

    logger.info(
        f"🎛️ Enhanced runtime mode: {'LIVE' if not config.dry_run_mode else 'DRY RUN'} | "
        f"auto_execution={'ENABLED' if config.auto_execution_enabled else 'DISABLED'} | "
        f"conservative={'ON' if config.conservative_mode else 'OFF'} | "
        f"*** LIQUIDITY VALIDATION: {'ENABLED' if config.enable_liquidity_validation else 'DISABLED'} | "
        f"max_impact={config.max_liquidity_impact_score}% | min_depth={config.min_market_depth_score} ***"
    )
    
    # Extended account setup (from centralized config)
    try:
        app_config.validate()
    except Exception as e:
        logger.error(f"Config validation failed: {e}")
        return

    stark_account = StarkPerpetualAccount(
        vault=app_config.EXTENDED_VAULT,
        private_key=app_config.EXTENDED_PRIVATE_KEY,
        public_key=app_config.EXTENDED_PUBLIC_KEY,
        api_key=app_config.EXTENDED_API_KEY,
    )
    
    logger.info(f"✅ Extended account initialized: vault {app_config.EXTENDED_VAULT}")
    
    # Enhanced user configuration display
    print(f"\n🎛️ ENHANCED SYSTEM CONFIGURATION:")
    print(f"   Opportunity scan interval: {config.opportunity_scan_interval}s")
    print(f"   Min execution spread: {config.min_execution_spread_bps} bps")
    print(f"   Max position size: ${config.max_position_size_usd}")
    print(f"   Auto-execution: {'ENABLED' if config.auto_execution_enabled else 'DISABLED'}")
    print(f"   Mode: {'DRY RUN' if config.dry_run_mode else 'LIVE TRADING'}")
    print(f"   *** ENHANCED LIQUIDITY VALIDATION ***")
    print(f"   Liquidity validation: {'ENABLED' if config.enable_liquidity_validation else 'DISABLED'}")
    print(f"   Max market impact: {config.max_liquidity_impact_score}%")
    print(f"   Min market depth: {config.min_market_depth_score}/100")
    print(f"   Max balance/liquidity ratio: {config.max_balance_to_liquidity_ratio}")
    print(f"   Safety buffer: {config.liquidity_safety_buffer * 100}%")
    
    input(f"\nPress Enter to start the {'LIVE' if not config.dry_run_mode else 'SIMULATED'} enhanced HFT arbitrage system...")
    
    # Create and start the enhanced system
    system = HFTArbitrageSystem(stark_account, config)

    # Initialize Lighter client with persistent API key management
    lighter_client = None
    lighter_api_client = None
    
    try:
        # Use the new enhanced initialization function
        lighter_client, lighter_api_client = await initialize_lighter_client(system, logger)
        
        if not lighter_client:
            logger.error("❌ Failed to initialize Lighter client")
            logger.error("   System will run without Lighter trading capability")
            # Decide whether to continue or exit
            # return  # Uncomment to exit if Lighter is required
        
        # Start the system
        await system.start()
        
    except KeyboardInterrupt:
        logger.info("👋 Enhanced system stopped by user")
    except Exception as e:
        logger.error(f"Enhanced system error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Ensure Lighter client teardown
        try:
            await close_lighter_client(lighter_client, lighter_api_client)
        except Exception as e:
            logger.error(f"Error closing Lighter client: {e}")


if __name__ == "__main__":
    # Enhanced system information
    print("Enhanced HFT Arbitrage System v3.1 - With Persistent API Key Management")
    print("=" * 80)
    print("Intelligent cross-exchange arbitrage with comprehensive liquidity protection")
    print("Components: Enhanced orderbooks.py | Enhanced orders.py | Enhanced balance.py")
    print("Features: Liquidity validation | Market impact protection | Dynamic sizing")
    print("Safety: Balance-to-liquidity analysis | Cross-exchange depth comparison")
    print("API Keys: Automatic persistence to .env | One-time generation | Smart validation")
    print()
    
    # Run the enhanced system
    asyncio.run(main())