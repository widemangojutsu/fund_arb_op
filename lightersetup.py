import asyncio
import logging
import time
import json
from datetime import datetime
import requests
import aiohttp
import eth_account
import lighter
import dotenv
import os

logging.basicConfig(level=logging.DEBUG)

# this is a dummy private key which is registered on Testnet.
# It serves as a good example

# load environment variables
dotenv.load_dotenv()

BASE_URL = os.getenv("LIGHTER_BASE_URL")
ETH_PRIVATE_KEY = os.getenv("ETH_PRIVATE_KEY")
API_KEY_INDEX = os.getenv("LIGHTER_API_KEY_INDEX")


def get_funding_rates(instrument_id=None):
    """
    Synchronous function to fetch funding rates from Thalex API.
    
    Args:
        instrument_id (str, optional): Specific instrument ID to filter by
        
    Returns:
        dict: Structured funding rates data or error information
    """
    try:
        url = f"{BASE_URL}/api/v1/funding-rates"
        headers = {"accept": "application/json"}
        
        params = {}
        if instrument_id:
            params["instrument_id"] = instrument_id
            
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        
        return {
            "success": True,
            "data": response.json(),
            "timestamp": datetime.now().isoformat(),
            "status_code": response.status_code
        }
        
    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
            "data": None
        }


async def get_funding_rates_async(instrument_id=None):
    """
    Asynchronous function to fetch funding rates from Thalex API.
    Better for HFT applications requiring low latency.
    
    Args:
        instrument_id (str, optional): Specific instrument ID to filter by
        
    Returns:
        dict: Structured funding rates data or error information
    """
    try:
        url = f"{BASE_URL}/api/v1/funding-rates"
        headers = {"accept": "application/json"}
        
        params = {}
        if instrument_id:
            params["instrument_id"] = instrument_id
            
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                data = await response.json()
                
                return {
                    "success": True,
                    "data": data,
                    "timestamp": datetime.now().isoformat(),
                    "status_code": response.status
                }
                
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
            "data": None
        }


def get_lighter_funding_rates():
    """
    Get clean funding rates data for all Lighter exchange instruments.
    Optimized for HFT framework integration.
    
    Returns:
        dict: {
            "success": bool,
            "data": [{"market_id": int, "symbol": str, "rate": float}, ...],
            "count": int,
            "timestamp": str,
            "error": str or None
        }
    """
    try:
        # Fetch raw funding rates data
        raw_result = get_funding_rates()
        
        if not raw_result["success"]:
            return {
                "success": False,
                "data": [],
                "count": 0,
                "timestamp": datetime.now().isoformat(),
                "error": raw_result["error"]
            }
        
        # Extract clean market data
        market_data = []
        raw_data = raw_result["data"]
        
        # Simple direct parsing - we know it's a list of dicts
        if isinstance(raw_data, list):
            for instrument in raw_data:
                if isinstance(instrument, dict):
                    # Extract the fields we need directly
                    market_data.append({
                        "market_id": instrument.get("market_id"),
                        "symbol": instrument.get("symbol"),
                        "rate": instrument.get("rate")
                    })
        elif isinstance(raw_data, dict):
            # Handle dictionary format if needed
            for key, value in raw_data.items():
                if isinstance(value, dict):
                    market_data.append({
                        "market_id": value.get("market_id"),
                        "symbol": value.get("symbol"),
                        "rate": value.get("rate")
                    })
        
        return {
            "success": True,
            "data": market_data,
            "count": len(market_data),
            "timestamp": datetime.now().isoformat(),
            "error": None
        }
        
    except Exception as e:
        print(f"DEBUG - Exception: {e}")
        return {
            "success": False,
            "data": [],
            "count": 0,
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }


async def get_lighter_funding_rates_async():
    """
    Async version of get_lighter_funding_rates for low-latency HFT applications.
    
    Returns:
        dict: Same structure as get_lighter_funding_rates()
    """
    try:
        # Fetch raw funding rates data asynchronously
        raw_result = await get_funding_rates_async()
        
        if not raw_result["success"]:
            return {
                "success": False,
                "data": [],
                "count": 0,
                "timestamp": datetime.now().isoformat(),
                "error": raw_result["error"]
            }
        
        # Extract clean market data
        market_data = []
        raw_data = raw_result["data"]
        
        # Simple direct parsing - we know it's a list of dicts
        if isinstance(raw_data, list):
            for instrument in raw_data:
                if isinstance(instrument, dict):
                    # Extract the fields we need directly
                    market_data.append({
                        "market_id": instrument.get("market_id"),
                        "symbol": instrument.get("symbol"),
                        "rate": instrument.get("rate")
                    })
        elif isinstance(raw_data, dict):
            # Handle dictionary format if needed
            for key, value in raw_data.items():
                if isinstance(value, dict):
                    market_data.append({
                        "market_id": value.get("market_id"),
                        "symbol": value.get("symbol"),
                        "rate": value.get("rate")
                    })
        
        return {
            "success": True,
            "data": market_data,
            "count": len(market_data),
            "timestamp": datetime.now().isoformat(),
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "data": [],
            "count": 0,
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }


def get_market_data_clean():
    """
    Get clean market data using the working funding rates function.
    Built on the working get_funding_rates() function.
    """
    try:
        url = f"{BASE_URL}/api/v1/funding-rates"
        headers = {"accept": "application/json"}
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        raw_data = response.json()
        
        # Extract clean market data - handle dictionary response
        market_data = []
        
        # The API returns a dictionary, find the key with instruments list
        if isinstance(raw_data, dict):
            # Look for the key containing the instruments list
            instruments_list = None
            for key, value in raw_data.items():
                if isinstance(value, list) and len(value) > 0:
                    # Check if first item looks like an instrument
                    first_item = value[0]
                    if isinstance(first_item, dict) and 'market_id' in first_item and 'symbol' in first_item:
                        instruments_list = value
                        print(f"DEBUG - Found instruments in key: '{key}' with {len(value)} items")
                        break
            
            if instruments_list:
                for instrument in instruments_list:
                    if isinstance(instrument, dict) and instrument.get("exchange") == "lighter":
                        market_data.append({
                            "market_id": instrument.get("market_id"),
                            "symbol": instrument.get("symbol"), 
                            "rate": instrument.get("rate"),
                            "exchange": instrument.get("exchange")
                        })
            else:
                print(f"DEBUG - No instruments list found in keys: {list(raw_data.keys())}")
        elif isinstance(raw_data, list):
            # Direct list format (fallback)
            for instrument in raw_data:
                if isinstance(instrument, dict) and instrument.get("exchange") == "lighter":
                    market_data.append({
                        "market_id": instrument.get("market_id"),
                        "symbol": instrument.get("symbol"), 
                        "rate": instrument.get("rate"),
                        "exchange": instrument.get("exchange")
                    })
        
        print(f"DEBUG - Final market_data length: {len(market_data)}")
        
        return {
            "success": True,
            "data": market_data,
            "count": len(market_data),
            "timestamp": datetime.now().isoformat(),
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "data": [],
            "count": 0,
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }


async def get_market_data_clean_async():
    """
    Async version using the working funding rates approach.
    """
    try:
        url = f"{BASE_URL}/api/v1/funding-rates"
        headers = {"accept": "application/json"}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response.raise_for_status()
                raw_data = await response.json()
                
                # Extract clean market data - handle dictionary response
                market_data = []
                
                # The API returns a dictionary, find the key with instruments list
                if isinstance(raw_data, dict):
                    # Look for the key containing the instruments list
                    instruments_list = None
                    for key, value in raw_data.items():
                        if isinstance(value, list) and len(value) > 0:
                            # Check if first item looks like an instrument
                            first_item = value[0]
                            if isinstance(first_item, dict) and 'market_id' in first_item and 'symbol' in first_item:
                                instruments_list = value
                                break
                    
                    if instruments_list:
                        for instrument in instruments_list:
                            if isinstance(instrument, dict) and instrument.get("exchange") == "lighter":
                                market_data.append({
                                    "market_id": instrument.get("market_id"),
                                    "symbol": instrument.get("symbol"),
                                    "rate": instrument.get("rate"),
                                    "exchange": instrument.get("exchange")
                                })
                elif isinstance(raw_data, list):
                    # Direct list format (fallback)
                    for instrument in raw_data:
                        if isinstance(instrument, dict) and instrument.get("exchange") == "lighter":
                            market_data.append({
                                "market_id": instrument.get("market_id"),
                                "symbol": instrument.get("symbol"),
                                "rate": instrument.get("rate"),
                                "exchange": instrument.get("exchange")
                            })
                
                return {
                    "success": True,
                    "data": market_data,
                    "count": len(market_data),
                    "timestamp": datetime.now().isoformat(),
                    "error": None
                }
                
    except Exception as e:
        return {
            "success": False,
            "data": [],
            "count": 0,
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }


async def main():
    # verify that the account exists & fetch account index
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=BASE_URL))
    eth_acc = eth_account.Account.from_key(ETH_PRIVATE_KEY)
    eth_address = eth_acc.address

    try:
        response = await lighter.AccountApi(api_client).accounts_by_l1_address(l1_address=eth_address)
    except lighter.ApiException as e:
        if e.data.message == "account not found":
            print(f"error: account not found for {eth_address}")
            return
        else:
            raise e

    # Process all account indices found
    print(f"Found {len(response.sub_accounts)} account(s) - setting up API keys...")
    
    account_configs = []
    tx_clients = []
    
    for i, sub_account in enumerate(response.sub_accounts):
        account_index = sub_account.index
        print(f"Processing account {i+1}/{len(response.sub_accounts)} (Index: {account_index})...")
        
        # create a private/public key pair for the new API key
        private_key, public_key, err = lighter.create_api_key()
        if err is not None:
            raise Exception(f"Error creating API key for account {account_index}: {err}")

        tx_client = lighter.SignerClient(
            url=BASE_URL,
            private_key=private_key,
            account_index=account_index,
            api_key_index=API_KEY_INDEX,
        )
        tx_clients.append(tx_client)

        # change the API key
        response_change, err = await tx_client.change_api_key(
            eth_private_key=ETH_PRIVATE_KEY,
            new_pubkey=public_key,
        )
        if err is not None:
            raise Exception(f"Error changing API key for account {account_index}: {err}")

        # wait for API key propagation
        time.sleep(10)

        # verify API key setup
        err = tx_client.check_client()
        if err is not None:
            raise Exception(f"Error verifying API key for account {account_index}: {err}")

        # Store configuration for this account
        account_config = {
            'account_index': account_index,
            'api_key_private_key': private_key,
            'base_url': BASE_URL,
            'api_key_index': API_KEY_INDEX,
            'eth_address': eth_address,
            'setup_timestamp': datetime.now().isoformat(),
        }
        account_configs.append(account_config)
        
        print(f"✓ Account {account_index} ready")

    # Close all clients
    for tx_client in tx_clients:
        await tx_client.close()
    await api_client.close()
    
    # Prepare final configuration structure
    setup_config = {
        'setup_completed': datetime.now().isoformat(),
        'total_accounts': len(account_configs),
        'accounts': account_configs
    }
    
    # Save configuration to file
    config_filename = 'account_configs.json'
    with open(config_filename, 'w') as f:
        json.dump(setup_config, f, indent=2)
    
    print(f"\n✓ Setup complete for {len(account_configs)} accounts")
    print(f"✓ Configuration saved to: {config_filename}")
    
    return setup_config


async def test_funding_rates():
    """
    Test function to demonstrate funding rates functionality.
    """
    print("Testing funding rates functions...")
    
    # Test synchronous version
    print("\n1. Testing synchronous funding rates:")
    sync_result = get_funding_rates()
    if sync_result["success"]:
        data = sync_result['data']
        print(f"✓ Sync request successful")
        print(f"\nDEBUG - Data structure: {type(data)}")
        print(f"DEBUG - Data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
        print(f"DEBUG - Raw data: {data}")
        
        print("\nAvailable instruments and funding rates:")
        if isinstance(data, dict):
            # Handle dictionary structure
            for instrument_id, instrument_data in data.items():
                print(f"  • {instrument_id}")
                if isinstance(instrument_data, dict):
                    for key, value in instrument_data.items():
                        print(f"    - {key}: {value}")
                else:
                    print(f"    - Value: {instrument_data}")
                print()
        elif isinstance(data, list):
            # Handle list structure (original expected format)
            for i, instrument in enumerate(data):
                if isinstance(instrument, dict):
                    print(f"  • {instrument.get('instrument_id', f'Instrument {i+1}')}")
                    print(f"    - Current Rate: {instrument.get('current_funding_rate', 'N/A')}")
                    print(f"    - Next Rate: {instrument.get('next_funding_rate', 'N/A')}")
                    print(f"    - Next Time: {instrument.get('next_funding_time', 'N/A')}")
                else:
                    print(f"  • Instrument {i+1}: {instrument}")
                print()
    else:
        print(f"✗ Sync request failed: {sync_result['error']}")
    
    # Test asynchronous version
    print("\n2. Testing asynchronous funding rates:")
    async_result = await get_funding_rates_async()
    if async_result["success"]:
        data = async_result['data']
        print(f"✓ Async request successful")
        print("\nAsync results:")
        if isinstance(data, dict):
            for instrument_id, instrument_data in data.items():
                if isinstance(instrument_data, dict):
                    rate = instrument_data.get('current_funding_rate', instrument_data.get('funding_rate', 'N/A'))
                    print(f"  • {instrument_id}: {rate}")
                else:
                    print(f"  • {instrument_id}: {instrument_data}")
        elif isinstance(data, list):
            for i, instrument in enumerate(data):
                if isinstance(instrument, dict):
                    print(f"  • {instrument.get('instrument_id', f'Instrument {i+1}')}: {instrument.get('current_funding_rate', 'N/A')}")
                else:
                    print(f"  • Instrument {i+1}: {instrument}")
    else:
        print(f"✗ Async request failed: {async_result['error']}")
    
    return sync_result, async_result


async def test_lighter_market_data():
    """
    Test function for clean Lighter market data extraction.
    Optimized for HFT framework integration.
    """
    print("Testing clean Lighter market data functions...")
    
    # Test synchronous version
    print("\n1. Testing sync lighter market data:")
    sync_market_result = get_lighter_funding_rates()
    if sync_market_result["success"]:
        print(f"✓ Found {sync_market_result['count']} Lighter instruments")
        print("\nSample instruments (first 5):")
        for i, instrument in enumerate(sync_market_result['data'][:5]):
            print(f"  {i+1}. {instrument['symbol']} (ID: {instrument['market_id']}) - Rate: {instrument['rate']:.6f}")
    else:
        print(f"✗ Failed: {sync_market_result['error']}")
    
    # Test asynchronous version
    print("\n2. Testing async lighter market data:")
    async_market_result = await get_lighter_funding_rates_async()
    if async_market_result["success"]:
        print(f"✓ Async found {async_market_result['count']} instruments")
        
        # Show instruments with negative rates (potential arbitrage opportunities)
        negative_rates = [inst for inst in async_market_result['data'] if inst['rate'] < 0]
        if negative_rates:
            print(f"\nInstruments with negative funding rates ({len(negative_rates)}):")
            for inst in negative_rates:
                print(f"  • {inst['symbol']} (ID: {inst['market_id']}) - Rate: {inst['rate']:.6f}")
        
        # Show instruments with highest positive rates
        positive_rates = [inst for inst in async_market_result['data'] if inst['rate'] > 0]
        if positive_rates:
            positive_rates.sort(key=lambda x: x['rate'], reverse=True)
            print(f"\nTop 3 highest funding rates:")
            for inst in positive_rates[:3]:
                print(f"  • {inst['symbol']} (ID: {inst['market_id']}) - Rate: {inst['rate']:.6f}")
    else:
        print(f"✗ Async failed: {async_market_result['error']}")
    
    return sync_market_result, async_market_result


if __name__ == "__main__":
    config = asyncio.run(main())
    
    # Optional: print summary for verification
    print(f"\nSETUP SUMMARY:")
    for i, account in enumerate(config['accounts'], 1):
        print(f"  Account {i}: Index {account['account_index']} - Ready")
    
    # Test funding rates functionality
    print(f"\n{'='*50}")
    print("TESTING FUNDING RATES FUNCTIONALITY")
    print(f"{'='*50}")
    sync_result, async_result = asyncio.run(test_funding_rates())
    
    # Test clean market data for HFT framework
    print(f"\n{'='*60}")
    print("CLEAN MARKET DATA FOR HFT FRAMEWORK")
    print(f"{'='*60}")
    
    # Test the new working functions
    print("Testing NEW working market data functions...")
    
    print("\n1. Testing sync clean market data:")
    sync_clean = get_market_data_clean()
    if sync_clean["success"]:
        print(f"✓ Found {sync_clean['count']} instruments")
        print("Sample instruments (first 5):")
        for i, inst in enumerate(sync_clean['data'][:5]):
            print(f"  {i+1}. {inst['symbol']} (ID: {inst['market_id']}) - Rate: {inst['rate']:.6f}")
    else:
        print(f"✗ Failed: {sync_clean['error']}")
    
    print("\n2. Testing async clean market data:")
    async_clean = asyncio.run(get_market_data_clean_async())
    if async_clean["success"]:
        print(f"✓ Found {async_clean['count']} instruments")
        
        # Show instruments with negative rates
        negative_rates = [inst for inst in async_clean['data'] if inst['rate'] < 0]
        if negative_rates:
            print(f"\nNegative funding rates ({len(negative_rates)}):")
            for inst in negative_rates:
                print(f"  • {inst['symbol']} - Rate: {inst['rate']:.6f}")
    else:
        print(f"✗ Failed: {async_clean['error']}")
    
    sync_market_data, async_market_data = sync_clean, async_clean
    
    # Save market data for framework use
    if sync_market_data["success"]:
        market_data_filename = 'lighter_market_data.json'
        with open(market_data_filename, 'w') as f:
            json.dump(sync_market_data, f, indent=2)
        print(f"\n✓ Market data saved to: {market_data_filename}")
        print(f"✓ Ready for HFT framework integration")
