# filename: option_chain_handler.py (Unified Complete Version)
import asyncio
import json
import ssl
import websockets
import requests
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from google.protobuf.json_format import MessageToDict
import MarketDataFeedV3_pb2 as pb
from auth import auth

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OptionChainHandler:
    """
    🚀 Complete Option Chain Handler - REST API + WebSocket + Caching
    Combines all option chain functionality in one manageable class
    """
    
    def __init__(self):
        # API Configuration
        self.base_url = "https://api.upstox.com/v2"
        self.symbols = {
            'NIFTY': 'NSE_INDEX|Nifty 50',
            'BANKNIFTY': 'NSE_INDEX|Nifty Bank', 
            'FINNIFTY': 'NSE_INDEX|Nifty Fin Service',
            'MIDCPNIFTY': 'NSE_INDEX|Nifty MidCap 100',
            'SENSEX': 'BSE_INDEX|SENSEX'
        }
        
        # Data Storage
        self.option_chains_cache = {}  # Static option chain data
        self.live_prices_cache = {}    # Real-time price updates
        self.market_data_cache = {}    # Latest market data from WebSocket
        self.expiries_cache = {}       # Cached expiry dates
        self.holidays_cache = []       # Market holidays
        
        # WebSocket Management
        self.sio = None
        self.websocket_connected = False
        self.last_option_refresh = None
        self.refresh_interval = 300  # 5 minutes
        
        logger.info("✅ Unified Option Chain Handler - Complete Integration")
    
    # ============== REST API METHODS ==============
    
    def _get_headers(self) -> Dict[str, str]:
        """Get API headers with auth token"""
        return {
            "Authorization": f"Bearer {auth.access_token}",
            "Accept": "application/json"
        }
    
    def get_market_holidays(self) -> List[str]:
        """Fetch market holidays with caching"""
        if self.holidays_cache:
            return self.holidays_cache
            
        try:
            response = requests.get(
                f"{self.base_url}/market/holidays",
                headers=self._get_headers(),
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "success":
                self.holidays_cache = [h["date"] for h in data.get("data", [])]
                logger.info(f"✅ Cached {len(self.holidays_cache)} market holidays")
                return self.holidays_cache
            return []
        except Exception as e:
            logger.error(f"❌ Error fetching holidays: {e}")
            return []
    
    def get_expiry_dates(self, symbol: str) -> List[str]:
        """Fetch expiry dates with caching"""
        symbol_upper = symbol.upper()
        
        # Return cached if available
        if symbol_upper in self.expiries_cache:
            return self.expiries_cache[symbol_upper]
        
        try:
            instrument_key = self.symbols.get(symbol_upper)
            if not instrument_key:
                return []
            
            response = requests.get(
                f"{self.base_url}/option/contract",
                headers=self._get_headers(),
                params={"instrument_key": instrument_key},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "success":
                expiries = list(set([contract.get("expiry") for contract in data.get("data", []) if contract.get("expiry")]))
                expiries.sort()
                self.expiries_cache[symbol_upper] = expiries
                logger.info(f"✅ Cached {len(expiries)} expiries for {symbol}")
                return expiries
            return []
        except Exception as e:
            logger.error(f"❌ Error fetching expiries for {symbol}: {e}")
            return []
    
    def get_next_trading_expiry(self, symbol: str) -> Optional[str]:
        """Get next valid trading expiry"""
        try:
            expiries = self.get_expiry_dates(symbol)
            holidays = self.get_market_holidays()
            today = datetime.now().date().strftime('%Y-%m-%d')
            
            for expiry in expiries:
                if expiry > today and expiry not in holidays:
                    logger.info(f"✅ Next trading expiry for {symbol}: {expiry}")
                    return expiry
            
            return expiries[0] if expiries else None
        except Exception as e:
            logger.error(f"❌ Error getting next expiry for {symbol}: {e}")
            return None
    
    def fetch_option_chain_rest(self, symbol: str, expiry_date: Optional[str] = None) -> Dict[str, Any]:
        """Fetch option chain via REST API"""
        try:
            symbol_upper = symbol.upper()
            instrument_key = self.symbols.get(symbol_upper)
            if not instrument_key:
                return {"status": "error", "message": f"Symbol {symbol} not found"}
            
            if not expiry_date:
                expiry_date = self.get_next_trading_expiry(symbol_upper)
                if not expiry_date:
                    return {"status": "error", "message": "No trading expiry available"}
            
            logger.info(f"📊 REST API Call: {symbol} | Expiry: {expiry_date}")
            
            response = requests.get(
                f"{self.base_url}/option/chain",
                headers=self._get_headers(),
                params={
                    "instrument_key": instrument_key,
                    "expiry_date": expiry_date
                },
                timeout=15
            )
            response.raise_for_status()
            
            data = response.json()
            if data.get("status") != "success":
                return {"status": "error", "message": data.get("message", "API failed")}
            
            # Process and structure data
            chain_data = []
            raw_data = data.get("data", [])
            
            for entry in raw_data:
                chain_entry = {
                    "strike_price": entry.get("strike_price"),
                    "expiry": entry.get("expiry"),
                    "pcr": entry.get("pcr"),
                    "underlying_spot_price": entry.get("underlying_spot_price"),
                    "call": {
                        "instrument_key": entry.get("call_options", {}).get("instrument_key"),
                        "ltp": entry.get("call_options", {}).get("market_data", {}).get("ltp", 0),
                        "volume": entry.get("call_options", {}).get("market_data", {}).get("volume", 0),
                        "oi": entry.get("call_options", {}).get("market_data", {}).get("oi", 0),
                        "delta": entry.get("call_options", {}).get("option_greeks", {}).get("delta", 0),
                        "gamma": entry.get("call_options", {}).get("option_greeks", {}).get("gamma", 0),
                        "theta": entry.get("call_options", {}).get("option_greeks", {}).get("theta", 0),
                        "iv": entry.get("call_options", {}).get("option_greeks", {}).get("iv", 0)
                    },
                    "put": {
                        "instrument_key": entry.get("put_options", {}).get("instrument_key"),
                        "ltp": entry.get("put_options", {}).get("market_data", {}).get("ltp", 0),
                        "volume": entry.get("put_options", {}).get("market_data", {}).get("volume", 0),
                        "oi": entry.get("put_options", {}).get("market_data", {}).get("oi", 0),
                        "delta": entry.get("put_options", {}).get("option_greeks", {}).get("delta", 0),
                        "gamma": entry.get("put_options", {}).get("option_greeks", {}).get("gamma", 0),
                        "theta": entry.get("put_options", {}).get("option_greeks", {}).get("theta", 0),
                        "iv": entry.get("put_options", {}).get("option_greeks", {}).get("iv", 0)
                    }
                }
                chain_data.append(chain_entry)
            
            # Cache the result
            cache_key = f"{symbol_upper}_{expiry_date}"
            result = {
                "status": "success",
                "data": chain_data,
                "symbol": symbol_upper,
                "expiry": expiry_date,
                "count": len(chain_data),
                "timestamp": datetime.now().isoformat(),
                "underlying_spot_price": chain_data[0].get("underlying_spot_price", 0) if chain_data else 0
            }
            
            self.option_chains_cache[cache_key] = result
            logger.info(f"✅ Cached option chain: {symbol} - {len(chain_data)} strikes")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ REST API failed: {e}")
            return {"status": "error", "message": f"API request failed: {str(e)}"}
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            return {"status": "error", "message": f"Error: {str(e)}"}
    
    # ============== WEBSOCKET METHODS ==============
    
    def decode_protobuf(self, buffer: bytes) -> Optional[Dict]:
        """Decode protobuf message to dictionary"""
        try:
            feed_response = pb.FeedResponse()
            feed_response.ParseFromString(buffer)
            return MessageToDict(feed_response)
        except Exception as e:
            logger.error(f"❌ Error decoding protobuf: {e}")
            return None
    
    def transform_market_data(self, decoded_data: Dict) -> Optional[Dict]:
        """Transform backend data to frontend structure"""
        if not decoded_data:
            return None
        
        try:
            feeds_data = None
            
            # Extract feeds from various possible structures
            if 'feeds' in decoded_data:
                feeds_data = decoded_data['feeds']
            elif isinstance(decoded_data, dict):
                for key, value in decoded_data.items():
                    if isinstance(value, dict) and 'feeds' in value:
                        feeds_data = value['feeds']
                        break
                
                if not feeds_data:
                    for key, value in decoded_data.items():
                        if isinstance(value, dict) and any(exchange in str(key) for exchange in ['NSE_INDEX', 'BSE_INDEX', 'NSE_FO']):
                            feeds_data = decoded_data
                            break
            
            if not feeds_data:
                return None
            
            transformed_data = {"feeds": {}}
            processed_feeds = 0
            
            for instrument_key, feed_data in feeds_data.items():
                try:
                    if not any(exchange in str(instrument_key) for exchange in ['NSE_INDEX', 'BSE_INDEX', 'NSE_FO']):
                        continue
                    
                    # Extract LTPC data from multiple possible paths
                    ltpc_data = None
                    possible_paths = [
                        ['ff', 'indexFF', 'ltpc'],
                        ['ff', 'marketFF', 'ltpc'],
                        ['ltpc'],
                        ['fullFeed', 'indexFF', 'ltpc'],
                        ['fullFeed', 'marketFF', 'ltpc']
                    ]
                    
                    for path in possible_paths:
                        temp_data = feed_data
                        try:
                            for key in path:
                                if key in temp_data:
                                    temp_data = temp_data[key]
                                else:
                                    temp_data = None
                                    break
                            if temp_data and isinstance(temp_data, dict) and 'ltp' in temp_data:
                                ltpc_data = temp_data
                                break
                        except:
                            continue
                    
                    if not ltpc_data:
                        continue
                    
                    # Create standardized structure
                    transformed_feed = {
                        "fullFeed": {
                            "indexFF": {
                                "ltpc": {
                                    "ltp": float(ltpc_data.get('ltp', 0)),
                                    "cp": float(ltpc_data.get('cp', 0)),
                                    "chp": float(ltpc_data.get('chp', 0)),
                                    "chpPer": float(ltpc_data.get('chpPer', 0))
                                }
                            }
                        }
                    }
                    
                    transformed_data["feeds"][instrument_key] = transformed_feed
                    processed_feeds += 1
                    
                except Exception as e:
                    logger.error(f"❌ Error processing feed for {instrument_key}: {e}")
                    continue
            
            if processed_feeds > 0:
                # Update live prices cache
                self.market_data_cache.update(transformed_data["feeds"])
                return transformed_data
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error transforming data: {e}")
            return None
    
    async def refresh_all_option_data(self):
        """Refresh option data for all symbols"""
        try:
            logger.info("🔄 Refreshing option data for all symbols...")
            successful_fetches = 0
            
            for symbol in self.symbols.keys():
                try:
                    result = self.fetch_option_chain_rest(symbol)
                    
                    if result.get("status") == "success":
                        # Store in cache with symbol key for easy access
                        self.option_chains_cache[symbol] = {
                            "status": "success",
                            "data": result.get("data", []),
                            "count": result.get("count", 0),
                            "expiry": result.get("expiry"),
                            "symbol": symbol,
                            "timestamp": result.get("timestamp"),
                            "underlying_spot_price": result.get("underlying_spot_price", 0)
                        }
                        successful_fetches += 1
                        logger.info(f"✅ {symbol}: {result.get('count', 0)} strikes cached")
                    else:
                        logger.error(f"❌ Failed to refresh {symbol}: {result.get('message')}")
                        
                    await asyncio.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"❌ Error refreshing {symbol}: {e}")
                    continue
            
            self.last_option_refresh = asyncio.get_event_loop().time()
            logger.info(f"✅ Option refresh completed: {successful_fetches}/{len(self.symbols)} symbols")
            
            # Emit updated data to all clients
            if self.sio:
                await self.sio.emit("optionData", {
                    "status": "success",
                    "data": self.option_chains_cache,
                    "count": len(self.option_chains_cache),
                    "successful": successful_fetches,
                    "timestamp": datetime.now().isoformat()
                })
                logger.info("📡 Emitted updated option data to all clients")
            
        except Exception as e:
            logger.error(f"❌ Error in refresh_all_option_data: {e}")
    
    async def websocket_feed_handler(self):
        """Main WebSocket feed handler"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                logger.info(f"🔄 WebSocket connection attempt {retry_count + 1}/{max_retries}")
                
                # Get authorized WebSocket URL
                response = auth.get_market_data_feed_authorize_v3()
                if not response or "data" not in response:
                    logger.error("❌ Failed to get authorized WebSocket URL")
                    retry_count += 1
                    await asyncio.sleep(10)
                    continue
                
                websocket_url = response["data"]["authorized_redirect_uri"]
                logger.info(f"🔗 Connecting to: {websocket_url}")
                
                async with websockets.connect(
                    websocket_url,
                    ssl=ssl_context,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10,
                    compression=None
                ) as websocket:
                    logger.info("✅ WebSocket connected")
                    self.websocket_connected = True
                    retry_count = 0
                    
                    # Initial option data fetch
                    await self.refresh_all_option_data()
                    
                    # Subscribe to market data
                    subscription_instruments = list(self.symbols.values())
                    sub_request = {
                        "guid": "unified_option_guid",
                        "method": "sub",
                        "data": {
                            "mode": "full",
                            "instrumentKeys": subscription_instruments
                        }
                    }
                    
                    await websocket.send(json.dumps(sub_request).encode('utf-8'))
                    logger.info(f"📡 Subscribed to {len(subscription_instruments)} instruments")
                    
                    # Message processing loop
                    message_count = 0
                    last_option_refresh = asyncio.get_event_loop().time()
                    last_log = asyncio.get_event_loop().time()
                    
                    while True:
                        try:
                            message = await websocket.recv()
                            message_count += 1
                            
                            # Decode message
                            decoded = None
                            if isinstance(message, bytes):
                                decoded = self.decode_protobuf(message)
                            else:
                                try:
                                    decoded = json.loads(message)
                                except json.JSONDecodeError:
                                    continue
                            
                            if decoded:
                                # Transform and emit market data
                                transformed_data = self.transform_market_data(decoded)
                                
                                if transformed_data and transformed_data.get("feeds") and self.sio:
                                    await self.sio.emit("marketData", transformed_data)
                                    
                                    # Log periodically
                                    current_time = asyncio.get_event_loop().time()
                                    if current_time - last_log > 30:
                                        feeds_count = len(transformed_data['feeds'])
                                        logger.info(f"📊 Message #{message_count}: {feeds_count} feeds flowing...")
                                        last_log = current_time
                            
                            # Periodic option data refresh (5 minutes)
                            current_time = asyncio.get_event_loop().time()
                            if current_time - last_option_refresh > self.refresh_interval:
                                logger.info("🔄 5-minute option data refresh...")
                                await self.refresh_all_option_data()
                                last_option_refresh = current_time
                        
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("❌ WebSocket connection closed")
                            break
                        except websockets.exceptions.WebSocketException as e:
                            logger.error(f"❌ WebSocket error: {e}")
                            break
                        except Exception as e:
                            logger.error(f"❌ Error processing message: {e}")
                            await asyncio.sleep(0.1)
            
            except Exception as e:
                logger.error(f"❌ WebSocket connection failed: {e}")
                self.websocket_connected = False
                retry_count += 1
                if retry_count < max_retries:
                    await asyncio.sleep(10)
                else:
                    logger.error("❌ Max WebSocket retries exceeded")
                    break
    
    # ============== PUBLIC API METHODS ==============
    
    async def start_unified_feed(self, sio):
        """Start unified option chain + market data feed"""
        logger.info("🚀 Starting unified option chain handler...")
        self.sio = sio
        
        # Start WebSocket feed in background
        asyncio.create_task(self.websocket_feed_handler())
        logger.info("✅ Unified feed started")
    
    def fetch_option_chain(self, symbol: str, expiry_date: Optional[str] = None) -> Dict[str, Any]:
        """Main method to fetch option chain (for compatibility)"""
        return self.fetch_option_chain_rest(symbol, expiry_date)
    
    def get_cached_option_data(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Get cached option data"""
        if symbol:
            return self.option_chains_cache.get(symbol.upper(), {})
        return self.option_chains_cache
    
    def get_live_market_data(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Get live market data from WebSocket"""
        if symbol and symbol.upper() in self.symbols:
            instrument_key = self.symbols[symbol.upper()]
            return self.market_data_cache.get(instrument_key, {})
        return self.market_data_cache
    
    def get_symbol_price(self, symbol: str) -> Optional[Dict[str, float]]:
        """Get current price for a symbol"""
        try:
            instrument_key = self.symbols.get(symbol.upper())
            if instrument_key and instrument_key in self.market_data_cache:
                feed_data = self.market_data_cache[instrument_key]
                ltpc = feed_data.get('fullFeed', {}).get('indexFF', {}).get('ltpc', {})
                return {
                    "ltp": float(ltpc.get('ltp', 0)),
                    "chp": float(ltpc.get('chp', 0)),
                    "chpPer": float(ltpc.get('chpPer', 0)),
                    # Remove timestamp to strictly match Dict[str, float]
                }
            return None
        except Exception as e:
            logger.error(f"❌ Error getting symbol price: {e}")
            return None
    
    def get_complete_option_data(self, symbol: str) -> Dict[str, Any]:
        """Get complete option data (chain + live prices)"""
        try:
            symbol_upper = symbol.upper()
            
            # Get cached option chain
            option_data = self.get_cached_option_data(symbol_upper)
            
            # Get current spot price
            live_price = self.get_symbol_price(symbol_upper)
            spot_price = live_price.get('ltp', 0) if live_price else 0
            
            if option_data and option_data.get('status') == 'success':
                # Update with live spot price
                result = option_data.copy()
                if spot_price > 0:
                    result['current_spot_price'] = spot_price
                    result['live_price_data'] = live_price
                
                return result
            
            # If no cached data, fetch fresh
            return self.fetch_option_chain_rest(symbol_upper)
            
        except Exception as e:
            logger.error(f"❌ Error getting complete option data: {e}")
            return {"status": "error", "message": str(e)}
    
    def get_all_symbols_data(self) -> Dict[str, Any]:
        """Get option data for all symbols"""
        results = {}
        for symbol in self.symbols.keys():
            results[symbol] = self.get_complete_option_data(symbol)
        return results
    
    def get_market_status(self) -> Dict[str, str]:
        """Get market status"""
        try:
            if self.websocket_connected and self.market_data_cache:
                return {"status": "open", "message": "Market data available", "websocket": "connected"}
            
            # Test with a REST API call
            test_result = self.fetch_option_chain_rest('NIFTY')
            if test_result.get('status') == 'success':
                return {"status": "open", "message": "Market data available", "websocket": "disconnected"}
            else:
                return {"status": "closed", "message": "No market data available"}
        except Exception as e:
            logger.error(f"❌ Market status error: {e}")
            return {"status": "error", "message": str(e)}
    
    def get_stats(self) -> Dict[str, Any]:
        """Get handler statistics"""
        return {
            "websocket_connected": self.websocket_connected,
            "cached_option_chains": len(self.option_chains_cache),
            "live_market_data_feeds": len(self.market_data_cache),
            "supported_symbols": list(self.symbols.keys()),
            "cached_expiries": len(self.expiries_cache),
            "last_option_refresh": self.last_option_refresh,
            "has_sio": self.sio is not None,
            "market_holidays_count": len(self.holidays_cache)
        }

# ============== QUICK USAGE FUNCTIONS ==============

def quick_fetch(symbol: str = 'NIFTY', expiry: Optional[str] = None) -> Dict[str, Any]:
    """Quick function to fetch option chain"""
    handler = OptionChainHandler()
    return handler.fetch_option_chain(symbol, expiry)

# ============== MAIN TESTING ==============

if __name__ == "__main__":
    async def test_unified_handler():
        handler = OptionChainHandler()
        
        print("🧪 Testing Unified Option Chain Handler...")
        
        # Test REST API
        print("\n📊 Testing REST API:")
        for symbol in ['NIFTY', 'BANKNIFTY']:
            result = handler.fetch_option_chain_rest(symbol)
            print(f"{symbol}: {result.get('status')} - {result.get('count', 0)} strikes")
        
        # Test market status
        print(f"\n📈 Market Status: {handler.get_market_status()}")
        
        # Test stats
        print(f"\n📋 Stats: {handler.get_stats()}")
        
        print("\n✅ All tests completed!")
    
    # Run tests
    asyncio.run(test_unified_handler())