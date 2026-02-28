# filename: market_data_handler.py (Fixed for Option Chain Integration)
import asyncio
import json
import ssl
import websockets
from google.protobuf.json_format import MessageToDict
import MarketDataFeedV3_pb2 as pb
from auth import auth
from datetime import datetime

class MarketDataHandler:
    def __init__(self):
        self.latest_market_data = {}
        self.option_data_cache = {}
        self.last_option_fetch = None
        self.sio = None
        self.option_chain_handler = None
    
    def get_latest_data(self):
        """Get latest market data"""
        return self.latest_market_data
    
    def get_option_data(self):
        """Get cached option data"""
        return self.option_data_cache
    
    # ✅ Fixed option data fetching using the working option chain handler
    async def fetch_option_data(self, option_chain_handler):
        """Fetch option data for major indices - Fixed Integration"""
        if not option_chain_handler:
            print("❌ Cannot fetch option data - handler not initialized")
            return
        
        try:
            print("🎯 Fetching option chain data using working handler...")
            
            # Get option data for all supported symbols
            symbols = list(option_chain_handler.symbols.keys())
            successful_fetches = 0
            
            for symbol in symbols:
                try:
                    print(f"📊 Fetching {symbol} option data...")
                    
                    # Use the working fetch_option_chain method
                    result = option_chain_handler.fetch_option_chain(symbol)
                    
                    if result.get("status") == "success":
                        # Store the complete result
                        self.option_data_cache[symbol] = {
                            "status": "success",
                            "data": result.get("data", []),
                            "count": result.get("count", 0),
                            "expiry": result.get("expiry"),
                            "symbol": symbol,
                            "timestamp": result.get("timestamp", datetime.now().isoformat()),
                            "underlying_spot_price": self._extract_spot_price(result.get("data", []))
                        }
                        
                        successful_fetches += 1
                        print(f"✅ {symbol} option data cached: {result.get('count', 0)} strikes")
                    else:
                        print(f"❌ Failed to get {symbol} option data: {result.get('message', 'Unknown error')}")
                        
                        # Store error result
                        self.option_data_cache[symbol] = {
                            "status": "error",
                            "error": result.get("message", "Failed to fetch data"),
                            "symbol": symbol,
                            "timestamp": datetime.now().isoformat()
                        }
                    
                    # Rate limiting to avoid overwhelming the API
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    print(f"❌ Error fetching {symbol} option data: {e}")
                    self.option_data_cache[symbol] = {
                        "status": "error",
                        "error": str(e),
                        "symbol": symbol,
                        "timestamp": datetime.now().isoformat()
                    }
                    continue
            
            self.last_option_fetch = asyncio.get_event_loop().time()
            print(f"✅ Option data fetch completed. Success: {successful_fetches}/{len(symbols)} symbols")
            
            # Emit updated option data to all clients
            if self.sio and self.option_data_cache:
                await self.sio.emit("optionData", {
                    "status": "success",
                    "data": self.option_data_cache,
                    "count": len(self.option_data_cache),
                    "successful": successful_fetches,
                    "timestamp": datetime.now().isoformat()
                })
                print(f"📡 Emitted option data to all clients")
            
        except Exception as e:
            print(f"❌ Error in fetch_option_data: {e}")
    
    def _extract_spot_price(self, option_data):
        """Extract underlying spot price from option data"""
        if option_data and len(option_data) > 0:
            return option_data[0].get("underlying_spot_price", 0)
        return 0
    
    # ✅ Decode Protobuf message
    def decode_protobuf(self, buffer):
        """Decode protobuf message to dictionary"""
        try:
            feed_response = pb.FeedResponse()
            feed_response.ParseFromString(buffer)
            return MessageToDict(feed_response)
        except Exception as e:
            print(f"❌ Error decoding protobuf: {e}")
            return None
    
    # ✅ Enhanced transform function for better data handling
    def transform_market_data(self, decoded_data):
        """Transform backend data to match frontend structure"""
        if not decoded_data:
            return None
        
        try:
            # Handle different possible data structures
            feeds_data = None
            
            # Check if data has 'feeds' key directly
            if 'feeds' in decoded_data:
                feeds_data = decoded_data['feeds']
            elif isinstance(decoded_data, dict):
                # Look for feeds in various possible locations
                for key, value in decoded_data.items():
                    if isinstance(value, dict) and 'feeds' in value:
                        feeds_data = value['feeds']
                        break
                
                # If still no feeds found, check if the data itself contains market data
                if not feeds_data:
                    for key, value in decoded_data.items():
                        if isinstance(value, dict) and any(exchange in str(key) for exchange in ['NSE_INDEX', 'BSE_INDEX', 'NSE_FO']):
                            feeds_data = decoded_data
                            break
            
            if not feeds_data:
                return None
            
            # Create the structure that frontend expects
            transformed_data = {"feeds": {}}
            processed_feeds = 0
            
            # Process each feed
            for instrument_key, feed_data in feeds_data.items():
                try:
                    # Skip if not a valid instrument
                    if not any(exchange in str(instrument_key) for exchange in ['NSE_INDEX', 'BSE_INDEX', 'NSE_FO']):
                        continue
                    
                    # Extract LTPC and other data
                    ltpc_data = None
                    market_data = {}
                    
                    # Handle different data structures
                    if isinstance(feed_data, dict):
                        # Try multiple paths to find LTPC data
                        possible_paths = [
                            ['ff', 'indexFF', 'ltpc'],
                            ['ff', 'marketFF', 'ltpc'],
                            ['ltpc'],
                            ['fullFeed', 'indexFF', 'ltpc'],
                            ['fullFeed', 'marketFF', 'ltpc'],
                            ['fullFeed', 'ltpc']
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
                        
                        # Extract additional market data
                        if 'ff' in feed_data:
                            if 'indexFF' in feed_data['ff']:
                                market_data = feed_data['ff']['indexFF']
                            elif 'marketFF' in feed_data['ff']:
                                market_data = feed_data['ff']['marketFF']
                        elif 'fullFeed' in feed_data:
                            if 'indexFF' in feed_data['fullFeed']:
                                market_data = feed_data['fullFeed']['indexFF']
                            elif 'marketFF' in feed_data['fullFeed']:
                                market_data = feed_data['fullFeed']['marketFF']
                    
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
                    
                    # Add additional market data if available
                    if market_data and isinstance(market_data, dict):
                        additional_data = {}
                        for key in ['callOI', 'putOI', 'pcr', 'iv', 'volume', 'oi', 'bid', 'ask', 'vol']:
                            if key in market_data:
                                try:
                                    additional_data[key] = float(market_data[key])
                                except:
                                    additional_data[key] = 0
                        
                        if additional_data:
                            transformed_feed["fullFeed"]["indexFF"].update(additional_data)
                    
                    transformed_data["feeds"][instrument_key] = transformed_feed
                    processed_feeds += 1
                    
                except Exception as e:
                    print(f"❌ Error processing feed for {instrument_key}: {e}")
                    continue
            
            if processed_feeds > 0:
                # Update global latest data
                self.latest_market_data.update(transformed_data["feeds"])
                
                # Log periodic updates
                if processed_feeds % 10 == 0 or processed_feeds < 10:
                    print(f"📈 Processed {processed_feeds} market feeds")
                
                return transformed_data
            
            return None
            
        except Exception as e:
            print(f"❌ Error transforming data: {e}")
            return None
    
    # ✅ Enhanced WebSocket connection with better error handling
    async def fetch_market_data(self):
        """Main WebSocket connection handler - Fixed"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                print(f"🔄 Attempting WebSocket connection (attempt {retry_count + 1}/{max_retries})")
                
                # Get WebSocket URL from auth
                response = auth.get_market_data_feed_authorize_v3()
                
                if not response or "data" not in response:
                    print(f"❌ Failed to get authorized URL. Retry {retry_count + 1}/{max_retries}")
                    retry_count += 1
                    await asyncio.sleep(10)
                    continue
                
                websocket_url = response["data"]["authorized_redirect_uri"]
                print(f"🔗 Connecting to WebSocket: {websocket_url}")
                
                async with websockets.connect(
                    websocket_url,
                    ssl=ssl_context,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10,
                    compression=None
                ) as websocket:
                    print("✅ WebSocket connection established")
                    retry_count = 0
                    
                    # Fetch initial option data using the working handler
                    if self.option_chain_handler:
                        print("🎯 Fetching initial option data...")
                        await self.fetch_option_data(self.option_chain_handler)
                    
                    # Subscribe to major indices using correct instrument keys
                    if self.option_chain_handler and hasattr(self.option_chain_handler, 'symbols'):
                        subscription_instruments = list(self.option_chain_handler.symbols.values())
                    else:
                        # Fallback to hardcoded values
                        subscription_instruments = [
                            "NSE_INDEX|Nifty 50",
                            "NSE_INDEX|Nifty Bank",
                            "NSE_INDEX|Nifty Fin Service",
                            "NSE_INDEX|Nifty MidCap 100",
                            "BSE_INDEX|SENSEX"
                        ]
                    
                    sub_request = {
                        "guid": "marketdata_guid",
                        "method": "sub",
                        "data": {
                            "mode": "full",
                            "instrumentKeys": subscription_instruments
                        }
                    }
                    
                    print(f"📡 Subscribing to {len(subscription_instruments)} instruments...")
                    await websocket.send(json.dumps(sub_request).encode('utf-8'))
                    print("✅ Subscription request sent")
                    
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
                                    try:
                                        await self.sio.emit("marketData", transformed_data)
                                    except Exception as e:
                                        print(f"❌ Error emitting market data: {e}")
                                    
                                    # Log periodically
                                    current_time = asyncio.get_event_loop().time()
                                    if current_time - last_log > 30:  # Log every 30 seconds
                                        feeds_count = len(transformed_data['feeds'])
                                        print(f"📊 Message #{message_count}: {feeds_count} feeds | Market data flowing...")
                                        last_log = current_time
                            
                            # Refresh option data every 5 minutes
                            current_time = asyncio.get_event_loop().time()
                            if current_time - last_option_refresh > 300:  # 5 minutes
                                if self.option_chain_handler:
                                    print("🔄 Refreshing option data (5 min interval)...")
                                    await self.fetch_option_data(self.option_chain_handler)
                                last_option_refresh = current_time
                        
                        except websockets.exceptions.ConnectionClosed as e:
                            print(f"❌ WebSocket connection closed: {e}")
                            break
                        except websockets.exceptions.WebSocketException as e:
                            print(f"❌ WebSocket error: {e}")
                            break
                        except Exception as e:
                            print(f"❌ Error processing message #{message_count}: {e}")
                            # Don't break on individual message errors
                            await asyncio.sleep(0.1)
            
            except Exception as e:
                print(f"❌ WebSocket connection failed: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"🔁 Reconnecting in 10 seconds... (attempt {retry_count}/{max_retries})")
                    await asyncio.sleep(10)
                else:
                    print("❌ Max retries exceeded for WebSocket connection")
                    break
    
    # ✅ Start market data feed with socket.io reference - Fixed
    async def start_market_data_feed(self, sio, option_chain_handler):
        """Initialize and start the market data feed - Fixed Integration"""
        print("🚀 Starting market data feed with fixed integration...")
        
        self.sio = sio
        self.option_chain_handler = option_chain_handler
        
        # Validate option chain handler
        if not option_chain_handler:
            print("❌ Option chain handler not provided")
            return
        
        # Test option chain handler
        try:
            test_result = option_chain_handler.fetch_option_chain('NIFTY')
            if test_result.get("status") == "success":
                print(f"✅ Option chain handler validated - {test_result.get('count', 0)} strikes")
            else:
                print(f"❌ Option chain handler test failed: {test_result.get('message')}")
        except Exception as e:
            print(f"❌ Error validating option chain handler: {e}")
        
        # Start WebSocket connection
        try:
            await self.fetch_market_data()
        except Exception as e:
            print(f"❌ Error in market data feed: {e}")
    
    # ✅ Additional helper methods
    def get_symbol_price(self, symbol):
        """Get current price for a symbol"""
        try:
            if not self.option_chain_handler or not hasattr(self.option_chain_handler, 'symbols'):
                return None
            
            instrument_key = self.option_chain_handler.symbols.get(symbol.upper())
            if instrument_key and instrument_key in self.latest_market_data:
                feed_data = self.latest_market_data[instrument_key]
                if isinstance(feed_data, dict) and 'fullFeed' in feed_data:
                    ltpc = feed_data['fullFeed'].get('indexFF', {}).get('ltpc', {})
                    return {
                        "ltp": ltpc.get('ltp', 0),
                        "chp": ltpc.get('chp', 0),
                        "chpPer": ltpc.get('chpPer', 0)
                    }
            return None
        except Exception as e:
            print(f"❌ Error getting symbol price: {e}")
            return None
    
    def get_market_summary(self):
        """Get market summary for all tracked symbols"""
        try:
            summary = {}
            if not self.option_chain_handler or not hasattr(self.option_chain_handler, 'symbols'):
                return summary
            
            for symbol, instrument_key in self.option_chain_handler.symbols.items():
                price_data = self.get_symbol_price(symbol)
                if price_data:
                    summary[symbol] = {
                        "price": price_data["ltp"],
                        "change": price_data["chp"],
                        "change_percent": price_data["chpPer"]
                    }
            
            return summary
        except Exception as e:
            print(f"❌ Error getting market summary: {e}")
            return {}
    
    def get_stats(self):
        """Get handler statistics"""
        return {
            "latest_data_count": len(self.latest_market_data),
            "option_cache_count": len(self.option_data_cache),
            "last_option_fetch": self.last_option_fetch,
            "has_sio": self.sio is not None,
            "has_option_handler": self.option_chain_handler is not None,
            "cached_symbols": list(self.option_data_cache.keys()) if self.option_data_cache else []
        }