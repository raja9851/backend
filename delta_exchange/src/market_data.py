import requests
import time
import hmac
import hashlib
import urllib.parse
from . import config
from .mongo_db import save_to_mongodb
import logging

logger = logging.getLogger(__name__)

# Enhanced Rate limiting with proper weights according to Delta Exchange API v2
class RateLimiter:
    def __init__(self, max_quota=10000, window_seconds=300):
        self.max_quota = max_quota
        self.window_seconds = window_seconds
        self.requests = []
        self.current_quota_used = 0
        
        # Rate limit weights according to Delta Exchange API v2 documentation
        self.endpoint_weights = {
            '/v2/products': 3,
            '/v2/l2orderbook': 3,
            '/v2/tickers': 3,
            '/v2/orders': 3,  # GET orders
            '/v2/wallet/balances': 3,
            '/v2/history/candles': 3,
            '/v2/positions/margined': 3,
            '/v2/orders_post': 5,  # POST orders
            '/v2/orders_put': 5,   # PUT orders
            '/v2/orders_delete': 5, # DELETE orders
            '/v2/positions/change_margin': 5,
            '/v2/orders/history': 10,
            '/v2/fills': 10,
            '/v2/wallet/transactions': 10,
            '/v2/orders/batch': 25
        }
    
    def get_endpoint_weight(self, path, method='GET'):
        """Get weight for specific endpoint"""
        if method in ['POST', 'PUT', 'DELETE'] and '/orders' in path:
            if 'batch' in path:
                return 25
            return 5
        return self.endpoint_weights.get(path, 1)
    
    def can_make_request(self, weight=1):
        now = time.time()
        # Remove old requests outside the window
        self.requests = [(req_time, req_weight) for req_time, req_weight in self.requests 
                        if now - req_time < self.window_seconds]
        
        # Calculate current quota used
        self.current_quota_used = sum(weight for _, weight in self.requests)
        
        if self.current_quota_used + weight <= self.max_quota:
            self.requests.append((now, weight))
            return True
        return False
    
    def wait_if_needed(self, endpoint_path='', method='GET'):
        weight = self.get_endpoint_weight(endpoint_path, method)
        
        if not self.can_make_request(weight):
            if self.requests:
                wait_time = self.window_seconds - (time.time() - self.requests[0][0])
                logger.warning(f"Rate limit reached. Waiting {wait_time:.2f} seconds")
                time.sleep(wait_time + 1)  # Add 1 second buffer
                return self.can_make_request(weight)
            else:
                return True
        return True

rate_limiter = RateLimiter()

def make_authenticated_request(method, path, params=None, data=None):
    """Make authenticated request with proper signature and error handling"""
    if not rate_limiter.wait_if_needed(path, method):
        return {"error": "Rate limit exceeded"}
    
    try:
        timestamp = str(int(time.time()))
        query_string = '?' + urllib.parse.urlencode(params) if params else ''
        
        # Handle payload properly
        if data:
            if isinstance(data, dict):
                import json
                payload = json.dumps(data)
            else:
                payload = str(data)
        else:
            payload = ''
        
        # Create signature according to Delta Exchange API v2
        signature_data = method + timestamp + path + query_string + payload
        signature = hmac.new(
            config.API_SECRET.encode('utf-8'),
            signature_data.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        headers = {
            'api-key': config.API_KEY,
            'signature': signature,
            'timestamp': timestamp,
            'User-Agent': 'python-delta-client',
            'Content-Type': 'application/json'
        }
        
        url = f"{config.BASE_URL}{path}"
        
        # Use json parameter instead of data for proper JSON encoding
        if method == 'GET':
            response = requests.get(url, params=params, headers=headers, timeout=(5, 30))
        elif method == 'POST':
            response = requests.post(url, params=params, json=data, headers=headers, timeout=(5, 30))
        elif method == 'PUT':
            response = requests.put(url, params=params, json=data, headers=headers, timeout=(5, 30))
        elif method == 'DELETE':
            response = requests.delete(url, params=params, json=data, headers=headers, timeout=(5, 30))
        else:
            response = requests.request(method, url, params=params, json=data, headers=headers, timeout=(5, 30))
        
        # Handle rate limiting from server
        if response.status_code == 429:
            reset_time = response.headers.get('X-RATE-LIMIT-RESET', '5000')
            wait_seconds = int(reset_time) / 1000
            logger.warning(f"Server rate limit hit. Waiting {wait_seconds} seconds")
            time.sleep(wait_seconds)
            return {"error": "Server rate limit exceeded", "retry_after": wait_seconds}
        
        response.raise_for_status()
        result = response.json()
        
        # Save successful responses to MongoDB
        if result.get('success'):
            save_to_mongodb('api_responses', {
                'endpoint': path,
                'method': method,
                'timestamp': timestamp,
                'response': result,
                'created_at': time.time()
            })
        
        return result
        
    except requests.exceptions.Timeout:
        logger.error("Request timeout")
        return {"error": "Request timeout"}
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return {"error": f"Request failed: {str(e)}"}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {"error": f"Unexpected error: {str(e)}"}

# Market Data Functions
def get_all_assets():
    """Get list of all assets supported on Delta Exchange"""
    logger.info("Fetching all assets")
    try:
        assets = make_authenticated_request('GET', '/v2/assets')
        if assets and assets.get('success'):
            save_to_mongodb('assets', {
                'data': assets,
                'timestamp': time.time(),
                'type': 'all_assets'
            })
        return assets
    except Exception as e:
        logger.error(f"Error fetching assets: {e}")
        return {"error": f"Error fetching assets: {e}"}

def get_all_products(contract_types=None, states='live'):
    """Get list of products with optional filters"""
    logger.info("Fetching all products")
    try:
        params = {'states': states}
        if contract_types:
            params['contract_types'] = contract_types
            
        products = make_authenticated_request('GET', '/v2/products', params=params)
        if products and products.get('success'):
            save_to_mongodb('products', {
                'contract_types': contract_types,
                'states': states,
                'data': products,
                'timestamp': time.time(),
                'type': 'all_products'
            })
        return products
    except Exception as e:
        logger.error(f"Error fetching products: {e}")
        return {"error": f"Error fetching products: {e}"}

def get_product_by_symbol(symbol):
    """Get product details by symbol"""
    logger.info(f"Fetching product details for symbol: {symbol}")
    try:
        path = f'/v2/products/{symbol}'
        product = make_authenticated_request('GET', path)
        if product and product.get('success'):
            save_to_mongodb('products', {
                'symbol': symbol,
                'data': product,
                'timestamp': time.time(),
                'type': 'product_detail'
            })
        return product
    except Exception as e:
        logger.error(f"Error fetching product by symbol: {e}")
        return {"error": f"Error fetching product by symbol: {e}"}

def get_ticker(symbol):
    """Get ticker data for a specific symbol"""
    logger.info(f"Fetching ticker for symbol: {symbol}")
    try:
        path = f"/v2/tickers/{symbol}"
        ticker_data = make_authenticated_request('GET', path)
        if ticker_data and ticker_data.get('success'):
            save_to_mongodb('tickers', {
                'symbol': symbol,
                'data': ticker_data,
                'timestamp': time.time(),
                'type': 'ticker'
            })
        return ticker_data
    except Exception as e:
        logger.error(f"Error fetching ticker: {e}")
        return {"error": f"Error fetching ticker: {e}"}

def get_all_tickers(contract_types=None, underlying_asset_symbols=None):
    """Get all tickers with optional filters"""
    logger.info("Fetching all tickers")
    try:
        params = {}
        if contract_types:
            params['contract_types'] = contract_types
        if underlying_asset_symbols:
            params['underlying_asset_symbols'] = underlying_asset_symbols
            
        tickers = make_authenticated_request('GET', '/v2/tickers', params=params)
        if tickers and tickers.get('success'):
            save_to_mongodb('all_tickers', {
                'contract_types': contract_types,
                'underlying_asset_symbols': underlying_asset_symbols,
                'data': tickers,
                'timestamp': time.time(),
                'type': 'all_tickers'
            })
        return tickers
    except Exception as e:
        logger.error(f"Error fetching all tickers: {e}")
        return {"error": f"Error fetching all tickers: {e}"}

def get_orderbook(symbol, depth=20):
    """Get Level-2 orderbook (bids and asks) for a symbol"""
    logger.info(f"Fetching orderbook for symbol: {symbol}")
    try:
        path = f'/v2/l2orderbook/{symbol}'
        params = {'depth': depth}
        orderbook = make_authenticated_request('GET', path, params=params)
        if orderbook and orderbook.get('success'):
            save_to_mongodb('orderbooks', {
                'symbol': symbol,
                'data': orderbook,
                'timestamp': time.time(),
                'depth': depth,
                'type': 'orderbook'
            })
        return orderbook
    except Exception as e:
        logger.error(f"Error fetching orderbook: {e}")
        return {"error": f"Error fetching orderbook: {e}"}

def get_historical_candles(symbol, resolution='1h', start=None, end=None, limit=500):
    """Get historical OHLC (candlestick) data for a symbol
    
    Args:
        symbol: Product symbol (e.g., BTCUSD)
        resolution: Candle resolution (1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 1w)
        start: Start time in epoch seconds
        end: End time in epoch seconds
        limit: Number of candles to return (max 500)
    """
    logger.info(f"Fetching historical candles for symbol: {symbol}")
    try:
        # Get product_id first since candles API requires product_id
        product = get_product_by_symbol(symbol)
        if not product or not product.get('success'):
            return {"error": f"Product not found for symbol: {symbol}"}
        
        product_id = product['result']['id']
        
        path = '/v2/history/candles'
        params = {
            'symbol': symbol,
            'resolution': resolution
        }
        
        if start:
            params['start'] = int(start) if isinstance(start, (int, float)) else start
        if end:
            params['end'] = int(end) if isinstance(end, (int, float)) else end
        if limit:
            params['limit'] = limit
            
        result = make_authenticated_request('GET', path, params=params)
        
        if result and result.get('success'):
            save_to_mongodb('candles', {
                'symbol': symbol,
                'product_id': product_id,
                'resolution': resolution,
                'start': start,
                'end': end,
                'data': result,
                'timestamp': time.time(),
                'type': 'historical_candles'
            })
        return result
    except Exception as e:
        logger.error(f"Error fetching historical candles: {e}")
        return {"error": f"Error fetching historical candles: {e}"}

def get_live_candles(symbol, resolution='1m'):
    """Get current/live candle data for a symbol
    
    Args:
        symbol: Product symbol
        resolution: Candle resolution
    """
    logger.info(f"Fetching live candles for symbol: {symbol}")
    try:
        # Get latest candle (limit=1)
        return get_historical_candles(symbol, resolution=resolution, limit=1)
    except Exception as e:
        logger.error(f"Error fetching live candles: {e}")
        return {"error": f"Error fetching live candles: {e}"}

def get_option_chain(underlying_asset='BTC', expiry_date=None):
    """Get option chain data for an underlying asset"""
    logger.info(f"Fetching option chain for: {underlying_asset}")
    try:
        params = {
            'contract_types': 'call_options,put_options',
            'underlying_asset_symbols': underlying_asset
        }
        
        if expiry_date:
            params['expiry_date'] = expiry_date
        
        result = make_authenticated_request('GET', '/v2/tickers', params=params)
        
        if result and result.get('success'):
            save_to_mongodb('option_chains', {
                'underlying_asset': underlying_asset,
                'expiry_date': expiry_date,
                'data': result,
                'timestamp': time.time(),
                'type': 'option_chain'
            })
        return result
    except Exception as e:
        logger.error(f"Error fetching option chain: {e}")
        return {"error": f"Error fetching option chain: {e}"}

def get_trades(symbol):
    """Get recent trades data for a symbol"""
    logger.info(f"Fetching recent trades for symbol: {symbol}")
    try:
        path = f"/v2/trades/{symbol}"
        result = make_authenticated_request('GET', path)
        
        if result and result.get('success'):
            save_to_mongodb('trades', {
                'symbol': symbol,
                'data': result,
                'timestamp': time.time(),
                'type': 'recent_trades'
            })
        return result
    except Exception as e:
        logger.error(f"Error fetching trades: {e}")
        return {"error": f"Error fetching trades: {e}"}

def get_indices():
    """Get index prices"""
    logger.info("Fetching indices")
    try:
        result = make_authenticated_request('GET', '/v2/indices')
        
        if result and result.get('success'):
            save_to_mongodb('indices', {
                'data': result,
                'timestamp': time.time(),
                'type': 'indices'
            })
        return result
    except Exception as e:
        logger.error(f"Error fetching indices: {e}")
        return {"error": f"Error fetching indices: {e}"}

def get_24hr_stats():
    """Get 24 hour volume statistics"""
    logger.info("Fetching 24hr stats")
    try:
        result = make_authenticated_request('GET', '/v2/stats')
        
        if result and result.get('success'):
            save_to_mongodb('stats', {
                'data': result,
                'timestamp': time.time(),
                'type': '24hr_stats'
            })
        return result
    except Exception as e:
        logger.error(f"Error fetching 24hr stats: {e}")
        return {"error": f"Error fetching 24hr stats: {e}"}

# Utility functions
def get_product_id_by_symbol(symbol):
    """Get product ID from symbol"""
    try:
        product = get_product_by_symbol(symbol)
        if product and product.get('success') and product.get('result'):
            result = product['result']
            if isinstance(result, dict):
                return result.get('id')
            else:
                return result
        return None
    except Exception as e:
        logger.error(f"Error getting product ID: {e}")
        return None

def validate_product_symbol(symbol):
    """Check if product symbol exists"""
    try:
        product = get_product_by_symbol(symbol)
        return product and product.get('success') and product.get('result')
    except Exception as e:
        return False

def get_candle_data_formatted(symbol, resolution='1h', start=None, end=None, limit=100):
    """Get historical candles in a formatted structure
    
    Returns:
        dict: {
            'symbol': str,
            'candles': [
                {
                    'timestamp': int,
                    'open': float,
                    'high': float,
                    'low': float,
                    'close': float,
                    'volume': float
                }
            ]
        }
    """
    try:
        candles_data = get_historical_candles(symbol, resolution, start, end, limit)
        
        if not candles_data or not candles_data.get('success'):
            return {"error": "Failed to fetch candle data"}
        
        candles = candles_data.get('result', [])
        formatted_candles = []
        
        for candle in candles:
            if isinstance(candle, list) and len(candle) >= 6:
                # Standard OHLCV format: [timestamp, open, high, low, close, volume]
                formatted_candles.append({
                    'timestamp': int(candle[0]),
                    'open': float(candle[1]),
                    'high': float(candle[2]),
                    'low': float(candle[3]),
                    'close': float(candle[4]),
                    'volume': float(candle[5])
                })
            elif isinstance(candle, dict):
                # Object format
                formatted_candles.append({
                    'timestamp': int(candle.get('timestamp', 0)),
                    'open': float(candle.get('open', 0)),
                    'high': float(candle.get('high', 0)),
                    'low': float(candle.get('low', 0)),
                    'close': float(candle.get('close', 0)),
                    'volume': float(candle.get('volume', 0))
                })
        
        return {
            'symbol': symbol,
            'resolution': resolution,
            'candles': formatted_candles
        }
        
    except Exception as e:
        logger.error(f"Error formatting candle data: {e}")
        return {"error": f"Error formatting candle data: {e}"}