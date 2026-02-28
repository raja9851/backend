import requests
import hmac
import hashlib
import time
import urllib.parse
from . import config
import logging

logger = logging.getLogger(__name__)

class DeltaExchangeClient:
    """Delta Exchange REST API Client"""
    
    def __init__(self):
        self.base_url = config.BASE_URL
        self.api_key = config.API_KEY
        self.api_secret = config.API_SECRET
    
    def _generate_signature(self, method, path, query_string="", payload=""):
        """Generate API signature"""
        timestamp = str(int(time.time()))
        signature_data = method + timestamp + path + query_string + payload
        
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            signature_data.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature, timestamp
    
    def _make_request(self, method, path, params=None, data=None):
        """Make authenticated API request"""
        try:
            query_string = '?' + urllib.parse.urlencode(params) if params else ''
            payload = str(data) if data else ''
            
            signature, timestamp = self._generate_signature(method, path, query_string, payload)
            
            headers = {
                'api-key': self.api_key,
                'signature': signature,
                'timestamp': timestamp,
                'Content-Type': 'application/json'
            }
            
            url = f"{self.base_url}{path}"
            response = requests.request(method, url, params=params, json=data, headers=headers, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"API request failed: {e}")
            return {"error": str(e)}
    
    # Market Data Methods
    def get_assets(self):
        """Get all assets"""
        return self._make_request('GET', '/v2/assets')
    
    def get_product(self, product_id):
        """Get product details"""
        return self._make_request('GET', f'/v2/products/{product_id}')
    
    def get_ticker(self, symbol):
        """Get ticker data"""
        return self._make_request('GET', f'/v2/tickers/{symbol}')
    
    def get_l2_orderbook(self, product_id):
        """Get L2 orderbook"""
        return self._make_request('GET', f'/v2/l2orderbook/{product_id}')
    
    # Account Methods  
    def get_balances(self, asset_id=None):
        """Get wallet balances"""
        path = f'/v2/wallet/balances/{asset_id}' if asset_id else '/v2/wallet/balances'
        return self._make_request('GET', path)
    
    def get_position(self, product_id):
        """Get position for product"""
        return self._make_request('GET', f'/v2/positions/{product_id}')
    
    def get_live_orders(self):
        """Get open orders"""
        return self._make_request('GET', '/v2/orders')
    
    def order_history(self, query, page_size=50):
        """Get order history"""
        params = {**query, 'page_size': page_size}
        return self._make_request('GET', '/v2/orders/history', params=params)
    
    def fills(self, query, page_size=50):
        """Get fills history"""
        params = {**query, 'page_size': page_size}
        return self._make_request('GET', '/v2/fills', params=params)

# Global client instance
delta_client = DeltaExchangeClient()