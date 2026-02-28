"""
Delta Exchange REST & WebSocket Client - FIXED VERSION
Core client implementation with corrected authentication
"""

import os
import time
import json
import hmac
import hashlib
import logging
import threading
import queue
import secrets
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import websocket

# Load environment
from dotenv import load_dotenv
load_dotenv()

# Configuration
ENV = os.getenv("DELTA_ENV", "india").strip().lower()
API_KEY = os.getenv("DELTA_API_KEY", "")
API_SECRET = os.getenv("DELTA_API_SECRET", "")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes")
USER_AGENT = os.getenv("USER_AGENT", "delta-exchange-client/1.0")

# --- DEBUG BLOCK (Remove in production) ---
if API_KEY and API_SECRET:
    print("--- 🕵️  DEBUGGING CREDENTIALS 🕵️  ---")
    print(f"Loaded API Key: '{API_KEY[:4]}...{API_KEY[-4:]}'") 
    print(f"Loaded API Secret: '{API_SECRET[:4]}...{API_SECRET[-4:]}'")
    print("------------------------------------")
else:
    print("⚠️  WARNING: No API credentials found in environment")

ENVIRONMENTS = {
    "india": {
        "rest": "https://api.india.delta.exchange",
        "ws": "wss://socket.india.delta.exchange",
    },
    "global": {
        "rest": "https://api.delta.exchange", 
        "ws": "wss://socket.delta.exchange",
    },
    "testnet": {
        "rest": "https://cdn.testnet.delta.exchange",
        "ws": "wss://socket.testnet.delta.exchange",
    },
}

if ENV not in ENVIRONMENTS:
    raise RuntimeError(f"Unknown DELTA_ENV='{ENV}'. Use: {list(ENVIRONMENTS)}")

REST_API_URL = ENVIRONMENTS[ENV]["rest"]
WS_API_URL = ENVIRONMENTS[ENV]["ws"]

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("delta")

def _redact(obj: Any, max_len: int = 200) -> str:
    """Redact long values in logs for security"""
    s = json.dumps(obj, default=str, ensure_ascii=False)
    return s if len(s) <= max_len else s[:max_len] + "...[REDACTED]"


class DeltaRESTClient:
    """Delta Exchange REST API Client with fixed authentication"""
    
    def __init__(self, api_key: str = API_KEY, api_secret: str = API_SECRET):
        self.base_url = REST_API_URL
        self.api_key = api_key
        self.api_secret = api_secret
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Content-Type": "application/json"
        })

    def _sign_request(self, method: str, path: str, params: Optional[Dict] = None, 
                     data: Optional[Dict] = None) -> Tuple[Dict[str, str], str]:
        """Generate authentication headers and body - FIXED VERSION"""
        timestamp = str(int(time.time()))
        
        # Build query string
        query_string = ""
        if params:
            query_string = "?" + urlencode(params, doseq=True)
        
        # Build request body
        body = ""
        if method.upper() != "GET" and data:
            body = json.dumps(data, separators=(",", ":"), sort_keys=True)
        
        # Create signature string: METHOD + TIMESTAMP + PATH + QUERY + BODY
        message = method.upper() + timestamp + path + query_string + body
        
        # Generate HMAC-SHA256 signature
        signature = hmac.new(
            self.api_secret.encode('utf-8'), 
            message.encode('utf-8'), 
            hashlib.sha256
        ).hexdigest()
        
        # Debug signature generation (remove in production)
        logger.debug(f"Signature Message: {message}")
        logger.debug(f"Generated Signature: {signature}")
        
        headers = {
            "api-key": self.api_key,
            "timestamp": timestamp,
            "signature": signature
        }
        
        return headers, body

    def _request(self, method: str, path: str, params: Optional[Dict] = None,
                data: Optional[Dict] = None, signed: bool = False) -> Any:
        """Make HTTP request with improved error handling"""
        url = self.base_url + path
        headers = {}
        request_data = None
        
        if signed:
            if not (self.api_key and self.api_secret):
                raise RuntimeError("API credentials required for signed requests")
            
            auth_headers, body = self._sign_request(method, path, params, data)
            headers.update(auth_headers)
            
            if method.upper() != "GET" and body:
                request_data = body
        else:
            if data and method.upper() != "GET":
                request_data = json.dumps(data, separators=(",", ":"))
        
        try:
            logger.debug("Request: %s %s", method.upper(), path)
            if signed:
                logger.debug("Headers: %s", {k: v for k, v in headers.items() if k != 'signature'})
            
            response = self.session.request(
                method, 
                url, 
                params=params, 
                headers=headers, 
                data=request_data, 
                timeout=(10, 30)
            )
            
            # Log response for debugging
            logger.debug("Response Status: %s", response.status_code)
            
            if response.status_code == 400:
                error_text = response.text
                logger.error("Bad Request (400) Response: %s", error_text)
                
                # Common Delta Exchange API errors
                if "SignatureExpired" in error_text:
                    raise requests.exceptions.RequestException("Signature expired - check system time")
                elif "InvalidSignature" in error_text:
                    raise requests.exceptions.RequestException("Invalid signature - check API credentials")
                elif "InvalidApiKey" in error_text:
                    raise requests.exceptions.RequestException("Invalid API key")
                elif "IPNotWhitelisted" in error_text:
                    raise requests.exceptions.RequestException("IP address not whitelisted")
                else:
                    raise requests.exceptions.RequestException(f"Bad Request: {error_text}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error("Request failed: %s", e)
            raise

    # Public endpoints
    def get_products(self, page_size: int = 200) -> Dict:
        """Get all tradeable products"""
        return self._request("GET", "/v2/products", {"page_size": page_size})
    
    def get_product(self, symbol: str) -> Dict:
        """Get product by symbol"""
        return self._request("GET", f"/v2/products/{symbol}")
    
    def get_ticker(self, symbol: str) -> Dict:
        """Get ticker for symbol"""
        return self._request("GET", "/v2/tickers", {"symbol": symbol})
    
    def get_orderbook(self, symbol: str, depth: int = 10) -> Dict:
        """Get orderbook for symbol"""
        return self._request("GET", f"/v2/l2orderbook/{symbol}", {"depth": depth})
    
    def get_candles(self, symbol: str, resolution: str, start: int, end: int) -> Dict:
        """Get historical candles"""
        params = {"symbol": symbol, "resolution": resolution, "start": start, "end": end}
        return self._request("GET", "/v2/history/candles", params)

    # Private endpoints
    def get_balance(self) -> Dict:
        """Get wallet balance"""
        return self._request("GET", "/v2/wallet/balances", signed=True)
    
    def get_positions(self, state: Optional[str] = None) -> Dict:
        """Get positions"""
        params = {"state": state} if state else {}
        return self._request("GET", "/v2/positions", params, signed=True)
    
    def get_orders(self, product_id: Optional[int] = None, state: Optional[str] = None) -> Dict:
        """Get orders"""
        params = {}
        if product_id:
            params["product_id"] = product_id
        if state:
            params["state"] = state
        return self._request("GET", "/v2/orders", params, signed=True)

    def place_order(self, product_id: int, side: str, size: int, 
                   order_type: str = "limit_order", limit_price: Optional[str] = None,
                   time_in_force: str = "gtc", post_only: bool = False,
                   client_order_id: Optional[str] = None) -> Dict:
        """Place order with safety checks"""
        if order_type == "limit_order" and not limit_price:
            raise ValueError("limit_price required for limit orders")
        
        payload = {
            "product_id": product_id,
            "side": side,
            "size": size,
            "order_type": order_type,
            "time_in_force": time_in_force,
            "post_only": post_only
        }
        
        if limit_price:
            payload["limit_price"] = str(limit_price)
        
        if not client_order_id:
            client_order_id = f"py-{int(time.time())}-{secrets.token_hex(4)}"
        payload["client_order_id"] = client_order_id
        
        if DRY_RUN:
            logger.info("DRY_RUN: Would place order: %s", _redact(payload))
            return {"dry_run": True, "payload": payload}
        
        return self._request("POST", "/v2/orders", data=payload, signed=True)
    
    def cancel_order(self, order_id: str) -> Dict:
        """Cancel order by ID"""
        if DRY_RUN:
            logger.info("DRY_RUN: Would cancel order: %s", order_id)
            return {"dry_run": True, "order_id": order_id}
        
        return self._request("DELETE", f"/v2/orders/{order_id}", signed=True)
    
    def cancel_all_orders(self, product_id: Optional[int] = None) -> Dict:
        """Cancel all orders"""
        data = {"product_id": product_id} if product_id else {}
        
        if DRY_RUN:
            logger.info("DRY_RUN: Would cancel all orders: %s", data)
            return {"dry_run": True, "data": data}
        
        return self._request("DELETE", "/v2/orders/all", data=data, signed=True)


class DeltaWebSocket:
    """Delta Exchange WebSocket Client with improved connection handling"""
    
    def __init__(self, api_key: str = API_KEY, api_secret: str = API_SECRET):
        self.url = WS_API_URL
        self.api_key = api_key
        self.api_secret = api_secret
        self.ws = None
        self.message_queue = queue.Queue()
        self._stop_event = threading.Event()
        
    def _on_open(self, ws):
        logger.info("WebSocket connected")
        # Send queued messages
        while not self.message_queue.empty():
            try:
                message = self.message_queue.get_nowait()
                ws.send(message)
            except queue.Empty:
                break
    
    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            msg_type = data.get("type", "")
            
            if msg_type == "heartbeat":
                return
            elif msg_type in ["v2/ticker", "candlestick_1m", "candlestick_5m", "candlestick_1h", "l2_orderbook"]:
                logger.debug("Market data: %s", msg_type)
            else:
                logger.debug("WebSocket message: %s", _redact(data))
        except json.JSONDecodeError:
            logger.debug("Non-JSON message: %s", message)
    
    def _on_error(self, ws, error):
        logger.error("WebSocket error: %s", error)
    
    def _on_close(self, ws, code, reason):
        logger.info("WebSocket closed: %s %s", code, reason)
    
    def connect(self):
        """Connect to WebSocket"""
        if self.ws:
            return
        
        # Enable WebSocket debug logging
        websocket.enableTrace(False)  # Set to True for debugging
        
        self.ws = websocket.WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        thread = threading.Thread(
            target=self.ws.run_forever, 
            kwargs={"ping_interval": 30, "ping_timeout": 10},
            daemon=True
        )
        thread.start()
        time.sleep(1)  # Allow connection
    
    def disconnect(self):
        """Disconnect WebSocket"""
        self._stop_event.set()
        if self.ws:
            self.ws.close()
    
    def _send_message(self, data: Dict):
        """Send message to WebSocket"""
        message = json.dumps(data)
        if self.ws and hasattr(self.ws, 'sock') and self.ws.sock:
            self.ws.send(message)
        else:
            self.message_queue.put(message)
    
    def authenticate(self):
        """Authenticate WebSocket connection"""
        if not (self.api_key and self.api_secret):
            raise RuntimeError("API credentials required for authentication")
        
        timestamp = str(int(time.time()))
        message = "GET" + timestamp + "/live"
        signature = hmac.new(
            self.api_secret.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        
        auth_data = {
            "type": "auth",
            "payload": {
                "api-key": self.api_key,
                "signature": signature,
                "timestamp": timestamp
            }
        }
        self._send_message(auth_data)
    
    def subscribe_ticker(self, symbols: List[str]):
        """Subscribe to ticker updates"""
        subscribe_data = {
            "type": "subscribe",
            "payload": {
                "channels": [{
                    "name": "v2/ticker",
                    "symbols": symbols
                }]
            }
        }
        self._send_message(subscribe_data)
    
    def subscribe_orderbook(self, symbols: List[str]):
        """Subscribe to orderbook updates"""
        subscribe_data = {
            "type": "subscribe", 
            "payload": {
                "channels": [{
                    "name": "l2_orderbook",
                    "symbols": symbols
                }]
            }
        }
        self._send_message(subscribe_data)
    
    def subscribe_candles(self, symbols: List[str], resolution: str = "1m"):
        """Subscribe to live candle updates"""
        subscribe_data = {
            "type": "subscribe",
            "payload": {
                "channels": [{
                    "name": f"candlestick_{resolution}",
                    "symbols": symbols
                }]
            }
        }
        self._send_message(subscribe_data)
    
    def subscribe_trades(self, symbols: List[str]):
        """Subscribe to trade updates"""
        subscribe_data = {
            "type": "subscribe",
            "payload": {
                "channels": [{
                    "name": "all_trades",
                    "symbols": symbols
                }]
            }
        }
        self._send_message(subscribe_data)
    
    def subscribe_user_trades(self):
        """Subscribe to user's trade updates (requires auth)"""
        subscribe_data = {
            "type": "subscribe",
            "payload": {
                "channels": [{
                    "name": "user_trades"
                }]
            }
        }
        self._send_message(subscribe_data)
    
    def subscribe_user_orders(self):
        """Subscribe to user's order updates (requires auth)"""
        subscribe_data = {
            "type": "subscribe",
            "payload": {
                "channels": [{
                    "name": "orders"
                }]
            }
        }
        self._send_message(subscribe_data)
    
    def unsubscribe_channel(self, channel: str, symbols: Optional[List[str]] = None):
        """Unsubscribe from a channel"""
        unsubscribe_data = {
            "type": "unsubscribe",
            "payload": {
                "channels": [{
                    "name": channel,
                    **({"symbols": symbols} if symbols else {})
                }]
            }
        }
        self._send_message(unsubscribe_data)


# Demo function
def demo():
    """Demo usage"""
    print("Testing Delta Exchange API Client...")
    client = DeltaRESTClient()
    
    # Test public endpoints
    try:
        print("\n1. Testing public endpoints...")
        products = client.get_products(page_size=5)
        logger.info("Products fetched: %d", len(products.get("result", [])))
        
        ticker = client.get_ticker("BTCUSD")
        price = ticker.get("result", {}).get("price", "N/A")
        logger.info("BTCUSD ticker: %s", price)
        
    except Exception as e:
        logger.error("Public API error: %s", e)
    
    # Test private endpoints if credentials available
    if API_KEY and API_SECRET:
        try:
            print("\n2. Testing private endpoints...")
            balance = client.get_balance()
            logger.info("Balance fetched successfully: %s", type(balance.get("result", [])))
            
            positions = client.get_positions()
            logger.info("Positions: %d", len(positions.get("result", [])))
            
        except Exception as e:
            logger.error("Private API error: %s", e)
            print("Common fixes for authentication errors:")
            print("- Verify API key and secret are correct")
            print("- Check if IP address is whitelisted")
            print("- Ensure system time is synchronized")
            print("- Verify API permissions include balance/trading access")
    else:
        print("Skipping private API tests - no credentials")
    
    # WebSocket demo
    print("\n3. Testing WebSocket connection...")
    ws = DeltaWebSocket()
    ws.connect()
    ws.subscribe_ticker(["BTCUSD", "ETHUSD"])
    ws.subscribe_candles(["BTCUSD"], "1m")
    
    try:
        print("Listening for WebSocket data for 5 seconds...")
        time.sleep(5)
    finally:
        ws.disconnect()
        print("WebSocket test completed.")


if __name__ == "__main__":
    demo()