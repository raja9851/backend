"""
Complete Compact Upstox Client with WebSocket Events - FIXED VERSION
"""

import requests
import json
import websocket
import threading
import time
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from urllib.parse import quote

class UpstoxClient:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = "https://api.upstox.com/v3"
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, params: Optional[Dict] = None) -> Dict:
        """Make HTTP request to Upstox API"""
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.request(method, url, headers=self.headers, json=data, params=params, timeout=10)
            response.raise_for_status()
            return {"data": response.json(), "status": "success", "status_code": response.status_code}
        except Exception as e:
            return {"error": str(e), "status": "error"}

    # MARGINS
    def get_user_funds_and_margin(self, segment: Optional[str] = None) -> Dict:
        params = {"segment": segment} if segment else None
        return self._make_request("GET", "/user/get-funds-and-margin", params=params)

    # ORDERS
    def place_order(self, instrument_token: str, quantity: int, product: str, validity: str,
                   price: float, order_type: str, transaction_type: str, **kwargs) -> Dict:
        data = {
            "quantity": quantity, "product": product, "validity": validity, "price": price,
            "instrument_token": instrument_token, "order_type": order_type, "transaction_type": transaction_type,
            "disclosed_quantity": kwargs.get('disclosed_quantity', 0), "trigger_price": kwargs.get('trigger_price', 0),
            "is_amo": kwargs.get('is_amo', False), "tag": kwargs.get('tag', 'order')
        }
        return self._make_request("POST", "/order/place", data)

    def modify_order(self, order_id: str, **kwargs) -> Dict:
        data = {k: v for k, v in kwargs.items() if v is not None}
        return self._make_request("PUT", f"/order/modify/{order_id}", data)

    def cancel_order(self, order_id: str) -> Dict:
        return self._make_request("DELETE", f"/order/cancel/{order_id}")

    def get_order_book(self) -> Dict:
        return self._make_request("GET", "/order/retrieve-all")

    def get_order_details(self, order_id: str) -> Dict:
        return self._make_request("GET", f"/order/history/{order_id}")

    def get_trades(self) -> Dict:
        return self._make_request("GET", "/order/trades/get-trades-for-day")

    def get_trade_by_order(self, order_id: str) -> Dict:
        return self._make_request("GET", f"/order/trades/{order_id}")

    # GTT ORDERS
    def create_gtt(self, condition: Dict, orders: List[Dict]) -> Dict:
        data = {"condition": condition, "orders": orders}
        return self._make_request("POST", "/gtt/place-gtt-order", data)

    def modify_gtt(self, id: str, condition: Dict, orders: List[Dict]) -> Dict:
        data = {"condition": condition, "orders": orders}
        return self._make_request("PUT", f"/gtt/modify-gtt-order/{id}", data)

    def cancel_gtt(self, id: str) -> Dict:
        return self._make_request("DELETE", f"/gtt/cancel-gtt-order/{id}")

    def get_gtt_orders(self) -> Dict:
        return self._make_request("GET", "/gtt/retrieve-all-gtt-orders")

    # PORTFOLIO
    def get_holdings(self) -> Dict:
        return self._make_request("GET", "/portfolio/long-term-holdings")

    def get_positions(self) -> Dict:
        return self._make_request("GET", "/portfolio/short-term-positions")

    def convert_position(self, instrument_token: str, new_product: str, old_product: str,
                        transaction_type: str, quantity: int) -> Dict:
        data = {
            "instrument_token": instrument_token, "new_product": new_product,
            "old_product": old_product, "transaction_type": transaction_type, "quantity": quantity
        }
        return self._make_request("PUT", "/portfolio/convert-position", data)

    # HISTORICAL DATA - FIXED FOR V3 API
    def get_historical_candle_data(self, instrument_key: str, unit: str, interval: str, to_date: str, from_date: Optional[str] = None) -> Dict:
        """Get historical candle data using V3 API"""
        encoded_instrument_key = quote(instrument_key)
        if from_date:
            endpoint = f"/historical-candle/{encoded_instrument_key}/{unit}/{interval}/{to_date}/{from_date}"
        else:
            endpoint = f"/historical-candle/{encoded_instrument_key}/{unit}/{interval}/{to_date}"
        return self._make_request("GET", endpoint)

    def get_intra_day_candle_data(self, instrument_key: str, unit: str, interval: str) -> Dict:
        """Get intraday candle data using V3 API"""
        encoded_instrument_key = quote(instrument_key)
        endpoint = f"/historical-candle/intraday/{encoded_instrument_key}/{unit}/{interval}"
        return self._make_request("GET", endpoint)

    def get_option_chain_data(self, instrument_key: str, expiry_date: str) -> Dict:
        params = {"instrument_key": instrument_key, "expiry_date": expiry_date}
        return self._make_request("GET", "/option/chain", params=params)

    # MARKET QUOTES
    def get_full_market_quote(self, symbol: str) -> Dict:
        params = {"symbol": symbol}
        return self._make_request("GET", "/market-quote/quotes", params=params)

    def get_market_data_feed_authorize(self) -> Dict:
        return self._make_request("GET", "/feed/market-data-feed/authorize")

    def get_portfolio_stream_feed_authorize(self) -> Dict:
        return self._make_request("GET", "/feed/portfolio-stream-feed/authorize")

    def ltp(self, symbol: str) -> Dict:
        params = {"symbol": symbol}
        return self._make_request("GET", "/market-quote/ltp", params=params)

    def multi_ltp(self, symbols: List[str]) -> Dict:
        params = {"symbol": ",".join(symbols)}
        return self._make_request("GET", "/market-quote/ltp", params=params)

    def get_ohlc(self, symbols: List[str]) -> Dict:
        params = {"symbol": ",".join(symbols)}
        return self._make_request("GET", "/market-quote/ohlc", params=params)

    # MARKET INFORMATION
    def get_market_status(self, exchange: str) -> Dict:
        return self._make_request("GET", f"/market/status/{exchange}")

    def search_instruments(self, query: str) -> Dict:
        """Search instruments from hardcoded list since V3 API doesn't have search endpoint"""
        # Common instrument keys for major symbols
        instruments_map = {
            'NIFTY': 'NSE_INDEX|Nifty 50',
            'BANKNIFTY': 'NSE_INDEX|Nifty Bank', 
            'SENSEX': 'BSE_INDEX|SENSEX',
            'RELIANCE': 'NSE_EQ|INE002A01018',
            'TCS': 'NSE_EQ|INE467B01029',
            'INFY': 'NSE_EQ|INE009A01021',
            'HDFCBANK': 'NSE_EQ|INE040A01034',
            'ICICIBANK': 'NSE_EQ|INE090A01021',
            'ITC': 'NSE_EQ|INE154A01025',
            'SBIN': 'NSE_EQ|INE062A01020',
            'LT': 'NSE_EQ|INE018A01030',
            'BHARTIAIRTEL': 'NSE_EQ|INE397D01024',
            'AXISBANK': 'NSE_EQ|INE238A01034'
        }
        
        query_upper = query.upper()
        results = []
        
        for symbol, instrument_key in instruments_map.items():
            if query_upper in symbol:
                instrument_type = 'INDEX' if 'INDEX' in instrument_key else 'EQ'
                exchange = instrument_key.split('|')[0].split('_')[0]
                
                results.append({
                    'instrument_key': instrument_key,
                    'name': symbol,
                    'tradingsymbol': symbol,
                    'instrument_type': instrument_type,
                    'exchange': exchange
                })
        
        return {"data": results, "status": "success" if results else "error", 
                "error": "No instruments found" if not results else None}

    def get_user_profile(self) -> Dict:
        return self._make_request("GET", "/user/profile")

    def get_brokerage(self, instrument_token: str, quantity: int, product: str,
                     transaction_type: str, price: float) -> Dict:
        params = {
            "instrument_token": instrument_token, "quantity": quantity, "product": product,
            "transaction_type": transaction_type, "price": price
        }
        return self._make_request("GET", "/charges/brokerage", params=params)


class UpstoxEvents:
    """WebSocket Events Handler for Upstox"""

    def __init__(self, upstox_client: UpstoxClient):
        self.client = upstox_client
        self.market_ws: Optional[Any] = None
        self.portfolio_ws: Optional[Any] = None
        self.callbacks = {
            'market_data': [], 'order_update': [], 'position_update': [],
            'trade_update': [], 'holding_update': [], 'gtt_update': [], 'connection_status': []
        }
        self.is_running = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 3

    def add_callback(self, event_type: str, callback: Callable):
        """Add callback for events"""
        if event_type in self.callbacks:
            self.callbacks[event_type].append(callback)

    def _trigger_callbacks(self, event_type: str, data: Dict):
        """Trigger all callbacks for event type"""
        for callback in self.callbacks.get(event_type, []):
            try:
                callback(data)
            except Exception as e:
                print(f"Callback error for {event_type}: {e}")

    def _handle_reconnection(self, ws_type: str):
        """Handle WebSocket reconnection"""
        if self.reconnect_attempts < self.max_reconnect_attempts:
            self.reconnect_attempts += 1
            print(f"Attempting to reconnect {ws_type} (attempt {self.reconnect_attempts})")
            time.sleep(2 ** self.reconnect_attempts)
        else:
            print(f"Max reconnection attempts reached for {ws_type}")

    def start_market_feed(self, symbols: List[str], mode: str = "ltpc"):
        """Start market WebSocket feed"""
        self.market_symbols = symbols
        self.market_mode = mode

        def on_message(ws, message):
            try:
                data = json.loads(message)
                processed_data = {
                    "type": "market_data",
                    "timestamp": datetime.now().isoformat(),
                    "raw_data": data,
                    "symbols_count": len(symbols),
                    "mode": mode
                }
                self._trigger_callbacks('market_data', processed_data)
                self.reconnect_attempts = 0
            except Exception as e:
                print(f"Market feed message error: {e}")

        def on_error(ws, error):
            print(f"Market WebSocket error: {error}")
            self._trigger_callbacks('connection_status', {
                "type": "market", "status": "error", "error": str(error)
            })

        def on_close(ws, close_status_code, close_msg):
            print(f"Market WebSocket closed: {close_status_code}")
            self._trigger_callbacks('connection_status', {
                "type": "market", "status": "disconnected", "code": close_status_code
            })
            if self.is_running:
                self._handle_reconnection("market")

        def on_open(ws):
            print("Market WebSocket connected")
            self._trigger_callbacks('connection_status', {
                "type": "market", "status": "connected", "symbols": symbols, "mode": mode
            })
            try:
                subscribe_msg = {
                    "guid": f"market_{int(time.time())}", "method": "sub",
                    "data": {"mode": mode, "instrumentKeys": symbols}
                }
                ws.send(json.dumps(subscribe_msg))
                print(f"Subscribed to {len(symbols)} symbols in {mode} mode")
            except Exception as e:
                print(f"Subscription error: {e}")

        try:
            auth_response = self.client.get_market_data_feed_authorize()
            if auth_response.get('status') == 'success':
                ws_url = auth_response['data'].get('authorizedRedirectUri', 'wss://ws-api.upstox.com/v2/feed/market-data-feed')

                self.market_ws = websocket.WebSocketApp(
                    ws_url, on_open=on_open, on_message=on_message,
                    on_error=on_error, on_close=on_close
                )

                self.is_running = True
                market_thread = threading.Thread(target=self.market_ws.run_forever, daemon=True)
                market_thread.start()
                print(f"Market feed started for {len(symbols)} symbols")
            else:
                print(f"Market feed auth failed: {auth_response.get('error')}")
        except Exception as e:
            print(f"Market feed setup error: {e}")

    def start_portfolio_feed(self):
        """Start portfolio WebSocket feed"""
        def on_message(ws, message):
            try:
                data = json.loads(message)
                msg_type = data.get('type', '').lower()

                processed_data = {
                    "timestamp": datetime.now().isoformat(),
                    "raw_data": data, "message_type": msg_type
                }

                if 'order' in msg_type or 'execution' in msg_type:
                    processed_data["category"] = "order"
                    self._trigger_callbacks('order_update', processed_data)
                elif 'position' in msg_type:
                    processed_data["category"] = "position"
                    self._trigger_callbacks('position_update', processed_data)
                elif 'holding' in msg_type:
                    processed_data["category"] = "holding"
                    self._trigger_callbacks('holding_update', processed_data)
                elif 'gtt' in msg_type:
                    processed_data["category"] = "gtt"
                    self._trigger_callbacks('gtt_update', processed_data)
                else:
                    processed_data["category"] = "trade"
                    self._trigger_callbacks('trade_update', processed_data)

                self.reconnect_attempts = 0
            except Exception as e:
                print(f"Portfolio feed message error: {e}")

        def on_error(ws, error):
            print(f"Portfolio WebSocket error: {error}")
            self._trigger_callbacks('connection_status', {
                "type": "portfolio", "status": "error", "error": str(error)
            })

        def on_close(ws, close_status_code, close_msg):
            print(f"Portfolio WebSocket closed: {close_status_code}")
            self._trigger_callbacks('connection_status', {
                "type": "portfolio", "status": "disconnected", "code": close_status_code
            })
            if self.is_running:
                self._handle_reconnection("portfolio")

        def on_open(ws):
            print("Portfolio WebSocket connected")
            self._trigger_callbacks('connection_status', {
                "type": "portfolio", "status": "connected"
            })

        try:
            auth_response = self.client.get_portfolio_stream_feed_authorize()
            if auth_response.get('status') == 'success':
                ws_url = auth_response['data'].get('authorizedRedirectUri', 'wss://ws-api.upstox.com/v2/feed/portfolio-stream-feed')

                self.portfolio_ws = websocket.WebSocketApp(
                    ws_url, on_open=on_open, on_message=on_message,
                    on_error=on_error, on_close=on_close
                )

                portfolio_thread = threading.Thread(target=self.portfolio_ws.run_forever, daemon=True)
                portfolio_thread.start()
                print("Portfolio feed started")
            else:
                print(f"Portfolio feed auth failed: {auth_response.get('error')}")
        except Exception as e:
            print(f"Portfolio feed setup error: {e}")

    def stop_feeds(self):
        """Stop all WebSocket feeds"""
        try:
            self.is_running = False
            if self.market_ws:
                self.market_ws.close()
                print("Market feed stopped")
            if self.portfolio_ws:
                self.portfolio_ws.close()
                print("Portfolio feed stopped")
            self._trigger_callbacks('connection_status', {
                "type": "all", "status": "stopped", "message": "All feeds stopped manually"
            })
        except Exception as e:
            print(f"Stop feeds error: {e}")

    def get_feed_status(self) -> Dict:
        """Get current status of all feeds"""
        return {
            "is_running": self.is_running,
            "market_ws_connected": self.market_ws is not None,
            "portfolio_ws_connected": self.portfolio_ws is not None,
            "reconnect_attempts": self.reconnect_attempts,
            "callbacks_registered": {
                event: len(callbacks) for event, callbacks in self.callbacks.items()
            },
            "timestamp": datetime.now().isoformat()
        }