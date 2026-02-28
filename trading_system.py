# trading_system.py - Complete Upstox Trading System
import requests
import json
import time
import logging
import threading
from typing import Dict, List, Optional, Union
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime
import asyncio

# Import auth module and mongo_service
try:
    from auth import auth
    ACCESS_TOKEN = auth.access_token
except ImportError:
    ACCESS_TOKEN = "fallback_token"

try:
    from mongo_service import save_script_to_db, load_script_from_db, delete_script_from_db, get_all_scripts # Assuming these are available
except ImportError:
    logging.warning("MongoDB service not found. Script persistence functions will not work.")
    save_script_to_db = lambda script: None
    load_script_from_db = lambda script_id: None
    delete_script_from_db = lambda script_id: None
    get_all_scripts = lambda: []


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"
    SL_M = "SL-M"

class TransactionType(Enum):
    BUY = "BUY"
    SELL = "SELL"

@dataclass
class TradeConfig:
    symbol: str
    quantity: int
    buy_price: float
    sell_price: float
    stop_loss: float
    auto_enabled: bool = False
    position_size: int = 1

@dataclass
class Position:
    symbol: str
    quantity: int
    avg_price: float
    pnl: float
    order_ids: List[str]

class UpstoxTradingSystem:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = "https://api.upstox.com"
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Trading state
        self.positions = {}
        self.active_orders = {}
        self.auto_trades = {}
        self.auto_scripts = {}  # For custom trading scripts
        self.market_data = {}
        
        # WebSocket reference (will be set from main server)
        self.sio = None
        self._loop = None  # For asyncio event loop
        
    def set_socketio(self, sio):
        """Set SocketIO instance for real-time updates"""
        self.sio = sio
        try:
            import asyncio
            self._loop = asyncio.get_event_loop()
        except Exception:
            self._loop = None
        
    def _make_request(self, method: str, endpoint: str, data: Optional[dict] = None) -> dict:
        """Make HTTP request to Upstox API"""
        url = f"{self.base_url}{endpoint}"
        
        response = None
        try:
            if method.upper() == 'GET':
                response = self.session.get(url, params=data)
            elif method.upper() == 'POST':
                response = self.session.post(url, json=data)
            elif method.upper() == 'PUT':
                response = self.session.put(url, json=data)
            elif method.upper() == 'DELETE':
                response = self.session.delete(url, json=data)
            
            if response:
                response.raise_for_status()
                return response.json()
            return {"status": "error", "message": "No response received"}
        
        except Exception as e:
            logger.error(f"API request failed: {e}")
            return {"status": "error", "message": str(e)}
    
    # ORDER MANAGEMENT METHODS
    def place_order(self, symbol: str, quantity: int, price: float, order_type: str, transaction_type: str):
        """Place order on Upstox"""
        endpoint = "/v2/order/place"
        
        payload = {
            "quantity": quantity,
            "product": "I",  # Intraday
            "validity": "DAY",
            "price": price if order_type != "MARKET" else 0,
            "instrument_token": symbol,
            "order_type": order_type,
            "transaction_type": transaction_type,
            "disclosed_quantity": 0,
            "trigger_price": 0,
            "is_amo": False
        }
        
        result = self._make_request('POST', endpoint, payload)
        
        if result.get('status') == 'success':
            order_id = result.get('data', {}).get('order_id')
            if order_id:
                self.active_orders[order_id] = {
                    'symbol': symbol,
                    'quantity': quantity,
                    'price': price,
                    'type': transaction_type,
                    'status': 'PENDING',
                    'timestamp': time.time()
                }
                # Emit real-time update
                if self.sio and self._loop:
                    import asyncio
                    asyncio.run_coroutine_threadsafe(
                        self.sio.emit('trade_update', {
                            'id': order_id,
                            'symbol': symbol,
                            'type': transaction_type,
                            'quantity': quantity,
                            'price': price,
                            'timestamp': time.time(),
                            'status': 'PLACED'
                        }),
                        self._loop
                    )
        
        return result
    
    def modify_order(self, order_id: str, quantity: Union[int, None] = None, price: Union[float, None] = None, order_type: Union[str, None] = None):
        """Modify existing order"""
        endpoint = "/v2/order/modify"
        
        payload = {"order_id": order_id}
        
        if quantity is not None:
            payload["quantity"] = str(quantity)
        if price is not None:
            payload["price"] = str(price)
        if order_type is not None:
            payload["order_type"] = order_type
            
        result = self._make_request('PUT', endpoint, payload)
        
        # Update local order record
        if order_id in self.active_orders and result.get('status') == 'success':
            if quantity is not None:
                self.active_orders[order_id]['quantity'] = quantity
            if price is not None:
                self.active_orders[order_id]['price'] = price
                
            # Emit update
            if self.sio and self._loop:
                import asyncio
                asyncio.run_coroutine_threadsafe(
                    self.sio.emit('order_modified', {
                        'order_id': order_id,
                        'status': 'MODIFIED',
                        'timestamp': time.time()
                    }),
                    self._loop
                )
        
        return result
    
    def cancel_order(self, order_id: str):
        """Cancel order - V3"""
        endpoint = "/v2/order/cancel"
        payload = {"order_id": order_id}
        result = self._make_request('DELETE', endpoint, payload)
        
        # Update local state
        if order_id in self.active_orders and result.get('status') == 'success':
            self.active_orders[order_id]['status'] = 'CANCELLED'
            
            # Emit update
            if self.sio and self._loop:
                import asyncio
                asyncio.run_coroutine_threadsafe(
                    self.sio.emit('order_cancelled', {
                        'order_id': order_id,
                        'status': 'CANCELLED',
                        'timestamp': time.time()
                    }),
                    self._loop
                )
        
        return result
    
    def cancel_multi_order(self, order_ids: List[str]):
        """Cancel multiple orders - V3"""
        endpoint = "/v2/order/multi/cancel"
        payload = {"order_ids": order_ids}
        result = self._make_request('DELETE', endpoint, payload)
        
        # Update local state
        if result.get('status') == 'success':
            for order_id in order_ids:
                if order_id in self.active_orders:
                    self.active_orders[order_id]['status'] = 'CANCELLED'
        
        return result
    
    def place_multi_order(self, orders: List[dict]):
        """Place multiple orders - V3"""
        endpoint = "/v2/order/multi/place"
        
        orders_payload = []
        for order in orders:
            order_data = {
                "quantity": order['quantity'],
                "product": order.get('product', 'I'),
                "validity": order.get('validity', 'DAY'),
                "price": order['price'] if order.get('order_type') != 'MARKET' else 0,
                "instrument_token": order['symbol'],
                "order_type": order['order_type'],
                "transaction_type": order['transaction_type'],
                "disclosed_quantity": order.get('disclosed_quantity', 0),
                "trigger_price": order.get('trigger_price', 0),
                "is_amo": order.get('is_amo', False)
            }
            orders_payload.append(order_data)
        
        payload = {"orders": orders_payload}
        result = self._make_request('POST', endpoint, payload)
        
        # Update local state
        if result.get('status') == 'success':
            order_results = result.get('data', [])
            for i, order_result in enumerate(order_results):
                if order_result.get('status') == 'success' and i < len(orders):
                    order_id = order_result.get('order_id')
                    if order_id:
                        self.active_orders[order_id] = {
                            'symbol': orders[i]['symbol'],
                            'quantity': orders[i]['quantity'],
                            'price': orders[i]['price'],
                            'type': orders[i]['transaction_type'],
                            'status': 'PENDING',
                            'timestamp': time.time()
                        }
        
        return result
    
    def exit_all_positions(self):
        """Exit all open positions"""
        try:
            positions_result = self.get_positions()
            if positions_result.get('status') != 'success':
                return {'status': 'error', 'message': 'Failed to get positions'}
            
            positions = positions_result.get('data', [])
            exit_orders = []
            
            for position in positions:
                quantity = position.get('quantity', 0)
                if quantity > 0:  # Long position - sell to exit
                    exit_orders.append({
                        'symbol': position.get('instrument_token'),
                        'quantity': abs(quantity),
                        'price': 0,
                        'order_type': 'MARKET',
                        'transaction_type': 'SELL',
                        'product': position.get('product', 'I')
                    })
                elif quantity < 0:  # Short position - buy to exit
                    exit_orders.append({
                        'symbol': position.get('instrument_token'),
                        'quantity': abs(quantity),
                        'price': 0,
                        'order_type': 'MARKET',
                        'transaction_type': 'BUY',
                        'product': position.get('product', 'I')
                    })
            
            if exit_orders:
                return self.place_multi_order(exit_orders)
            else:
                return {'status': 'success', 'message': 'No positions to exit'}
                
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def get_orders(self):
        """Get all orders"""
        endpoint = "/v2/order/retrieve-all"
        return self._make_request('GET', endpoint)
    
    def get_order_details(self, order_id: str):
        """Get specific order details - V3"""
        endpoint = "/v2/order/details"
        return self._make_request('GET', endpoint, {"order_id": order_id})
    
    def get_order_history(self, order_id: str):
        """Get order history for specific order"""
        endpoint = "/v2/order/history"
        return self._make_request('GET', endpoint, {"order_id": order_id})
    
    def get_order_book(self):
        """Get order book - all orders for the day"""
        return self.get_orders()
    
    def get_trades(self):
        """Get all trades for the day"""
        endpoint = "/v2/order/trades/get-trades-for-day"
        return self._make_request('GET', endpoint)
    
    def get_order_trades(self, order_id: str):
        """Get trades for specific order"""
        endpoint = "/v2/order/trades"
        return self._make_request('GET', endpoint, {"order_id": order_id})
    
    def get_trade_history(self, page_number: int = 1, page_size: int = 50):
        """Get historical trades"""
        endpoint = "/v2/charges/historical-trades"
        params = {
            "page_number": page_number,
            "page_size": page_size
        }
        return self._make_request('GET', endpoint, params)
    
    def get_positions(self):
        """Get current positions"""
        endpoint = "/v2/portfolio/short-term-positions"
        return self._make_request('GET', endpoint)
    
    def get_holdings(self):
        """Get holdings"""
        endpoint = "/v2/portfolio/long-term-holdings"
        return self._make_request('GET', endpoint)
    
    def get_funds(self):
        """Get fund details"""
        endpoint = "/v2/user/get-funds-and-margin"
        return self._make_request('GET', endpoint)
    
    # HELPER FUNCTIONS FOR SCRIPT EXECUTION
    def place_buy_order(self, symbol: str, quantity: int, price: float = 0):
        """Helper function for script execution - Buy order"""
        order_type = "MARKET" if price == 0 else "LIMIT"
        result = self.place_order(symbol, quantity, price, order_type, "BUY")
        
        # Emit trade update
        if self.sio and self._loop:
            import asyncio
            asyncio.run_coroutine_threadsafe(
                self.sio.emit('trade_update', {
                    'id': str(time.time()),
                    'symbol': symbol,
                    'type': 'BUY',
                    'quantity': quantity,
                    'price': price,
                    'timestamp': time.time(),
                    'status': 'EXECUTED' if result.get('status') == 'success' else 'FAILED'
                }),
                self._loop
            )
        
        return result
    
    def place_sell_order(self, symbol: str, quantity: int, price: float = 0):
        """Helper function for script execution - Sell order"""
        order_type = "MARKET" if price == 0 else "LIMIT"
        result = self.place_order(symbol, quantity, price, order_type, "SELL")
        
        # Emit trade update
        if self.sio and self._loop:
            import asyncio
            asyncio.run_coroutine_threadsafe(
                self.sio.emit('trade_update', {
                    'id': str(time.time()),
                    'symbol': symbol,
                    'type': 'SELL',
                    'quantity': quantity,
                    'price': price,
                    'timestamp': time.time(),
                    'status': 'EXECUTED' if result.get('status') == 'success' else 'FAILED'
                }),
                self._loop
            )
        
        return result
    
    def get_position_for_symbol(self, symbol: str):
        """Get position for specific symbol"""
        positions = self.get_positions()
        if positions.get('status') == 'success':
            for pos in positions.get('data', []):
                if pos.get('instrument_token') == symbol:
                    return {
                        'quantity': pos.get('quantity', 0),
                        'avg_price': pos.get('average_price', 0),
                        'pnl': pos.get('unrealised_pnl', 0),
                        'market_value': pos.get('market_value', 0)
                    }
        return {'quantity': 0, 'avg_price': 0, 'pnl': 0, 'market_value': 0}
    
    # SCRIPT MANAGEMENT METHODS (NEW)
    def save_script(self, script_data: dict):
        """Save a trading script to the database."""
        try:
            save_script_to_db(script_data)
            return 'success'
        except Exception as e:
            logger.error(f"Error saving script: {e}")
            return f'error: {str(e)}'

    def load_script(self, script_id: str):
        """Load a specific script from the database."""
        try:
            script = load_script_from_db(script_id)
            if script:
                return script
            else:
                return None
        except Exception as e:
            logger.error(f"Error loading script: {e}")
            return None

    def get_all_scripts(self):
        """Get all saved scripts from the database."""
        try:
            scripts_list = get_all_scripts()
            return scripts_list
        except Exception as e:
            logger.error(f"Error getting all scripts: {e}")
            return []

    def delete_script(self, script_id: str):
        """Delete a script from the database."""
        try:
            delete_script_from_db(script_id)
            return 'success'
        except Exception as e:
            logger.error(f"Error deleting script: {e}")
            return f'error: {str(e)}'

    # SCRIPT EXECUTION METHODS (Original)
    def execute_script(self, code: str, script_name: str = "Untitled"):
        """Execute trading script immediately"""
        try:
            # Create safe execution environment
            exec_globals = {
                'buy': lambda symbol, quantity, price=0: self.place_buy_order(symbol, quantity, price),
                'sell': lambda symbol, quantity, price=0: self.place_sell_order(symbol, quantity, price),
                'get_price': lambda symbol: self.market_data.get(symbol, {}).get('ltp', 0),
                'get_position': lambda symbol: self.get_position_for_symbol(symbol),
                'log': lambda message: self._emit_script_log(message),
                'time': time,
                'datetime': datetime
            }
            
            # Execute the script
            exec(code, exec_globals)
            
            return {'status': 'success', 'message': 'Script executed successfully'}
            
        except Exception as e:
            error_msg = f"Script execution error: {str(e)}"
            logger.error(error_msg)
            return {'status': 'error', 'error': error_msg}
    
    def start_auto_script(self, script_id: str, code: str, name: str):
        """Start auto trading script"""
        # Store script for continuous execution
        self.auto_scripts[script_id] = {
            'code': code,
            'name': name,
            'active': True,
            'last_execution': 0,
            'execution_count': 0
        }
        
        # Start monitoring thread for this script
        self.start_script_monitoring(script_id)
        
        if self.sio:
            asyncio.create_task(self.sio.emit('auto_trade_status', {
                'script_id': script_id, 
                'status': 'Started',
                'name': name
            }))
        
        return {'status': 'success', 'message': f'Auto script {name} started'}
    
    def stop_auto_script(self, script_id: str):
        """Stop auto trading script"""
        if script_id in self.auto_scripts:
            self.auto_scripts[script_id]['active'] = False
            
            if self.sio:
                asyncio.create_task(self.sio.emit('auto_trade_status', {
                    'script_id': script_id, 
                    'status': 'Stopped'
                }))
            
            return {'status': 'success', 'message': 'Auto script stopped'}
        
        return {'status': 'error', 'message': 'Script not found'}
    
    def start_script_monitoring(self, script_id: str):
        """Monitor and execute script continuously"""
        def monitor():
            while (script_id in self.auto_scripts and 
                   self.auto_scripts[script_id]['active']):
                try:
                    script_data = self.auto_scripts[script_id]
                    code = script_data['code']
                    
                    # Execute script every 5 seconds
                    if time.time() - script_data['last_execution'] > 5:
                        exec_globals = {
                            'buy': lambda symbol, quantity, price=0: self.place_buy_order(symbol, quantity, price),
                            'sell': lambda symbol, quantity, price=0: self.place_sell_order(symbol, quantity, price),
                            'get_price': lambda symbol: self.market_data.get(symbol, {}).get('ltp', 0),
                            'get_position': lambda symbol: self.get_position_for_symbol(symbol),
                            'log': lambda message: self._emit_script_log(message),
                            'time': time,
                            'datetime': datetime
                        }
                        
                        exec(code, exec_globals)
                        script_data['last_execution'] = time.time()
                        script_data['execution_count'] += 1
                        
                    time.sleep(1)
                    
                except Exception as e:
                    error_msg = f"Auto script error: {str(e)}"
                    logger.error(error_msg)
                    if self.sio and self._loop:
                        asyncio.run_coroutine_threadsafe(
                            self.sio.emit('script_execution_result', {
                                'status': 'error', 
                                'error': error_msg
                            }),
                            self._loop
                        )
                    time.sleep(5)
        
        thread = threading.Thread(target=monitor)
        thread.daemon = True
        thread.start()
    
    def _emit_script_log(self, message: str):
        """Emit script log message"""
        if self.sio and self._loop:
            import asyncio
            asyncio.run_coroutine_threadsafe(
                self.sio.emit('script_log', {
                    'message': message,
                    'timestamp': datetime.now().isoformat()
                }),
                self._loop
            )
    
    def get_auto_scripts_status(self):
        """Get status of all auto scripts"""
        return {
            script_id: {
                'name': data['name'],
                'active': data['active'],
                'execution_count': data['execution_count'],
                'last_execution': data['last_execution']
            }
            for script_id, data in self.auto_scripts.items()
        }
    
    # AUTO TRADING LOGIC (Original functionality)
    def add_auto_trade(self, config: TradeConfig):
        """Add auto trading configuration"""
        self.auto_trades[config.symbol] = {
            'config': config,
            'status': 'WAITING',
            'position': None,
            'buy_order_id': None,
            'sell_order_id': None
        }
        
        if config.auto_enabled:
            self.start_auto_monitoring(config.symbol)
    
    def start_auto_monitoring(self, symbol: str):
        """Start monitoring symbol for auto trade"""
        def monitor():
            while symbol in self.auto_trades and self.auto_trades[symbol]['config'].auto_enabled:
                try:
                    self.check_auto_trade_conditions(symbol)
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Auto trade error for {symbol}: {e}")
                    time.sleep(5)
        thread = threading.Thread(target=monitor)
        thread.daemon = True
        thread.start()
    
    def check_auto_trade_conditions(self, symbol: str):
        """Check if auto trade conditions are met"""
        if symbol not in self.auto_trades:
            return
        
        trade_info = self.auto_trades[symbol]
        config = trade_info['config']
        current_price = self.market_data.get(symbol, {}).get('ltp', 0)
        
        if not current_price:
            return
        
        # Entry logic
        if trade_info['status'] == 'WAITING' and current_price <= config.buy_price:
            result = self.place_buy_order(symbol, config.quantity)
            
            if result.get('status') == 'success':
                trade_info['status'] = 'BOUGHT'
                trade_info['buy_order_id'] = result.get('data', {}).get('order_id')
                logger.info(f"Auto buy executed for {symbol} at {current_price}")
        
        # Exit logic
        elif trade_info['status'] == 'BOUGHT':
            should_exit = (
                current_price >= config.sell_price or
                current_price <= config.stop_loss
            )
            
            if should_exit:
                result = self.place_sell_order(symbol, config.quantity)
                
                if result.get('status') == 'success':
                    trade_info['status'] = 'SOLD'
                    trade_info['sell_order_id'] = result.get('data', {}).get('order_id')
                    reason = "TARGET" if current_price >= config.sell_price else "STOP_LOSS"
                    logger.info(f"Auto sell executed for {symbol} at {current_price} - {reason}")
    
    def stop_auto_trade(self, symbol: str):
        """Stop auto trading for symbol"""
        if symbol in self.auto_trades:
            self.auto_trades[symbol]['config'].auto_enabled = False
            
    def update_market_data(self, market_data: dict):
        """Update market data from WebSocket feed"""
        self.market_data.update(market_data)

# Create global trading system instance
trading_system = UpstoxTradingSystem(ACCESS_TOKEN)