"""
Delta Exchange Live Trading System
Continuous live trading with balance monitoring and order execution
"""

import time
import json
import logging
import threading
from delta_client import DeltaRESTClient, DeltaWebSocket
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("delta.live")



class DeltaLiveTrader:
    def __init__(self):
        self.client = DeltaRESTClient()
        self.ws_client = None
        
        self.ws_connected = False
        self.current_balance = {}
        self.current_positions = []
        self.latest_price = {}
        self.running = True
        
        
        # Start background tasks
        self.start_websocket()
        self.start_balance_monitor()

    def start_websocket(self):
        """Start continuous WebSocket connection"""
        print("Starting live WebSocket connection...")
        
        class LiveWebSocket(DeltaWebSocket):
            def __init__(self, parent):
                super().__init__()
                self.parent = parent
                
            def _on_open(self, ws):
                print("WebSocket connected - Live data streaming")
                self.parent.ws_connected = True
                super()._on_open(ws)
                
            def _on_message(self, ws, message):
                try:
                    data = json.loads(message)
                    msg_type = data.get("type", "")
                    
                    if msg_type == "heartbeat":
                        return
                    elif msg_type == "v2/ticker":
                        symbol = data.get("symbol")
                        price = data.get("price") or data.get("mark_price") or data.get("close")
                        if symbol and price:
                            old_price = self.parent.latest_price.get(symbol)
                            if old_price != price:
                                self.parent.latest_price[symbol] = price
                                direction = "🔴" if old_price and float(price) < float(old_price) else "🟢"
                                print(f"{direction} {symbol}: {price}")
                    elif msg_type == "subscriptions":
                        channels = data.get("channels", [])
                        print(f"Subscribed to {len(channels)} channels")
                        
                except json.JSONDecodeError:
                    pass
                except Exception as e:
                    logger.error(f"WebSocket message error: {e}")
                    
            def _on_error(self, ws, error):
                if "Connection timed out" not in str(error):
                    print(f"WebSocket error: {error}")
                self.parent.ws_connected = False
                
            def _on_close(self, ws, code, reason):
                print("WebSocket disconnected - attempting reconnect...")
                self.parent.ws_connected = False
                if self.parent.running:
                    time.sleep(5)
                    self.parent.reconnect_websocket()
        
        self.ws_client = LiveWebSocket(self)
        self.ws_client.connect()
        time.sleep(2)
        
        # Subscribe to major pairs
        symbols = ["BTCUSD", "ETHUSD", "SOLUSD"]
        if self.ws_connected:
            print(f"Subscribing to {', '.join(symbols)}")
            self.ws_client.subscribe_ticker(symbols)

    def reconnect_websocket(self):
        """Reconnect WebSocket if disconnected"""
        if not self.running:
            return
            
        try:
            self.start_websocket()
        except Exception as e:
            print(f"Reconnection failed: {e}")

    def start_balance_monitor(self):
        """Start background balance monitoring"""
        def monitor_balance():
            while self.running:
                try:
                    self.update_balance()
                    self.update_positions()
                    time.sleep(30)  # Update every 30 seconds
                except Exception as e:
                    logger.error(f"Balance monitor error: {e}")
                    time.sleep(60)  # Wait longer on error
        
        balance_thread = threading.Thread(target=monitor_balance, daemon=True)
        balance_thread.start()

    def update_balance(self):
        """Update current balance"""
        try:
            balance_response = self.client.get_balance()
            if balance_response and 'result' in balance_response:
                self.current_balance = {}
                for asset in balance_response['result']:
                    symbol = asset.get('asset', {}).get('symbol', 'Unknown')
                    balance = float(asset.get('balance', 0))
                    if balance > 0:
                        self.current_balance[symbol] = balance
        except Exception as e:
            logger.error(f"Balance update failed: {e}")

    def update_positions(self):
        """Update current positions"""
        try:
            positions_response = self.client._request("GET", "/v2/positions/margined", signed=True)
            if positions_response and 'result' in positions_response:
                self.current_positions = []
                for pos in positions_response['result']:
                    if float(pos.get('size', 0)) != 0:
                        self.current_positions.append({
                            'symbol': pos.get('product_symbol'),
                            'size': pos.get('size'),
                            'side': pos.get('side'),
                            'entry_price': pos.get('entry_price'),
                            'unrealized_pnl': pos.get('unrealized_pnl')
                        })
        except Exception as e:
            logger.error(f"Positions update failed: {e}")

    def show_status(self):
        """Display current status"""
        print("\n" + "="*60)
        print(f"📊 LIVE TRADING STATUS - {time.strftime('%H:%M:%S')}")
        print("="*60)
        
        # Balance
        print("💰 BALANCE:")
        if self.current_balance:
            for symbol, balance in self.current_balance.items():
                print(f"   {symbol}: {balance}")
        else:
            print("   No balance data or zero balance")
        
        # Positions
        print("\n📈 POSITIONS:")
        if self.current_positions:
            for pos in self.current_positions:
                pnl = pos['unrealized_pnl']
                pnl_color = "🟢" if float(pnl) >= 0 else "🔴"
                print(f"   {pos['symbol']}: {pos['side']} {pos['size']} @ {pos['entry_price']} {pnl_color}PnL: {pnl}")
        else:
            print("   No open positions")
        
        # Latest Prices
        print("\n💹 LATEST PRICES:")
        for symbol, price in self.latest_price.items():
            print(f"   {symbol}: {price}")
        
        print("="*60)

    def place_buy_order(self, symbol, size, price=None):
        """Place buy order"""
        try:
            # Get product info
            product_response = self.client.get_product(symbol)
            if not product_response or 'result' not in product_response:
                print(f"❌ Symbol {symbol} not found")
                return None
                
            product_id = product_response['result']['id']
            
            if price:
                # Limit order
                result = self.client.place_order(
                    product_id=product_id,
                    side="buy",
                    size=size,
                    order_type="limit_order",
                    limit_price=str(price)
                )
            else:
                # Market order
                result = self.client.place_order(
                    product_id=product_id,
                    side="buy",
                    size=size,
                    order_type="market_order"
                )
            
            if result:
                order_id = result.get('result', {}).get('id', 'Unknown')
                print(f"✅ BUY order placed: {symbol} x{size} {'@ '+str(price) if price else '(market)'} | ID: {order_id}")
                return result
            
        except Exception as e:
            print(f"❌ Buy order failed: {e}")
            return None

    def place_sell_order(self, symbol, size, price=None):
        """Place sell order"""
        try:
            # Get product info
            product_response = self.client.get_product(symbol)
            if not product_response or 'result' not in product_response:
                print(f"❌ Symbol {symbol} not found")
                return None
                
            product_id = product_response['result']['id']
            
            if price:
                # Limit order
                result = self.client.place_order(
                    product_id=product_id,
                    side="sell",
                    size=size,
                    order_type="limit_order",
                    limit_price=str(price)
                )
            else:
                # Market order
                result = self.client.place_order(
                    product_id=product_id,
                    side="sell",
                    size=size,
                    order_type="market_order"
                )
            
            if result:
                order_id = result.get('result', {}).get('id', 'Unknown')
                print(f"✅ SELL order placed: {symbol} x{size} {'@ '+str(price) if price else '(market)'} | ID: {order_id}")
                return result
            
        except Exception as e:
            print(f"❌ Sell order failed: {e}")
            return None

    def cancel_order(self, order_id):
        """Cancel specific order"""
        try:
            result = self.client.cancel_order(order_id)
            if result:
                print(f"✅ Order cancelled: {order_id}")
                return result
        except Exception as e:
            print(f"❌ Cancel order failed: {e}")
            return None

    def cancel_all_orders(self):
        """Cancel all orders"""
        try:
            result = self.client.cancel_all_orders()
            if result:
                print("✅ All orders cancelled")
                return result
        except Exception as e:
            print(f"❌ Cancel all orders failed: {e}")
            return None

    def show_help(self):
        """Show available commands"""
        print("\n📋 AVAILABLE COMMANDS:")
        print("status          - Show balance, positions, prices")
        print("buy SYMBOL SIZE [PRICE] - Place buy order (market if no price)")
        print("sell SYMBOL SIZE [PRICE] - Place sell order (market if no price)")
        print("cancel ORDER_ID - Cancel specific order")
        print("cancel_all      - Cancel all orders")
        print("help            - Show this help")
        print("quit            - Exit system")
        print("\nExamples:")
        print("buy BTCUSD 1           - Buy 1 BTCUSD at market price")
        print("sell ETHUSD 2 100.50   - Sell 2 ETHUSD at limit price 100.50")

    def run(self):
        """Main interactive loop"""
        print("\n🚀 DELTA LIVE TRADING SYSTEM STARTED")
        print("Type 'help' for commands or 'quit' to exit")
        
        # Initial status
        time.sleep(3)  # Wait for initial data
        self.show_status()
        self.show_help()
        
        while self.running:
            try:
                command = input("\n> ").strip().lower()
                
                if not command:
                    continue
                    
                parts = command.split()
                cmd = parts[0]
                
                if cmd == "quit" or cmd == "exit":
                    print("Shutting down...")
                    self.running = False
                    break
                    
                elif cmd == "status":
                    self.show_status()
                    
                elif cmd == "help":
                    self.show_help()
                    
                elif cmd == "buy" and len(parts) >= 3:
                    symbol = parts[1].upper()
                    try:
                        size = int(parts[2])
                        price = float(parts[3]) if len(parts) > 3 else None
                        self.place_buy_order(symbol, size, price)
                    except ValueError:
                        print("❌ Invalid size or price format")
                        
                elif cmd == "sell" and len(parts) >= 3:
                    symbol = parts[1].upper()
                    try:
                        size = int(parts[2])
                        price = float(parts[3]) if len(parts) > 3 else None
                        self.place_sell_order(symbol, size, price)
                    except ValueError:
                        print("❌ Invalid size or price format")
                        
                elif cmd == "cancel" and len(parts) >= 2:
                    if parts[1] == "all":
                        self.cancel_all_orders()
                    else:
                        self.cancel_order(parts[1])
                        
                elif cmd == "cancel_all":
                    self.cancel_all_orders()
                    
                else:
                    print("❌ Unknown command. Type 'help' for available commands.")
                    
            except KeyboardInterrupt:
                print("\nShutting down...")
                self.running = False
                break
            except Exception as e:
                print(f"❌ Error: {e}")
        
        # Cleanup
        if self.ws_client:
            try:
                self.ws_client.disconnect()
            except:
                pass
        print("System stopped.")

# Main execution
if __name__ == "__main__":
    # Check for dry run mode warning
    if os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes"):
        print("⚠️  WARNING: DRY_RUN mode is enabled. No real orders will be placed!")
        print("Set DRY_RUN=false in your .env file for live trading.")
        input("Press Enter to continue or Ctrl+C to exit...")
    
    trader = DeltaLiveTrader()
    trader.run()