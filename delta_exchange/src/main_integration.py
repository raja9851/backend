# delta_exchange/main_integration.py

from . import config
from .client import delta_client

# Import all functions from the two main files
# Market Data imports
from .market_data import (
    get_all_assets, get_ticker, get_orderbook,
    get_historical_candles, get_option_chain, get_trades,
    get_24hr_stats, get_product_by_symbol
)

# Account & Trading imports
from .account_trading import (
    get_open_orders, get_order_history, get_fills_history,
    get_wallet_balance, get_position, place_limit_order,
    place_market_order, place_stop_order, batch_create_orders,
    batch_cancel_orders, cancel_single_order, cancel_all_product_orders,
    set_leverage, change_position_margin, get_all_positions
)

def validate_client():
    """Client validation function."""
    try:
        if delta_client and hasattr(delta_client, 'get_assets'):
            return True
        return False
    except Exception as e:
        print(f"Client validation failed: {e}")
        return False

def get_all_market_data(product_id=None, symbol=None):
    """Sabka market data ek saath fetch karta hai."""
    if not validate_client():
        return {"error": "Client validation failed"}

    try:
        if symbol:
            product = get_product_by_symbol(symbol)
            if product and product.get('success'):
                product_id = product['result']['id']
            else:
                return {"error": f"Invalid symbol: {symbol}"}
        
        if not product_id:
            return {"error": "Product ID or symbol required"}
        
        market_data = {
            'ticker': get_ticker(symbol=symbol),
            'orderbook': get_orderbook(symbol=symbol),
            'trades': get_trades(symbol=symbol),
            'historical_candles': get_historical_candles(product_id=product_id, resolution='1m', start_time=int(time.time()) - 3600, end_time=int(time.time())),
            '24hr_stats': get_24hr_stats(),
        }
        return market_data
    except Exception as e:
        return {"error": f"Error fetching all market data: {e}"}

def manage_position(product_id, action, **kwargs):
    """Positions ko manage karta hai."""
    try:
        if action == "set_leverage":
            leverage = kwargs.get('leverage')
            if not leverage or leverage <= 0:
                return {"error": "Valid leverage required"}
            return set_leverage(product_id, leverage)

        elif action == "add_margin":
            margin_amount = kwargs.get('margin_amount')
            if not margin_amount or margin_amount <= 0:
                return {"error": "Valid margin amount required"}
            return change_position_margin(product_id, margin_amount)

        elif action == "remove_margin":
            margin_amount = kwargs.get('margin_amount')
            if not margin_amount or margin_amount <= 0:
                return {"error": "Valid margin amount required"}
            return change_position_margin(product_id, -margin_amount)  # Negative for removal

        else:
            return {"error": "Invalid action. Use: set_leverage, add_margin, remove_margin"}

    except Exception as e:
        return {"error": f"Error managing position: {e}"}

def get_complete_portfolio():
    """Complete portfolio ka data."""
    if not validate_client():
        return {"error": "Client validation failed"}

    if not config.API_KEY or not config.API_SECRET:
        return {"error": "API credentials required"}

    try:
        portfolio = {
            'balances': get_wallet_balance(),
            'positions': get_all_positions(),
            'open_orders': get_open_orders(),
            'recent_fills': get_fills_history(page_size=20) # Limit recent fills
        }
        return portfolio
    except Exception as e:
        return {"error": f"Error fetching complete portfolio: {e}"}