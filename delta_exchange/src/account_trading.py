# delta_exchange/account_trading.py

from .client import delta_client
from .mongo_db import save_to_mongodb
from .market_data import make_authenticated_request
import logging
import time

logger = logging.getLogger(__name__)

# Account Data Functions
def get_open_orders(product_ids=None, states='open,pending'):
    """Aapke saare open orders ki list deta hai."""
    logger.info("Fetching open orders")
    try:
        path = "/v2/orders"
        params = {}
        if product_ids:
            # Ensure product_ids is a comma-separated string if it's a list
            params['product_ids'] = ','.join(map(str, product_ids)) if isinstance(product_ids, list) else product_ids
        
        # FIX: Simplified the logic for handling states
        if states:
            params['states'] = states

        orders = make_authenticated_request('GET', path, params=params)
        if orders and not orders.get('error'):
            save_to_mongodb('open_orders', {
                'data': orders,
                'timestamp': time.time(),
                'type': 'open_orders',
                'product_ids': product_ids,
                'states': states
            })
        return orders
    except Exception as e:
        logger.error(f"Error fetching open orders: {e}")
        return {"error": f"Error fetching open orders: {e}"}

def get_order_history(product_ids=None, states='filled,cancelled,rejected', page_size=50):
    """Historical order data."""
    logger.info("Fetching order history")
    try:
        path = "/v2/orders/history"
        params = {
            'page_size': page_size,
            'states': states
        }
        if product_ids:
            params['product_ids'] = ','.join(map(str, product_ids)) if isinstance(product_ids, list) else product_ids
            
        history = make_authenticated_request('GET', path, params=params)
        
        if history and not history.get('error'):
            save_to_mongodb('order_history', {
                'data': history,
                'timestamp': time.time(),
                'type': 'order_history'
            })
        return history
    except Exception as e:
        logger.error(f"Error fetching order history: {e}")
        return {"error": f"Error fetching order history: {e}"}

def get_fills_history(product_ids=None, page_size=50):
    """Filled orders ka history deta hai."""
    logger.info("Fetching fills history")
    try:
        path = "/v2/fills"
        params = {'page_size': page_size}
        if product_ids:
            params['product_ids'] = ','.join(map(str, product_ids)) if isinstance(product_ids, list) else product_ids
            
        fills = make_authenticated_request('GET', path, params=params)
        
        if fills and not fills.get('error'):
            save_to_mongodb('fills_history', {
                'data': fills,
                'timestamp': time.time(),
                'type': 'fills_history'
            })
        return fills
    except Exception as e:
        logger.error(f"Error fetching fills history: {e}")
        return {"error": f"Error fetching fills history: {e}"}

def get_wallet_balance():
    """Wallet ka balance deta hai."""
    logger.info("Fetching wallet balance")
    try:
        response = make_authenticated_request('GET', '/v2/wallet/balances')
        
        if response and not response.get('error'):
            save_to_mongodb('wallet_balances', {
                'data': response,
                'timestamp': time.time(),
                'type': 'wallet_balances'
            })
        return response
    except Exception as e:
        logger.error(f"Error fetching wallet balance: {e}")
        return {"error": f"Error fetching wallet balance: {e}"}

def get_position(product_id):
    """Individual position ka data."""
    logger.info(f"Fetching position for product {product_id}")
    try:
        path = f"/v2/positions"
        params = {'product_id': product_id}
        response = make_authenticated_request('GET', path, params=params)
        
        if response and not response.get('error'):
            save_to_mongodb('position', {
                'data': response,
                'timestamp': time.time(),
                'type': 'single_position',
                'product_id': product_id
            })
        return response
    except Exception as e:
        logger.error(f"Error fetching position: {e}")
        return {"error": f"Error fetching position: {e}"}

def get_all_positions():
    """Saari open positions ki list deta hai."""
    logger.info("Fetching all open positions")
    try:
        response = make_authenticated_request('GET', '/v2/positions')
        
        if response and not response.get('error'):
            save_to_mongodb('positions', {
                'data': response,
                'timestamp': time.time(),
                'type': 'all_positions'
            })
        return response
    except Exception as e:
        logger.error(f"Error fetching all positions: {e}")
        return {"error": f"Error fetching all positions: {e}"}

def place_limit_order(product_id, side, size, price, client_order_id=None):
    """Limit order lagata hai."""
    logger.info(f"Placing limit order for product {product_id}")
    try:
        order_data = {
            'product_id': product_id,
            'side': side,
            'size': size,
            'price': price,
            'order_type': 'limit'
        }
        if client_order_id:
            order_data['client_order_id'] = client_order_id
            
        path = "/v2/orders"
        response = make_authenticated_request('POST', path, data=order_data)
        
        if response and not response.get('error'):
            save_to_mongodb('orders', {'data': response, 'type': 'limit_order_placed'})
            logger.info("Limit order placed successfully")
        
        return response
    except Exception as e:
        logger.error(f"Error placing limit order: {e}")
        return {"error": f"Error placing limit order: {e}"}

def place_market_order(product_id, side, size, client_order_id=None):
    """Market order lagata hai."""
    logger.info(f"Placing market order for product {product_id}")
    try:
        order_data = {
            'product_id': product_id,
            'side': side,
            'size': size,
            'order_type': 'market'
        }
        if client_order_id:
            order_data['client_order_id'] = client_order_id
            
        path = "/v2/orders"
        response = make_authenticated_request('POST', path, data=order_data)
        
        if response and not response.get('error'):
            save_to_mongodb('orders', {'data': response, 'type': 'market_order_placed'})
            logger.info("Market order placed successfully")
        
        return response
    except Exception as e:
        logger.error(f"Error placing market order: {e}")
        return {"error": f"Error placing market order: {e}"}

def place_stop_order(product_id, side, size, stop_price, client_order_id=None):
    """Stop order lagata hai."""
    logger.info(f"Placing stop order for product {product_id}")
    try:
        order_data = {
            'product_id': product_id,
            'side': side,
            'size': size,
            'stop_price': stop_price,
            'order_type': 'stop_limit'
        }
        if client_order_id:
            order_data['client_order_id'] = client_order_id
            
        path = "/v2/orders"
        response = make_authenticated_request('POST', path, data=order_data)
        
        if response and not response.get('error'):
            save_to_mongodb('orders', {'data': response, 'type': 'stop_order_placed'})
            logger.info("Stop order placed successfully")
        
        return response
    except Exception as e:
        logger.error(f"Error placing stop order: {e}")
        return {"error": f"Error placing stop order: {e}"}

def batch_create_orders(orders):
    """Batch me orders lagata hai."""
    logger.info(f"Placing {len(orders)} orders in a batch")
    try:
        path = "/v2/orders/batch"
        data = {'orders': orders}
        response = make_authenticated_request('POST', path, data=data)
        
        if response and not response.get('error'):
            save_to_mongodb('orders', {'data': response, 'type': 'batch_orders_placed'})
            logger.info(f"{len(orders)} orders placed successfully in batch")
        
        return response
    except Exception as e:
        logger.error(f"Error placing batch orders: {e}")
        return {"error": f"Error placing batch orders: {e}"}

def cancel_single_order(product_id, order_id):
    """Ek single order cancel karta hai."""
    logger.info(f"Cancelling order {order_id} for product {product_id}")
    try:
        path = "/v2/orders"
        params = {'order_id': order_id, 'product_id': product_id}
        response = make_authenticated_request('DELETE', path, params=params)
        
        if response and not response.get('error'):
            save_to_mongodb('order_cancellations', {'data': response, 'type': 'single_order_cancelled'})
            logger.info(f"Order {order_id} cancelled successfully")
        
        return response
    except Exception as e:
        logger.error(f"Error cancelling order: {e}")
        return {"error": f"Error cancelling order: {e}"}

def batch_cancel_orders(product_id, order_ids):
    """Batch me orders cancel karta hai."""
    logger.info(f"Cancelling {len(order_ids)} orders in a batch for product {product_id}")
    try:
        path = "/v2/orders/batch"
        data = {
            'product_id': product_id,
            'order_ids': order_ids
        }
        response = make_authenticated_request('DELETE', path, data=data)
        
        if response and not response.get('error'):
            save_to_mongodb('order_cancellations', {'data': response, 'type': 'batch_orders_cancelled'})
            logger.info(f"Orders for product {product_id} cancelled successfully in batch")
        
        return response
    except Exception as e:
        logger.error(f"Error cancelling batch orders: {e}")
        return {"error": f"Error cancelling batch orders: {e}"}

def cancel_all_product_orders(product_id):
    """Product ke saare open orders cancel karta hai."""
    logger.info(f"Cancelling all open orders for product {product_id}")
    try:
        path = "/v2/orders"
        params = {'product_id': product_id, 'all_orders': True}
        response = make_authenticated_request('DELETE', path, params=params)
        
        if response and not response.get('error'):
            save_to_mongodb('order_cancellations', {'data': response, 'type': 'all_product_orders_cancelled'})
            logger.info(f"All open orders for product {product_id} cancelled successfully")
        
        return response
    except Exception as e:
        logger.error(f"Error cancelling all product orders: {e}")
        return {"error": f"Error cancelling all product orders: {e}"}

def set_leverage(product_id, leverage):
    """Position ka leverage set karta hai."""
    logger.info(f"Setting leverage for product {product_id} to {leverage}")
    try:
        leverage_data = {
            'product_id': product_id,
            'leverage': str(leverage)
        }
        path = "/v2/positions/set_leverage"
        response = make_authenticated_request('POST', path, data=leverage_data)
        
        if response and not response.get('error'):
            save_to_mongodb('leverage_changes', {'data': response, 'type': 'leverage_changed'})
            logger.info("Leverage set successfully")
        
        return response
    except Exception as e:
        logger.error(f"Error setting leverage: {e}")
        return {"error": f"Error setting leverage: {e}"}

def change_position_margin(product_id, delta_margin):
    """Position me margin add ya remove karta hai."""
    logger.info(f"Changing margin for product {product_id} by {delta_margin}")
    try:
        margin_data = {
            'product_id': product_id,
            'delta_margin': str(delta_margin)
        }
        
        path = "/v2/positions/change_margin"
        response = make_authenticated_request('POST', path, data=margin_data)
        
        if response and not response.get('error'):
            save_to_mongodb('margin_changes', {'data': response, 'type': 'position_margin_changed'})
            logger.info("Position margin changed successfully")
        
        return response
    except Exception as e:
        logger.error(f"Error changing position margin: {e}")
        return {"error": f"Error changing position margin: {e}"}

def close_all_positions():
    """Saari positions close kar deta hai."""
    logger.info("Closing all positions")
    try:
        close_data = {
            'close_all_portfolio': True,
            'close_all_isolated': True
        }
        
        path = "/v2/positions/close_all"
        response = make_authenticated_request('POST', path, data=close_data)
        
        if response and not response.get('error'):
            save_to_mongodb('position_closures', {'data': response, 'type': 'all_positions_closed'})
            logger.info("All positions closed successfully")
        
        return response
    except Exception as e:
        logger.error(f"Error closing all positions: {e}")
        return {"error": f"Error closing all positions: {e}"}