"""
Upstox Socket.IO Events Handler - FIXED for V3 API
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Optional

# Import your existing services
from mongo_service import save_user_action, save_system_log, save_order, save_positions
from upstox_client import UpstoxClient, UpstoxEvents

# Global instances
upstox_client = None
upstox_events = None
logger = logging.getLogger(__name__)

def parse_interval_for_v3(interval_str: str):
    """Parse interval string for V3 API format"""
    # Extract numbers and letters
    numbers = re.findall(r'\d+', interval_str)
    letters = re.findall(r'[a-zA-Z]+', interval_str)
    
    if not numbers or not letters:
        raise ValueError(f"Invalid interval format: {interval_str}")
    
    interval_value = numbers[0]
    unit_base = letters[0].lower()
    
    # Map singular to plural
    unit_mapping = {
        'minute': 'minutes',
        'hour': 'hours', 
        'day': 'days',
        'week': 'weeks',
        'month': 'months'
    }
    
    unit = unit_mapping.get(unit_base, unit_base)
    if unit is None:
        raise ValueError(f"Invalid interval unit: {unit_base}")
    if not unit.endswith('s'):
        unit += 's'
    
    return unit, interval_value

def register_upstox_events(sio, access_token: Optional[str] = None):
    """Register all Upstox Socket.IO events"""
    global upstox_client, upstox_events
    
    if access_token:
        try:
            upstox_client = UpstoxClient(access_token)
            upstox_events = UpstoxEvents(upstox_client)
            logger.info("Upstox client initialized successfully")
            
            # Add callbacks to send data to frontend via Socket.IO
            def on_market_data(data):
                asyncio.create_task(sio.emit("upstoxMarketData", data))
            
            def on_order_update(data):
                asyncio.create_task(sio.emit("upstoxOrderUpdate", data))
            
            def on_position_update(data):
                asyncio.create_task(sio.emit("upstoxPositionUpdate", data))
            
            def on_connection_status(data):
                asyncio.create_task(sio.emit("upstoxConnectionStatus", data))
            
            # Register callbacks
            upstox_events.add_callback('market_data', on_market_data)
            upstox_events.add_callback('order_update', on_order_update)
            upstox_events.add_callback('position_update', on_position_update)
            upstox_events.add_callback('connection_status', on_connection_status)
            
        except Exception as e:
            logger.error(f"Upstox client initialization failed: {e}")
            save_system_log("ERROR", "Upstox initialization failed", {"error": str(e)})

    # MARGINS
    @sio.event
    async def upstox_get_funds(sid, data=None):
        """Get Upstox funds and margin"""
        try:
            if not upstox_client:
                raise Exception("Upstox client not initialized")
                
            save_user_action("upstox_get_funds", {}, sid)
            segment = data.get('segment') if data else None
            result = upstox_client.get_user_funds_and_margin(segment)
            
            await sio.emit("upstoxFunds", {
                "status": result.get("status"),
                "data": result.get("data", {}),
                "timestamp": datetime.now().isoformat()
            }, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox funds failed", {"error": str(e)})
            await sio.emit("upstoxFunds", {"status": "error", "error": str(e)}, room=sid)

    # ORDERS
    @sio.event
    async def upstox_place_order(sid, data):
        """Place order via Upstox"""
        try:
            if not upstox_client:
                raise Exception("Upstox client not initialized")
                
            save_user_action("upstox_place_order", data, sid)
            
            result = upstox_client.place_order(
                instrument_token=data.get('instrument_token'),
                quantity=data.get('quantity'),
                product=data.get('product', 'MIS'),
                validity=data.get('validity', 'DAY'),
                price=data.get('price', 0),
                order_type=data.get('order_type', 'MARKET'),
                transaction_type=data.get('transaction_type', 'BUY'),
                **{k: v for k, v in data.items() if k in ['disclosed_quantity', 'trigger_price', 'is_amo', 'tag']}
            )
            
            # Save to MongoDB
            save_order({**data, "status": result.get("status"), "response": result.get("data", {}), "source": "upstox"}, "place")
            
            await sio.emit("upstoxOrderResponse", {
                "status": result.get("status"),
                "data": result.get("data", {}),
                "order_id": result.get("data", {}).get("order_id") if result.get("status") == "success" else None
            }, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox order placement failed", {"error": str(e)})
            await sio.emit("upstoxOrderResponse", {"status": "error", "error": str(e)}, room=sid)

    @sio.event
    async def upstox_get_orders(sid, data=None):
        """Get Upstox order book"""
        try:
            if not upstox_client:
                raise Exception("Upstox client not initialized")
                
            save_user_action("upstox_get_orders", {}, sid)
            result = upstox_client.get_order_book()
            
            await sio.emit("upstoxOrders", {
                "status": result.get("status"),
                "orders": result.get("data", []),
                "count": len(result.get("data", []))
            }, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox order book failed", {"error": str(e)})
            await sio.emit("upstoxOrders", {"status": "error", "error": str(e)}, room=sid)

    @sio.event
    async def upstox_cancel_order(sid, data):
        """Cancel Upstox order"""
        try:
            if not upstox_client:
                raise Exception("Upstox client not initialized")
                
            save_user_action("upstox_cancel_order", data, sid)
            result = upstox_client.cancel_order(data.get('order_id'))
            
            save_order({"order_id": data.get('order_id'), "status": result.get("status"), "response": result.get("data", {}), "source": "upstox"}, "cancel")
            await sio.emit("upstoxCancelResponse", result, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox order cancellation failed", {"error": str(e)})
            await sio.emit("upstoxCancelResponse", {"status": "error", "error": str(e)}, room=sid)

    # PORTFOLIO
    @sio.event
    async def upstox_get_holdings(sid, data=None):
        """Get holdings"""
        try:
            if not upstox_client:
                raise Exception("Upstox client not initialized")
                
            save_user_action("upstox_get_holdings", {}, sid)
            result = upstox_client.get_holdings()
            
            await sio.emit("upstoxHoldings", {
                "status": result.get("status"),
                "holdings": result.get("data", []),
                "count": len(result.get("data", []))
            }, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox holdings failed", {"error": str(e)})
            await sio.emit("upstoxHoldings", {"status": "error", "error": str(e)}, room=sid)

    @sio.event
    async def upstox_get_positions(sid, data=None):
        """Get positions"""
        try:
            if not upstox_client:
                raise Exception("Upstox client not initialized")
                
            save_user_action("upstox_get_positions", {}, sid)
            result = upstox_client.get_positions()
            
            # Save to MongoDB
            if result.get("status") == "success":
                save_positions(result.get("data", []))
            
            await sio.emit("upstoxPositions", {
                "status": result.get("status"),
                "positions": result.get("data", []),
                "count": len(result.get("data", []))
            }, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox positions failed", {"error": str(e)})
            await sio.emit("upstoxPositions", {"status": "error", "error": str(e)}, room=sid)

    # HISTORICAL DATA - FIXED FOR V3 API
    @sio.event
    async def upstox_get_historical_data(sid, data):
        """Get historical data using V3 API"""
        try:
            if not upstox_client:
                raise Exception("Upstox client not initialized")
            
            save_user_action("upstox_get_historical_data", data, sid)

            instrument_key = data.get('instrument_key')
            interval_str = data.get('interval', '1minute')
            
            # Parse interval correctly for V3 API
            unit, interval_value = parse_interval_for_v3(interval_str)
            
            # Get current date and calculate from_date properly
            to_date = datetime.now().strftime('%Y-%m-%d')
            from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')  # Last 7 days
            
            logger.info(f"Fetching V3 HISTORICAL data for {instrument_key} from {from_date} to {to_date} with unit '{unit}' and interval '{interval_value}'")
            
            # Always use historical endpoint with proper date range
            result = upstox_client.get_historical_candle_data(
                instrument_key,
                unit,
                interval_value,
                to_date,
                from_date  # from_date should be the earlier date
            )
            
            await sio.emit("upstoxHistoricalData", result, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox historical data failed", {"error": str(e)})
            await sio.emit("upstoxHistoricalData", {"status": "error", "error": str(e)}, room=sid)

    # MARKET QUOTES
    @sio.event
    async def upstox_get_ltp(sid, data):
        """Get LTP"""
        try:
            if not upstox_client:
                raise Exception("Upstox client not initialized")
                
            save_user_action("upstox_get_ltp", data, sid)
            symbol = data.get('symbol')
            result = upstox_client.ltp(symbol)
            
            await sio.emit("upstoxLtp", {
                "symbol": symbol,
                "status": result.get("status"),
                "data": result.get("data", {})
            }, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox LTP failed", {"error": str(e)})
            await sio.emit("upstoxLtp", {"status": "error", "error": str(e)}, room=sid)

    @sio.event
    async def upstox_search_instruments(sid, data):
        """Search instruments using local mapping"""
        try:
            save_user_action("upstox_search_instruments", data, sid)
            
            # Local instrument mapping since V3 API doesn't have search endpoint
            instruments_map = {
                'NIFTY': {
                    'instrument_key': 'NSE_INDEX|Nifty 50',
                    'name': 'NIFTY 50',
                    'tradingsymbol': 'NIFTY',
                    'instrument_type': 'INDEX',
                    'exchange': 'NSE'
                },
                'BANKNIFTY': {
                    'instrument_key': 'NSE_INDEX|Nifty Bank',
                    'name': 'NIFTY BANK',
                    'tradingsymbol': 'BANKNIFTY',
                    'instrument_type': 'INDEX',
                    'exchange': 'NSE'
                },
                'SENSEX': {
                    'instrument_key': 'BSE_INDEX|SENSEX',
                    'name': 'SENSEX',
                    'tradingsymbol': 'SENSEX',
                    'instrument_type': 'INDEX',
                    'exchange': 'BSE'
                },
                'RELIANCE': {
                    'instrument_key': 'NSE_EQ|INE002A01018',
                    'name': 'RELIANCE INDUSTRIES LTD',
                    'tradingsymbol': 'RELIANCE',
                    'instrument_type': 'EQ',
                    'exchange': 'NSE'
                },
                'TCS': {
                    'instrument_key': 'NSE_EQ|INE467B01029',
                    'name': 'TATA CONSULTANCY SERVICES LTD',
                    'tradingsymbol': 'TCS',
                    'instrument_type': 'EQ',
                    'exchange': 'NSE'
                }
            }
            
            query = data.get('query', '').upper()
            results = []
            
            # Find matching instruments
            for symbol, instrument_data in instruments_map.items():
                if query in symbol or query in instrument_data['name']:
                    results.append(instrument_data)
            
            await sio.emit("upstoxInstrumentSearch", {
                "status": "success" if results else "error",
                "data": results,
                "error": "No instruments found" if not results else None
            }, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox instrument search failed", {"error": str(e)})
            await sio.emit("upstoxInstrumentSearch", {"status": "error", "error": str(e)}, room=sid)

    # WEBSOCKET FEED CONTROL
    @sio.event
    async def upstox_start_market_feed(sid, data):
        """Start market data feed"""
        try:
            if not upstox_events:
                raise Exception("Upstox events not initialized")
                
            save_user_action("upstox_start_market_feed", data, sid)
            symbols = data.get('symbols', [])
            mode = data.get('mode', 'ltpc')
            
            upstox_events.start_market_feed(symbols, mode)
            await sio.emit("upstoxFeedResponse", {
                "status": "success", "message": "Market feed started", "symbols": symbols, "mode": mode
            }, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox market feed start failed", {"error": str(e)})
            await sio.emit("upstoxFeedResponse", {"status": "error", "error": str(e)}, room=sid)

    @sio.event
    async def upstox_start_portfolio_feed(sid, data=None):
        """Start portfolio feed"""
        try:
            if not upstox_events:
                raise Exception("Upstox events not initialized")
                
            save_user_action("upstox_start_portfolio_feed", {}, sid)
            upstox_events.start_portfolio_feed()
            await sio.emit("upstoxFeedResponse", {"status": "success", "message": "Portfolio feed started"}, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox portfolio feed start failed", {"error": str(e)})
            await sio.emit("upstoxFeedResponse", {"status": "error", "error": str(e)}, room=sid)

    @sio.event
    async def upstox_stop_feeds(sid, data=None):
        """Stop all feeds"""
        try:
            if not upstox_events:
                raise Exception("Upstox events not initialized")
                
            save_user_action("upstox_stop_feeds", {}, sid)
            upstox_events.stop_feeds()
            await sio.emit("upstoxFeedResponse", {"status": "success", "message": "All feeds stopped"}, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox feed stop failed", {"error": str(e)})
            await sio.emit("upstoxFeedResponse", {"status": "error", "error": str(e)}, room=sid)

    @sio.event
    async def upstox_get_dashboard(sid, data=None):
        """Get complete dashboard data"""
        try:
            if not upstox_client:
                raise Exception("Upstox client not initialized")
                
            save_user_action("upstox_get_dashboard", {}, sid)
            
            # Fetch all data
            profile = upstox_client.get_user_profile()
            funds = upstox_client.get_user_funds_and_margin()
            holdings = upstox_client.get_holdings()
            positions = upstox_client.get_positions()
            orders = upstox_client.get_order_book()
            
            dashboard_data = {
                "profile": profile.get("data", {}) if profile.get("status") == "success" else {},
                "funds": funds.get("data", {}) if funds.get("status") == "success" else {},
                "holdings": holdings.get("data", []) if holdings.get("status") == "success" else [],
                "positions": positions.get("data", []) if positions.get("status") == "success" else [],
                "orders": orders.get("data", []) if orders.get("status") == "success" else [],
                "summary": {
                    "total_holdings": len(holdings.get("data", [])) if holdings.get("status") == "success" else 0,
                    "total_positions": len(positions.get("data", [])) if positions.get("status") == "success" else 0,
                    "total_orders": len(orders.get("data", [])) if orders.get("status") == "success" else 0,
                },
                "timestamp": datetime.now().isoformat()
            }
            
            await sio.emit("upstoxDashboard", {"status": "success", "data": dashboard_data}, room=sid)
            
        except Exception as e:
            save_system_log("ERROR", "Upstox dashboard failed", {"error": str(e)})
            await sio.emit("upstoxDashboard", {"status": "error", "error": str(e)}, room=sid)

    logger.info("Upstox Socket.IO events registered successfully")


def get_upstox_status():
    """Get current Upstox client status"""
    global upstox_client
    
    if not upstox_client:
        return {"status": "not_initialized", "message": "Upstox client not initialized"}
    
    try:
        result = upstox_client.get_user_profile()
        if result.get("status") == "success":
            return {"status": "connected", "message": "Upstox client active and connected"}
        else:
            return {"status": "error", "message": result.get("error", "Unknown error")}
    except Exception as e:
        return {"status": "error", "message": str(e)}