# Enhanced WebSocket Server with Complete Data Storage
import asyncio
import uvicorn
from fastapi import FastAPI, Query, Path, Body
from fastapi.middleware.cors import CORSMiddleware
import socketio
from typing import Optional
import logging
from datetime import datetime

# Import enhanced MongoDB service
from mongo_service import (
    save_option_chain, save_market_data, save_order, save_positions, 
    save_script_execution, save_auto_script_status, save_user_action,
    save_system_log, save_funds_snapshot, save_price_update,
    get_recent_data, get_trading_summary, get_database_stats
)

# Import other modules
from auth import auth
from option_chain import OptionChainHandler
from instrument_manager import InstrumentManager
from market_data_handler import MarketDataHandler
from trading_system import trading_system, TradeConfig, OrderType, TransactionType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ FastAPI setup with CORS
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Socket.IO setup
sio = socketio.AsyncServer(
    cors_allowed_origins="*",
    async_mode='asgi',
    logger=False,
    engineio_logger=False
)
sio_app = socketio.ASGIApp(sio, app)

# Global instances
instrument_manager = InstrumentManager()
market_data_handler = MarketDataHandler()
option_chain_handler = OptionChainHandler()

# ============== ENHANCED SOCKET.IO EVENTS WITH DATA STORAGE ==============

@sio.event
async def connect(sid, environ):
    logger.info(f"🔗 Client connected: {sid}")
    
    # Save user action
    save_user_action("client_connect", {"session_id": sid}, sid)
    
    # Send latest market data
    latest_data = market_data_handler.get_latest_data()
    if latest_data:
        await sio.emit("marketData", {"feeds": latest_data}, room=sid)
    
    # Send cached option data
    option_data = market_data_handler.get_option_data()
    if option_data:
        await sio.emit("optionData", option_data, room=sid)

@sio.event
async def disconnect(sid):
    logger.info(f"❌ Client disconnected: {sid}")
    # Save user action
    save_user_action("client_disconnect", {"session_id": sid}, sid)

@sio.event
async def search_instrument(sid, data):
    symbol = data.get('symbol', '')
    type_filter = data.get('type')

    results = instrument_manager.search_instrument(symbol, type_filter)
    await sio.emit("instrumentSearchResults", {
        "symbol": symbol,
        "results": results[:50]   # limit 50 results
    }, room=sid)

@sio.event
async def get_option_chain(sid, data):
    """Handle option chain requests with complete data storage"""
    symbol = data.get('symbol', 'NIFTY')
    expiry = data.get('expiry')
    
    try:
        logger.info(f"📊 Option chain request for {symbol}, expiry: {expiry}")
        
        # Save user action
        save_user_action("get_option_chain", {
            "symbol": symbol,
            "expiry": expiry
        }, sid)
        
        result = option_chain_handler.fetch_option_chain(symbol, expiry)
        
        if result.get("status") == "success":
            # Save option chain data
            save_option_chain(result)
            
            await sio.emit("optionChainData", {
                "symbol": symbol,
                "status": "success",
                "data": result.get("data", []),
                "expiry": result.get("expiry"),
                "count": result.get("count", 0),
                "timestamp": result.get("timestamp")
            }, room=sid)
            
            logger.info(f"✅ Sent option data for {symbol}: {result.get('count', 0)} strikes")
        else:
            # Save error log
            save_system_log("ERROR", f"Option chain fetch failed for {symbol}", {
                "symbol": symbol,
                "expiry": expiry,
                "error": result.get("message")
            })
            
            await sio.emit("optionChainData", {
                "symbol": symbol,
                "status": "error",
                "error": result.get("message", "Failed to fetch option chain")
            }, room=sid)
            
    except Exception as e:
        logger.error(f"❌ Error in get_option_chain: {e}")
        
        # Save error log
        save_system_log("ERROR", f"Option chain exception for {symbol}", {
            "symbol": symbol,
            "expiry": expiry,
            "error": str(e)
        })
        
        await sio.emit("optionChainData", {
            "symbol": symbol,
            "status": "error",
            "error": str(e)
        }, room=sid)

@sio.event
async def get_expiries(sid, data):
    """Handle expiry dates request with logging"""
    symbol = data.get('symbol', 'NIFTY')
    
    try:
        # Save user action
        save_user_action("get_expiries", {"symbol": symbol}, sid)
        
        expiries = option_chain_handler.get_expiry_dates(symbol)
        current_expiry = option_chain_handler.get_next_trading_expiry(symbol)
        
        await sio.emit("expiryDates", {
            "symbol": symbol,
            "status": "success",
            "expiries": expiries,
            "current_expiry": current_expiry,
            "count": len(expiries)
        }, room=sid)
        
        logger.info(f"✅ Sent {len(expiries)} expiries for {symbol}")
        
    except Exception as e:
        logger.error(f"❌ Error fetching expiries: {e}")
        save_system_log("ERROR", f"Expiries fetch failed for {symbol}", {"error": str(e)})
        
        await sio.emit("expiryDates", {
            "symbol": symbol,
            "status": "error",
            "error": str(e)
        }, room=sid)

# ============== ENHANCED TRADING EVENTS WITH DATA STORAGE ==============

@sio.event
async def place_order(sid, data):
    """Place order with complete data storage"""
    try:
        symbol = data.get('symbol')
        quantity = data.get('quantity')
        price = data.get('price', 0)
        order_type = data.get('order_type', 'MARKET')
        transaction_type = data.get('transaction_type', 'BUY')
        
        # Save user action
        save_user_action("place_order", {
            "symbol": symbol,
            "quantity": quantity,
            "price": price,
            "order_type": order_type,
            "transaction_type": transaction_type
        }, sid)
        
        result = trading_system.place_order(symbol, quantity, price, order_type, transaction_type)
        
        # Save order data
        order_data = {
            "symbol": symbol,
            "quantity": quantity,
            "price": price,
            "order_type": order_type,
            "transaction_type": transaction_type,
            "status": result.get("status"),
            "response": result
        }
        
        if result.get("status") == "success":
            order_data["order_id"] = result.get("data", {}).get("order_id")
        
        save_order(order_data, "place")
        
        await sio.emit("orderResponse", {
            "status": result.get("status"),
            "order_id": result.get("data", {}).get("order_id") if result.get("status") == "success" else None,
            "message": result.get("message", ""),
            "symbol": symbol,
            "quantity": quantity,
            "price": price,
            "type": transaction_type
        }, room=sid)
        
    except Exception as e:
        save_system_log("ERROR", "Order placement failed", {"error": str(e)})
        await sio.emit("orderResponse", {
            "status": "error",
            "error": str(e)
        }, room=sid)

@sio.event
async def cancel_order(sid, data):
    """Cancel order with logging"""
    try:
        order_id = data.get('order_id')
        
        # Save user action
        save_user_action("cancel_order", {"order_id": order_id}, sid)
        
        result = trading_system.cancel_order(order_id)
        
        # Save cancellation data
        save_order({
            "order_id": order_id,
            "status": result.get("status"),
            "response": result
        }, "cancel")
        
        await sio.emit("cancelResponse", {
            "status": result.get("status"),
            "order_id": order_id,
            "message": result.get("message", "")
        }, room=sid)
        
    except Exception as e:
        save_system_log("ERROR", "Order cancellation failed", {"error": str(e)})
        await sio.emit("cancelResponse", {
            "status": "error",
            "error": str(e)
        }, room=sid)

@sio.event
async def get_positions(sid, data=None):
    """Get positions with data storage"""
    try:
        # Save user action
        save_user_action("get_positions", {}, sid)
        
        result = trading_system.get_positions()
        
        # Save positions snapshot
        if result.get("status") == "success":
            save_positions(result.get("data", []))
        
        await sio.emit("positionsData", {
            "status": result.get("status"),
            "positions": result.get("data", []),
            "count": len(result.get("data", []))
        }, room=sid)
        
    except Exception as e:
        save_system_log("ERROR", "Get positions failed", {"error": str(e)})
        await sio.emit("positionsData", {
            "status": "error",
            "error": str(e)
        }, room=sid)

@sio.event
async def get_orders(sid, data=None):
    """Get orders with logging"""
    try:
        # Save user action
        save_user_action("get_orders", {}, sid)
        
        result = trading_system.get_orders()
        
        await sio.emit("ordersData", {
            "status": result.get("status"),
            "orders": result.get("data", []),
            "count": len(result.get("data", []))
        }, room=sid)
        
    except Exception as e:
        save_system_log("ERROR", "Get orders failed", {"error": str(e)})
        await sio.emit("ordersData", {
            "status": "error",
            "error": str(e)
        }, room=sid)

@sio.event
async def execute_script(sid, data):
    """Execute script with complete logging"""
    script_name = 'Untitled'  # Default value
    try:
        code = data.get('code', '')
        script_name = data.get('script_name', script_name)
        
        # Save user action
        save_user_action("execute_script", {
            "script_name": script_name,
            "code_length": len(code)
        }, sid)
        
        result = trading_system.execute_script(code, script_name)
        
        # Save script execution
        save_script_execution({
            "name": script_name,
            "code": code,
            "status": result.get("status"),
            "message": result.get("message"),
            "error": result.get("error"),
            "execution_time": datetime.now().isoformat()
        })
        
        await sio.emit("scriptResponse", {
            "status": result.get("status"),
            "message": result.get("message", ""),
            "error": result.get("error", ""),
            "script_name": script_name
        }, room=sid)
        
    except Exception as e:
        save_system_log("ERROR", "Script execution failed", {
            "script_name": script_name,
            "error": str(e)
        })
        await sio.emit("scriptResponse", {
            "status": "error",
            "error": str(e)
        }, room=sid)

@sio.event
async def start_auto_script(sid, data):
    """Start auto script with logging"""
    try:
        script_id = data.get('script_id')
        
        # Save user action
        save_user_action("start_auto_script", {
            "script_id": script_id,
        }, sid)
        
        result = trading_system.start_auto_script(script_id, data.get('code', ''), data.get('name', 'Auto Script'))
        
        # Save auto script status
        save_auto_script_status(script_id, "started", {
            "name": data.get('name', 'Auto Script'),
            "result": result
        })
        
        await sio.emit("autoScriptResponse", {
            "status": result.get("status"),
            "script_id": script_id,
            "message": result.get("message", ""),
            "action": "start"
        }, room=sid)
        
    except Exception as e:
        save_system_log("ERROR", "Auto script start failed", {"error": str(e)})
        await sio.emit("autoScriptResponse", {
            "status": "error",
            "error": str(e)
        }, room=sid)

@sio.event
async def stop_auto_script(sid, data):
    """Stop auto script with logging"""
    try:
        script_id = data.get('script_id')
        
        # Save user action
        save_user_action("stop_auto_script", {"script_id": script_id}, sid)
        
        result = trading_system.stop_auto_script(script_id)
        
        # Save auto script status
        save_auto_script_status(script_id, "stopped", {"result": result})
        
        await sio.emit("autoScriptResponse", {
            "status": result.get("status"),
            "script_id": script_id,
            "message": result.get("message", ""),
            "action": "stop"
        }, room=sid)
        
    except Exception as e:
        save_system_log("ERROR", "Auto script stop failed", {"error": str(e)})
        await sio.emit("autoScriptResponse", {
            "status": "error",
            "error": str(e)
        }, room=sid)

@sio.event
async def get_trading_status(sid, data):
    """Get trading status with funds snapshot"""
    try:
        # Save user action
        save_user_action("get_trading_status", {}, sid)
        
        # Get all trading data
        funds_result = trading_system.get_funds()
        positions_result = trading_system.get_positions()
        orders_result = trading_system.get_orders()
        auto_scripts_status = trading_system.get_auto_scripts_status()
        
        # Save funds snapshot
        if funds_result.get("status") == "success":
            save_funds_snapshot(funds_result.get("data", {}))
        
        # Save positions if available
        if positions_result.get("status") == "success":
            save_positions(positions_result.get("data", []))
        
        await sio.emit("tradingStatus", {
            "status": "success",
            "funds": funds_result.get("data", {}) if funds_result.get("status") == "success" else {},
            "positions_count": len(positions_result.get("data", [])) if positions_result.get("status") == "success" else 0,
            "orders_count": len(orders_result.get("data", [])) if orders_result.get("status") == "success" else 0,
            "auto_scripts": auto_scripts_status,
            "connection_status": "connected" if funds_result.get("status") == "success" else "disconnected"
        }, room=sid)
        
    except Exception as e:
        save_system_log("ERROR", "Get trading status failed", {"error": str(e)})
        await sio.emit("tradingStatus", {
            "status": "error",
            "error": str(e)
        }, room=sid)

@sio.event
async def exit_all_positions(sid, data):
    """Exit all positions with logging"""
    try:
        # Save user action
        save_user_action("exit_all_positions", {}, sid)
        
        result = trading_system.exit_all_positions()
        
        # Save the exit action
        save_order({
            "action_type": "exit_all",
            "status": result.get("status"),
            "response": result
        }, "exit_all")
        
        await sio.emit("exitResponse", {
            "status": result.get("status"),
            "message": result.get("message", ""),
            "orders": result.get("data", []) if result.get("status") == "success" else []
        }, room=sid)
        
    except Exception as e:
        save_system_log("ERROR", "Exit all positions failed", {"error": str(e)})
        await sio.emit("exitResponse", {
            "status": "error",
            "error": str(e)
        }, room=sid)

# ============== ENHANCED DATA ANALYTICS EVENTS ==============

@sio.event
async def get_database_analytics(sid, data):
    """Get comprehensive database analytics"""
    try:
        # Save user action
        save_user_action("get_database_analytics", {}, sid)
        
        # Get various analytics
        db_stats = get_database_stats()
        trading_summary_24h = get_trading_summary(24)
        trading_summary_7d = get_trading_summary(168)  # 7 days
        
        recent_orders = get_recent_data("trading_orders", 10)
        recent_scripts = get_recent_data("script_executions", 5)
        recent_user_actions = get_recent_data("user_actions", 20)
        
        analytics_data = {
            "database_stats": db_stats,
            "trading_summary": {
                "last_24h": trading_summary_24h,
                "last_7d": trading_summary_7d
            },
            "recent_activity": {
                "orders": recent_orders,
                "script_executions": recent_scripts,
                "user_actions": recent_user_actions
            },
            "timestamp": datetime.now().isoformat()
        }
        
        await sio.emit("databaseAnalytics", {
            "status": "success",
            "data": analytics_data
        }, room=sid)
        
    except Exception as e:
        save_system_log("ERROR", "Database analytics failed", {"error": str(e)})
        await sio.emit("databaseAnalytics", {
            "status": "error",
            "error": str(e)
        }, room=sid)

@sio.event
async def get_recent_activity(sid, data):
    """Get recent activity from database"""
    try:
        collection_name = data.get('collection', 'user_actions')
        limit = data.get('limit', 10)
        
        # Save user action
        save_user_action("get_recent_activity", {
            "collection": collection_name,
            "limit": limit
        }, sid)
        
        recent_data = get_recent_data(collection_name, limit)
        
        await sio.emit("recentActivity", {
            "status": "success",
            "collection": collection_name,
            "data": recent_data,
            "count": len(recent_data)
        }, room=sid)
        
    except Exception as e:
        save_system_log("ERROR", "Get recent activity failed", {"error": str(e)})
        await sio.emit("recentActivity", {
            "status": "error",
            "error": str(e)
        }, room=sid)
        
# ============== SCRIPT MANAGEMENT EVENTS (NEW) ==============

@sio.event
async def save_script(sid, data):
    """Save a trading script from the frontend to the database."""
    try:
        result = trading_system.save_script(data)
        response = {"status": "success" if result == "success" else "error", "message": result}
        await sio.emit("scriptSaveResponse", response, room=sid)
        if result == "success":
            logger.info(f"Script saved by client {sid}: {data.get('name')}")
    except Exception as e:
        logger.error(f"Error saving script: {e}")
        await sio.emit("scriptSaveResponse", {"status": "error", "error": str(e)}, room=sid)

@sio.event
async def load_scripts(sid):
    """Load all scripts for the frontend."""
    try:
        result = trading_system.get_all_scripts()
        # The result from trading_system.get_all_scripts() is already a list of scripts.
        # It needs to be wrapped in a dictionary with 'status' and 'data' keys
        # to match the frontend's expected format.
        await sio.emit("scriptsData", {"status": "success", "data": result}, room=sid)
        logger.info(f"Sent {len(result)} scripts to client {sid}")
    except Exception as e:
        logger.error(f"Error loading scripts: {e}")
        await sio.emit("scriptsData", {"status": "error", "error": str(e)}, room=sid)

@sio.event
async def delete_script(sid, data):
    """Delete a script from the database."""
    try:
        script_id = data.get('script_id')
        result = trading_system.delete_script(script_id)
        response = {"status": "success"} if result == "success" else {"status": "error", "message": result}
        await sio.emit("scriptDeleteResponse", response, room=sid)
        if response["status"] == "success":
            logger.info(f"Script deleted by client {sid}: {script_id}")
    except Exception as e:
        logger.error(f"Error deleting script: {e}")
        await sio.emit("scriptDeleteResponse", {"status": "error", "error": str(e)}, room=sid)

# ============== ENHANCED MARKET DATA WITH STORAGE ==============

async def enhanced_market_data_handler():
    """Enhanced market data handler that saves data periodically"""
    while True:
        try:
            # Get current market data
            latest_data = market_data_handler.get_latest_data()
            option_data = market_data_handler.get_option_data()
            
            # Save market data snapshot every 5 minutes
            if latest_data:
                save_market_data(latest_data, "periodic_snapshot")
            
            # Broadcast to all clients
            if latest_data:
                await sio.emit("marketData", {"feeds": latest_data})
            
            if option_data:
                await sio.emit("optionData", option_data)
                
            # Save individual price updates for key symbols
            key_symbols = ["NIFTY", "BANKNIFTY", "SENSEX"]
            for symbol in key_symbols:
                instrument_key = option_chain_handler.symbols.get(symbol)
                if instrument_key and instrument_key in latest_data:
                    price_data = latest_data[instrument_key]
                    save_price_update(symbol, price_data)
            
            await asyncio.sleep(300)  # 5 minutes
            
        except Exception as e:
            save_system_log("ERROR", "Market data handler error", {"error": str(e)})
            await asyncio.sleep(60)  # Wait 1 minute on error

# ============== ENHANCED REST API ENDPOINTS ==============

@app.get("/")
async def root():
    return {
        "message": "Enhanced Market Data & Trading Server with Complete Data Storage",
        "version": "6.0",
        "features": [
            "Market Data with Storage",
            "Option Chains with History", 
            "Trading with Order Logging",
            "Auto Scripts with Execution History",
            "User Activity Analytics",
            "Complete MongoDB Integration",
            "Real-time Data Streaming"
        ],
        "database_collections": [
            "option_chains", "market_data", "trading_orders", 
            "positions", "script_executions", "auto_scripts", 
            "user_actions", "system_logs", "funds_history"
        ],
        "supported_symbols": list(option_chain_handler.symbols.keys())
    }

@app.get("/analytics/database")
async def get_database_analytics_rest():
    """REST endpoint for database analytics"""
    try:
        db_stats = get_database_stats()
        trading_summary_24h = get_trading_summary(24)
        
        return {
            "status": "success",
            "database_stats": db_stats,
            "trading_summary_24h": trading_summary_24h,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/analytics/recent/{collection_name}")
async def get_recent_data_rest(collection_name: str, limit: int = 10):
    """REST endpoint for recent data"""
    try:
        data = get_recent_data(collection_name, limit)
        return {
            "status": "success",
            "collection": collection_name,
            "data": data,
            "count": len(data)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/analytics/trading-summary")
async def get_trading_summary_rest(hours: int = 24):
    """REST endpoint for trading summary"""
    try:
        summary = get_trading_summary(hours)
        return {
            "status": "success",
            "summary": summary,
            "period_hours": hours
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/health/enhanced")
async def enhanced_health_check():
    """Enhanced health check with database stats"""
    try:
        # Basic health checks
        latest_data = market_data_handler.get_latest_data()
        option_data = market_data_handler.get_option_data()
        test_result = option_chain_handler.fetch_option_chain('NIFTY')
        
        # Database health
        db_stats = get_database_stats()
        trading_summary = get_trading_summary(1)  # Last hour
        
        return {
            "status": "healthy",
            "message": "Enhanced WebSocket server with complete data storage",
            "market_data": {
                "latest_data_count": len(latest_data),
                "option_data_symbols": list(option_data.keys()),
                "instruments_loaded": len(instrument_manager.instruments)
            },
            "option_chain": {
                "supported_symbols": list(option_chain_handler.symbols.keys()),
                "test_status": test_result.get("status"),
                "test_count": test_result.get("count", 0)
            },
            "database": db_stats,
            "recent_activity": trading_summary,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/instruments/search")
async def instruments_search(
    q: str = Query("", alias="q"),
    limit: int = Query(50, alias="limit"),
    type: str = Query(None, alias="type")
):
    """
    Search instruments (stocks + F&O) using InstrumentManager.
    """
    results = instrument_manager.search_instrument(q, type)
    return {
        "status": "success",
        "query": q,
        "type": type,
        "count": len(results[:limit]),
        "results": results[:limit]
    }

@app.get("/instruments/stats")
async def instruments_stats():
    """
    Quick health of instrument cache.
    """
    try:
        stats = {
            "total_instruments": len(instrument_manager.instruments),
            "last_update": getattr(instrument_manager, "last_update", None),
            "symbols_loaded": [instrument["symbol"] for instrument in instrument_manager.instruments[:10]],  # show sample
        }
        return {"status": "success", "stats": stats}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/scripts")
async def get_scripts():
    """
    Get all scripts (for frontend REST).
    """
    try:
        scripts = trading_system.get_all_scripts()
        return {"status": "success", "data": scripts}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.post("/scripts")
async def post_script(data: dict = Body(...)):
    """
    Save a trading script (for frontend REST).
    """
    try:
        result = trading_system.save_script(data)
        if result == 'success':
            return {"status": "success"}
        else:
            return {"status": "error", "error": result}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.delete("/scripts/{script_id}")
async def delete_script_rest(script_id: str = Path(...)):
    """
    Delete a script by id (for frontend REST).
    """
    try:
        result = trading_system.delete_script(script_id)
        if result == 'success':
            return {"status": "success"}
        else:
            return {"status": "error", "error": result}
    except Exception as e:
        return {"status": "error", "error": str(e)}

# ============== STARTUP EVENT ==============

@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Starting enhanced market data & trading server...")
    
    # Log server startup
    save_system_log("INFO", "Server starting up", {
        "version": "6.0",
        "features": "Complete data storage integration"
    })
    
    # Set SocketIO reference for trading system
    trading_system.set_socketio(sio)
    logger.info("✅ Trading system connected to WebSocket")
    
    try:
        instrument_manager.update_all()
        logger.info("✅ Instruments loaded")
        save_system_log("INFO", "Instruments loaded successfully", {
            "count": len(instrument_manager.instruments)
        })
    except Exception as e:
        logger.error(f"❌ Error loading instruments: {e}")
        save_system_log("ERROR", "Instrument loading failed", {"error": str(e)})
    
    try:
        test_result = option_chain_handler.fetch_option_chain('NIFTY')
        if test_result.get("status") == "success":
            logger.info(f"✅ Option chain working - {test_result.get('count', 0)} strikes")
            save_option_chain(test_result)
            save_system_log("INFO", "Option chain test successful", {
                "strikes_count": test_result.get('count', 0)
            })
        else:
            logger.error(f"❌ Option chain test failed")
            save_system_log("ERROR", "Option chain test failed", {
                "error": test_result.get("message")
            })
    except Exception as e:
        logger.error(f"❌ Error testing option chain: {e}")
        save_system_log("ERROR", "Option chain test exception", {"error": str(e)})
    
    try:
        asyncio.create_task(market_data_handler.start_market_data_feed(sio, option_chain_handler))
        asyncio.create_task(enhanced_market_data_handler())
        logger.info("✅ Enhanced market data feed started")
        save_system_log("INFO", "Market data feed started", {"enhanced": True})
    except Exception as e:
        logger.error(f"❌ Error starting market data feed: {e}")
        save_system_log("ERROR", "Market data feed start failed", {"error": str(e)})
    
    # Log successful startup
    save_system_log("INFO", "Server startup completed", {
        "status": "ready",
        "features_active": True,
        "database_connected": True
    })
    
    logger.info("🌟 Enhanced server ready with complete data storage!")

if __name__ == "__main__":
    print("🌟 Starting Enhanced Market Data & Trading Server...")
    print("📊 Market Data: Real-time with historical storage")
    print("💰 Trading: Complete order & position tracking")
    print("🤖 Scripts: Execution history & auto script monitoring")
    print("📈 Analytics: User activity & trading summaries")
    print("🗄️  Database: 9 collections for complete data storage")
    print("🔗 WebSocket: Real-time updates with data persistence")
    print("📡 REST API: All endpoints with analytics")
    uvicorn.run(sio_app, host="0.0.0.0", port=3001, log_level="info")