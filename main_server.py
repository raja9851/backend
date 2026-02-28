"""
main_server.py
- FastAPI app, REST endpoints, startup logic, and Socket.IO setup.
- Imports and registers Socket.IO event handlers from socketio_events.py and upstox_events.py.
"""

# Enhanced WebSocket Server with Complete Data Storage
import asyncio
import uvicorn
import contextlib
from fastapi import FastAPI, Query, Path, Body
from fastapi.middleware.cors import CORSMiddleware
import socketio
from typing import Optional
import logging
from datetime import datetime

# Import enhanced MongoDB service
from mongo_service import (
    save_option_chain, save_market_data, save_user_action,
    save_system_log, save_price_update, get_recent_data,
    get_trading_summary, get_database_stats
)

# Import other modules
from option_chain import OptionChainHandler
from instrument_manager import InstrumentManager
from market_data_handler import MarketDataHandler
from trading_system import trading_system

# Import and register Socket.IO events
from socketio_events import register_socketio_events
from upstox_events import register_upstox_events

# !!! --- CORRECTION 1: Import the auth module --- !!!
from auth import auth

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
instrument_manager = InstrumentManager()
market_data_handler = MarketDataHandler()
option_chain_handler = OptionChainHandler()


# ============== ENHANCED MARKET DATA HANDLER ==============

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


# ============== LIFESPAN EVENT HANDLER ==============

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles startup and shutdown events for the FastAPI application.
    """
    # --- Code to run on startup ---
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
        # Start background tasks
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

    yield

    # --- Code to run on shutdown (optional) ---
    logger.info("🔌 Server is shutting down.")


# ============== FASTAPI & SOCKET.IO SETUP ==============

# ✅ FastAPI setup with lifespan handler
app = FastAPI(lifespan=lifespan)

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

# Register all socketio events from both files
register_socketio_events(sio, instrument_manager, market_data_handler, option_chain_handler, trading_system, logger)

# !!! --- CORRECTION 2: Pass the access token to initialize the Upstox client --- !!!
register_upstox_events(sio, auth.access_token)


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
            "Real-time Data Streaming",
            "Upstox Advanced Features"
        ],
        "database_collections": [
            "option_chains", "market_data", "trading_orders", 
            "positions", "script_executions", "auto_scripts", 
            "user_actions", "system_logs", "funds_history"
        ],
        "socket_events": {
            "basic_events": [
                "connect", "disconnect", "search_instrument",
                "get_option_chain", "get_expiries", "place_order",
                "cancel_order", "get_positions", "get_orders",
                "execute_script", "start_auto_script", "stop_auto_script",
                "get_trading_status", "exit_all_positions",
                "get_database_analytics", "save_script", "load_scripts"
            ],
            "upstox_events": [
                "create_gtt_order", "get_gtt_orders", "get_historical_data",
                "get_portfolio_analytics", "calculate_margin", "get_market_depth",
                "get_trade_history", "get_complete_dashboard"
            ]
        },
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
            "upstox_integration": "Available",
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


# ============== SERVER EXECUTION ==============

if __name__ == "__main__":
    print("🌟 Starting Enhanced Market Data & Trading Server...")
    print("📊 Market Data: Real-time with historical storage")
    print("💰 Trading: Complete order & position tracking")
    print("🤖 Scripts: Execution history & auto script monitoring")
    print("📈 Analytics: User activity & trading summaries")
    print("🗄️  Database: 9 collections for complete data storage")
    print("🔗 WebSocket: Real-time updates with data persistence")
    print("📡 REST API: All endpoints with analytics")
    print("🚀 Upstox: Advanced trading features enabled")
    uvicorn.run(sio_app, host="0.0.0.0", port=3001, log_level="info")