from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from . import config
from .main_integration import (
    get_all_assets,
    get_ticker,
    get_orderbook,
    get_historical_candles,
    get_wallet_balance,
    get_all_positions,
    get_open_orders,
    place_limit_order,
    place_market_order,
    cancel_single_order
)
from .mongo_db import get_portfolio_summary, get_market_overview
from typing import Optional
import logging
import time

logger = logging.getLogger(__name__)

app = FastAPI(title="Delta Exchange API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Market Data Endpoints
@app.get("/api/market/assets")
async def api_get_assets():
    """Get all assets"""
    try:
        result = get_all_assets()
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/market/ticker/{symbol}")
async def api_get_ticker(symbol: str):
    """Get ticker for a symbol"""
    try:
        result = get_ticker(symbol=symbol)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/market/orderbook/{symbol}")
async def api_get_orderbook(symbol: str, depth: int = 10):
    """Get orderbook for a symbol"""
    try:
        result = get_orderbook(symbol=symbol, depth=depth)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/market/historical-candles")
async def api_get_historical_candles(
    product_id: int = Query(..., description="Product ID to fetch candles for"),
    resolution: str = Query(..., description="Candle resolution (e.g., '1m', '1h', '1d')"),
    start_time: Optional[int] = Query(None, description="Start time (Unix timestamp in seconds)"),
    end_time: Optional[int] = Query(None, description="End time (Unix timestamp in seconds)")
):
    """Get historical candles for a product"""
    try:
        if start_time is None:
            # Default to fetching last 24 hours of data if no time is provided
            start_time = int(time.time()) - 86400
        if end_time is None:
            end_time = int(time.time())

        result = get_historical_candles(
            product_id=product_id,
            resolution=resolution,
            start_time=start_time,
            end_time=end_time
        )
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Trading Endpoints
@app.get("/api/trading/open-orders")
async def api_get_open_orders(product_id: Optional[int] = None):
    """Get all open orders for a product or all products"""
    try:
        result = get_open_orders(product_id=product_id)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/trading/positions")
async def api_get_positions():
    """Get all open positions"""
    try:
        result = get_all_positions()
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/trading/balance")
async def api_get_balance():
    """Get wallet balance"""
    try:
        result = get_wallet_balance()
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trading/order/limit")
async def api_place_limit_order(order_data: dict):
    """Place a limit order"""
    try:
        result = place_limit_order(**order_data)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trading/order/market")
async def api_place_market_order(order_data: dict):
    """Place a market order"""
    try:
        result = place_market_order(**order_data)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/trading/order/{order_id}")
async def api_cancel_order(order_id: str, product_id: int):
    """Cancel single order by its ID"""
    try:
        result = cancel_single_order(product_id=product_id, order_id=order_id)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Dashboard Endpoints
@app.get("/api/dashboard/portfolio")
async def api_portfolio_summary():
    """Get portfolio summary"""
    try:
        result = get_portfolio_summary()
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dashboard/market")
async def api_market_overview():
    """Get market overview"""
    try:
        result = get_market_overview()
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Delta Exchange API Server is running!", "status": "active"}