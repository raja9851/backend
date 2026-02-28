"""
Delta Exchange FastAPI Server - FIXED VERSION
Ultra-fast live data streaming with proper message structure
"""

import logging
import asyncio
import json
import time
import queue
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any
from collections import defaultdict, deque

from fastapi import FastAPI, HTTPException, Query, Body, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import threading
import websocket

# Import Delta client
from delta_client import DeltaRESTClient, DeltaWebSocket, ENV, DRY_RUN

# Enhanced logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("delta.server")

class ConnectionManager:
    """Ultra-fast WebSocket connection manager with batching"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.subscribers: Dict[str, List[WebSocket]] = defaultdict(list)
        self.message_buffer: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.last_broadcast_time = {}
        
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"✅ Client connected | Total: {len(self.active_connections)}")
        
        # Send connection confirmation
        await self.send_personal_message({
            "type": "connection_status",
            "status": "connected",
            "server_time": int(time.time() * 1000)
        }, websocket)
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        
        # Remove from all subscriptions
        removed_count = 0
        for channel, subscribers in self.subscribers.items():
            if websocket in subscribers:
                subscribers.remove(websocket)
                removed_count += 1
        
        logger.info(f"❌ Client disconnected | Total: {len(self.active_connections)} | Unsubscribed from {removed_count} channels")
    
    async def send_personal_message(self, message: dict, websocket: WebSocket):
        try:
            message["server_timestamp"] = int(time.time() * 1000)
            await websocket.send_text(json.dumps(message, separators=(',', ':')))
        except Exception as e:
            logger.error(f"💥 Send error: {e}")
            self.disconnect(websocket)
    
    async def broadcast_fast(self, channel: str, message: dict):
        """Ultra-fast broadcast with minimal latency"""
        if channel not in self.subscribers or not self.subscribers[channel]:
            return
        
        # Add timestamps for latency tracking
        message["server_timestamp"] = int(time.time() * 1000)
        json_message = json.dumps(message, separators=(',', ':'))
        
        # Parallel sending for ultra-fast delivery
        tasks = []
        for connection in self.subscribers[channel]:
            task = asyncio.create_task(self._send_fast(connection, json_message, channel))
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.debug(f"⚡ Broadcast {channel}: {len(tasks)} clients | Message: {message.get('t', 'unknown')}")
    
    async def _send_fast(self, connection: WebSocket, message: str, channel: str):
        """Ultra-fast single message send"""
        try:
            await connection.send_text(message)
        except Exception:
            # Silent removal for speed - logging happens in disconnect
            if connection in self.subscribers[channel]:
                self.subscribers[channel].remove(connection)
            if connection in self.active_connections:
                self.active_connections.remove(connection)
    
    def subscribe(self, websocket: WebSocket, channel: str):
        if websocket not in self.subscribers[channel]:
            self.subscribers[channel].append(websocket)
        logger.info(f"📡 Subscribed to {channel} | Subscribers: {len(self.subscribers[channel])}")
    
    def unsubscribe(self, websocket: WebSocket, channel: str):
        if websocket in self.subscribers[channel]:
            self.subscribers[channel].remove(websocket)
        logger.info(f"📡 Unsubscribed from {channel} | Subscribers: {len(self.subscribers[channel])}")

# Global connection manager
manager = ConnectionManager()

class OptimizedDeltaWebSocket(DeltaWebSocket):
    """Ultra-fast Delta WebSocket client with thread-safe message queue"""
    
    def __init__(self, manager: ConnectionManager):
        super().__init__()
        self.manager = manager
        self.connected = False
        self.last_heartbeat = time.time()
        self.message_count = 0
        self.message_queue = queue.Queue(maxsize=1000)
        self.processor_running = False
        
    def _on_open(self, ws):
        self.connected = True
        self.last_heartbeat = time.time()
        logger.info("🚀 Delta WebSocket CONNECTED - Live streaming active")
        
        # Start message processor if not running
        if not self.processor_running:
            self.processor_running = True
            threading.Thread(target=self._process_messages, daemon=True).start()
        
        super()._on_open(ws)
    
    def _on_message(self, ws, message):
        try:
            self.message_count += 1
            data = json.loads(message)
            msg_type = data.get("type", "")
            
            if msg_type == "heartbeat":
                self.last_heartbeat = time.time()
                return
            
            # Queue message for async processing
            try:
                self.message_queue.put_nowait(data)
            except queue.Full:
                logger.warning("⚠️ Message queue full, dropping message")
            
            # Minimal logging for performance
            if self.message_count % 100 == 0:
                logger.info(f"📊 Processed {self.message_count} messages | Queue: {self.message_queue.qsize()}")
            
        except Exception as e:
            logger.error(f"💥 Message processing error: {e}")
    
    def _process_messages(self):
        """Thread-safe message processor"""
        logger.info("🔄 Message processor started")
        
        while True:
            try:
                # Get message from queue with timeout
                data = self.message_queue.get(timeout=1.0)
                
                # Create new event loop for this thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    loop.run_until_complete(self._forward_ultra_fast(data))
                finally:
                    loop.close()
                
                self.message_queue.task_done()
                
            except queue.Empty:
                # Check if we should continue running
                if not self.connected:
                    break
                continue
            except Exception as e:
                logger.error(f"💥 Message processor error: {e}")
        
        self.processor_running = False
        logger.info("🛑 Message processor stopped")
    
    async def _forward_ultra_fast(self, data):
        """FIXED: Ultra-fast message forwarding with proper structure"""
        try:
            msg_type = data.get("type", "")
            symbol = data.get("symbol", "")
            
            logger.debug(f"📨 Processing {msg_type} for {symbol}")
            
            # Create optimized message structure for ticker
            if msg_type == "v2/ticker":
                ticker_msg = {
                    "t": "ticker",  # type
                    "s": symbol,    # symbol
                    "p": data.get("price"),
                    "c": data.get("change_24_hour"),
                    "v": data.get("volume"),
                    "ts": data.get("timestamp", int(time.time() * 1000))
                }
                await self.manager.broadcast_fast("ticker", ticker_msg)
                logger.debug(f"💰 Ticker: {symbol} = ${ticker_msg['p']}")
                
            elif msg_type.startswith("candlestick_"):
                resolution = msg_type.replace("candlestick_", "")
                candle_msg = {
                    "t": "candle",  # type
                    "s": symbol,    # symbol
                    "r": resolution, # resolution
                    "o": data.get("open"),
                    "h": data.get("high"),
                    "l": data.get("low"),
                    "c": data.get("close"),
                    "v": data.get("volume"),
                    "ts": data.get("timestamp", int(time.time() * 1000))
                }
                await self.manager.broadcast_fast("candles", candle_msg)
                logger.debug(f"🕯️ Candle: {symbol} {resolution} OHLC({candle_msg['o']},{candle_msg['h']},{candle_msg['l']},{candle_msg['c']})")
                
            elif msg_type == "l2_orderbook":
                orderbook_msg = {
                    "t": "orderbook",  # type
                    "s": symbol,       # symbol
                    "b": data.get("buy", [])[:10],  # Top 10 bids
                    "a": data.get("sell", [])[:10], # Top 10 asks
                    "ts": data.get("timestamp", int(time.time() * 1000))
                }
                await self.manager.broadcast_fast("orderbook", orderbook_msg)
                
            elif msg_type == "all_trades":
                trade_msg = {
                    "t": "trade",   # type
                    "s": symbol,    # symbol
                    "p": data.get("price"),
                    "q": data.get("size"),
                    "side": data.get("buyer_role"),
                    "ts": data.get("timestamp", int(time.time() * 1000))
                }
                await self.manager.broadcast_fast("trades", trade_msg)
                
        except Exception as e:
            logger.error(f"💥 Forward error: {e}")
    
    def _on_error(self, ws, error):
        if "timeout" not in str(error).lower():
            logger.error(f"💥 Delta WebSocket error: {error}")
        self.connected = False
    
    def _on_close(self, ws, code, reason):
        logger.warning(f"📡 Delta WebSocket closed: {code} {reason}")
        self.connected = False

# Request models
class OrderRequest(BaseModel):
    product_id: int
    side: str
    size: int
    order_type: str = "limit_order"
    limit_price: Optional[str] = None
    time_in_force: str = "gtc"
    post_only: bool = False

class SubscribeRequest(BaseModel):
    symbols: List[str]
    resolution: Optional[str] = "1m"

# Global WebSocket client
delta_ws_client = None

async def maintain_ultra_fast_connection():
    """Ultra-fast Delta connection with immediate reconnection"""
    global delta_ws_client
    
    reconnect_count = 0
    
    while True:
        try:
            if not delta_ws_client or not delta_ws_client.connected:
                reconnect_count += 1
                logger.info(f"🔄 Starting Delta connection (attempt #{reconnect_count})")
                
                delta_ws_client = OptimizedDeltaWebSocket(manager)
                delta_ws_client.connect()
                
                # Quick connection check
                await asyncio.sleep(2)
                
                if delta_ws_client and delta_ws_client.connected:
                    # Subscribe to high-volume symbols immediately
                    symbols = ["BTCUSD", "ETHUSD", "SOLUSD", "BNBUSD", "ADAUSD"]
                    
                    try:
                        delta_ws_client.subscribe_ticker(symbols)
                        delta_ws_client.subscribe_candles(symbols, "1m")
                        delta_ws_client.subscribe_orderbook(symbols)
                        delta_ws_client.subscribe_trades(symbols)
                        
                        logger.info(f"✅ Delta connected | Symbols: {len(symbols)} | Reconnects: {reconnect_count}")
                    except Exception as e:
                        logger.error(f"💥 Subscription error: {e}")
                else:
                    logger.warning("⚠️ Delta connection failed")
                
            # Health check every 10 seconds for faster recovery
            await asyncio.sleep(10)
            
        except Exception as e:
            logger.error(f"💥 Connection error: {e}")
            await asyncio.sleep(3)  # Quick retry

@asynccontextmanager
async def lifespan(app: FastAPI):
    """App lifespan with connection monitoring"""
    logger.info(f"🚀 Delta Server starting | ENV: {ENV} | DRY_RUN: {DRY_RUN}")
    
    # Start ultra-fast connection maintenance
    connection_task = asyncio.create_task(maintain_ultra_fast_connection())
    
    yield
    
    logger.info("🛑 Delta Server shutting down")
    connection_task.cancel()
    
    if delta_ws_client:
        try:
            delta_ws_client.disconnect()
        except:
            pass

# FastAPI app
app = FastAPI(
    title="Delta Exchange Ultra-Fast API",
    description="Ultra-fast Delta Exchange API with live streaming",
    version="3.1.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize client
client = DeltaRESTClient()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                msg_type = message.get("type")
                
                if msg_type == "subscribe":
                    channel = message.get("channel")
                    if channel:
                        manager.subscribe(websocket, channel)
                        await manager.send_personal_message({
                            "type": "subscribed", 
                            "channel": channel
                        }, websocket)
                        logger.info(f"📡 Client subscribed to: {channel}")
                
                elif msg_type == "unsubscribe":
                    channel = message.get("channel")
                    if channel:
                        manager.unsubscribe(websocket, channel)
                        logger.info(f"📡 Client unsubscribed from: {channel}")
                
                elif msg_type == "subscribe_symbols":
                    symbols = message.get("symbols", [])
                    if delta_ws_client and delta_ws_client.connected and symbols:
                        try:
                            delta_ws_client.subscribe_ticker(symbols)
                            delta_ws_client.subscribe_candles(symbols, "1m")
                            delta_ws_client.subscribe_orderbook(symbols)
                            logger.info(f"🎯 Subscribed symbols: {symbols}")
                            
                            await manager.send_personal_message({
                                "type": "symbols_subscribed",
                                "symbols": symbols
                            }, websocket)
                        except Exception as e:
                            logger.error(f"💥 Symbol subscription error: {e}")
                
                elif msg_type == "ping":
                    await manager.send_personal_message({
                        "type": "pong",
                        "timestamp": int(time.time() * 1000)
                    }, websocket)
                    logger.debug("🏓 Ping-pong")
                
            except json.JSONDecodeError:
                logger.error("💥 Invalid JSON from client")
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/health")
def health_check():
    """Comprehensive health check"""
    ws_status = "connected" if delta_ws_client and delta_ws_client.connected else "disconnected"
    
    health_data = {
        "status": "healthy",
        "environment": ENV,
        "dry_run": DRY_RUN,
        "websocket_status": ws_status,
        "active_connections": len(manager.active_connections),
        "total_subscribers": sum(len(subs) for subs in manager.subscribers.values()),
        "channels": list(manager.subscribers.keys()),
        "timestamp": int(time.time() * 1000)
    }
    
    if delta_ws_client:
        health_data["delta_messages"] = delta_ws_client.message_count
        health_data["last_heartbeat"] = int(delta_ws_client.last_heartbeat)
        health_data["queue_size"] = delta_ws_client.message_queue.qsize()
    
    logger.info(f"💚 Health: {ws_status} | Connections: {health_data['active_connections']} | Messages: {health_data.get('delta_messages', 0)}")
    return health_data

# Market data endpoints
@app.get("/api/products")
def get_products(page_size: int = Query(200, ge=1, le=500)):
    try:
        result = client.get_products(page_size=page_size)
        logger.info(f"📊 Products fetched: {len(result.get('result', []))}")
        return result
    except Exception as e:
        logger.error(f"💥 Products error: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch products")

@app.get("/api/ticker/{symbol}")
def get_ticker(symbol: str):
    try:
        result = client.get_ticker(symbol)
        price = result.get('result', {}).get('price', 'N/A')
        logger.debug(f"📈 Ticker {symbol}: ${price}")
        return result
    except Exception as e:
        logger.error(f"💥 Ticker {symbol} error: {e}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch ticker for {symbol}")

@app.get("/api/orderbook/{symbol}")
def get_orderbook(symbol: str, depth: int = Query(20, ge=1, le=100)):
    try:
        result = client.get_orderbook(symbol, depth=depth)
        logger.debug(f"📋 Orderbook {symbol}: depth {depth}")
        return result
    except Exception as e:
        logger.error(f"💥 Orderbook {symbol} error: {e}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch orderbook")

# NEW: Candles endpoint for historical data
@app.get("/api/candles/{symbol}")
def get_candles(
    symbol: str,
    resolution: str = Query("1m", description="1m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d"),
    start: Optional[int] = Query(None, description="Start timestamp"),
    end: Optional[int] = Query(None, description="End timestamp")
):
    try:
        # If no start/end provided, get last 24 hours
        if not start or not end:
            end_time = int(time.time())
            start_time = end_time - (24 * 60 * 60)  # 24 hours ago
        else:
            start_time = start
            end_time = end
            
        result = client.get_candles(
            symbol=symbol,
            resolution=resolution,
            start=start_time,
            end=end_time
        )
        
        candles = result.get('result', [])
        logger.info(f"🕯️ Candles {symbol} {resolution}: {len(candles)} candles")
        
        return result
        
    except Exception as e:
        logger.error(f"💥 Candles {symbol} error: {e}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch candles for {symbol}")

# Enhanced balance endpoint
@app.get("/api/balance")
def get_balance():
    try:
        result = client.get_balance()
        balances = result.get("result", [])
        
        # Calculate total portfolio value
        total_value = 0
        available_balance = 0
        margin_used = 0
        
        for balance in balances:
            if balance.get("asset", {}).get("symbol") == "USDT":
                available_balance = float(balance.get("available_balance", 0))
                margin_used = float(balance.get("order_margin", 0)) + float(balance.get("position_margin", 0))
                total_value = float(balance.get("wallet_balance", 0))
        
        logger.info(f"💰 Balance - Total: ${total_value:.2f} | Available: ${available_balance:.2f} | Margin: ${margin_used:.2f}")
        
        return {
            **result,
            "summary": {
                "total_value": total_value,
                "available_balance": available_balance,
                "margin_used": margin_used,
                "free_margin": available_balance - margin_used
            }
        }
    except Exception as e:
        logger.error(f"💥 Balance error: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch balance")

@app.get("/api/positions")
def get_positions(state: Optional[str] = Query(None)):
    try:
        result = client.get_positions(state=state)
        positions = result.get("result", [])
        
        total_pnl = sum(float(pos.get("unrealized_pnl", 0)) for pos in positions)
        
        logger.info(f"📊 Positions: {len(positions)} | Total PnL: ${total_pnl:.2f}")
        return result
    except Exception as e:
        logger.error(f"💥 Positions error: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch positions")

@app.get("/api/orders")
def get_orders(product_id: Optional[int] = Query(None), state: Optional[str] = Query(None)):
    try:
        result = client.get_orders(product_id=product_id, state=state)
        orders = result.get("result", [])
        logger.info(f"📄 Orders fetched: {len(orders)} | State: {state or 'all'}")
        return result
    except Exception as e:
        logger.error(f"💥 Orders error: {e}")
        raise HTTPException(status_code=502, detail="Failed to fetch orders")

@app.post("/api/orders")
def place_order(order: OrderRequest):
    try:
        if order.order_type == "limit_order" and not order.limit_price:
            raise HTTPException(status_code=400, detail="limit_price required for limit orders")
        
        if order.side not in ["buy", "sell"]:
            raise HTTPException(status_code=400, detail="side must be 'buy' or 'sell'")
        
        logger.info(f"🛒 Placing order: {order.side} {order.size} @ {order.limit_price} | Product: {order.product_id}")
        
        result = client.place_order(
            product_id=order.product_id,
            side=order.side,
            size=order.size,
            order_type=order.order_type,
            limit_price=order.limit_price,
            time_in_force=order.time_in_force,
            post_only=order.post_only
        )
        
        if not result.get("dry_run"):
            order_id = result.get('result', {}).get('id', 'unknown')
            logger.info(f"✅ Order placed: {order_id}")
        else:
            logger.info("🔄 DRY RUN - Order simulated")
        
        return result
        
    except Exception as e:
        logger.error(f"💥 Order placement error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Live subscription endpoints
@app.post("/api/subscribe/ticker")
async def subscribe_ticker_api(request: SubscribeRequest):
    try:
        if delta_ws_client and delta_ws_client.connected:
            delta_ws_client.subscribe_ticker(request.symbols)
            logger.info(f"📡 Ticker subscription: {request.symbols}")
            return {"status": "subscribed", "symbols": request.symbols, "type": "ticker"}
        else:
            raise HTTPException(status_code=503, detail="WebSocket not connected")
    except Exception as e:
        logger.error(f"💥 Ticker subscription error: {e}")
        raise HTTPException(status_code=500, detail="Subscription failed")

@app.post("/api/subscribe/candles")
async def subscribe_candles_api(request: SubscribeRequest):
    try:
        if delta_ws_client and delta_ws_client.connected:
            resolution = request.resolution or "1m"
            delta_ws_client.subscribe_candles(request.symbols, resolution)
            logger.info(f"🕯️ Candles subscription: {request.symbols} | {resolution}")
            return {"status": "subscribed", "symbols": request.symbols, "resolution": resolution}
        else:
            raise HTTPException(status_code=503, detail="WebSocket not connected")
    except Exception as e:
        logger.error(f"💥 Candles subscription error: {e}")
        raise HTTPException(status_code=500, detail="Subscription failed")

# Debug endpoint
@app.get("/api/debug/websocket")
def debug_websocket():
    """Debug WebSocket connection status"""
    debug_info = {
        "connected": delta_ws_client.connected if delta_ws_client else False,
        "message_count": delta_ws_client.message_count if delta_ws_client else 0,
        "queue_size": delta_ws_client.message_queue.qsize() if delta_ws_client else 0,
        "last_heartbeat": delta_ws_client.last_heartbeat if delta_ws_client else 0,
        "processor_running": delta_ws_client.processor_running if delta_ws_client else False,
        "active_connections": len(manager.active_connections),
        "subscribers": {k: len(v) for k, v in manager.subscribers.items()},
        "timestamp": int(time.time() * 1000)
    }
    
    logger.info(f"🐛 Debug info requested: {debug_info}")
    return debug_info

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "delta_server:app",
        host="0.0.0.0", 
        port=8080,
        log_level="info",
        access_log=False,  # Disable for performance
        workers=1
    )