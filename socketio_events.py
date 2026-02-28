"""
Socket.IO events for trading, option chain, analytics, and script management
"""

# Import necessary modules and services
from mongo_service import (
    save_option_chain, save_order, save_positions, 
    save_script_execution, save_auto_script_status, save_user_action,
    save_system_log, save_funds_snapshot,
    get_recent_data, get_trading_summary, get_database_stats
)
from datetime import datetime

def register_socketio_events(sio, instrument_manager, market_data_handler, option_chain_handler, trading_system, logger):
    # ============== SOCKET.IO EVENTS ==============

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
        
        # --- CHANGE: Send a simpler response that is easier for the frontend to use ---
        # Instead of a complex object, just send the array of results.
        await sio.emit("instrumentSearchResults", results[:50], room=sid)


    # ============== TRADING EVENTS ==============

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

    # ============== OPTION CHAIN EVENTS ==============

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

    # ============== SCRIPT EVENTS ==============

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

    # ============== ANALYTICS EVENTS ==============

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
            
    # ============== SCRIPT MANAGEMENT EVENTS ==============

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
    async def load_scripts(sid, data=None):
        """Load all scripts for the frontend."""
        try:
            result = trading_system.get_all_scripts()
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