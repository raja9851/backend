# enhanced_mongo_service.py - Store ALL Trading & Market Data
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime, UTC, timedelta
from copy import deepcopy
import logging
import urllib.parse
# import ssl # Not needed anymore if using tlsInsecure

# ---------------- Logging Setup ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- MongoDB Connection - WORKING ----------------
DB_PASSWORD = "Gupta@9851"  # Your actual password
DB_PASSWORD_ENCODED = urllib.parse.quote_plus(DB_PASSWORD)  # URL encode for special chars

# Correct connection string
MONGO_URI = f"mongodb+srv://aartech_ceo:{DB_PASSWORD_ENCODED}@cluster0.bzujt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Global variables for connection
client = None
db = None
collections = {}

def initialize_mongodb():
    """Initialize MongoDB connection with multiple collections"""
    global client, db, collections
    
    try:
        logger.info("🔄 Attempting MongoDB connection...")
        
        # Create client with timeout and specific SSL context
        client = MongoClient(
            MONGO_URI, 
            server_api=ServerApi('1'),
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=10000,
            # Use tlsInsecure for local development
            tlsInsecure=True 
        )
        
        # Test connection
        client.admin.command('ping')
        logger.info("✅ Connected to MongoDB Atlas successfully!")
        
        # Initialize database
        db = client["trading_system_db"]
        
        # Initialize all collections
        collections = {
            "option_chains": db["option_chains"],
            "market_data": db["market_data"],
            "trading_orders": db["trading_orders"],
            "positions": db["positions"],
            "script_executions": db["script_executions"],
            "auto_scripts": db["auto_scripts"],
            "user_actions": db["user_actions"],
            "system_logs": db["system_logs"],
            "funds_history": db["funds_history"],
            "scripts": db["scripts"]
        }
        
        # Create indexes for better performance
        try:
            # Option chains indexes
            collections["option_chains"].create_index([("symbol", 1), ("timestamp", -1)])
            collections["option_chains"].create_index("expiry")
            
            # Market data indexes
            collections["market_data"].create_index([("symbol", 1), ("timestamp", -1)])
            collections["market_data"].create_index("timestamp")
            
            # Trading orders indexes
            collections["trading_orders"].create_index([("symbol", 1), ("timestamp", -1)])
            collections["trading_orders"].create_index("order_id")
            collections["trading_orders"].create_index("status")
            
            # Positions indexes
            collections["positions"].create_index([("symbol", 1), ("timestamp", -1)])
            collections["positions"].create_index("timestamp")
            
            # Script executions indexes
            collections["script_executions"].create_index("timestamp")
            collections["script_executions"].create_index("script_name")
            
            # Auto scripts indexes
            collections["auto_scripts"].create_index("script_id")
            collections["auto_scripts"].create_index([("status", 1), ("timestamp", -1)])
            
            # User actions indexes
            collections["user_actions"].create_index([("action_type", 1), ("timestamp", -1)])
            collections["user_actions"].create_index("user_session")
            
            # System logs indexes
            collections["system_logs"].create_index([("log_level", 1), ("timestamp", -1)])
            
            # Funds history indexes
            collections["funds_history"].create_index("timestamp")
            
            # Script management indexes
            collections["scripts"].create_index("name")
            collections["scripts"].create_index("script_id")
            
            logger.info("✅ All database indexes created successfully")
        except Exception as idx_error:
            logger.warning(f"⚠️ Index creation warning: {idx_error}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ MongoDB connection error: {e}")
        return False

# ============== OPTION CHAIN DATA ==============
def save_option_chain(final_data: dict):
    """Save option chain snapshot"""
    global collections
    
    if not collections or not final_data:
        logger.warning("⚠️ MongoDB not connected or empty data")
        return False

    try:
        data_to_save = deepcopy(final_data)
        data_to_save["saved_timestamp"] = datetime.now(UTC)
        data_to_save["data_type"] = "option_chain"
        
        result = collections["option_chains"].insert_one(data_to_save)
        logger.info(f"💾 Option chain saved - ID: {result.inserted_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving option chain: {e}")
        return False

# ============== MARKET DATA ==============
def save_market_data(market_data: dict, data_type: str = "live_feed"):
    """Save live market data snapshots"""
    try:
        if not collections or not market_data:
            return False
            
        # Prepare data for storage
        data_to_save = {
            "data_type": data_type,
            "timestamp": datetime.now(UTC),
            "server_time": datetime.now().isoformat(),
            "data": deepcopy(market_data),
            "symbols_count": len(market_data) if isinstance(market_data, dict) else 0
        }
        
        result = collections["market_data"].insert_one(data_to_save)
        logger.debug(f"📊 Market data saved - ID: {result.inserted_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving market data: {e}")
        return False

def save_price_update(symbol: str, price_data: dict):
    """Save individual price updates"""
    try:
        if not collections:
            return False
            
        data_to_save = {
            "data_type": "price_update",
            "symbol": symbol,
            "timestamp": datetime.now(UTC),
            "price_data": deepcopy(price_data)
        }
        
        collections["market_data"].insert_one(data_to_save)
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving price update for {symbol}: {e}")
        return False

# ============== TRADING ORDERS ==============
def save_order(order_data: dict, action_type: str = "place"):
    """Save trading order data"""
    try:
        if not collections or not order_data:
            return False
            
        data_to_save = {
            "action_type": action_type,  # place, cancel, modify
            "timestamp": datetime.now(UTC),
            "server_time": datetime.now().isoformat(),
            "order_data": deepcopy(order_data)
        }
        
        # Extract key fields for indexing
        if "symbol" in order_data:
            data_to_save["symbol"] = order_data["symbol"]
        if "order_id" in order_data:
            data_to_save["order_id"] = order_data["order_id"]
        if "status" in order_data:
            data_to_save["status"] = order_data["status"]
            
        result = collections["trading_orders"].insert_one(data_to_save)
        logger.info(f"📝 Order {action_type} saved - ID: {result.inserted_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving order: {e}")
        return False

# ============== POSITIONS ==============
def save_positions(positions_data: list):
    """Save positions snapshot"""
    try:
        if not collections or not positions_data:
            return False
            
        data_to_save = {
            "data_type": "positions_snapshot",
            "timestamp": datetime.now(UTC),
            "server_time": datetime.now().isoformat(),
            "positions": deepcopy(positions_data),
            "positions_count": len(positions_data)
        }
        
        result = collections["positions"].insert_one(data_to_save)
        logger.info(f"💼 Positions saved - {len(positions_data)} positions")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving positions: {e}")
        return False

def save_position_update(position_data: dict, action_type: str = "update"):
    """Save individual position updates"""
    try:
        if not collections:
            return False
            
        data_to_save = {
            "data_type": "position_update",
            "action_type": action_type,
            "timestamp": datetime.now(UTC),
            "position_data": deepcopy(position_data)
        }
        
        if "symbol" in position_data:
            data_to_save["symbol"] = position_data["symbol"]
            
        collections["positions"].insert_one(data_to_save)
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving position update: {e}")
        return False

# ============== SCRIPT EXECUTIONS ==============
def save_script_execution(script_data: dict):
    """Save script execution results"""
    try:
        if not collections or not script_data:
            return False
            
        data_to_save = {
            "timestamp": datetime.now(UTC),
            "server_time": datetime.now().isoformat(),
            "script_data": deepcopy(script_data)
        }
        
        # Extract key fields
        if "name" in script_data:
            data_to_save["script_name"] = script_data["name"]
        if "status" in script_data:
            data_to_save["status"] = script_data["status"]
        if "code" in script_data:
            data_to_save["code_length"] = len(script_data["code"])
            
        result = collections["script_executions"].insert_one(data_to_save)
        logger.info(f"🤖 Script execution saved - ID: {result.inserted_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving script execution: {e}")
        return False

from typing import Optional

def save_auto_script_status(script_id: str, status: str, script_data: Optional[dict] = None):
    """Save auto script status updates"""
    try:
        if not collections:
            return False
            
        data_to_save = {
            "script_id": script_id,
            "status": status,  # started, stopped, error, running
            "timestamp": datetime.now(UTC),
            "server_time": datetime.now().isoformat()
        }
        
        if script_data:
            data_to_save["script_data"] = deepcopy(script_data)
            
        result = collections["auto_scripts"].insert_one(data_to_save)
        logger.info(f"⚡ Auto script status saved - {script_id}: {status}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving auto script status: {e}")
        return False

# ============== USER ACTIONS ==============
from typing import Optional

def save_user_action(action_type: str, action_data: dict, user_session: Optional[str] = None):
    """Save user actions for analytics"""
    try:
        if not collections:
            return False
            
        data_to_save = {
            "action_type": action_type,  # search, get_option_chain, place_order, etc.
            "timestamp": datetime.now(UTC),
            "server_time": datetime.now().isoformat(),
            "action_data": deepcopy(action_data)
        }
        
        if user_session:
            data_to_save["user_session"] = user_session
            
        collections["user_actions"].insert_one(data_to_save)
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving user action: {e}")
        return False

# ============== SYSTEM LOGS ==============
from typing import Optional

def save_system_log(log_level: str, message: str, additional_data: Optional[dict] = None):
    """Save system logs"""
    try:
        if not collections:
            return False
            
        data_to_save = {
            "log_level": log_level,  # INFO, ERROR, WARNING, DEBUG
            "message": message,
            "timestamp": datetime.now(UTC),
            "server_time": datetime.now().isoformat()
        }
        
        if additional_data:
            data_to_save["additional_data"] = deepcopy(additional_data)
            
        collections["system_logs"].insert_one(data_to_save)
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving system log: {e}")
        return False

# ============== FUNDS HISTORY ==============
def save_funds_snapshot(funds_data: dict):
    """Save funds/balance snapshots"""
    try:
        if not collections or not funds_data:
            return False
            
        data_to_save = {
            "data_type": "funds_snapshot",
            "timestamp": datetime.now(UTC),
            "server_time": datetime.now().isoformat(),
            "funds_data": deepcopy(funds_data)
        }
        
        result = collections["funds_history"].insert_one(data_to_save)
        logger.info(f"💰 Funds snapshot saved - ID: {result.inserted_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving funds snapshot: {e}")
        return False

# ============== SCRIPT MANAGEMENT FUNCTIONS (NEW) ==============
def save_script_to_db(script: dict):
    """Save a trading script to the database."""
    try:
        from bson.objectid import ObjectId
        # Check if script_id exists to determine update or insert
        script_id = script.get('id')
        if script_id:
            collections['scripts'].update_one(
                {'_id': ObjectId(script_id)},
                {'$set': {
                    'name': script['name'],
                    'code': script['code'],
                    'description': script.get('description', ''),
                    'schedule': script.get('schedule', {}),
                    'updatedAt': datetime.now(UTC)
                }}
            )
        else:
            collections['scripts'].insert_one({
                'name': script['name'],
                'code': script['code'],
                'description': script.get('description', ''),
                'schedule': script.get('schedule', {}),
                'createdAt': datetime.now(UTC),
                'updatedAt': datetime.now(UTC)
            })
        return {'status': 'success'}
    except Exception as e:
        logger.error(f"❌ Error saving script to db: {e}")
        return {'status': 'error', 'error': str(e)}

def load_script_from_db(script_id: str):
    """Load a specific script from the database by ID."""
    try:
        from bson.objectid import ObjectId
        script = collections['scripts'].find_one({"_id": ObjectId(script_id)})
        return script
    except Exception as e:
        logger.error(f"❌ Error loading script from db: {e}")
        return None

def delete_script_from_db(script_id: str):
    """Delete a script from the database by ID."""
    try:
        from bson.objectid import ObjectId
        result = collections['scripts'].delete_one({"_id": ObjectId(script_id)})
        return {'status': 'success', 'deleted_count': result.deleted_count}
    except Exception as e:
        logger.error(f"❌ Error deleting script from db: {e}")
        return {'status': 'error', 'error': str(e)}

def get_all_scripts():
    """Get all saved scripts from the database."""
    try:
        scripts = list(collections['scripts'].find({}))
        # Convert ObjectId to string for JSON serialization
        for script in scripts:
            script['_id'] = str(script['_id'])
        return scripts
    except Exception as e:
        logger.error(f"❌ Error getting all scripts from db: {e}")
        return []

# ============== DATA RETRIEVAL FUNCTIONS ==============
from typing import Optional

def get_recent_data(collection_name: str, limit: int = 10, filter_dict: Optional[dict] = None):
    """Get recent data from any collection"""
    try:
        if not collections or collection_name not in collections:
            return []
            
        query = filter_dict if filter_dict else {}
        docs = list(collections[collection_name].find(query).sort("timestamp", -1).limit(limit))
        
        # Convert ObjectId to string
        for doc in docs:
            doc["_id"] = str(doc["_id"])
            
        return docs
        
    except Exception as e:
        logger.error(f"❌ Error fetching data from {collection_name}: {e}")
        return []

def get_trading_summary(hours: int = 24):
    """Get trading summary for last N hours"""
    try:
        if not collections:
            return {}
            
        cutoff_time = datetime.now(UTC) - timedelta(hours=hours)
        
        # Get counts from different collections
        summary = {
            "period_hours": hours,
            "option_chains_fetched": collections["option_chains"].count_documents({"timestamp": {"$gte": cutoff_time}}),
            "orders_placed": collections["trading_orders"].count_documents({"timestamp": {"$gte": cutoff_time}}),
            "script_executions": collections["script_executions"].count_documents({"timestamp": {"$gte": cutoff_time}}),
            "user_actions": collections["user_actions"].count_documents({"timestamp": {"$gte": cutoff_time}}),
            "market_data_updates": collections["market_data"].count_documents({"timestamp": {"$gte": cutoff_time}})
        }
        
        return summary
        
    except Exception as e:
        logger.error(f"❌ Error generating trading summary: {e}")
        return {}

def get_database_stats():
    """Get comprehensive database statistics"""
    try:
        if not db or not collections:
            return {"error": "Database not connected"}
            
        stats = {
            "database": "trading_system_db",
            "collections": {},
            "total_documents": 0
        }
        
        for collection_name, collection in collections.items():
            count = collection.count_documents({})
            stats["collections"][collection_name] = count
            stats["total_documents"] += count
            
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error getting database stats: {e}")
        return {"error": str(e)}

def get_connection_status():
    """Get MongoDB connection status"""
    global client, collections
    try:
        if client and collections:
            client.admin.command('ping')
            stats = get_database_stats()
            return {
                "status": "connected", 
                "message": "MongoDB connected successfully",
                "database": "trading_system_db",
                "collections": list(collections.keys()),
                "total_documents": stats.get("total_documents", 0)
            }
        else:
            return {"status": "disconnected", "message": "MongoDB not initialized"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ============== CLEANUP FUNCTIONS ==============
def cleanup_old_data(days: int = 30):
    """Clean up data older than N days"""
    try:
        if not collections:
            return False
            
        cutoff_time = datetime.now(UTC) - timedelta(days=days)
        
        cleanup_stats = {}
        for collection_name, collection in collections.items():
            # Skip option_chains and script_executions (keep for analysis)
            if collection_name in ["option_chains", "script_executions"]:
                continue
                
            result = collection.delete_many({"timestamp": {"$lt": cutoff_time}})
            cleanup_stats[collection_name] = result.deleted_count
            
        logger.info(f"🧹 Cleanup completed: {cleanup_stats}")
        return cleanup_stats
        
    except Exception as e:
        logger.error(f"❌ Error during cleanup: {e}")
        return False

# Initialize connection on import
mongodb_connected = initialize_mongodb()

if mongodb_connected:
    logger.info("🌟 Enhanced MongoDB service ready with all collections!")
else:
    logger.error("❌ MongoDB connection failed on startup")
    logger.info("📝 Server will continue without MongoDB - data won't be saved")