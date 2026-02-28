import pymongo
from pymongo.errors import ServerSelectionTimeoutError
import urllib.parse
import logging
from datetime import datetime, timedelta
import time

# ---------------- Logging Setup ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- MongoDB Connection - WORKING ----------------
DB_PASSWORD = "Gupta@9851"  # Your actual password
DB_PASSWORD_ENCODED = urllib.parse.quote_plus(DB_PASSWORD)  # URL encode for special chars

# Correct connection string
MONGO_URI = f"mongodb+srv://aartech_ceo:{DB_PASSWORD_ENCODED}@cluster0.bzujt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

class DeltaExchangeDB:
    def __init__(self):
        self.client = None
        self.db = None
        self.connect()
    
    def connect(self):
        """MongoDB connection establish karta hai."""
        try:
            self.client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            # Test the connection
            self.client.admin.command('ismaster')
            self.db = self.client['delta_exchange']
            logger.info("✅ MongoDB connection established successfully")
            
            # Create indexes for better performance
            self.create_indexes()
            
        except ServerSelectionTimeoutError as e:
            logger.error(f"❌ MongoDB connection timeout: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ MongoDB connection error: {e}")
            raise
    
    def create_indexes(self):
        """Important collections ke liye indexes create karta hai."""
        try:
            if not self.db:
                logger.error("Database connection not established")
                return
                
            # Market data indexes
            self.db.tickers.create_index([("symbol", 1), ("timestamp", -1)])
            self.db.candles.create_index([("product_id", 1), ("resolution", 1), ("timestamp", -1)])
            self.db.orderbooks.create_index([("product_id", 1), ("timestamp", -1)])
            self.db.trades.create_index([("symbol", 1), ("timestamp", -1)])
            
            # Trading indexes
            self.db.order_executions.create_index([("product_id", 1), ("timestamp", -1)])
            self.db.positions.create_index([("product_id", 1), ("timestamp", -1)])
            self.db.wallet_balances.create_index([("asset_id", 1), ("timestamp", -1)])
            
            # WebSocket data indexes
            self.db.websocket_data.create_index([("channel", 1), ("symbol", 1), ("timestamp", -1)])
            
            # TTL index for cleaning old data (30 days)
            self.db.websocket_data.create_index([("timestamp", 1)], expireAfterSeconds=2592000)  # 30 days
            
            logger.info("✅ Database indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
    
    def get_collection(self, collection_name):
        """Collection object return karta hai."""
        if self.db:
            return self.db[collection_name]
        else:
            logger.error("Database not connected")
            return None
    
    def close_connection(self):
        """MongoDB connection close karta hai."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

# Global database instance
db_instance = DeltaExchangeDB()

def save_to_mongodb(collection_name, data):
    """Data ko MongoDB mein save karta hai."""
    try:
        collection = db_instance.get_collection(collection_name)
        if collection is not None:
            # Add metadata
            data['created_at'] = datetime.utcnow()
            data['timestamp'] = time.time()
            
            result = collection.insert_one(data)
            logger.debug(f"Data saved to {collection_name}: {result.inserted_id}")
            return result.inserted_id
        else:
            logger.error(f"Failed to get collection: {collection_name}")
            return None
    except Exception as e:
        logger.error(f"Error saving to MongoDB {collection_name}: {e}")
        return None

def get_from_mongodb(collection_name, filter_query=None, limit=100, sort_field="timestamp", sort_order=-1):
    """MongoDB se data retrieve karta hai."""
    try:
        collection = db_instance.get_collection(collection_name)
        if collection is not None:
            if filter_query is None:
                filter_query = {}
            
            cursor = collection.find(filter_query).sort(sort_field, sort_order).limit(limit)
            data = list(cursor)
            
            # Convert ObjectId to string for JSON serialization
            for doc in data:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            return data
        else:
            logger.error(f"Failed to get collection: {collection_name}")
            return []
    except Exception as e:
        logger.error(f"Error retrieving from MongoDB {collection_name}: {e}")
        return []

def update_in_mongodb(collection_name, filter_query, update_data):
    """MongoDB mein data update karta hai."""
    try:
        collection = db_instance.get_collection(collection_name)
        if collection is not None:
            update_data['updated_at'] = datetime.utcnow()
            result = collection.update_one(filter_query, {"$set": update_data})
            logger.debug(f"Updated {result.modified_count} documents in {collection_name}")
            return result.modified_count
        else:
            logger.error(f"Failed to get collection: {collection_name}")
            return 0
    except Exception as e:
        logger.error(f"Error updating in MongoDB {collection_name}: {e}")
        return 0

def delete_from_mongodb(collection_name, filter_query):
    """MongoDB se data delete karta hai."""
    try:
        collection = db_instance.get_collection(collection_name)
        if collection is not None:
            result = collection.delete_many(filter_query)
            logger.debug(f"Deleted {result.deleted_count} documents from {collection_name}")
            return result.deleted_count
        else:
            logger.error(f"Failed to get collection: {collection_name}")
            return 0
    except Exception as e:
        logger.error(f"Error deleting from MongoDB {collection_name}: {e}")
        return 0

def get_latest_ticker(symbol):
    """Latest ticker data for a symbol."""
    filter_query = {"symbol": symbol, "type": "ticker"}
    data = get_from_mongodb("tickers", filter_query, limit=1)
    return data[0] if data else None

def get_latest_candles(product_id, resolution="1h", limit=100):
    """Latest candle data for a product."""
    filter_query = {"product_id": product_id, "resolution": resolution, "type": "historical_candles"}
    data = get_from_mongodb("candles", filter_query, limit=limit)
    return data

def get_recent_trades(symbol, limit=50):
    """Recent trades for a symbol."""
    filter_query = {"symbol": symbol, "type": "recent_trades"}
    data = get_from_mongodb("trades", filter_query, limit=limit)
    return data

def get_user_orders(user_id, limit=50):
    """User ke recent orders."""
    filter_query = {"type": "order_placed"}
    data = get_from_mongodb("order_executions", filter_query, limit=limit)
    return data

def get_user_positions():
    """User ki current positions."""
    filter_query = {"type": "position"}
    data = get_from_mongodb("positions", filter_query, limit=20)
    return data

def get_wallet_balance():
    """Current wallet balance."""
    filter_query = {"type": "wallet_balance"}
    data = get_from_mongodb("wallet_balances", filter_query, limit=1)
    return data[0] if data else None

def save_websocket_data(channel, symbol, data):
    """WebSocket data save karta hai."""
    websocket_data = {
        'channel': channel,
        'symbol': symbol,
        'data': data,
        'timestamp': time.time(),
        'type': 'websocket_data'
    }
    return save_to_mongodb('websocket_data', websocket_data)

def get_websocket_data(channel, symbol=None, limit=100):
    """WebSocket data retrieve karta hai."""
    filter_query = {"channel": channel}
    if symbol:
        filter_query["symbol"] = symbol
    
    return get_from_mongodb("websocket_data", filter_query, limit=limit)

def cleanup_old_data(days=30):
    """Purana data cleanup karta hai."""
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        collections_to_cleanup = [
            'websocket_data', 'tickers', 'orderbooks', 'trades',
            'api_responses'  # Keep trading data for longer
        ]
        
        total_deleted = 0
        for collection_name in collections_to_cleanup:
            filter_query = {"created_at": {"$lt": cutoff_date}}
            deleted_count = delete_from_mongodb(collection_name, filter_query)
            total_deleted += deleted_count
            logger.info(f"Cleaned up {deleted_count} old records from {collection_name}")
        
        logger.info(f"Total cleaned up: {total_deleted} records older than {days} days")
        return total_deleted
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return 0

def get_portfolio_summary():
    """Complete portfolio summary."""
    try:
        summary = {}
        
        # Latest wallet balance
        balance = get_wallet_balance()
        summary['wallet_balance'] = balance
        
        # All positions
        positions = get_user_positions()
        summary['positions'] = positions
        
        # Recent orders
        orders = get_user_orders(None, 20)
        summary['recent_orders'] = orders
        
        # Trading statistics
        stats_filter = {"type": "order_placed"}
        collection = db_instance.get_collection("order_executions")
        total_orders = collection.count_documents(stats_filter) if collection else 0
        summary['total_orders'] = total_orders
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting portfolio summary: {e}")
        return {}

def get_market_overview():
    """Market overview data."""
    try:
        overview = {}
        
        # Latest tickers for major symbols
        major_symbols = ['BTCUSD', 'ETHUSD', 'BNBUSD', 'SOLUSD']
        overview['major_tickers'] = {}
        
        for symbol in major_symbols:
            ticker = get_latest_ticker(symbol)
            if ticker:
                overview['major_tickers'][symbol] = ticker
        
        # Recent market stats
        stats_filter = {"type": "24hr_stats"}
        market_stats = get_from_mongodb("stats", stats_filter, limit=1)
        overview['market_stats'] = market_stats[0] if market_stats else None
        
        return overview
        
    except Exception as e:
        logger.error(f"Error getting market overview: {e}")
        return {}

def backup_important_data():
    """Important data ka backup create karta hai."""
    try:
        backup_collections = [
            'order_executions', 'positions', 'wallet_balances',
            'fills_history', 'order_history'
        ]
        
        backup_data = {}
        for collection_name in backup_collections:
            data = get_from_mongodb(collection_name, limit=1000)
            backup_data[collection_name] = data
        
        # Save backup with timestamp
        backup_doc = {
            'backup_data': backup_data,
            'backup_timestamp': datetime.utcnow(),
            'type': 'data_backup'
        }
        
        result = save_to_mongodb('backups', backup_doc)
        logger.info(f"Data backup created with ID: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        return None