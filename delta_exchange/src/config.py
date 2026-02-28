# Delta Exchange Configuration

# API Configuration
API_KEY = "3LvCvYAFzAaxjEFhB0PfTabM7GMuc8"  # Replace with your actual API key
API_SECRET = "ARIX8ERFGb9h98zqHR2fFnMfA2rJkNaVoZnLfbxN6EWRry7gJsTJBVh1yxrF"  # Replace with your actual API secret
BASE_URL = "https://api.india.delta.exchange"
WS_URL = "wss://production-esocket.delta.exchange"  # Updated WebSocket URL

# Example Product/Symbol Configuration
EXAMPLE_PRODUCT_ID = 27  # BTC-USDT Perpetual
EXAMPLE_SYMBOL = "BTCUSDT"
EXAMPLE_ASSET_ID = 1  # USDT

# Rate Limiting
MAX_REQUESTS_PER_WINDOW = 10000
RATE_LIMIT_WINDOW_SECONDS = 300

# WebSocket Configuration
ENABLE_WEBSOCKET = True
WS_AUTO_RECONNECT = True
WS_RECONNECT_INTERVAL = 5
WS_RECONNECT_INTERVAL = 5  # seconds
WS_PING_INTERVAL = 30  # seconds

# Server Configuration
SERVER_HOST = "localhost"
SERVER_PORT = 8000

# Enable/Disable features
ENABLE_WEBSOCKET = True
ENABLE_DATABASE = True
ENABLE_API_SERVER = True