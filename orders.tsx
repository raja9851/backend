const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Server } = require('socket.io');
const http = require('http');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;

// WebSocket Configuration
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// Middleware
app.use(cors());
app.use(express.json());

// Upstox API Configuration
const UPSTOX_BASE_URL = 'https://api.upstox.com/v2';
const ACCESS_TOKEN = process.env.UPSTOX_ACCESS_TOKEN; // Store in .env file
const MARKET_DATA_WS_URL = process.env.MARKET_DATA_WS_URL || 'ws://localhost:3001'; // Your existing WebSocket

// Helper function to get headers
const getHeaders = () => ({
  'Authorization': `Bearer ${ACCESS_TOKEN}`,
  'Content-Type': 'application/json',
  'Accept': 'application/json'
});

// Error handler
const handleError = (error, res) => {
  console.error('API Error:', error.response?.data || error.message);
  res.status(error.response?.status || 500).json({
    success: false,
    error: error.response?.data || error.message
  });
};

// WebSocket Connection to your Market Data Server
const { io: ioClient } = require('socket.io-client');
let marketDataSocket = null;
let marketDataCache = {};

// Connect to your existing WebSocket server
const connectToMarketDataServer = () => {
  try {
    marketDataSocket = ioClient(MARKET_DATA_WS_URL, {
      transports: ['websocket', 'polling']
    });

    marketDataSocket.on('connect', () => {
      console.log('🔗 Connected to Market Data WebSocket Server');
      io.emit('marketDataStatus', { status: 'connected', server: MARKET_DATA_WS_URL });
    });

    marketDataSocket.on('disconnect', () => {
      console.log('❌ Disconnected from Market Data Server');
      io.emit('marketDataStatus', { status: 'disconnected' });
    });

    // Forward market data to connected clients
    marketDataSocket.on('marketData', (data) => {
      marketDataCache = { ...marketDataCache, ...data.feeds };
      io.emit('marketData', data);
    });

    marketDataSocket.on('optionData', (data) => {
      io.emit('optionData', data);
    });

    marketDataSocket.on('optionChainData', (data) => {
      io.emit('optionChainData', data);
    });

    marketDataSocket.on('allSymbolsData', (data) => {
      io.emit('allSymbolsData', data);
    });

  } catch (error) {
    console.error('❌ Failed to connect to Market Data Server:', error.message);
  }
};

// WebSocket Events for Trading Backend
io.on('connection', (socket) => {
  console.log('🔗 Trading client connected:', socket.id);
  
  // Send cached market data to new client
  if (Object.keys(marketDataCache).length > 0) {
    socket.emit('marketData', { feeds: marketDataCache });
  }

  // Forward requests to market data server
  socket.on('getOptionChain', (data) => {
    if (marketDataSocket?.connected) {
      marketDataSocket.emit('get_option_chain', data);
    }
  });

  socket.on('getExpiries', (data) => {
    if (marketDataSocket?.connected) {
      marketDataSocket.emit('get_expiries', data);
    }
  });

  socket.on('refreshOptionChain', (data) => {
    if (marketDataSocket?.connected) {
      marketDataSocket.emit('refresh_option_chain', data);
    }
  });

  socket.on('getAllSymbolsData', () => {
    if (marketDataSocket?.connected) {
      marketDataSocket.emit('get_all_symbols_data', {});
    }
  });

  socket.on('disconnect', () => {
    console.log('❌ Trading client disconnected:', socket.id);
  });
});

// ===== ORDER PLACEMENT =====

// Place Order V3 - Single order with all configurations
app.post('/api/v3/orders', async (req, res) => {
  try {
    const orderData = {
      quantity: req.body.quantity,
      product: req.body.product || 'D', // D=Delivery, I=Intraday, M=Margin
      validity: req.body.validity || 'DAY',
      price: req.body.price || 0,
      tag: req.body.tag || 'string',
      instrument_token: req.body.instrument_token,
      order_type: req.body.order_type || 'MARKET', // MARKET, LIMIT, SL, SLM
      transaction_type: req.body.transaction_type, // BUY, SELL
      disclosed_quantity: req.body.disclosed_quantity || 0,
      trigger_price: req.body.trigger_price || 0,
      is_amo: req.body.is_amo || false
    };

    const response = await axios.post(
      `${UPSTOX_BASE_URL}/order/place`,
      orderData,
      { headers: getHeaders() }
    );

    // Emit order placed event via WebSocket
    io.emit('orderPlaced', {
      success: true,
      data: response.data,
      orderData,
      timestamp: new Date().toISOString()
    });

    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// Place Multi Order - Multiple orders at once
app.post('/api/orders/multi', async (req, res) => {
  try {
    const orders = req.body.orders; // Array of order objects
    const results = [];
    
    for (const order of orders) {
      try {
        const response = await axios.post(
          `${UPSTOX_BASE_URL}/order/place`,
          order,
          { headers: getHeaders() }
        );
        results.push({ success: true, data: response.data, order });
      } catch (error) {
        results.push({ success: false, error: error.response?.data, order });
      }
    }

    res.json({ success: true, results });
  } catch (error) {
    handleError(error, res);
  }
});

// Place Order (Legacy) - Single order
app.post('/api/orders', async (req, res) => {
  try {
    const response = await axios.post(
      `${UPSTOX_BASE_URL}/order/place`,
      req.body,
      { headers: getHeaders() }
    );
    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// ===== ORDER MODIFICATION =====

// Modify Order V3
app.put('/api/v3/orders/:orderId', async (req, res) => {
  try {
    const { orderId } = req.params;
    const modifyData = {
      quantity: req.body.quantity,
      validity: req.body.validity,
      price: req.body.price,
      order_type: req.body.order_type,
      disclosed_quantity: req.body.disclosed_quantity,
      trigger_price: req.body.trigger_price
    };

    const response = await axios.put(
      `${UPSTOX_BASE_URL}/order/modify`,
      { order_id: orderId, ...modifyData },
      { headers: getHeaders() }
    );

    // Emit order modified event
    io.emit('orderModified', {
      success: true,
      orderId,
      data: response.data,
      modifyData,
      timestamp: new Date().toISOString()
    });

    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// Modify Order (Legacy)
app.put('/api/orders/:orderId', async (req, res) => {
  try {
    const { orderId } = req.params;
    const response = await axios.put(
      `${UPSTOX_BASE_URL}/order/modify`,
      { order_id: orderId, ...req.body },
      { headers: getHeaders() }
    );
    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// ===== ORDER CANCELLATION =====

// Cancel Order V3
app.delete('/api/v3/orders/:orderId', async (req, res) => {
  try {
    const { orderId } = req.params;
    const response = await axios.delete(
      `${UPSTOX_BASE_URL}/order/cancel`,
      {
        headers: getHeaders(),
        data: { order_id: orderId }
      }
    );

    // Emit order cancelled event
    io.emit('orderCancelled', {
      success: true,
      orderId,
      data: response.data,
      timestamp: new Date().toISOString()
    });

    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// Cancel Order (Legacy)
app.delete('/api/orders/:orderId', async (req, res) => {
  try {
    const { orderId } = req.params;
    const response = await axios.delete(
      `${UPSTOX_BASE_URL}/order/cancel`,
      {
        headers: getHeaders(),
        data: { order_id: orderId }
      }
    );
    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// Cancel Multi Order - Cancel all open orders
app.delete('/api/orders/cancel-all', async (req, res) => {
  try {
    // First get all orders
    const ordersResponse = await axios.get(
      `${UPSTOX_BASE_URL}/order/retrieve-all`,
      { headers: getHeaders() }
    );

    const openOrders = ordersResponse.data.data.filter(
      order => order.status === 'OPEN' || order.status === 'PENDING'
    );

    const results = [];
    for (const order of openOrders) {
      try {
        const cancelResponse = await axios.delete(
          `${UPSTOX_BASE_URL}/order/cancel`,
          {
            headers: getHeaders(),
            data: { order_id: order.order_id }
          }
        );
        results.push({ success: true, orderId: order.order_id, data: cancelResponse.data });
      } catch (error) {
        results.push({ success: false, orderId: order.order_id, error: error.response?.data });
      }
    }

    res.json({ success: true, results, totalCancelled: results.filter(r => r.success).length });
  } catch (error) {
    handleError(error, res);
  }
});

// ===== POSITIONS MANAGEMENT =====

// Exit All Positions
app.delete('/api/positions/exit-all', async (req, res) => {
  try {
    // Get all positions
    const positionsResponse = await axios.get(
      `${UPSTOX_BASE_URL}/portfolio/long-term-positions`,
      { headers: getHeaders() }
    );

    const positions = positionsResponse.data.data;
    const results = [];

    for (const position of positions) {
      if (position.quantity > 0) {
        try {
          const orderData = {
            quantity: Math.abs(position.quantity),
            product: position.product,
            validity: 'DAY',
            price: 0,
            instrument_token: position.instrument_token,
            order_type: 'MARKET',
            transaction_type: position.quantity > 0 ? 'SELL' : 'BUY',
            disclosed_quantity: 0,
            trigger_price: 0,
            is_amo: false
          };

          const response = await axios.post(
            `${UPSTOX_BASE_URL}/order/place`,
            orderData,
            { headers: getHeaders() }
          );
          results.push({ success: true, position: position.instrument_token, data: response.data });
        } catch (error) {
          results.push({ success: false, position: position.instrument_token, error: error.response?.data });
        }
      }
    }

    res.json({ success: true, results });
  } catch (error) {
    handleError(error, res);
  }
});

// ===== ORDER INFORMATION =====

// Get Order Details
app.get('/api/orders/:orderId', async (req, res) => {
  try {
    const { orderId } = req.params;
    const response = await axios.get(
      `${UPSTOX_BASE_URL}/order/details?order_id=${orderId}`,
      { headers: getHeaders() }
    );
    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// Get Order History
app.get('/api/orders/:orderId/history', async (req, res) => {
  try {
    const { orderId } = req.params;
    const response = await axios.get(
      `${UPSTOX_BASE_URL}/order/history?order_id=${orderId}`,
      { headers: getHeaders() }
    );
    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// Get Order Book - All orders for the day
app.get('/api/orders', async (req, res) => {
  try {
    const response = await axios.get(
      `${UPSTOX_BASE_URL}/order/retrieve-all`,
      { headers: getHeaders() }
    );
    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// ===== TRADES INFORMATION =====

// Get Trades - All trades for the day
app.get('/api/trades', async (req, res) => {
  try {
    const response = await axios.get(
      `${UPSTOX_BASE_URL}/order/trades/get-trades-for-day`,
      { headers: getHeaders() }
    );
    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// Get Order Trades - Trades for specific order
app.get('/api/orders/:orderId/trades', async (req, res) => {
  try {
    const { orderId } = req.params;
    const response = await axios.get(
      `${UPSTOX_BASE_URL}/order/trades?order_id=${orderId}`,
      { headers: getHeaders() }
    );
    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// Get Trade History
app.get('/api/trades/history', async (req, res) => {
  try {
    const response = await axios.get(
      `${UPSTOX_BASE_URL}/order/trades/get-trades-by-date`,
      { 
        headers: getHeaders(),
        params: req.query // date parameters
      }
    );
    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// ===== UTILITY ENDPOINTS =====

// ===== MARKET DATA & WEBSOCKET ENDPOINTS =====

// Get market data from WebSocket cache
app.get('/api/market-data', (req, res) => {
  res.json({
    success: true,
    data: marketDataCache,
    connected: marketDataSocket?.connected || false,
    server: MARKET_DATA_WS_URL
  });
});

// Request option chain via WebSocket
app.get('/api/option-chain/:symbol', async (req, res) => {
  const { symbol } = req.params;
  const { expiry } = req.query;
  
  if (marketDataSocket?.connected) {
    marketDataSocket.emit('get_option_chain', { symbol, expiry });
    res.json({
      success: true,
      message: `Option chain request sent for ${symbol}`,
      listen_event: 'optionChainData'
    });
  } else {
    res.status(503).json({
      success: false,
      error: 'Market data server not connected'
    });
  }
});

// Health check
app.get('/api/health', (req, res) => {
  res.json({ 
    success: true, 
    message: 'Upstox Trading API is running', 
    timestamp: new Date().toISOString(),
    websocket: {
      market_data_connected: marketDataSocket?.connected || false,
      clients_connected: io.engine.clientsCount
    }
  });
});

// Get User Profile
app.get('/api/user/profile', async (req, res) => {
  try {
    const response = await axios.get(
      `${UPSTOX_BASE_URL}/user/profile`,
      { headers: getHeaders() }
    );
    res.json({ success: true, data: response.data });
  } catch (error) {
    handleError(error, res);
  }
});

// WebSocket status endpoint
app.get('/api/websocket/status', (req, res) => {
  res.json({
    market_data_server: {
      url: MARKET_DATA_WS_URL,
      connected: marketDataSocket?.connected || false
    },
    trading_clients: io.engine.clientsCount,
    cached_symbols: Object.keys(marketDataCache).length
  });
});

// Connect to market data server on startup
connectToMarketDataServer();

// Start server
server.listen(PORT, () => {
  console.log(`🚀 Upstox Trading Backend running on port ${PORT}`);
  console.log(`📊 API Documentation: http://localhost:${PORT}/api/health`);
  console.log(`🔗 WebSocket Server: ws://localhost:${PORT}`);
  console.log(`📈 Market Data Server: ${MARKET_DATA_WS_URL}`);
});

module.exports = app;