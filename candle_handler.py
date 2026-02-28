"""
candle_handler.py
- Receives live ticks and aggregates them into OHLC candles.
"""
import asyncio
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class CandleHandler:
    def __init__(self, sio, interval_minutes=1):
        self.sio = sio
        self.interval = timedelta(minutes=interval_minutes)
        self.candles = {}  # Stores ongoing candle data for each instrument
        self.last_emit_time = {}

    def process_tick(self, instrument_key, ltp):
        """Processes a single price tick for a given instrument."""
        now = datetime.now()
        
        if instrument_key not in self.candles:
            # Start a new candle
            self.candles[instrument_key] = {
                "o": ltp, "h": ltp, "l": ltp, "c": ltp,
                "timestamp": now.replace(second=0, microsecond=0)
            }
            self.last_emit_time[instrument_key] = self.candles[instrument_key]["timestamp"]
            return

        candle = self.candles[instrument_key]
        
        # Update current candle
        candle["h"] = max(candle["h"], ltp)
        candle["l"] = min(candle["l"], ltp)
        candle["c"] = ltp
        
        # Check if the interval has passed to finalize and emit the candle
        if now.replace(second=0, microsecond=0) >= (self.last_emit_time[instrument_key] + self.interval):
            # Finalize the current candle
            final_candle = {
                "instrument_key": instrument_key,
                "o": candle["o"], "h": candle["h"], "l": candle["l"], "c": candle["c"],
                "time": self.last_emit_time[instrument_key].isoformat()
            }
            
            # Emit the completed candle
            asyncio.create_task(self.emit_candle(final_candle))
            
            # Start a new candle for the new interval
            self.candles[instrument_key] = {
                "o": ltp, "h": ltp, "l": ltp, "c": ltp,
                "timestamp": now.replace(second=0, microsecond=0)
            }
            self.last_emit_time[instrument_key] = self.candles[instrument_key]["timestamp"]

    async def emit_candle(self, candle_data):
        """Emits a candle to all connected clients."""
        logger.info(f"Emitting 1-Min Candle for {candle_data['instrument_key']}: O:{candle_data['o']}, H:{candle_data['h']}, L:{candle_data['l']}, C:{candle_data['c']}")
        await self.sio.emit("liveCandleData", candle_data)