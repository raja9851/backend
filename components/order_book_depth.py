# order_book_depth.py
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests

logger = logging.getLogger(__name__)


class OrderBookHandler:
    """
    Market depth/order book handler for Upstox v2.
    Provides:
      - Depth (bids/asks) with top 5 levels
      - Analytics (spread, buy/sell pressure, sentiment)
      - Cached depth (30s validity)
      - Market status check (Nifty index proxy)
    """

    def __init__(self, access_token: Optional[str] = None, base_url: str = "https://api.upstox.com/v2"):
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token
        self._session = requests.Session()
        self._cache: Dict[str, Dict[str, Any]] = {}

    # ---------------- Auth & HTTP ----------------

    def set_access_token(self, token: str) -> None:
        self.access_token = token

    def _require_token(self) -> None:
        if not self.access_token:
            raise RuntimeError("Access token is not set. Call set_access_token(token).")

    def _headers(self) -> Dict[str, str]:
        self._require_token()
        return {"Authorization": f"Bearer {self.access_token}", "Accept": "application/json"}

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, timeout: float = 15.0) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self._session.get(url, headers=self._headers(), params=params, timeout=timeout)
            resp.raise_for_status()
            try:
                payload = resp.json()
            except ValueError:
                payload = {"raw": resp.text}
            return {"status": "success", "data": payload}
        except requests.HTTPError as http_err:
            code = getattr(http_err.response, "status_code", None)
            text = getattr(http_err.response, "text", "")
            logger.error("OrderBook API HTTPError %s: %s %s", code, url, text)
            return {"status": "error", "message": f"API Error {code}", "error": text}
        except requests.RequestException as req_err:
            logger.error("OrderBook API RequestException: %s (%s)", req_err, url)
            return {"status": "error", "message": str(req_err)}
        except Exception as e:
            logger.exception("Unexpected error during OrderBook API request")
            return {"status": "error", "message": str(e)}

    # ---------------- Depth ----------------

    def get_market_depth(self, instrument_key: str) -> Dict[str, Any]:
        """Fetch market depth for an instrument_key."""
        try:
            if not instrument_key:
                return {"status": "error", "message": "instrument_key is required."}

            params = {"instrument_key": instrument_key}
            res = self._get("/market-quote/depth", params=params)
            if res.get("status") != "success":
                return res

            depth_raw = res["data"].get("data", {})
            formatted = self._format_depth(depth_raw, instrument_key)
            self._cache[instrument_key] = {"data": formatted, "timestamp": datetime.now()}
            return formatted
        except Exception as e:
            logger.exception("Error fetching market depth")
            return {"status": "error", "message": str(e)}

    def _format_depth(self, depth_data: Dict[str, Any], instrument_key: str) -> Dict[str, Any]:
        """Format and normalize depth response."""
        try:
            inst_data = depth_data.get(instrument_key, {})
            bids = (inst_data.get("depth") or {}).get("buy", [])
            asks = (inst_data.get("depth") or {}).get("sell", [])
            ltp = inst_data.get("last_price", 0)
            vol = inst_data.get("volume", 0)

            analytics = self._depth_analytics(bids, asks, ltp)

            return {
                "status": "success",
                "instrument_key": instrument_key,
                "last_price": ltp,
                "volume": vol,
                "timestamp": datetime.now().isoformat(),
                "bids": [
                    {"price": b.get("price", 0), "quantity": b.get("quantity", 0), "orders": b.get("orders", 0)}
                    for b in bids[:5]
                ],
                "asks": [
                    {"price": a.get("price", 0), "quantity": a.get("quantity", 0), "orders": a.get("orders", 0)}
                    for a in asks[:5]
                ],
                "analytics": analytics,
            }
        except Exception as e:
            logger.exception("Error formatting depth data")
            return {"status": "error", "message": str(e)}

    def _depth_analytics(self, bids: List[Dict[str, Any]], asks: List[Dict[str, Any]], ltp: float) -> Dict[str, Any]:
        """Calculate spread, pressure, and sentiment from bids/asks."""
        try:
            if not bids or not asks:
                return {}

            best_bid = bids[0].get("price", 0)
            best_ask = asks[0].get("price", 0)
            spread = best_ask - best_bid
            spread_pct = (spread / ltp * 100) if ltp > 0 else 0

            tot_bid_qty = sum(b.get("quantity", 0) for b in bids)
            tot_ask_qty = sum(a.get("quantity", 0) for a in asks)

            buy_pressure = (tot_bid_qty / (tot_bid_qty + tot_ask_qty) * 100) if (tot_bid_qty + tot_ask_qty) > 0 else 50
            sell_pressure = 100 - buy_pressure

            if buy_pressure > 60:
                sentiment = "Bullish"
            elif buy_pressure < 40:
                sentiment = "Bearish"
            else:
                sentiment = "Neutral"

            return {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": round(spread, 2),
                "spread_percent": round(spread_pct, 4),
                "total_bid_quantity": tot_bid_qty,
                "total_ask_quantity": tot_ask_qty,
                "buy_pressure": round(buy_pressure, 2),
                "sell_pressure": round(sell_pressure, 2),
                "market_sentiment": sentiment,
            }
        except Exception as e:
            logger.exception("Error calculating depth analytics")
            return {}

    # ---------------- Utilities ----------------

    def get_multiple_depths(self, instrument_keys: List[str]) -> Dict[str, Any]:
        """Fetch depth for multiple instruments."""
        try:
            results = {}
            for ik in instrument_keys or []:
                results[ik] = self.get_market_depth(ik)
            return {
                "status": "success",
                "data": results,
                "instruments_count": len(instrument_keys or []),
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.exception("Error fetching multiple depths")
            return {"status": "error", "message": str(e)}

    def get_cached_depth(self, instrument_key: str) -> Optional[Dict[str, Any]]:
        """Return cached depth if not older than 30s."""
        cached = self._cache.get(instrument_key)
        if cached:
            age = (datetime.now() - cached["timestamp"]).total_seconds()
            if age < 30:
                return cached["data"]
        return None
    
    # FIXED: Add missing live feed methods to prevent crashes.
    # A full WebSocket implementation is out of scope, so they return a 'not implemented' message.
    def subscribe_live_feed(self, instruments: List[str]) -> Dict[str, Any]:
        logger.warning("subscribe_live_feed is not implemented. Returning placeholder.")
        return {
            "status": "error",
            "message": "Live feed subscription is not implemented in this version.",
            "subscribed_instruments": instruments,
        }

    def unsubscribe_live_feed(self, instruments: List[str]) -> Dict[str, Any]:
        logger.warning("unsubscribe_live_feed is not implemented. Returning placeholder.")
        return {
            "status": "error",
            "message": "Live feed unsubscription is not implemented in this version.",
            "unsubscribed_instruments": instruments,
        }

    def get_market_status(self) -> Dict[str, Any]:
        """Check if market is open (using Nifty index LTP)."""
        try:
            res = self._get("/market-quote/ltp", {"instrument_key": "NSE_INDEX|Nifty 50"})
            if res.get("status") != "success":
                return res
            data = res["data"].get("data", {})
            nifty_ltp = 0
            if data and isinstance(data, dict):
                first_val = list(data.values())[0]
                nifty_ltp = first_val.get("last_price", 0)

            return {
                "status": "success",
                "market_open": self._is_market_open(),
                "nifty_ltp": nifty_ltp,
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.exception("Error checking market status")
            return {"status": "error", "message": str(e)}

    def _is_market_open(self) -> bool:
        now = datetime.now()
        if now.weekday() >= 5:  # Sat=5, Sun=6
            return False
        open_t = now.replace(hour=9, minute=15, second=0, microsecond=0)
        close_t = now.replace(hour=15, minute=30, second=0, microsecond=0)
        return open_t <= now <= close_t


# Global instance
order_book_handler = OrderBookHandler()