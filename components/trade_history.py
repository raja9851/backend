# trade_history.py
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests
import pandas as pd

logger = logging.getLogger(__name__)


class TradeHistoryHandler:
    """
    Trade history handler for Upstox v2.
    Provides:
      - Trade list with filters
      - Trade analytics (PnL, win rate, top instruments)
      - Pandas DataFrame output
    """

    def __init__(self, access_token: Optional[str] = None, base_url: str = "https://api.upstox.com/v2"):
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token
        self._session = requests.Session()

    # ---------------- Auth & HTTP ----------------

    def set_access_token(self, token: str) -> None:
        self.access_token = token

    def _require_token(self) -> None:
        if not self.access_token:
            raise RuntimeError("Access token not set. Call set_access_token(token).")

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
            logger.error("TradeHistory API HTTPError %s: %s %s", code, url, text)
            return {"status": "error", "message": f"API Error {code}", "error": text}
        except requests.RequestException as req_err:
            logger.error("TradeHistory API RequestException: %s (%s)", req_err, url)
            return {"status": "error", "message": str(req_err)}
        except Exception as e:
            logger.exception("Unexpected error during TradeHistory API request")
            return {"status": "error", "message": str(e)}

    # ---------------- Trades ----------------

    def get_trade_history(
        self,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        segment: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch trades, optionally filtered by date or segment."""
        try:
            params: Dict[str, Any] = {}
            if from_date:
                params["from_date"] = from_date
            if to_date:
                params["to_date"] = to_date
            if segment:
                params["segment"] = segment

            res = self._get("/order/trades", params=params)
            if res.get("status") != "success":
                return res

            trades = res["data"].get("data", []) if isinstance(res["data"], dict) else []
            return {"status": "success", "trades": trades, "count": len(trades)}
        except Exception as e:
            logger.exception("Error fetching trade history")
            return {"status": "error", "message": str(e)}

    # ---------------- Analytics ----------------

    def get_trade_analytics(self, from_date: Optional[str] = None, to_date: Optional[str] = None) -> Dict[str, Any]:
        """Analyze trades: PnL, win rate, top instruments."""
        try:
            res = self.get_trade_history(from_date, to_date)
            if res.get("status") != "success":
                return res
            trades = res["trades"]

            total = len(trades)
            if total == 0:
                return {"status": "success", "analytics": {"total_trades": 0}}

            pnl = 0.0
            wins = 0
            losses = 0
            instr_stats: Dict[str, Dict[str, Any]] = {}

            for t in trades:
                qty = t.get("quantity", 0)
                price = t.get("trade_price", 0.0)
                side = t.get("transaction_type", "BUY")
                ik = t.get("instrument_token") or t.get("instrument_key", "Unknown")

                if side == "SELL":
                    pnl += qty * price
                else:
                    pnl -= qty * price

                if "pnl" in t and t["pnl"] > 0:
                    wins += 1
                elif "pnl" in t and t["pnl"] < 0:
                    losses += 1

                if ik not in instr_stats:
                    instr_stats[ik] = {"trades": 0, "qty": 0}
                instr_stats[ik]["trades"] += 1
                instr_stats[ik]["qty"] += qty

            win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
            top_instr = sorted(instr_stats.items(), key=lambda x: x[1]["trades"], reverse=True)[:5]

            return {
                "status": "success",
                "analytics": {
                    "total_trades": total,
                    "gross_pnl": round(pnl, 2),
                    "win_rate_percent": round(win_rate, 2),
                    "winning_trades": wins,
                    "losing_trades": losses,
                    "top_instruments": top_instr,
                },
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.exception("Error analyzing trades")
            return {"status": "error", "message": str(e)}

    # ---------------- DataFrame ----------------

    def get_trades_dataframe(self, from_date: Optional[str] = None, to_date: Optional[str] = None) -> pd.DataFrame:
        """Return trades as DataFrame."""
        try:
            res = self.get_trade_history(from_date, to_date)
            if res.get("status") != "success":
                return pd.DataFrame()
            trades = res["trades"]
            df = pd.DataFrame(trades)
            if not df.empty:
                if "trade_time" in df.columns:
                    df["trade_time"] = pd.to_datetime(df["trade_time"])
                elif "timestamp" in df.columns:
                    df["trade_time"] = pd.to_datetime(df["timestamp"])
            return df
        except Exception as e:
            logger.exception("Error creating trades DataFrame")
            return pd.DataFrame()


# Global instance
trade_history_handler = TradeHistoryHandler()
