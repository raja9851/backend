# portfolio_analytics.py
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests
import pandas as pd

logger = logging.getLogger(__name__)


class PortfolioAnalytics:
    """
    Portfolio analytics wrapper using Upstox v2 API.
    Provides:
      - Holdings and positions fetch
      - Portfolio summary (value, PnL, allocation)
      - Risk & diversification analysis
      - Pandas DataFrame conversion
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
            logger.error("Portfolio API HTTPError %s: %s %s", code, url, text)
            return {"status": "error", "message": f"API Error {code}", "error": text}
        except requests.RequestException as req_err:
            logger.error("Portfolio API RequestException: %s (%s)", req_err, url)
            return {"status": "error", "message": str(req_err)}
        except Exception as e:
            logger.exception("Unexpected error during Portfolio API request")
            return {"status": "error", "message": str(e)}

    # ---------------- Holdings & Positions ----------------

    def get_holdings(self) -> Dict[str, Any]:
        """Fetch holdings from API."""
        res = self._get("/portfolio/long-term-holdings")
        if res.get("status") != "success":
            return res
        holdings = res["data"].get("data", []) if isinstance(res["data"], dict) else []
        return {"status": "success", "holdings": holdings, "count": len(holdings)}

    def get_positions(self) -> Dict[str, Any]:
        """Fetch intraday/delivery positions."""
        res = self._get("/portfolio/positions")
        if res.get("status") != "success":
            return res
        positions = res["data"].get("data", []) if isinstance(res["data"], dict) else []
        return {"status": "success", "positions": positions, "count": len(positions)}

    # ---------------- Analytics ----------------

    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Summarize holdings + positions with PnL and allocation."""
        try:
            holdings_res = self.get_holdings()
            positions_res = self.get_positions()
            if holdings_res.get("status") != "success" or positions_res.get("status") != "success":
                return {"status": "error", "message": "Failed to fetch holdings or positions"}

            holdings = holdings_res["holdings"]
            positions = positions_res["positions"]

            total_invested = 0.0
            current_value = 0.0
            pnl = 0.0
            allocation: Dict[str, float] = {}

            for h in holdings:
                qty = h.get("quantity", 0)
                avg = h.get("average_price", 0.0)
                ltp = h.get("last_price", 0.0)
                invested = qty * avg
                value = qty * ltp
                total_invested += invested
                current_value += value
                pnl += (ltp - avg) * qty
                sec = h.get("sector", "Unknown")
                allocation[sec] = allocation.get(sec, 0) + value

            for p in positions:
                qty = p.get("quantity", 0)
                avg = p.get("average_price", 0.0)
                ltp = p.get("last_price", 0.0)
                invested = qty * avg
                value = qty * ltp
                total_invested += invested
                current_value += value
                pnl += (ltp - avg) * qty
                sec = p.get("sector", "Unknown")
                allocation[sec] = allocation.get(sec, 0) + value

            alloc_pct = {s: round(v / current_value * 100, 2) for s, v in allocation.items() if current_value > 0}
            overall_ret = ((current_value - total_invested) / total_invested * 100) if total_invested > 0 else 0

            return {
                "status": "success",
                "summary": {
                    "total_invested": round(total_invested, 2),
                    "current_value": round(current_value, 2),
                    "total_pnl": round(pnl, 2),
                    "overall_return_percent": round(overall_ret, 2),
                    "sector_allocation": alloc_pct,
                    "holdings_count": len(holdings),
                    "positions_count": len(positions),
                },
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.exception("Error generating portfolio summary")
            return {"status": "error", "message": str(e)}

    def get_risk_analysis(self) -> Dict[str, Any]:
        """Simple risk analysis: diversification, sector concentration, exposure."""
        try:
            summary = self.get_portfolio_summary()
            if summary.get("status") != "success":
                return summary
            s = summary["summary"]

            div_score = len(s.get("sector_allocation", {}))
            max_sector_pct = max(s.get("sector_allocation", {}).values(), default=0)
            concentration_risk = "High" if max_sector_pct > 50 else "Medium" if max_sector_pct > 30 else "Low"
            exposure = s.get("current_value", 0)

            return {
                "status": "success",
                "risk_analysis": {
                    "diversification_score": div_score,
                    "max_sector_exposure": max_sector_pct,
                    "concentration_risk": concentration_risk,
                    "total_exposure": exposure,
                },
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.exception("Error generating risk analysis")
            return {"status": "error", "message": str(e)}

    # ---------------- Utilities ----------------

    def get_portfolio_dataframe(self) -> pd.DataFrame:
        """Return holdings + positions as DataFrame."""
        try:
            holdings_res = self.get_holdings()
            positions_res = self.get_positions()
            if holdings_res.get("status") != "success" or positions_res.get("status") != "success":
                return pd.DataFrame()
            holdings = holdings_res["holdings"]
            positions = positions_res["positions"]
            df = pd.DataFrame(holdings + positions)
            if not df.empty:
                df["instrument"] = df.get("instrument", df.get("tradingsymbol", "Unknown"))
            return df
        except Exception as e:
            logger.exception("Error creating portfolio DataFrame")
            return pd.DataFrame()


# Global instance
portfolio_analytics = PortfolioAnalytics()
