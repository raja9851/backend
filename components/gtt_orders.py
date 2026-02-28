# gtt_orders.py
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests

logger = logging.getLogger(__name__)


class GTTOrdersHandler:
    """
    Thin wrapper over Upstox v2 GTT endpoints with:
      - Access-token validation
      - Robust request handling (timeouts, raise_for_status)
      - Safer instrument_key/exchange parsing
      - Consistent response structure
    """

    def __init__(self, access_token: Optional[str] = None, base_url: str = "https://api.upstox.com/v2"):
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token
        # Single persistent session for connection pooling + retries (basic)
        self._session = requests.Session()

    # ------------------------- Auth -------------------------

    def set_access_token(self, token: str) -> None:
        """Set/replace OAuth access token."""
        self.access_token = token

    def _require_token(self) -> None:
        if not self.access_token:
            raise RuntimeError("Access token is not set. Call set_access_token(token) first.")

    def _headers(self) -> Dict[str, str]:
        self._require_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # ------------------------- HTTP -------------------------

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        timeout: float = 15.0,
    ) -> Dict[str, Any]:
        """
        Generic HTTP helper that normalizes success/error responses.
        Always returns: {status: "success"|"error", ...}
        """
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self._session.request(
                method=method.upper(),
                url=url,
                headers=self._headers(),
                params=params,
                json=json,
                timeout=timeout,
            )
            resp.raise_for_status()
            # Defensive: JSON decode guard
            try:
                payload = resp.json()
            except ValueError:
                payload = {"raw": resp.text}
            return {"status": "success", "data": payload}
        except requests.HTTPError as http_err:
            text = getattr(http_err.response, "text", "")
            code = getattr(http_err.response, "status_code", None)
            logger.error("GTT API HTTPError %s: %s %s", code, url, text)
            return {
                "status": "error",
                "message": f"API Error {code}",
                "error": text,
            }
        except requests.RequestException as req_err:
            logger.error("GTT API RequestException: %s (%s)", req_err, url)
            return {"status": "error", "message": str(req_err)}
        except Exception as e:
            logger.exception("Unexpected error during GTT API request")
            return {"status": "error", "message": str(e)}

    # ------------------------- Utilities -------------------------

    @staticmethod
    def _parse_exchange_from_instrument_key(instrument_key: str) -> Optional[str]:
        """
        Upstox instrument_key format commonly looks like: 'NSE_EQ|SBIN' or 'NSE_INDEX|Nifty 50'.
        We pull the exchange segment before the first '|'.
        """
        if "|" in instrument_key:
            return instrument_key.split("|", 1)[0]
        return None

    @staticmethod
    def _ok(res: Dict[str, Any]) -> bool:
        return res.get("status") == "success"

    # ------------------------- Public API -------------------------

    def create_gtt_order(self, instrument_key: str, condition: Dict[str, Any], orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create a GTT order.

        Parameters
        ----------
        instrument_key : str
            e.g. "NSE_EQ|SBIN"
        condition : Dict
            {
                "type": "single" | "two_leg",
                "trigger_values": [price1]  # for single
                # or [price1, price2]       # for two_leg
            }
        orders : List[Dict]
            Each order like:
            {
                "transaction_type": "BUY"|"SELL",
                "quantity": 1,
                "product": "D"|"I"|"NRML"|...,
                "order_type": "MARKET"|"LIMIT",
                "price": 0 or limit_price
            }
        """
        try:
            trigger_type = condition.get("type", "single")
            trigger_values = condition.get("trigger_values", [])
            if trigger_type not in {"single", "two_leg"}:
                return {"status": "error", "message": "condition.type must be 'single' or 'two_leg'."}
            if not isinstance(trigger_values, list) or not trigger_values:
                return {"status": "error", "message": "condition.trigger_values must be a non-empty list."}
            if trigger_type == "single" and len(trigger_values) != 1:
                return {"status": "error", "message": "single trigger requires exactly 1 trigger value."}
            if trigger_type == "two_leg" and len(trigger_values) != 2:
                return {"status": "error", "message": "two_leg trigger requires exactly 2 trigger values."}
            if not orders or not isinstance(orders, list):
                return {"status": "error", "message": "orders must be a non-empty list."}

            exchange = self._parse_exchange_from_instrument_key(instrument_key)

            payload = {
                "condition": {
                    "instrument_key": instrument_key,
                    # Some APIs do not require 'exchange', but including when derivable:
                    **({"exchange": exchange} if exchange else {}),
                    "trigger_type": trigger_type,
                    "trigger_values": trigger_values,
                },
                "orders": orders,
            }

            res = self._request("POST", "/gtt", json=payload)
            if self._ok(res):
                logger.info("✅ GTT order created for %s at %s", instrument_key, datetime.now().isoformat())
                return {
                    "status": "success",
                    "message": "GTT order created successfully",
                    "data": res["data"],
                }
            return res
        except Exception as e:
            logger.exception("Error creating GTT order")
            return {"status": "error", "message": str(e)}

    def get_gtt_orders(self) -> Dict[str, Any]:
        """Fetch all GTT orders."""
        res = self._request("GET", "/gtt")
        if self._ok(res):
            data = res["data"]
            # Many APIs nest under 'data' -> 'data'; we normalize gently:
            orders = (data.get("data") if isinstance(data, dict) else None) or []
            logger.info("✅ Retrieved %d GTT orders", len(orders))
            return {"status": "success", "data": orders, "count": len(orders)}
        return res

    def get_gtt_order_details(self, gtt_order_id: str) -> Dict[str, Any]:
        """Fetch details of a specific GTT order."""
        if not gtt_order_id:
            return {"status": "error", "message": "gtt_order_id is required."}
        res = self._request("GET", f"/gtt/{gtt_order_id}")
        if self._ok(res):
            logger.info("✅ GTT order details fetched for %s", gtt_order_id)
        return res

    def modify_gtt_order(self, gtt_order_id: str, condition: Dict[str, Any], orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Modify an existing GTT order."""
        if not gtt_order_id:
            return {"status": "error", "message": "gtt_order_id is required."}
        if not orders:
            return {"status": "error", "message": "orders must be a non-empty list."}
        payload = {"condition": condition, "orders": orders}
        res = self._request("PUT", f"/gtt/{gtt_order_id}", json=payload)
        if self._ok(res):
            logger.info("✅ GTT order %s modified", gtt_order_id)
            return {
                "status": "success",
                "message": "GTT order modified successfully",
                "data": res["data"],
            }
        return res

    def cancel_gtt_order(self, gtt_order_id: str) -> Dict[str, Any]:
        """Cancel a GTT order."""
        if not gtt_order_id:
            return {"status": "error", "message": "gtt_order_id is required."}
        res = self._request("DELETE", f"/gtt/{gtt_order_id}")
        if self._ok(res):
            logger.info("✅ GTT order %s cancelled", gtt_order_id)
            return {"status": "success", "message": "GTT order cancelled successfully"}
        return res


# Global instance
gtt_handler = GTTOrdersHandler()
