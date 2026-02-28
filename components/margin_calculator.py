# margin_calculator.py
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests

logger = logging.getLogger(__name__)


class MarginCalculator:
    """
    Margin calculator using Upstox v2 API.
    Provides:
      - API-based margin calculation (preferred)
      - Fallback manual margin calculation
      - Basket margin optimization
      - Available margin + sufficiency check
    """

    def __init__(self, access_token: Optional[str] = None, base_url: str = "https://api.upstox.com/v2"):
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token
        self._session = requests.Session()

        # Approx default margin requirements (fallback)
        self.default_margins = {
            "equity_delivery": 0.20,  # 20% for delivery
            "equity_intraday": 0.05,  # 5% for intraday
            "fno": 0.10,              # 10% for futures
            "options": 1.00           # premium amount for options
        }

    # ------------------- Auth & HTTP -------------------

    def set_access_token(self, token: str) -> None:
        self.access_token = token

    def _require_token(self) -> None:
        if not self.access_token:
            raise RuntimeError("Access token not set. Call set_access_token(token).")

    def _headers(self) -> Dict[str, str]:
        self._require_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        timeout: float = 15.0,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self._session.request(
                method.upper(), url, headers=self._headers(), params=params, json=json, timeout=timeout
            )
            resp.raise_for_status()
            try:
                payload = resp.json()
            except ValueError:
                payload = {"raw": resp.text}
            return {"status": "success", "data": payload}
        except requests.HTTPError as http_err:
            code = getattr(http_err.response, "status_code", None)
            text = getattr(http_err.response, "text", "")
            logger.error("Margin API HTTPError %s: %s %s", code, url, text)
            return {"status": "error", "message": f"API Error {code}", "error": text}
        except requests.RequestException as req_err:
            logger.error("Margin API RequestException: %s (%s)", req_err, url)
            return {"status": "error", "message": str(req_err)}
        except Exception as e:
            logger.exception("Unexpected error during Margin API request")
            return {"status": "error", "message": str(e)}

    # ------------------- Margin Calculations -------------------

    def calculate_order_margin(self, orders: List[Dict]) -> Dict[str, Any]:
        """
        Calculate margin required for multiple orders.

        orders = [{
            "instrument_key": "NSE_EQ|SBIN",
            "quantity": 100,
            "price": 500,
            "transaction_type": "BUY",
            "product": "D"  # D=Delivery, I=Intraday, MIS/NRML=F&O
        }]
        """
        try:
            if not orders:
                return {"status": "error", "message": "orders list is empty"}

            payload = {"orders": orders}
            res = self._request("POST", "/charges/margin", json=payload)
            if res["status"] == "success":
                margin_data = res["data"].get("data", {})
                return self._enhance_margin_data(margin_data, orders)
            else:
                logger.warning("Falling back to manual margin calculation")
                return self._manual_margin_calculation(orders)
        except Exception as e:
            logger.exception("Error calculating order margin")
            return self._manual_margin_calculation(orders)

    def _enhance_margin_data(self, margin_data: Dict[str, Any], orders: List[Dict]) -> Dict[str, Any]:
        """Add derived fields (efficiency, breakdown counts, etc.)"""
        try:
            total_margin = margin_data.get("total", 0)
            total_value = sum(o.get("quantity", 0) * o.get("price", 0) for o in orders)
            efficiency = (total_value / total_margin) if total_margin > 0 else 0

            delivery = sum(1 for o in orders if o.get("product") == "D")
            intraday = sum(1 for o in orders if o.get("product") == "I")
            fno = sum(1 for o in orders if o.get("product") in {"MIS", "NRML"})

            return {
                "status": "success",
                "total_margin_required": round(total_margin, 2),
                "total_order_value": round(total_value, 2),
                "margin_efficiency": round(efficiency, 2),
                "orders_breakdown": {
                    "delivery_orders": delivery,
                    "intraday_orders": intraday,
                    "fno_orders": fno,
                },
                "margin_breakdown": margin_data,
                "orders_count": len(orders),
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.exception("Error enhancing margin data")
            return {"status": "error", "message": str(e)}

    def _manual_margin_calculation(self, orders: List[Dict]) -> Dict[str, Any]:
        """Fallback calculation if API not available."""
        try:
            total_margin = 0.0
            total_value = 0.0
            breakdown = {"delivery_margin": 0, "intraday_margin": 0, "fno_margin": 0, "options_premium": 0}

            for o in orders:
                q = o.get("quantity", 0)
                p = o.get("price", 0.0)
                prod = o.get("product", "D")
                ikey = o.get("instrument_key", "")

                value = q * p
                total_value += value

                if prod == "D":
                    m = value * self.default_margins["equity_delivery"]
                    breakdown["delivery_margin"] += m
                elif prod == "I":
                    m = value * self.default_margins["equity_intraday"]
                    breakdown["intraday_margin"] += m
                elif "FUT" in ikey or "CE" in ikey or "PE" in ikey:
                    if "CE" in ikey or "PE" in ikey:
                        m = value * self.default_margins["options"]
                        breakdown["options_premium"] += m
                    else:
                        m = value * self.default_margins["fno"]
                        breakdown["fno_margin"] += m
                else:
                    m = value * self.default_margins["equity_delivery"]
                    breakdown["delivery_margin"] += m

                total_margin += m

            return {
                "status": "success",
                "total_margin_required": round(total_margin, 2),
                "total_order_value": round(total_value, 2),
                "margin_breakdown": {k: round(v, 2) for k, v in breakdown.items()},
                "calculation_method": "manual",
                "orders_count": len(orders),
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.exception("Manual margin calculation failed")
            return {"status": "error", "message": str(e)}

    def calculate_basket_margin(self, basket_orders: List[Dict]) -> Dict[str, Any]:
        """Calculate margin for basket orders with netting benefit estimation."""
        try:
            res = self.calculate_order_margin(basket_orders)
            if res.get("status") != "success":
                return res

            gross = res.get("total_margin_required", 0)
            buy_orders = [o for o in basket_orders if o.get("transaction_type") == "BUY"]
            sell_orders = [o for o in basket_orders if o.get("transaction_type") == "SELL"]

            net_benefit = self._netting_benefit(buy_orders, sell_orders)
            net_margin = max(0, gross - net_benefit)
            savings = gross - net_margin
            savings_pct = (savings / gross * 100) if gross > 0 else 0

            return {
                "status": "success",
                "basket_analysis": {
                    "total_orders": len(basket_orders),
                    "buy_orders": len(buy_orders),
                    "sell_orders": len(sell_orders),
                    "gross_margin": round(gross, 2),
                    "netting_benefit": round(net_benefit, 2),
                    "net_margin_required": round(net_margin, 2),
                    "margin_savings": round(savings, 2),
                    "savings_percent": round(savings_pct, 2),
                },
                "recommendation": f"Net margin optimized by ₹{savings:.2f}" if savings > 0 else "No netting benefits",
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.exception("Error calculating basket margin")
            return {"status": "error", "message": str(e)}

    def _netting_benefit(self, buys: List[Dict], sells: List[Dict]) -> float:
        """Estimate netting benefit for opposite positions of same instrument."""
        try:
            buy_val: Dict[str, float] = {}
            for o in buys:
                ik = o.get("instrument_key", "")
                buy_val[ik] = buy_val.get(ik, 0) + o.get("quantity", 0) * o.get("price", 0.0)

            sell_val: Dict[str, float] = {}
            for o in sells:
                ik = o.get("instrument_key", "")
                sell_val[ik] = sell_val.get(ik, 0) + o.get("quantity", 0) * o.get("price", 0.0)

            benefit = 0.0
            for ik in buy_val:
                if ik in sell_val:
                    nettable = min(buy_val[ik], sell_val[ik])
                    benefit += nettable * 0.5  # assume 50% benefit
            return benefit
        except Exception as e:
            logger.exception("Netting benefit calculation failed")
            return 0.0

    # ------------------- Available Margin -------------------

    def get_available_margin(self) -> Dict[str, Any]:
        """Fetch margin funds from account."""
        try:
            res = self._request("GET", "/user/get-funds-and-margin")
            if res.get("status") != "success":
                return res

            eq = res["data"].get("data", {}).get("equity", {})
            avail = eq.get("available_margin", 0)
            used = eq.get("used_margin", 0)
            total = avail + used
            util = (used / total * 100) if total > 0 else 0

            return {
                "status": "success",
                "available_margin": avail,
                "used_margin": used,
                "total_margin": total,
                "utilization_percent": round(util, 2),
                "margin_status": self._margin_status(util),
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.exception("Error getting available margin")
            return {"status": "error", "message": str(e)}

    def _margin_status(self, util: float) -> str:
        if util >= 90:
            return "Critical - Very High Usage"
        elif util >= 75:
            return "Warning - High Usage"
        elif util >= 50:
            return "Caution - Medium Usage"
        return "Safe - Low Usage"

    def check_margin_sufficiency(self, orders: List[Dict]) -> Dict[str, Any]:
        """Check if available margin is enough for given orders."""
        try:
            req = self.calculate_order_margin(orders)
            if req.get("status") != "success":
                return req
            need = req["total_margin_required"]

            avail_res = self.get_available_margin()
            if avail_res.get("status") != "success":
                return avail_res
            avail = avail_res["available_margin"]

            sufficient = avail >= need
            shortfall = max(0, need - avail)
            return {
                "status": "success",
                "required_margin": need,
                "available_margin": avail,
                "is_sufficient": sufficient,
                "shortfall": round(shortfall, 2),
                "utilization_after_orders": round((need / avail * 100), 2) if avail > 0 else 100,
                "recommendation": "Proceed with orders" if sufficient else f"Add ₹{shortfall:.2f} margin to proceed",
                "orders_count": len(orders),
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.exception("Error checking margin sufficiency")
            return {"status": "error", "message": str(e)}


# Global instance
margin_calculator = MarginCalculator()
