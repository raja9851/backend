# historical_data.py
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import requests
import pandas as pd
import re

logger = logging.getLogger(__name__)


def _is_yyyy_mm_dd(s: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", s))


class HistoricalDataHandler:
    """
    Upstox v2 historical & intraday candles helper with:
      - Access-token validation
      - Single requests.Session (connection pooling)
      - Robust GET wrapper (timeouts + raise_for_status)
      - Simple in-memory cache for EOD/week/month data
      - Consistent response schema
    """

    def __init__(self, access_token: Optional[str] = None, base_url: str = "https://api.upstox.com/v2"):
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token

        # Cache for slow-moving intervals (day/week/month)
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.cache_expiry: Dict[str, datetime] = {}

        # persistent session
        self._session = requests.Session()

    # --------------------- Auth & HTTP ---------------------

    def set_access_token(self, token: str) -> None:
        self.access_token = token

    def _require_token(self) -> None:
        if not self.access_token:
            raise RuntimeError("Access token is not set. Call set_access_token(token) first.")

    def _headers(self) -> Dict[str, str]:
        self._require_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

    def _get(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 15.0,
    ) -> Dict[str, Any]:
        """Generic GET with normalized return."""
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
            text = getattr(http_err.response, "text", "")
            code = getattr(http_err.response, "status_code", None)
            logger.error("Historical API HTTPError %s: %s %s", code, url, text)
            return {"status": "error", "message": f"API Error {code}", "error": text}
        except requests.RequestException as req_err:
            logger.error("Historical API RequestException: %s (%s)", req_err, url)
            return {"status": "error", "message": str(req_err)}
        except Exception as e:
            logger.exception("Unexpected error during Historical API GET")
            return {"status": "error", "message": str(e)}

    # --------------------- Cache helpers ---------------------

    @staticmethod
    def _cache_key(instrument_key: str, interval: str, from_date: str, to_date: str) -> str:
        return f"{instrument_key}|{interval}|{from_date}|{to_date}"

    def _cache_valid(self, key: str) -> bool:
        exp = self.cache_expiry.get(key)
        return bool(exp and datetime.now() < exp)

    # --------------------- Normalizers ---------------------

    @staticmethod
    def _normalize_candles(candles_raw: List[List[Any]]) -> List[Dict[str, Any]]:
        """
        Upstox candle tuple format (typical):
        [timestamp, open, high, low, close, volume]
        """
        out: List[Dict[str, Any]] = []
        for c in candles_raw or []:
            if not isinstance(c, (list, tuple)) or len(c) < 6:
                # skip malformed
                continue
            out.append(
                {
                    "timestamp": c[0],
                    "open": c[1],
                    "high": c[2],
                    "low": c[3],
                    "close": c[4],
                    "volume": c[5],
                }
            )
        return out

    @staticmethod
    def _unwrap_candles_payload(api_result: Dict[str, Any]) -> List[List[Any]]:
        """
        Many Upstox responses look like: {"data": {"candles": [...]}}
        We unwrap safely.
        """
        data = api_result.get("data")
        if isinstance(data, dict):
            inner = data.get("data") if isinstance(data.get("data"), dict) else data
            return inner.get("candles", []) if isinstance(inner, dict) else []
        return []

    # --------------------- Public methods ---------------------

    def get_historical_candles(
        self,
        instrument_key: str,
        interval: str,
        from_date: str,
        to_date: str,
    ) -> Dict[str, Any]:
        """
        Fetch historical candles.
        interval: "minute" | "day" | "week" | "month"
        dates: "YYYY-MM-DD"
        """
        try:
            if not instrument_key:
                return {"status": "error", "message": "instrument_key is required."}
            if interval not in {"minute", "day", "week", "month"}:
                return {"status": "error", "message": "interval must be one of: minute, day, week, month."}
            if not (_is_yyyy_mm_dd(from_date) and _is_yyyy_mm_dd(to_date)):
                return {"status": "error", "message": "from_date/to_date must be in YYYY-MM-DD format."}

            key = self._cache_key(instrument_key, interval, from_date, to_date)
            if interval in {"day", "week", "month"} and self._cache_valid(key):
                logger.info("📊 Using cached historical data for %s %s", instrument_key, interval)
                return self.cache[key]

            params = {
                "instrument_key": instrument_key,
                "interval": interval,
                "from_date": from_date,
                "to_date": to_date,
            }
            res = self._get("/historical-candle", params=params)
            if res.get("status") != "success":
                return res

            candles_raw = self._unwrap_candles_payload(res)
            candles = self._normalize_candles(candles_raw)

            payload = {
                "status": "success",
                "instrument_key": instrument_key,
                "interval": interval,
                "from_date": from_date,
                "to_date": to_date,
                "candles": candles,
                "count": len(candles),
            }

            # Cache for 1 hour for slow-moving intervals
            if interval in {"day", "week", "month"}:
                self.cache[key] = payload
                self.cache_expiry[key] = datetime.now() + timedelta(hours=1)

            logger.info("✅ Historical data: %d candles for %s", len(candles), instrument_key)
            return payload

        except Exception as e:
            logger.exception("Error fetching historical candles")
            return {"status": "error", "message": str(e)}

    def get_intraday_candles(self, instrument_key: str, interval: str) -> Dict[str, Any]:
        """
        Fetch intraday candles.
        Common intervals: "1minute", "30minute" (kept flexible, passed through).
        """
        try:
            if not instrument_key:
                return {"status": "error", "message": "instrument_key is required."}
            if not interval:
                return {"status": "error", "message": "interval is required."}

            today = datetime.now().strftime("%Y-%m-%d")

            params = {
                "instrument_key": instrument_key,
                "interval": interval,
            }
            res = self._get("/intra-day-candle", params=params)
            if res.get("status") != "success":
                return res

            candles_raw = self._unwrap_candles_payload(res)
            candles = self._normalize_candles(candles_raw)

            payload = {
                "status": "success",
                "instrument_key": instrument_key,
                "interval": interval,
                "date": today,
                "candles": candles,
                "count": len(candles),
            }
            logger.info("✅ Intraday data: %d candles for %s", len(candles), instrument_key)
            return payload

        except Exception as e:
            logger.exception("Error fetching intraday candles")
            return {"status": "error", "message": str(e)}

    def get_candles_dataframe(
        self,
        instrument_key: str,
        interval: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Returns a pandas DataFrame indexed by timestamp with columns:
        open, high, low, close, volume
        """
        try:
            if from_date and to_date:
                res = self.get_historical_candles(instrument_key, interval, from_date, to_date)
            else:
                res = self.get_intraday_candles(instrument_key, interval)

            if res.get("status") != "success":
                logger.error("❌ DataFrame creation failed: %s", res.get("message"))
                return pd.DataFrame()

            df = pd.DataFrame(res.get("candles", []))
            if df.empty:
                return df

            # Normalize timestamp -> pandas datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df.set_index("timestamp", inplace=True)
            logger.info("✅ DataFrame created with %d rows", len(df))
            return df

        except Exception as e:
            logger.exception("Error creating candles DataFrame")
            return pd.DataFrame()

    def get_multiple_instruments_data(
        self,
        instruments: List[str],
        interval: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Batch fetch for multiple instruments; returns a dict keyed by instrument_key."""
        try:
            results: Dict[str, Any] = {}
            for ik in instruments or []:
                if from_date and to_date:
                    results[ik] = self.get_historical_candles(ik, interval, from_date, to_date)
                else:
                    results[ik] = self.get_intraday_candles(ik, interval)

            logger.info("✅ Multiple instruments fetched: %d", len(instruments or []))
            return {"status": "success", "data": results, "instruments_count": len(instruments or [])}
        except Exception as e:
            logger.exception("Error fetching multiple instruments data")
            return {"status": "error", "message": str(e)}

    def clear_cache(self) -> None:
        self.cache.clear()
        self.cache_expiry.clear()
        logger.info("✅ Historical data cache cleared")


# Global instance
historical_data_handler = HistoricalDataHandler()
