# filename: option_analytics.py
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

# Import the data fetcher
# Make sure option_data.py exists in the same directory or update the import path accordingly.
# Example: If UpstoxOptionData is defined in this file, import it directly.
# from .option_data import UpstoxOptionData  # If using a package structure

# If UpstoxOptionData is defined in this file, remove the import and define the class here.
# Otherwise, ensure option_data.py is present in the same folder as this script.
from backend.option_chain import UpstoxOptionData

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OptionAnalytics:
    """
    Option Analytics - All calculations and analysis functions
    """
    
    def __init__(self, data_fetcher: Optional[UpstoxOptionData] = None):
        self.data_fetcher = data_fetcher or UpstoxOptionData()
        logger.info("✅ OptionAnalytics initialized")
    
    def get_option_greeks_summary(self, symbol: str, expiry_date: Optional[str] = None) -> Dict[str, Any]:
        """Get summary of option greeks for a symbol"""
        chain_data = self.data_fetcher.get_complete_option_chain(symbol, expiry_date)
        
        if chain_data.get("status") != "success":
            return {"status": "error", "message": "Failed to get option chain"}
        
        try:
            data = chain_data["data"]
            if not data:
                return {"status": "error", "message": "No data available"}
            
            # Calculate aggregated metrics
            metrics = {
                "total_call_oi": 0, "total_put_oi": 0,
                "total_call_volume": 0, "total_put_volume": 0,
                "max_call_oi": 0, "max_put_oi": 0,
                "max_call_oi_strike": 0, "max_put_oi_strike": 0,
                "spot_price": 0, "pcr": 0
            }
            
            for strike_data in data:
                if "call_options" in strike_data:
                    call_oi = strike_data["call_options"]["market_data"].get("oi", 0)
                    call_volume = strike_data["call_options"]["market_data"].get("volume", 0)
                    metrics["total_call_oi"] += call_oi
                    metrics["total_call_volume"] += call_volume
                    
                    if call_oi > metrics["max_call_oi"]:
                        metrics["max_call_oi"] = call_oi
                        metrics["max_call_oi_strike"] = strike_data["strike_price"]
                
                if "put_options" in strike_data:
                    put_oi = strike_data["put_options"]["market_data"].get("oi", 0)
                    put_volume = strike_data["put_options"]["market_data"].get("volume", 0)
                    metrics["total_put_oi"] += put_oi
                    metrics["total_put_volume"] += put_volume
                    
                    if put_oi > metrics["max_put_oi"]:
                        metrics["max_put_oi"] = put_oi
                        metrics["max_put_oi_strike"] = strike_data["strike_price"]
                
                # Get spot price and PCR from first strike
                if not metrics["spot_price"]:
                    metrics["spot_price"] = strike_data.get("underlying_spot_price", 0)
                    metrics["pcr"] = strike_data.get("pcr", 0)
            
            summary = {
                "symbol": symbol,
                "total_strikes": len(data),
                **metrics,
                "expiry_date": expiry_date or "nearest",
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"📊 Greeks summary calculated for {symbol}")
            return {"status": "success", "data": summary}
            
        except Exception as e:
            logger.error(f"❌ Error calculating greeks summary: {e}")
            return {"status": "error", "message": f"Error calculating summary: {str(e)}"}
    
    def get_option_chain_dataframe(self, symbol: str, expiry_date: Optional[str] = None) -> pd.DataFrame:
        """Get option chain data as pandas DataFrame"""
        chain_data = self.data_fetcher.get_complete_option_chain(symbol, expiry_date)
        
        if chain_data.get("status") != "success":
            logger.error(f"❌ Failed to get option chain for {symbol}")
            return pd.DataFrame()
        
        try:
            data = chain_data["data"]
            rows = []
            
            for strike_data in data:
                strike_price = strike_data["strike_price"]
                
                # Call options data
                if "call_options" in strike_data:
                    call_data = strike_data["call_options"]["market_data"]
                    rows.append({
                        "strike_price": strike_price,
                        "option_type": "CALL",
                        "ltp": call_data.get("ltp", 0),
                        "bid": call_data.get("bid_price", 0),
                        "ask": call_data.get("ask_price", 0),
                        "volume": call_data.get("volume", 0),
                        "oi": call_data.get("oi", 0),
                        "oi_change": call_data.get("oi_day_change", 0),
                        "ltt": call_data.get("ltt", "")
                    })
                
                # Put options data
                if "put_options" in strike_data:
                    put_data = strike_data["put_options"]["market_data"]
                    rows.append({
                        "strike_price": strike_price,
                        "option_type": "PUT",
                        "ltp": put_data.get("ltp", 0),
                        "bid": put_data.get("bid_price", 0),
                        "ask": put_data.get("ask_price", 0),
                        "volume": put_data.get("volume", 0),
                        "oi": put_data.get("oi", 0),
                        "oi_change": put_data.get("oi_day_change", 0),
                        "ltt": put_data.get("ltt", "")
                    })
            
            df = pd.DataFrame(rows)
            logger.info(f"📈 DataFrame created with {len(df)} rows for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error creating DataFrame: {e}")
            return pd.DataFrame()
    
    def get_atm_options(self, symbol: str, expiry_date: Optional[str] = None) -> Dict[str, Any]:
        """Get At-The-Money (ATM) options for a symbol"""
        chain_data = self.data_fetcher.get_complete_option_chain(symbol, expiry_date)
        
        if chain_data.get("status") != "success":
            return {"status": "error", "message": "Failed to get option chain"}
        
        try:
            data = chain_data["data"]
            if not data:
                return {"status": "error", "message": "No data available"}
            
            # Get spot price and find ATM strike
            spot_price = data[0].get("underlying_spot_price", 0)
            atm_strike = min(data, key=lambda x: abs(x["strike_price"] - spot_price))["strike_price"]
            
            # Get ATM options data
            atm_data = next((strike for strike in data if strike["strike_price"] == atm_strike), None)
            
            if not atm_data:
                return {"status": "error", "message": "ATM data not found"}
            
            result = {
                "symbol": symbol,
                "spot_price": spot_price,
                "atm_strike": atm_strike,
                "call_options": atm_data.get("call_options", {}),
                "put_options": atm_data.get("put_options", {}),
                "expiry_date": expiry_date or "nearest",
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"🎯 ATM options found for {symbol} at strike {atm_strike}")
            return {"status": "success", "data": result}
            
        except Exception as e:
            logger.error(f"❌ Error getting ATM options: {e}")
            return {"status": "error", "message": f"Error getting ATM options: {str(e)}"}
    
    def get_option_activity(self, symbol: str, expiry_date: Optional[str] = None, 
                           activity_type: str = "volume") -> Dict[str, Any]:
        """Get option activity (volume/OI) sorted by highest activity"""
        chain_data = self.data_fetcher.get_complete_option_chain(symbol, expiry_date)
        
        if chain_data.get("status") != "success":
            return {"status": "error", "message": "Failed to get option chain"}
        
        try:
            data = chain_data["data"]
            activity_data = []
            
            for strike_data in data:
                strike_price = strike_data["strike_price"]
                
                # Call options activity
                if "call_options" in strike_data:
                    call_market_data = strike_data["call_options"]["market_data"]
                    activity_data.append({
                        "strike_price": strike_price,
                        "option_type": "CALL",
                        "volume": call_market_data.get("volume", 0),
                        "oi": call_market_data.get("oi", 0),
                        "oi_change": call_market_data.get("oi_day_change", 0),
                        "ltp": call_market_data.get("ltp", 0)
                    })
                
                # Put options activity
                if "put_options" in strike_data:
                    put_market_data = strike_data["put_options"]["market_data"]
                    activity_data.append({
                        "strike_price": strike_price,
                        "option_type": "PUT",
                        "volume": put_market_data.get("volume", 0),
                        "oi": put_market_data.get("oi", 0),
                        "oi_change": put_market_data.get("oi_day_change", 0),
                        "ltp": put_market_data.get("ltp", 0)
                    })
            
            # Sort by activity type
            sort_key = {"volume": "volume", "oi": "oi", "oi_change": "oi_change"}.get(activity_type, "volume")
            sorted_data = sorted(activity_data, key=lambda x: x[sort_key], reverse=True)
            
            result = {
                "symbol": symbol,
                "activity_type": activity_type,
                "data": sorted_data[:20],  # Top 20 most active
                "total_contracts": len(activity_data),
                "expiry_date": expiry_date or "nearest",
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"📊 Option activity retrieved for {symbol}")
            return {"status": "success", "data": result}
            
        except Exception as e:
            logger.error(f"❌ Error getting option activity: {e}")
            return {"status": "error", "message": f"Error getting activity: {str(e)}"}
    
    def get_pcr_analysis(self, symbol: str, expiry_date: Optional[str] = None) -> Dict[str, Any]:
        """Get detailed Put-Call Ratio (PCR) analysis"""
        chain_data = self.data_fetcher.get_complete_option_chain(symbol, expiry_date)
        
        if chain_data.get("status") != "success":
            return {"status": "error", "message": "Failed to get option chain"}
        
        try:
            data = chain_data["data"]
            
            # Initialize counters
            totals = {"call_oi": 0, "put_oi": 0, "call_volume": 0, "put_volume": 0}
            itm_otm = {"itm_call_oi": 0, "itm_put_oi": 0, "otm_call_oi": 0, "otm_put_oi": 0}
            
            spot_price = data[0].get("underlying_spot_price", 0) if data else 0
            
            for strike_data in data:
                strike_price = strike_data["strike_price"]
                
                # Call options
                if "call_options" in strike_data:
                    call_oi = strike_data["call_options"]["market_data"].get("oi", 0)
                    call_volume = strike_data["call_options"]["market_data"].get("volume", 0)
                    totals["call_oi"] += call_oi
                    totals["call_volume"] += call_volume
                    
                    if strike_price < spot_price:  # ITM call
                        itm_otm["itm_call_oi"] += call_oi
                    else:  # OTM call
                        itm_otm["otm_call_oi"] += call_oi
                
                # Put options
                if "put_options" in strike_data:
                    put_oi = strike_data["put_options"]["market_data"].get("oi", 0)
                    put_volume = strike_data["put_options"]["market_data"].get("volume", 0)
                    totals["put_oi"] += put_oi
                    totals["put_volume"] += put_volume
                    
                    if strike_price > spot_price:  # ITM put
                        itm_otm["itm_put_oi"] += put_oi
                    else:  # OTM put
                        itm_otm["otm_put_oi"] += put_oi
            
            # Calculate PCR ratios
            pcr_oi = totals["put_oi"] / totals["call_oi"] if totals["call_oi"] > 0 else 0
            pcr_volume = totals["put_volume"] / totals["call_volume"] if totals["call_volume"] > 0 else 0
            pcr_itm = itm_otm["itm_put_oi"] / itm_otm["itm_call_oi"] if itm_otm["itm_call_oi"] > 0 else 0
            pcr_otm = itm_otm["otm_put_oi"] / itm_otm["otm_call_oi"] if itm_otm["otm_call_oi"] > 0 else 0
            
            result = {
                "symbol": symbol,
                "spot_price": spot_price,
                "pcr_oi": round(pcr_oi, 3),
                "pcr_volume": round(pcr_volume, 3),
                "pcr_itm": round(pcr_itm, 3),
                "pcr_otm": round(pcr_otm, 3),
                **totals,
                "market_sentiment": self._get_market_sentiment(pcr_oi),
                "expiry_date": expiry_date or "nearest",
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"📊 PCR analysis completed for {symbol}")
            return {"status": "success", "data": result}
            
        except Exception as e:
            logger.error(f"❌ Error in PCR analysis: {e}")
            return {"status": "error", "message": f"Error in PCR analysis: {str(e)}"}
    
    def _get_market_sentiment(self, pcr: float) -> str:
        """Get market sentiment based on PCR value"""
        if pcr < 0.7:
            return "Bullish"
        elif pcr > 1.3:
            return "Bearish"
        else:
            return "Neutral"
    
    def get_option_pain(self, symbol: str, expiry_date: Optional[str] = None) -> Dict[str, Any]:
        """Calculate Max Pain point for options"""
        chain_data = self.data_fetcher.get_complete_option_chain(symbol, expiry_date)
        
        if chain_data.get("status") != "success":
            return {"status": "error", "message": "Failed to get option chain"}
        
        try:
            data = chain_data["data"]
            strikes = []
            
            # Collect strike data
            for strike_data in data:
                strikes.append({
                    "strike": strike_data["strike_price"],
                    "call_oi": strike_data.get("call_options", {}).get("market_data", {}).get("oi", 0),
                    "put_oi": strike_data.get("put_options", {}).get("market_data", {}).get("oi", 0)
                })
            
            # Calculate pain for each strike
            pain_data = []
            for target_strike in strikes:
                target_price = target_strike["strike"]
                total_pain = 0
                
                for strike in strikes:
                    strike_price = strike["strike"]
                    
                    # Calculate pain from calls (ITM calls lose money)
                    if target_price > strike_price:
                        total_pain += strike["call_oi"] * (target_price - strike_price)
                    
                    # Calculate pain from puts (ITM puts lose money)
                    if target_price < strike_price:
                        total_pain += strike["put_oi"] * (strike_price - target_price)
                
                pain_data.append({
                    "strike": target_price,
                    "total_pain": total_pain
                })
            
            # Find max pain point (minimum total pain)
            max_pain_point = min(pain_data, key=lambda x: x["total_pain"])
            
            result = {
                "symbol": symbol,
                "max_pain_strike": max_pain_point["strike"],
                "max_pain_value": max_pain_point["total_pain"],
                "all_strikes_pain": pain_data,
                "expiry_date": expiry_date or "nearest",
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"🎯 Max pain calculated for {symbol}: {max_pain_point['strike']}")
            return {"status": "success", "data": result}
            
        except Exception as e:
            logger.error(f"❌ Error calculating max pain: {e}")
            return {"status": "error", "message": f"Error calculating max pain: {str(e)}"}


# Example usage and testing
def main():
    """Example usage of the OptionAnalytics class"""
    
    # Initialize analytics
    analytics = OptionAnalytics()
    
    # Example 1: Get option greeks summary
    print("📈 Getting option greeks summary...")
    summary = analytics.get_option_greeks_summary("NIFTY")
    if summary.get("status") == "success":
        data = summary["data"]
        print(f"PCR: {data['pcr']}")
        print(f"Max Call OI Strike: {data['max_call_oi_strike']}")
        print(f"Max Put OI Strike: {data['max_put_oi_strike']}")
    
    # Example 2: Get ATM options
    print("\n🎯 Getting ATM options...")
    atm = analytics.get_atm_options("NIFTY")
    if atm.get("status") == "success":
        data = atm["data"]
        print(f"ATM Strike: {data['atm_strike']}")
        print(f"Spot Price: {data['spot_price']}")
    
    # Example 3: Get PCR analysis
    print("\n📊 Getting PCR analysis...")
    pcr = analytics.get_pcr_analysis("NIFTY")
    if pcr.get("status") == "success":
        data = pcr["data"]
        print(f"PCR OI: {data['pcr_oi']}")
        print(f"Market Sentiment: {data['market_sentiment']}")
    
    # Example 4: Get max pain
    print("\n🎯 Getting max pain...")
    pain = analytics.get_option_pain("NIFTY")
    if pain.get("status") == "success":
        data = pain["data"]
        print(f"Max Pain Strike: {data['max_pain_strike']}")
    
    # Example 5: Get as DataFrame
    print("\n📈 Getting option chain as DataFrame...")
    df = analytics.get_option_chain_dataframe("NIFTY")
    if not df.empty:
        print(f"DataFrame shape: {df.shape}")
        print(df.head())


if __name__ == "__main__":
    main()