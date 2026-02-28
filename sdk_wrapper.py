# sdk_wrapper.py
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import upstox_client
from upstox_client.rest import ApiException

class UpstoxClientWrapper:
    def __init__(self, access_token: str):
        """
        Initialize Upstox client with access token
        """
        self.access_token = access_token
        
        # Configure API client
        configuration = upstox_client.Configuration()
        configuration.access_token = access_token
        self.api_client = upstox_client.ApiClient(configuration)
        
        # Initialize API instances
        self._init_apis()
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
    
    def _init_apis(self):
        """Initialize all API instances"""
        try:
            # Market Data APIs
            self.market_quote_api = upstox_client.MarketQuoteApi(self.api_client)
            self.history_api = upstox_client.HistoryApi(self.api_client)
            self.options_api = upstox_client.OptionsApi(self.api_client)
            
            # Trading APIs
            self.order_api = upstox_client.OrderApi(self.api_client)
            self.portfolio_api = upstox_client.PortfolioApi(self.api_client)
            self.post_trade_api = upstox_client.PostTradeApi(self.api_client)
            
            # User & Account APIs
            self.user_api = upstox_client.UserApi(self.api_client)
            self.charge_api = upstox_client.ChargeApi(self.api_client)
            
            # Utility APIs
            self.market_holidays_api = upstox_client.MarketHolidaysAndTimingsApi(self.api_client)
            self.login_api = upstox_client.LoginApi(self.api_client)
            
        except Exception as e:
            self.logger.error(f"Error initializing APIs: {e}")
            raise

    def _handle_api_call(self, api_call, *args, **kwargs):
        """Generic API call handler with error handling"""
        try:
            response = api_call(*args, **kwargs)
            return response
        except ApiException as e:
            self.logger.error(f"API Exception: {e}")
            return {"error": str(e), "status_code": e.status}
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return {"error": str(e)}

    # ==================== USER & ACCOUNT METHODS ====================
    
    def get_user_profile(self) -> Dict:
        """Get user profile information"""
        return self._handle_api_call(self.user_api.get_profile)
    
    def get_balance(self) -> Dict:
        """Get account balance and margin details"""
        return self._handle_api_call(self.user_api.get_user_fund_margin)
    
    def get_charges(self, instrument_token: str, quantity: int, product: str, 
                   transaction_type: str, price: float) -> Dict:
        """Calculate brokerage charges"""
        return self._handle_api_call(
            self.charge_api.get_brokerage,
            instrument_token=instrument_token,
            quantity=quantity,
            product=product,
            transaction_type=transaction_type,
            price=price
        )

    # ==================== MARKET DATA METHODS ====================
    
    def get_ltp(self, instrument_keys: List[str]) -> Dict:
        """Get Last Traded Price for instruments"""
        instrument_keys_str = ",".join(instrument_keys)
        return self._handle_api_call(
            self.market_quote_api.ltp,
            instrument_key=instrument_keys_str
        )
    
    def get_quotes(self, instrument_keys: List[str]) -> Dict:
        """Get detailed quotes for instruments"""
        instrument_keys_str = ",".join(instrument_keys)
        return self._handle_api_call(
            self.market_quote_api.get_full_market_quote,
            instrument_key=instrument_keys_str
        )
    
    def get_ohlc(self, instrument_keys: List[str]) -> Dict:
        """Get OHLC data for instruments"""
        instrument_keys_str = ",".join(instrument_keys)
        return self._handle_api_call(
            self.market_quote_api.get_market_quote_ohlc,
            instrument_key=instrument_keys_str
        )

    def get_historical_data(self, instrument_key: str, interval: str, 
                          from_date: str, to_date: str) -> Dict:
        """
        Get historical candle data
        
        Args:
            instrument_key: Instrument identifier
            interval: 1minute, 30minute, day, week, month
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        """
        return self._handle_api_call(
            self.history_api.get_historical_candle_data,
            instrument_key=instrument_key,
            interval=interval,
            to_date=to_date,
            from_date=from_date
        )
    
    def get_intraday_data(self, instrument_key: str, interval: str) -> Dict:
        """Get intraday candle data"""
        return self._handle_api_call(
            self.history_api.get_intra_day_candle_data,
            instrument_key=instrument_key,
            interval=interval
        )

    # ==================== OPTIONS METHODS ====================
    
    def get_option_contracts(self, instrument_key: str, expiry_date: str) -> Dict:
        """Get option contracts for underlying"""
        return self._handle_api_call(
            self.options_api.get_option_contracts,
            instrument_key=instrument_key,
            expiry_date=expiry_date
        )
    
    def get_put_call_option_chain(self, instrument_key: str, expiry_date: str) -> Dict:
        """Get complete option chain"""
        return self._handle_api_call(
            self.options_api.get_put_call_option_chain,
            instrument_key=instrument_key,
            expiry_date=expiry_date
        )

    # ==================== ORDER MANAGEMENT METHODS ====================
    
    def place_order(self, quantity: int, product: str, validity: str,
                   price: float, instrument_token: str, order_type: str,
                   transaction_type: str, disclosed_quantity: int = 0,
                   trigger_price: float = 0.0, is_amo: bool = False) -> Dict:
        """
        Place a new order
        
        Args:
            quantity: Order quantity
            product: CNC, MIS, NRML
            validity: DAY, IOC
            price: Order price
            instrument_token: Instrument identifier
            order_type: MARKET, LIMIT, SL, SL-M
            transaction_type: BUY, SELL
            disclosed_quantity: Iceberg quantity
            trigger_price: Stop loss trigger price
            is_amo: After market order flag
        """
        body = upstox_client.PlaceOrderRequest(
            quantity=quantity,
            product=product,
            validity=validity,
            price=price,
            instrument_token=instrument_token,
            order_type=order_type,
            transaction_type=transaction_type,
            disclosed_quantity=disclosed_quantity,
            trigger_price=trigger_price,
            is_amo=is_amo
        )
        
        return self._handle_api_call(self.order_api.place_order, body=body)
    
    def modify_order(self, order_id: str, quantity: Optional[int] = None, validity: Optional[str] = None,
                    price: Optional[float] = None, order_type: Optional[str] = None,
                    disclosed_quantity: Optional[int] = None, trigger_price: Optional[float] = None) -> Dict:
        """Modify existing order"""
        body = upstox_client.ModifyOrderRequest(
            quantity=quantity,
            validity=validity,
            price=price,
            order_type=order_type,
            disclosed_quantity=disclosed_quantity,
            trigger_price=trigger_price
        )
        
        return self._handle_api_call(
            self.order_api.modify_order,
            order_id=order_id,
            body=body
        )
    
    def cancel_order(self, order_id: str) -> Dict:
        """Cancel an order"""
        return self._handle_api_call(self.order_api.cancel_order, order_id=order_id)
    
    def get_order_details(self, order_id: str) -> Dict:
        """Get order details by order ID"""
        return self._handle_api_call(self.order_api.get_order_details, order_id=order_id)
    
    def get_order_book(self) -> Dict:
        """Get all orders"""
        return self._handle_api_call(self.order_api.get_order_book)
    
    def get_trade_book(self) -> Dict:
        """Get all executed trades"""
        return self._handle_api_call(self.order_api.get_trades_by_order)

    # ==================== PORTFOLIO METHODS ====================
    
    def get_holdings(self) -> Dict:
        """Get portfolio holdings"""
        return self._handle_api_call(self.portfolio_api.get_holdings)
    
    def get_positions(self) -> Dict:
        """Get current positions"""
        return self._handle_api_call(self.portfolio_api.get_positions)
    
    def convert_position(self, instrument_token: str, new_product: str,
                        old_product: str, transaction_type: str, quantity: int) -> Dict:
        """Convert position product type"""
        body = upstox_client.ConvertPositionRequest(
            instrument_token=instrument_token,
            new_product=new_product,
            old_product=old_product,
            transaction_type=transaction_type,
            quantity=quantity
        )
        
        return self._handle_api_call(self.portfolio_api.convert_positions, body=body)

    # ==================== P&L METHODS ====================
    
    def get_profit_loss(self, segment: Optional[str] = None, financial_year: Optional[str] = None) -> Dict:
        """Get profit and loss report"""
        return self._handle_api_call(
            self.post_trade_api.get_trades_by_date_range,
            segment=segment,
            financial_year=financial_year
        )

    # ==================== UTILITY METHODS ====================
    
    def get_market_holidays(self, date: str) -> Dict:
        """Get market holidays"""
        return self._handle_api_call(
            self.market_holidays_api.get_holidays,
            date=date
        )
    
    def get_market_status(self, exchange: str) -> Dict:
        """Get market status for exchange"""
        return self._handle_api_call(
            self.market_holidays_api.get_exchange_timings,
            date=datetime.now().strftime("%Y-%m-%d")
        )

    # ==================== HELPER METHODS ====================
    
    def search_instruments(self, query: str) -> List[Dict]:
        """Search for instruments (you'll need to implement instrument master loading)"""
        # This would require loading and searching through instrument master file
        # Implementation depends on your specific needs
        return []
    
    def get_instrument_by_symbol(self, symbol: str, exchange: str = "NSE") -> Optional[str]:
        """Get instrument token by symbol"""
        # This would require instrument master data
        # Implementation depends on your specific needs
        pass

# Example usage
if __name__ == "__main__":
    # Initialize wrapper
    ACCESS_TOKEN = "your_access_token_here"
    client = UpstoxClientWrapper(ACCESS_TOKEN)
    
    # Example calls
    try:
        # Get user profile
        profile = client.get_user_profile()
        print("Profile:", profile)
        
        # Get balance
        balance = client.get_balance()
        print("Balance:", balance)
        
        # Get LTP for instruments
        ltp = client.get_ltp(["NSE_EQ|INE062A01020"])  # Example: Reliance
        print("LTP:", ltp)
        
        # Place order example (commented out for safety)
        # order = client.place_order(
        #     quantity=1,
        #     product="CNC",
        #     validity="DAY",
        #     price=2500.0,
        #     instrument_token="NSE_EQ|INE062A01020",
        #     order_type="LIMIT",
        #     transaction_type="BUY"
        # )
        # print("Order placed:", order)
        
    except Exception as e:
        print(f"Error: {e}")