import os
import json
import requests
import gzip
from io import BytesIO
from datetime import datetime
from typing import Optional, List, Dict, Any

# Import authentication system
try:
    from auth import auth
    AUTH_AVAILABLE = True
    print("✅ Authentication system imported successfully")
except ImportError:
    auth = None
    AUTH_AVAILABLE = False
    print("⚠️  Authentication system not found, using fallback mode")

DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

class InstrumentManager:
    """
    Updated Upstox Instrument Manager for 2025
    Integrated with existing authentication system
    """

    # Updated URLs based on official documentation
    API_BASE_URL = 'https://api.upstox.com/v2'
    
    # Alternative direct URLs (may require authentication)
    ALTERNATIVE_URLS = {
        "nse": "https://assets.upstox.com/market-quote/instruments/exchange/NSE/NSE.json.gz",
        "bse": "https://assets.upstox.com/market-quote/instruments/exchange/BSE/BSE.json.gz",
        "mcx": "https://assets.upstox.com/market-quote/instruments/exchange/MCX/MCX.json.gz",
    }
    
    # Fallback URLs to try
    FALLBACK_URLS = {
        "nse": [
            "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
            "https://assets.upstox.com/market-quote/instruments/exchange/nse.json.gz",
            "https://assets.upstox.com/market-quote/instruments/exchange/NSE/NSE.json.gz",
        ],
        "bse": [
            "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz",
            "https://assets.upstox.com/market-quote/instruments/exchange/bse.json.gz",
            "https://assets.upstox.com/market-quote/instruments/exchange/BSE/BSE.json.gz",
        ],
        "mcx": [
            "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz",
            "https://assets.upstox.com/market-quote/instruments/exchange/mcx.json.gz",
            "https://assets.upstox.com/market-quote/instruments/exchange/MCX/MCX.json.gz",
        ]
    }

    def __init__(self):
        self.data: Dict[str, List[Dict[str, Any]]] = {}
        self.instruments: List[Dict[str, Any]] = []  # Flat list for easy searching
        self.last_updated: Optional[datetime] = None
        self.auth_available = AUTH_AVAILABLE

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication if available."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, application/gzip, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        # Use existing auth system if available
        if self.auth_available and auth is not None:
            auth_headers = auth.get_headers()
            headers.update(auth_headers)
        
        return headers

    def _download_with_fallback(self, name: str, urls: List[str]) -> bool:
        """Try multiple URLs for downloading a file."""
        for i, url in enumerate(urls):
            try:
                print(f"⬇️  Trying {name} URL {i+1}/{len(urls)}: {url}")
                
                response = requests.get(url, headers=self._get_headers(), timeout=30)
                
                if response.status_code == 200:
                    # Try to decompress gzip
                    try:
                        with gzip.open(BytesIO(response.content), 'rt', encoding='utf-8') as f:
                            json_data = json.load(f)
                    except:
                        # If not gzip, try as plain JSON
                        json_data = response.json()
                    
                    # Save to file
                    file_path = os.path.join(DATA_DIR, f"{name}.json")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, indent=2)
                    
                    self.data[name] = json_data
                    print(f"✅ Successfully downloaded {name} ({len(json_data):,} items)")
                    return True
                    
                else:
                    print(f"❌ Failed with status {response.status_code}: {response.reason}")
                    
            except Exception as e:
                print(f"❌ Error with URL {i+1}: {e}")
                continue
        
        print(f"⚠️  All URLs failed for {name}")
        return False

    def _download_file_authenticated(self, name: str, exchange: str) -> bool:
        """Download using authenticated API endpoint."""
        if not self.auth_available:
            return False  # Silently fail if no auth
        
        try:
            url = f"{self.API_BASE_URL}/market-quote/instruments/{exchange}"
            response = requests.get(url, headers=self._get_headers(), timeout=30)
            
            if response.status_code == 200:
                json_data = response.json()
                
                # Save to file
                file_path = os.path.join(DATA_DIR, f"{name}.json")
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2)
                
                self.data[name] = json_data
                print(f"✅ Downloaded {name} via API ({len(json_data):,} items)")
                return True
            else:
                # Silently fail - fallbacks will handle it
                return False
                
        except Exception as e:
            # Silently fail - fallbacks will handle it
            return False

    def _rebuild_instruments_list(self):
        """Rebuild the flat instruments list from exchange data."""
        self.instruments = []
        for exchange_name, instruments in self.data.items():
            for instrument in instruments:
                # Add exchange info to each instrument
                instrument_copy = instrument.copy()
                instrument_copy['exchange'] = exchange_name.upper()
                self.instruments.append(instrument_copy)
        
        print(f"📋 Rebuilt instruments list: {len(self.instruments):,} total instruments")

    def update_core(self) -> Dict[str, bool]:
        """Download core files with multiple fallback strategies."""
        print("\n📊 Updating core instrument files...")
        results = {}
        
        for name in ["nse", "bse", "mcx"]:
            print(f"\n🔄 Processing {name.upper()}...")
            
            # Strategy 1: Try authenticated API first
            if self.auth_available:
                if self._download_file_authenticated(name, name.upper()):
                    results[name] = True
                    continue
            
            # Strategy 2: Try fallback URLs
            if name in self.FALLBACK_URLS:
                if self._download_with_fallback(name, self.FALLBACK_URLS[name]):
                    results[name] = True
                    continue
            
            results[name] = False
        
        if any(results.values()):
            self.last_updated = datetime.now()
            self._rebuild_instruments_list()
            
        return results

    def update_all(self) -> Dict[str, bool]:
        """
        Update all instrument data.
        This is an alias for update_core() with additional processing.
        """
        print("\n🔄 Updating all instruments...")
        results = self.update_core()
        
        # Additional processing if needed
        if any(results.values()):
            print("✅ All instruments updated successfully")
        else:
            print("⚠️  Failed to update instruments, trying to load from local files...")
            load_results = self.load_all()
            if any(load_results.values()):
                print("✅ Loaded instruments from local cache")
                results.update(load_results)
        
        return results

    def load_all(self) -> Dict[str, bool]:
        """Load all saved instrument files from disk."""
        print("\n📂 Loading instrument files from local storage...")
        results = {}
        
        for name in ["nse", "bse", "mcx"]:
            file_path = os.path.join(DATA_DIR, f"{name}.json")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    self.data[name] = json.load(f)
                print(f"✅ Loaded {name} ({len(self.data[name]):,} items)")
                results[name] = True
            except FileNotFoundError:
                print(f"⚠️  File not found: {name}.json")
                results[name] = False
            except Exception as e:
                print(f"❌ Error loading {name}: {e}")
                results[name] = False
        
        # Rebuild instruments list if any data was loaded
        if any(results.values()):
            self._rebuild_instruments_list()
                
        return results

    def search_symbol(self, symbol: str, exchange: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for instruments by symbol."""
        results = []
        
        for exchange_name, instruments in self.data.items():
            if exchange and exchange_name.lower() != exchange.lower():
                continue
                
            for instrument in instruments:
                if symbol.upper() in instrument.get('trading_symbol', '').upper():
                    instrument_copy = instrument.copy()
                    instrument_copy['exchange'] = exchange_name.upper()
                    results.append(instrument_copy)
        
        return results

    def search_instrument(self, symbol: str, type_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for instruments by symbol and optional type filter.
        Returns a list of matching instruments.
        """
        results = []
        
        for instrument in self.instruments:
            # --- CHANGE START: Make the search more robust ---
            # Create a combined string of the most important fields to search in.
            searchable_string = (
                f"{instrument.get('trading_symbol', '')} "
                f"{instrument.get('name', '')}"
            )
            
            # Check if the search symbol appears anywhere in this combined string.
            if symbol.upper() in searchable_string.upper():
            # --- CHANGE END ---
            
                # Apply type filter if specified
                if type_filter:
                    instrument_type = instrument.get('instrument_type', '') or instrument.get('type', '')
                    if instrument_type.lower() == type_filter.lower():
                        results.append(instrument)
                else:
                    results.append(instrument)
        
        return results

    def get_instrument_by_key(self, instrument_key: str) -> Optional[Dict[str, Any]]:
        """Get instrument by its unique key/token."""
        for instrument in self.instruments:
            if instrument.get('instrument_key') == instrument_key or instrument.get('instrument_token') == instrument_key:
                return instrument
        return None

    def get_instruments_by_exchange(self, exchange: str) -> List[Dict[str, Any]]:
        """Get all instruments for a specific exchange."""
        exchange_upper = exchange.upper()
        return [inst for inst in self.instruments if inst.get('exchange') == exchange_upper]

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics of loaded data."""
        stats = {
            'total_instruments': len(self.instruments),
            'exchanges': list(self.data.keys()),
            'last_updated': self.last_updated.isoformat() if self.last_updated else None
        }
        
        for exchange, instruments in self.data.items():
            stats[f'{exchange}_count'] = len(instruments)
        
        return stats

    def print_status(self):
        """Print current status of loaded data."""
        print("\n📈 Instrument Manager Status:")
        print(f"📊 Total Exchanges: {len(self.data)}")
        print(f"📋 Total Instruments: {len(self.instruments):,}")
        
        for exchange, instruments in self.data.items():
            print(f"📋 {exchange.upper()}: {len(instruments):,} instruments")
        
        if self.last_updated:
            print(f"🕒 Last Updated: {self.last_updated.strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"🔑 Authenticated: {'Yes' if self.auth_available else 'No'}")
        
        if self.auth_available and auth is not None:
            token = auth.get_access_token()
            print(f"🎫 Token Status: {'Valid' if token else 'Missing'}")

# Usage example
if __name__ == "__main__":
    # Initialize with integrated auth system
    manager = InstrumentManager()
    
    # Try to update all files
    results = manager.update_all()
    
    # Print status
    manager.print_status()
    
    # Example searches
    if manager.instruments:
        print("\n🔍 Example Searches:")
        
        # Search by symbol
        reliance = manager.search_symbol("RELIANCE")
        if reliance:
            print(f"📊 Found {len(reliance)} RELIANCE instruments via search_symbol")
            for r in reliance[:3]:  # Show first 3
                print(f"   {r.get('trading_symbol')} - {r.get('name', 'N/A')} [{r.get('exchange')}]")
        
        # Search with type filter
        equity_reliance = manager.search_instrument("RELIANCE", "EQ")
        if equity_reliance:
            print(f"📈 Found {len(equity_reliance)} RELIANCE equity instruments via search_instrument")
            for r in equity_reliance[:2]:  # Show first 2
                print(f"   {r.get('trading_symbol')} - {r.get('name', 'N/A')} [{r.get('exchange')}]")
        
        # Get NSE instruments
        nse_instruments = manager.get_instruments_by_exchange("NSE")
        print(f"🏢 NSE has {len(nse_instruments):,} instruments")