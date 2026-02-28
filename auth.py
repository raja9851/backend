# filename: auth.py
import requests
import os

class UpstoxAuth:
    def __init__(self):
        self.access_token = os.getenv('UPSTOX_ACCESS_TOKEN', '')
        
        if not self.access_token:
            print("⚠️ WARNING: UPSTOX_ACCESS_TOKEN environment variable not set!")
        else:
            print("✅ Access token loaded from environment")
    
    def get_access_token(self):
        """Get current access token"""
        return self.access_token
    
    def get_headers(self):
        """Get headers for API requests"""
        return {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
    
    def get_market_data_feed_authorize_v3(self):
        """Get WebSocket authorization URL"""
        headers = self.get_headers()
        url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
        
        try:
            api_response = requests.get(url=url, headers=headers)
            api_response.raise_for_status()
            return api_response.json()
        except Exception as e:
            print(f"❌ Failed to get WebSocket auth: {e}")
            return None
    
    def update_token(self, new_token):
        """Update access token"""
        self.access_token = new_token
        os.environ['UPSTOX_ACCESS_TOKEN'] = new_token
        print(f"✅ Access token updated")

# Global auth instance
auth = UpstoxAuth()
