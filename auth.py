# filename: auth.py
import requests

class UpstoxAuth:
    def __init__(self):
        self.access_token = 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzTUNUM1MiLCJqdGkiOiI2OTljOGQ0NTA0NTQxZTc2ZWRkMzI2NGYiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc3MTg2NzQ2MSwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzcxODg0MDAwfQ.WujS_6rw48qh7M40K-DzEBVKA_BZRiorUPJQa62zE18'
    
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
        print(f"✅ Access token updated")

# Global auth instance
auth = UpstoxAuth()