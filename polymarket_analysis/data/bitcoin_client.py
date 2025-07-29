"""
Bitcoin price data client using CoinGecko API
"""

import requests
import time
from typing import Dict, Optional
from datetime import datetime, timedelta


class BitcoinClient:
    """Client for fetching Bitcoin price data"""
    
    BASE_URL = "https://api.coingecko.com/api/v3"
    
    def __init__(self, rate_limit_delay: float = 1.0):
        """
        Initialize the Bitcoin client
        
        Args:
            rate_limit_delay: Delay between API calls (CoinGecko has stricter limits)
        """
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'polymarket-bitcoin-analysis/0.1.0'
        })
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make a request to the CoinGecko API with error handling
        
        Args:
            endpoint: API endpoint to call
            params: Query parameters
            
        Returns:
            JSON response or None if request failed
        """
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            # Rate limiting - CoinGecko is stricter
            time.sleep(self.rate_limit_delay)
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def get_current_price(self) -> Optional[Dict]:
        """
        Get current Bitcoin price and 24h statistics
        
        Returns:
            Dictionary with current price data or None if failed
        """
        params = {
            'ids': 'bitcoin',
            'vs_currencies': 'usd',
            'include_24hr_change': 'true',
            'include_24hr_vol': 'true',
            'include_market_cap': 'true',
            'include_last_updated_at': 'true'
        }
        
        response = self._make_request('/simple/price', params)
        
        if response and 'bitcoin' in response:
            btc_data = response['bitcoin']
            return {
                'current_price': btc_data.get('usd', 0),
                'market_cap': btc_data.get('usd_market_cap', 0),
                'volume_24h': btc_data.get('usd_24h_vol', 0),
                'change_24h_percent': btc_data.get('usd_24h_change', 0),
                'last_updated': btc_data.get('last_updated_at', int(time.time()))
            }
        
        return None
    
    def get_historical_prices(self, days: int = 7) -> Optional[Dict]:
        """
        Get historical Bitcoin prices for the last N days
        
        Args:
            days: Number of days of historical data to fetch
            
        Returns:
            Dictionary with historical price data or None if failed
        """
        params = {
            'vs_currency': 'usd',
            'days': str(days),
            'interval': 'daily' if days > 1 else 'hourly'
        }
        
        response = self._make_request('/coins/bitcoin/market_chart', params)
        
        if response:
            return {
                'prices': response.get('prices', []),
                'market_caps': response.get('market_caps', []),
                'total_volumes': response.get('total_volumes', [])
            }
        
        return None
    
    def get_daily_ohlc(self, days: int = 7) -> Optional[Dict]:
        """
        Get OHLC (Open, High, Low, Close) data for Bitcoin
        
        Args:
            days: Number of days of OHLC data to fetch
            
        Returns:
            Dictionary with OHLC data or None if failed
        """
        params = {
            'vs_currency': 'usd',
            'days': str(days)
        }
        
        response = self._make_request('/coins/bitcoin/ohlc', params)
        
        if response:
            # Response format: [[timestamp, open, high, low, close], ...]
            ohlc_data = []
            for entry in response:
                if len(entry) >= 5:
                    timestamp, open_price, high, low, close = entry[:5]
                    date = datetime.fromtimestamp(timestamp / 1000).date()
                    
                    ohlc_data.append({
                        'date': date,
                        'timestamp': timestamp,
                        'open': open_price,
                        'high': high,
                        'low': low,
                        'close': close,
                        'daily_return': (close - open_price) / open_price if open_price > 0 else 0,
                        'price_direction': 'UP' if close > open_price else 'DOWN'
                    })
            
            return {
                'ohlc_data': ohlc_data,
                'days_retrieved': len(ohlc_data)
            }
        
        return None
    
    def calculate_today_performance(self) -> Optional[Dict]:
        """
        Calculate Bitcoin's performance for today (or latest available day)
        
        Returns:
            Dictionary with today's performance data
        """
        # Get last 2 days to have today's open and current price
        historical = self.get_historical_prices(days=2)
        current = self.get_current_price()
        
        if not historical or not current:
            return None
        
        prices = historical['prices']
        if len(prices) < 2:
            return None
        
        # Get today's open (yesterday's close) and current price
        today_open = prices[-2][1]  # Yesterday's last price as today's open
        current_price = current['current_price']
        
        # Calculate performance
        daily_return = (current_price - today_open) / today_open if today_open > 0 else 0
        price_direction = 'UP' if daily_return > 0 else 'DOWN'
        
        return {
            'date': datetime.now().date(),
            'open_price': today_open,
            'current_price': current_price,
            'daily_return': daily_return,
            'daily_return_percent': daily_return * 100,
            'price_direction': price_direction,
            'volume_24h': current['volume_24h'],
            'is_up_today': daily_return > 0,
            'timestamp': datetime.now()
        }
    
    def get_comprehensive_data(self, days: int = 7) -> Dict:
        """
        Get comprehensive Bitcoin data including current price, historical data, and today's performance
        
        Args:
            days: Number of days of historical data to include
            
        Returns:
            Comprehensive data dictionary
        """
        print("Fetching comprehensive Bitcoin data...")
        
        # Get current price
        print("  1. Fetching current price...")
        current_data = self.get_current_price()
        
        # Get historical OHLC
        print(f"  2. Fetching {days} days of OHLC data...")
        ohlc_data = self.get_daily_ohlc(days=days)
        
        # Calculate today's performance
        print("  3. Calculating today's performance...")
        today_performance = self.calculate_today_performance()
        
        result = {
            'current_data': current_data,
            'historical_ohlc': ohlc_data,
            'today_performance': today_performance,
            'fetch_timestamp': datetime.now(),
            'success': True
        }
        
        # Validation
        if not current_data:
            print("  ⚠ Warning: Could not fetch current price data")
            result['success'] = False
        
        if not ohlc_data:
            print("  ⚠ Warning: Could not fetch historical OHLC data")
            result['success'] = False
        
        if not today_performance:
            print("  ⚠ Warning: Could not calculate today's performance")
            result['success'] = False
        
        if result['success']:
            current_price = current_data['current_price']
            today_return = today_performance['daily_return_percent']
            direction = today_performance['price_direction']
            
            print(f"  ✓ Current Bitcoin price: ${current_price:,.2f}")
            print(f"  ✓ Today's performance: {today_return:+.2f}% ({direction})")
            print(f"  ✓ Historical data: {ohlc_data['days_retrieved']} days retrieved")
        
        return result


def main():
    """Test the Bitcoin client"""
    print("Testing Bitcoin API client...")
    
    client = BitcoinClient()
    
    # Test current price
    print("\n1. Fetching current Bitcoin price...")
    current = client.get_current_price()
    if current:
        print(f"  Current price: ${current['current_price']:,.2f}")
        print(f"  24h change: {current['change_24h_percent']:+.2f}%")
        print(f"  24h volume: ${current['volume_24h']:,.0f}")
    
    # Test today's performance
    print("\n2. Calculating today's performance...")
    today = client.calculate_today_performance()
    if today:
        print(f"  Today's open: ${today['open_price']:,.2f}")
        print(f"  Current price: ${today['current_price']:,.2f}")
        print(f"  Daily return: {today['daily_return_percent']:+.2f}%")
        print(f"  Direction: {today['price_direction']}")
    
    # Test historical data
    print("\n3. Fetching historical OHLC data...")
    ohlc = client.get_daily_ohlc(days=3)
    if ohlc and ohlc['ohlc_data']:
        print(f"  Retrieved {len(ohlc['ohlc_data'])} days of data:")
        for day in ohlc['ohlc_data'][-3:]:  # Show last 3 days
            print(f"    {day['date']}: ${day['open']:.0f} → ${day['close']:.0f} ({day['daily_return']:+.2%})")
    
    # Test comprehensive data
    print("\n4. Fetching comprehensive data...")
    comprehensive = client.get_comprehensive_data(days=5)
    
    if comprehensive['success']:
        print("  ✓ All Bitcoin data fetched successfully!")
    else:
        print("  ⚠ Some Bitcoin data could not be fetched")
    
    print("\n✓ Bitcoin client test completed!")


if __name__ == "__main__":
    main()