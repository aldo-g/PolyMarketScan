"""
Polymarket API client for fetching market data
"""

import requests
import time
from typing import Dict, List, Optional
from datetime import datetime


class PolymarketClient:
    """Client for interacting with Polymarket API"""
    
    BASE_URL = "https://gamma-api.polymarket.com"
    
    def __init__(self, rate_limit_delay: float = 0.5):
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'polymarket-bitcoin-analysis/0.1.0'
        })
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            time.sleep(self.rate_limit_delay)
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None
    
    def get_bitcoin_daily_markets(self) -> List[Dict]:
        """Get Bitcoin daily 'up or down' markets using direct slug lookup"""
        
        # Known Bitcoin daily market patterns - add more as we find them
        daily_market_slugs = [
            "bitcoin-up-or-down-on-july-28",
            "bitcoin-up-or-down-on-july-29", 
            "bitcoin-up-or-down-on-july-30",
            # Add more dates as needed
        ]
        
        bitcoin_markets = []
        
        for slug in daily_market_slugs:
            print(f"Checking market: {slug}")
            response = self._make_request('/markets', {'slug': slug})
            
            if response and len(response) > 0:
                market = response[0]
                if market.get('active') and not market.get('closed'):
                    bitcoin_markets.append(market)
                    print(f"  ✅ Found active market: {market.get('question')}")
                else:
                    print(f"  ⏸️  Found inactive market: {market.get('question')}")
            else:
                print(f"  ❌ No market found for {slug}")
        
        # Also try the general search as fallback
        print("\nTrying general market search as backup...")
        general_markets = self._make_request('/markets', {'closed': 'false', 'limit': 500})
        
        if general_markets:
            for market in general_markets:
                question = market.get('question', '').lower()
                if ('bitcoin' in question and 
                    'up or down' in question and 
                    market.get('active') and 
                    not market.get('closed')):
                    
                    # Avoid duplicates
                    if not any(m.get('id') == market.get('id') for m in bitcoin_markets):
                        bitcoin_markets.append(market)
                        print(f"  ✅ Found via search: {market.get('question')}")
        
        print(f"\nTotal Bitcoin daily markets found: {len(bitcoin_markets)}")
        return bitcoin_markets
    
    def extract_market_odds(self, market_data: Dict) -> Dict:
        """Extract odds directly from market data (no orderbook needed)"""
        
        market_id = market_data.get('id', 'unknown')
        market_title = market_data.get('question', 'Unknown Market')
        
        # Extract outcome prices from market data
        outcome_prices = market_data.get('outcomePrices', [])
        
        yes_price = 0.5
        no_price = 0.5
        
        if outcome_prices and len(outcome_prices) >= 2:
            try:
                # Parse the price strings
                yes_price = float(outcome_prices[0])
                no_price = float(outcome_prices[1])
            except (ValueError, TypeError) as e:
                print(f"Warning: Could not parse outcome prices {outcome_prices}: {e}")
                # Use best bid/ask as fallback
                yes_price = market_data.get('bestBid', 0.5)
                no_price = 1 - market_data.get('bestAsk', 0.5)
        
        # These are already probabilities (0-1), not prices
        yes_probability = yes_price
        no_probability = no_price
        
        # Get volume and liquidity
        volume = market_data.get('volume', 0)
        liquidity = market_data.get('liquidity', 0)
        volume_24hr = market_data.get('volume24hr', 0)
        
        return {
            'market_id': market_id,
            'market_title': market_title,
            'timestamp': datetime.now(),
            'yes_price': yes_price,
            'no_price': no_price,
            'yes_probability': yes_probability,
            'no_probability': no_probability,
            'volume': volume,
            'volume_24hr': volume_24hr,
            'liquidity': liquidity,
            'total_volume': volume,
            'active': market_data.get('active', False),
            'raw_market_data': market_data
        }
    
    def fetch_current_bitcoin_odds(self) -> List[Dict]:
        """Fetch current odds for Bitcoin daily markets"""
        
        bitcoin_markets = self.get_bitcoin_daily_markets()
        odds_data = []
        
        print(f"\nExtracting odds from {len(bitcoin_markets)} markets...")
        
        for i, market in enumerate(bitcoin_markets, 1):
            print(f"\n{i}. Processing: {market.get('question', 'Unknown')}")
            
            try:
                odds = self.extract_market_odds(market)
                odds_data.append(odds)
                
                # Show the results
                direction = "DOWN" if odds['yes_probability'] < 0.5 else "UP"
                confidence = max(odds['yes_probability'], odds['no_probability'])
                
                print(f"   Market prediction: Bitcoin will go {direction}")
                print(f"   Confidence: {confidence:.1%}")
                print(f"   Volume (24hr): ${odds['volume_24hr']:,.0f}")
                
            except Exception as e:
                print(f"   ❌ Error processing market: {e}")
        
        print(f"\n✅ Successfully processed {len(odds_data)} markets")
        return odds_data


if __name__ == "__main__":
    print("Testing updated Polymarket client...")
    
    client = PolymarketClient()
    odds_data = client.fetch_current_bitcoin_odds()
    
    if odds_data:
        print(f"\n🎯 SUMMARY:")
        for odds in odds_data:
            print(f"Market: {odds['market_title']}")
            print(f"Bitcoin UP probability: {odds['yes_probability']:.2%}")
            print(f"Bitcoin DOWN probability: {odds['no_probability']:.2%}")
            print()
    
    print("✅ Test completed!")
