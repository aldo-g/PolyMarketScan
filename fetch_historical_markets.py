from polymarket_analysis.data.polymarket_client import PolymarketClient
import json
from datetime import datetime, timedelta

class HistoricalMarketFetcher:
    def __init__(self):
        self.client = PolymarketClient()
    
    def find_resolved_bitcoin_markets(self, days_back=30):
        """Find resolved Bitcoin daily markets from recent weeks"""
        print(f"🔍 Searching for resolved Bitcoin markets from last {days_back} days...")
        
        # Search closed/resolved markets
        resolved_markets = self.client._make_request('/markets', {
            'closed': 'true',  # Get resolved markets
            'limit': 500,
            'offset': 0
        })
        
        bitcoin_resolved = []
        
        if resolved_markets:
            for market in resolved_markets:
                question = market.get('question', '').lower()
                
                # Look for Bitcoin daily markets
                if ('bitcoin' in question and 
                    ('up or down' in question or 
                     'july' in question or 
                     'june' in question or
                     'daily' in question)):
                    
                    # Check if it was resolved recently
                    end_date = market.get('endDate', '')
                    if end_date:
                        try:
                            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                            days_ago = (datetime.now(end_dt.tzinfo) - end_dt).days
                            
                            if days_ago <= days_back:
                                bitcoin_resolved.append(market)
                                print(f"  ✅ Found: {market.get('question')} (ended {days_ago} days ago)")
                        except:
                            pass
        
        print(f"\n📊 Found {len(bitcoin_resolved)} resolved Bitcoin markets")
        return bitcoin_resolved
    
    def get_market_historical_data(self, market):
        """Try different approaches to get historical odds data"""
        market_id = market.get('id')
        slug = market.get('slug', '')
        condition_id = market.get('conditionId', '')
        
        print(f"\n📈 Fetching historical data for: {market.get('question')}")
        print(f"   Market ID: {market_id}")
        print(f"   Slug: {slug}")
        
        historical_data = {}
        
        # Key insight from the data: We saw a series field that might contain historical info
        # Let's look for series data which might have the chart information
        events_data = market.get('events', [])
        
        if events_data:
            print(f"  🔍 Found events data with {len(events_data)} events")
            for event in events_data:
                series_info = event.get('series', [])
                if series_info:
                    print(f"     Series found: {series_info}")
                    # Try to get series data
                    for series in series_info:
                        series_slug = series.get('slug', '')
                        if series_slug:
                            print(f"     Trying series endpoint: /series/{series_slug}")
                            series_data = self.client._make_request(f'/series/{series_slug}')
                            if series_data:
                                historical_data[f'series_{series_slug}'] = series_data
                                print(f"     ✅ Series data found!")
        
        # Try different chart/history endpoints 
        history_endpoints = [
            f'/markets/{market_id}/chart',
            f'/chart/{market_id}',
            f'/price-history/{market_id}',
            f'/markets/{market_id}/history',
            f'/events/{market_id}/chart',
            f'/series/btc-up-or-down-daily/markets/{market_id}/chart',
        ]
        
        for endpoint in history_endpoints:
            try:
                result = self.client._make_request(endpoint)
                if result:
                    print(f"  ✅ SUCCESS: {endpoint}")
                    historical_data[endpoint] = result
                    
                    # Save to file
                    safe_name = endpoint.replace('/', '_').replace('?', '_')
                    filename = f"data/historical_{market_id}_{safe_name}.json"
                    try:
                        with open(filename, 'w') as f:
                            json.dump(result, f, indent=2, default=str)
                        print(f"     💾 Saved: {filename}")
                    except Exception as e:
                        print(f"     ⚠️ Could not save: {e}")
                        
            except Exception as e:
                pass  # Continue silently
        
        # Try the series endpoint directly since we know there's a BTC daily series
        series_endpoints = [
            '/series/btc-up-or-down-daily',
            '/series/btc-up-or-down-daily/markets',
            f'/series/btc-up-or-down-daily/markets/{market_id}',
            '/events/btc-up-or-down-daily',
        ]
        
        for endpoint in series_endpoints:
            try:
                result = self.client._make_request(endpoint)
                if result:
                    print(f"  ✅ SERIES SUCCESS: {endpoint}")
                    historical_data[f'series_{endpoint.replace("/", "_")}'] = result
            except:
                pass
        
        return historical_data


def main():
    fetcher = HistoricalMarketFetcher()
    
    print("=== FETCHING HISTORICAL BITCOIN MARKET DATA ===\n")
    
    # Find resolved markets
    resolved_markets = fetcher.find_resolved_bitcoin_markets(days_back=14)
    
    if not resolved_markets:
        print("❌ No resolved Bitcoin markets found.")
        print("Let's try looking at ALL resolved markets to see what's available...")
        
        # Get any resolved markets to see the structure
        all_resolved = fetcher.client._make_request('/markets', {'closed': 'true', 'limit': 50})
        
        if all_resolved:
            print(f"\nFound {len(all_resolved)} total resolved markets:")
            for i, market in enumerate(all_resolved[:10], 1):
                question = market.get('question', 'Unknown')
                end_date = market.get('endDate', 'Unknown')
                print(f"{i:2}. {question} (ended: {end_date})")
        
        return
    
    # Process the first resolved market to test
    if resolved_markets:
        print(f"\n{'='*60}")
        print(f"TESTING HISTORICAL DATA FETCH")
        print(f"{'='*60}")
        
        test_market = resolved_markets[0]
        historical_data = fetcher.get_market_historical_data(test_market)
        
        if historical_data:
            print(f"\n✅ Found {len(historical_data)} historical data sources!")
            
            for key, data in historical_data.items():
                print(f"\n📊 Data source: {key}")
                if isinstance(data, list):
                    print(f"   Type: List with {len(data)} items")
                    if len(data) > 0 and isinstance(data[0], dict):
                        sample_keys = list(data[0].keys())[:10]  # First 10 keys
                        print(f"   Sample keys: {sample_keys}")
                elif isinstance(data, dict):
                    print(f"   Type: Dict with keys: {list(data.keys())[:10]}")
        
        else:
            print("❌ No historical data found")
            
            # Let's examine the raw market data more closely
            print("\n🔍 Examining market structure for clues...")
            print(f"Market keys: {list(test_market.keys())}")
            
            # Look for any field that might contain historical data
            for key, value in test_market.items():
                if isinstance(value, list) and len(value) > 0:
                    print(f"   {key}: List with {len(value)} items")
                elif 'price' in key.lower() or 'volume' in key.lower() or 'history' in key.lower():
                    print(f"   {key}: {value}")


if __name__ == "__main__":
    main()
