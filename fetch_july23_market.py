from polymarket_analysis.data.polymarket_client import PolymarketClient
import json
from datetime import datetime

class July23MarketFetcher:
    def __init__(self):
        self.client = PolymarketClient()
    
    def find_july23_market(self):
        """Find the specific July 23 Bitcoin market"""
        print("🔍 Searching for 'Bitcoin Up or Down on July 23?' market...")
        
        # Try the direct slug approach first
        slug = "bitcoin-up-or-down-on-july-23"
        response = self.client._make_request('/markets', {'slug': slug})
        
        if response and len(response) > 0:
            market = response[0]
            print(f"✅ Found via slug: {market.get('question')}")
            print(f"   Market ID: {market.get('id')}")
            print(f"   Closed: {market.get('closed')}")
            print(f"   End Date: {market.get('endDate')}")
            return market
        
        # If slug doesn't work, search through closed markets
        print("Slug not found, searching through closed markets...")
        
        closed_markets = self.client._make_request('/markets', {
            'closed': 'true',
            'limit': 1000,  # Get more markets
            'offset': 0
        })
        
        if closed_markets:
            for market in closed_markets:
                question = market.get('question', '')
                if 'july 23' in question.lower() and 'bitcoin' in question.lower():
                    print(f"✅ Found in closed markets: {question}")
                    print(f"   Market ID: {market.get('id')}")
                    print(f"   End Date: {market.get('endDate')}")
                    return market
        
        print("❌ Market not found")
        return None
    
    def extract_all_possible_data(self, market):
        """Extract every possible piece of historical data from the market"""
        print(f"\n📊 EXTRACTING ALL DATA FROM JULY 23 MARKET")
        print(f"Market: {market.get('question')}")
        
        market_id = market.get('id')
        slug = market.get('slug', '')
        condition_id = market.get('conditionId', '')
        
        all_data = {}
        
        # 1. Save the full market data first
        all_data['full_market_data'] = market
        
        # 2. Try all possible historical endpoints
        print(f"\n🔍 Testing historical endpoints...")
        
        endpoints_to_try = [
            # Market-specific endpoints
            f'/markets/{market_id}',
            f'/markets/{market_id}/history',
            f'/markets/{market_id}/chart',
            f'/markets/{market_id}/prices',
            f'/markets/{market_id}/trades',
            f'/markets/{market_id}/candles',
            f'/markets/{market_id}/time-series',
            
            # Slug-based endpoints
            f'/markets?slug={slug}',
            f'/markets?slug={slug}&include_history=true',
            f'/markets?slug={slug}&include_chart=true',
            f'/markets?slug={slug}&include_trades=true',
            f'/markets?slug={slug}&include_prices=true',
            
            # Chart endpoints
            f'/chart/{market_id}',
            f'/charts/{market_id}',
            f'/price-history/{market_id}',
            f'/history/{market_id}',
            
            # Condition-based endpoints
            f'/conditions/{condition_id}/history',
            f'/conditions/{condition_id}/chart',
            f'/conditions/{condition_id}/prices',
            
            # Event-based endpoints
            f'/events/{slug}',
            f'/events/{slug}/chart',
            f'/events/{slug}/history',
            f'/events/{slug}/prices',
            
            # Series endpoints (we know it's part of btc-up-or-down-daily series)
            f'/series/btc-up-or-down-daily',
            f'/series/btc-up-or-down-daily/history',
            f'/series/btc-up-or-down-daily/charts',
            f'/series/btc-up-or-down-daily/markets',
            f'/series/btc-up-or-down-daily/markets/{market_id}',
            f'/series/btc-up-or-down-daily/markets/{market_id}/chart',
            
            # Alternative API patterns
            f'/market-data/{market_id}',
            f'/market-history/{market_id}',
            f'/historical/{market_id}',
            f'/time-series/{market_id}',
        ]
        
        working_endpoints = []
        
        for endpoint in endpoints_to_try:
            try:
                result = self.client._make_request(endpoint)
                if result is not None:
                    print(f"  ✅ SUCCESS: {endpoint}")
                    
                    # Analyze the response
                    if isinstance(result, list):
                        print(f"     → List with {len(result)} items")
                        if len(result) > 0 and isinstance(result[0], dict):
                            sample_keys = list(result[0].keys())
                            print(f"     → Sample keys: {sample_keys[:8]}...")
                            
                            # Look for time/price fields
                            time_keys = [k for k in sample_keys if 'time' in k.lower() or 'date' in k.lower()]
                            price_keys = [k for k in sample_keys if 'price' in k.lower() or 'prob' in k.lower() or 'outcome' in k.lower()]
                            
                            if time_keys and price_keys:
                                print(f"     → 🎯 POTENTIAL TIME SERIES! Time: {time_keys}, Price: {price_keys}")
                    
                    elif isinstance(result, dict):
                        print(f"     → Dict with keys: {list(result.keys())[:8]}...")
                    
                    # Save the data
                    safe_endpoint = endpoint.replace('/', '_').replace('?', '_').replace('=', '_')
                    all_data[safe_endpoint] = result
                    working_endpoints.append(endpoint)
                    
                    # Save to file immediately
                    filename = f"data/july23_{market_id}_{safe_endpoint}.json"
                    try:
                        with open(filename, 'w') as f:
                            json.dump(result, f, indent=2, default=str)
                        print(f"     → 💾 Saved: {filename}")
                    except Exception as e:
                        print(f"     → ⚠️ Save failed: {e}")
            
            except Exception as e:
                pass  # Continue silently
        
        # 3. Try with different parameters for successful endpoints
        if working_endpoints:
            print(f"\n🔧 Testing parameters on working endpoints...")
            
            for endpoint in working_endpoints[:3]:  # Test top 3 working endpoints
                test_params = [
                    {'include_history': 'true'},
                    {'include_chart': 'true'},
                    {'include_trades': 'true'},
                    {'interval': '15m'},
                    {'resolution': '15'},
                    {'granularity': '900'},
                    {'period': '1d'},
                    {'from': '2025-07-22', 'to': '2025-07-24'},
                ]
                
                for params in test_params:
                    try:
                        result = self.client._make_request(endpoint, params)
                        if result:
                            param_str = '_'.join(f"{k}{v}" for k, v in params.items())
                            print(f"  ✅ PARAMS SUCCESS: {endpoint}?{param_str}")
                            all_data[f"{endpoint.replace('/', '_')}_{param_str}"] = result
                    except:
                        pass
        
        print(f"\n📈 SUMMARY:")
        print(f"Total data sources found: {len(all_data)}")
        print(f"Working endpoints: {len(working_endpoints)}")
        
        return all_data
    
    def analyze_for_time_series(self, all_data):
        """Look through all the data for potential time series"""
        print(f"\n🎯 ANALYZING FOR TIME SERIES DATA...")
        
        time_series_candidates = []
        
        for source_name, data in all_data.items():
            if source_name == 'full_market_data':
                continue
                
            print(f"\n📊 Analyzing: {source_name}")
            
            if isinstance(data, list) and len(data) > 0:
                # Check if this is a time series
                sample = data[0]
                if isinstance(sample, dict):
                    keys = list(sample.keys())
                    
                    # Look for time and price fields
                    time_fields = [k for k in keys if any(t in k.lower() for t in ['time', 'date', 'timestamp', 'created'])]
                    price_fields = [k for k in keys if any(p in k.lower() for p in ['price', 'prob', 'outcome', 'value', 'odds'])]
                    
                    if time_fields and price_fields:
                        print(f"  🎯 TIME SERIES CANDIDATE!")
                        print(f"     Length: {len(data)} points")
                        print(f"     Time fields: {time_fields}")
                        print(f"     Price fields: {price_fields}")
                        print(f"     Sample point: {sample}")
                        
                        time_series_candidates.append({
                            'source': source_name,
                            'data': data,
                            'time_fields': time_fields,
                            'price_fields': price_fields,
                            'length': len(data)
                        })
                    
                    elif len(keys) > 2:  # Might still be useful data
                        print(f"     Other data with {len(data)} items: {keys[:5]}...")
            
            elif isinstance(data, dict):
                # Check if dict contains time series data
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 5:  # Potential time series
                        print(f"     Dict contains list '{key}' with {len(value)} items")
                        if len(value) > 0 and isinstance(value[0], (dict, list)):
                            print(f"       First item: {value[0]}")
        
        if time_series_candidates:
            print(f"\n🏆 FOUND {len(time_series_candidates)} TIME SERIES CANDIDATES!")
            
            for i, candidate in enumerate(time_series_candidates, 1):
                print(f"\n{i}. {candidate['source']}")
                print(f"   Length: {candidate['length']} data points")
                print(f"   Time fields: {candidate['time_fields']}")
                print(f"   Price fields: {candidate['price_fields']}")
                
                # Show first and last few points
                data = candidate['data']
                print(f"   First point: {data[0]}")
                if len(data) > 1:
                    print(f"   Last point: {data[-1]}")
        
        else:
            print("❌ No clear time series data found")
        
        return time_series_candidates


def main():
    fetcher = July23MarketFetcher()
    
    print("=== FETCHING JULY 23 BITCOIN MARKET HISTORICAL DATA ===\n")
    
    # Find the specific market
    market = fetcher.find_july23_market()
    
    if not market:
        return
    
    # Extract all possible data
    all_data = fetcher.extract_all_possible_data(market)
    
    # Analyze for time series
    time_series = fetcher.analyze_for_time_series(all_data)
    
    # Save everything to a master file
    master_file = f"data/july23_complete_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        with open(master_file, 'w') as f:
            json.dump(all_data, f, indent=2, default=str)
        print(f"\n💾 All data saved to: {master_file}")
    except Exception as e:
        print(f"⚠️ Could not save master file: {e}")
    
    if time_series:
        print(f"\n🎉 SUCCESS! Found historical time series data!")
        print(f"Check the saved JSON files in the data/ directory")
    else:
        print(f"\n🤔 No time series found, but {len(all_data)} data sources saved for analysis")


if __name__ == "__main__":
    main()
