from polymarket_analysis.data.polymarket_client import PolymarketClient
import json
from datetime import datetime

client = PolymarketClient()

print("=== EXPLORING ALL AVAILABLE MARKET DATA ===")

# Get our Bitcoin market
event_slug = "bitcoin-up-or-down-on-july-28"
response = client._make_request('/markets', {'slug': event_slug})

if response and len(response) > 0:
    market = response[0]
    market_id = market.get('id')
    condition_id = market.get('conditionId')
    
    print(f"Market: {market.get('question')}")
    print(f"Market ID: {market_id}")
    print(f"Condition ID: {condition_id}")
    
    # 1. FULL MARKET DATA DUMP
    print(f"\n=== 1. COMPLETE MARKET DATA STRUCTURE ===")
    print("All available fields:")
    for key, value in market.items():
        print(f"  {key}: {value}")
    
    # 2. TRY HISTORICAL DATA ENDPOINTS
    print(f"\n=== 2. TESTING HISTORICAL DATA ENDPOINTS ===")
    
    historical_endpoints = [
        f'/markets/{market_id}/history',
        f'/markets/{market_id}/trades',
        f'/markets/{market_id}/prices',
        f'/markets/{market_id}/chart',
        f'/markets/{market_id}/candles',
        f'/market-history/{market_id}',
        f'/trades?market={market_id}',
        f'/trades?condition_id={condition_id}',
        f'/price-history/{market_id}',
        f'/chart/{market_id}',
        f'/candles/{market_id}',
        f'/history?market={market_id}',
        f'/events/{market_id}/prices',
        f'/time-series/{market_id}',
    ]
    
    working_endpoints = []
    
    for endpoint in historical_endpoints:
        print(f"Testing: {endpoint}")
        result = client._make_request(endpoint)
        
        if result is not None:
            print(f"  ✅ SUCCESS! Response type: {type(result)}")
            if isinstance(result, list):
                print(f"     List with {len(result)} items")
                if len(result) > 0:
                    print(f"     Sample item keys: {list(result[0].keys()) if isinstance(result[0], dict) else 'Not dict'}")
            elif isinstance(result, dict):
                print(f"     Dict with keys: {list(result.keys())}")
            
            working_endpoints.append((endpoint, result))
            
            # Save the data to examine
            timestamp = datetime.now().strftime("%H%M%S")
            filename = f"market_data_{endpoint.replace('/', '_').replace('?', '_')}_{timestamp}.json"
            filename = filename.replace('=', '_')
            
            try:
                with open(f"data/{filename}", 'w') as f:
                    json.dump(result, f, indent=2, default=str)
                print(f"     💾 Saved to: data/{filename}")
            except Exception as e:
                print(f"     ⚠️ Could not save: {e}")
        else:
            print(f"  ❌ No data")
    
    # 3. TRY DIFFERENT TIME PARAMETERS
    print(f"\n=== 3. TESTING TIME-BASED QUERIES ===")
    
    time_endpoints = [
        f'/trades?market={market_id}&limit=100',
        f'/trades?condition_id={condition_id}&limit=100',
        f'/markets/{market_id}?include_history=true',
        f'/markets?slug={event_slug}&include_trades=true',
        f'/candles?market={market_id}&resolution=1&from=1722100000&to=1722200000',  # Unix timestamps
        f'/price-history?market={market_id}&interval=1h',
        f'/chart-data?market={market_id}&period=1d',
    ]
    
    for endpoint in time_endpoints:
        print(f"Testing time query: {endpoint}")
        result = client._make_request(endpoint)
        
        if result is not None:
            print(f"  ✅ Time data found!")
            working_endpoints.append((endpoint, result))
    
    # 4. CHECK IF THERE ARE WEBSOCKET ENDPOINTS OR REAL-TIME DATA
    print(f"\n=== 4. CHECKING REAL-TIME DATA OPTIONS ===")
    
    realtime_endpoints = [
        '/ws',  # WebSocket endpoint
        '/stream',
        '/live',
        '/subscribe',
        f'/markets/{market_id}/live',
        f'/markets/{market_id}/stream'
    ]
    
    for endpoint in realtime_endpoints:
        print(f"Testing real-time endpoint: {endpoint}")
        result = client._make_request(endpoint)
        if result:
            print(f"  ✅ Real-time endpoint found: {list(result.keys()) if isinstance(result, dict) else type(result)}")
    
    # 5. SUMMARY OF FINDINGS
    print(f"\n=== 5. SUMMARY OF DATA AVAILABILITY ===")
    print(f"Working endpoints found: {len(working_endpoints)}")
    
    for endpoint, data in working_endpoints:
        print(f"\n📊 {endpoint}:")
        if isinstance(data, list) and len(data) > 0:
            sample = data[0]
            if isinstance(sample, dict):
                print(f"   Sample fields: {list(sample.keys())}")
                # Look for timestamp fields
                time_fields = [k for k in sample.keys() if 'time' in k.lower() or 'date' in k.lower() or 'created' in k.lower()]
                if time_fields:
                    print(f"   Time fields: {time_fields}")
                # Look for price fields  
                price_fields = [k for k in sample.keys() if 'price' in k.lower() or 'outcome' in k.lower() or 'prob' in k.lower()]
                if price_fields:
                    print(f"   Price fields: {price_fields}")
        elif isinstance(data, dict):
            print(f"   Keys: {list(data.keys())}")
    
    # 6. CURRENT SNAPSHOT FOR COMPARISON
    print(f"\n=== 6. CURRENT MARKET SNAPSHOT ===")
    current_snapshot = {
        'timestamp': datetime.now().isoformat(),
        'yes_probability': float(market.get('outcomePrices', [0.5, 0.5])[0]),
        'no_probability': float(market.get('outcomePrices', [0.5, 0.5])[1]),
        'volume': market.get('volume', 0),
        'volume_24hr': market.get('volume24hr', 0),
        'liquidity': market.get('liquidity', 0),
        'best_bid': market.get('bestBid', 0),
        'best_ask': market.get('bestAsk', 0),
        'last_trade_price': market.get('lastTradePrice', 0)
    }
    
    print("Current snapshot:")
    for key, value in current_snapshot.items():
        print(f"  {key}: {value}")
    
    # Save current snapshot
    with open(f"data/current_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
        json.dump(current_snapshot, f, indent=2)
    
    print(f"\n💡 RECOMMENDATIONS FOR TRACKING:")
    print("1. If historical trades found → can reconstruct price movements")
    print("2. If no historical data → need to poll current prices regularly") 
    print("3. Check data/ folder for saved responses to analyze structure")
    print("4. Consider WebSocket connection for real-time updates")

else:
    print("❌ Could not find the Bitcoin market")
