from polymarket_analysis.data.polymarket_client import PolymarketClient

client = PolymarketClient()

print("=== SEARCHING FOR 'Bitcoin Up or Down on' MARKETS ===")

# Fetch markets in batches and look for the exact pattern
all_bitcoin_updown = []
offset = 0
limit = 100

while offset < 2000:  # Search through first 2000 markets
    print(f"Searching markets {offset}-{offset+limit}...")
    
    markets_batch = client._make_request('/markets', {
        'closed': 'false',
        'limit': limit,
        'offset': offset
    })
    
    if not markets_batch or len(markets_batch) == 0:
        break
    
    # Look for exact pattern
    for market in markets_batch:
        question = market.get('question', '')
        if question.startswith('Bitcoin Up or Down on'):
            all_bitcoin_updown.append(market)
            print(f"  ✅ FOUND: {question}")
            # Also print some details
            print(f"      ID: {market.get('condition_id', market.get('id', 'unknown'))}")
            print(f"      Active: {not market.get('closed', True)}")
    
    offset += limit

print(f"\n🎯 TOTAL 'Bitcoin Up or Down on' MARKETS FOUND: {len(all_bitcoin_updown)}")

if all_bitcoin_updown:
    print("\n=== ALL BITCOIN UP/DOWN MARKETS ===")
    for i, market in enumerate(all_bitcoin_updown, 1):
        question = market.get('question', '')
        market_id = market.get('condition_id', market.get('id', 'unknown'))
        print(f"{i}. {question}")
        print(f"   ID: {market_id}")
        print(f"   Closed: {market.get('closed', 'unknown')}")
        print()
    
    # Test fetching odds for the first one
    if all_bitcoin_updown:
        print("=== TESTING ODDS FETCH FOR FIRST MARKET ===")
        test_market = all_bitcoin_updown[0]
        test_id = test_market.get('condition_id', test_market.get('id'))
        
        if test_id:
            print(f"Fetching odds for: {test_market.get('question', '')}")
            orderbook = client.get_market_orderbook(test_id)
            
            if orderbook:
                print("✅ Successfully fetched orderbook!")
                print(f"Orderbook keys: {list(orderbook.keys())}")
            else:
                print("❌ Could not fetch orderbook")
        
else:
    print("❌ No 'Bitcoin Up or Down on' markets found")
    print("\n🔍 Let's try a broader search...")
    
    # Fallback: search for any market with "up or down" in the title
    print("\n=== SEARCHING FOR ANY 'up or down' MARKETS ===")
    offset = 0
    while offset < 1000:
        markets_batch = client._make_request('/markets', {
            'closed': 'false',
            'limit': 100,
            'offset': offset
        })
        
        if not markets_batch:
            break
            
        for market in markets_batch:
            question = market.get('question', '').lower()
            if 'up or down' in question and 'bitcoin' in question:
                print(f"  FOUND: {market.get('question', '')}")
        
        offset += 100
