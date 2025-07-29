from polymarket_analysis.data.polymarket_client import PolymarketClient

client = PolymarketClient()

print("=== EXTRACTING BITCOIN MARKET DATA ===")
event_slug = "bitcoin-up-or-down-on-july-28"

response = client._make_request('/markets', {'slug': event_slug})

if response and len(response) > 0:
    market = response[0]  # Get the first (and likely only) market
    
    print(f"✅ Market: {market.get('question')}")
    print(f"Market ID: {market.get('id')}")
    print(f"Condition ID: {market.get('conditionId')}")
    print(f"Active: {market.get('active')}")
    print(f"Closed: {market.get('closed')}")
    
    # Extract prices from the market data itself
    print(f"\n--- EXTRACTING PRICES ---")
    print(f"Outcome Prices: {market.get('outcomePrices', 'Not available')}")
    print(f"Best Bid: {market.get('bestBid', 'Not available')}")
    print(f"Best Ask: {market.get('bestAsk', 'Not available')}")
    print(f"Last Trade Price: {market.get('lastTradePrice', 'Not available')}")
    print(f"Volume: {market.get('volume', 'Not available')}")
    print(f"Volume 24hr: {market.get('volume24hr', 'Not available')}")
    print(f"Liquidity: {market.get('liquidity', 'Not available')}")
    
    # Check outcomes structure
    outcomes = market.get('outcomes', [])
    print(f"\n--- OUTCOMES ---")
    print(f"Number of outcomes: {len(outcomes)}")
    for i, outcome in enumerate(outcomes):
        print(f"Outcome {i+1}: {outcome}")
    
    # Try different orderbook approaches
    print(f"\n--- TRYING ORDERBOOK APPROACHES ---")
    
    # Try with conditionId
    condition_id = market.get('conditionId')
    if condition_id:
        print(f"Trying orderbook with conditionId: {condition_id}")
        orderbook1 = client._make_request('/book', {'token_id': condition_id})
        if orderbook1:
            print(f"✅ Success with conditionId!")
            print(f"Orderbook keys: {list(orderbook1.keys())}")
        else:
            print("❌ Failed with conditionId")
    
    # Try with clobTokenIds if available
    clob_ids = market.get('clobTokenIds', [])
    if clob_ids:
        print(f"Trying with CLOB token IDs: {clob_ids}")
        for clob_id in clob_ids:
            orderbook2 = client._make_request('/book', {'token_id': clob_id})
            if orderbook2:
                print(f"✅ Success with CLOB ID {clob_id}!")
                print(f"Orderbook keys: {list(orderbook2.keys())}")
                break
        else:
            print("❌ Failed with all CLOB IDs")
    
    # Try different orderbook endpoints
    orderbook_endpoints = ['/orderbook', '/orders', '/book']
    for endpoint in orderbook_endpoints:
        print(f"Trying endpoint: {endpoint}")
        result = client._make_request(endpoint, {'market': market.get('id')})
        if result:
            print(f"✅ Success with {endpoint}!")
            break
    
    # Extract what we can from the market data itself
    print(f"\n--- CREATING ODDS FROM MARKET DATA ---")
    
    outcome_prices = market.get('outcomePrices', [])
    if outcome_prices and len(outcome_prices) >= 2:
        yes_price = float(outcome_prices[0]) if outcome_prices[0] else 0.5
        no_price = float(outcome_prices[1]) if outcome_prices[1] else 0.5
        
        # Normalize to probabilities
        total = yes_price + no_price
        if total > 0:
            yes_prob = yes_price / total
            no_prob = no_price / total
        else:
            yes_prob = yes_price
            no_prob = no_price
        
        print(f"Yes price: {yes_price}")
        print(f"No price: {no_price}") 
        print(f"Yes probability: {yes_prob:.2%}")
        print(f"No probability: {no_prob:.2%}")
        
        # Create odds data structure
        odds_data = {
            'market_id': market.get('id'),
            'market_title': market.get('question'),
            'yes_price': yes_price,
            'no_price': no_price,
            'yes_probability': yes_prob,
            'no_probability': no_prob,
            'volume': market.get('volume', 0),
            'liquidity': market.get('liquidity', 0),
            'active': market.get('active', False)
        }
        
        print(f"\n🎯 FINAL ODDS DATA:")
        print(f"Market: {odds_data['market_title']}")
        print(f"Yes probability: {odds_data['yes_probability']:.1%}")
        print(f"Volume: ${odds_data['volume']:,.0f}")
        print(f"✅ Successfully extracted market data!")
        
    else:
        print("❌ Could not extract outcome prices")
        print(f"Raw outcome prices: {outcome_prices}")

else:
    print("❌ No market data found")
