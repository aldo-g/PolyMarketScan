#!/usr/bin/env python3
"""
One-time data fetch script for Polymarket Bitcoin analysis
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from polymarket_analysis.data.polymarket_client import PolymarketClient
from polymarket_analysis.data.bitcoin_client import BitcoinClient


def save_data_to_json(data: dict, filename: str, data_dir: Path) -> Path:
    """Save data to JSON file with timestamp"""
    data_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = data_dir / f"{timestamp}_{filename}"
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    
    return filepath


def fetch_polymarket_data() -> dict:
    """Fetch current Polymarket Bitcoin odds"""
    print("=== FETCHING POLYMARKET DATA ===")
    
    client = PolymarketClient()
    
    try:
        # Get Bitcoin markets
        bitcoin_markets = client.get_bitcoin_markets(closed=False)
        
        if not bitcoin_markets:
            print("⚠ No Bitcoin markets found")
            return {'success': False, 'error': 'No Bitcoin markets found'}
        
        # Get current odds
        odds_data = client.fetch_current_bitcoin_odds()
        
        return {
            'success': True,
            'timestamp': datetime.now(),
            'markets_found': len(bitcoin_markets),
            'bitcoin_markets': bitcoin_markets,
            'odds_data': odds_data,
            'total_markets_with_odds': len(odds_data)
        }
        
    except Exception as e:
        print(f"Error fetching Polymarket data: {e}")
        return {'success': False, 'error': str(e)}


def fetch_bitcoin_data() -> dict:
    """Fetch current Bitcoin price and historical data"""
    print("=== FETCHING BITCOIN DATA ===")
    
    client = BitcoinClient()
    
    try:
        comprehensive_data = client.get_comprehensive_data(days=7)
        return comprehensive_data
        
    except Exception as e:
        print(f"Error fetching Bitcoin data: {e}")
        return {'success': False, 'error': str(e)}


def analyze_fetched_data(polymarket_data: dict, bitcoin_data: dict) -> dict:
    """Perform basic analysis on the fetched data"""
    print("=== ANALYZING FETCHED DATA ===")
    
    analysis = {
        'fetch_summary': {
            'timestamp': datetime.now(),
            'polymarket_success': polymarket_data.get('success', False),
            'bitcoin_success': bitcoin_data.get('success', False)
        }
    }
    
    # Polymarket analysis
    if polymarket_data.get('success'):
        odds_data = polymarket_data.get('odds_data', [])
        
        if odds_data:
            probabilities = [odds['yes_probability'] for odds in odds_data]
            volumes = [odds['total_volume'] for odds in odds_data]
            
            analysis['polymarket_summary'] = {
                'total_markets': len(odds_data),
                'average_yes_probability': sum(probabilities) / len(probabilities),
                'min_yes_probability': min(probabilities),
                'max_yes_probability': max(probabilities),
                'total_volume': sum(volumes),
                'average_volume': sum(volumes) / len(volumes) if volumes else 0,
                'markets_above_50pct': sum(1 for p in probabilities if p > 0.5),
                'markets_high_confidence': sum(1 for p in probabilities if p > 0.7 or p < 0.3)
            }
            
            print(f"  ✓ Found {len(odds_data)} markets with odds")
            print(f"  ✓ Average 'Bitcoin up' probability: {analysis['polymarket_summary']['average_yes_probability']:.1%}")
            print(f"  ✓ Total volume across markets: {analysis['polymarket_summary']['total_volume']:.0f}")
    
    # Bitcoin analysis
    if bitcoin_data.get('success'):
        today_perf = bitcoin_data.get('today_performance', {})
        current_data = bitcoin_data.get('current_data', {})
        
        if today_perf and current_data:
            analysis['bitcoin_summary'] = {
                'current_price': current_data.get('current_price', 0),
                'today_return_percent': today_perf.get('daily_return_percent', 0),
                'price_direction': today_perf.get('price_direction', 'UNKNOWN'),
                'is_up_today': today_perf.get('is_up_today', False),
                'volume_24h': current_data.get('volume_24h', 0),
                'change_24h_percent': current_data.get('change_24h_percent', 0)
            }
            
            price = analysis['bitcoin_summary']['current_price']
            direction = analysis['bitcoin_summary']['price_direction']
            return_pct = analysis['bitcoin_summary']['today_return_percent']
            
            print(f"  ✓ Current Bitcoin price: ${price:,.2f}")
            print(f"  ✓ Today's performance: {return_pct:+.2f}% ({direction})")
    
    # Cross-analysis (if both successful)
    if polymarket_data.get('success') and bitcoin_data.get('success'):
        odds_data = polymarket_data.get('odds_data', [])
        bitcoin_summary = analysis.get('bitcoin_summary', {})
        polymarket_summary = analysis.get('polymarket_summary', {})
        
        if odds_data and bitcoin_summary:
            is_bitcoin_up_today = bitcoin_summary.get('is_up_today', False)
            avg_market_prob = polymarket_summary.get('average_yes_probability', 0.5)
            
            analysis['cross_analysis'] = {
                'bitcoin_actually_up': is_bitcoin_up_today,
                'market_avg_probability': avg_market_prob,
                'market_consensus': 'UP' if avg_market_prob > 0.5 else 'DOWN',
                'market_vs_reality': {
                    'markets_predict_up': avg_market_prob > 0.5,
                    'bitcoin_actually_up': is_bitcoin_up_today,
                    'consensus_correct': (avg_market_prob > 0.5) == is_bitcoin_up_today
                },
                'potential_insights': []
            }
            
            # Generate insights
            insights = analysis['cross_analysis']['potential_insights']
            
            if avg_market_prob > 0.7 and not is_bitcoin_up_today:
                insights.append("Markets were overconfident about Bitcoin going up")
            elif avg_market_prob < 0.3 and is_bitcoin_up_today:
                insights.append("Markets were overconfident about Bitcoin going down")
            elif 0.4 <= avg_market_prob <= 0.6:
                insights.append("Markets showed uncertainty - good calibration opportunity")
            
            if polymarket_summary.get('markets_high_confidence', 0) > polymarket_summary.get('total_markets', 1) * 0.5:
                insights.append("Many markets showing high confidence - potential mispricing")
            
            consensus = analysis['cross_analysis']['market_consensus']
            reality = 'UP' if is_bitcoin_up_today else 'DOWN'
            correct = analysis['cross_analysis']['market_vs_reality']['consensus_correct']
            
            print(f"  ✓ Market consensus: {consensus}, Reality: {reality} ({'✓' if correct else '✗'})")
            print(f"  ✓ Average market probability: {avg_market_prob:.1%}")
    
    return analysis


def generate_summary_report(polymarket_data: dict, bitcoin_data: dict, analysis: dict) -> str:
    """Generate a human-readable summary report"""
    report = []
    report.append("=" * 60)
    report.append("POLYMARKET BITCOIN ANALYSIS - DATA FETCH SUMMARY")
    report.append("=" * 60)
    report.append(f"Fetch completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Polymarket section
    if polymarket_data.get('success'):
        pm_summary = analysis.get('polymarket_summary', {})
        report.append("📊 POLYMARKET DATA:")
        report.append(f"  • Markets found: {pm_summary.get('total_markets', 0)}")
        report.append(f"  • Average 'Bitcoin up' probability: {pm_summary.get('average_yes_probability', 0):.1%}")
        report.append(f"  • Markets predicting UP: {pm_summary.get('markets_above_50pct', 0)}")
        report.append(f"  • High confidence markets: {pm_summary.get('markets_high_confidence', 0)}")
        report.append(f"  • Total volume: {pm_summary.get('total_volume', 0):.0f}")
    else:
        report.append("❌ POLYMARKET DATA: Failed to fetch")
    
    report.append("")
    
    # Bitcoin section
    if bitcoin_data.get('success'):
        btc_summary = analysis.get('bitcoin_summary', {})
        report.append("₿ BITCOIN DATA:")
        report.append(f"  • Current price: ${btc_summary.get('current_price', 0):,.2f}")
        report.append(f"  • Today's performance: {btc_summary.get('today_return_percent', 0):+.2f}%")
        report.append(f"  • Direction: {btc_summary.get('price_direction', 'UNKNOWN')}")
        report.append(f"  • 24h volume: ${btc_summary.get('volume_24h', 0):,.0f}")
    else:
        report.append("❌ BITCOIN DATA: Failed to fetch")
    
    report.append("")
    
    # Cross-analysis section
    if 'cross_analysis' in analysis:
        cross = analysis['cross_analysis']
        report.append("🔍 ANALYSIS:")
        report.append(f"  • Market consensus: {cross.get('market_consensus', 'UNKNOWN')}")
        report.append(f"  • Bitcoin actually: {'UP' if cross.get('bitcoin_actually_up', False) else 'DOWN'}")
        report.append(f"  • Consensus correct: {'Yes' if cross['market_vs_reality']['consensus_correct'] else 'No'}")
        
        insights = cross.get('potential_insights', [])
        if insights:
            report.append("  • Key insights:")
            for insight in insights:
                report.append(f"    - {insight}")
    
    report.append("")
    report.append("📁 DATA FILES:")
    report.append("  • Raw data saved to data/ directory with timestamps")
    report.append("  • Use analyze_data.py to perform deeper analysis")
    
    report.append("")
    report.append("🚀 NEXT STEPS:")
    report.append("  1. Run this script multiple times throughout the day")
    report.append("  2. Collect data over several days")
    report.append("  3. Use analyze_data.py to find patterns and opportunities")
    report.append("  4. Look for systematic biases in market predictions")
    
    return "\n".join(report)


def main():
    """Main function to fetch and analyze data"""
    print("🚀 POLYMARKET BITCOIN DATA FETCH")
    print("=" * 50)
    
    # Create data directory
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Fetch Polymarket data
    polymarket_data = fetch_polymarket_data()
    
    # Fetch Bitcoin data
    bitcoin_data = fetch_bitcoin_data()
    
    # Analyze the data
    analysis = analyze_fetched_data(polymarket_data, bitcoin_data)
    
    # Save raw data
    print("\n=== SAVING DATA ===")
    
    if polymarket_data.get('success'):
        pm_file = save_data_to_json(polymarket_data, "polymarket_data.json", data_dir)
        print(f"  ✓ Polymarket data saved: {pm_file}")
    
    if bitcoin_data.get('success'):
        btc_file = save_data_to_json(bitcoin_data, "bitcoin_data.json", data_dir)
        print(f"  ✓ Bitcoin data saved: {btc_file}")
    
    # Save analysis
    analysis_file = save_data_to_json(analysis, "analysis.json", data_dir)
    print(f"  ✓ Analysis saved: {analysis_file}")
    
    # Generate and display summary
    print("\n" + "=" * 60)
    summary = generate_summary_report(polymarket_data, bitcoin_data, analysis)
    print(summary)
    
    # Save summary to file
    summary_file = data_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_summary.txt"
    with open(summary_file, 'w') as f:
        f.write(summary)
    print(f"\n📄 Summary report saved: {summary_file}")
    
    # Return status for other scripts
    success = polymarket_data.get('success', False) and bitcoin_data.get('success', False)
    return {
        'success': success,
        'polymarket_data': polymarket_data,
        'bitcoin_data': bitcoin_data,
        'analysis': analysis
    }


if __name__ == "__main__":
    try:
        result = main()
        
        if result['success']:
            print("\n✅ Data fetch completed successfully!")
            sys.exit(0)
        else:
            print("\n⚠️  Data fetch completed with some failures")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Data fetch interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Data fetch failed with error: {e}")
        sys.exit(1)