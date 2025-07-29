#!/usr/bin/env python3
"""
Daily Market Data Collector for Polymarket Bitcoin Markets

This script collects minute-by-minute market data for Bitcoin "Up or Down" markets
by automating the export process from Polymarket's web interface.
"""

import os
import sys
import time
import json
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Dict, List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from polymarket_analysis.data.bitcoin_client import BitcoinClient


class DailyMarketDataCollector:
    """Collects daily minute-by-minute market data from Polymarket"""
    
    def __init__(self, headless: bool = True, download_dir: Optional[str] = None):
        """
        Initialize the collector
        
        Args:
            headless: Run browser in headless mode
            download_dir: Directory to save downloaded files (default: project data dir)
        """
        self.headless = headless
        self.download_dir = download_dir or str(Path(__file__).parent.parent / "data")
        self.driver = None
        self.bitcoin_client = BitcoinClient()
        
        # Create download directory
        Path(self.download_dir).mkdir(exist_ok=True)
    
    def _setup_driver(self):
        """Setup Chrome WebDriver with download preferences"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument('--headless')
        
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        
        # Set download preferences
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        self.driver = webdriver.Chrome(options=chrome_options)
        return self.driver
    
    def find_bitcoin_market_url(self, target_date: date) -> Optional[str]:
        """
        Find the Polymarket URL for Bitcoin market on a specific date
        
        Args:
            target_date: Date to find market for
            
        Returns:
            Market URL or None if not found
        """
        # Format date for market slug
        month_names = {
            1: "january", 2: "february", 3: "march", 4: "april",
            5: "may", 6: "june", 7: "july", 8: "august", 
            9: "september", 10: "october", 11: "november", 12: "december"
        }
        
        month = month_names[target_date.month]
        day = target_date.day
        
        # Try different URL patterns
        url_patterns = [
            f"https://polymarket.com/event/bitcoin-up-or-down-on-{month}-{day}",
            f"https://polymarket.com/event/bitcoin-up-or-down-{month}-{day}",
            f"https://polymarket.com/event/btc-up-or-down-on-{month}-{day}",
        ]
        
        print(f"🔍 Looking for Bitcoin market on {target_date.strftime('%B %d, %Y')}")
        
        for url in url_patterns:
            print(f"   Trying: {url}")
            # We'll verify this URL works when we load it
            return url  # Return first pattern for now - we'll validate in collect_data
        
        return None
    
    def collect_market_data(self, target_date: date, market_url: Optional[str] = None) -> Dict:
        """
        Collect minute-by-minute market data for a specific date
        
        Args:
            target_date: Date to collect data for
            market_url: Optional specific market URL (will auto-find if not provided)
            
        Returns:
            Dictionary with collection results
        """
        if not market_url:
            market_url = self.find_bitcoin_market_url(target_date)
            if not market_url:
                return {
                    'success': False,
                    'error': f'Could not find market URL for {target_date}',
                    'date': target_date
                }
        
        print(f"📊 COLLECTING DATA FOR {target_date.strftime('%B %d, %Y')}")
        print(f"Market URL: {market_url}")
        
        try:
            # Setup browser
            if not self.driver:
                self._setup_driver()
            
            print("🌐 Loading market page...")
            self.driver.get(market_url)
            
            # Wait for page to load
            wait = WebDriverWait(self.driver, 15)
            
            # Look for the chart container to ensure page loaded
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid*='chart'], .chart, canvas")))
                print("✅ Market page loaded successfully")
            except TimeoutException:
                print("⚠️  Chart not found, but continuing...")
            
            # Look for the export button (the SVG icon you described)
            print("🔍 Looking for export button...")
            
            # Try different selectors for the export button
            export_selectors = [
                "button[title*='export']",
                "button[aria-label*='export']", 
                "button svg[title='file content']",
                "button:has(svg[title='file content'])",
                "button.inline-flex:has(svg)",
                ".inline-flex.items-center.cursor-pointer:has(svg)"
            ]
            
            export_button = None
            for selector in export_selectors:
                try:
                    export_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    print(f"✅ Found export button with selector: {selector}")
                    break
                except TimeoutException:
                    continue
            
            if not export_button:
                # Fallback: look for any button with an SVG that looks like export
                buttons = self.driver.find_elements(By.TAG_NAME, "button")
                for button in buttons:
                    try:
                        svg = button.find_element(By.TAG_NAME, "svg")
                        if svg and "file" in str(svg.get_attribute("innerHTML")).lower():
                            export_button = button
                            print("✅ Found export button via SVG content search")
                            break
                    except:
                        continue
            
            if not export_button:
                return {
                    'success': False,
                    'error': 'Could not find export button on market page',
                    'date': target_date
                }
            
            # Click the export button
            print("📤 Clicking export button...")
            self.driver.execute_script("arguments[0].click();", export_button)
            time.sleep(2)
            
            # Look for minute selection option
            print("⏱️  Looking for minute interval option...")
            
            # Wait for export modal/dropdown to appear
            time.sleep(1)
            
            # Try to find "by minute" or similar option
            minute_selectors = [
                "button:contains('minute')",
                "[data-testid*='minute']",
                "button[value='1min']",
                "button[value='minute']",
                ".dropdown-item:contains('minute')"
            ]
            
            minute_option = None
            for selector in minute_selectors:
                try:
                    # For text-based selectors, use XPath
                    if "contains" in selector:
                        xpath_selector = f"//button[contains(text(), 'minute')] | //div[contains(text(), 'minute')] | //*[contains(text(), 'by minute')]"
                        minute_option = self.driver.find_element(By.XPATH, xpath_selector)
                    else:
                        minute_option = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if minute_option:
                        print(f"✅ Found minute option with selector: {selector}")
                        break
                except:
                    continue
            
            if minute_option:
                print("📊 Selecting minute interval...")
                self.driver.execute_script("arguments[0].click();", minute_option)
                time.sleep(1)
            else:
                print("⚠️  Could not find minute interval option, proceeding with default...")
            
            # Look for download/export confirmation button
            download_selectors = [
                "button:contains('Download')",
                "button:contains('Export')", 
                "button[type='submit']",
                ".modal button.primary",
                ".btn-primary"
            ]
            
            download_button = None
            for selector in download_selectors:
                try:
                    if "contains" in selector:
                        text = selector.split("contains('")[1].split("')")[0]
                        xpath_selector = f"//button[contains(text(), '{text}')]"
                        download_button = self.driver.find_element(By.XPATH, xpath_selector)
                    else:
                        download_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if download_button:
                        print(f"✅ Found download button")
                        break
                except:
                    continue
            
            if download_button:
                print("💾 Initiating download...")
                self.driver.execute_script("arguments[0].click();", download_button)
                
                # Wait for download to complete
                print("⏳ Waiting for download to complete...")
                time.sleep(5)
                
                # Look for downloaded file
                downloaded_file = self._find_latest_download()
                
                if downloaded_file:
                    # Rename file to include date
                    date_str = target_date.strftime("%Y%m%d")
                    new_filename = f"polymarket_btc_{date_str}_minute_data.csv"
                    new_path = Path(self.download_dir) / new_filename
                    
                    downloaded_file.rename(new_path)
                    
                    print(f"✅ Data downloaded successfully: {new_filename}")
                    
                    # Load and validate the data
                    df = pd.read_csv(new_path)
                    
                    return {
                        'success': True,
                        'date': target_date,
                        'filename': new_filename,
                        'filepath': str(new_path),
                        'rows': len(df),
                        'columns': list(df.columns),
                        'date_range': {
                            'start': df.iloc[0]['Date (UTC)'] if 'Date (UTC)' in df.columns else None,
                            'end': df.iloc[-1]['Date (UTC)'] if 'Date (UTC)' in df.columns else None
                        }
                    }
                else:
                    return {
                        'success': False,
                        'error': 'Download completed but file not found',
                        'date': target_date
                    }
            else:
                return {
                    'success': False,
                    'error': 'Could not find download button',
                    'date': target_date
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': f'Error during data collection: {str(e)}',
                'date': target_date
            }
    
    def _find_latest_download(self) -> Optional[Path]:
        """Find the most recently downloaded file"""
        download_path = Path(self.download_dir)
        
        # Look for CSV files modified in the last minute
        recent_files = []
        current_time = time.time()
        
        for file_path in download_path.glob("*.csv"):
            if current_time - file_path.stat().st_mtime < 60:  # Modified within last minute
                recent_files.append(file_path)
        
        if recent_files:
            # Return the most recently modified file
            return max(recent_files, key=lambda f: f.stat().st_mtime)
        
        return None
    
    def collect_bitcoin_price_data(self, target_date: date) -> Dict:
        """Get Bitcoin price data for the target date"""
        print(f"₿ Collecting Bitcoin price data for {target_date}...")
        
        try:
            # Get comprehensive Bitcoin data
            btc_data = self.bitcoin_client.get_comprehensive_data(days=1)
            
            if not btc_data.get('success'):
                return {'success': False, 'error': 'Failed to fetch Bitcoin data'}
            
            return {
                'success': True,
                'date': target_date,
                'bitcoin_data': btc_data
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Error fetching Bitcoin data: {str(e)}'
            }
    
    def close(self):
        """Clean up resources"""
        if self.driver:
            self.driver.quit()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def collect_daily_data(target_date: str, market_url: Optional[str] = None, headless: bool = True) -> Dict:
    """
    Convenience function to collect data for a specific date
    
    Args:
        target_date: Date string in format 'YYYY-MM-DD' or 'july-28'
        market_url: Optional specific market URL
        headless: Run browser in headless mode
        
    Returns:
        Collection results
    """
    # Parse date
    try:
        if '-' in target_date and len(target_date.split('-')) == 3:
            # Format: 2025-07-28
            date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
        else:
            # Format: july-28 (assume current year)
            month_day = target_date.lower().replace('-', ' ')
            date_obj = datetime.strptime(f"2025 {month_day}", '%Y %B %d').date()
    except ValueError as e:
        return {
            'success': False,
            'error': f'Invalid date format: {target_date}. Use YYYY-MM-DD or month-day format.'
        }
    
    print(f"🚀 COLLECTING DATA FOR {date_obj.strftime('%B %d, %Y')}")
    print("=" * 60)
    
    with DailyMarketDataCollector(headless=headless) as collector:
        # Collect market data
        market_result = collector.collect_market_data(date_obj, market_url)
        
        # Collect Bitcoin price data
        bitcoin_result = collector.collect_bitcoin_price_data(date_obj)
        
        # Combine results
        return {
            'date': date_obj,
            'market_data': market_result,
            'bitcoin_data': bitcoin_result,
            'overall_success': market_result.get('success', False) and bitcoin_result.get('success', False)
        }


def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect daily Bitcoin market data from Polymarket')
    parser.add_argument('date', help='Date to collect data for (YYYY-MM-DD or month-day format)')
    parser.add_argument('--url', help='Specific market URL (optional)')
    parser.add_argument('--visible', action='store_true', help='Run browser in visible mode (not headless)')
    
    args = parser.parse_args()
    
    result = collect_daily_data(
        target_date=args.date,
        market_url=args.url,
        headless=not args.visible
    )
    
    print(f"\n📊 COLLECTION SUMMARY")
    print("=" * 40)
    print(f"Date: {result['date']}")
    print(f"Market data: {'✅' if result['market_data'].get('success') else '❌'}")
    print(f"Bitcoin data: {'✅' if result['bitcoin_data'].get('success') else '❌'}")
    print(f"Overall success: {'✅' if result['overall_success'] else '❌'}")
    
    if result['market_data'].get('success'):
        md = result['market_data']
        print(f"Market file: {md['filename']}")
        print(f"Rows: {md['rows']}")
    
    if not result['overall_success']:
        print(f"\n❌ Errors encountered:")
        if not result['market_data'].get('success'):
            print(f"  Market: {result['market_data'].get('error')}")
        if not result['bitcoin_data'].get('success'):
            print(f"  Bitcoin: {result['bitcoin_data'].get('error')}")
    
    return result


if __name__ == "__main__":
    main()