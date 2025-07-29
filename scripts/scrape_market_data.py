"""
Optimized Polymarket Daily Bitcoin Market Data Scraper
Cleaner output with proper 16:00-to-16:00 filtering
"""

import os
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager


class PolymarketDataScraper:
    """Optimized scraper for Polymarket Bitcoin daily market data"""
    
    def __init__(self, headless: bool = True, download_dir: Optional[str] = None, verbose: bool = False):
        """
        Initialize the scraper
        
        Args:
            headless: Run browser in headless mode
            download_dir: Directory for downloads (defaults to ./data)
            verbose: Show detailed output (default: False for cleaner logs)
        """
        self.download_dir = Path(download_dir or "data")
        self.download_dir.mkdir(exist_ok=True)
        self.verbose = verbose
        
        # Setup Chrome options
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument('--headless')
        
        # Configure download settings
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-logging')
        self.chrome_options.add_argument('--log-level=3')  # Suppress browser logs
        
        self.chrome_options.add_experimental_option('prefs', {
            'download.default_directory': str(self.download_dir.absolute()),
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True
        })
        
        self.driver = None
        self.wait = None
    
    def log(self, message: str, force: bool = False):
        """Print message only if verbose mode is enabled or force is True"""
        if self.verbose or force:
            print(message)
    
    def start_browser(self):
        """Start the Chrome browser"""
        self.log("🚀 Starting Chrome browser...")
        
        # Suppress WebDriver Manager logs
        os.environ['WDM_LOG_LEVEL'] = '0'
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=self.chrome_options)
        
        self.wait = WebDriverWait(self.driver, 20)
        self.log("✅ Browser started successfully")
    
    def stop_browser(self):
        """Stop the browser"""
        if self.driver:
            self.driver.quit()
            self.log("🛑 Browser stopped")
    
    def get_bitcoin_market_url(self, date: datetime) -> str:
        """Generate the Polymarket URL for Bitcoin market on given date"""
        month = date.strftime("%B").lower()
        day = date.day
        slug = f"bitcoin-up-or-down-on-{month}-{day}"
        url = f"https://polymarket.com/event/{slug}"
        return url
    
    def dismiss_popups(self) -> bool:
        """Dismiss any popups or modals that might be blocking the interface"""
        popup_selectors = [
            "button[aria-label='Close']",
            "button[title='Close']",
            "[data-testid='close-button']",
            ".close-button",
            "button:has(svg):has(path[d*='M6 6l12 12'])",
            "[role='dialog'] button",
            ".modal button",
        ]
        
        for selector in popup_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element.is_displayed() and element.is_enabled():
                        element.click()
                        time.sleep(1)
                        return True
            except:
                continue
        
        # Try pressing Escape key
        try:
            self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            time.sleep(1)
        except:
            pass
        
        return True
    
    def navigate_to_market(self, url: str) -> bool:
        """Navigate to the market page"""
        try:
            self.log(f"🌐 Loading market: {url}")
            self.driver.get(url)
            
            # Wait for page load
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(3)
            
            # Dismiss popups
            self.dismiss_popups()
            time.sleep(2)
            
            # Check if market exists
            market_indicators = [
                "//h1[contains(text(), 'Bitcoin')]",
                "//div[contains(@class, 'chart')]", 
                "//canvas",
                "//div[contains(text(), 'Bitcoin Up or Down')]",
                "//div[contains(text(), 'UP')]",
                "//div[contains(text(), 'chance')]"
            ]
            
            for xpath in market_indicators:
                if self.driver.find_elements(By.XPATH, xpath):
                    self.log("✅ Market page loaded successfully")
                    return True
            
            self.log("❌ Market not found")
            return False
                    
        except Exception as e:
            self.log(f"❌ Navigation failed: {e}")
            return False
    
    def find_export_button(self) -> Optional[object]:
        """Find the export button on the page"""
        self.log("🔍 Looking for export button...")
        
        export_selectors = [
            "button svg[title='file content']",
            "button:has(svg[title='file content'])",
            "button[aria-label*='export']",
            "button[title*='export']",
            "button svg[viewBox='0 0 18 18']",
            "button:has(svg):has(path[d*='M2.75,14.25V3.75'])",
        ]
        
        for selector in export_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element.tag_name == 'svg':
                        button = element.find_element(By.XPATH, "./..")
                    else:
                        button = element
                    
                    if self.is_export_button(button):
                        self.log("✅ Found export button")
                        return button
            except:
                continue
        
        # Fallback search
        all_buttons = self.driver.find_elements(By.TAG_NAME, "button")
        for button in all_buttons:
            if self.is_export_button(button):
                self.log("✅ Found export button (fallback)")
                return button
        
        self.log("❌ Export button not found")
        return None
    
    def is_export_button(self, button) -> bool:
        """Check if a button looks like an export button"""
        try:
            svgs = button.find_elements(By.TAG_NAME, "svg")
            for svg in svgs:
                title = svg.get_attribute('title')
                if title and 'file' in title.lower():
                    return True
                
                svg_content = svg.get_attribute('innerHTML') or ''
                if any(keyword in svg_content.lower() for keyword in ['file', 'download', 'export']):
                    return True
            
            button_text = button.text.lower()
            if any(keyword in button_text for keyword in ['export', 'download', 'file']):
                return True
        except:
            pass
        
        return False
    
    def click_export_button(self, button) -> bool:
        """Click the export button to open the download modal"""
        try:
            self.log("📤 Clicking export button...")
            self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
            time.sleep(1)
            
            try:
                button.click()
            except:
                self.driver.execute_script("arguments[0].click();", button)
            
            self.log("✅ Export button clicked")
            return True
        except Exception as e:
            self.log(f"❌ Failed to click export button: {e}")
            return False
    
    def configure_download_modal(self, date: datetime) -> bool:
        """Configure the download modal with minutely frequency and correct date range"""
        self.log("⚙️ Configuring download modal...")
        
        try:
            time.sleep(3)  # Wait for modal to load
            
            # Calculate date range: 16:00 day before to 16:00 target day
            from_date = date - timedelta(days=1)  # Day before
            from_date_str = from_date.strftime("%m/%d/%Y")
            
            self.log(f"📅 Setting date range: {from_date_str} 16:00 to {date.strftime('%m/%d/%Y')} 16:00")
            
            # 1. Change frequency to Minutely
            self.log("📊 Setting frequency to Minutely...")
            frequency_selectors = [
                "button[role='combobox']",
                "button[class*='c-gBrBnR'][aria-expanded='false']",
                "button[data-state='closed']"
            ]
            
            frequency_success = False
            for selector in frequency_selectors:
                try:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for button in buttons:
                        if button.is_displayed() and 'daily' in button.text.lower():
                            button.click()
                            time.sleep(2)
                            
                            # Find minutely option
                            minutely_options = self.driver.find_elements(
                                By.XPATH, 
                                "//*[contains(text(), 'Minutely') or contains(text(), 'minutely') or contains(text(), 'Minute')]"
                            )
                            
                            for option in minutely_options:
                                if option.is_displayed():
                                    option.click()
                                    frequency_success = True
                                    time.sleep(1)
                                    break
                            
                            if frequency_success:
                                break
                    
                    if frequency_success:
                        break
                except:
                    continue
            
            if frequency_success:
                self.log("✅ Frequency set to Minutely")
            else:
                self.log("⚠️ Could not set frequency - using default")
            
            # 2. Set FROM date (first date picker)
            self.log("📅 Setting FROM date...")
            date_picker_approaches = [
                "button[aria-haspopup='dialog'][data-state='closed']",
                "button[aria-haspopup='dialog']",
                "button:has(span.c-PJLV)",
            ]
            
            date_picker_buttons = []
            for selector in date_picker_approaches:
                try:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for button in buttons:
                        if button.is_displayed():
                            button_text = button.text.strip()
                            if '/' in button_text and '2025' in button_text:
                                date_picker_buttons.append(button)
                except:
                    continue
                
                if date_picker_buttons:
                    break
            
            # Process only the FIRST date picker (FROM date)
            if date_picker_buttons:
                from_button = date_picker_buttons[0]
                
                try:
                    current_date = from_button.text.strip()
                    if current_date != from_date_str:
                        self.log(f"📅 Updating FROM date: {current_date} → {from_date_str}")
                        
                        # Click to open calendar
                        self.driver.execute_script("arguments[0].click();", from_button)
                        time.sleep(2)
                        
                        # Look for dialog
                        try:
                            dialog = self.wait.until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "[role='dialog']"))
                            )
                            
                            # Find target day
                            target_day = int(from_date_str.split('/')[1])
                            day_buttons = dialog.find_elements(
                                By.XPATH, 
                                f"//button[text()='{target_day}']"
                            )
                            
                            if day_buttons:
                                day_buttons[0].click()
                                time.sleep(1)
                                self.log(f"✅ FROM date updated to {from_date_str}")
                            else:
                                self.log("⚠️ Could not find target day in calendar")
                            
                            # Close dialog
                            self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                            time.sleep(1)
                            
                        except:
                            self.log("⚠️ Could not interact with date picker")
                    else:
                        self.log(f"✅ FROM date already correct: {from_date_str}")
                
                except Exception as e:
                    self.log(f"⚠️ FROM date update failed: {e}")
            else:
                self.log("⚠️ No date picker buttons found")
            
            time.sleep(2)
            self.log("✅ Modal configuration completed")
            return True
            
        except Exception as e:
            self.log(f"❌ Modal configuration failed: {e}")
            return False
    
    def click_download_csv_button(self) -> bool:
        """Click the Download (.csv) button"""
        self.log("📥 Looking for download button...")
        
        time.sleep(2)
        
        download_selectors = [
            "//button[text()='Download (.csv)']",
            "//button[contains(text(), 'Download (.csv)')]",
            "//button[contains(text(), 'Download') and contains(text(), 'csv')]",
            "button[class*='c-gBrBnR'][class*='variant-primary']",
        ]
        
        for selector in download_selectors:
            try:
                if selector.startswith('//'):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    if element.is_displayed() and element.is_enabled():
                        element_text = element.text.strip()
                        if 'Download' in element_text and 'csv' in element_text:
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                            time.sleep(1)
                            
                            try:
                                element.click()
                            except:
                                self.driver.execute_script("arguments[0].click();", element)
                            
                            self.log("✅ Download button clicked")
                            return True
            except:
                continue
        
        self.log("❌ Download button not found")
        return False
    
    def wait_for_download(self, timeout: int = 30) -> Optional[Path]:
        """Wait for the download to complete"""
        self.log(f"⏳ Waiting for download...")
        
        start_time = time.time()
        initial_files = set(self.download_dir.glob("*"))
        
        while time.time() - start_time < timeout:
            current_files = set(self.download_dir.glob("*"))
            new_files = current_files - initial_files
            
            if new_files:
                completed_files = [
                    f for f in new_files 
                    if not f.name.endswith(('.crdownload', '.tmp', '.part'))
                ]
                
                if completed_files:
                    downloaded_file = completed_files[0]
                    self.log(f"✅ Download completed: {downloaded_file.name}")
                    return downloaded_file
            
            time.sleep(1)
        
        self.log("❌ Download timeout")
        return None
    
    def process_downloaded_csv(self, file_path: Path, target_date: datetime) -> Optional[Path]:
        """
        Process CSV to filter data between 16:00 timestamps and show summary
        """
        print(f"\n📊 Processing CSV data: {file_path.name}")
        
        try:
            df = pd.read_csv(file_path)
            
            print(f"  📋 Original data: {len(df):,} rows")
            print(f"  📋 Columns: {list(df.columns)}")
            
            if len(df) == 0:
                print("  ⚠️ CSV file is empty")
                return file_path
            
            # Find timestamp column
            timestamp_columns = ['Date (UTC)', 'Timestamp (UTC)', 'timestamp', 'date', 'time']
            timestamp_col = None
            
            for col in timestamp_columns:
                if col in df.columns:
                    timestamp_col = col
                    break
            
            if not timestamp_col:
                print(f"  ❌ No timestamp column found in: {list(df.columns)}")
                return file_path
            
            print(f"  🕐 Using timestamp column: '{timestamp_col}'")
            
            # Convert timestamps
            try:
                if 'Timestamp' in timestamp_col and df[timestamp_col].dtype in ['int64', 'float64']:
                    if df[timestamp_col].max() > 1e10:  # Milliseconds
                        df['datetime'] = pd.to_datetime(df[timestamp_col], unit='ms', utc=True)
                    else:  # Seconds
                        df['datetime'] = pd.to_datetime(df[timestamp_col], unit='s', utc=True)
                else:
                    df['datetime'] = pd.to_datetime(df[timestamp_col], utc=True)
            except Exception as e:
                print(f"  ❌ Timestamp conversion failed: {e}")
                return file_path
            
            # Show original date range
            min_date = df['datetime'].min()
            max_date = df['datetime'].max()
            print(f"  📅 Original range: {min_date.strftime('%m/%d %H:%M')} to {max_date.strftime('%m/%d %H:%M')} UTC")
            
            # Define target range: 16:00 day before to 16:00 target day
            from_datetime = (target_date - timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
            to_datetime = target_date.replace(hour=16, minute=0, second=0, microsecond=0)
            
            # Make timezone-aware for comparison
            import pytz
            utc = pytz.UTC
            if from_datetime.tzinfo is None:
                from_datetime = utc.localize(from_datetime)
                to_datetime = utc.localize(to_datetime)
            
            print(f"  🎯 Target range: {from_datetime.strftime('%m/%d %H:%M')} to {to_datetime.strftime('%m/%d %H:%M')} UTC")
            
            # Filter data
            mask = (df['datetime'] >= from_datetime) & (df['datetime'] < to_datetime)
            filtered_df = df[mask].copy()
            
            print(f"  ✂️ Filtered data: {len(filtered_df):,} rows ({len(filtered_df)/len(df)*100:.1f}% of original)")
            
            if len(filtered_df) == 0:
                print("  ⚠️ No data found in target time range")
                print("  🔍 Available time distribution:")
                df['hour'] = df['datetime'].dt.hour
                hour_counts = df['hour'].value_counts().sort_index()
                for hour, count in hour_counts.head(10).items():
                    print(f"    Hour {hour:02d}: {count:,} records")
                return file_path
            
            # Show filtered data info
            filtered_min = filtered_df['datetime'].min()
            filtered_max = filtered_df['datetime'].max()
            print(f"  📅 Filtered range: {filtered_min.strftime('%m/%d %H:%M')} to {filtered_max.strftime('%m/%d %H:%M')} UTC")
            
            # Show price info if available
            price_cols = [col for col in filtered_df.columns if 'price' in col.lower()]
            if price_cols:
                price_col = price_cols[0]
                start_price = filtered_df.iloc[0][price_col]
                end_price = filtered_df.iloc[-1][price_col]
                price_change = end_price - start_price
                price_change_pct = (price_change / start_price) * 100 if start_price > 0 else 0
                
                print(f"  💰 Price movement: {start_price:.3f} → {end_price:.3f} ({price_change_pct:+.2f}%)")
                print(f"  📊 Data points: {len(filtered_df):,} minute intervals")
            
            # Save filtered data
            original_name = file_path.stem
            processed_name = f"{original_name}_16h_filtered.csv"
            processed_path = file_path.parent / processed_name
            
            # Remove helper datetime column before saving
            output_df = filtered_df.drop('datetime', axis=1)
            output_df.to_csv(processed_path, index=False)
            
            print(f"  💾 Filtered data saved: {processed_name}")
            
            return processed_path
            
        except Exception as e:
            print(f"  ❌ CSV processing failed: {e}")
            return file_path
    
    def scrape_market_data(self, date: datetime, timeout: int = 60) -> Dict:
        """Main scraping function with clean output"""
        print(f"\n{'='*60}")
        print(f"🎯 SCRAPING BITCOIN MARKET DATA FOR {date.strftime('%Y-%m-%d')}")
        print(f"{'='*60}")
        
        if not self.driver:
            self.start_browser()
        
        url = self.get_bitcoin_market_url(date)
        print(f"🌐 Market URL: {url}")
        
        try:
            # Navigate to market
            if not self.navigate_to_market(url):
                return {'success': False, 'error': 'Navigation failed', 'date': date.isoformat()}
            
            # Find and click export button
            export_button = self.find_export_button()
            if not export_button:
                return {'success': False, 'error': 'Export button not found', 'date': date.isoformat()}
            
            if not self.click_export_button(export_button):
                return {'success': False, 'error': 'Could not click export button', 'date': date.isoformat()}
            
            # Configure modal
            if not self.configure_download_modal(date):
                return {'success': False, 'error': 'Modal configuration failed', 'date': date.isoformat()}
            
            # Download CSV
            if not self.click_download_csv_button():
                return {'success': False, 'error': 'Download button not found', 'date': date.isoformat()}
            
            # Wait for download
            downloaded_file = self.wait_for_download(timeout=30)
            if not downloaded_file:
                return {'success': False, 'error': 'Download timeout', 'date': date.isoformat()}
            
            # Rename file
            date_str = date.strftime('%Y%m%d')
            new_filename = f"bitcoin_market_{date_str}_{downloaded_file.name}"
            new_path = self.download_dir / new_filename
            
            try:
                downloaded_file.rename(new_path)
                print(f"📁 File saved as: {new_filename}")
            except:
                new_path = downloaded_file
            
            # Process CSV with 16:00-to-16:00 filtering
            processed_path = self.process_downloaded_csv(new_path, date)
            
            return {
                'success': True,
                'date': date.isoformat(),
                'url': url,
                'original_file': str(new_path),
                'processed_file': str(processed_path),
                'file_size': processed_path.stat().st_size,
                'scraped_at': datetime.now().isoformat()
            }
        
        except Exception as e:
            return {'success': False, 'error': f'Unexpected error: {str(e)}', 'date': date.isoformat()}


def scrape_single_date(date_str: str, headless: bool = True, verbose: bool = False) -> Dict:
    """Scrape data for a single date with clean output"""
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return {'success': False, 'error': f'Invalid date format: {date_str}. Use YYYY-MM-DD.'}
    
    scraper = PolymarketDataScraper(headless=headless, verbose=verbose)
    
    try:
        result = scraper.scrape_market_data(date)
        return result
    finally:
        scraper.stop_browser()


def main():
    """Clean main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape Polymarket Bitcoin market data (16:00-to-16:00)')
    parser.add_argument('date', help='Date in YYYY-MM-DD format')
    parser.add_argument('--visible', action='store_true', help='Run browser in visible mode')
    parser.add_argument('--verbose', action='store_true', help='Show detailed output')
    
    args = parser.parse_args()
    
    print(f"🚀 Polymarket Bitcoin Data Scraper")
    print(f"📅 Target date: {args.date}")
    print(f"⏰ Time range: 16:00 day before to 16:00 target day")
    
    result = scrape_single_date(args.date, headless=not args.visible, verbose=args.verbose)
    
    # Save results
    results_file = Path("data") / f"scrape_result_{args.date.replace('-', '')}.json"
    with open(results_file, 'w') as f:
        json.dump(result, f, indent=2, default=str)
    
    print(f"\n📄 Results saved: {results_file}")
    
    # Summary
    if result['success']:
        print(f"✅ SUCCESS: Data scraped and filtered successfully")
        print(f"📊 Processed file: {Path(result['processed_file']).name}")
    else:
        print(f"❌ FAILED: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()