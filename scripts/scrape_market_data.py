"""
Polymarket Daily Bitcoin Market Data Scraper
Scrapes minute-by-minute market data for Bitcoin daily markets
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
    """Scraper for Polymarket Bitcoin daily market data"""
    
    def __init__(self, headless: bool = True, download_dir: Optional[str] = None):
        """
        Initialize the scraper
        
        Args:
            headless: Run browser in headless mode
            download_dir: Directory for downloads (defaults to ./data)
        """
        self.download_dir = Path(download_dir or "data")
        self.download_dir.mkdir(exist_ok=True)
        
        # Setup Chrome options
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument('--headless')
        
        # Configure download settings
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_experimental_option('prefs', {
            'download.default_directory': str(self.download_dir.absolute()),
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True
        })
        
        self.driver = None
        self.wait = None
    
    def start_browser(self):
        """Start the Chrome browser"""
        print("🚀 Starting Chrome browser...")
        
        # Use WebDriver Manager to automatically handle ChromeDriver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=self.chrome_options)
        
        self.wait = WebDriverWait(self.driver, 20)
        print("✅ Browser started successfully")
    
    def stop_browser(self):
        """Stop the browser"""
        if self.driver:
            self.driver.quit()
            print("🛑 Browser stopped")
    
    def get_bitcoin_market_url(self, date: datetime) -> str:
        """
        Generate the Polymarket URL for Bitcoin market on given date
        
        Args:
            date: Date for the Bitcoin market
            
        Returns:
            URL string for the market
        """
        # Format: bitcoin-up-or-down-on-july-28
        month = date.strftime("%B").lower()
        day = date.day
        
        slug = f"bitcoin-up-or-down-on-{month}-{day}"
        url = f"https://polymarket.com/event/{slug}"
        
        print(f"📅 Generated URL for {date.strftime('%Y-%m-%d')}: {url}")
        return url
    
    def dismiss_popups(self) -> bool:
        """
        Dismiss any popups or modals that might be blocking the interface
        
        Returns:
            True if popups were handled successfully
        """
        print("🔍 Checking for popups to dismiss...")
        
        # List of popup selectors to try
        popup_selectors = [
            # Close buttons
            "button[aria-label='Close']",
            "button[title='Close']",
            "[data-testid='close-button']",
            ".close-button",
            
            # X buttons
            "button:has(svg):has(path[d*='M6 6l12 12'])",  # X icon path
            "button:has(svg):has(line[x1='18'][y1='6'])",   # X lines
            
            # Skip/Next/Cancel buttons
            "button:contains('Skip')",
            "button:contains('Cancel')",
            "button:contains('No thanks')",
            "button:contains('Not now')",
            
            # Modal overlays
            "[role='dialog'] button",
            ".modal button",
            "[data-testid='modal'] button",
        ]
        
        for selector in popup_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    try:
                        if element.is_displayed() and element.is_enabled():
                            print(f"  🎯 Found popup element, clicking: {selector}")
                            element.click()
                            time.sleep(2)  # Wait for popup to close
                            return True
                    except:
                        continue
            except:
                continue
        
        # Try pressing Escape key to close modals
        try:
            self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            print("  ⌨️ Pressed Escape to close popups")
            time.sleep(2)
        except:
            pass
        
        return True
    
    def navigate_to_market(self, url: str) -> bool:
        """
        Navigate to the market page
        
        Args:
            url: Market URL
            
        Returns:
            True if successful, False otherwise
        """
        try:
            print(f"🌐 Navigating to: {url}")
            self.driver.get(url)
            
            # Wait for the page to load
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Give page extra time to fully load
            time.sleep(3)
            
            # Dismiss any popups
            self.dismiss_popups()
            
            # Additional wait after dismissing popups
            time.sleep(2)
            
            # Check if market exists (look for error or valid market content)
            try:
                # Look for market title or chart - be more flexible
                market_indicators = [
                    "//h1[contains(text(), 'Bitcoin')]",
                    "//div[contains(@class, 'chart')]", 
                    "//canvas",
                    "//div[contains(text(), 'Bitcoin Up or Down')]",
                    "//div[contains(text(), 'UP')]",
                    "//div[contains(text(), 'chance')]"
                ]
                
                market_found = False
                for xpath in market_indicators:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    if elements:
                        print(f"✅ Market indicator found: {xpath}")
                        market_found = True
                        break
                
                if market_found:
                    print("✅ Market page loaded successfully")
                    return True
                else:
                    print("⚠️ Market page loaded but no market content found")
                    # Take screenshot for debugging
                    self.driver.save_screenshot("data/debug_no_market_content.png")
                    return False
                    
            except NoSuchElementException:
                print("❌ Market not found or page not loaded properly")
                return False
                
        except TimeoutException:
            print("❌ Timeout waiting for page to load")
            return False
        except Exception as e:
            print(f"❌ Error navigating to market: {e}")
            return False
    
    def find_export_button(self) -> Optional[object]:
        """
        Find the export button on the page
        
        Returns:
            WebElement for export button or None if not found
        """
        print("🔍 Looking for export button...")
        
        # First, try to dismiss any remaining popups
        self.dismiss_popups()
        
        # Multiple selectors to try for the export button
        export_selectors = [
            # Based on the button HTML you provided
            "button[class*='inline-flex'][class*='cursor-pointer'] svg[title='file content']",
            "button svg[title='file content']",
            "button:has(svg[title='file content'])",
            
            # Alternative approaches
            "button[aria-label*='export']",
            "button[title*='export']",
            "button:has(svg):has([title*='file'])",
            
            # More generic based on the SVG structure
            "button svg[viewBox='0 0 18 18']",
            "button:has(svg):has(line[x1='5.75'])",  # From the SVG content
            "button:has(svg):has(path[d*='M2.75,14.25V3.75'])",  # Document path
            
            # Try finding all small icon buttons
            "button.h-8.w-8",  # Size classes from your HTML
            "button[class*='h-8'][class*='w-8']",
        ]
        
        for selector in export_selectors:
            try:
                print(f"  Trying selector: {selector}")
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    try:
                        # If we found SVG, get the parent button
                        if element.tag_name == 'svg':
                            button = element.find_element(By.XPATH, "./..")
                        else:
                            button = element
                        
                        # Check if this looks like an export button
                        if self.is_export_button(button):
                            print(f"  ✅ Found export button: {button.tag_name}")
                            return button
                    except:
                        continue
                        
            except Exception as e:
                print(f"  ❌ Selector failed: {e}")
                continue
        
        # Fallback: look for any button with download-like icons
        try:
            all_buttons = self.driver.find_elements(By.TAG_NAME, "button")
            print(f"  Found {len(all_buttons)} buttons total, checking for export-like content...")
            
            for i, button in enumerate(all_buttons):
                if self.is_export_button(button):
                    print(f"  ✅ Found export-like button #{i}")
                    return button
                    
        except Exception as e:
            print(f"  ❌ Fallback search failed: {e}")
        
        print("❌ Export button not found")
        
        # Take screenshot for debugging
        self.driver.save_screenshot("data/debug_no_export_button.png")
        
        return None
    
    def is_export_button(self, button) -> bool:
        """
        Check if a button looks like an export button
        
        Args:
            button: WebElement to check
            
        Returns:
            True if this looks like an export button
        """
        try:
            # Check if button contains SVG with file-related content
            svgs = button.find_elements(By.TAG_NAME, "svg")
            for svg in svgs:
                # Check title attribute
                title = svg.get_attribute('title')
                if title and 'file' in title.lower():
                    return True
                
                # Check SVG content
                svg_content = svg.get_attribute('innerHTML') or ''
                if any(keyword in svg_content.lower() for keyword in ['file', 'download', 'export']):
                    return True
                
                # Check for document-like paths in SVG
                paths = svg.find_elements(By.TAG_NAME, "path")
                for path in paths:
                    d_attr = path.get_attribute('d')
                    if d_attr and ('M2.75,14.25V3.75' in d_attr or 'document' in d_attr.lower()):
                        return True
            
            # Check button text
            button_text = button.text.lower()
            if any(keyword in button_text for keyword in ['export', 'download', 'file']):
                return True
                
        except:
            pass
        
        return False
    
    def click_export_button(self, button) -> bool:
        """
        Click the export button to open the download modal
        
        Args:
            button: WebElement for the export button
            
        Returns:
            True if clicked successfully
        """
        try:
            print("📤 Clicking export button...")
            
            # Scroll button into view
            self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
            time.sleep(1)
            
            # Try regular click first
            try:
                button.click()
                print("✅ Export button clicked successfully")
                return True
            except:
                # If regular click fails, try JavaScript click
                print("  Regular click failed, trying JavaScript click...")
                self.driver.execute_script("arguments[0].click();", button)
                print("✅ Export button clicked via JavaScript")
                return True
                
        except Exception as e:
            print(f"❌ Failed to click export button: {e}")
            return False
    
    def configure_download_modal(self, date: datetime) -> bool:
        """
        Configure the download modal with the correct date and frequency
        
        Args:
            date: Target date (we'll download from day before to day after)
            
        Returns:
            True if configuration was successful
        """
        print("⚙️ Configuring download modal...")
        
        try:
            # Wait for modal to be visible
            time.sleep(3)  # Give modal time to load
            
            # Set from date: day before target (keep to date as is)
            from_date = date - timedelta(days=1)  # Day before
            from_date_str = from_date.strftime("%m/%d/%Y")  # Format: MM/DD/YYYY
            
            print(f"  📅 Target date: {date.strftime('%Y-%m-%d')} ({date.strftime('%m/%d/%Y')})")
            print(f"  📅 Setting FROM date only:")
            print(f"    From: {from_date_str} ({from_date.strftime('%Y-%m-%d')}) - Day BEFORE target")
            print(f"    To: (keeping existing date)")
            
            # 1. Find and click the frequency dropdown (Daily -> Minutely)
            print("  📊 Changing frequency to Minutely...")
            
            frequency_success = False
            frequency_selectors = [
                "button[role='combobox']",  # Generic combobox
                "button[class*='c-gBrBnR'][aria-expanded='false']",  # Based on your HTML
                "button[data-state='closed']"  # Closed dropdown state
            ]
            
            for selector in frequency_selectors:
                try:
                    frequency_buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for button in frequency_buttons:
                        if button.is_displayed():
                            button_text = button.text.lower()
                            print(f"    Found button with text: '{button_text}'")
                            
                            if 'daily' in button_text or 'frequency' in button_text:
                                print(f"    🎯 Clicking frequency dropdown")
                                button.click()
                                time.sleep(2)
                                
                                # Look for minutely option
                                minutely_options = self.driver.find_elements(
                                    By.XPATH, 
                                    "//*[contains(text(), 'Minutely') or contains(text(), 'minutely') or contains(text(), 'Minute')]"
                                )
                                
                                for option in minutely_options:
                                    if option.is_displayed():
                                        print(f"    ✅ Clicking Minutely option")
                                        option.click()
                                        frequency_success = True
                                        time.sleep(1)
                                        break
                                
                                if frequency_success:
                                    break
                    
                    if frequency_success:
                        break
                        
                except Exception as e:
                    print(f"    ❌ Frequency selector '{selector}' failed: {e}")
                    continue
            
            if not frequency_success:
                print("  ⚠️ Could not change frequency to Minutely")
            
            # 2. Find and update ONLY the FROM date field (first one)
            print("  📅 Setting FROM date field...")
            
            # Try multiple approaches to find date picker buttons
            date_picker_approaches = [
                ("Specific closed state", "button[aria-haspopup='dialog'][data-state='closed']"),
                ("Any dialog popup", "button[aria-haspopup='dialog']"),
                ("Calendar icon buttons", "button:has(svg[viewBox='0 0 24 24'])"),
                ("Date-like buttons", "button[class*='c-gBrBnR']:has(span)"),
                ("All buttons with spans", "button:has(span.c-PJLV)"),
            ]
            
            date_picker_buttons = []
            
            for approach_name, selector in date_picker_approaches:
                try:
                    buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    print(f"    {approach_name}: Found {len(buttons)} buttons")
                    
                    # Filter for buttons that look like date pickers
                    for i, button in enumerate(buttons):
                        if button.is_displayed():
                            try:
                                # Look for date-like text in the button
                                button_text = button.text.strip()
                                spans = button.find_elements(By.CSS_SELECTOR, "span")
                                span_texts = [span.text.strip() for span in spans if span.text.strip()]
                                
                                print(f"      Button {i+1}: '{button_text}', spans: {span_texts}")
                                
                                # Check if any text looks like a date
                                all_texts = [button_text] + span_texts
                                for text in all_texts:
                                    if '/' in text and '2025' in text:
                                        print(f"        ✅ Found date picker: '{text}'")
                                        date_picker_buttons.append(button)
                                        break
                            except:
                                continue
                    
                    if date_picker_buttons:
                        print(f"    ✅ Found {len(date_picker_buttons)} date picker buttons using {approach_name}")
                        break
                        
                except Exception as e:
                    print(f"    ❌ {approach_name} failed: {e}")
                    continue
            
            if not date_picker_buttons:
                print("    🔍 No date pickers found with smart detection, trying brute force...")
                
                # Brute force: check ALL buttons on the page
                all_buttons = self.driver.find_elements(By.TAG_NAME, "button")
                print(f"    Checking all {len(all_buttons)} buttons on page...")
                
                for i, button in enumerate(all_buttons):
                    if button.is_displayed():
                        try:
                            button_text = button.text.strip()
                            if '/' in button_text and '2025' in button_text:
                                print(f"      Found date button {i+1}: '{button_text}'")
                                date_picker_buttons.append(button)
                        except:
                            continue
            
            print(f"    📊 Total date picker buttons found: {len(date_picker_buttons)}")
            
            # Only process the FIRST date picker (FROM date)
            if len(date_picker_buttons) > 0:
                button = date_picker_buttons[0]  # First button = FROM date
                
                try:
                    if button.is_displayed() and button.is_enabled():
                        # Get the current date shown in the button
                        current_date = ""
                        try:
                            # Try to find span with date
                            date_span = button.find_element(By.CSS_SELECTOR, "span.c-PJLV")
                            current_date = date_span.text.strip()
                        except:
                            # Fallback to button text
                            current_date = button.text.strip()
                        
                        print(f"    FROM date picker: '{current_date}' → {from_date_str}")
                        
                        if current_date == from_date_str:
                            print(f"      ✅ FROM date already correct, skipping")
                        else:
                            # Scroll into view
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
                            time.sleep(1)
                            
                            # Click the date picker button to open calendar
                            print(f"      🎯 Clicking FROM date picker button")
                            
                            try:
                                button.click()
                                print(f"      ✅ Button clicked successfully")
                                time.sleep(2)
                            except:
                                try:
                                    self.driver.execute_script("arguments[0].click();", button)
                                    print(f"      ✅ Button clicked via JavaScript")
                                    time.sleep(2)
                                except Exception as e:
                                    print(f"      ❌ Could not click button: {e}")
                                    raise
                            
                            # Wait for the date picker dialog to open
                            dialog_found = False
                            try:
                                # Look for the opened dialog
                                dialog = self.wait.until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, "[role='dialog']"))
                                )
                                print(f"      📅 Date picker dialog opened")
                                dialog_found = True
                                
                                # Parse target date to get day number
                                month, day, year = from_date_str.split('/')
                                target_day = int(day)
                                
                                print(f"      🎯 Looking for day {target_day} in calendar")
                                
                                # Simple approach: find any button with the target day number
                                day_buttons = dialog.find_elements(
                                    By.XPATH, 
                                    f"//button[text()='{target_day}' and @role='button']"
                                )
                                
                                if not day_buttons:
                                    # Try with different button types
                                    day_buttons = dialog.find_elements(
                                        By.XPATH, 
                                        f"//button[text()='{target_day}']"
                                    )
                                
                                if day_buttons:
                                    day_button = day_buttons[0]
                                    print(f"      ✅ Found day {target_day} button, clicking...")
                                    day_button.click()
                                    time.sleep(1)
                                    print(f"      ✅ FROM date updated successfully")
                                else:
                                    print(f"      ❌ Could not find day {target_day} button")
                                    
                                    # Debug: show all available buttons in dialog
                                    all_dialog_buttons = dialog.find_elements(By.TAG_NAME, "button")
                                    print(f"      Debug: Found {len(all_dialog_buttons)} buttons in dialog:")
                                    for i, btn in enumerate(all_dialog_buttons[:10]):
                                        btn_text = btn.text.strip()
                                        btn_role = btn.get_attribute('role') or 'no-role'
                                        if btn_text:
                                            print(f"        {i+1}. '{btn_text}' (role: {btn_role})")
                                
                            except Exception as e:
                                print(f"      ❌ Date picker dialog interaction failed: {e}")
                            
                            # Close any open dialog
                            if dialog_found:
                                try:
                                    self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                                    time.sleep(1)
                                    print(f"      📴 Dialog closed")
                                except:
                                    pass
                
                except Exception as e:
                    print(f"    ❌ FROM date picker processing failed: {e}")
            else:
                print("    ❌ No date picker buttons found")
                
                # Debug: Take a screenshot
                self.driver.save_screenshot("data/debug_no_date_pickers.png")
                print("    📸 Screenshot saved for debugging")
            
            # Verify the FROM date was set correctly
            print("  🔍 Verifying FROM date setting...")
            try:
                # Check the first date picker button again
                if date_picker_buttons:
                    button = date_picker_buttons[0]
                    try:
                        date_span = button.find_element(By.CSS_SELECTOR, "span.c-PJLV")
                        final_date = date_span.text.strip()
                    except:
                        final_date = button.text.strip()
                    
                    print(f"    Final FROM date: '{final_date}'")
                    
                    if final_date == from_date_str:
                        print(f"    ✅ FROM date correctly set!")
                    else:
                        print(f"    ⚠️ FROM date may not have been set correctly")
            except Exception as e:
                print(f"    ❌ Could not verify FROM date: {e}")
            
            # Give everything time to update
            time.sleep(2)
            
            print("✅ Modal configuration completed")
            return True
            
        except Exception as e:
            print(f"❌ Error configuring modal: {e}")
            return False
    
    def click_download_csv_button(self) -> bool:
        """
        Click the "Download (.csv)" button in the modal
        
        Returns:
            True if button was clicked successfully
        """
        print("📥 Looking for Download CSV button...")
        
        # Wait a moment for any UI updates after date changes
        time.sleep(2)
        
        # Target the specific download button you identified
        download_selectors = [
            # Your exact button class pattern
            "button[class*='c-gBrBnR'][class*='variant-primary']",
            "button[class*='c-gBrBnR-gDWzxt-variant-primary']",
            
            # Text-based selectors for the exact text
            "//button[text()='Download (.csv)']",
            "//button[contains(text(), 'Download (.csv)')]",
            "//button[contains(text(), 'Download') and contains(text(), 'csv')]",
            
            # Class-based selectors
            "button.c-gBrBnR[class*='variant-primary']",
            "button[class*='c-gBrBnR'][class*='primary']",
        ]
        
        for selector in download_selectors:
            try:
                print(f"  🔍 Trying selector: {selector}")
                
                if selector.startswith('//'):
                    # XPath selector
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    # CSS selector
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    if element.is_displayed() and element.is_enabled():
                        element_text = element.text.strip()
                        print(f"    📋 Found button: '{element_text}'")
                        
                        # Check if this is the download button
                        if 'Download' in element_text and 'csv' in element_text:
                            print(f"    🎯 Found download CSV button!")
                            
                            # Scroll into view
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                            time.sleep(1)
                            
                            try:
                                element.click()
                                print("✅ Download button clicked successfully")
                                return True
                            except:
                                try:
                                    # Try JavaScript click
                                    self.driver.execute_script("arguments[0].click();", element)
                                    print("✅ Download button clicked via JavaScript")
                                    return True
                                except Exception as e:
                                    print(f"    ❌ Click failed: {e}")
                                    continue
                                    
            except Exception as e:
                print(f"  ❌ Selector '{selector}' failed: {e}")
                continue
        
        # Fallback: Look for any button with the exact text anywhere
        print("  🔄 Fallback: Looking for any button with 'Download (.csv)' text")
        
        try:
            all_buttons = self.driver.find_elements(By.TAG_NAME, "button")
            print(f"  📊 Checking {len(all_buttons)} buttons on page")
            
            for i, button in enumerate(all_buttons):
                if button.is_displayed():
                    button_text = button.text.strip()
                    if button_text:  # Only print non-empty text
                        print(f"    Button {i+1}: '{button_text}'")
                        
                        if 'Download (.csv)' in button_text:
                            print(f"    🎯 Found exact match!")
                            try:
                                self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
                                time.sleep(1)
                                button.click()
                                print("✅ Fallback download button clicked")
                                return True
                            except:
                                try:
                                    self.driver.execute_script("arguments[0].click();", button)
                                    print("✅ Fallback download button clicked via JavaScript")
                                    return True
                                except:
                                    continue
                                    
        except Exception as e:
            print(f"  ❌ Fallback search failed: {e}")
        
        # Final debug: Take screenshot
        print("  📸 Taking screenshot for manual inspection")
        self.driver.save_screenshot("data/debug_download_button_search.png")
        
        print("❌ Download (.csv) button not found")
        return False
    
    def wait_for_download(self, timeout: int = 30) -> Optional[Path]:
        """
        Wait for the download to complete
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            Path to downloaded file or None if timeout
        """
        print(f"⏳ Waiting for download to complete (timeout: {timeout}s)...")
        
        start_time = time.time()
        initial_files = set(self.download_dir.glob("*"))
        
        while time.time() - start_time < timeout:
            current_files = set(self.download_dir.glob("*"))
            new_files = current_files - initial_files
            
            if new_files:
                # Check if any new file is not a temp file (.crdownload, .tmp, etc.)
                completed_files = [
                    f for f in new_files 
                    if not f.name.endswith(('.crdownload', '.tmp', '.part'))
                ]
                
                if completed_files:
                    downloaded_file = completed_files[0]
                    print(f"✅ Download completed: {downloaded_file.name}")
                    return downloaded_file
            
            time.sleep(1)
        
        print("❌ Download timeout")
        return None
    
    def process_downloaded_csv(self, file_path: Path, target_date: datetime) -> Optional[Path]:
        """
        Process the downloaded CSV to filter data between 16:00 timestamps
        
        Args:
            file_path: Path to the downloaded CSV file
            target_date: The target date we're analyzing
            
        Returns:
            Path to the processed CSV file or None if processing failed
        """
        print(f"📊 Processing CSV data: {file_path.name}")
        
        try:
            # Read the CSV file
            import pandas as pd
            df = pd.read_csv(file_path)
            
            print(f"  📋 Original data: {len(df)} rows")
            print(f"  📋 Columns: {list(df.columns)}")
            
            if len(df) == 0:
                print("  ⚠️ CSV file is empty")
                return file_path
            
            # Show sample of original data
            print(f"  📋 Sample rows:")
            for i, row in df.head(3).iterrows():
                print(f"    {i+1}. {dict(row)}")
            
            # Determine the timestamp column
            timestamp_columns = ['Date (UTC)', 'Timestamp (UTC)', 'timestamp', 'date', 'time']
            timestamp_col = None
            
            for col in timestamp_columns:
                if col in df.columns:
                    timestamp_col = col
                    break
            
            if not timestamp_col:
                print(f"  ❌ Could not find timestamp column in: {list(df.columns)}")
                return file_path
            
            print(f"  🕐 Using timestamp column: '{timestamp_col}'")
            
            # Convert timestamp column to datetime
            try:
                if 'Timestamp' in timestamp_col and df[timestamp_col].dtype in ['int64', 'float64']:
                    # Unix timestamp in seconds or milliseconds
                    if df[timestamp_col].max() > 1e10:  # Milliseconds
                        df['datetime'] = pd.to_datetime(df[timestamp_col], unit='ms', utc=True)
                    else:  # Seconds
                        df['datetime'] = pd.to_datetime(df[timestamp_col], unit='s', utc=True)
                else:
                    # String datetime
                    df['datetime'] = pd.to_datetime(df[timestamp_col], utc=True)
                
                print(f"  ✅ Converted timestamps successfully")
                
            except Exception as e:
                print(f"  ❌ Could not convert timestamps: {e}")
                return file_path
            
            # Show date range of data
            min_date = df['datetime'].min()
            max_date = df['datetime'].max()
            print(f"  📅 Data range: {min_date} to {max_date}")
            
            # Find 16:00 (4 PM) timestamps
            target_date_start = target_date.replace(hour=16, minute=0, second=0, microsecond=0)
            target_date_end = (target_date + timedelta(days=1)).replace(hour=16, minute=0, second=0, microsecond=0)
            
            # Convert to UTC if needed
            if target_date_start.tzinfo is None:
                target_date_start = target_date_start.replace(tzinfo=pd.Timestamp.now().tz)
                target_date_end = target_date_end.replace(tzinfo=pd.Timestamp.now().tz)
            
            print(f"  🎯 Target range: {target_date_start} to {target_date_end}")
            
            # Filter data between the 16:00 timestamps
            mask = (df['datetime'] >= target_date_start) & (df['datetime'] < target_date_end)
            filtered_df = df[mask].copy()
            
            print(f"  ✂️ Filtered data: {len(filtered_df)} rows (from {len(df)} original)")
            
            if len(filtered_df) == 0:
                print("  ⚠️ No data found in target time range")
                print("  🔍 Available hours in data:")
                df['hour'] = df['datetime'].dt.hour
                hour_counts = df['hour'].value_counts().sort_index()
                for hour, count in hour_counts.items():
                    print(f"    Hour {hour:02d}: {count} records")
                return file_path
            
            # Show sample of filtered data
            print(f"  📋 Filtered sample:")
            for i, row in filtered_df.head(3).iterrows():
                datetime_str = row['datetime'].strftime('%Y-%m-%d %H:%M:%S UTC')
                print(f"    {i+1}. {datetime_str} - Price: {row.get('Price', 'N/A')}")
            
            # Create processed filename
            original_name = file_path.stem
            processed_name = f"{original_name}_filtered_16h.csv"
            processed_path = file_path.parent / processed_name
            
            # Save filtered data (drop the helper datetime column)
            filtered_df_output = filtered_df.drop('datetime', axis=1)
            filtered_df_output.to_csv(processed_path, index=False)
            
            print(f"  💾 Saved filtered data: {processed_name}")
            print(f"  📊 Summary:")
            print(f"    Original: {len(df)} rows")
            print(f"    Filtered: {len(filtered_df)} rows")
            print(f"    Time range: {target_date_start.strftime('%Y-%m-%d %H:%M')} to {target_date_end.strftime('%Y-%m-%d %H:%M')}")
            
            return processed_path
            
        except Exception as e:
            print(f"  ❌ Error processing CSV: {e}")
            return file_path
    
    def scrape_market_data(self, date: datetime, timeout: int = 60) -> Optional[Dict]:
        """
        Scrape market data for a specific date
        
        Args:
            date: Date to scrape data for
            timeout: Total timeout for the operation
            
        Returns:
            Dictionary with scraping results
        """
        print(f"\n{'='*60}")
        print(f"🎯 SCRAPING BITCOIN MARKET DATA FOR {date.strftime('%Y-%m-%d')}")
        print(f"{'='*60}")
        
        if not self.driver:
            self.start_browser()
        
        try:
            # 1. Generate URL and navigate
            url = self.get_bitcoin_market_url(date)
            if not self.navigate_to_market(url):
                return {
                    'success': False,
                    'error': 'Could not navigate to market page',
                    'date': date.isoformat(),
                    'url': url
                }
            
            # 2. Find export button
            export_button = self.find_export_button()
            if not export_button:
                return {
                    'success': False,
                    'error': 'Export button not found',
                    'date': date.isoformat(),
                    'url': url
                }
            
            # 3. Click export button to open modal
            if not self.click_export_button(export_button):
                return {
                    'success': False,
                    'error': 'Could not click export button',
                    'date': date.isoformat(),
                    'url': url
                }
            
            # 4. Configure the download modal
            if not self.configure_download_modal(date):
                return {
                    'success': False,
                    'error': 'Could not configure download modal',
                    'date': date.isoformat(),
                    'url': url
                }
            
            # 5. Click the download CSV button
            if not self.click_download_csv_button():
                return {
                    'success': False,
                    'error': 'Could not click download button',
                    'date': date.isoformat(),
                    'url': url
                }
            
            # 6. Wait for download
            downloaded_file = self.wait_for_download(timeout=30)
            if not downloaded_file:
                return {
                    'success': False,
                    'error': 'Download timeout or failed',
                    'date': date.isoformat(),
                    'url': url
                }
            
            # 7. Rename file with date for organization
            date_str = date.strftime('%Y%m%d')
            new_filename = f"bitcoin_market_data_{date_str}_{downloaded_file.name}"
            new_path = self.download_dir / new_filename
            
            try:
                downloaded_file.rename(new_path)
                final_path = new_path
                print(f"📁 File renamed to: {new_filename}")
            except Exception as e:
                print(f"⚠️ Could not rename file: {e}")
                final_path = downloaded_file  # Keep original name if rename fails
            
            # 8. Process the CSV to filter for 16:00 time range
            print(f"\n{'='*60}")
            print(f"📊 PROCESSING CSV DATA")
            print(f"{'='*60}")
            
            processed_path = self.process_downloaded_csv(final_path, date)
            
            if processed_path and processed_path != final_path:
                # Successfully created a filtered version
                final_result_path = processed_path
                print(f"✅ Processed CSV saved: {processed_path.name}")
            else:
                # Use original file if processing failed
                final_result_path = final_path
                print(f"⚠️ Using original CSV file")
            
            return {
                'success': True,
                'date': date.isoformat(),
                'url': url,
                'original_file_path': str(final_path),
                'processed_file_path': str(final_result_path),
                'file_size': final_result_path.stat().st_size,
                'scraped_at': datetime.now().isoformat(),
                'data_filtered': processed_path != final_path if processed_path else False
            }
        
        except Exception as e:
            return {
                'success': False,
                'error': f'Unexpected error: {str(e)}',
                'date': date.isoformat(),
                'url': url if 'url' in locals() else 'unknown'
            }


def scrape_single_date(date_str: str, headless: bool = True) -> Dict:
    """
    Scrape data for a single date
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        headless: Run browser in headless mode
        
    Returns:
        Scraping result dictionary
    """
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return {
            'success': False,
            'error': f'Invalid date format: {date_str}. Use YYYY-MM-DD format.',
            'date': date_str
        }
    
    scraper = PolymarketDataScraper(headless=headless)
    
    try:
        result = scraper.scrape_market_data(date)
        return result
    finally:
        scraper.stop_browser()


def scrape_date_range(start_date: str, end_date: str, headless: bool = True) -> Dict:
    """
    Scrape data for a range of dates
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        headless: Run browser in headless mode
        
    Returns:
        Dictionary with results for all dates
    """
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError as e:
        return {
            'success': False,
            'error': f'Invalid date format: {str(e)}. Use YYYY-MM-DD format.'
        }
    
    if start > end:
        return {
            'success': False,
            'error': 'Start date must be before end date'
        }
    
    scraper = PolymarketDataScraper(headless=headless)
    results = {
        'success': True,
        'start_date': start_date,
        'end_date': end_date,
        'dates_processed': [],
        'successful_scrapes': [],
        'failed_scrapes': [],
        'total_files_downloaded': 0
    }
    
    try:
        current_date = start
        while current_date <= end:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n📅 Processing date: {date_str}")
            
            result = scraper.scrape_market_data(current_date)
            results['dates_processed'].append(date_str)
            
            if result['success']:
                results['successful_scrapes'].append(result)
                results['total_files_downloaded'] += 1
                print(f"✅ Successfully scraped {date_str}")
            else:
                results['failed_scrapes'].append(result)
                print(f"❌ Failed to scrape {date_str}: {result.get('error', 'Unknown error')}")
            
            # Small delay between requests
            time.sleep(2)
            
            current_date += timedelta(days=1)
    
    finally:
        scraper.stop_browser()
    
    return results


def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape Polymarket Bitcoin daily market data')
    parser.add_argument('date', help='Date in YYYY-MM-DD format or date range (start:end)')
    parser.add_argument('--headless', action='store_true', default=True, 
                       help='Run browser in headless mode (default: True)')
    parser.add_argument('--visible', action='store_true', 
                       help='Run browser in visible mode (overrides --headless)')
    
    args = parser.parse_args()
    
    # Handle visible mode
    headless = args.headless and not args.visible
    
    # Check if it's a date range
    if ':' in args.date:
        start_date, end_date = args.date.split(':')
        print(f"🎯 Scraping date range: {start_date} to {end_date}")
        result = scrape_date_range(start_date, end_date, headless=headless)
    else:
        print(f"🎯 Scraping single date: {args.date}")
        result = scrape_single_date(args.date, headless=headless)
    
    # Save results
    results_file = Path("data") / f"scrape_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(results_file, 'w') as f:
        json.dump(result, f, indent=2, default=str)
    
    print(f"\n📄 Results saved to: {results_file}")
    
    # Print summary
    if result.get('success'):
        if 'total_files_downloaded' in result:
            print(f"✅ Successfully downloaded {result['total_files_downloaded']} files")
        else:
            print("✅ Successfully downloaded 1 file")
    else:
        print(f"❌ Operation failed: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()