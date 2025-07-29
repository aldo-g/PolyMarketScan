from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json

def scrape_polymarket_chart(url):
    """Scrape chart data using browser automation"""
    
    # Setup Chrome driver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run headless
    driver = webdriver.Chrome(options=options)
    
    try:
        print(f"Loading page: {url}")
        driver.get(url)
        
        # Wait for chart to load
        time.sleep(5)
        
        # Look for chart data in network requests or DOM
        # This would need to be customized based on how Polymarket loads chart data
        
        # Try to find chart container
        chart_elements = driver.find_elements(By.CSS_SELECTOR, "[data-testid*='chart'], .chart, canvas")
        
        for element in chart_elements:
            print(f"Found chart element: {element.tag_name} - {element.get_attribute('class')}")
        
        # Execute JavaScript to get chart data if it's stored in window object
        chart_data = driver.execute_script("""
            // Look for common chart data storage patterns
            if (window.chartData) return window.chartData;
            if (window.Polymarket && window.Polymarket.chartData) return window.Polymarket.chartData;
            
            // Try to find data in global variables
            for (let key in window) {
                if (key.includes('chart') || key.includes('Chart')) {
                    console.log('Found chart-related key:', key);
                }
            }
            
            return null;
        """)
        
        if chart_data:
            print("Found chart data!")
            return chart_data
        else:
            print("No chart data found in JavaScript")
            
    finally:
        driver.quit()
    
    return None

# Test on July 23 market
url = "https://polymarket.com/event/bitcoin-up-or-down-on-july-23?tid=1753775789482"
chart_data = scrape_polymarket_chart(url)
