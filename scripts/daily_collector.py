"""
Daily Bitcoin Market Data Collector for CRON Jobs
Runs at 12:00 PM to collect previous day's data (16:00-to-16:00)

File: scripts/daily_collector.py

Usage in CRON:
0 12 * * * cd /path/to/polymarket_analysis && /usr/bin/python3 scripts/daily_collector.py >> logs/cron.log 2>&1
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

# Add the project root to Python path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import our scraper classes
from scripts.scrape_market_data import PolymarketDataScraper


class DailyCollector:
    """Automated daily data collector for CRON jobs"""
    
    def __init__(self, data_dir: str = None, log_dir: str = None):
        """
        Initialize the daily collector
        
        Args:
            data_dir: Directory to store collected data (default: project_root/data)
            log_dir: Directory to store log files (default: project_root/logs)
        """
        # Set default directories relative to project root
        project_root = Path(__file__).parent.parent
        
        self.data_dir = Path(data_dir) if data_dir else project_root / "data"
        self.log_dir = Path(log_dir) if log_dir else project_root / "logs"
        
        # Create directories
        self.data_dir.mkdir(exist_ok=True)
        self.log_dir.mkdir(exist_ok=True)
        
        # Setup logging
        self.setup_logging()
        
        # Calculate target date (yesterday when run at 12:00 PM)
        self.target_date = datetime.now().date() - timedelta(days=1)
        
        self.logger.info(f"=== Daily Collector Started ===")
        self.logger.info(f"Current time: {datetime.now().isoformat()}")
        self.logger.info(f"Target date for collection: {self.target_date}")
        self.logger.info(f"Data will cover: {self.target_date - timedelta(days=1)} 16:00 to {self.target_date} 16:00")
    
    def setup_logging(self):
        """Setup logging for CRON job monitoring"""
        log_file = self.log_dir / f"bitcoin_collector_{datetime.now().strftime('%Y%m')}.log"
        
        # Create logger
        self.logger = logging.getLogger('BitcoinCollector')
        self.logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # File handler for persistent logging
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Console handler for CRON output
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def check_if_already_collected(self) -> bool:
        """
        Check if data for target date has already been collected today
        
        Returns:
            True if data already exists, False otherwise
        """
        date_str = self.target_date.strftime('%Y%m%d')
        
        # Look for existing files with today's date
        existing_files = list(self.data_dir.glob(f"bitcoin_market_{date_str}_*.csv"))
        
        if existing_files:
            self.logger.info(f"Data already collected for {self.target_date}: {len(existing_files)} files found")
            for file in existing_files:
                self.logger.info(f"  Existing file: {file.name} ({file.stat().st_size} bytes)")
            return True
        
        self.logger.info(f"No existing data found for {self.target_date} - proceeding with collection")
        return False
    
    def collect_data(self, force: bool = False) -> Dict:
        """
        Collect data for the target date
        
        Args:
            force: Force collection even if data already exists
            
        Returns:
            Collection result dictionary
        """
        self.logger.info(f"Starting data collection for {self.target_date}")
        
        # Check if already collected (unless forced)
        if not force and self.check_if_already_collected():
            return {
                'success': True,
                'skipped': True,
                'reason': 'Data already collected',
                'date': self.target_date.isoformat()
            }
        
        # Initialize scraper with headless mode for CRON
        scraper = PolymarketDataScraper(
            headless=True,
            download_dir=str(self.data_dir),
            verbose=False  # Minimal output for CRON
        )
        
        try:
            self.logger.info(f"Initializing browser for data collection...")
            
            # Convert date to datetime for scraper
            target_datetime = datetime.combine(self.target_date, datetime.min.time())
            
            # Collect the data
            result = scraper.scrape_market_data(target_datetime)
            
            if result['success']:
                self.logger.info(f"✅ Data collection successful!")
                self.logger.info(f"Original file: {Path(result['original_file']).name}")
                self.logger.info(f"Processed file: {Path(result['processed_file']).name}")
                self.logger.info(f"File size: {result['file_size']:,} bytes")
                
                # Save collection metadata
                self.save_collection_metadata(result)
                
            else:
                self.logger.error(f"❌ Data collection failed: {result.get('error', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Unexpected error during collection: {str(e)}")
            return {
                'success': False,
                'error': f'Unexpected error: {str(e)}',
                'date': self.target_date.isoformat()
            }
        
        finally:
            # Always cleanup browser
            try:
                scraper.stop_browser()
                self.logger.info("Browser cleanup completed")
            except:
                pass
    
    def save_collection_metadata(self, result: Dict):
        """Save metadata about the collection for tracking"""
        metadata = {
            'collection_date': datetime.now().isoformat(),
            'target_date': self.target_date.isoformat(),
            'time_range': f"{self.target_date - timedelta(days=1)} 16:00 to {self.target_date} 16:00",
            'result': result,
            'collector_version': '1.0'
        }
        
        metadata_file = self.data_dir / f"metadata_{self.target_date.strftime('%Y%m%d')}.json"
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        self.logger.info(f"Collection metadata saved: {metadata_file.name}")
    
    def cleanup_old_logs(self, keep_days: int = 30):
        """Clean up log files older than specified days"""
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        
        for log_file in self.log_dir.glob("*.log"):
            try:
                file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
                if file_time < cutoff_date:
                    log_file.unlink()
                    self.logger.info(f"Cleaned up old log: {log_file.name}")
            except Exception as e:
                self.logger.warning(f"Could not clean up {log_file.name}: {e}")
    
    def run(self, force: bool = False) -> Dict:
        """
        Main run method for CRON execution
        
        Args:
            force: Force collection even if data exists
            
        Returns:
            Execution result
        """
        try:
            # Clean up old logs first
            self.cleanup_old_logs()
            
            # Collect data
            result = self.collect_data(force=force)
            
            # Log final status
            if result.get('success'):
                if result.get('skipped'):
                    self.logger.info(f"✅ Collection completed (skipped - data exists)")
                else:
                    self.logger.info(f"✅ Collection completed successfully")
            else:
                self.logger.error(f"❌ Collection failed: {result.get('error', 'Unknown error')}")
            
            self.logger.info(f"=== Daily Collector Finished ===")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Critical error in daily collector: {str(e)}")
            return {
                'success': False,
                'error': f'Critical error: {str(e)}',
                'date': self.target_date.isoformat()
            }


def main():
    """Main function for CRON execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Daily Bitcoin market data collector for CRON')
    parser.add_argument('--force', action='store_true', 
                       help='Force collection even if data already exists')
    parser.add_argument('--data-dir', 
                       help='Directory to store data files (default: project_root/data)')
    parser.add_argument('--log-dir',
                       help='Directory to store log files (default: project_root/logs)')
    parser.add_argument('--test-date', 
                       help='Test with specific date (YYYY-MM-DD) instead of yesterday')
    
    args = parser.parse_args()
    
    # Initialize collector
    collector = DailyCollector(data_dir=args.data_dir, log_dir=args.log_dir)
    
    # Override target date if testing
    if args.test_date:
        try:
            collector.target_date = datetime.strptime(args.test_date, '%Y-%m-%d').date()
            collector.logger.info(f"Using test date: {collector.target_date}")
        except ValueError:
            collector.logger.error(f"Invalid test date format: {args.test_date}. Use YYYY-MM-DD")
            sys.exit(1)
    
    # Run collection
    result = collector.run(force=args.force)
    
    # Exit with appropriate code for CRON monitoring
    if result['success']:
        sys.exit(0)  # Success
    else:
        sys.exit(1)  # Failure


if __name__ == "__main__":
    main()