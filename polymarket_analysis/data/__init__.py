"""
Data collection modules for Polymarket and Bitcoin APIs
"""

from .polymarket_client import PolymarketClient
from .bitcoin_client import BitcoinClient

__all__ = ["PolymarketClient", "BitcoinClient"]
