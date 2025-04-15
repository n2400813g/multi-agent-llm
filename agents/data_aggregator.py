# agents/data_aggregator.py

# agents/data_aggregator.py
import os
import yaml
import pandas as pd
from datetime import datetime, timedelta
import requests
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/data_aggregator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DataAggregator")

class DataAggregator:
    """
    Simplified Data Aggregator using free data sources
    """
    
    def __init__(self):
        self.config_dir = "config"
        self.data_dir = "data"
        self.settings = self._load_yaml("settings.yaml")
        self.api_keys = self._load_yaml("api_keys.yaml")
        
    def _load_yaml(self, filename):
        """Load configuration files"""
        try:
            with open(f"{self.config_dir}/{filename}") as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading {filename}: {str(e)}")
            return {}

    # New free data sources ========================
    
    def fetch_worldbank_data(self, indicator, country_code):
        """Get economic data from World Bank API"""
        url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{indicator}"
        params = {
            "format": "json",
            "date": "2013:2023",
            "per_page": 1000
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()[1]
            df = pd.DataFrame([
                {
                    "date": entry["date"],
                    "value": entry["value"]
                } for entry in data
            ])
            df['date'] = pd.to_datetime(df['date'])
            return df.set_index('date')
        except Exception as e:
            logger.error(f"World Bank error: {str(e)}")
            return None

    def fetch_yahoo_finance(self, symbol):
        """Get market data from Yahoo Finance"""
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {
            "interval": "1mo",
            "range": "10y"
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()["chart"]["result"][0]
            timestamps = data["timestamp"]
            prices = data["indicators"]["quote"][0]["close"]
            
            return pd.DataFrame({
                "date": pd.to_datetime(timestamps, unit='s'),
                "close": prices
            }).set_index('date')
        except Exception as e:
            logger.error(f"Yahoo Finance error: {str(e)}")
            return None

    def fetch_news(self, country):
        """Get news articles from NewsAPI"""
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": country,
            "from": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            "sortBy": "publishedAt",
            "apiKey": self.api_keys["news_api"],
            "pageSize": 100
        }
        
        try:
            response = requests.get(url, params=params)
            articles = response.json().get("articles", [])
            return pd.DataFrame([
                {
                    "date": pd.to_datetime(article["publishedAt"]),
                    "title": article["title"],
                    "content": article["content"]
                } for article in articles
            ])
        except Exception as e:
            logger.error(f"News API error: {str(e)}")
            return None

    # Main aggregation workflow =====================
    
    def aggregate_data(self):
        """Main function to collect all data"""
        countries = self.settings["countries"]
        
        for country in countries:
            logger.info(f"Processing {country}")
            
            # Economic indicators
            self._save_data(
                self.fetch_worldbank_data("NY.GDP.MKTP.CD", country["wb_code"]),
                f"{country['name']}_gdp.csv"
            )
            
            self._save_data(
                self.fetch_worldbank_data("FP.CPI.TOTL.ZG", country["wb_code"]),
                f"{country['name']}_cpi.csv"
            )
            
            # Market data
            self._save_data(
                self.fetch_yahoo_finance(country["stock_index"]),
                f"{country['name']}_stock.csv"
            )
            
            # News data
            self._save_data(
                self.fetch_news(country["name"]),
                f"{country['name']}_news.csv"
            )

    def _save_data(self, df, filename):
        """Helper to save data to CSV"""
        if df is not None:
            path = f"{self.data_dir}/raw/{filename}"
            df.to_csv(path)
            logger.info(f"Saved {path}")

if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    aggregator = DataAggregator()
    aggregator.aggregate_data()
