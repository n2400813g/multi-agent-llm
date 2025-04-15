# agents/data_aggregator.py
import os
import yaml
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import requests
import logging

# Configure logging
os.makedirs("data/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data/logs/data_aggregator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DataAggregator")

class DataAggregator:
    """
    Institutional-Grade Data Aggregator with yfinance Integration
    """
    
    def __init__(self):
        self.config_dir = "config"
        self.data_dir = "data"
        self.settings = self._load_yaml("settings.yaml")
        self.api_keys = self._load_yaml("api_keys.yaml")
        
    def _load_yaml(self, filename):
        """Load configuration files safely"""
        try:
            with open(os.path.join(self.config_dir, filename)) as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Config error: {str(e)}")
            return {}

    # Enhanced Data Sources ==============================================
    
    def fetch_worldbank_data(self, indicator, country_code):
        """Get cleansed economic data from World Bank"""
        url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{indicator}"
        params = {"format": "json", "date": "2013:2023", "per_page": 1000}
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()[1]
            
            return pd.DataFrame([
                {"date": pd.to_datetime(entry["date"]), "value": entry["value"]} 
                for entry in data if entry["value"] is not None
            ]).set_index('date').sort_index().asfreq('AS').ffill()
            
        except Exception as e:
            logger.error(f"World Bank error: {str(e)}")
            return None

    def fetch_yahoo_finance(self, symbol):
        """Professional market data fetching with yfinance"""
        try:
            # Institutional-grade data validation
            if not isinstance(symbol, str) or len(symbol) < 2:
                raise ValueError("Invalid symbol format")
                
            logger.info(f"Fetching institutional-grade data for {symbol}")
            
            ticker = yf.Ticker(symbol)
            df = ticker.history(
                period="10y",
                interval="1mo",
                auto_adjust=True,
                prepost=True,
                repair=True
            )
            
            # Hedge fund-style data validation
            if df.empty:
                raise ValueError("No data returned from Yahoo Finance")
                
            # Institutional data cleaning
            df = df[['Close']].rename(columns={'Close': 'close'})
            df = df.resample('M').last()  # Ensure monthly frequency
            
            return df.dropna()
            
        except Exception as e:
            logger.error(f"YFinance error: {str(e)}")
            return None

    def fetch_news_sentiment(self, country):
        """Institutional news analysis pipeline"""
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": f"{country} economy",
            "from": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            "sortBy": "relevancy",
            "pageSize": 100,
            "language": "en"
        }
        
        # Extract API key as plain string
        api_key = str(self.api_keys.get("news_api", ""))
        
        headers = {"X-Api-Key": api_key}  # Use string directly
        
        try:
            # Validate API key
            if not api_key:
                raise ValueError("News API key not found in config")
            
            logger.info(f"Using News API key: {api_key[:5]}...")
                
            response = requests.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()
            
            # Handle potential API errors
            response_data = response.json()
            if response_data.get("status") != "ok":
                raise ValueError(f"News API error: {response_data.get('message', 'Unknown error')}")
                    
            articles = response_data.get("articles", [])
            
            # Safe dataframe construction
            news_data = []
            for art in articles:
                try:
                    news_data.append({
                        "date": pd.to_datetime(art.get("publishedAt")).normalize(),
                        "title": art.get("title", ""),
                        "sentiment": self._analyze_sentiment(art.get("content")),
                        "source": art.get("source", {}).get("name", "unknown")
                    })
                except Exception as art_error:
                    logger.warning(f"Error processing article: {str(art_error)}")
                    continue
                    
            return pd.DataFrame(news_data).drop_duplicates().reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"News API error: {str(e)}")
            return None

    # Institutional Helpers ==============================================
    
    def _analyze_sentiment(self, text):
        """Basic institutional sentiment scoring"""
        if not text:
            return 0
            
        positive = len([w for w in ["rise", "growth", "strong", "bullish"] if w in str(text).lower()])
        negative = len([w for w in ["fall", "decline", "weak", "bearish"] if w in str(text).lower()])
        return (positive - negative) / max(positive + negative, 1)

    def _save_data(self, df, filename):
        """Professional data persistence"""
        if df is not None and not df.empty:
            path = os.path.join(self.data_dir, "raw", filename)
            df.to_csv(path, index=True)
            logger.info(f"Saved institutional dataset: {path}")
            return True
        return False

    # Main Aggregation Workflow ==========================================
    
    def aggregate_data(self):
        """Hedge fund-style data aggregation pipeline"""
        logger.info("Starting institutional data aggregation")
        
        for country in self.settings["countries"]:
            logger.info(f"Processing {country['name'].upper()}")
            
            # Economic Fundamentals
            self._save_data(
                self.fetch_worldbank_data("NY.GDP.MKTP.CD", country["wb_code"]),
                f"{country['name']}_gdp.csv"
            )
            
            self._save_data(
                self.fetch_worldbank_data("FP.CPI.TOTL.ZG", country["wb_code"]),
                f"{country['name']}_cpi.csv"
            )
            
            # Market Data
            self._save_data(
                self.fetch_yahoo_finance(country["stock_index"]),
                f"{country['name']}_stock.csv"
            )
            
            # Alternative Data
            self._save_data(
                self.fetch_news_sentiment(country["name"]),
                f"{country['name']}_sentiment.csv"
            )
            
        logger.info("Institutional data aggregation complete")

if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    DataAggregator().aggregate_data()
