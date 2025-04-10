import requests
import pandas as pd
from alpha_vantage.foreignexchange import ForeignExchange
from fredapi import Fred
from config import ALPHA_VANTAGE_KEY, FRED_API_KEY

def fetch_forex_data():
    fx = ForeignExchange(key=ALPHA_VANTAGE_KEY)
    brl_usd, _ = fx.get_currency_exchange_daily(from_symbol='BRL', to_symbol='USD')
    inr_usd, _ = fx.get_currency_exchange_daily(from_symbol='INR', to_symbol='USD')
    zar_usd, _ = fx.get_currency_exchange_daily(from_symbol='ZAR', to_symbol='USD')

    # Convert data into pandas DataFrame
    brl_df = pd.DataFrame(brl_usd).T.reset_index()
    inr_df = pd.DataFrame(inr_usd).T.reset_index()
    zar_df = pd.DataFrame(zar_usd).T.reset_index()

    # Save raw data to CSV files
    brl_df.to_csv("data/raw/brl_usd.csv", index=False)
    inr_df.to_csv("data/raw/inr_usd.csv", index=False)
    zar_df.to_csv("data/raw/zar_usd.csv", index=False)

    print("Forex data saved!")

def fetch_inflation_data():
    fred = Fred(api_key=FRED_API_KEY)

    # Get inflation rate series IDs (CPI)
    brazil_cpi = fred.get_series('CPALTT01BRQ657N')  # Brazil CPI (Quarterly)
    india_cpi = fred.get_series('CPALTT01INQ657N')   # India CPI (Quarterly)
    sa_cpi = fred.get_series('CPALTT01ZAQ657N')     # South Africa CPI (Quarterly)

    # Convert series into DataFrames
    brazil_df = pd.DataFrame(brazil_cpi).reset_index()
    india_df = pd.DataFrame(india_cpi).reset_index()
    sa_df = pd.DataFrame(sa_cpi).reset_index()

    # Save raw data to CSV files
    brazil_df.to_csv("data/raw/brazil_cpi.csv", index=False)
    india_df.to_csv("data/raw/india_cpi.csv", index=False)
    sa_df.to_csv("data/raw/sa_cpi.csv", index=False)

    print("Inflation data saved!")

def fetch_gdp_data():
    url_template = "http://api.worldbank.org/v2/country/{}/indicator/NY.GDP.MKTP.CD?format=json"

    countries = {
        "Brazil": "BRA",
        "India": "IND",
        "South Africa": "ZAF"
    }

    for country_name, country_code in countries.items():
        response = requests.get(url_template.format(country_code))
        if response.status_code == 200:
            data = response.json()[1]  # Extract relevant part of JSON response

            # Convert JSON into DataFrame
            gdp_data = pd.DataFrame(data)[['date', 'value']]
            gdp_data.to_csv(f"src/agents/data-aggregator/data/raw/{country_name.lower()}_gdp.csv", index=False)

            print(f"GDP data for {country_name} saved!")

if __name__ == "__main__":
    print("Starting Data Aggregator Agent...")
    
    fetch_forex_data()
    fetch_inflation_data()
    fetch_gdp_data()

    print("Data aggregation complete!")
