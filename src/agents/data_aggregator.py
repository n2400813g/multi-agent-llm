import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
import wbgapi as wb
import fredpy as fp

# API keys (replace with your actual keys)
ALPHA_VANTAGE_API_KEY = 'CBIGQHLUM03IHTJ1'
FRED_API_KEY = 'fac50a842b34957c64da118c1d15a3b6'

# Main functions will go here

# Execute the data collection
if __name__ == "__main__":
    # Run the data aggregator
    collected_data = run_data_aggregator()
    
    # Perform analyses if data was collected
    if 'EUR_USD' in collected_data:
        analyze_forex_trends(collected_data['EUR_USD'])
    
    if 'US_GDP' in collected_data and 'US_UNEMPLOYMENT' in collected_data:
        analyze_economic_indicators(collected_data['US_GDP'], 
                                   collected_data['US_UNEMPLOYMENT'])
    
    print("Data aggregation and analysis complete!")
    
def fetch_forex_data(from_currency, to_currency):
    """Fetch forex exchange rate data using Alpha Vantage API"""
    BASE_URL = 'https://www.alphavantage.co/query?'
    
    parameters = {
        'function': 'FX_DAILY',
        'from_symbol': from_currency,
        'to_symbol': to_currency,
        'outputsize': 'compact',
        'apikey': ALPHA_VANTAGE_API_KEY
    }
    
    response = requests.get(BASE_URL, params=parameters)
    data = response.json()
    
    if "Error Message" in data:
        print(f"Error: {data['Error Message']}")
        return None
    
    # Extract the time series data
    ts_data = data['Time Series FX (Daily)']
    
    # Convert to DataFrame
    df = pd.DataFrame.from_dict(ts_data, orient='index')
    df.columns = ['Open', 'High', 'Low', 'Close']
    
    # Convert index to datetime and values to float
    df.index = pd.to_datetime(df.index)
    df = df.astype(float)
    
    print(f"Successfully fetched {from_currency}/{to_currency} exchange rate data")
    return df

def fetch_commodity_data(symbol):
    """Fetch commodity price data using Alpha Vantage API"""
    BASE_URL = 'https://www.alphavantage.co/query?'
    
    parameters = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'outputsize': 'compact',
        'apikey': ALPHA_VANTAGE_API_KEY
    }
    
    response = requests.get(BASE_URL, params=parameters)
    data = response.json()
    
    if "Error Message" in data:
        print(f"Error: {data['Error Message']}")
        return None
    
    # Extract the time series data
    if 'Time Series (Daily)' not in data:
        print("Invalid API response. Make sure the symbol is correct.")
        return None
    
    ts_data = data['Time Series (Daily)']
    
    # Convert to DataFrame
    df = pd.DataFrame.from_dict(ts_data, orient='index')
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    
    # Convert index to datetime and values to float
    df.index = pd.to_datetime(df.index)
    df = df.astype(float)
    
    print(f"Successfully fetched {symbol} commodity data")
    return df

def handle_rate_limit():
    """Handle Alpha Vantage API rate limits by waiting"""
    print("Waiting for 60 seconds due to API rate limits...")
    time.sleep(60)  # Sleep for one minute between requests


def setup_fred_api():
    """Set up FRED API with your API key"""
    fp.api_key = FRED_API_KEY
    print("FRED API key set successfully")

def fetch_fred_series(series_id):
    """Fetch data series from FRED using fredpy"""
    try:
        # Download the series data
        series = fp.series(series_id)
        
        # Extract the data as a pandas Series
        data = series.data
        
        print(f"Successfully fetched FRED series: {series.title}")
        return data
    except Exception as e:
        print(f"Error fetching FRED series {series_id}: {str(e)}")
        return None
    
def search_world_bank_indicator(search_term):
    """Search for World Bank indicators by keyword"""
    results = wb.search(search_term)
    return results

def fetch_world_bank_data(country_code, indicator, start_year=2000, end_year=2022):
    """Fetch data from World Bank API for a specific country and indicator"""
    try:
        # Fetch the data
        data = wb.data.DataFrame(indicator, 
                                economy=country_code, 
                                time=range(start_year, end_year + 1))
        
        # Clean and reshape the data
        if data.empty:
            print(f"No data found for indicator {indicator} in {country_code}")
            return None
            
        # Transpose the data for easier analysis
        data = data.T
        
        # The first row contains the indicator names
        data.columns = data.iloc[0]
        data = data.iloc[1:]
        
        print(f"Successfully fetched World Bank data for {indicator} in {country_code}")
        return data
    except Exception as e:
        print(f"Error fetching World Bank data: {str(e)}")
        return None

def run_data_aggregator():
    """Main function to run the data aggregator"""
    print("Starting Data Aggregator Agent...")
    
    # Dictionary to store all collected data
    aggregated_data = {}
    
    # 1. Fetch forex data (example: EUR/USD)
    forex_data = fetch_forex_data('EUR', 'USD')
    if forex_data is not None:
        aggregated_data['EUR_USD'] = forex_data
    
    # Wait due to rate limits
    handle_rate_limit()
    
    # 2. Fetch commodity data (example: Gold ETF)
    commodity_data = fetch_commodity_data('GLD')
    if commodity_data is not None:
        aggregated_data['GOLD'] = commodity_data
    
    # 3. Set up FRED API
    setup_fred_api()
    
    # 4. Fetch US unemployment rate from FRED
    unemployment_data = fetch_fred_series('UNRATE')
    if unemployment_data is not None:
        aggregated_data['US_UNEMPLOYMENT'] = unemployment_data
    
    # 5. Fetch GDP data from FRED
    gdp_data = fetch_fred_series('GDPC1')
    if gdp_data is not None:
        aggregated_data['US_GDP'] = gdp_data
    
    # 6. Fetch World Bank data for GDP growth (example: United States)
    # Indicator NY.GDP.MKTP.KD.ZG is GDP growth annual %
    wb_gdp_growth = fetch_world_bank_data('USA', 'NY.GDP.MKTP.KD.ZG')
    if wb_gdp_growth is not None:
        aggregated_data['WB_US_GDP_GROWTH'] = wb_gdp_growth
    
    # 7. Fetch World Bank data for population (example: United States)
    # Indicator SP.POP.TOTL is total population
    wb_population = fetch_world_bank_data('USA', 'SP.POP.TOTL')
    if wb_population is not None:
        aggregated_data['WB_US_POPULATION'] = wb_population
    
    print("\nData aggregation complete!")
    return aggregated_data

def analyze_forex_trends(forex_data):
    """Analyze forex data for trends"""
    if forex_data is None:
        return
    
    # Calculate moving averages
    forex_data['MA_7'] = forex_data['Close'].rolling(window=7).mean()
    forex_data['MA_30'] = forex_data['Close'].rolling(window=30).mean()
    
    # Plot the data
    plt.figure(figsize=(12, 6))
    plt.plot(forex_data.index, forex_data['Close'], label='Exchange Rate')
    plt.plot(forex_data.index, forex_data['MA_7'], label='7-Day MA')
    plt.plot(forex_data.index, forex_data['MA_30'], label='30-Day MA')
    plt.title('Forex Exchange Rate Trends')
    plt.xlabel('Date')
    plt.ylabel('Exchange Rate')
    plt.legend()
    plt.grid(True)
    plt.show()

def analyze_economic_indicators(gdp_data, unemployment_data):
    """Analyze relationship between GDP and unemployment"""
    if gdp_data is None or unemployment_data is None:
        return
    
    # Align the dates (both series might have different frequencies)
    common_dates = gdp_data.index.intersection(unemployment_data.index)
    gdp_aligned = gdp_data.loc[common_dates]
    unemployment_aligned = unemployment_data.loc[common_dates]
    
    # Create a combined DataFrame
    combined_df = pd.DataFrame({
        'GDP': gdp_aligned.values,
        'Unemployment': unemployment_aligned.values
    }, index=common_dates)
    
    # Calculate correlation
    correlation = combined_df['GDP'].corr(combined_df['Unemployment'])
    print(f"Correlation between GDP and Unemployment: {correlation:.4f}")
    
    # Plot the data
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    ax1.set_xlabel('Date')
    ax1.set_ylabel('GDP (Billions of Dollars)', color='blue')
    ax1.plot(combined_df.index, combined_df['GDP'], color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    
    ax2 = ax1.twinx()
    ax2.set_ylabel('Unemployment Rate (%)', color='red')
    ax2.plot(combined_df.index, combined_df['Unemployment'], color='red')
    ax2.tick_params(axis='y', labelcolor='red')
    
    plt.title('GDP vs Unemployment Rate')
    plt.grid(True)
    plt.show()
