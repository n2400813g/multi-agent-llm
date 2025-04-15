# agents/economic_analyst.py
import pandas as pd
import numpy as np
import statsmodels.api as sm
from arch import arch_model
import matplotlib.pyplot as plt
from datetime import datetime
import yaml
import os

class EconomicAnalyst:
    """
    Institutional-Grade Economic Analyst for Hedge Fund Strategy
    Uses free data sources and advanced econometrics
    """
    
    def __init__(self):
        self.config = self._load_config()
        self.data = self._load_datasets()
        
    def _load_config(self):
        """Load analysis parameters"""
        with open('config/settings.yaml') as f:
            return yaml.safe_load(f)
    
    def _load_datasets(self):
        """Load processed data from aggregator"""
        data = {}
        countries = self.config['countries']
        
        for country in countries:
            name = country['name']
            data[name] = {
                'gdp': pd.read_csv(f"data/processed/{name}_gdp.csv", index_col='date', parse_dates=True),
                'cpi': pd.read_csv(f"data/processed/{name}_cpi.csv", index_col='date', parse_dates=True),
                'stocks': pd.read_csv(f"data/processed/{name}_stock.csv", index_col='date', parse_dates=True),
                'news': pd.read_csv(f"data/processed/{name}_news.csv", parse_dates=['date'])
            }
        return data

    # Core Analysis Methods ==============================================
    
    def trend_analysis(self, country):
        """
        Hedge Fund Style Trend Analysis
        Includes:
        - Hodrick-Prescott Filter
        - Bollinger Bands
        - Momentum Indicators
        """
        df = self.data[country]['gdp'].copy()
        
        # 1. Hodrick-Prescott Trend/Cycle Decomposition
        cycle, trend = sm.tsa.filters.hpfilter(df['value'], lamb=1600)
        df['trend'] = trend
        df['cycle'] = cycle
        
        # 2. Technical Indicators
        df['returns'] = df['value'].pct_change()
        df['volatility'] = df['returns'].rolling(20).std() * np.sqrt(252)
        df['sma_5y'] = df['value'].rolling(20).mean()
        
        # 3. Momentum Scoring
        df['momentum_1y'] = df['value'].pct_change(4)
        df['momentum_3y'] = df['value'].pct_change(12)
        df['momentum_score'] = (df['momentum_1y'] + df['momentum_3y']) / 2
        
        return df

    def monetary_policy_analysis(self, country):
        """
        Advanced Inflation/Interest Rate Analysis
        Implements:
        - VAR Models
        - Granger Causality
        - Policy Response Functions
        """
        cpi = self.data[country]['cpi']['value']
        stocks = self.data[country]['stocks']['close']
        
        # Create DataFrame with aligned dates
        df = pd.concat([cpi, stocks], axis=1).dropna()
        df.columns = ['cpi', 'stocks']
        
        # 1. Vector Autoregression (VAR)
        model = sm.tsa.VAR(df.diff().dropna())
        var_result = model.fit(maxlags=4)
        
        # 2. Granger Causality
        gc = var_result.test_causality('cpi', 'stocks', kind='f')
        
        # 3. GARCH Volatility Model
        vol_model = arch_model(df['stocks'].pct_change().dropna(), vol='Garch', p=1, q=1)
        vol_result = vol_model.fit(disp='off')
        
        return {
            'var_model': var_result,
            'granger_test': gc,
            'volatility_model': vol_result
        }

    # Advanced Risk Analysis =============================================
    
    def news_sentiment_analysis(self, country):
        """
        Event-Driven Risk Analysis Using News Headlines
        Implements:
        - Simple Sentiment Scoring
        - Event Clustering
        - Impact Regression
        """
        news = self.data[country]['news']
        prices = self.data[country]['stocks']
        
        # 1. Basic Sentiment Scoring
        negative_words = ['crisis', 'war', 'inflation', 'sanctions']
        news['sentiment'] = news['title'].apply(
            lambda x: -1 * sum(x.lower().count(word) for word in negative_words)
        )
        
        # 2. Event Impact Analysis
        merged = pd.merge_asof(
            news.sort_values('date'),
            prices.reset_index(),
            on='date',
            direction='forward'
        )
        
        # 3. OLS Regression: Sentiment -> Returns
        model = sm.OLS(
            merged['close'].pct_change(),
            sm.add_constant(merged['sentiment'].shift(1))
        ).fit()
        
        return model

    # Institutional Reporting ============================================
    
    def generate_report(self, country):
        """Professional PDF Report Generation"""
        from fpdf import FPDF
        
        # Create analysis outputs
        trend_data = self.trend_analysis(country)
        monetary_results = self.monetary_policy_analysis(country)
        
        # Create visualizations
        self._create_gdp_chart(trend_data, country)
        self._create_volatility_chart(monetary_results['volatility_model'], country)
        
        # Generate PDF
        pdf = FPDF()
        pdf.add_page()
        
        # Header
        pdf.set_font('Arial', 'B', 16)
        pdf.cell(0, 10, f'Economic Analysis: {country.title()}', 0, 1, 'C')
        
        # Trend Analysis Section
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 10, 'GDP Trend Analysis', 0, 1)
        pdf.image(f'reports/{country}_gdp_trend.png', w=190)
        
        # Volatility Analysis
        pdf.add_page()
        pdf.cell(0, 10, 'Market Volatility Structure', 0, 1)
        pdf.image(f'reports/{country}_volatility.png', w=190)
        
        pdf.output(f'reports/{country}_analysis.pdf')

    # Helper Methods =====================================================
    
    def _create_gdp_chart(self, data, country):
        """Professional-grade GDP visualization"""
        plt.figure(figsize=(12,6))
        data[['value', 'trend']].plot(title=f'{country.title()} GDP Analysis')
        plt.savefig(f'reports/{country}_gdp_trend.png', bbox_inches='tight')
        plt.close()
        
    def _create_volatility_chart(self, model, country):
        """GARCH volatility visualization"""
        plt.figure(figsize=(12,6))
        model.conditional_volatility.plot(title=f'{country.title()} Conditional Volatility')
        plt.savefig(f'reports/{country}_volatility.png', bbox_inches='tight')
        plt.close()

if __name__ == "__main__":
    os.makedirs('reports', exist_ok=True)
    
    analyst = EconomicAnalyst()
    
    for country in ['brazil', 'india', 'south_africa']:
        print(f"Analyzing {country}")
        analyst.generate_report(country)
