import pandas as pd
import numpy as np
import boto3
import json
import plotly.express as px
import plotly.graph_objects as go
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta

class NewsAnalyticsPipeline:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, bucket_name):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.bucket_name = bucket_name

    def fetch_data_from_s3(self, theme):
        """Fetch processed data from S3 bucket"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f'processed_data/{theme.replace(" ", "_")}.json'
            )
            data = json.loads(response['Body'].read().decode('utf-8'))
            return pd.DataFrame(data)
        except Exception as e:
            print(f"Error fetching data from S3: {e}")
            return None

    def prepare_features(self, df):
        """Prepare features for analysis"""
        # Convert date strings to datetime
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Group by date to get article counts
        daily_counts = df.groupby('date').size().reset_index(name='article_count')
        
        # Create time-based features
        daily_counts['article_count_lag_1'] = daily_counts['article_count'].shift(1)
        daily_counts['article_count_lag_3'] = daily_counts['article_count'].shift(3)
        daily_counts['article_count_lag_7'] = daily_counts['article_count'].shift(7)
        
        # Calculate rolling means
        daily_counts['rolling_mean_7d'] = daily_counts['article_count'].rolling(window=7).mean()
        daily_counts['rolling_mean_30d'] = daily_counts['article_count'].rolling(window=30).mean()
        
        # Calculate momentum indicators
        daily_counts['momentum_7d'] = daily_counts['article_count'] - daily_counts['article_count'].shift(7)
        
        # Merge back with original data
        df = pd.merge(df, daily_counts, on='date', how='left')
        
        return df.dropna()

    def analyze_sentiment_trends(self, df):
        """Analyze sentiment trends"""
        sentiment_analysis = {
            'avg_tone': df['tone'].mean(),
            'tone_trend': df['tone'].diff().mean(),
            'sentiment_volatility': df['tone'].std(),
            'recent_sentiment': df['tone'].tail(7).mean()
        }
        return sentiment_analysis

    def analyze_geographic_spread(self, df):
        """Analyze geographic distribution"""
        # Handle empty or null cases
        if df.empty or 'countries' not in df:
            return {
                'total_countries': 0,
                'top_countries': {},
                'geographic_concentration': 0.0
            }

        # Explode and count countries
        country_counts = df['countries'].explode().value_counts()
        
        # Handle empty country counts
        if country_counts.empty:
            return {
                'total_countries': 0,
                'top_countries': {},
                'geographic_concentration': 0.0
            }

        total_articles = country_counts.sum()
        top_5_sum = country_counts.head(5).sum()

        return {
            'total_countries': len(country_counts),
            'top_countries': country_counts.head(5).to_dict(),
            'geographic_concentration': float(top_5_sum / total_articles) if total_articles > 0 else 0.0
        }

    def create_visualizations(self, df):
        """Create analysis visualizations"""
        figs = {}
        
        # Article Volume Trend - use daily counts
        daily_counts = df.groupby('date').size().reset_index(name='article_count')
        daily_counts['rolling_mean_7d'] = daily_counts['article_count'].rolling(window=7).mean()
        
        fig_volume = px.line(daily_counts, x='date', 
                            y=['article_count', 'rolling_mean_7d'],
                            title='Article Volume Trend')
        figs['volume_trend'] = fig_volume

        # Sentiment Distribution
        fig_sentiment = px.histogram(df, x='tone', 
                                    title='Sentiment Distribution',
                                    nbins=50)
        figs['sentiment_dist'] = fig_sentiment

        # Geographic Distribution
        country_data = df['countries'].explode().value_counts().head(10)
        # Convert to DataFrame for plotting
        country_df = pd.DataFrame({
            'country': country_data.index,
            'count': country_data.values
        })
        fig_geo = px.bar(country_df, 
                        x='country', 
                        y='count',
                        title='Top 10 Countries by Coverage')
        figs['geo_dist'] = fig_geo

        # Momentum Indicator
        daily_counts['momentum_7d'] = daily_counts['article_count'] - \
                                    daily_counts['article_count'].shift(7)
        fig_momentum = go.Figure()
        fig_momentum.add_trace(go.Scatter(x=daily_counts['date'], 
                                        y=daily_counts['momentum_7d'],
                                        mode='lines', 
                                        name='7-day Momentum'))
        fig_momentum.update_layout(title='Topic Momentum (7-day)')
        figs['momentum'] = fig_momentum

        return figs

    def generate_insights(self, df, sentiment_analysis, geo_analysis):
        """Generate key insights from the analysis"""
        recent_trend = df['momentum_7d'].tail(7).mean()
        
        insights = {
            'trend_strength': {
                'value': recent_trend,
                'interpretation': 'increasing' if recent_trend > 0 else 'decreasing'
            },
            'sentiment_outlook': {
                'value': sentiment_analysis['recent_sentiment'],
                'interpretation': 'positive' if sentiment_analysis['recent_sentiment'] > 0 else 'negative'
            },
            'geographic_reach': {
                'value': geo_analysis['total_countries'],
                'interpretation': 'global' if geo_analysis['total_countries'] > 20 else 'regional'
            }
        }
        return insights

    def run_analysis(self, theme):
        """Run the complete analysis pipeline"""
        # Fetch data
        df = self.fetch_data_from_s3(theme)
        if df is None:
            return None

        # Prepare features
        df_processed = self.prepare_features(df)

        # Run analyses
        sentiment_analysis = self.analyze_sentiment_trends(df_processed)
        geo_analysis = self.analyze_geographic_spread(df_processed)
        visualizations = self.create_visualizations(df_processed)
        insights = self.generate_insights(df_processed, sentiment_analysis, geo_analysis)

        return {
            'data': df_processed,
            'sentiment_analysis': sentiment_analysis,
            'geographic_analysis': geo_analysis,
            'visualizations': visualizations,
            'insights': insights
        }