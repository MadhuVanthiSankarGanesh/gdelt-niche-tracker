import pandas as pd
import numpy as np
from datetime import datetime
import re
from collections import Counter
from typing import List, Dict, Any, Tuple
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.sentiment_analyzer = None
    
    def process_articles(self, articles: List[Dict]) -> pd.DataFrame:
        """Process raw articles into structured DataFrame with sentiment analysis"""
        if not articles:
            return pd.DataFrame()
        
        # Convert to DataFrame first
        df = pd.DataFrame(articles)
        
        if df.empty:
            return df
        
        # Convert date strings to datetime
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%dT%H%M%SZ', errors='coerce')
        
        # Extract date components
        df['year'] = df['date'].dt.year.fillna(0).astype(int)
        df['month'] = df['date'].dt.month.fillna(0).astype(int)
        df['day'] = df['date'].dt.day.fillna(0).astype(int)
        df['weekday'] = df['date'].dt.weekday.fillna(0).astype(int)
        df['hour'] = df['date'].dt.hour.fillna(0).astype(int)
        
        # Clean and standardize data
        df['source_country'] = df['source_country'].fillna('unknown').astype(str)
        df['region'] = df['region'].fillna('unknown').astype(str)
        df['domain'] = df['url'].apply(self._extract_domain)
        df['title'] = df['title'].fillna('No title').astype(str)
        
        # Analyze sentiment
        from sentiment_analyzer import sentiment_analyzer
        articles_with_sentiment = sentiment_analyzer.analyze_articles_sentiment(articles)
        
        # Add sentiment columns
        sentiment_data = []
        for article in articles_with_sentiment:
            sentiment_data.append({
                'sentiment': article.get('sentiment', 'NEUTRAL'),
                'sentiment_score': article.get('sentiment_score', 0.0),
                'tone': article.get('tone', 0.0)
            })
        
        sentiment_df = pd.DataFrame(sentiment_data)
        df = pd.concat([df, sentiment_df], axis=1)
        
        logger.info(f"Processed {len(df)} articles with sentiment analysis")
        return df
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        if not url or not isinstance(url, str):
            return 'unknown'
        try:
            domain = urlparse(url).netloc
            # Remove www. prefix if present
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain if domain else 'unknown'
        except:
            return 'unknown'
    
    def extract_keywords(self, texts: List[str], top_n: int = 20, custom_stopwords: List[str] = None) -> List[Tuple[str, int]]:
        """Extract top keywords from texts with custom stopwords"""
        if not texts:
            return []
        
        # Combine all texts
        all_text = ' '.join([str(text).lower() for text in texts if text and isinstance(text, str)])
        
        # Default stopwords
        stop_words = {
            'the', 'and', 'to', 'of', 'a', 'in', 'that', 'is', 'it', 'for',
            'on', 'with', 'as', 'by', 'at', 'from', 'this', 'be', 'are', 'was',
            'for', 'with', 'on', 'at', 'from', 'by', 'about', 'against', 'between',
            'has', 'have', 'had', 'but', 'not', 'or', 'so', 'if', 'then', 'when',
            'which', 'what', 'where', 'who', 'why', 'how', 'all', 'any', 'both',
            'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor',
            'only', 'own', 'same', 'than', 'too', 'very', 'can', 'will', 'just'
        }
        
        # Add custom stopwords if provided
        if custom_stopwords:
            stop_words.update(set(custom_stopwords))
        
        words = re.findall(r'\b[a-z]{3,}\b', all_text.lower())
        filtered_words = [word for word in words if word not in stop_words]
        
        # Count word frequencies
        word_counts = Counter(filtered_words)
        return word_counts.most_common(top_n)
    
    def create_time_series_data(self, df: pd.DataFrame, freq: str = 'D') -> pd.DataFrame:
        """Create time series data for analysis"""
        if df.empty or 'date' not in df.columns:
            return pd.DataFrame()
        
        df = df.copy()
        df.set_index('date', inplace=True)
        
        # Resample by frequency
        time_series = df.resample(freq).agg({
            'title': 'count',
            'tone': 'mean',
            'sentiment_score': 'mean'
        }).rename(columns={'title': 'article_count'})
        
        time_series['article_count'] = time_series['article_count'].fillna(0)
        time_series['tone'] = time_series['tone'].fillna(0)
        time_series['sentiment_score'] = time_series['sentiment_score'].fillna(0)
        
        return time_series.reset_index()