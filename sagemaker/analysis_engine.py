import nltk
import pandas as pd
import numpy as np
from typing import List, Dict, Any
from collections import Counter, defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer
from datetime import datetime
import logging
import re
import warnings
import sys
import os

# --- PATCH: Ensure sagemaker directory is in sys.path for local imports ---
sagemaker_dir = os.path.abspath(os.path.dirname(__file__))
if sagemaker_dir not in sys.path:
    sys.path.insert(0, sagemaker_dir)
# -------------------------------------------------------------------------

from sentiment_analyzer import SentimentAnalyzer

warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class AnalysisEngine:
    def __init__(self):
        # Download required NLTK data
        try:
            nltk.download('punkt', quiet=True)
            nltk.download('stopwords', quiet=True)
            nltk.download('wordnet', quiet=True)
            nltk.download('omw-1.4', quiet=True)
        except Exception as e:
            logger.warning(f"Could not download NLTK data: {str(e)}")

        self.stop_words = set(nltk.corpus.stopwords.words('english'))
        self.stop_words.update(['said', 'says', 'will', 'new', 'also', 'one', 'two', 'year', 'says'])
        self.lemmatizer = nltk.stem.WordNetLemmatizer()
        self.sentiment_analyzer = SentimentAnalyzer()
    
    def clean_text(self, text: str) -> str:
        """Clean text for analysis"""
        if not isinstance(text, str):
            return ""
        
        # Convert to lowercase and remove special characters
        text = text.lower()
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def preprocess_text(self, text: str) -> str:
        """Preprocess text for topic modeling"""
        if not text:
            return ""
        
        # Clean text first
        text = self.clean_text(text)
        
        # Tokenize and process
        tokens = nltk.word_tokenize(text)
        tokens = [self.lemmatizer.lemmatize(token) 
                 for token in tokens 
                 if token not in self.stop_words 
                 and len(token) > 2 
                 and not token.isnumeric()]
        
        return ' '.join(tokens)

    def _parse_date(self, date_str: str) -> str:
        """Parse GDELT date format to YYYY-MM format"""
        try:
            if not date_str:
                return None
            # Handle both formats: YYYYMMDDThhmmss and YYYYMMDDHHMMSS
            date_str = date_str.replace('T', '').replace('Z', '')
            date = datetime.strptime(date_str, '%Y%m%d%H%M%S')
            return date.strftime('%Y-%m')
        except Exception as e:
            logger.error(f"Error parsing date {date_str}: {str(e)}")
            return None
    
    def analyze_coverage(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze coverage patterns"""
        try:
            # Monthly coverage
            monthly_counts = df.groupby('year_month').size().reset_index()
            monthly_coverage = [
                {
                    'date': row['year_month'],
                    'count': int(row[0])
                }
                for _, row in monthly_counts.iterrows()
            ]
            
            return {
                'monthly_coverage': monthly_coverage,
                'total_articles': len(df)
            }
            
        except Exception as e:
            logger.error(f"Error in coverage analysis: {str(e)}")
            return {}

    def analyze_sentiment(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze sentiment patterns using NLP"""
        try:
            # Get titles for sentiment analysis
            titles = df['title'].fillna('').astype(str).tolist()
            
            # Analyze sentiments
            sentiment_results = self.sentiment_analyzer.analyze_batch(titles)
            
            # Convert results to DataFrame
            sentiment_df = pd.DataFrame(sentiment_results)
            df['sentiment_score'] = sentiment_df['score']
            df['sentiment'] = sentiment_df['sentiment']
            
            # Calculate distributions and trends
            sentiment_dist = df['sentiment'].value_counts().to_dict()
            
            # Calculate monthly averages
            df['year_month'] = df['date'].dt.strftime('%Y-%m')
            monthly_sentiment = df.groupby('year_month').agg({
                'sentiment_score': ['mean', 'std', 'count']
            }).fillna(0)
            
            return {
                'sentiment_distribution': sentiment_dist,
                'monthly_sentiment': [
                    {
                        'date': idx,
                        'tone_mean': float(row['sentiment_score']['mean']),
                        'tone_std': float(row['sentiment_score']['std']),
                        'count': int(row['sentiment_score']['count'])
                    }
                    for idx, row in monthly_sentiment.iterrows()
                ]
            }
            
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {str(e)}")
            return {}

    def analyze_geographic(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze geographic distribution"""
        try:
            return {
                'country_coverage': df['source_country'].value_counts().to_dict(),
                'region_coverage': df['region'].value_counts().to_dict()
            }
        except Exception as e:
            logger.error(f"Error in geographic analysis: {str(e)}")
            return {}

    def analyze_temporal(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze temporal patterns"""
        try:
            df['year_month'] = df['date'].dt.strftime('%Y-%m')
            monthly_counts = df.groupby('year_month').size()
            
            return {
                'monthly_coverage': [
                    {'date': date, 'count': int(count)}
                    for date, count in monthly_counts.items()
                ]
            }
        except Exception as e:
            logger.error(f"Error in temporal analysis: {str(e)}")
            return {}

    def analyze_sources(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze news sources"""
        try:
            return {
                'domain_distribution': df['source_domain'].value_counts().head(20).to_dict(),
                'language_distribution': df['language'].value_counts().to_dict()
            }
        except Exception as e:
            logger.error(f"Error in source analysis: {str(e)}")
            return {}

    def analyze_topics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze topics over time"""
        try:
            df['year_month'] = df['date'].dt.strftime('%Y-%m')
            monthly_counts = df.groupby('year_month').size().reset_index()
            
            # Generate sample topic data (replace with actual topic modeling)
            return {
                'topic_evolution': [
                    {
                        'date': row['year_month'],
                        'topic_1': np.random.randint(1, 100),
                        'topic_2': np.random.randint(1, 100),
                        'topic_3': np.random.randint(1, 100)
                    }
                    for _, row in monthly_counts.iterrows()
                ]
            }
        except Exception as e:
            logger.error(f"Error in topic analysis: {str(e)}")
            return {}

    def analyze_all(self, articles: List[Dict]) -> Dict[str, Any]:
        """Run all analyses on the articles"""
        try:
            # Convert to DataFrame and handle dates
            df = pd.DataFrame(articles)
            
            # Add default tone if missing
            if 'tone' not in df.columns:
                df['tone'] = 0.0
            else:
                df['tone'] = pd.to_numeric(df['tone'], errors='coerce').fillna(0.0)
            
            # Convert date and handle missing values
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['year_month'] = df['date'].dt.strftime('%Y-%m')
            
            # Initialize results dictionary
            results = {
                'geographic_analysis': self.analyze_geographic(df),
                'source_analysis': self.analyze_sources(df),
                'sentiment_analysis': self.analyze_sentiment(df),
                'temporal_analysis': self.analyze_temporal(df),
                'content_analysis': self.analyze_content(df),
                'coverage_analysis': self.analyze_coverage(df)
            }
            
            # Filter out None values
            return {k: v for k, v in results.items() if v}
            
        except Exception as e:
            logger.error(f"Error in analysis: {str(e)}")
            return {}

    def analyze_content(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze content patterns"""
        try:
            # Process titles
            titles = df['title'].fillna('').astype(str).tolist()
            processed_titles = [self.clean_text(title) for title in titles if title]
            
            # Get word frequencies
            word_freq = Counter()
            for title in processed_titles:
                words = [w for w in title.split() if len(w) > 2 and w not in self.stop_words]
                word_freq.update(words)
            
            return {
                'word_cloud_data': dict(word_freq.most_common(100)),
                'top_keywords': [w for w, _ in word_freq.most_common(20)],
                'document_count': len(processed_titles)
            }
            
        except Exception as e:
            logger.error(f"Error in content analysis: {str(e)}")
            return {}

    def _get_topic_evolution(self, articles: List[Dict], topics: List[Dict]) -> List[Dict]:
        """Calculate topic evolution over time"""
        try:
            df = pd.DataFrame(articles)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # Group by month
            monthly = df.groupby(df['date'].dt.to_period('M')).size()
            dates = [d.strftime('%Y-%m') for d in monthly.index]
            
            return [{
                'date': date,
                'topic_1': np.random.randint(1, 100),  # Placeholder
                'topic_2': np.random.randint(1, 100),
                'topic_3': np.random.randint(1, 100)
            } for date in dates]
            
        except Exception as e:
            logger.error(f"Error in topic evolution: {str(e)}")
            return []
            
            # Group by month
            monthly = df.groupby(df['date'].dt.to_period('M')).size()
            dates = [d.strftime('%Y-%m') for d in monthly.index]
            
            return [{
                'date': date,
                'topic_1': np.random.randint(1, 100),  # Placeholder
                'topic_2': np.random.randint(1, 100),
                'topic_3': np.random.randint(1, 100)
            } for date in dates]
            
        except Exception as e:
            logger.error(f"Error in topic evolution: {str(e)}")
            return []
