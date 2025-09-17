import boto3
import pandas as pd
from analysis_engine import AnalysisEngine
from visualization_engine import VisualizationEngine, EnhancedGDELTVisualizationEngine
import json
import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict, Tuple
from tqdm import tqdm
from parallel_loader import ParallelDataLoader
import multiprocessing

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
env_path = Path(__file__).parents[1] / '.env'
load_dotenv(dotenv_path=env_path)

def get_collection_data(collection_id):
    """Get collection status and metadata"""
    try:
        s3 = boto3.client('s3',
            region_name=os.getenv('AWS_DEFAULT_REGION'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        bucket = os.getenv('AWS_S3_BUCKET')
        status_key = f'status/{collection_id}.json'
        
        response = s3.get_object(Bucket=bucket, Key=status_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Error loading collection data: {str(e)}")
        return None

def list_collections():
    """List all available collections in the bucket"""
    try:
        s3 = boto3.client('s3',
            region_name=os.getenv('AWS_DEFAULT_REGION'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        bucket = os.getenv('AWS_S3_BUCKET')
        
        # List collections directory
        paginator = s3.get_paginator('list_objects_v2')
        collections = set()
        
        for page in paginator.paginate(Bucket=bucket, Prefix='collections/'):
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                # Extract collection ID from path
                parts = obj['Key'].split('/')
                if len(parts) > 1:
                    collections.add(parts[1])
        
        return list(collections)
    except Exception as e:
        logger.error(f"Error listing collections: {str(e)}")
        return []

def load_test_data() -> Tuple[pd.DataFrame, List[str]]:
    """Load articles using parallel processing and return DataFrame"""
    try:
        loader = ParallelDataLoader(
            bucket=os.getenv('AWS_S3_BUCKET'),
            region=os.getenv('AWS_DEFAULT_REGION'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        
        # Use 75% of available CPU cores
        max_workers = max(1, int(multiprocessing.cpu_count() * 0.75))
        articles, queries = loader.load_data(max_workers=max_workers)
        
        if not articles:
            logger.warning("No articles found in the collections")
            return pd.DataFrame(), []
            
        # Clean and convert date formats
        cleaned_articles = []
        for article in articles:
            try:
                # Create a copy to avoid modifying original
                cleaned_article = article.copy()
                
                if article.get('date'):
                    # Convert string date to datetime object
                    date_obj = pd.to_datetime(article['date'])
                    cleaned_article['date'] = date_obj
                    cleaned_article['year'] = int(date_obj.year)
                    cleaned_article['month'] = int(date_obj.month)
                else:
                    # Use year/month from article if date is missing
                    cleaned_article['year'] = int(article.get('year', 0))
                    cleaned_article['month'] = int(article.get('month', 0))
                
                if cleaned_article['year'] > 0:
                    cleaned_articles.append(cleaned_article)
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping article with invalid date: {e}")
                continue
        
        # Create DataFrame and sort
        if cleaned_articles:
            df = pd.DataFrame(cleaned_articles)
            if 'date' in df.columns:
                df = df.sort_values('date')
            return df, queries
        
        return pd.DataFrame(), queries
        
    except Exception as e:
        logger.error(f"Error loading test data: {str(e)}")
        return pd.DataFrame(), []

def main():
    try:
        # Initialize engines
        analysis_engine = AnalysisEngine()
        viz_engine = VisualizationEngine()
        enhanced_viz_engine = EnhancedGDELTVisualizationEngine()
        
        # Load all articles
        df, queries = load_test_data()
        
        if df.empty:
            logger.error("No data available for analysis")
            return
        
        logger.info(f"Successfully loaded {len(df)} articles")
        logger.info(f"Articles by region:")
        logger.info(df.groupby('region').size())
        
        # Convert DataFrame back to list of dicts for analysis engine
        articles = df.to_dict('records')

        # Run analysis
        analysis_results = analysis_engine.analyze_all(articles)

        # --- ADD SENTIMENT SCORE TO DATAFRAME USING VISUALIZATION ENGINE ---
        if 'sentiment_score' not in df.columns and 'title' in df.columns:
            logger.info("Calculating sentiment scores for articles...")
            # Use the sentiment analyzer from the visualization engine
            df = viz_engine.sentiment_analyzer.add_sentiment_scores(df)
            logger.info(f"Sentiment analysis completed. Added 'sentiment_score' column")
        
        # Use combined queries for visualization title
        query = " & ".join(queries) if queries else "GDELT Analysis"

        # Create basic visualizations from analysis results
        basic_visualizations = viz_engine.create_all_visualizations(
            analysis_results,
            query=query,
            df=df
        )

        # Create enhanced visualizations from DataFrame
        enhanced_visualizations = enhanced_viz_engine.create_all_enhanced_visualizations(
            df,
            query=query
        )

        # Combine all visualizations
        all_visualizations = {**basic_visualizations, **enhanced_visualizations}

        if not all_visualizations:
            logger.error("No visualizations were generated")
            return

        logger.info(f"Generated {len(all_visualizations)} visualizations:")
        for name in all_visualizations.keys():
            logger.info(f"  - {name}")

        # Show visualizations
        for name, fig in all_visualizations.items():
            logger.info(f"Displaying visualization: {name}")
            fig.show()

        # --- DEBUG: CHECK SENTIMENT DATA QUALITY ---
        if 'sentiment_score' in df.columns:
            logger.info(f"Sentiment score statistics:")
            logger.info(f"  Min: {df['sentiment_score'].min():.3f}")
            logger.info(f"  Max: {df['sentiment_score'].max():.3f}")
            logger.info(f"  Mean: {df['sentiment_score'].mean():.3f}")
            logger.info(f"  Std: {df['sentiment_score'].std():.3f}")
            
            # Check if we have enough data for time series
            required_cols = ['sentiment_score', 'region', 'year', 'month']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                logger.warning(f"Cannot create sentiment time series - missing columns: {missing_cols}")
            else:
                # Check for null values
                null_counts = df[required_cols].isnull().sum()
                logger.info(f"Null counts in required columns: {null_counts.to_dict()}")
                
                # Check if we have data across multiple time periods
                time_periods = df.groupby(['year', 'month']).size()
                logger.info(f"Number of unique time periods: {len(time_periods)}")
                
                if len(time_periods) < 2:
                    logger.warning("Not enough time periods for meaningful time series analysis")
                
                # Check if we have data across multiple regions
                regions_with_data = df['region'].nunique()
                logger.info(f"Number of regions with data: {regions_with_data}")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()