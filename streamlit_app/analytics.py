import streamlit as st
import pandas as pd
import logging
from pathlib import Path
from dotenv import load_dotenv
import os
import sys

# --- PATCH: Add project root to sys.path for local imports ---
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# ------------------------------------------------------------

from sagemaker.analysis_engine import AnalysisEngine
from sagemaker.visualization_engine import VisualizationEngine, EnhancedGDELTVisualizationEngine
from sagemaker.parallel_loader import ParallelDataLoader
import multiprocessing

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set Streamlit light theme and custom styles
st.set_page_config(page_title="GDELT Analytics", layout="wide")
st.markdown(
    """
    <style>
    body, .stApp {
        background-color: #F8F9FA !important;
        color: #222 !important;
    }
    .stApp {
        font-family: 'Segoe UI', 'Roboto', 'Arial', sans-serif;
    }
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .stHeader, .stSidebar, .stMarkdown, .stDataFrame, .stPlotlyChart {
        background-color: #FFFFFF !important;
        color: #222 !important;
        border-radius: 12px;
    }
    .stPlotlyChart {
        background-color: #FFFFFF !important;
        border-radius: 12px;
        padding: 1rem;
    }
    .stMetric {
        background-color: #F3F6F9 !important;
        border-radius: 8px;
        color: #222 !important;
    }
    .stButton>button {
        background-color: #E9ECEF !important;
        color: #222 !important;
        border-radius: 8px;
        border: none;
    }
    .stButton>button:hover {
        background-color: #DEE2E6 !important;
        color: #222 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Load environment variables from .env file
env_path = Path(__file__).parents[1] / '.env'
load_dotenv(dotenv_path=env_path)

def load_articles() -> pd.DataFrame:
    """Load articles using parallel processing and return DataFrame"""
    try:
        loader = ParallelDataLoader(
            bucket=os.getenv('AWS_S3_BUCKET'),
            region=os.getenv('AWS_DEFAULT_REGION'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        max_workers = max(1, int(multiprocessing.cpu_count() * 0.75))
        articles, queries = loader.load_data(max_workers=max_workers)
        if not articles:
            st.warning("No articles found in the collections")
            return pd.DataFrame(), []
        cleaned_articles = []
        for article in articles:
            try:
                cleaned_article = article.copy()
                if article.get('date'):
                    date_obj = pd.to_datetime(article['date'])
                    cleaned_article['date'] = date_obj
                    cleaned_article['year'] = int(date_obj.year)
                    cleaned_article['month'] = int(date_obj.month)
                else:
                    cleaned_article['year'] = int(article.get('year', 0))
                    cleaned_article['month'] = int(article.get('month', 0))
                if cleaned_article['year'] > 0:
                    cleaned_articles.append(cleaned_article)
            except (ValueError, TypeError):
                continue
        if cleaned_articles:
            df = pd.DataFrame(cleaned_articles)
            if 'date' in df.columns:
                df = df.sort_values('date')
            return df, queries
        return pd.DataFrame(), queries
    except Exception as e:
        st.error(f"Error loading articles: {str(e)}")
        return pd.DataFrame(), []

def main():
    st.title("📊 GDELT Analytics Dashboard")
    st.markdown(
        "<h4 style='color:#AEB6BF;'>Explore analytics and visualizations for your GDELT news collections.</h4>",
        unsafe_allow_html=True
    )

    # Load data
    df, queries = load_articles()
    if df.empty:
        st.warning("No data available for analytics.")
        return

    st.success(f"Loaded {len(df)} articles.")
    st.markdown("### Articles by Region")
    st.dataframe(
        df['region'].value_counts().rename('count').reset_index().rename(columns={'index': 'region'}),
        use_container_width=True,
        hide_index=True
    )

    # Analysis and Visualization
    analysis_engine = AnalysisEngine()
    viz_engine = VisualizationEngine()
    enhanced_viz_engine = EnhancedGDELTVisualizationEngine()

    articles = df.to_dict('records')
    analysis_results = analysis_engine.analyze_all(articles)

    # Add sentiment scores if missing
    if 'sentiment_score' not in df.columns and 'title' in df.columns:
        df = viz_engine.sentiment_analyzer.add_sentiment_scores(df)

    query = " & ".join(queries) if queries else "GDELT Analysis"

    # Visualizations
    st.markdown("---")
    st.header("Visualizations")
    basic_visualizations = viz_engine.create_all_visualizations(
        analysis_results,
        query=query,
        df=df
    )
    enhanced_visualizations = enhanced_viz_engine.create_all_enhanced_visualizations(
        df,
        query=query
    )
    all_visualizations = {**basic_visualizations, **enhanced_visualizations}

    # Organize visualizations by type
    viz_sections = {
        "Geographic": [],
        "Source": [],
        "Sentiment": [],
        "Content": [],
        "Other": []
    }
    for name, fig in all_visualizations.items():
        lname = name.lower()
        if "geo" in lname or "country" in lname or "world" in lname:
            viz_sections["Geographic"].append((name, fig))
        elif "source" in lname or "domain" in lname:
            viz_sections["Source"].append((name, fig))
        elif "sentiment" in lname:
            viz_sections["Sentiment"].append((name, fig))
        elif "word_cloud" in lname or "topic" in lname:
            viz_sections["Content"].append((name, fig))
        else:
            viz_sections["Other"].append((name, fig))

    # --- Show world map (geographic coverage) at the very top, full width ---
    world_map = None
    for name, fig in viz_sections["Geographic"]:
        if "world_coverage_map" in name.lower():
            st.markdown(f"## 🌎 World Coverage Map")
            st.plotly_chart(fig, use_container_width=True)
            world_map = name
            break
    # Remove the world map from the Geographic section so it's not shown again
    if world_map:
        viz_sections["Geographic"] = [item for item in viz_sections["Geographic"] if item[0] != world_map]

    # --- Show the rest of the visualizations in sections with columns ---
    for section, items in viz_sections.items():
        if items:
            st.markdown(f"## {section} Analytics")
            cols = st.columns(min(2, len(items)))
            for idx, (name, fig) in enumerate(items):
                with cols[idx % len(cols)]:
                    st.markdown(f"**{name.replace('_', ' ').title()}**")
                    st.plotly_chart(fig, use_container_width=True)

    # Show some stats
    if 'sentiment_score' in df.columns:
        st.markdown("---")
        st.header("Sentiment Score Statistics")
        stats_cols = st.columns(4)
        stats = [
            ("Min", df['sentiment_score'].min()),
            ("Max", df['sentiment_score'].max()),
            ("Mean", df['sentiment_score'].mean()),
            ("Std", df['sentiment_score'].std())
        ]
        for i, (label, value) in enumerate(stats):
            stats_cols[i].metric(label, f"{value:.3f}")

if __name__ == "__main__":
    main()
