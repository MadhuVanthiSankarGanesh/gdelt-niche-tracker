import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import pandas as pd
from typing import Dict, Any, Optional, List
import logging
import seaborn as sns
from wordcloud import WordCloud
from textblob import TextBlob
import numpy as np

logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    """Class to handle sentiment analysis of articles"""
    
    @staticmethod
    def calculate_sentiment(text):
        """Calculate sentiment polarity using TextBlob"""
        if not text or pd.isna(text):
            return 0.0
        try:
            analysis = TextBlob(str(text))
            return analysis.sentiment.polarity
        except:
            return 0.0
    
    def add_sentiment_scores(self, df):
        """Add sentiment scores to DataFrame based on article titles"""
        if 'title' not in df.columns:
            logger.warning("No 'title' column found for sentiment analysis")
            return df
        
        logger.info("Calculating sentiment scores for articles...")
        df['sentiment_score'] = df['title'].apply(self.calculate_sentiment)
        logger.info(f"Sentiment analysis completed. Score range: {df['sentiment_score'].min():.3f} to {df['sentiment_score'].max():.3f}")
        return df

class VisualizationEngine:
    def __init__(self):
        plt.style.use('default')
        sns.set_palette("husl")
        self.color_palette = px.colors.qualitative.Set3
        plt.close('all')
        self.sentiment_analyzer = SentimentAnalyzer()
    
    def create_coverage_timeline(self, analysis_data: Dict[str, Any], query: str = None) -> Optional[go.Figure]:
        """Create coverage timeline chart for any topic"""
        coverage_data = analysis_data.get('coverage_analysis', {})
        monthly_data = coverage_data.get('monthly_coverage', [])
        
        if not monthly_data:
            return None
        
        df = pd.DataFrame(monthly_data)
        title = f'Monthly Coverage: {query}' if query else 'Monthly Article Coverage'
        
        fig = px.line(df, x='date', y='count', 
                     title=title,
                     labels={'count': 'Number of Articles', 'date': 'Date'})
        fig.update_traces(line=dict(width=3, color=self.color_palette[0]))
        fig.update_layout(
            hovermode='x unified', 
            showlegend=False,
            width=1000,
            height=500
        )
        return fig
    
    def create_sentiment_timeline(self, analysis_data: Dict[str, Any], query: str = None) -> Optional[go.Figure]:
        """Create sentiment timeline chart for any topic"""
        sentiment_data = analysis_data.get('sentiment_analysis', {})
        monthly_data = sentiment_data.get('monthly_sentiment', [])
        
        if not monthly_data:
            return None
        
        df = pd.DataFrame(monthly_data)
        
        # Ensure column names are correctly formatted
        if 'd_a_t_e' in df.columns:
            df = df.rename(columns={
                'd_a_t_e': 'date',
                't_o_n_e___m_e_a_n': 'tone_mean',
                't_o_n_e___s_t_d': 'tone_std'
            })
        
        title = f'Monthly Sentiment Trends: {query}' if query else 'Monthly Sentiment Trends'
        
        fig = px.line(df, x='date', y='tone_mean', 
                     error_y='tone_std',
                     title=title,
                     labels={'tone_mean': 'Average Tone', 'date': 'Date'})
        fig.update_traces(line=dict(width=3, color=self.color_palette[1]))
        fig.add_hline(y=0, line_dash="dash", line_color="red")
        fig.update_layout(
            width=1000,
            height=500
        )
        return fig
    
    def create_geographic_chart(self, analysis_data: Dict[str, Any], query: str = None) -> Optional[go.Figure]:
        """Create geographic distribution chart for any topic"""
        geo_data = analysis_data.get('geographic_analysis', {})
        country_data = geo_data.get('country_coverage', {})
        
        if not country_data:
            return None
        
        df = pd.DataFrame(list(country_data.items()), columns=['Country', 'Articles'])
        title = f'Top Countries Covering: {query}' if query else 'Top Countries by Article Count'
        
        fig = px.bar(df, x='Articles', y='Country', orientation='h',
                    title=title,
                    labels={'Articles': 'Number of Articles', 'Country': 'Country'},
                    color='Articles', color_continuous_scale='Viridis')
        fig.update_layout(
            yaxis={'categoryorder':'total ascending'},
            width=1000,
            height=600
        )
        return fig
    
    def create_sentiment_distribution(self, analysis_data: Dict[str, Any], query: str = None) -> Optional[go.Figure]:
        """Create sentiment distribution chart for any topic"""
        sentiment_data = analysis_data.get('sentiment_analysis', {})
        distribution = sentiment_data.get('sentiment_distribution', {})
        
        if not distribution:
            return None
        
        title = f'Sentiment Distribution: {query}' if query else 'Sentiment Distribution'
        
        fig = px.pie(values=list(distribution.values()), 
                    names=list(distribution.keys()),
                    title=title,
                    color=list(distribution.keys()),
                    color_discrete_map={
                        'POSITIVE': 'green',
                        'NEGATIVE': 'red', 
                        'NEUTRAL': 'blue'
                    })
        fig.update_layout(width=600, height=500)
        return fig
    
    def create_word_cloud(self, analysis_data: Dict[str, Any], query: str = None) -> Optional[go.Figure]:
        """Create word cloud from keywords for any topic"""
        content_data = analysis_data.get('content_analysis', {})
        word_cloud_data = content_data.get('word_cloud_data', {})
        
        if not word_cloud_data:
            return None
        
        # Generate word cloud
        wordcloud = WordCloud(
            width=1200,
            height=600,
            background_color='white',
            colormap='viridis',
            max_words=50,
            relative_scaling=0.5,
            prefer_horizontal=0.7
        ).generate_from_frequencies(word_cloud_data)
        
        # Get the image array
        img_array = wordcloud.to_array()
        
        # Create plotly figure
        fig = px.imshow(
            img_array,
            binary_string=True,
            aspect='auto'
        )
        
        # Update layout
        title = f'Key Topics: {query}' if query else 'Key Topics'
        fig.update_layout(
            title=dict(
                text=title,
                x=0.5,
                y=0.98,
                xanchor='center',
                yanchor='top',
                font=dict(size=20)
            ),
            width=1200,
            height=600,
            xaxis=dict(
                showticklabels=False,
                showgrid=False,
                zeroline=False
            ),
            yaxis=dict(
                showticklabels=False,
                showgrid=False,
                zeroline=False
            ),
            margin=dict(l=20, r=20, t=50, b=20),
            paper_bgcolor='white',
            plot_bgcolor='white'
        )
        
        return fig
    
    def create_source_analysis_chart(self, analysis_data: Dict[str, Any], query: str = None) -> Optional[go.Figure]:
        """Create source analysis chart for any topic"""
        source_data = analysis_data.get('source_analysis', {})
        domain_data = source_data.get('domain_distribution', {})
        
        if not domain_data:
            return None
        
        df = pd.DataFrame(list(domain_data.items()), columns=['Domain', 'Articles'])
        title = f'Top Media Sources: {query}' if query else 'Top Media Sources'
        
        fig = px.bar(df, x='Articles', y='Domain', orientation='h',
                    title=title,
                    labels={'Articles': 'Number of Articles', 'Domain': 'Domain'},
                    color='Articles', color_continuous_scale='Plasma')
        fig.update_layout(
            yaxis={'categoryorder':'total ascending'},
            width=1000,
            height=600
        )
        return fig

    def create_sentiment_timeline_overall(self, df: pd.DataFrame, query: str = None) -> Optional[go.Figure]:
        """Create overall sentiment time series graph over months"""
        if 'sentiment_score' not in df.columns or 'year' not in df.columns or 'month' not in df.columns:
            return None
        
        try:
            # Create year-month for grouping
            df = df.copy()
            df['year_month'] = df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2)
            
            # Calculate average sentiment by month
            sentiment_by_month = df.groupby('year_month')['sentiment_score'].mean().reset_index()
            
            title = f"Average Sentiment Over Time: {query}" if query else "Average Sentiment Over Time"
            
            fig = px.line(sentiment_by_month, x='year_month', y='sentiment_score', markers=True,
                          title=title,
                          labels={'sentiment_score': 'Average Sentiment Score', 'year_month': 'Month'})
            
            fig.update_traces(line=dict(width=3, color=self.color_palette[0]))
            fig.add_hline(y=0, line_dash="dash", line_color="red", opacity=0.7)
            
            fig.update_layout(
                width=1000,
                height=500,
                xaxis_title="Time Period",
                yaxis_title="Average Sentiment Score",
                xaxis_tickangle=45
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Error creating overall sentiment timeline: {str(e)}")
            return None

    def create_sentiment_timeline_by_region(self, df: pd.DataFrame, query: str = None) -> Optional[go.Figure]:
        """Create sentiment time series graph over months for each region"""
        if not all(col in df.columns for col in ['sentiment_score', 'region', 'year', 'month']):
            return None
        
        try:
            df = df.copy()
            # Create year-month for grouping
            df['year_month'] = df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2)
            
            # Calculate average sentiment by month and region
            sentiment_by_region = df.groupby(['year_month', 'region'])['sentiment_score'].mean().reset_index()
            
            title = f"Sentiment Over Time by Region: {query}" if query else "Sentiment Over Time by Region"
            
            fig = px.line(
                sentiment_by_region,
                x='year_month',
                y='sentiment_score',
                color='region',
                markers=True,
                title=title,
                labels={'sentiment_score': 'Average Sentiment Score', 'year_month': 'Month', 'region': 'Region'}
            )
            
            fig.add_hline(y=0, line_dash="dash", line_color="red", opacity=0.7)
            
            fig.update_layout(
                width=1000,
                height=500,
                xaxis_title="Time Period",
                yaxis_title="Average Sentiment Score",
                legend_title="Region",
                xaxis_tickangle=45
            )
            
            return fig
            
        except Exception as e:
            logger.error(f"Error creating sentiment timeline by region: {str(e)}")
            return None

    def create_sentiment_distribution_chart(self, df: pd.DataFrame, query: str = None) -> Optional[go.Figure]:
        """Create sentiment distribution chart based on calculated scores"""
        if 'sentiment_score' not in df.columns:
            return None
        
        try:
            # Categorize sentiment scores
            df = df.copy()
            df['sentiment_category'] = pd.cut(
                df['sentiment_score'],
                bins=[-1, -0.1, 0.1, 1],
                labels=['Negative', 'Neutral', 'Positive']
            )
            
            sentiment_counts = df['sentiment_category'].value_counts()
            
            title = f"Sentiment Distribution: {query}" if query else "Sentiment Distribution"
            
            fig = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title=title,
                color=sentiment_counts.index,
                color_discrete_map={
                    'Positive': 'green',
                    'Negative': 'red', 
                    'Neutral': 'blue'
                }
            )
            
            fig.update_layout(width=600, height=500)
            return fig
            
        except Exception as e:
            logger.error(f"Error creating sentiment distribution chart: {str(e)}")
            return None

    def create_all_visualizations(self, analysis_data: Dict[str, Any], query: str = None, df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """Create all available visualizations including sentiment analysis"""
        visualizations = {}
        
        try:
            # First, add sentiment scores if we have a DataFrame with titles
            if df is not None and 'title' in df.columns and 'sentiment_score' not in df.columns:
                df = self.sentiment_analyzer.add_sentiment_scores(df)
            
            # Geographic analysis
            if analysis_data.get('geographic_analysis'):
                geo_viz = self.create_geographic_chart(analysis_data, query)
                if geo_viz: visualizations['geographic_distribution'] = geo_viz
            
            # Source analysis
            if analysis_data.get('source_analysis'):
                source_viz = self.create_source_analysis_chart(analysis_data, query)
                if source_viz: visualizations['source_analysis'] = source_viz
            
            # Sentiment analysis from analysis data
            if analysis_data.get('sentiment_analysis'):
                sent_viz = self.create_sentiment_distribution(analysis_data, query)
                if sent_viz: visualizations['sentiment_distribution'] = sent_viz
            
            # Temporal analysis
            if analysis_data.get('temporal_analysis'):
                time_viz = self.create_coverage_timeline(analysis_data, query)
                if time_viz: visualizations['coverage_timeline'] = time_viz
            
            # Sentiment visualizations from calculated scores
            if df is not None and 'sentiment_score' in df.columns:
                # Overall sentiment timeline
                overall_sentiment = self.create_sentiment_timeline_overall(df, query)
                if overall_sentiment: visualizations['sentiment_timeline_overall'] = overall_sentiment
                
                # Sentiment by region
                region_sentiment = self.create_sentiment_timeline_by_region(df, query)
                if region_sentiment: visualizations['sentiment_timeline_by_region'] = region_sentiment
                
                # Sentiment distribution
                sentiment_dist = self.create_sentiment_distribution_chart(df, query)
                if sentiment_dist: visualizations['calculated_sentiment_distribution'] = sentiment_dist
            
        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
        
        return visualizations

# Enhanced visualization engine remains the same as before
class EnhancedGDELTVisualizationEngine:
    def __init__(self):
        self.color_palette = px.colors.qualitative.Set3
        plt.style.use('default')
        self.sentiment_analyzer = SentimentAnalyzer()
    
    def create_language_distribution(self, df: pd.DataFrame, query: str = None) -> Optional[go.Figure]:
        """Create language distribution chart"""
        if 'language' not in df.columns:
            return None
            
        lang_counts = df['language'].value_counts().head(10)
        title = f'Article Language Distribution: {query}' if query else 'Article Language Distribution'
        
        fig = px.bar(x=lang_counts.values, y=lang_counts.index, orientation='h',
                    title=title,
                    labels={'x': 'Number of Articles', 'y': 'Language'},
                    color=lang_counts.values,
                    color_continuous_scale='Viridis')
        fig.update_layout(width=800, height=500)
        return fig

    def create_source_country_analysis(self, df: pd.DataFrame, query: str = None) -> Optional[go.Figure]:
        """Create source country analysis with bubble chart"""
        if 'source_country' not in df.columns:
            return None
            
        country_stats = df['source_country'].value_counts().reset_index()
        country_stats.columns = ['country', 'count']
        
        title = f'Source Country Analysis: {query}' if query else 'Source Country Analysis'
        
        fig = px.scatter(country_stats, x='count', y='country',
                        size='count', color='count',
                        title=title,
                        labels={'count': 'Number of Articles', 'country': 'Country'},
                        size_max=60)
        fig.update_layout(width=900, height=600)
        return fig

    def create_domain_network_chart(self, df: pd.DataFrame, query: str = None) -> Optional[go.Figure]:
        """Create domain network influence chart"""
        if 'source_domain' not in df.columns:
            return None
            
        domain_counts = df['source_domain'].value_counts().head(15)
        
        title = f'Top Media Domains: {query}' if query else 'Top Media Domains'
        
        fig = px.treemap(names=domain_counts.index, 
                        parents=[''] * len(domain_counts),
                        values=domain_counts.values,
                        title=title,
                        color=domain_counts.values,
                        color_continuous_scale='Rainbow')
        fig.update_layout(width=1000, height=600)
        return fig

    def create_geographic_coverage_map(self, df: pd.DataFrame, query: str = None) -> Optional[go.Figure]:
        """Create world map showing coverage by country"""
        if 'source_country' not in df.columns:
            return None
            
        country_coverage = df['source_country'].value_counts().reset_index()
        country_coverage.columns = ['country', 'articles']
        
        # Map country names to ISO codes
        country_mapping = {
            'United States': 'USA', 'India': 'IND', 'United Kingdom': 'GBR',
            'Australia': 'AUS', 'China': 'CHN', 'Japan': 'JPN',
            'South Korea': 'KOR', 'Germany': 'DEU', 'France': 'FRA',
            'Canada': 'CAN', 'Brazil': 'BRA', 'Mexico': 'MEX',
            'Russia': 'RUS', 'Italy': 'ITA', 'Spain': 'ESP',
            'Netherlands': 'NLD', 'Sweden': 'SWE', 'Norway': 'NOR',
            'Saudi Arabia': 'SAU', 'United Arab Emirates': 'ARE', 'Egypt': 'EGY',
            'South Africa': 'ZAF', 'Nigeria': 'NGA', 'Kenya': 'KEN',
            'Argentina': 'ARG', 'Chile': 'CHL', 'Colombia': 'COL',
            'Peru': 'PER', 'Venezuela': 'VEN', 'Singapore': 'SGP',
            'Malaysia': 'MYS', 'Thailand': 'THA', 'Vietnam': 'VNM',
            'Indonesia': 'IDN', 'Philippines': 'PHL', 'New Zealand': 'NZL',
            'Pakistan': 'PAK', 'Bangladesh': 'BGD', 'Sri Lanka': 'LKA'
        }
        
        country_coverage['iso_code'] = country_coverage['country'].map(country_mapping)
        country_coverage = country_coverage.dropna()
        
        title = f'Geographic Coverage: {query}' if query else 'Geographic Coverage'
        
        fig = px.choropleth(
            country_coverage, 
            locations="iso_code",
            color="articles",
            hover_name="country",
            color_continuous_scale=px.colors.sequential.Plasma,
            title=title
        )
        fig.update_layout(
            autosize=True,
            width=None,
            height=600,
            margin=dict(l=0, r=0, t=50, b=0)
        )
        return fig

    def create_all_enhanced_visualizations(self, df: pd.DataFrame, query: str = None) -> Dict[str, go.Figure]:
        """Create all enhanced visualizations"""
        visualizations = {}
        
        try:
            # First, add sentiment scores if we have titles
            if 'title' in df.columns and 'sentiment_score' not in df.columns:
                df = self.sentiment_analyzer.add_sentiment_scores(df)
            
            # Basic visualizations
            if 'language' in df.columns:
                viz = self.create_language_distribution(df, query)
                if viz: visualizations['language_distribution'] = viz
            
            if 'source_country' in df.columns:
                viz = self.create_source_country_analysis(df, query)
                if viz: visualizations['source_country_analysis'] = viz
            
            if 'source_domain' in df.columns:
                viz = self.create_domain_network_chart(df, query)
                if viz: visualizations['domain_network'] = viz
            
            if 'source_country' in df.columns:
                viz = self.create_geographic_coverage_map(df, query)
                if viz: visualizations['world_coverage_map'] = viz
            
            # Sentiment visualizations
            if 'sentiment_score' in df.columns:
                # Create a visualization engine instance for sentiment charts
                viz_engine = VisualizationEngine()
                
                # Overall sentiment timeline
                overall_sentiment = viz_engine.create_sentiment_timeline_overall(df, query)
                if overall_sentiment: visualizations['sentiment_timeline_overall'] = overall_sentiment
                
                # Sentiment by region
                region_sentiment = viz_engine.create_sentiment_timeline_by_region(df, query)
                if region_sentiment: visualizations['sentiment_timeline_by_region'] = region_sentiment
                
                # Sentiment distribution
                sentiment_dist = viz_engine.create_sentiment_distribution_chart(df, query)
                if sentiment_dist: visualizations['calculated_sentiment_distribution'] = sentiment_dist
            
        except Exception as e:
            logger.error(f"Error creating enhanced visualizations: {str(e)}")
        
        return visualizations
