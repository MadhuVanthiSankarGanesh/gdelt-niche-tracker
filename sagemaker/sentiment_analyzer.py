from textblob import TextBlob
import nltk
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    def __init__(self):
        # Download required NLTK data
        try:
            nltk.download('vader_lexicon', quiet=True)
            from nltk.sentiment.vader import SentimentIntensityAnalyzer
            self.vader = SentimentIntensityAnalyzer()
        except Exception as e:
            logger.error(f"Error initializing VADER: {str(e)}")
            self.vader = None

    def analyze_sentiment(self, text: str) -> Tuple[float, str]:
        """Analyze sentiment using both VADER and TextBlob"""
        try:
            # Use VADER for sentiment scoring
            if self.vader:
                scores = self.vader.polarity_scores(text)
                compound_score = scores['compound']
            else:
                # Fallback to TextBlob if VADER fails
                blob = TextBlob(text)
                compound_score = blob.sentiment.polarity

            # Classify sentiment
            if compound_score >= 0.1:
                sentiment = 'POSITIVE'
            elif compound_score <= -0.1:
                sentiment = 'NEGATIVE'
            else:
                sentiment = 'NEUTRAL'

            return compound_score, sentiment
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {str(e)}")
            return 0.0, 'NEUTRAL'

    def analyze_batch(self, texts: List[str]) -> List[Dict[str, Any]]:
        """Analyze sentiment for a batch of texts"""
        results = []
        for text in texts:
            score, label = self.analyze_sentiment(text)
            results.append({
                'score': score,
                'sentiment': label,
                'text': text
            })
        return results