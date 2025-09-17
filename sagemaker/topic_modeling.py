from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
import numpy as np
from typing import List, Tuple
import logging
from collections import Counter
import re

logger = logging.getLogger(__name__)

class TopicModeler:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            ngram_range=(1, 2)
        )
        self.topic_model = NMF(
            n_components=3,
            random_state=42,
            init='nndsvd'
        )
    
    def _clean_text(self, text: str) -> str:
        """Clean text for topic modeling"""
        text = text.lower()
        text = re.sub(r'[^\w\s]', '', text)
        return text
    
    def extract_topics(self, texts: List[str], n_topics: int = 3) -> Tuple[List[str], np.ndarray]:
        """Extract topics using TF-IDF and NMF"""
        try:
            # Clean texts
            cleaned_texts = [self._clean_text(text) for text in texts]
            
            # Create document-term matrix
            dtm = self.vectorizer.fit_transform(cleaned_texts)
            
            # Extract topics
            self.topic_model.n_components = n_topics
            topic_scores = self.topic_model.fit_transform(dtm)
            
            # Get feature names
            feature_names = np.array(self.vectorizer.get_feature_names_out())
            
            # Get topic names
            topic_names = []
            for topic_idx, topic in enumerate(self.topic_model.components_):
                top_words_idx = topic.argsort()[:-4:-1]
                top_words = feature_names[top_words_idx]
                topic_names.append(' + '.join(top_words))
            
            return topic_names, topic_scores
            
        except Exception as e:
            logger.error(f"Error in topic modeling: {str(e)}")
            return self._fallback_topic_extraction(texts, n_topics)
    
    def _fallback_topic_extraction(self, texts: List[str], n_topics: int) -> Tuple[List[str], np.ndarray]:
        """Simple fallback method for topic extraction"""
        # Count word frequencies
        word_counts = Counter()
        for text in texts:
            words = self._clean_text(text).split()
            word_counts.update(words)
        
        # Get top words as topics
        top_words = [word for word, _ in word_counts.most_common(n_topics * 3)]
        topic_names = [' + '.join(top_words[i:i+3]) for i in range(0, len(top_words), 3)][:n_topics]
        
        # Create dummy topic scores
        topic_scores = np.zeros((len(texts), len(topic_names)))
        return topic_names, topic_scores
