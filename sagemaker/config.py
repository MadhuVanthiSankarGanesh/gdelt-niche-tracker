# Configuration settings
AWS_CONFIG = {
    'bucket_name': 'gdelt-niche-data-bucket',
    'region': 'eu-north-1',
    'collections_prefix': 'collections/',
    'status_prefix': 'status/',
    'analytics_prefix': 'analytics/'
}

ANALYSIS_CONFIG = {
    'min_articles_per_topic': 5,
    'time_window_days': 30,
    'sentiment_threshold': 0.2,
    'top_n_keywords': 20,
    'top_n_countries': 15,
    'top_n_domains': 10
}

SENTIMENT_CONFIG = {
    'model_name': 'cardiffnlp/twitter-roberta-base-sentiment-latest',
    'batch_size': 32,
    'max_length': 128
}

VISUALIZATION_CONFIG = {
    'color_palette': 'Set3',
    'default_width': 1000,
    'default_height': 600
}