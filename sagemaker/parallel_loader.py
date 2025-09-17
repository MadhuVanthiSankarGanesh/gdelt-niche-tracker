import boto3
import json
import logging
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import re

logger = logging.getLogger(__name__)

class ParallelDataLoader:
    def __init__(self, bucket: str, region: str, aws_access_key_id: str, aws_secret_access_key: str):
        self.bucket = bucket
        self.region = region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
    
    def _get_s3_client(self):
        """Create new S3 client for each process"""
        return boto3.client('s3',
            region_name=self.region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
    
    def list_collection_files(self, collection_id: str) -> List[str]:
        """List all JSON files in a collection hierarchically"""
        files = []
        try:
            paginator = self._get_s3_client().get_paginator('list_objects_v2')
            prefix = f'collections/{collection_id}/'
            
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                
                for obj in page['Contents']:
                    if obj['Key'].endswith('.json'):
                        # Extract year/month/region from path
                        match = re.search(r'/(\d{4})/(\d{2})/(\w+)\.json$', obj['Key'])
                        if match:
                            files.append(obj['Key'])
            
            logger.info(f"Found {len(files)} files for collection {collection_id}")
            return files
        except Exception as e:
            logger.error(f"Error listing files for {collection_id}: {str(e)}")
            return []

    def load_file(self, file_key: str) -> List[Dict]:
        """Load a single JSON file"""
        try:
            response = self._get_s3_client().get_object(Bucket=self.bucket, Key=file_key)
            data = json.loads(response['Body'].read())
            articles = data.get('articles', [])
            
            # Add collection metadata to each article
            collection_id = file_key.split('/')[1]
            query = data.get('query', '')
            
            for article in articles:
                article['collection_id'] = collection_id
                article['query'] = query
            
            return articles
        except Exception as e:
            logger.error(f"Error loading file {file_key}: {str(e)}")
            return []

    def load_data(self, max_workers: int = 4) -> Tuple[List[Dict], List[str]]:
        """Load all articles using parallel processing"""
        try:
            # List all collections
            paginator = self._get_s3_client().get_paginator('list_objects_v2')
            collections = set()
            
            for page in paginator.paginate(Bucket=self.bucket, Prefix='collections/'):
                if 'Contents' not in page:
                    continue
                
                for obj in page['Contents']:
                    parts = obj['Key'].split('/')
                    if len(parts) > 1:
                        collections.add(parts[1])
            
            logger.info(f"Found {len(collections)} collections")
            
            # Process each collection
            all_articles = []
            queries = set()
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for collection_id in tqdm(collections, desc="Loading collections"):
                    # Get all files for this collection
                    files = self.list_collection_files(collection_id)
                    
                    # Load files in parallel
                    future_to_file = {
                        executor.submit(self.load_file, file_key): file_key 
                        for file_key in files
                    }
                    
                    # Process results
                    for future in future_to_file:
                        articles = future.result()
                        if articles:
                            all_articles.extend(articles)
                            # Get query from first article
                            if articles[0].get('query'):
                                queries.add(articles[0]['query'])
            
            logger.info(f"Loaded {len(all_articles)} articles from {len(queries)} unique queries")
            return all_articles, list(queries)
            
        except Exception as e:
            logger.error(f"Error in parallel loading: {str(e)}")
            return [], []
