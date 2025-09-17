import boto3
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
from tqdm import tqdm
from botocore.exceptions import ClientError
import re

logger = logging.getLogger(__name__)

class S3DataLoader:
    def __init__(self, bucket_name: str, region: str = 'eu-north-1'):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region)
    
    def load_collection_data(self, collection_id: str) -> List[Dict[str, Any]]:
        """Load all data from a specific collection"""
        logger.info(f"Loading data for collection: {collection_id}")
        all_articles = []
        
        try:
            # List all files in the collection
            prefix = f'collections/{collection_id}/'
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            # Get all files in the collection
            files = []
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.json'):
                            files.append(obj['Key'])
            
            logger.info(f"Found {len(files)} files in collection {collection_id}")
            
            # Load each file
            for file_key in tqdm(files, desc="Loading collection files"):
                try:
                    response = self.s3_client.get_object(
                        Bucket=self.bucket_name, 
                        Key=file_key
                    )
                    data = json.loads(response['Body'].read().decode('utf-8'))
                    
                    # Extract articles and add metadata
                    if 'articles' in data and data['articles']:
                        for article in data['articles']:
                            # Add collection metadata to each article
                            article['collection_id'] = collection_id
                            article['query'] = data.get('query', 'unknown')
                            article['region'] = data.get('region', 'unknown')
                            article['year'] = data.get('year')
                            article['month'] = data.get('month')
                            article['source_file'] = file_key
                            all_articles.append(article)
                            
                except Exception as e:
                    logger.error(f"Error processing {file_key}: {str(e)}")
                    continue
        
        except Exception as e:
            logger.error(f"Error loading collection {collection_id}: {str(e)}")
        
        logger.info(f"Loaded {len(all_articles)} articles from collection {collection_id}")
        return all_articles
    
    def get_collection_status(self, query: str, collection_id: str) -> Dict[str, Any]:
        """Get the status of a collection"""
        try:
            # Try to find status file with proper naming
            status_key = f'status/{self._slugify(query)}_{collection_id}.json'
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=status_key
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"Status file not found for collection {collection_id}")
                return {'status': 'not_found'}
            raise
    
    def list_collections(self) -> List[Dict[str, Any]]:
        """List all collections in the bucket with their metadata"""
        collections = []
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix='collections/',
                Delimiter='/'
            )
            
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    collection_id = prefix['Prefix'].split('/')[1]
                    if collection_id:
                        # Try to get basic info about the collection
                        collections.append({
                            'collection_id': collection_id,
                            'prefix': prefix['Prefix']
                        })
            
        except Exception as e:
            logger.error(f"Error listing collections: {str(e)}")
        
        return collections
    
    def get_all_collection_ids(self) -> List[str]:
        """Get all collection IDs from the collections folder"""
        logger.info("Getting all collection IDs from S3 bucket")
        collection_ids = []
        
        try:
            # Use paginator to handle large number of collections
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix='collections/',
                Delimiter='/'
            ):
                if 'CommonPrefixes' in page:
                    for prefix in page['CommonPrefixes']:
                        # Extract collection ID from the prefix
                        # Prefix format: "collections/{collection_id}/"
                        collection_id = prefix['Prefix'].split('/')[1]
                        if collection_id:  # Ensure it's not empty
                            collection_ids.append(collection_id)
            
            logger.info(f"Found {len(collection_ids)} collection IDs")
            
        except Exception as e:
            logger.error(f"Error getting collection IDs: {str(e)}")
        
        return collection_ids
    
    def get_collections_with_info(self) -> List[Dict[str, Any]]:
        """Get all collections with their basic information and status"""
        logger.info("Getting all collections with detailed information")
        collections_info = []
        collection_ids = self.get_all_collection_ids()
        
        for collection_id in tqdm(collection_ids, desc="Fetching collection info"):
            try:
                # Try to find the status file by checking common query patterns
                status_info = None
                possible_queries = ['climate change', 'politics', 'technology', 'health', 'business', 'science']
                
                for query in possible_queries:
                    status = self.get_collection_status(query, collection_id)
                    if status.get('status') != 'not_found':
                        status_info = status
                        break
                
                # If no status found with common queries, try to find any status file
                if not status_info:
                    # List status files for this collection ID pattern
                    status_prefix = f'status/'
                    status_response = self.s3_client.list_objects_v2(
                        Bucket=self.bucket_name,
                        Prefix=status_prefix,
                    )
                    
                    if 'Contents' in status_response:
                        for obj in status_response['Contents']:
                            if collection_id in obj['Key']:
                                # Found a status file for this collection
                                status_obj = self.s3_client.get_object(
                                    Bucket=self.bucket_name,
                                    Key=obj['Key']
                                )
                                status_info = json.loads(status_obj['Body'].read().decode('utf-8'))
                                break
                
                collection_info = {
                    'collection_id': collection_id,
                    'status_info': status_info or {'status': 'unknown'},
                    'has_data': self._check_collection_has_data(collection_id)
                }
                
                collections_info.append(collection_info)
                
            except Exception as e:
                logger.error(f"Error getting info for collection {collection_id}: {str(e)}")
                collections_info.append({
                    'collection_id': collection_id,
                    'error': str(e),
                    'has_data': False
                })
        
        return collections_info
    
    def _check_collection_has_data(self, collection_id: str) -> bool:
        """Check if a collection has any data files"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=f'collections/{collection_id}/',
                MaxKeys=1  # Just check if any objects exist
            )
            return 'Contents' in response and len(response['Contents']) > 0
        except Exception:
            return False
    
    def save_analysis_results(self, results: Dict[str, Any], query: str, collection_id: str):
        """Save analysis results to S3"""
        try:
            analysis_key = f"analytics/{self._slugify(query)}_{collection_id}/full_analysis.json"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=analysis_key,
                Body=json.dumps(results, indent=2, default=str),
                ContentType='application/json'
            )
            logger.info(f"Analysis results saved to: {analysis_key}")
        except Exception as e:
            logger.error(f"Error saving analysis results: {str(e)}")
    
    def _slugify(self, text: str) -> str:
        """Convert text to URL-friendly slug"""
        text = text.lower().strip()
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[\s_-]+', '_', text)
        text = re.sub(r'^-+|-+$', '', text)
        return text