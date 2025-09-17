# gdelt-task-worker.py (Worker) - Updated with country-based filters
import boto3
import json
import logging
import os
from datetime import datetime
import requests
from botocore.exceptions import ClientError
from urllib.parse import quote
import uuid

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_collection_status(s3_client, collection_id, query):
    """Get current collection status from S3"""
    try:
        status_key = f'status/{query.lower().replace(" ", "_")}_{collection_id}.json'
        response = s3_client.get_object(
            Bucket=os.environ['S3_BUCKET_NAME'],
            Key=status_key
        )
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Error getting collection status: {str(e)}")
        return None

def update_collection_status(s3_client, collection_id, query, articles_added=0, tasks_completed=0):
    """Update collection status file in S3"""
    try:
        status_key = f'status/{query.lower().replace(" ", "_")}_{collection_id}.json'
        
        # Get current status first
        try:
            response = s3_client.get_object(
                Bucket=os.environ['S3_BUCKET_NAME'],
                Key=status_key
            )
            status_data = json.loads(response['Body'].read().decode('utf-8'))
        except ClientError:
            # Create new status if it doesn't exist
            logger.info(f"Creating new status file for collection {collection_id}")
            status_data = {
                'collection_id': collection_id,
                'query': query,
                'status': 'processing',
                'total_tasks': 0,  # This should be set by the orchestrator
                'completed_tasks': 0,
                'total_articles': 0,
                'created_at': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat()
            }
        
        # Update fields
        status_data['completed_tasks'] += tasks_completed
        status_data['total_articles'] += articles_added
        status_data['last_updated'] = datetime.now().isoformat()
        
        # Check if all tasks are completed
        if status_data.get('total_tasks', 0) > 0 and status_data['completed_tasks'] >= status_data['total_tasks']:
            status_data['status'] = 'completed'
            status_data['completed_at'] = datetime.now().isoformat()
        
        # Save updated status
        s3_client.put_object(
            Bucket=os.environ['S3_BUCKET_NAME'],
            Key=status_key,
            Body=json.dumps(status_data),
            ContentType='application/json'
        )
        
        logger.info(
            f"Updated collection status: {status_data['completed_tasks']}/{status_data.get('total_tasks', 'unknown')} "
            f"tasks, {status_data['total_articles']} articles"
        )
        return True
        
    except Exception as e:
        logger.error(f"Error updating collection status: {str(e)}")
        return False

def create_api_status(s3_client, collection_id, query, region, year, month):
    """Create status for individual API call"""
    api_call_id = str(uuid.uuid4())
    status_key = f'status/api/{api_call_id}.json'
    
    status_data = {
        'api_call_id': api_call_id,
        'collection_id': collection_id,
        'query': query,
        'region': region,
        'year': year,
        'month': month,
        'status': 'processing',
        'start_time': datetime.now().isoformat(),
        'articles_found': 0
    }
    
    try:
        s3_client.put_object(
            Bucket=os.environ['S3_BUCKET_NAME'],
            Key=status_key,
            Body=json.dumps(status_data),
            ContentType='application/json'
        )
        return status_data
    except Exception as e:
        logger.error(f"Error creating API call status: {str(e)}")
        return None

def update_api_status(s3_client, api_call_id, status, article_count=0, error_message=None):
    """Update status for individual API call"""
    try:
        status_key = f'status/api/{api_call_id}.json'
        
        # Get current status
        response = s3_client.get_object(
            Bucket=os.environ['S3_BUCKET_NAME'],
            Key=status_key
        )
        status_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Update fields
        status_data['status'] = status
        status_data['articles_found'] = article_count
        status_data['last_updated'] = datetime.now().isoformat()
        
        if status == 'completed':
            status_data['end_time'] = datetime.now().isoformat()
        elif status == 'failed' and error_message:
            status_data['error_message'] = error_message
            status_data['end_time'] = datetime.now().isoformat()
        
        s3_client.put_object(
            Bucket=os.environ['S3_BUCKET_NAME'],
            Key=status_key,
            Body=json.dumps(status_data),
            ContentType='application/json'
        )
        
        logger.info(f"Updated API call status: {api_call_id} - {status} with {article_count} articles")
        return True
        
    except Exception as e:
        logger.error(f"Error updating API call status: {str(e)}")
        return False

def save_articles(s3_client, collection_id, api_call_id, articles, query, region, year, month, max_articles, url_constructed):
    """Save articles to S3 with incremental updates"""
    try:
        articles_key = f'collections/{collection_id}/{year}/{month:02d}/{region}.json'
        
        # Prepare article data
        article_data = {
            'collection_id': collection_id,
            'api_call_id': api_call_id,
            'query': query,
            'region': region,
            'year': year,
            'month': month,
            'articles': articles,
            'article_count': len(articles),
            'processed_at': datetime.now().isoformat(),
            'status': 'completed',
            'metadata': {
                'max_articles_requested': max_articles,
                'articles_found': len(articles),
                'url_constructed': url_constructed
            }
        }
        
        # Save articles
        s3_client.put_object(
            Bucket=os.environ['S3_BUCKET_NAME'],
            Key=articles_key,
            Body=json.dumps(article_data),
            ContentType='application/json'
        )
        
        logger.info(f"Saved {len(articles)} articles for {year}-{month:02d} in {region}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving articles: {str(e)}")
        return False

def fetch_gdelt_data(year, month, region, query, max_articles):
    """Fetch data from GDELT API with robust error handling"""
    base_url = "https://api.gdeltproject.org/api/v2/doc/doc"

    # Expanded country filters for more comprehensive coverage
    expanded_country_filters = {
        'north_america': 'sourcecountry:UnitedStates OR sourcecountry:Canada OR sourcecountry:Mexico',
        'europe': 'sourcecountry:UnitedKingdom OR sourcecountry:Germany OR sourcecountry:France OR sourcecountry:Italy OR sourcecountry:Spain OR sourcecountry:Netherlands OR sourcecountry:Sweden OR sourcecountry:Norway OR sourcecountry:Denmark OR sourcecountry:Finland OR sourcecountry:Poland OR sourcecountry:Switzerland OR sourcecountry:Belgium OR sourcecountry:Austria OR sourcecountry:Ireland OR sourcecountry:Portugal OR sourcecountry:Greece OR sourcecountry:CzechRepublic OR sourcecountry:Romania OR sourcecountry:Hungary',
        'asia_pacific': 'sourcecountry:India OR sourcecountry:China OR sourcecountry:Japan OR sourcecountry:SouthKorea OR sourcecountry:Australia OR sourcecountry:NewZealand OR sourcecountry:Singapore OR sourcecountry:Malaysia OR sourcecountry:Thailand OR sourcecountry:Vietnam OR sourcecountry:Indonesia OR sourcecountry:Philippines',
        'latin_america': 'sourcecountry:Brazil OR sourcecountry:Argentina OR sourcecountry:Chile OR sourcecountry:Colombia OR sourcecountry:Mexico OR sourcecountry:Peru OR sourcecountry:Venezuela OR sourcecountry:Ecuador OR sourcecountry:Bolivia OR sourcecountry:Uruguay OR sourcecountry:Paraguay',
        'middle_east': 'sourcecountry:SaudiArabia OR sourcecountry:UnitedArabEmirates OR sourcecountry:Israel OR sourcecountry:Turkey OR sourcecountry:Egypt OR sourcecountry:Qatar OR sourcecountry:Kuwait OR sourcecountry:Bahrain OR sourcecountry:Oman OR sourcecountry:Jordan OR sourcecountry:Lebanon OR sourcecountry:Iran OR sourcecountry:Iraq OR sourcecountry:Syria OR sourcecountry:Yemen',
        'africa': 'sourcecountry:Nigeria OR sourcecountry:SouthAfrica OR sourcecountry:Egypt OR sourcecountry:Kenya OR sourcecountry:Ethiopia OR sourcecountry:Ghana OR sourcecountry:Tanzania OR sourcecountry:Uganda OR sourcecountry:Morocco OR sourcecountry:Algeria OR sourcecountry:Angola OR sourcecountry:Sudan OR sourcecountry:Cameroon OR sourcecountry:CoteDIvoire OR sourcecountry:Senegal',
        'oceania': 'sourcecountry:Australia OR sourcecountry:NewZealand OR sourcecountry:Fiji OR sourcecountry:PapuaNewGuinea OR sourcecountry:Samoa OR sourcecountry:Tonga',
        'south_asia': 'sourcecountry:India OR sourcecountry:Pakistan OR sourcecountry:Bangladesh OR sourcecountry:SriLanka OR sourcecountry:Nepal OR sourcecountry:Bhutan OR sourcecountry:Maldives OR sourcecountry:Afghanistan',
        'southeast_asia': 'sourcecountry:Singapore OR sourcecountry:Malaysia OR sourcecountry:Thailand OR sourcecountry:Vietnam OR sourcecountry:Indonesia OR sourcecountry:Philippines OR sourcecountry:Myanmar OR sourcecountry:Cambodia OR sourcecountry:Laos OR sourcecountry:Brunei OR sourcecountry:TimorLeste'
    }
    
    try:
        # Use the expanded country filters
        country_filter = expanded_country_filters.get(region, '')
        
        # Construct query with country filters
        if country_filter:
            full_query = f"{query} AND ({country_filter})"
        else:
            full_query = query
            
        encoded_query = quote(full_query)
        date_str = f"{year}{month:02d}"
        
        # Construct URL
        url = f"{base_url}?query={encoded_query}&mode=artlist&maxrecords={max_articles}&format=json&startdatetime={date_str}01000000&enddatetime={date_str}31235959&sort=datedesc"
        
        logger.info(f"Fetching from URL: {url}")
        
        # Make request with timeout
        response = requests.get(url, timeout=30)
        
        # Check for successful response
        if response.status_code != 200:
            logger.warning(f"GDELT API returned status {response.status_code} for {year}-{month:02d} in {region}")
            return [], url
        
        # Check if response is valid JSON
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            logger.warning(f"GDELT API returned invalid JSON for {year}-{month:02d} in {region}: {str(e)}")
            return [], url
        
        # Check if response contains articles
        if not data or 'articles' not in data:
            logger.info(f"No articles found for {year}-{month:02d} in {region}")
            return [], url
        
        articles = []
        for article in data.get('articles', []):
            # Parse and format date properly
            try:
                seen_date = article.get('seendate', '')
                if seen_date:
                    # Convert GDELT date format (YYYYMMDDhhmmss) to datetime
                    date_obj = datetime.strptime(seen_date, '%Y%m%d%H%M%S')
                    formatted_date = date_obj.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    formatted_date = None
            except ValueError:
                formatted_date = None

            articles.append({
                'title': article.get('title', 'No title'),
                'url': article.get('url', ''),
                'url_mobile': article.get('url_mobile', ''),
                'date': formatted_date,
                'year': int(year),  # Ensure these are integers
                'month': int(month),
                'socialimage': article.get('socialimage', ''),
                'source_country': article.get('sourcecountry', ''),
                'source_domain': article.get('domain', ''),
                'language': article.get('language', 'eng'),
                'region': region,
                'query': query
            })
        
        logger.info(f"Fetched {len(articles)} articles for {year}-{month:02d} in {region}")
        return articles, url
        
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout fetching data for {year}-{month:02d} in {region}")
        return [], url if 'url' in locals() else ""
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {year}-{month:02d} in {region}: {str(e)}")
        return [], url if 'url' in locals() else ""
    except Exception as e:
        logger.error(f"Unexpected error fetching data for {year}-{month:02d} in {region}: {str(e)}")
        return [], url if 'url' in locals() else ""

def process_single_message(record, s3_client, sqs_client):
    """Process a single SQS message"""
    try:
        # Parse message
        message_body = json.loads(record['body'])
        collection_id = message_body['collection_id']
        query = message_body['query']
        region = message_body['region']
        max_articles = message_body['max_articles']
        year = message_body['year']
        month = message_body['month']
        
        logger.info(f"Processing task: {collection_id}, {year}-{month:02d}, {region}")
        
        # Create API call status first
        api_status = create_api_status(s3_client, collection_id, query, region, year, month)
        if not api_status:
            logger.error("Failed to create API call status")
            return False
            
        # Update API status to processing
        update_api_status(s3_client, api_status['api_call_id'], 'processing')
        
        # Fetch articles
        articles, url_constructed = fetch_gdelt_data(year, month, region, query, max_articles)
        article_count = len(articles)
        
        # Save articles
        if not save_articles(s3_client, collection_id, api_status['api_call_id'], articles, query, region, year, month, max_articles, url_constructed):
            logger.error("Failed to save articles")
            update_api_status(s3_client, api_status['api_call_id'], 'failed', 0, "Failed to save articles")
            return False
        
        # Update API status to completed
        update_api_status(s3_client, api_status['api_call_id'], 'completed', article_count)
        
        # Update collection status
        if not update_collection_status(s3_client, collection_id, query, article_count, 1):
            logger.error("Failed to update collection status")
            return False
        
        # Delete message from queue
        sqs_client.delete_message(
            QueueUrl=os.environ['SQS_QUEUE_URL'],
            ReceiptHandle=record['receiptHandle']
        )
        
        logger.info(f"Successfully processed task: {collection_id}, {year}-{month:02d}, {region}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        
        # Try to update API status to failed
        try:
            if 'api_status' in locals():
                update_api_status(s3_client, api_status['api_call_id'], 'failed', 0, str(e))
        except:
            pass
            
        return False

def lambda_handler(event, context):
    """Worker Lambda handler with enhanced error handling"""
    try:
        logger.info(f"Received event with {len(event.get('Records', []))} records")
        
        # Initialize AWS clients
        s3_client = boto3.client('s3')
        sqs_client = boto3.client('sqs')
        
        if 'Records' not in event:
            logger.error("No records found in event")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No records found in event'})
            }
        
        success_count = 0
        failure_count = 0
        
        for record in event['Records']:
            if process_single_message(record, s3_client, sqs_client):
                success_count += 1
            else:
                failure_count += 1
        
        logger.info(f"Processing completed: {success_count} successes, {failure_count} failures")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing completed',
                'success_count': success_count,
                'failure_count': failure_count
            })
        }
        
    except Exception as e:
        logger.error(f"Fatal error in worker lambda: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }