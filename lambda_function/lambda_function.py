# lambda_function.py (Orchestrator) - No changes needed
import boto3
import json
import logging
import os
from datetime import datetime, timedelta
import uuid
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def update_collection_status(s3_client, collection_id, query, total_tasks, completed_tasks=0, total_articles=0, status='initializing'):
    """Update collection status in S3"""
    try:
        status_data = {
            'collection_id': collection_id,
            'query': query,
            'status': status,
            'total_tasks': total_tasks,
            'completed_tasks': completed_tasks,
            'total_articles': total_articles,
            'start_time': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat()
        }
        
        status_key = f'status/{query.lower().replace(" ", "_")}_{collection_id}.json'
        s3_client.put_object(
            Bucket=os.environ['S3_BUCKET_NAME'],
            Key=status_key,
            Body=json.dumps(status_data),
            ContentType='application/json'
        )
        logger.info(f"Updated status for collection {collection_id}: {status}")
        return True
    except Exception as e:
        logger.error(f"Error updating status: {str(e)}")
        return False

def lambda_handler(event, context):
    """Main Lambda handler - Queues tasks for workers"""
    try:
        # Extract parameters with validation
        if 'query' not in event:
            raise ValueError("Missing required parameter: query")
            
        query = event['query']
        max_articles = event.get('max_articles_per_month', 20)
        years_back = event.get('years_back', 3)
        regions = event.get('regions', [
            'north_america', 'europe', 'asia_pacific', 'latin_america', 'middle_east',
            'africa', 'oceania', 'south_asia', 'southeast_asia'
        ])
        
        # Initialize clients
        s3_client = boto3.client('s3')
        sqs_client = boto3.client('sqs')
        
        collection_id = str(uuid.uuid4())
        
        # Generate tasks list with validation
        tasks = []
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years_back * 365)
        current_date = start_date.replace(day=1)
        
        # Count tasks and validate
        while current_date <= end_date:
            for region in regions:
                task = {
                    'collection_id': collection_id,
                    'query': query,
                    'region': region,
                    'max_articles': max_articles,
                    'year': current_date.year,
                    'month': current_date.month,
                }
                tasks.append(task)
            
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

        total_tasks = len(tasks)
        logger.info(f"Generated {total_tasks} tasks for {len(regions)} regions over {years_back} years")
        
        # Initialize status BEFORE queueing
        status = {
            'collection_id': collection_id,
            'query': query,
            'status': 'initializing',
            'total_tasks': total_tasks,
            'completed_tasks': 0,
            'total_articles': 0,
            'start_time': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'collections': []  # This will store API call IDs for tracking
        }
        
        status_key = f'status/{query.lower().replace(" ", "_")}_{collection_id}.json'
        s3_client.put_object(
            Bucket=os.environ['S3_BUCKET_NAME'],
            Key=status_key,
            Body=json.dumps(status),
            ContentType='application/json'
        )
        
        # Queue tasks with error checking
        queued_count = 0
        for task in tasks:
            try:
                sqs_client.send_message(
                    QueueUrl=os.environ['SQS_QUEUE_URL'],
                    MessageBody=json.dumps(task)
                )
                queued_count += 1
            except Exception as e:
                logger.error(f"Failed to queue task: {str(e)}")
                continue
        
        # Update status after queueing
        if queued_count != total_tasks:
            logger.error(f"Only queued {queued_count}/{total_tasks} tasks")
            status['status'] = 'error'
            status['error_message'] = f'Only queued {queued_count}/{total_tasks} tasks'
        else:
            status['status'] = 'running'
            
        # Update status after queueing
        s3_client.put_object(
            Bucket=os.environ['S3_BUCKET_NAME'],
            Key=status_key,
            Body=json.dumps(status),
            ContentType='application/json'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Collection initiated',
                'collection_id': collection_id,
                'total_tasks': total_tasks,
                'queued_tasks': queued_count,
                'status_key': status_key,
                'expected_files': f's3://{os.environ["S3_BUCKET_NAME"]}/collections/{collection_id}/[year]/[month]/[region].json'
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }