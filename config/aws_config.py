import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import logging

# Set up logging
logger = logging.getLogger(__name__)

def load_aws_config():
    """
    Load AWS configuration with fallback options
    """
    config = {
        'aws_access_key_id': None,
        'aws_secret_access_key': None,
        'region_name': os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    }
    
    # Try environment variables first
    config['aws_access_key_id'] = os.environ.get('AWS_ACCESS_KEY_ID')
    config['aws_secret_access_key'] = os.environ.get('AWS_SECRET_ACCESS_KEY')
    
    # Try AWS credentials file
    if not config['aws_access_key_id'] or not config['aws_secret_access_key']:
        try:
            session = boto3.Session()
            credentials = session.get_credentials()
            if credentials:
                config['aws_access_key_id'] = credentials.access_key
                config['aws_secret_access_key'] = credentials.secret_key
        except Exception as e:
            logger.warning(f"Could not load credentials from file: {e}")
    
    return config

def get_parameter_from_ssm(parameter_name, with_decryption=True):
    """
    Retrieve a parameter from AWS Systems Manager Parameter Store
    """
    try:
        config = load_aws_config()
        ssm = boto3.client('ssm', **config)
        response = ssm.get_parameter(
            Name=parameter_name,
            WithDecryption=with_decryption
        )
        return response['Parameter']['Value']
    except (ClientError, NoCredentialsError) as e:
        logger.error(f"Error retrieving parameter {parameter_name}: {e}")
        return None