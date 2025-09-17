import streamlit as st
import boto3
import json
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import threading
from analytics_pipeline import NewsAnalyticsPipeline

# Initialize AWS clients
@st.cache_resource
def get_aws_clients():
    """Initialize AWS clients"""
    s3_client = boto3.client('s3',
        aws_access_key_id=st.secrets['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=st.secrets['AWS_SECRET_ACCESS_KEY'],
        region_name=st.secrets.get('AWS_REGION', 'eu-north-1')
    )
    
    lambda_client = boto3.client('lambda',
        aws_access_key_id=st.secrets['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=st.secrets['AWS_SECRET_ACCESS_KEY'],
        region_name=st.secrets.get('AWS_REGION', 'eu-north-1')
    )
    
    return {
        's3': s3_client,
        'lambda': lambda_client
    }

def invoke_collection_lambda(query, max_articles_per_month=20, years_back=3, regions=None):
    """Invoke Lambda function to start data collection"""
    aws_clients = get_aws_clients()
    lambda_client = aws_clients['lambda']
    
    # Prepare payload
    payload = {
        'query': query,
        'max_articles_per_month': max_articles_per_month,
        'years_back': years_back,
        'regions': regions or ['north_america', 'europe', 'asia_pacific']
    }
    
    try:
        # Make synchronous invocation
        response = lambda_client.invoke(
            FunctionName=st.secrets['LAMBDA_FUNCTION_NAME'],  # Use direct secret access
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Check for Lambda execution errors
        if response['StatusCode'] != 200:
            st.error(f"Lambda execution failed with status code: {response['StatusCode']}")
            return None
            
        # Parse response payload
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        
        # Check for function errors
        if response_payload.get('statusCode') != 200:
            error_body = json.loads(response_payload.get('body', '{}'))
            st.error(f"Function error: {error_body.get('error', 'Unknown error')}")
            return None
            
        # Parse successful response
        response_body = json.loads(response_payload['body'])
        
        if 'collection_id' not in response_body:
            st.error("No collection ID in response")
            return None
            
        return {
            'collection_id': response_body['collection_id'],
            'total_tasks': response_body['total_tasks']
        }
        
    except Exception as e:
        st.error(f"Error invoking Lambda: {str(e)}")
        return None

def check_collection_status(query, collection_id):
    """Check the status of data collection for a query"""
    aws_clients = get_aws_clients()
    s3_client = aws_clients['s3']
    bucket_name = st.secrets.get('S3_BUCKET_NAME')

    # --- GUARD: Ensure query and collection_id are valid ---
    if not query or not collection_id:
        st.warning("No query or collection ID provided. Please start a new collection or select a valid one.")
        # --- SHOW STATUS IF ANY EXIST IN BUCKET ---
        try:
            # List all status files in the bucket
            status_files = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix="status/"
            ).get("Contents", [])
            if status_files:
                st.markdown("### Available Status Files in S3:")
                for obj in status_files:
                    st.write(obj["Key"])
            else:
                st.info("No status files found in S3 bucket.")
        except Exception as e:
            st.error(f"Could not list status files: {str(e)}")
        # -------------------------------------------
        return None
    # -------------------------------------------------------

    try:
        status_key = f'status/{query.lower().replace(" ", "_")}_{collection_id}.json'
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=status_key
        )
        status_data = json.loads(response['Body'].read().decode('utf-8'))
        return status_data
    except Exception as e:
        st.error(f"Error checking collection status: {str(e)}")
        # --- SHOW STATUS IF ANY EXIST IN BUCKET ON ERROR ---
        try:
            status_files = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix="status/"
            ).get("Contents", [])
            if status_files:
                st.markdown("### Available Status Files in S3:")
                for obj in status_files:
                    st.write(obj["Key"])
            else:
                st.info("No status files found in S3 bucket.")
        except Exception as e2:
            st.error(f"Could not list status files: {str(e2)}")
        # ---------------------------------------------------
        return None

def get_collected_data(query, collection_id, min_articles=1):
    """Retrieve collected data for a query"""
    aws_clients = get_aws_clients()
    s3_client = aws_clients['s3']
    bucket_name = st.secrets.get('S3_BUCKET_NAME')
    
    try:
        # Check for merged data
        merged_key = f"processed_data/{query.replace(' ', '_').replace(':', '').lower()[:100]}/merged.json"
        response = s3_client.get_object(Bucket=bucket_name, Key=merged_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        if data.get('total_articles', 0) >= min_articles:
            return data
        return None
        
    except Exception as e:
        st.error(f"Error retrieving collected data: {str(e)}")
        return None

def display_collection_progress(status_data, progress_placeholder=None, metrics_placeholder=None):
    """Display collection progress with placeholders"""
    if not status_data:
        if progress_placeholder:
            progress_placeholder.progress(0)
        if metrics_placeholder:
            with metrics_placeholder.container():
                st.info("Initializing collection...")
        return
        
    status = status_data.get('status', 'unknown')
    total_tasks = status_data.get('total_tasks', 0)
    completed_tasks = status_data.get('completed_tasks', 0)
    total_articles = status_data.get('total_articles', 0)
    
    # Calculate progress and article rate
    progress_pct = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
    articles_per_task = total_articles / completed_tasks if completed_tasks > 0 else 0
    
    if progress_placeholder and metrics_placeholder:
        progress_placeholder.progress(min(progress_pct / 100, 1.0))
        
        with metrics_placeholder.container():
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Tasks", f"{completed_tasks}/{total_tasks}")
            with col2:
                st.metric("Progress", f"{progress_pct:.1f}%")
            with col3:
                st.metric("Articles", f"{total_articles} ({articles_per_task:.1f}/task)")
            
            if total_articles == 0 and completed_tasks > 0:
                st.warning("⚠️ No articles collected yet - check query and regions")
    else:
        st.write(f"**Status:** {status}")
        st.progress(min(progress_pct / 100, 1.0))
        st.write(f"Tasks: {completed_tasks}/{total_tasks}")
        st.write(f"Articles: {total_articles}")

def main():
    st.set_page_config(page_title="GDELT News Analyzer", layout="wide")
    st.title("🌍 GDELT News Analyzer")
    st.write("Enterprise-grade analysis of global news trends")

    # --- PAGE SELECTION ---
    page = st.sidebar.radio(
        "Navigation",
        ["Data Collection", "Data Analysis"],
        index=0
    )

    if page == "Data Collection":
        # Create sidebar for configuration
        with st.sidebar:
            st.header("Analysis Configuration")
            max_articles = st.slider("Max Articles per Month/Region", 5, 50, 20)
            years_back = st.slider("Years to Analyze", 1, 5, 3)

            st.subheader("Regions")
            # Expanded region/country options
            region_options = [
                ("North America", "north_america"),
                ("Europe", "europe"),
                ("Asia-Pacific", "asia_pacific"),
                ("Latin America", "latin_america"),
                ("Middle East", "middle_east"),
                ("Africa", "africa"),
                ("Oceania", "oceania"),
                ("South Asia", "south_asia"),
                ("Southeast Asia", "southeast_asia"),
            ]
            default_selected = ["north_america", "europe", "asia_pacific"]
            region_label_map = {v: k for k, v in region_options}
            selected_regions = st.multiselect(
                "Select Regions",
                options=[r[1] for r in region_options],
                default=default_selected,
                format_func=lambda x: region_label_map.get(x, x)
            )
            regions = selected_regions

            st.subheader("Status Updates")
            refresh_interval = st.slider("Refresh interval (seconds)", 5, 60, 10)
        
        # Main content area
        query = st.text_input("Enter topic to analyze", "climate change")
        
        col1, col2 = st.columns(2)
        with col1:
            analyze_button = st.button("Analyze Topic", type="primary")
        with col2:
            min_articles = st.number_input("Minimum articles for analysis", 
                                        min_value=5, 
                                        max_value=100, 
                                        value=20)
        
        if 'collection_id' not in st.session_state:
            st.session_state.collection_id = None
        
        if analyze_button:
            with st.spinner("Initiating data collection..."):
                response = invoke_collection_lambda(
                    query, 
                    max_articles_per_month=max_articles,
                    years_back=years_back,
                    regions=regions
                )
                
                if response and 'collection_id' in response:
                    st.session_state.collection_id = response['collection_id']
                    st.success(f"Data collection initiated! Collection ID: {response['collection_id']}")
                else:
                    st.error("Failed to initiate data collection.")
        
        if st.session_state.collection_id:
            # Create placeholders for updating status
            st.header("📊 Collection Progress")
            status_placeholder = st.empty()
            progress_placeholder = st.empty()
            metrics_container = st.container()
            
            # Check status
            status_data = check_collection_status(query, st.session_state.collection_id)
            
            if status_data:
                # Display progress
                total_tasks = status_data.get('total_tasks', 0)
                completed_tasks = status_data.get('completed_tasks', 0)
                total_articles = status_data.get('total_articles', 0)
                
                progress_pct = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
                
                progress_placeholder.progress(min(progress_pct / 100, 1.0))
                
                with metrics_container:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Tasks Completed", f"{completed_tasks}/{total_tasks}")
                    with col2:
                        st.metric("Progress", f"{progress_pct:.1f}%")
                    with col3:
                        st.metric("Articles Collected", total_articles)
                
                # Check if collection is complete
                if status_data.get('status') == 'completed':
                    status_placeholder.success("✅ Collection completed!")
                    
                    # Check if we have enough data for analysis
                    data = get_collected_data(query, st.session_state.collection_id, min_articles)
                    if data:
                        st.header("🔍 Analysis")
                        if st.button("Go to Data Analysis", type="primary"):
                            st.session_state.analysis_ready = True
                            st.session_state.analysis_query = query
                            st.session_state.analysis_collection_id = st.session_state.collection_id
                            st.switch_page("analytics.py")
                    else:
                        st.warning("Collection completed but no data found.")
                
                elif status_data.get('status') == 'error':
                    status_placeholder.error("❌ Collection failed. Please try again.")
                
                else:
                    status_placeholder.info("⏳ Collection in progress...")
                    time.sleep(refresh_interval)
                    st.rerun()  # Auto-refresh

    elif page == "Data Analysis":
        # --- DATA ANALYSIS PAGE ---
        st.header("📈 Data Analysis")
        st.info("You can analyze any data present in your S3 bucket, regardless of how it was collected.")
        # FIX: Use relative import for analytics.py in the same folder
        import analytics as analytics
        analytics.main()

if __name__ == "__main__":
    main()
