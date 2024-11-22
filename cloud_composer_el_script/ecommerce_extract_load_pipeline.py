"""
DAG for loading cleaned CSV files from Google Drive to BigQuery via Google Cloud Storage.

This pipeline retrieves CSV files from a specified Google Drive folder, preprocesses them,
uploads them to Google Cloud Storage (GCS), and finally loads the data into BigQuery.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage, secretmanager
from datetime import datetime
import pandas as pd
import io
import os
import json

# Load environment variables
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')
GDRIVE_FOLDER_ID = os.getenv('GDRIVE_FOLDER_ID')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

# Default arguments for the DAG
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Function to retrieve service account credentials from Secret Manager
def get_service_account_credentials(secret_name):
    """
    Fetches service account credentials from Secret Manager.
    
    Args:
        secret_name (str): The name of the secret in Secret Manager.
    
    Returns:
        dict: The service account credentials.
    """
    client = secretmanager.SecretManagerServiceClient()
    secret_path = client.secret_version_path(PROJECT_ID, secret_name, 'latest')
    response = client.access_secret_version(request={"name": secret_path})
    return json.loads(response.payload.data.decode('UTF-8'))

# Function to transfer files from Google Drive to GCS
def gdrive_to_gcs(file_name, gcs_folder_name, **kwargs):
    """
    Downloads a file from Google Drive, preprocesses it, and uploads it to GCS.
    
    Args:
        file_name (str): The name of the file in Google Drive.
        gcs_folder_name (str): The destination folder in GCS.
    """
    # Retrieve credentials from Secret Manager
    credentials_info = get_service_account_credentials(
        'ecommerce_elt_pipeline_service_account'
    )
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info
    )

    # Initialize Google Drive API client
    drive_service = build('drive', 'v3', credentials=credentials)

    # Search for the file in the specified Google Drive folder
    query = (
        f"'{GDRIVE_FOLDER_ID}' in parents and name='{file_name}' and trashed=false"
    )
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        raise FileNotFoundError(
            f"File {file_name} not found in Google Drive folder {GDRIVE_FOLDER_ID}"
        )

    file_id = files[0]['id']

    # Download the file from Google Drive
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    # Preprocess the CSV file
    fh.seek(0)
    cleaned_csv = preprocess_csv(fh)

    # Upload the cleaned file to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"{gcs_folder_name}/{file_name}")
    blob.upload_from_string(cleaned_csv, content_type='text/csv')

# Function to preprocess CSV data
def preprocess_csv(file_obj):
    """
    Cleans and preprocesses CSV data.
    
    Args:
        file_obj (BytesIO): The file-like object containing the CSV data.
    
    Returns:
        str: The cleaned CSV data as a string.
    """
    try:
        # Read the CSV into a pandas DataFrame
        df = pd.read_csv(file_obj, on_bad_lines='skip', quoting=3)

        # Convert DataFrame back to CSV string
        cleaned_csv = df.to_csv(index=False)
        return cleaned_csv
    except Exception as e:
        raise ValueError(f"Error during CSV preprocessing: {e}")

# Define table configurations
TABLES_CONFIG = [
    {
        'table_name': 'olist_closed_deals',
        'gcs_folder_name': 'data',
        'destination_table': f'{project_id}.ecommerce_data.olist_closed_deals',
        'schema_fields': [
            {'name': 'mql_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'seller_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'sdr_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'sr_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'won_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'business_segment', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'lead_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'lead_behaviour_profile', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'has_comnpany', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'has_gtin', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'average_stock', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'business_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'declared_product_catalog_size', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'declared_monthly_revenue', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]
    },
    {
        'table_name': 'olist_customers',
        'gcs_folder_name': 'data',
        'destination_table': f'{project_id}.ecommerce_data.olist_customers',
        'schema_fields': [
            {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'customer_unique_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'customer_zip_code_prefix', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'customer_city', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'customer_state', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    },
    {
        'table_name': 'olist_geolocation',
        'gcs_folder_name': 'data',
        'destination_table': f'{project_id}.ecommerce_data.olist_geolocation',
        'schema_fields': [
            {'name': 'geolocation_zip_code_prefix', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'geolocation_lat', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'geolocation_lng', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'geolocation_city', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'geolocation_state', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    },
    {
        'table_name': 'olist_marketing_qualified_leads',
        'gcs_folder_name': 'data',
        'destination_table': f'{project_id}.ecommerce_data.olist_marketing_qualified_leads',
        'schema_fields': [
            {'name': 'mql_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'first_contact_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'landing_page_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'origin', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    },
    {
        'table_name': 'olist_order_items',
        'gcs_folder_name': 'data',
        'destination_table': f'{project_id}.ecommerce_data.olist_order_items',
        'schema_fields': [
            {'name': 'order_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_item_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'product_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'seller_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'shipping_limit_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'price', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'freight_value', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    },
    {
        'table_name': 'olist_order_payments',
        'gcs_folder_name': 'data',
        'destination_table': f'{project_id}.ecommerce_data.olist_order_payments',
        'schema_fields': [
            {'name': 'order_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'payment_sequential', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'payment_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'payment_installments', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'payment_value', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    },
    {
        'table_name': 'olist_order_reviews',
        'gcs_folder_name': 'data',
        'destination_table': f'{project_id}.ecommerce_data.olist_order_reviews',
        'schema_fields': [
            {'name': 'review_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'review_score', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'review_comment_title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'review_comment_message', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'review_creation_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'review_answer_timestamp', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    },
    {
        'table_name': 'olist_orders',
        'gcs_folder_name': 'data',
        'destination_table': f'{project_id}.ecommerce_data.olist_orders',
        'schema_fields': [
            {'name': 'order_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_purchase_timestamp', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_approved_at', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_delivered_carrier_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_delivered_customer_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'order_estimated_delivery_date', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    },
    {
        'table_name': 'olist_products',
        'gcs_folder_name': 'data',
        'destination_table': f'{project_id}.ecommerce_data.olist_products',
        'schema_fields': [
            {'name': 'product_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'product_category_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'product_name_lenght', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'product_description_lenght', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'product_photos_qty', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'product_weight_g', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'product_length_cm', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'product_height_cm', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'product_width_cm', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    },
    {
        'table_name': 'olist_sellers',
        'gcs_folder_name': 'data',
        'destination_table': f'{project_id}.ecommerce_data.olist_sellers',
        'schema_fields': [
            {'name': 'seller_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'seller_zip_code_prefix', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'seller_city', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'seller_state', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    },
    {
        'table_name': 'olist_product_category_name',
        'gcs_folder_name': 'data',
        'destination_table': f'{project_id}.ecommerce_data.olist_product_category_name',
        'schema_fields': [
            {'name': 'product_category_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'product_category_name_english', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }
]


# Define the DAG
with DAG(
    'gdrive_to_bigquery_dag',
    default_args=DEFAULT_ARGS,
    description='Load cleaned CSVs from Google Drive to BigQuery via GCS',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 16),
    catchup=False,
) as dag:

    # Track all load tasks for dependencies
    previous_task = None

    # Loop through table configurations
    for table in TABLES_CONFIG:
        table_name = table['table_name']

        # Task: Download and preprocess file from Google Drive to GCS
        dump_file_to_gcs = PythonOperator(
            task_id=f'dump_gdrive_to_gcs_{table_name}',
            python_callable=gdrive_to_gcs,
            op_kwargs={
                'file_name': f"{table_name}.csv",
                'gcs_folder_name': table['gcs_folder_name'],
            },
        )

        # Task: Load GCS files to BigQuery
        load_to_bigquery = GCSToBigQueryOperator(
            task_id=f'load_gcs_to_bq_{table_name}',
            bucket=GCS_BUCKET,
            source_objects=[f"{table['gcs_folder_name']}/{table_name}.csv"],
            destination_project_dataset_table=table['destination_table'],
            schema_fields=table['schema_fields'],
            write_disposition='WRITE_TRUNCATE',
            skip_leading_rows=1,
            source_format='CSV',
            field_delimiter=',',
        )

        # Set dependencies
        if previous_task:
            previous_task >> dump_file_to_gcs
        dump_file_to_gcs >> load_to_bigquery
        previous_task = load_to_bigquery
