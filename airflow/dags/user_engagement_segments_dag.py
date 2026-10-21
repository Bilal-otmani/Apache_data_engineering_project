import os
from datetime import datetime

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator

def upload_csv_to_seaweedfs(**context):
    ds = context['ds']
    local_file_path = f"/tmp/segmented_users_{ds}.csv"
    object_name = f"segment_users/segmented_users_{ds}.csv"
    bucket_name = "customer-segments"

    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://seaweedfs:8333'),
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        ),
        region_name='us-east-1'
    )

    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as error:
        status_code = error.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        error_code = error.response.get('Error', {}).get('Code')
        if status_code != 404 and error_code not in {'404', 'NoSuchBucket', 'NotFound'}:
            raise
        s3_client.create_bucket(Bucket=bucket_name)

    s3_client.upload_file(local_file_path, bucket_name, object_name)
    print(f"Uploaded {local_file_path} to s3://{bucket_name}/{object_name}")

    context['ti'].xcom_push(key='s3_path', value=f"s3://{bucket_name}/{object_name}")
    
def export_segmented_users_to_csv(**context):
    ds = context['ds']  # Execution date
    output_path = f"/tmp/segmented_users_{ds}.csv"

    query = """
        SELECT user_id, email, full_name, total_pageviews, active_days,
               last_active_date, days_since_last_active, engagement_segment
        FROM demo.gold.user_engagement_segments
    """

    trino_hook = TrinoHook(trino_conn_id='trino_default')
    df = trino_hook.get_pandas_df(sql=query)

    df.to_csv(output_path, index=False)
    context['ti'].xcom_push(key='csv_path', value=output_path)

with DAG(
    dag_id="user_engagement_segments_dag", 
    start_date=datetime(2023,1,1), 
    schedule_interval='@daily'
) as dag:
    
    segment_users = TrinoOperator(
        task_id='segment_users',
        sql='sql/trino.sql',
        trino_conn_id='trino_default'
    )
    
    export_csv = PythonOperator(
        task_id='export_to_csv',
        python_callable=export_segmented_users_to_csv,
    )
    
    upload_to_seaweedfs = PythonOperator(
        task_id='upload_to_seaweedfs',
        python_callable=upload_csv_to_seaweedfs,
    )
    
    notify_success = EmailOperator(
        task_id='notify_success',
        to='marketing-team@example.com',
        subject='[Airflow] User Engagement Segments Exported',
        html_content="""
            <p>Hello Team,</p>
            <p>The user engagement segments have been refreshed and exported to a CSV file.</p>
            <p>File location: <code>s3://customer-segments/segment_users/segmented_users_{{ ds }}.csv</code></p>
            <p>– Airflow Bot</p>
        """
    )
    
    segment_users >> export_csv >> upload_to_seaweedfs >> notify_success