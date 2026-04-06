from airflow.sdk import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'feyisayo',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False
}

with DAG(
    dag_id='tradesphere_streaming_monitor',
    default_args=default_args,
    description='Monitors streaming data and loads to Snowflake every 15 minutes',
    schedule='*/15 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['tradesphere', 'streaming']
) as dag:

    check_streaming_files = BashOperator(
        task_id='check_streaming_files',
        bash_command='cd /opt/airflow/tradesphere && python -c "import boto3; s3 = boto3.client(\'s3\', region_name=\'us-east-1\'); r = s3.list_objects_v2(Bucket=\'tradesphere-raw-feyisayo\', Prefix=\'silver/streaming/orders/\'); print(f\'Found {r.get(\"KeyCount\", 0)} streaming files\')"'
    )

    load_streaming_to_snowflake = BashOperator(
        task_id='load_streaming_to_snowflake',
        bash_command='cd /opt/airflow/tradesphere && python snowflake/snowflake_loader.py'
    )

    check_streaming_files >> load_streaming_to_snowflake