from airflow.sdk import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'feyisayo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

with DAG(
    dag_id='tradesphere_batch_pipeline',
    default_args=default_args,
    description='TradeSphere daily batch pipeline',
    schedule='False',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['tradesphere', 'batch']
) as dag:

    validate_data = BashOperator(
        task_id='validate_data',
        bash_command='cd /opt/airflow/tradesphere && python validation/validate_data.py'
    )

    ingest_all_sources = BashOperator(
        task_id='ingest_all_sources',
        bash_command='cd /opt/airflow/tradesphere && python ingestion/main.py'
    )

    write_bronze_delta = BashOperator(
        task_id='write_bronze_delta',
        bash_command='cd /opt/airflow/tradesphere && python delta/delta_writer.py'
    )

    run_spark_batch = BashOperator(
        task_id='run_spark_batch',
        bash_command='cd /opt/airflow/tradesphere && python spark/batch_transform.py'
    )

    load_to_snowflake = BashOperator(
        task_id='load_to_snowflake',
        bash_command='cd /opt/airflow/tradesphere && python snowflake/snowflake_loader.py'
    )

    dbt_staging = BashOperator(
        task_id='dbt_staging',
        bash_command='cd /opt/airflow/tradesphere/tradesphere_dbt && dbt run --select staging'
    )

    dbt_snapshots = BashOperator(
        task_id='dbt_snapshots',
        bash_command='cd /opt/airflow/tradesphere/tradesphere_dbt && dbt snapshot'
    )

    dbt_analytics = BashOperator(
        task_id='dbt_analytics',
        bash_command='cd /opt/airflow/tradesphere/tradesphere_dbt && dbt run --select analytics'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/tradesphere/tradesphere_dbt && dbt test'
    )

    validate_data >> ingest_all_sources >> write_bronze_delta >> run_spark_batch >> load_to_snowflake >> dbt_staging >> dbt_snapshots >> dbt_analytics >> dbt_test