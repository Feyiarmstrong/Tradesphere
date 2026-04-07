# TradeSphere — A Unified Global Trade Analytics Platform

Stack: Python • Apache Kafka • Delta Lake • Snowflake • dbt • Airflow • Terraform • Docker • GitHub Actions  
AWS Cost: ~$3/month

-----

## What is TradeSphere?

TradeSphere is a production-grade, end-to-end data engineering platform that turns messy, scattered global trade data into clean, real-time business intelligence. It ingests data from four different sources, validates it, processes it through both batch and streaming pipelines, transforms it in a cloud data warehouse and serves it to business users through dashboards.

### The Problem it Solves

In a real global e-commerce company, data is scattered everywhere in CSV files, databases, APIs, spreadsheets. Data is dirty, slow, and unreliable. Reports come hours or days late. There is no single source of truth. Businesses struggle to answer basic questions like:

- How much did we sell globally today?
- Which region is underperforming right now?
- Are deliveries delayed in Canada?
- What is our return rate this week?

TradeSphere solves this by building a unified platform that collects, cleans, processes, standardises and serves all of this data in near-real-time.

-----

## Architecture Overview
Data Sources → Ingestion → Validation → Bronze Delta Lake → Silver Delta Lake → Snowflake → dbt → Dashboards
                                                                     

### Two Pipelines Running Side By Side

Pipeline 1 — Batch (Airflow, daily at 6am)
S3 CSV → Python Ingestion → Great Expectations → Delta Lake (Bronze) → 
Batch Transform → Delta Lake (Silver) → Snowflake → dbt → Metabase

Pipeline 2 — Streaming (Kafka, continuous)
Kafka Producer → tradesphere-orders topic → Kafka Consumer → 
Delta Lake (Silver/Streaming) → Snowflake (every 15 mins via Airflow DAG 2)

-----

## Data Sources

|Source               |Tool    |Description                          |Rows           |
|---------------------|--------|-------------------------------------|---------------|
|S3 CSV               |boto3   |DataCo Supply Chain dataset          |~180,000 orders|
|PostgreSQL (Supabase)|psycopg2|order_returns and customer_complaints|1,000 each     |
|Exchange Rates API   |requests|Live USD/GBP/EUR/NGN/JPY/ZAR rates   |Daily JSON     |
|Google Sheets        |gspread |Store region metadata                |500 rows       |

-----

## Project Structure
tradesphere/

├── .github/

│   └── workflows/

│       └── tradesphere_ci.yml       # GitHub Actions CI/CD

├── airflow/

│   └── dags/

│       ├── tradesphere_batch_pipeline.py     # Daily batch DAG

│       └── tradesphere_streaming_monitor.py  # 15-min streaming DAG

├── data/

│   └── DataCoSupplyChainDataset.csv  # Source CSV (Kaggle)

├── data_generation/

│   ├── generate_supabase_data.py     # Populates Supabase tables

│   └── generate_gsheet_data.py       # Populates Google Sheet

├── delta/

│   └── delta_writer.py               # Writes Bronze Delta Lake

├── docker/

│   ├── docker-compose.yml            # All Docker services

│   └── Dockerfile                    # Container image definition

├── ingestion/

│   ├── main.py                       # Runs all 4 ingesters

│   ├── s3_ingester.py                # S3 CSV ingestion

│   ├── postgres_ingester.py          # Supabase ingestion

│   ├── gsheets_ingester.py           # Google Sheets ingestion

│   └── api_ingester.py               # Exchange rates API

├── kafka/

│   ├── kafka_producer.py             # Streams orders to Kafka

│   └── kafka_consumer.py             # Consumes and writes to Delta

├── snowflake/

│   └── snowflake_loader.py           # Loads Silver Delta to Snowflake

├── spark/

│   └── batch_transform.py            # Bronze → Silver transformation

├── terraform/

│   ├── main.tf                       # Root Terraform config

│   ├── variables.tf                  # Input variables

│   ├── outputs.tf                    # Output values

│   ├── backend.tf                    # S3 remote state config

│   └── modules/

│       ├── s3/                       # S3 + DynamoDB resources


│       ├── sns/                      # SNS alerts topic

│       ├── lambda/                   # Lambda trigger + EventBridge

│       └── glue/                     # Glue crawler + catalog

├── tradesphere_dbt/

│   ├── models/

│   │   ├── staging/                  # 8 staging views

│   │   └── analytics/                # 7 analytics tables

│   └── snapshots/                    # 2 SCD Type 2 snapshots

├── validation/

│   └── validate_data.py              # Great Expectations suites

├── .env                              # Local credentials (gitignored)

├── .gitignore

├── requirements.txt

└── requirements-lock.txt

-----

## AWS Infrastructure (Terraform)

|Resource                      |Purpose                                |Cost     |
|------------------------------|---------------------------------------|---------|
|`tradesphere-raw-feyisayo`    |S3 — all data storage (Bronze + Silver)|~$0.50/mo|
|`tradesphere-tfstate-feyisayo`|S3 — Terraform remote state            |Minimal  |
|`tradesphere-tf-locks`        |DynamoDB — state locking               |Free tier|
|`tradesphere-trigger`         |Lambda — event-driven ingestion        |Free tier|
|`tradesphere-schedule`        |EventBridge — daily schedule           |Free tier|
|`tradesphere-alerts`          |SNS — email alerts on failures         |Free tier|
|`tradesphere-ingestion`       |ECR — ingestion Docker image           |Free tier|
|`tradesphere-spark`           |ECR — spark Docker image               |Free tier|
|`tradesphere-db-secrets`      |Secrets Manager — DB credentials       |~$0.40/mo|
|`tradesphere-crawler`         |Glue Crawler — catalog S3 files        |~$1/mo   |

Total estimated monthly cost: Under $3

-----

## Docker Services

|Service              |Image                          |Port|Purpose            |
|---------------------|-------------------------------|----|-------------------|
|airflow-apiserver    |apache/airflow:3.1.3           |8083|Airflow UI         |
|airflow-scheduler    |apache/airflow:3.1.3           |—   |Runs DAGs          |
|airflow-worker       |apache/airflow:3.1.3           |—   |Celery worker      |
|airflow-dag-processor|apache/airflow:3.1.3           |—   |Processes DAG files|
|postgres             |postgres:16                    |5434|Airflow metadata DB|
|redis                |redis:7.2                      |6379|Celery broker      |
|kafka                |confluentinc/cp-kafka:7.4.0    |9092|Message broker     |
|zookeeper            |confluentinc/cp-zookeeper:7.4.0|2181|Kafka coordinator  |
|kafka-ui             |provectuslabs/kafka-ui         |8082|Kafka monitoring   |
|metabase             |metabase/metabase              |3000|BI dashboards      |

-----

## Snowflake Setup

Database: TRADESPHERE_DB  
Warehouse: TRADESPHERE_WH (X-Small, auto-suspend 60s)  
Schemas: RAW, STAGING, ANALYTICS, SNAPSHOTS

### RAW Tables (loaded by Python)

- RAW_ORDERS — 65,752 unique orders
- RAW_RETURNS — 1,000 returns
- RAW_COMPLAINTS — 1,000 complaints
- RAW_STORE_REGIONS — 500 store regions

### Staging Views (dbt)

stg_orders, stg_products, stg_customers, stg_shipping, stg_order_returns, stg_customer_complaints, stg_exchange_rates, stg_store_regions

### Analytics Tables (dbt)

dim_customers, dim_products, dim_stores, dim_dates, fct_orders, fct_returns, fct_shipping_performance

### Snapshots (SCD Type 2)

customer_snapshot — tracks address and segment changes over time  
product_snapshot — tracks price changes over time

-----

## Data Quality

Great Expectations validation runs before every pipeline execution. Four suites cover all data sources:

|Suite              |Key Checks                                                  |
|-------------------|------------------------------------------------------------|
|orders_suite       |Order ID not null, Sales between 0–1M, Status not null      |
|returns_suite      |Return ID not null and unique, Refund amount 0–500          |
|complaints_suite   |Complaint ID not null and unique, Type and resolved not null|
|store_regions_suite|Store ID not null and unique, Country and region not null   |

If any check fails, the Airflow DAG stops immediately and SNS sends an email alert. Bad data never enters Snowflake.

-----

## Kafka Streaming

Three Kafka topics:

|Topic                |Purpose                              |
|---------------------|-------------------------------------|
|`tradesphere-orders` |Live order events streamed row by row|
|`tradesphere-returns`|Live returns events                  |
|`tradesphere-dlq`    |Dead letter queue for failed messages|

The producer reads the DataCo CSV row by row, simulating real-time order events. The consumer buffers 100 messages per micro-batch, deduplicates on Order ID, and writes each batch to Delta Lake at silver/streaming/orders/ on S3.

-----

## Airflow DAGs

### DAG 1 — tradesphere_batch_pipeline (daily at 6am)
validate_data → ingest_all_sources → write_bronze_delta → run_spark_batch → 
load_to_snowflake → dbt_staging → dbt_snapshots → dbt_analytics → dbt_test

### DAG 2 — tradesphere_streaming_monitor (every 15 minutes)
check_streaming_files → load_streaming_to_snowflake

-----

## CI/CD Pipeline (GitHub Actions)

Three jobs run on every push to main:

1. Code Quality — Python syntax checks on all scripts
2. Build and Push — Docker images built and pushed to AWS ECR
3. dbt Run — staging models, snapshots, analytics models and tests run against Snowflake

Success triggers an email notification.

-----

## FinOps — Cost Optimisation

|Component         |Expensive Option      |Our Choice       |Monthly Saving|
|------------------|----------------------|-----------------|--------------|
|Kafka             |Amazon MSK (~$200/mo) |Docker local     |$200          |
|Spark             |AWS EMR (~$100/mo)    |Local processing |$100          |
|Airflow           |AWS MWAA (~$300/mo)   |Docker local     |$300          |
|Dashboard         |Tableau (~$70/user/mo)|Metabase Docker  |$70           |
|Container Registry|Docker Hub Pro ($7/mo)|AWS ECR free tier|$7            |

Total savings: ~$677/month vs fully managed stack

FinOps principles applied:

- Snowflake warehouse auto-suspends after 60 seconds
- S3 Intelligent Tiering for cold data
- All resources tagged project=tradesphere for cost tracking
- AWS Budget Alert set at $5/month

-----

## Prerequisites

- Python 3.11+
- AWS CLI configured (`aws configure`)
- Terraform installed
- Docker Desktop
- Java 17 (for local PySpark — Eclipse Temurin recommended)
- Snowflake account
- Supabase account
- Google Cloud service account with Sheets and Drive APIs enabled
- Kaggle account (for DataCo dataset)

-----

## How to Run Locally

### 1. Clone the repository
git clone https://github.com/Feyiarmstrong/tradesphere.git
cd tradesphere

### 2. Create virtual environment and install dependencies
python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt

### 3. Set up environment variables

Create a .env file in the project root:

### 4. Add GCP service account

Place your gcp_service_account.json in the config/ folder.

### 5. Download DataCo dataset

Download from [Kaggle](https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis) and place DataCoSupplyChainDataset.csv in data/.

### 6. Provision AWS infrastructure
- cd terraform
- aws s3 mb s3://tradesphere-tfstate-feyisayo --region us-east-1
- terraform init
- terraform plan
- terraform apply

### 7. Generate data
- python data_generation/generate_supabase_data.py
- python data_generation/generate_gsheet_data.py

### 8. Run ingestion
- python ingestion/main.py

### 9. Run validation
- python validation/validate_data.py

### 10. Write Bronze Delta Lake
- python delta/delta_writer.py

### 11. Run batch transformation
# Set Java 17 path first (Windows)
- export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.18.8-hotspot"
- export PYSPARK_PYTHON=$(which python)
- python spark/batch_transform.py

### 12. Load to Snowflake
- python snowflake/snowflake_loader.py

### 13. Run dbt models
- cd tradesphere_dbt
- dbt run --select staging
- dbt snapshot
- dbt run --select analytics
- dbt test

### 14. Start Docker services
- docker-compose -f docker/docker-compose.yml up -d

### 15. Run Kafka streaming (two terminals)
# Terminal 1
- python kafka/kafka_consumer.py

# Terminal 2
- python kafka/kafka_producer.py

-----

## Accessing Services

|Service   |URL                  |Credentials         |
|----------|---------------------|--------------------|
|Airflow UI|http://localhost:8083|admin / admin       |
|Kafka UI  |http://localhost:8082|—                   |
|Metabase  |http://localhost:3000|Setup on first visit|

-----

## Environment Variables Reference

|Variable               |Description                                |
|-----------------------|-------------------------------------------|
|`DB_HOST`              |Supabase Transaction Pooler host           |
|`DB_PORT`              |Supabase port (6543 for Transaction Pooler)|
|`DB_NAME`              |Supabase database name                     |
|`DB_USER`              |Supabase user (format: postgres.project_id)|
|`DB_PASSWORD`          |Supabase password                          |
|`SNOWFLAKE_ACCOUNT`    |Snowflake account identifier               |
|`SNOWFLAKE_USER`       |Snowflake username                         |
|`SNOWFLAKE_PASSWORD`   |Snowflake password                         |
|`SNOWFLAKE_WAREHOUSE`  |Snowflake warehouse name                   |
|`SNOWFLAKE_DATABASE`   |Snowflake database name                    |
|`SNOWFLAKE_SCHEMA`     |Target schema (RAW for initial load)       |
|`AWS_ACCESS_KEY_ID`    |AWS access key                             |
|`AWS_SECRET_ACCESS_KEY`|AWS secret key                             |

-----

## Key Engineering Decisions

Why Delta Lake over plain Parquet?  
Delta Lake adds ACID transactions, time travel, schema enforcement and upsert support on top of S3. This means failed writes don’t leave corrupt files, and you can roll back to any previous version of the data.

Why `deltalake` Python library instead of PySpark?  
PySpark requires Java and Hadoop and has significant Windows compatibility issues. The deltalake library provides the same Delta Lake capabilities in pure Python with no JVM dependency, making it portable and simpler to run locally and in CI/CD.

Why Docker for Kafka/Airflow/Metabase instead of managed AWS services?  
Amazon MSK costs ~$200/month, AWS MWAA costs ~$300/month. Running them in Docker locally achieves identical pipeline capabilities at near-zero cost. This is a deliberate FinOps decision.

Why CeleryExecutor for Airflow?  
CeleryExecutor distributes task execution across multiple workers, enabling parallel DAG task execution. This is more production-representative than LocalExecutor which runs tasks sequentially.

Why Snowflake auto-suspend at 60 seconds?  
The warehouse only needs to run during active queries. Auto-suspend eliminates idle compute charges, reducing Snowflake costs by approximately 80%.

-----

## Feyisayo Ajiboye | Data Engineer

GitHub: [@Feyiarmstrong](https://github.com/Feyiarmstrong)