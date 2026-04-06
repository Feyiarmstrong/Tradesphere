import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()
fake = Faker()

# DB Connection
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    sslmode="require"
)
cur = conn.cursor()

# Create tables
cur.execute("""
    CREATE TABLE IF NOT EXISTS order_returns (
        return_id VARCHAR(20) PRIMARY KEY,
        order_id VARCHAR(20),
        return_date DATE,
        return_reason VARCHAR(100),
        refund_amount NUMERIC(10,2)
    );
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS customer_complaints (
        complaint_id VARCHAR(20) PRIMARY KEY,
        customer_id VARCHAR(20),
        complaint_date DATE,
        complaint_type VARCHAR(100),
        resolved BOOLEAN
    );
""")

conn.commit()
print("Tables created")

# Populate order_returns
return_reasons = ["Damaged item", "Wrong item", "Changed mind", "Late delivery", "Poor quality"]
returns_data = []
for i in range(1, 1001):
    returns_data.append((
        f"RET{i:05d}",
        f"ORD{random.randint(1, 50000):05d}",
        fake.date_between(start_date="-1y", end_date="today"),
        random.choice(return_reasons),
        round(random.uniform(10, 500), 2)
    ))

cur.executemany("""
    INSERT INTO order_returns (return_id, order_id, return_date, return_reason, refund_amount)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (return_id) DO NOTHING;
""", returns_data)

print("order_returns populated")

# Populate customer_complaints
complaint_types = ["Shipping delay", "Product defect", "Billing issue", "Customer service", "Website issue"]
complaints_data = []
for i in range(1, 1001):
    complaints_data.append((
        f"CMP{i:05d}",
        f"CUST{random.randint(1, 20000):05d}",
        fake.date_between(start_date="-1y", end_date="today"),
        random.choice(complaint_types),
        random.choice([True, False])
    ))

cur.executemany("""
    INSERT INTO customer_complaints (complaint_id, customer_id, complaint_date, complaint_type, resolved)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (complaint_id) DO NOTHING;
""", complaints_data)

print(" customer_complaints populated")

conn.commit()
cur.close()
conn.close()
print(" Supabase data generation complete!")