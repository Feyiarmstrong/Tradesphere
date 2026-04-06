import gspread
from google.oauth2.service_account import Credentials
from faker import Faker
import random

fake = Faker()

# GCP auth
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(
    "config/gcp_service_account.json",
    scopes=scopes
)

client = gspread.authorize(creds)

# Open existing sheet (create it manually in Google Drive first)
spreadsheet = client.open("tradesphere-store-regions")
sheet = spreadsheet.sheet1

print("Google Sheet opened:", spreadsheet.url)

# Header
headers = ["store_id", "store_name", "city", "country", "region", "manager_name", "open_date"]
sheet.append_row(headers)

# Data
regions = {
    "North America": ["New York", "Toronto", "Chicago", "Los Angeles", "Houston"],
    "Europe": ["London", "Paris", "Berlin", "Amsterdam", "Madrid"],
    "Africa": ["Lagos", "Nairobi", "Accra", "Cairo", "Johannesburg"],
    "Asia": ["Tokyo", "Shanghai", "Mumbai", "Singapore", "Seoul"],
    "Oceania": ["Sydney", "Melbourne", "Auckland", "Brisbane", "Perth"]
}

country_map = {
    "New York": "USA", "Chicago": "USA", "Los Angeles": "USA", "Houston": "USA",
    "Toronto": "Canada",
    "London": "UK",
    "Paris": "France",
    "Berlin": "Germany",
    "Amsterdam": "Netherlands",
    "Madrid": "Spain",
    "Lagos": "Nigeria", "Accra": "Ghana", "Nairobi": "Kenya",
    "Cairo": "Egypt", "Johannesburg": "South Africa",
    "Tokyo": "Japan", "Shanghai": "China", "Mumbai": "India",
    "Singapore": "Singapore", "Seoul": "South Korea",
    "Sydney": "Australia", "Melbourne": "Australia",
    "Auckland": "New Zealand", "Brisbane": "Australia", "Perth": "Australia"
}

rows = []
for i in range(1, 501):
    region = random.choice(list(regions.keys()))
    city = random.choice(regions[region])
    country = country_map[city]
    open_date = fake.date_between(start_date="-10y", end_date="today")

    rows.append([
        f"STR{i:05d}",
        f"{fake.company()} Store",
        city,
        country,
        region,
        fake.name(),
        str(open_date)
    ])

sheet.append_rows(rows)

print(f"{len(rows)} rows written to tradesphere-store-regions")
print("Day 2 complete - Google Sheets data generation done")