import great_expectations as gx
import pandas as pd
import boto3
from io import StringIO

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"

def read_csv_from_s3(s3_key):
    obj = s3_client.get_object(Bucket=RAW_BUCKET, Key=s3_key)
    return pd.read_csv(StringIO(obj["Body"].read().decode("latin-1")))

def run_suite(df, suite_name, expectations):
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas(f"{suite_name}_source")
    da = ds.add_dataframe_asset(f"{suite_name}_asset")
    batch_def = da.add_batch_definition_whole_dataframe(f"{suite_name}_batch")
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    for exp in expectations:
        suite.add_expectation(exp)

    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    results = batch.validate(suite)
    success = results["success"]
    print(f"{suite_name} validation: {'PASSED' if success else 'FAILED'}")
    return success

def validate_orders():
    df = read_csv_from_s3("source/orders/DataCoSupplyChainDataset.csv")
    return run_suite(df, "orders_suite", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Order Id"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="Sales", min_value=0, max_value=1000000),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Order Status")
    ])

def validate_returns():
    df = read_csv_from_s3("source/order_returns/order_returns.csv")
    return run_suite(df, "returns_suite", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="return_id"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="return_id"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="refund_amount", min_value=0, max_value=500),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="return_reason")
    ])

def validate_complaints():
    df = read_csv_from_s3("source/customer_complaints/customer_complaints.csv")
    return run_suite(df, "complaints_suite", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="complaint_id"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="complaint_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="complaint_type"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="resolved")
    ])

def validate_store_regions():
    df = read_csv_from_s3("source/store_regions/store_regions.csv")
    return run_suite(df, "store_regions_suite", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="store_id"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="store_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="country"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="region")
    ])

def run_all_validations():
    print("Running TradeSphere data validations...")

    results = {
        "orders": validate_orders(),
        "returns": validate_returns(),
        "complaints": validate_complaints(),
        "store_regions": validate_store_regions()
    }

    all_passed = all(results.values())

    print("\nValidation Summary:")
    for name, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        print(f"  {name}: {status}")

    if not all_passed:
        raise Exception("Data validation failed. Pipeline stopped.")

    print("\nAll validations passed. Pipeline can proceed.")

if __name__ == "__main__":
    run_all_validations()