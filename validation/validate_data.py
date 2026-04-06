import great_expectations as gx
import pandas as pd
import boto3
import logging
from io import StringIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3", region_name="us-east-1")
RAW_BUCKET = "tradesphere-raw-feyisayo"


def read_csv_from_s3(s3_key):
    """Reads a CSV from S3 into a pandas DataFrame. Uses latin-1 for DataCo dataset."""
    try:
        obj = s3_client.get_object(Bucket=RAW_BUCKET, Key=s3_key)
        return pd.read_csv(StringIO(obj["Body"].read().decode("latin-1")))
    except Exception as e:
        logger.error(f"Failed to read CSV from S3 key {s3_key}: {e}")
        raise


def run_suite(df, suite_name, expectations):
    """
    Runs a Great Expectations validation suite against a DataFrame.
    Returns True if all expectations pass, False otherwise.
    """
    try:
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

        if success:
            logger.info(f"{suite_name} validation: PASSED")
        else:
            logger.warning(f"{suite_name} validation: FAILED")

        return success

    except Exception as e:
        logger.error(f"Error running validation suite {suite_name}: {e}")
        raise


def validate_orders():
    """Validates orders data — checks nulls, value ranges and required fields."""
    df = read_csv_from_s3("source/orders/DataCoSupplyChainDataset.csv")
    return run_suite(df, "orders_suite", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Order Id"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="Sales", min_value=0, max_value=1000000),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="Order Status")
    ])


def validate_returns():
    """Validates order_returns data — checks nulls, unique IDs and refund amounts."""
    df = read_csv_from_s3("source/order_returns/order_returns.csv")
    return run_suite(df, "returns_suite", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="return_id"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="return_id"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="refund_amount", min_value=0, max_value=500),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="return_reason")
    ])


def validate_complaints():
    """Validates customer_complaints data — checks nulls and unique IDs."""
    df = read_csv_from_s3("source/customer_complaints/customer_complaints.csv")
    return run_suite(df, "complaints_suite", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="complaint_id"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="complaint_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="complaint_type"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="resolved")
    ])


def validate_store_regions():
    """Validates store_regions data — checks nulls and unique store IDs."""
    df = read_csv_from_s3("source/store_regions/store_regions.csv")
    return run_suite(df, "store_regions_suite", [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="store_id"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="store_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="country"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="region")
    ])


def run_all_validations():
    """
    Main entry point. Runs all 4 validation suites in sequence.
    Raises an exception if any suite fails — this stops the Airflow pipeline.
    """
    logger.info("Running TradeSphere data validations...")

    results = {
        "orders": validate_orders(),
        "returns": validate_returns(),
        "complaints": validate_complaints(),
        "store_regions": validate_store_regions()
    }

    all_passed = all(results.values())

    logger.info("Validation Summary:")
    for name, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        logger.info(f"  {name}: {status}")

    if not all_passed:
        failed = [k for k, v in results.items() if not v]
        raise Exception(f"Data validation failed for: {failed}. Pipeline stopped.")

    logger.info("All validations passed. Pipeline can proceed.")


if __name__ == "__main__":
    run_all_validations()