"""
BRONZE: Raw SST ingestion — schema enforcement, no transformations.
Run this first in Databricks. Creates raw Delta tables from CSV sources.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, BooleanType
)

# --- Configuration ---
RAW_PATH = "dbfs:/raw/bronze"
BRONZE_PATH = f"{RAW_PATH}/bronze"

# --- Supplier schema (explicit, enforced) ---
supplier_schema = StructType([
    StructField("supplier_id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("sst_number", StringType(), nullable=True),
    StructField("sector", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("registration_type", StringType(), nullable=True),
    StructField("is_active", BooleanType(), nullable=True),
    StructField("created_date", DateType(), nullable=True),
    StructField("updated_date", DateType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("phone", StringType(), nullable=True),
])

# --- SST Filings schema ---
filing_schema = StructType([
    StructField("filing_id", StringType(), nullable=False),
    StructField("supplier_id", StringType(), nullable=False),
    StructField("filing_date", DateType(), nullable=False),
    StructField("period", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("sector", StringType(), nullable=False),
    StructField("gross_sales", DoubleType(), nullable=False),
    StructField("taxable_sales", DoubleType(), nullable=False),
    StructField("sst_rate", DoubleType(), nullable=False),
    StructField("sst_payable", DoubleType(), nullable=False),
    StructField("filing_status", StringType(), nullable=False),
    StructField("submission_reference", StringType(), nullable=False),
])


def ingest_table(csv_path, table_name, schema):
    """Read CSV with explicit schema, write as Delta, return DataFrame."""
    print(f"\n{'='*60}")
    print(f"INGESTING: {table_name}")
    print(f"{'='*60}")

    df = spark.read.option("header", "true").schema(schema).csv(csv_path)
    row_count = df.count()
    print(f"Rows read: {row_count}")

    # Write to Bronze as Delta
    df.write.mode("overwrite").delta(f"{BRONZE_PATH}/{table_name}")
    print(f"Written to: {BRONZE_PATH}/{table_name}")

    # Show sample
    print(f"\nSample data ({table_name}):")
    df.show(5, truncate=False)

    return df


# --- Run ingestion ---
print("Starting Bronze ingestion...")
suppliers_bronze = ingest_table(
    f"{RAW_PATH}/suppliers.csv",
    "suppliers",
    supplier_schema
)

filings_bronze = ingest_table(
    f"{RAW_PATH}/sst_filings.csv",
    "sst_filings",
    filing_schema
)

# --- Verify Delta log ---
print("\n--- Delta Log Verification ---")
print("Supplier Delta log files:")
for item in dbutils.fs.ls(f"{BRONZE_PATH}/suppliers/_delta_log/"):
    print(f"  {item['name']}")

print("\nFiling Delta log files:")
for item in dbutils.fs.ls(f"{BRONZE_PATH}/sst_filings/_delta_log/"):
    print(f"  {item['name']}")

# --- Time travel demo ---
print("\n--- Time Travel Demo ---")
# Overwrite with fewer rows
filings_bronze.limit(100).write.mode("overwrite").delta(
    f"{BRONZE_PATH}/sst_filings"
)
current = spark.read.delta(f"{BRONZE_PATH}/sst_filings").count()
v0 = spark.read.option("versionAsOf", 0).delta(f"{BRONZE_PATH}/sst_filings").count()
print(f"Version 0 rows: {v0}, Current rows: {current}")
print("Time travel works — critical for audit trails in accounting data.")

print("\n✅ Bronze layer complete.")