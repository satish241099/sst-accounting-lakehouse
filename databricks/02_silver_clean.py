"""
SILVER: Data cleaning, validation, deduplication.
Transforms Bronze raw data into clean, validated Silver tables.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_PATH = "dbfs:/raw/bronze/bronze"
SILVER_PATH = "dbfs:/raw/silver"

print("=" * 60)
print("SILVER: Cleaning & Validation")
print("=" * 60)


# =====================================================
# SILVER: Suppliers (with dedup + SCD2 preparation)
# =====================================================
print("\n--- Processing Suppliers ---")

suppliers = spark.read.delta(f"{BRONZE_PATH}/suppliers")
print(f"Bronze supplier rows: {suppliers.count()}")

# Deduplicate on supplier_id (keep latest by updated_date)
window = Window.partitionBy("supplier_id").orderBy(F.col("updated_date").desc())
suppliers_deduped = (
    suppliers
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
print(f"After dedup: {suppliers_deduped.count()} rows")

# Data quality checks
total = suppliers_deduped.count()
null_ids = suppliers_deduped.filter(F.col("supplier_id").isNull()).count()
null_names = suppliers_deduped.filter(F.col("name").isNull()).count()
bad_sst = suppliers_deduped.filter(
    F.col("sst_number").rlike(r'^SST-\d{6}$') == False
).count()

print(f"Quality checks:")
print(f"  Null supplier_ids: {null_ids}")
print(f"  Null names: {null_names}")
print(f"  Invalid SST numbers: {bad_sst}")

# Add Silver metadata
suppliers_silver = (
    suppliers_deduped
    .withColumn("silver_processed_at", F.current_timestamp())
    .withColumn("data_quality_flag",
        F.when(F.col("supplier_id").isNull() | F.col("name").isNull(), "FAIL")
         .otherwise("PASS"))
)

suppliers_silver.write.mode("overwrite").delta(f"{SILVER_PATH}/suppliers")
print(f"Silver suppliers written: {suppliers_silver.count()} rows")


# =====================================================
# SILVER: SST Filings (validation + dedup)
# =====================================================
print("\n--- Processing SST Filings ---")

filings = spark.read.delta(f"{BRONZE_PATH}/sst_filings")
print(f"Bronze filing rows: {filings.count()}")

# Deduplicate on filing_id
filings_deduped = filings.dropDuplicates(["filing_id"])
print(f"After dedup: {filings_deduped.count()} rows")

# Business validation: SST payable should match taxable_sales * rate
filings_validated = (
    filings_deduped
    .withColumn("expected_sst",
        F.round(F.col("taxable_sales") * F.col("sst_rate") / 100, 2))
    .withColumn("sst_variance",
        F.abs(F.col("sst_payable") - F.col("expected_sst")))
    .withColumn("sst_mismatch",
        F.col("sst_variance") > 0.02)  # Allow 2 cent rounding
)

mismatch_count = filings_validated.filter(F.col("sst_mismatch") == True).count()
print(f"SST amount mismatches: {mismatch_count}")

# Flag negative amounts
neg_sales = filings_validated.filter(F.col("gross_sales") < 0).count()
print(f"Negative gross sales: {neg_sales}")

# Add quality flag
filings_silver = (
    filings_validated
    .withColumn("silver_processed_at", F.current_timestamp())
    .withColumn("data_quality_flag",
        F.when(F.col("sst_mismatch") == True, "REVIEW")
         .when(F.col("gross_sales") < 0, "FAIL")
         .otherwise("PASS"))
)

filings_silver.write.mode("overwrite").delta(f"{SILVER_PATH}/sst_filings")
print(f"Silver filings written: {filings_silver.count()} rows")

# Quality summary
print("\n--- Silver Quality Summary ---")
filings_silver.groupBy("data_quality_flag").count().display()

print("\n✅ Silver layer complete.")