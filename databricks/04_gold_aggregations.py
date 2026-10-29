"""
GOLD: Business aggregations for BI serving.
Creates pre-aggregated marts for SST compliance dashboards.
"""
from pyspark.sql import functions as F

SILVER_PATH = "dbfs:/raw/silver"
GOLD_PATH = "dbfs:/raw/gold"

print("=" * 60)
print("GOLD: Business Aggregations")
print("=" * 60)

# Load Silver
filings = spark.read.delta(f"{SILVER_PATH}/sst_filings")
suppliers = spark.read.delta(f"{SILVER_PATH}/suppliers_scd2")

# Filter to current suppliers for joins
suppliers_current = suppliers.filter(F.col("is_current") == True)


# =====================================================
# GOLD 1: SST Liability Summary (by period, state, sector)
# =====================================================
print("\n--- Building: sst_liability_summary ---")

sst_summary = (
    filings
    .groupBy("period", "state", "sector")
    .agg(
        F.count("filing_id").alias("total_filings"),
        F.sum("gross_sales").alias("total_gross_sales"),
        F.sum("taxable_sales").alias("total_taxable_sales"),
        F.sum("sst_payable").alias("total_sst_payable"),
        F.round(
            F.sum("sst_payable") / F.sum("gross_sales") * 100, 2
        ).alias("effective_sst_rate_pct"),
        F.round(
            F.count(F.when(F.col("filing_status") == "Submitted", True))
            / F.count("*") * 100, 2
        ).alias("submission_rate_pct"),
    )
    .withColumn("gold_created_at", F.current_timestamp())
)

sst_summary.write.mode("overwrite").delta(f"{GOLD_PATH}/sst_liability_summary")
print(f"Rows: {sst_summary.count()}")
sst_summary.show(10, truncate=False)


# =====================================================
# GOLD 2: Supplier Compliance Dashboard
# =====================================================
print("\n--- Building: supplier_compliance ---")

supplier_compliance = (
    filings
    .join(
        suppliers_current.select("supplier_id", "name", "state", "sector"),
        on="supplier_id",
        how="left"
    )
    .groupBy("supplier_id", "name", "state", "sector")
    .agg(
        F.count("filing_id").alias("total_filings"),
        F.sum("sst_payable").alias("total_sst_owed"),
        F.max("filing_date").alias("last_filing_date"),
        F.round(
            F.count(F.when(F.col("filing_status") == "Overdue", True))
            / F.count("*") * 100, 2
        ).alias("overdue_filing_pct"),
    )
    .withColumn("compliance_flag",
        F.when(F.col("overdue_filing_pct") > 20, "HIGH_RISK")
         .when(F.col("overdue_filing_pct") > 10, "MEDIUM_RISK")
         .otherwise("COMPLIANT"))
    .withColumn("gold_created_at", F.current_timestamp())
)

supplier_compliance.write.mode("overwrite").delta(f"{GOLD_PATH}/supplier_compliance")
print(f"Rows: {supplier_compliance.count()}")
supplier_compliance.show(10, truncate=False)


# =====================================================
# GOLD 3: Filing Status Trend (by period)
# =====================================================
print("\n--- Building: filing_status_trend ---")

status_trend = (
    filings
    .groupBy("period", "filing_status")
    .agg(F.count("filing_id").alias("filing_count"))
    .pivot("filing_status", ["Submitted", "Pending", "Overdue", "Rejected"])
    .agg(F.count("filing_id"))
    .fillna(0)
    .withColumn("gold_created_at", F.current_timestamp())
)

status_trend.write.mode("overwrite").delta(f"{GOLD_PATH}/filing_status_trend")
print(f"Rows: {status_trend.count()}")
status_trend.show(truncate=False)


# =====================================================
# GOLD 4: Top SST Payers (by state)
# =====================================================
print("\n--- Building: top_sst_payers ---")

top_payers = (
    filings
    .join(suppliers_current.select("supplier_id", "name"), on="supplier_id")
    .groupBy("supplier_id", "name")
    .agg(F.sum("sst_payable").alias("total_sst_paid"))
    .orderBy(F.col("total_sst_paid").desc())
    .limit(20)
    .withColumn("gold_created_at", F.current_timestamp())
)

top_payers.write.mode("overwrite").delta(f"{GOLD_PATH}/top_sst_payers")
print(f"Rows: {top_payers.count()}")
top_payers.show(10, truncate=False)


# =====================================================
# OPTIMIZE + Z-ORDER for query performance
# =====================================================
print("\n--- Optimizing Gold Tables ---")

for table in ["sst_liability_summary", "supplier_compliance", "filing_status_trend"]:
    spark.sql(f"OPTIMIZE {GOLD_PATH}/{table}")
    print(f"Optimized: {table}")

# Z-ORDER on the most queried column
spark.sql(f"OPTIMIZE {GOLD_PATH}/sst_liability_summary ZORDER BY (state)")
print("Z-ORDER applied on sst_liability_summary by state")

print("\n✅ Gold layer complete.")
print(f"Gold tables ready at: {GOLD_PATH}")