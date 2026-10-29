"""
SILVER: SCD2 for supplier master data.
Tracks supplier changes over time with valid_from / valid_to columns.
This is the pattern interviewers ask about for CDC and slowly changing dimensions.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType

SILVER_PATH = "dbfs:/raw/silver"

print("=" * 60)
print("SILVER: SCD2 Supplier History")
print("=" * 60)

suppliers = spark.read.delta(f"{SILVER_PATH}/suppliers")
print(f"Current supplier rows: {suppliers.count()}")

# SCD2 implementation using MERGE
# Strategy: 
# 1. Mark existing records as expired (valid_to = today)
# 2. Insert new records with valid_from = today
# 3. Handle updates by expiring old + inserting new

# Add SCD2 columns if not present
if "valid_from" not in suppliers.columns:
    suppliers = (
        suppliers
        .withColumn("valid_from", F.current_date())
        .withColumn("valid_to", F.cast(F.lit(None), DateType()))
        .withColumn("is_current", F.lit(True))
    )

# Simulate updates: change 5 suppliers' sector and state
updated_suppliers = (
    suppliers
    .filter(F.col("is_current") == True)
    .withColumn("sector", 
        F.when(F.col("supplier_id") % 40 == 0, F.lit("E-commerce"))
         .otherwise(F.col("sector")))
    .withColumn("state",
        F.when(F.col("supplier_id") % 40 == 0, F.lit("Sabah"))
         .otherwise(F.col("state")))
    .withColumn("updated_date", F.current_date())
)

# Identify which suppliers actually changed
changed = updated_suppliers.filter(
    (F.col("sector") != suppliers.select("sector").first()) |
    (F.col("state") != suppliers.select("state").first())
)

# Step 1: Expire old records (set valid_to = today, is_current = False)
# This is done via MERGE
suppliers.createOrReplaceTempView("suppliers_current")
changed.createOrReplaceTempView("suppliers_changed")

# Use SQL MERGE for SCD2 (interview favorite)
spark.sql("""
    MERGE INTO suppliers_current AS target
    USING suppliers_changed AS source
    ON target.supplier_id = source.supplier_id
       AND target.is_current = True
    WHEN MATCHED AND (
        target.sector != source.sector OR 
        target.state != source.state
    ) THEN
        UPDATE SET
            target.valid_to = current_date(),
            target.is_current = False
""")

# Step 2: Insert new versions
spark.sql("""
    INSERT INTO suppliers_current
    SELECT
        supplier_id, name, sst_number, sector, state,
        registration_type, is_active, created_date,
        current_date() AS updated_date,
        current_date() AS valid_from,
        CAST(NULL AS DATE) AS valid_to,
        TRUE AS is_current
    FROM suppliers_changed
    WHERE supplier_id IN (
        SELECT DISTINCT supplier_id FROM suppliers_current 
        WHERE valid_to = current_date()
    )
""")

# Write SCD2 table
result = spark.sql("SELECT * FROM suppliers_current")
result.write.mode("overwrite").delta(f"{SILVER_PATH}/suppliers_scd2")

# Verify SCD2 worked
total_history = result.count()
current_count = result.filter(F.col("is_current") == True).count()
historical_count = result.filter(F.col("is_current") == False).count()

print(f"\nSCD2 Results:")
print(f"  Total history records: {total_history}")
print(f"  Current records: {current_count}")
print(f"  Historical records: {historical_count}")

# Show a supplier with history
print("\nSample supplier history (supplier_id = SUP-00001):")
spark.sql("""
    SELECT supplier_id, sector, state, valid_from, valid_to, is_current
    FROM suppliers_current
    WHERE supplier_id = 'SUP-00001'
    ORDER BY valid_from
""").display()

print("\n✅ SCD2 complete — supplier history tracked.")