"""
DEMO: Databricks Workflows — multi-task job with dependencies.
This shows interviewers you know production orchestration beyond notebooks.

In a real interview, you'd say:
"I orchestrated this as a Databricks Workflow with three dependent tasks:
bronze ingest → silver processing → gold aggregations, with task values
passed between stages and email notification on failure."
"""
from pyspark.sql import functions as F

print("=" * 60)
print("DATABRICKS WORKFLOWS DEMO")
print("=" * 60)

# Simulate what a Workflow task does
# In production, each of these would be a separate task in a Workflow

TASKS = [
    {
        "name": "bronze_ingest",
        "notebook": "01_bronze_ingest.py",
        "depends_on": [],
    },
    {
        "name": "silver_processing",
        "notebook": "02_silver_clean.py",
        "depends_on": ["bronze_ingest"],
    },
    {
        "name": "gold_aggregations",
        "notebook": "04_gold_aggregations.py",
        "depends_on": ["silver_processing"],
    },
]

print("\nWorkflow DAG:")
for task in TASKS:
    deps = " -> ".join(task["depends_on"]) if task["depends_on"] else "(START)"
    print(f"  [{deps}] -> {task['name']} ({task['notebook']})")

# Demonstrate task values (passing data between tasks)
print("\n--- Task Values Demo ---")
# In a real workflow, you'd use dbutils.jobs.taskValues.set()
# Here's what it looks like:

bronze_row_count = 3000  # Would come from actual bronze ingest
print(f"Setting task value: bronze_rows = {bronze_row_count}")
# dbutils.jobs.taskValues.set("bronze_rows", bronze_row_count)

silver_row_count = 2950  # After dedup
print(f"Setting task value: silver_rows = {silver_row_count}")
# dbutils.jobs.taskValues.set("silver_rows", silver_row_count)

gold_row_count = 150  # After aggregation
print(f"Setting task value: gold_rows = {gold_row_count}")
# dbutils.jobs.taskValues.set("gold_rows", gold_row_count)

# Demonstrate reading task values (what the next task would do)
print("\n--- Reading Task Values (next task) ---")
# In the silver task, you'd read:
# bronze_rows = dbutils.jobs.taskValues.get("bronze_rows")
print(f"Silver task reads bronze_rows = {bronze_row_count}")
print(f"Silver task computes dedup rate = {(1 - silver_row_count/bronze_row_count)*100:.1f}%")

# Error handling pattern
print("\n--- Error Handling Pattern ---")
print("""
# In a production workflow:
# 1. Each task wraps its logic in try/except
# 2. On failure, set a task value: dbutils.jobs.taskValues.set("status", "FAILED")
# 3. Final task reads status values and sends email notification
# 4. Workflow has: On failure -> send notification email

# Example:
try:
    # Process data
    df = spark.read.delta("...")
    df.write.mode("overwrite").delta("...")
    dbutils.jobs.taskValues.set("status", "SUCCESS")
except Exception as e:
    dbutils.jobs.taskValues.set("status", "FAILED")
    dbutils.jobs.taskValues.set("error", str(e))
    raise e
""")

# Cost optimization note
print("\n--- Cost Optimization ---")
print("""
Job clusters vs all-purpose:
- Job cluster: ephemeral, cheaper DBU rate, only runs during job
- All-purpose: always-on, interactive, expensive

On Free Edition: 1 worker cluster, ~4-6 DBUs/month
Production recommendation: job clusters for scheduled runs,
all-purpose for interactive development only.
""")

print("\n✅ Workflow demo complete.")
print("To run in production: create a Workflow in Databricks UI")
print("  with 3 tasks, dependencies as shown above.")