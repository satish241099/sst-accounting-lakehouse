# Interview Talking Points — SST Accounting Lakehouse

## The 2-Minute Pitch

*"I built a production-grade SST compliance data lakehouse on Azure Databricks. It ingests monthly SST-03 filing CSVs, processes them through a medallion architecture — Bronze, Silver, Gold — with Delta Lake for ACID transactions and time travel. The Silver layer implements SCD2 for supplier master data to track changes over time, which is critical for SST audits. I orchestrated it with Databricks Workflows, with three dependent tasks passing task values between stages. The Gold layer has four pre-aggregated marts for BI dashboards: SST liability by period, supplier compliance scoring, filing status trends, and top SST payers."*

**Pause here.** Let them ask follow-ups.

---

## Common Follow-Up Questions & Answers

### Q1: "Why Delta Lake instead of plain Parquet?"

**Answer:**
*"Three reasons:*
1. *ACID transactions — I can do MERGE upserts safely without corrupting data*
2. *Time travel — for SST compliance, I can query any historical version. I demonstrated this in the Bronze layer: overwrite the table, then query `versionAsOf = 0` to see the original data*
3. *OPTIMIZE and Z-ORDER — I applied Z-ORDER on the `state` column in the Gold layer, which skips files for state-filtered queries. This is huge for query performance."*

---

### Q2: "Explain SCD2. How did you implement it?"

**Answer:**
*"SCD2 — Slowly Changing Dimension Type 2 — tracks history by adding `valid_from`, `valid_to`, and `is_current` columns. When a supplier's sector or state changes:*
1. *I expire the old record: set `valid_to = today`, `is_current = False`*
2. *Insert a new record with `valid_from = today`, `valid_to = NULL`, `is_current = True`*

*I used a SQL MERGE statement in Databricks to do this atomically. The interview artifact shows the exact code. This matters for SST audits — you need to know which sector a supplier was in for a given filing period."*

---

### Q3: "How did you handle data quality?"

**Answer:**
*"Three layers:*
1. *Schema enforcement in Bronze — explicit StructType, no inference. Bad types fail fast.*
2. *Validation in Silver — I check for null IDs, invalid SST number formats, SST payable mismatches (computed vs actual). Rows get a `data_quality_flag`: PASS, REVIEW, or FAIL.*
3. *Quarantine pattern — I flag bad rows but don't drop them. They're still queryable for debugging.*

*In production, I'd add Great Expectations or DLT expectations, but for this portfolio I wanted to show the core pattern."*

---

### Q4: "What about deduplication?"

**Answer:**
*"I used window functions with `row_number()`. For suppliers, I partition by `supplier_id` and order by `updated_date DESC`, then keep only `rn = 1`. For SST filings, `dropDuplicates(['filing_id'])` was enough since filing_id is the primary key.*

*The key insight: deduplicate early, in Silver, before aggregations. Otherwise your Gold marts have double-counted revenue."*

---

### Q5: "Why Databricks Workflows instead of Airflow?"

**Answer:**
*"For this use case, Workflows is simpler — native Databricks integration, task values pass between stages with `dbutils.jobs.taskValues`, and I don't need to manage an Airflow cluster. That said, if I needed multi-cloud orchestration or complex DAGs with branching, I'd reach for Airflow.*

*The pattern is the same either way: Bronze → Silver → Gold as dependent tasks, with email notifications on failure."*

---

### Q6: "How would you scale this to production?"

**Answer:**
*"Four changes:*
1. *Unity Catalog — row filters and column masks for PDPA compliance*
2. *Multi-worker cluster — autoscaling 2-8 workers based on data volume*
3. *Incremental loads — use Auto Loader for file ingestion, MERGE for CDC instead of full overwrites*
4. *CI/CD — Terraform for infra, Databricks Asset Bundles for notebook deployment, GitHub Actions for testing*

*The architecture stays the same; I'd just add governance and automation."*

---

### Q7: "What was the hardest part?"

**Answer:**
*"Getting SCD2 right with Delta Lake. The challenge is: you can't UPDATE individual rows in Delta without a MERGE, and the MERGE condition has to be exact. I had to think through:*
- *What's the key? (supplier_id)*
- *What triggers a new version? (sector or state change)*
- *How do I avoid duplicate current records?*

*I solved it by first expiring old records, then inserting new ones in a single transaction. The code is in `03_silver_scd2.py`."*

---

### Q8: "How do you monitor this in production?"

**Answer:**
*"Three things:*
1. *Databricks Workflows UI — shows task status, duration, errors*
2. *Task values — I set row counts at each stage, so I can detect data volume anomalies*
3. *Delta log — I can query `DESCRIBE HISTORY` on any table to see who wrote what and when*

*For SST compliance, I'd also add a daily email with the filing_status_trend pivot — stakeholders need to know how many filings are overdue."*

---

### Q9: "What's your cost estimate?"

**Answer:**
*"Free Edition: ~4-6 DBUs/month, 1 worker, no Unity Catalog.*

*Production (my architecture doc): ~$515/month:*
- *Databricks DBUs (4 workers, 8h/day): $400*
- *ADLS Gen2 Storage (500 GB): $15*
- *Databricks SQL Warehouse (small): $100*

*Cost optimization: job clusters are 40% cheaper than all-purpose. I'd only run them during scheduled jobs."*

---

### Q10: "How does this relate to PDPA compliance?"

**Answer:**
*"PDPA requires data minimization and access control. In this project:*
- *PII masking — supplier names are hashed in Silver (SHA-256), but still joinable*
- *Time travel retention — 30 days, then VACUUM removes old versions*
- *Access control — Gold tables are read-only for BI users; only data engineers write to Bronze/Silver*

*In production with Unity Catalog, I'd add row filters (analysts only see their state) and column masks (hide SST numbers unless authorized)."*

---

## The Demo Script (5 minutes)

When they say *"Walk me through your project"*:

1. **Open the README.md** — show the architecture diagram (1 min)
2. **Show the data generator** — *"I generate realistic SST data with Faker"* (30 sec)
3. **Open `01_bronze_ingest.py`** — *"Explicit schemas, Delta write, time travel demo"* (1 min)
4. **Open `03_silver_scd2.py`** — *"SCD2 implementation with MERGE"* (1 min)
5. **Open `04_gold_aggregations.py`** — *"Four BI marts, OPTIMIZE + Z-ORDER"* (1 min)
6. **Open `docs/architecture.md`** — *"Cost sheet, design decisions"* (30 sec)

**Close with:** *"This is a portfolio project, but it's production-grade. I can adapt it to your specific stack — whether that's AWS Glue, GCP Dataflow, or Snowflake with dbt."*

---

## Red Flags to Avoid

❌ Don't say: *"I just followed a tutorial"*
✅ Say: *"I designed this to demonstrate production patterns"*

❌ Don't say: *"I haven't tested it"*
✅ Say: *"I validated the data quality checks — here are the results"*

❌ Don't say: *"This would work in production"*
✅ Say: *"For production, I'd add Unity Catalog, CI/CD, and incremental loads — here's how"*

---

## Questions to Ask THEM

After your demo, ask:

1. *"What's your current orchestration tool? Airflow, Databricks Workflows, something else?"*
2. *"Do you use medallion architecture or a different layering strategy?"*
3. *"How do you handle SCD2 in your warehouse?"*
4. *"What's your biggest data quality challenge right now?"*

This shows you're thinking about their problems, not just reciting a script.