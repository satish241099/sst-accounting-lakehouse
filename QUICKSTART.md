# Quick Start — SST Accounting Lakehouse

Get this running in your Databricks workspace in 30 minutes.

---

## Step 1: Upload Data to Databricks

In your Databricks workspace:

```python
# Run this in a Databricks notebook cell
dbutils.fs.cp(
  "file:///C:/Users/satis/projects/sst-accounting-lakehouse/data/raw/suppliers.csv",
  "dbfs:/raw/bronze/suppliers.csv",
  True
)
dbutils.fs.cp(
  "file:///C:/Users/satis/projects/sst-accounting-lakehouse/data/raw/sst_filings.csv",
  "dbfs:/raw/bronze/sst_filings.csv",
  True
)
```

Or use the Databricks CLI:
```bash
databricks fs cp data/raw/suppliers.csv dbfs:/raw/bronze/suppliers.csv
databricks fs cp data/raw/sst_filings.csv dbfs:/raw/bronze/sst_filings.csv
```

---

## Step 2: Run the Pipeline

Create a notebook in Databricks and paste each file in order:

### Bronze (Ingestion)
```python
# Paste contents of 01_bronze_ingest.py
```
Creates `dbfs:/raw/bronze/bronze/suppliers` and `dbfs:/raw/bronze/bronze/sst_filings`

### Silver (Cleaning + SCD2)
```python
# Paste contents of 02_silver_clean.py
# Then 03_silver_scd2.py
```
Creates `dbfs:/raw/silver/suppliers`, `dbfs:/raw/silver/sst_filings`, `dbfs:/raw/silver/suppliers_scd2`

### Gold (Aggregations)
```python
# Paste contents of 04_gold_aggregations.py
```
Creates 4 BI marts in `dbfs:/raw/gold/`

---

## Step 3: Query the Results

```sql
-- SST liability by state and period
SELECT * FROM delta.`dbfs:/raw/gold/sst_liability_summary`
ORDER BY period DESC, total_sst_payable DESC;

-- Supplier compliance risk
SELECT * FROM delta.`dbfs:/raw/gold/supplier_compliance`
WHERE compliance_flag != 'COMPLIANT'
ORDER BY overdue_filing_pct DESC;

-- Filing status trend
SELECT * FROM delta.`dbfs:/raw/gold/filing_status_trend`
ORDER BY period;
```

---

## Step 4: Interview Demo

Open `docs/interview_talking_points.md` for the exact narrative. The README has the architecture diagram. The data dictionary has every column documented.

---

## Project Structure

```
sst-accounting-lakehouse/
├── README.md                    # Architecture + overview
├── data/
│   └── generate_sst_data.py     # Generate realistic SST data
├── databricks/
│   ├── 01_bronze_ingest.py      # Bronze: raw → Delta
│   ├── 02_silver_clean.py       # Silver: validation + dedup
│   ├── 03_silver_scd2.py        # Silver: SCD2 for suppliers
│   ├── 04_gold_aggregations.py  # Gold: BI marts
│   └── 05_workflow_demo.py      # Databricks Workflows demo
├── terraform/
│   └── databricks_resources.tf  # IaC for clusters, jobs, UC
└── docs/
    ├── architecture.md           # Interview artifact
    ├── data_dictionary.md        # Schema reference
    └── interview_talking_points.md # What to say in interviews
```

---

## What This Demonstrates

| Skill | Where It's Shown |
|-------|-----------------|
| Medallion architecture | Bronze → Silver → Gold in `01`–`04` |
| Delta Lake | All writes, time travel in `01_bronze_ingest.py` |
| SCD2 | `03_silver_scd2.py` with MERGE |
| Data quality | Validation + quarantine in `02_silver_clean.py` |
| Databricks Workflows | `05_workflow_demo.py` + Terraform |
| IaC | `terraform/databricks_resources.tf` |
| BI serving | Gold marts queryable via Databricks SQL |
| Domain knowledge | Malaysian SST, PDPA, accounting |
| Interview artifacts | Architecture doc, data dictionary, talking points |

---

## Next Steps

1. **Run the pipeline** — upload data, execute notebooks, verify output
2. **Record a screen recording** — 5-minute walkthrough of the architecture
3. **Push to GitHub** — this becomes your portfolio
4. **Customize for Upwork** — swap SST domain for your client's industry