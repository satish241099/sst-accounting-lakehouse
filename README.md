# SST Accounting Data Lakehouse

**A production-style medallion data lakehouse for Malaysian SST compliance analytics, built on Azure Databricks.**

> **What this proves:** End-to-end data engineering on Azure — from raw SST filings to
> BI-ready gold tables — with Delta Lake, medallion architecture, Databricks
> workflows, and interview-grade documentation.

---

## Architecture

```mermaid
flowchart LR
    subgraph Sources["Sources"]
        A1["CSV SST filings<br/>(monthly returns)"]
        A2["JSON supplier data<br/>(master data)"]
    end

    subgraph Landing["Landing"]
        S3[(\"ADLS Gen2<br/>raw/bronze/\")]
    end

    subgraph Processing["Databricks Medallion"]
        B["Bronze<br/>raw + schema cast"]
        S["Silver<br/>cleaned, validated, deduped"]
        G["Gold<br/>aggregated for BI"]
    end

    subgraph Serving["Serving"]
        SQL["Databricks SQL<br/>dashboards"]
        DOC["Architecture doc<br/>+ cost sheet"]
    end

    A1 --> S3
    A2 --> S3
    S3 --> B
    B --> S
    S --> G
    G --> SQL
    G --> DOC
```

## Data Flow

1. **Ingestion** — Monthly SST-03 filing CSVs and supplier master data land in ADLS Gen2 (`dbfs:/raw/bronze/`)
2. **Bronze** — Raw Delta tables with schema enforcement, no transformations
3. **Silver** — Cleaned, validated, deduplicated. SCD2 for supplier changes. Data quality gates.
4. **Gold** — Pre-aggregated marts: SST liability by period, supplier compliance, filing status dashboard
5. **Serving** — Databricks SQL queries for dashboards; architecture doc + cost sheet as interview artifacts

---

## Repository Structure

```
sst-accounting-lakehouse/
├── README.md
├── data/
│   └── generate_sst_data.py      # Generate realistic SST data
├── databricks/
│   ├── 01_bronze_ingest.py       # Bronze: raw → Delta
│   ├── 02_silver_clean.py        # Silver: validation + dedup
│   ├── 03_silver_scd2.py        # Silver: SCD2 for suppliers
│   ├── 04_gold_aggregations.py   # Gold: BI marts
│   └── 05_workflow_demo.py       # Databricks Workflows demo
├── terraform/
│   └── databricks_resources.tf   # IaC for clusters, jobs, UC
├── docs/
│   ├── architecture.md           # Your interview artifact
│   ├── data_dictionary.md        # Schema reference
│   └── interview_talking_points.md # What to say in interviews
└── notebooks/                    # Databricks notebook exports
```

---

## Quick Start

### 1. Generate sample data

```bash
pip install faker
python data/generate_sst_data.py --rows 5000
```

### 2. Upload to Databricks

```bash
# Upload CSVs to DBFS
dbfs cp data/raw/sst_filings.csv dbfs:/raw/bronze/sst_filings/
dbfs cp data/raw/suppliers.csv dbfs:/raw/bronze/suppliers/
```

### 3. Run the pipeline

In your Databricks notebook, run each file in order:
1. `01_bronze_ingest.py`
2. `02_silver_clean.py`
3. `03_silver_scd2.py`
4. `04_gold_aggregations.py`

### 4. Query the results

```sql
-- SST liability by state and period
SELECT * FROM gold.sst_liability_summary;

-- Supplier compliance dashboard
SELECT * FROM gold.supplier_compliance;
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Cloud | Azure (Databricks Workspace) |
| Storage | ADLS Gen2 / DBFS |
| Processing | PySpark, Delta Lake |
| Architecture | Medallion (Bronze → Silver → Gold) |
| Orchestration | Databricks Workflows |
| BI | Databricks SQL |
| IaC | Terraform |
| Documentation | Markdown + Mermaid diagrams |

---

## Key Features

| Feature | Implementation |
|---------|---------------|
| **Medallion architecture** | Clear contracts between bronze/silver/gold |
| **Delta Lake** | ACID transactions, time travel, OPTIMIZE, Z-ORDER |
| **Data quality** | Schema enforcement, null checks, quarantine pattern |
| **SCD2** | Supplier history tracking with `valid_from` / `valid_to` |
| **Incremental MERGE** | CDC-style upserts on silver and gold |
| **Databricks Workflows** | Multi-task job with task values and error handling |
| **Interview artifacts** | Architecture doc, cost sheet, data dictionary |
