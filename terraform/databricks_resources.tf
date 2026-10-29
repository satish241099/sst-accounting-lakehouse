# Terraform configuration for SST Accounting Lakehouse
# Deploys Databricks clusters, jobs, and storage

terraform {
  required_version = ">= 1.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.0"
    }
  }
}

# --- Cluster for ETL workloads ---
resource "databricks_cluster" "etl_cluster" {
  name            = "sst-etl-cluster"
  spark_version   = "14.3.x-ml-gpu-ml-scala2.12"
  node_type_id    = "Standard_DS3_v2"
  num_workers     = 1  # Free Edition: 1 worker max
  autoscale {
    min_workers = 1
    max_workers = 1
  }
  auto_termination_minutes = 60
  spark_env_vars = {
    PYSPARK_PYTHON = "/databricks/python3/bin/python3"
  }
  spark_conf {
    key   = "spark.sql.shuffle.partitions"
    value = "200"
  }
  spark_conf {
    key   = "spark.databricks.delta.retentionDurationCheck.enabled"
    value = "true"
  }
}

# --- Databricks Workflow (Job) ---
resource "databricks_job" "sst_pipeline" {
  name = "sst-accounting-pipeline"

  run_now = false

  task {
    task_key = "bronze_ingest"
    description = "Ingest raw SST filings to Bronze Delta"
    notebook_task {
      notebook_path = "/projects/sst-accounting-lakehouse/01_bronze_ingest.py"
      base_parameters = {
        raw_path = "dbfs:/raw/bronze"
      }
    }
    cluster_id = databricks_cluster.etl_cluster.cluster_id
    timeout_seconds = 3600
    email_notifications {
      on_failure = ["data-engineer@company.com"]
    }
  }

  task {
    task_key = "silver_processing"
    description = "Clean, validate, dedup to Silver Delta"
    depends_on {
      task_key = "bronze_ingest"
    }
    notebook_task {
      notebook_path = "/projects/sst-accounting-lakehouse/02_silver_clean.py"
      base_parameters = {
        bronze_path = "dbfs:/raw/bronze/bronze"
        silver_path = "dbfs:/raw/silver"
      }
    }
    cluster_id = databricks_cluster.etl_cluster.cluster_id
    timeout_seconds = 3600
    email_notifications {
      on_failure = ["data-engineer@company.com"]
    }
  }

  task {
    task_key = "gold_aggregations"
    description = "Build BI-ready Gold marts"
    depends_on {
      task_key = "silver_processing"
    }
    notebook_task {
      notebook_path = "/projects/sst-accounting-lakehouse/04_gold_aggregations.py"
      base_parameters = {
        silver_path = "dbfs:/raw/silver"
        gold_path   = "dbfs:/raw/gold"
      }
    }
    cluster_id = databricks_cluster.etl_cluster.cluster_id
    timeout_seconds = 3600
    email_notifications {
      on_failure = ["data-engineer@company.com"]
    }
  }
}

# --- Schedule ---
resource "databricks_schedule" "sst_pipeline_schedule" {
  job_id        = databricks_job.sst_pipeline.job_id
  quartz_cron_expression = "0 2 * * *"  # Daily at 2 AM
  timezone_id   = "Asia/Kuala_Louang"
  paused        = false
}

output "cluster_id" {
  value = databricks_cluster.etl_cluster.cluster_id
}

output "job_id" {
  value = databricks_job.sst_pipeline.job_id
}

output "job_url" {
  value = "https://<workspace>.cloud.databricks.com/jobs/${databricks_job.sst_pipeline.job_id}"
}