# Data Dictionary — SST Accounting Lakehouse

## Bronze Layer (Raw)

### `bronze.suppliers`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| supplier_id | STRING | No | Primary key, format: SUP-NNNNN |
| name | STRING | No | Supplier company name |
| sst_number | STRING | Yes | SST registration number |
| sector | STRING | Yes | Business sector |
| state | STRING | Yes | Malaysian state |
| registration_type | STRING | Yes | Standard / Small / Not Applicable |
| is_active | BOOLEAN | Yes | Active registration flag |
| created_date | DATE | Yes | Registration date |
| updated_date | DATE | Yes | Last modification date |
| email | STRING | Yes | Contact email |
| phone | STRING | Yes | Contact phone |

### `bronze.sst_filings`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| filing_id | STRING | No | Primary key, format: FIL-NNNNNN |
| supplier_id | STRING | No | Foreign key to suppliers |
| filing_date | DATE | No | Filing submission date |
| period | STRING | No | Filing period, format: YYYY-QN |
| state | STRING | No | Filing state |
| sector | STRING | No | Business sector |
| gross_sales | DOUBLE | No | Total sales amount (RM) |
| taxable_sales | DOUBLE | No | SST-taxable sales (RM) |
| sst_rate | DOUBLE | No | Applicable SST rate (0, 5, or 6%) |
| sst_payable | DOUBLE | No | SST amount payable (RM) |
| filing_status | STRING | No | Submitted / Pending / Overdue / Reject |
| submission_reference | STRING | No | Unique submission reference |

## Silver Layer (Cleaned)

### `silver.suppliers`
Same columns as bronze, plus:
| Column | Type | Description |
|--------|------|-------------|
| silver_processed_at | TIMESTAMP | Processing timestamp |
| data_quality_flag | STRING | PASS / FAIL / REVIEW |

### `silver.suppliers_scd2`
Same as silver.suppliers, plus SCD2 columns:
| Column | Type | Description |
|--------|------|-------------|
| valid_from | DATE | When this version became effective |
| valid_to | DATE | When this version was superseded (NULL = current) |
| is_current | BOOLEAN | True for the latest version |

### `silver.sst_filings`
Same as bronze, plus:
| Column | Type | Description |
|--------|------|-------------|
| expected_sst | DOUBLE | Computed: taxable_sales * sst_rate / 100 |
| sst_variance | DOUBLE | Absolute difference between payable and expected |
| sst_mismatch | BOOLEAN | True if variance > 0.02 |
| silver_processed_at | TIMESTAMP | Processing timestamp |
| data_quality_flag | STRING | PASS / REVIEW / FAIL |

## Gold Layer (BI Marts)

### `gold.sst_liability_summary`
| Column | Type | Description |
|--------|------|-------------|
| period | STRING | Filing period (YYYY-QN) |
| state | STRING | Malaysian state |
| sector | STRING | Business sector |
| total_filings | LONG | Count of filings |
| total_gross_sales | DOUBLE | Sum of gross sales (RM) |
| total_taxable_sales | DOUBLE | Sum of taxable sales (RM) |
| total_sst_payable | DOUBLE | Sum of SST payable (RM) |
| effective_sst_rate_pct | DOUBLE | Weighted effective SST rate |
| submission_rate_pct | DOUBLE | Percentage of filings submitted |
| gold_created_at | TIMESTAMP | Processing timestamp |

### `gold.supplier_compliance`
| Column | Type | Description |
|--------|------|-------------|
| supplier_id | STRING | Supplier identifier |
| name | STRING | Supplier name |
| state | STRING | Supplier state |
| sector | STRING | Supplier sector |
| total_filings | LONG | Number of filings |
| total_sst_owed | DOUBLE | Total SST owed (RM) |
| last_filing_date | DATE | Most recent filing |
| overdue_filing_pct | DOUBLE | Percentage of overdue filings |
| compliance_flag | STRING | COMPLIANT / MEDIUM_RISK / HIGH_RISK |
| gold_created_at | TIMESTAMP | Processing timestamp |

### `gold.filing_status_trend`
| Column | Type | Description |
|--------|------|-------------|
| period | STRING | Filing period |
| Submitted | BIGINT | Count submitted |
| Pending | BIGINT | Count pending |
| Overdue | BIGINT | Count overdue |
| Rejected | BIGINT | Count rejected |
| gold_created_at | TIMESTAMP | Processing timestamp |

### `gold.top_sst_payers`
| Column | Type | Description |
|--------|------|-------------|
| supplier_id | STRING | Supplier identifier |
| name | STRING | Supplier name |
| total_sst_paid | DOUBLE | Total SST paid (RM) |
| gold_created_at | TIMESTAMP | Processing timestamp |

## PII Masking

Supplier names in Silver are masked:
```
original: "ABC Manufacturing Sdn Bhd"
masked:   "AB***<sha256_hash>"
```
This allows join operations while protecting PII (PDPA compliance).