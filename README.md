# Payment-Segmented Sales Pipeline

An end-to-end data engineering pipeline that ingests card-transaction sales data segmented by payment type, orchestrates movement with Azure Data Factory, and processes it through a medallion architecture (Bronze/Silver/Gold) in Databricks with Unity Catalog — producing an analytics-ready star schema and a live dashboard.

## Problem Statement

Retail transaction data often arrives split by source (payment provider, region, POS system, etc.) and needs to be reliably consolidated, cleaned, and modeled before it's usable for analytics. This project simulates that scenario: three separate transaction extracts (Visa, Mastercard, American Express) are ingested via a parameterized, fault-tolerant pipeline and merged into a single, query-ready star schema.

## Architecture

```
source-drop/ (3 CSVs by payment type + config.json)
        │
        ▼
Azure Data Factory
  Lookup → ForEach → Copy Data (retry: 3x / 30s)
        │
        ▼
ADLS Gen2: raw-landing/raw/{visa,americanexpress,mastercard}/
        │
        ▼
Databricks: SAS-authenticated HTTPS pull → Pandas → Spark
        │
        ▼
  Bronze  (raw ingest, Delta, metadata columns)
        │
        ▼
  Silver  (cleaned, typed, deduplicated, rejects isolated)
        │
        ▼
  Gold    (star schema: fact_sales + dim_payment/product/customer/date)
          MERGE INTO for incremental upserts, OPTIMIZE + ZORDER
        │
        ▼
Databricks SQL Dashboard (revenue by payment type, top products,
                           monthly trend, loyalty card analysis)
```

## Tech Stack

- **Orchestration:** Azure Data Factory (Lookup, ForEach, parameterized Copy Data, retry policies)
- **Storage:** Azure Data Lake Storage Gen2 (hierarchical namespace)
- **Processing:** Databricks (PySpark), Delta Lake, Unity Catalog
- **Modeling:** Medallion architecture (Bronze/Silver/Gold), dimensional star schema
- **Incremental loading:** `MERGE INTO`, `OPTIMIZE` / `ZORDER`
- **Serving:** Databricks SQL, Lakeview Dashboards
- **Languages:** Python (Pandas, PySpark), SQL, JSON (pipeline config)

## Dataset

~7,246 transaction records split across three payment-type extracts, with columns: `transaction_id`, `transactional_date`, `product_id`, `customer_id`, `payment`, `loyalty_card`, `cost`, `quantity`, `price`.

## Key Engineering Decisions & Problems Solved

**1. Unity Catalog serverless blocks native Azure auth**
Databricks Free Edition runs on AWS infrastructure. Unity Catalog serverless compute blocks cluster-level Spark configuration injection (`spark.conf.set`), which is the standard method for mounting Azure Blob/ADLS via SAS token. Rather than abandon Azure connectivity, this was solved by authenticating over plain HTTPS (`requests` + SAS token) to pull blobs directly, then loading into Spark via Pandas — a constraint-driven design decision, documented rather than hidden.

**2. Parameterized, fault-tolerant ingestion in ADF**
Instead of three hardcoded Copy activities, a single parameterized pipeline reads a `config.json` manifest via Lookup, then fans out through ForEach — each iteration dynamically resolving source filename and sink folder path. Retry policies (3 attempts, 30s interval) are applied at the Copy activity level to handle transient failures.

**3. Data quality isolation in Silver**
Rows failing null checks on `cost`/`price` are not silently dropped — they're written to a separate `silver.retail_sales_rejects` table for auditability, a pattern used in production data quality frameworks.

**4. Debugging real pipeline failures**
- Fixed a doubled-path bug (`file.csv/file.csv`) caused by a stale folder path left over from dataset creation via "browse to sample file"
- Fixed sink files landing in the container root instead of the intended `raw/` subfolder due to a missing path prefix in dynamic content
- Handled non-standard date format (`dd-MM-yyyy HH:mm`) and string-based boolean flags (`T`/`F`) during Silver transformation

## Results

- 7,246 raw rows ingested → cleaned and deduplicated in Silver → modeled into a 4-dimension star schema in Gold
- Dashboard surfacing revenue by payment type, top products, monthly trend, and loyalty card impact on order value

## Repo Structure

```
/adf/          — Exported ADF pipeline (ARM template JSON)
/databricks/   — Bronze, Silver, Gold PySpark notebooks
/docs/         — Architecture diagram, dashboard screenshots
```

## Possible Extensions

- Add Databricks Autoloader for streaming ingestion alongside batch
- Parameterize the pipeline further to support arbitrary N payment types without config changes
- Add Great Expectations or dbt tests for automated data quality checks
