# Tecsys Data Engineering Project

A production-style local data engineering pipeline that transforms raw customer, product, order, delivery, and return data into analytics-ready datasets. It demonstrates layered processing, data quality checks, quarantine handling, reconciliation, and Gold outputs for BI reporting.

## Project Goals

The pipeline supports analysis of:
- Daily sales and order trends
- Customer purchasing behavior
- Product performance
- Delivery performance
- Returns and return reasons
- Revenue by product, category, and geography where supported by source fields

## Architecture

```text
Raw CSV Files
     |
     v
Ingestion
     |
     v
Bronze Layer (Parquet)
     |
     v
Data Quality Checks and Transformations
     |                         |
     v                         v
Silver Layer              Quarantine
(cleaned data)             (records to review)
     |
     v
Gold Layer (analytics-ready datasets)
     |
     v
SQL / BI Reporting
```

The project uses a Medallion-style architecture:
- **Bronze:** ingested source data
- **Silver:** validated and cleaned data
- **Quarantine:** records that fail defined quality rules
- **Gold:** analytics-ready datasets for reporting

## Technology Stack

- Python
- pandas
- Parquet
- SQL / PostgreSQL where configured in the local workflow
- Power BI for visualization
- Git and GitHub for version control

## Source Data

The local pipeline processes six CSV datasets:

| Dataset | Source rows |
|---|---:|
| `customers.csv` | 10,005 |
| `products.csv` | 1,002 |
| `orders.csv` | 50,005 |
| `order_items.csv` | 100,000 |
| `deliveries.csv` | 45,000 |
| `returns.csv` | 5,000 |
| **Total** | **211,012** |

These are the recorded source counts for the current project run.

## Data Quality and Reconciliation

The pipeline checks for duplicate identifiers, missing required values, invalid values, referential-integrity issues, and date inconsistencies. Records failing validation are quarantined for review.

| Dataset | Bronze | Silver | Quarantine | Deduplicated |
|---|---:|---:|---:|---:|
| Customers | 10,005 | 10,000 | 0 | 5 |
| Products | 1,002 | 998 | 2 | 2 |
| Orders | 50,005 | 49,993 | 12 | 0 |
| Order items | 100,000 | 99,781 | 219 | 0 |
| Deliveries | 45,000 | 44,988 | 12 | 0 |
| Returns | 5,000 | 4,992 | 8 | 0 |
| **Total** | **211,012** | **209,754** | **253** | **7** |

Reconciliation rule:

```text
Bronze rows = Silver rows + Quarantine rows + Deduplicated rows
```

The recorded result is **6 of 6 datasets reconciled successfully**. Quarantined records are quality exceptions for review and do not, by themselves, mean the pipeline failed.

## Gold Datasets

| Gold dataset | Rows | Purpose |
|---|---:|---|
| `gold_daily_sales` | 273 | Daily delivered-order sales, order-item counts, and units sold |
| `gold_product_performance` | 998 | Product-level units sold and net sales |
| `gold_delivery_performance` | 1 | Overall delivery timeliness and duration metrics |
| `gold_customer_analytics` | 10,000 | Customer order activity, units purchased, net sales, average order value, and sales rank |
| `gold_returns_analytics` | 5 | Return counts, affected orders, and returned quantities by return reason |

Recorded headline metrics:
- **Delivered orders:** 28,601
- **Matched delivered order-item rows:** 57,079
- **Units sold:** 170,937
- **Eligible deliveries:** 32,286
- **On-time deliveries:** 10,668
- **Late deliveries:** 21,618
- **On-time delivery rate:** 33.04%
- **Average delivery duration:** 6 days
- **Return records:** 4,992
- **Returned quantity:** 9,953

Small revenue differences have been observed between some Gold outputs. Revenue reconciliation remains an open verification item until the calculation and rounding differences are investigated.

## Run Locally

Run commands from the project root in Windows PowerShell.

1. Clone the repository:

   ```powershell
   git clone https://github.com/LeelaPrasad2710/tecsys_python_pg_bi.git
   cd tecsys_python_pg_bi
   ```

2. Create and activate a virtual environment if needed:

   ```powershell
   py -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

3. Install dependencies using the project's dependency file, if present. For a `requirements.txt` file:

   ```powershell
   python -m pip install -r requirements.txt
   ```

4. Place the source CSV files in the expected `data/raw/` locations.

5. Run the pipeline:

   ```powershell
   python .\run_pipeline.py
   ```

If the virtual environment already exists:

```powershell
.\.venv\Scripts\python.exe .\run_pipeline.py
```

The pipeline writes outputs to configured local data-lake paths. Refer to the scripts and configuration for exact output locations.

## Repository Layout

Conceptual layout (the exact directory contents may evolve):

```text
.
├── data/
│   └── raw/                  # Source CSV files, if included locally
├── src/
│   ├── ingestion/            # Ingestion scripts
│   └── quality/              # Quality checks and Gold builders
├── data_lake/                # Generated outputs; should not be committed
├── run_pipeline.py           # Local pipeline entry point
├── .gitignore
└── README.md
```

## Data and Privacy Notes

- Do not commit generated data-lake outputs, virtual environments, caches, logs, or secrets.
- Confirm source CSVs contain only synthetic or otherwise approved data before publishing them publicly.
- Keep credentials and connection strings out of source control. Use environment variables or a local configuration file excluded by `.gitignore`.

## Current Status

- Local pipeline: recorded successful end-to-end runs.
- Bronze/Silver reconciliation: 6 of 6 datasets passed.
- Data-quality exceptions: 253 quarantined records and 7 deduplicated records recorded.
- Gold layer: all five datasets generated.
- Remaining verification item: investigate small revenue differences across Gold outputs before claiming exact numerical parity.
