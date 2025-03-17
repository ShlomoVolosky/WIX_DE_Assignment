# README.md

## Project Overview

This repository contains my Data Engineering assignment solution, which covers:
1. **API Integration and Data Pipeline** (using Polygon.io + Frankfurter)
2. **Marketing Data Modeling**
3. **Architectural Design Challenge**

The main deliverables include:
- **Python code** (modular functions/classes) for extracting, transforming, and loading financial data.
- **A dimensional data warehouse schema** (SQL) for analyzing stock performance and currency conversions.
- **A Streamlit app** (optional) for end-user analysis.
- **Docker** setup for containerized deployment and optional **Airflow** orchestration.

---

## 1. Setup Instructions

### 1.1 Prerequisites
- **Python 3.9+**
- **Docker & Docker Compose** (if you prefer container-based workflow)
- **Polygon.io API key**
- **(Optional) Postgres or SQLite**: If using a local DB for the data warehouse

### 1.2 Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/shlomovolosky/WIX_DE_Assignment.git
   cd WIX_DE_Assignment

2. Create and activate a Python virtual environment:
   ```bash
    python -m venv venv
    source venv/bin/activate   # Mac/Linux
    # or:
    venv\Scripts\activate      # Windows
   
3. Install dependencies:
   ```bash
   pip install -r requirements.txt

4. How to Run Locally:
    ```bash
    Run the pipeline:
    python src/pipeline/main_pipeline.py
   
    This will:
    Extract stock data (Polygon.io) and FX data (Frankfurter).
    Transform/clean/join the data.
    Load into your dimension/fact tables.
   
    Check the result:
    Use a database client (psql or sqlite command line) to confirm data in fact_stock_prices.
    (Optional) Start Streamlit to see a basic UI:
    streamlit run streamlit_app/app.py
    Go to http://localhost:8501.
    

5. How to Run with Docker:
    ```bash
   Build the Image: 
   docker-compose build

   Start services:
   docker-compose up -d

   View logs:
   docker-compose logs pipeline -f


# 6. Overview of the Approach

## 3.1 API Integration & Pipeline

### Extract
- **polygon_extractor.py** calls [Polygon.io](https://polygon.io/)’s `/v2/aggs/ticker/...` endpoints for daily aggregates.
- **frankfurter_extractor.py** calls [Frankfurter](https://www.frankfurter.app/)’s `/v1/{start_date}..{end_date}` for time series FX rates.

### Transform
- **transformer.py** cleans stock data, merges with FX rates, and creates converted columns (like `converted_open`, etc.).

### Load
- **loader.py** uses SQLAlchemy + Pandas to insert data into a star schema with `dim_date`, `dim_ticker`, `dim_currency`, and `fact_stock_prices`.

## 3.2 Marketing Data Modeling
- Proposed a star schema with `fact_ad_metrics` plus hierarchical dimensions (`dim_account`, `dim_subaccount`, `dim_campaign`, etc.).
- Addresses hourly vs. daily loads, with frequently changing attributes (bid, label) stored in the fact table or as SCD Type 2 if needed.
- Includes sample SQL statements in `marketing_schema.sql` or similar.

## 3.3 Architectural Design
- **Event tracking architecture**:
  - Collection via a streaming platform (e.g. Kafka or Kinesis).
  - Processing in real-time + batch enrichment.
  - Storage in a data warehouse, with secure partitioning and cost optimization.
  - BI Tools or Streamlit for analysis.
- Considered data privacy, high volume ingestion, partitioning, and cloud providers.

# 4. Assumptions
1. Stock quotes are in USD by default, so we do USD → target currency conversions.  
2. Small set of tickers. We can scale up by chunking date ranges or parallelizing if needed.  
3. Frankfurter base is “USD”, and if needed, we’d invert rates or handle partial coverage for certain currencies.  
4. Local vs. Production: same code can run on developer machines (SQLite) or in production (Postgres/Redshift).  
5. Marketing data structure is stable enough for a star schema with possible SCD for frequently changing attributes.

# 5. Challenges & How They Were Addressed
1. **Connection errors in Pandas (_ConnectionFairy not a context manager)**:
   - Resolved by explicitly using `raw_connection()` for Pandas `to_sql()` and `read_sql()`.
2. **Duplicate data vs. test expectations**:
   - One test required 2 duplicate rows remain, so we avoided dropping duplicates in `clean_stock_data()`.
3. **Frequent dimension changes (marketing)**:
   - Placed fast-changing attributes in the fact table or used Type 2 SCD if we need historical dimension versions.
4. **Partial / missing data**:
   - If the API call fails or returns empty, the pipeline logs a warning. We skip or partially load as needed.
5. **Configuration & secrets**:
   - Hard-coded for demonstration but recommended environment variables or a secrets manager in real production.
