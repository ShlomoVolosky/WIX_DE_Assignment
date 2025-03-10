 config/settings.py holds API keys, base URLs, default tickers, etc.
 pipeline/polygon_extractor.py and pipeline/frankfurter_extractor.py fetch data from the respective APIs.
 pipeline/transformer.py cleans/merge data.
 pipeline/loader.py loads data into a dimensional schema (example: PostgreSQL or SQLite).
 database/schema.sql has the SCL DDL statements for the dimension and fact tables.
 streamlit_app/app.py hosts the use-facing analytics UI (including currency selection, date filters, etc.).
 main.py orchestrates the pipeline end to end (extract -> transform -> load).) 
