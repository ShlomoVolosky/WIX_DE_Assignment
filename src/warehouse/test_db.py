import os
import sqlalchemy
from sqlalchemy import text

engine_url = "sqlite:///C:/Users/shlom/Desktop/Projects/WIX_DE_Assignment/mydb.db"
engine = sqlalchemy.create_engine(engine_url)

with engine.connect() as conn:
    row = conn.execute(text("SELECT COUNT(*) FROM fact_stock_prices")).fetchone()
count = row[0] if row else 0
print("DEBUG test_db: fact_stock_prices count =", count)

db_path = engine.url.database
abs_path = os.path.abspath(db_path)
size_bytes = os.path.getsize(abs_path) if os.path.exists(abs_path) else 0
print("DEBUG test_db: Using DB at:", abs_path)
print("DEBUG test_db: DB file size =", size_bytes, "bytes")
