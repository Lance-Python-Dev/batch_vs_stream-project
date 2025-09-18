import pandas as pd 
import duckdb 
from pathlib import Path

DATA = Path("data")
DATA.mkdir(exist_ok=True)

#1. Simulate "raw daily CSV"(bronze)
raw = pd.DataFrame({
    "customer": ["Alice", "Bob", "Charlie", "David"],
    "amount": [100, 200, 50, 300],
    "date":["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"]
})

# Save the raw DataFrame as CSV
raw.to_csv(DATA/"transactions_raw.csv", index=False) #does not include an index column

#2. Transform → Silver(clean, standardize types)
df = pd.read_csv(DATA/"transactions_raw.csv")
df["date"] = pd.to_datetime(df["date"])
df.to_parquet(DATA/"transactions_clean.parquet", index=False)

#3. Aggregate → Gold (daily totals)
summary = df.groupby(["customer", "date"], as_index = False).agg(total=("amount", "sum"))
summary.to_parquet(DATA/"transactions_summary.parquet", index=False)

#4. Query with DuckDB
con = duckdb.connect()
result = con.execute("""
SELECT customer, SUM(total) as total_spent
FROM read_parquet('data/transactions_summary.parquet')
GROUP BY customer
ORDER BY total_spent DESC
""").df()

print("\n[Batch ETL result]\n", result)

# Save batch result to a Parquet file 
result.to_parquet(DATA/"batch_customer_totals.parquet", index=False)


