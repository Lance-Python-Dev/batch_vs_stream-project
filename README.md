## Batch Processing

- **Simulates a daily ETL job:**  
  I created a DataFrame of transactions, save it as a CSV, clean and transform the data, aggregate totals per customer, and save the results as Parquet files.
- **SQL-like queries:**  
  I used DuckDB to run SQL queries on the aggregated Parquet data.
- **Result:**  
  The final customer totals are saved in `data/batch_customer_totals.parquet`.  
  Example (from my file):
  - David: 300
  - Bob: 200
  - Alice: 100
  - Charlie: 50


## Stream Processing

- **Simulates real-time transaction events:**  
  I used the Streamz library to emit random transaction events one by one.
- **Real-time aggregation:**  
  Each event updates the running total for each customer instantly.
- **Result:**  
  After all events, the final totals are saved in `data/stream_totals.parquet`.
