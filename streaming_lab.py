from streamz import Stream
import random, time
import pandas as pd 
from pathlib import Path

DATA = Path("data")
DATA.mkdir(exist_ok=True)

# 1. Create source stream
source = Stream()

#2 Define pipeline: map, aggregate
def parse_event(event):
    return{"customer": event["customer"], "amount": event["amount"]}

def running_total(acc, x):
    acc[x["customer"]] = acc.get(x["customer"], 0) + x["amount"]
    return acc

final_totals = {}

def collect_totals(totals):
    global final_totals 
    final_totals = totals.copy()
    print(totals)

pipeline = (source.map(parse_event)
            .accumulate(running_total, start={})
            .sink(collect_totals)
)

#3. Push events (simulate real-time transactions)
customers = ["Alice", "Bob", "Eve"]
for i in range(10):
    event = {"customer": random.choice(customers), "amount": random.randint(10, 100)}
    print(f"[Event] {event}")
    source.emit(event)
    time.sleep(1) # one second interval per event 

# Note: To run this code, ensure you have the 'streamz' library installed.

# Save stream result to Parquet
stream_df = pd.DataFrame(list(final_totals.items()), columns=["customer", "total_spent"])
stream_df.to_parquet(DATA/"stream_totals.parquet", index=False)