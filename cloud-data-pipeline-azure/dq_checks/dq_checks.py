
# dq_checks.py
# Simple example of Python-based checks and email-style alert placeholder.
import pandas as pd

# In a real environment, replace with actual data source reads
a = pd.read_csv("data/source_a.csv")
c = pd.read_csv("data/source_c.csv")

# Row count threshold
if len(a) < 100:
    print("[ALERT] source_a rowcount below threshold!")

# Duplicate detection on event_id
dups = a[a.duplicated(subset=["event_id"], keep=False)]
if len(dups) > 0:
    print(f"[ALERT] Found {len(dups)} duplicate event_id rows in source_a")

# Referential integrity
missing_refs = a[~a["user_id"].isin(c["user_id"])]
if len(missing_refs) > 0:
    print(f"[ALERT] Found {len(missing_refs)} rows with missing user references")
