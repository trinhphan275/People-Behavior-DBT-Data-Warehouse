from pymongo import MongoClient
from collections import defaultdict
import datetime

# Connect to MongoDB
client = MongoClient("mongodb://34.170.115.216:27017/")
db = client["glamira"]
collection = db["glamira"]  # Replace with actual collection name

# Initialize counters for profiling
null_counts = defaultdict(int)
distinct_counts = defaultdict(set)
data_types = defaultdict(set)

# Iterate through the collection
for document in collection.find():
    for field, value in document.items():
        # Count null values
        if value == "" or value is None:
            null_counts[field] += 1

        # Count distinct values
        distinct_counts[field].add(value)

        # Data type consistency check
        data_types[field].add(type(value).__name__)

        # Convert timestamp to readable date (if time_stamp exists)
        if field == "time_stamp" and isinstance(value, int):
            readable_date = datetime.datetime.utcfromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S')
            print(f"Converted time_stamp {value} to {readable_date}")

# Output the profiling results
print("Null Counts:")
for field, count in null_counts.items():
    print(f"{field}: {count} null values")

print("\nDistinct Value Counts:")
for field, distinct_vals in distinct_counts.items():
    print(f"{field}: {len(distinct_vals)} distinct values")

print("\nData Types Consistency:")
for field, types in data_types.items():
    if len(types) == 1:
        print(f"{field}: Consistent data type ({types.pop()})")
    else:
        print(f"{field}: Inconsistent data types - {types}")
