import urllib.request
import os

FILE = "yellow_tripdata_2023-01.parquet"
URL  = "https://d37ci6vzurychx.cloudfront.net/trip-data/" + FILE

if os.path.exists(FILE):
    print(f"File already exists: {FILE}")
else:
    print("Downloading NYC taxi data (~47MB)...")
    print("Please wait 1-2 minutes...")
    urllib.request.urlretrieve(URL, FILE)
    size_mb = os.path.getsize(FILE) / (1024 * 1024)
    print(f"Done! Saved: {FILE} ({size_mb:.1f} MB)")
