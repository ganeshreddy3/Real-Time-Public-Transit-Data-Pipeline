import pandas as pd

files = [
    "agency.txt",
    "calendar.txt",
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt"
]

base_path = "data/mmts/"

for f in files:
    df = pd.read_csv(base_path + f)
    print(f"\n{f} → rows: {len(df)}")
    print(df.head(3))