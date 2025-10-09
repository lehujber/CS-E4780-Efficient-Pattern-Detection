import sys, pandas as pd
src = sys.argv[1] if len(sys.argv) > 1 else "test/EventFiles/201801-citibike-tripdata_1.csv"
dst = sys.argv[2] if len(sys.argv) > 2 else src + "sorted.csv"
df = pd.read_csv(src)
stcol = "stoptime"
df.iloc[pd.to_datetime(df[stcol], errors="coerce").argsort()].to_csv(dst, index=False)
print(f"Sorted: {dst}")
