import requests
import pandas as pd
import time
import os

API_KEY = "Add_Your_API_Key"
SYMBOLS = ["AAPL", "MSFT"]
SLEEP_SECONDS = 300

URL = "https://financialmodelingprep.com/stable/historical-chart/5min"

def fetch_latest(symbol):
    params = {
        "symbol": symbol,
        "apikey": API_KEY,
        "limit": 2   # small buffer
    }
    r = requests.get(URL, params=params, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    df["date"] = pd.to_datetime(df["date"], utc=True)
    return df

def update_csv(symbol):
    filename = f"{symbol}_5min.csv"
    df_new = fetch_latest(symbol)

    if df_new.empty:
        return

    if os.path.exists(filename):
        df_old = pd.read_csv(filename)
        df_old["date"] = pd.to_datetime(df_old["date"], utc=True)
        last_ts = df_old["date"].max()
        df_new = df_new[df_new["date"] > last_ts]
        if df_new.empty:
            print(f"[{symbol}] Up to date")
            return
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    df.sort_values("date", inplace=True)
    df.to_csv(filename, index=False)
    print(f"[{symbol}] Added {len(df_new)} bars")

print("5-minute updater running... Ctrl+C to stop")

while True:
    for s in SYMBOLS:
        try:
            update_csv(s)
        except Exception as e:
            print(f"[{s}] Error:", e)
    time.sleep(SLEEP_SECONDS)