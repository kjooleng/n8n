import requests
import pandas as pd
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import json
import time

# --- Date setup ---
enddate = date.today()
startdate = enddate - timedelta(days=28)
startdate_obj = datetime.combine(startdate, datetime.min.time())
enddate_obj = datetime.combine(enddate, datetime.min.time())

s_startdate = str(int(startdate_obj.timestamp()))
s_enddate = str(int(enddate_obj.timestamp()))

# --- Input tickers and names ---
stocks_list = pd.read_csv("Commodities.csv")
stocks = list(stocks_list["<TICKER>"])
tickers = stocks

name_list = pd.read_csv("Commodities.csv")
names = list(name_list["<NAME>"])

# --- HTTP headers for Yahoo Finance API ---
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

output_path = "/files/Yahoo/Commodities/"

# --- Function to fetch and save data with retries ---
def fetch_and_save_ticker_data(ticker, name, max_retries=3):
    url = (
        f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?period1={s_startdate}&period2={s_enddate}&interval=1d&events=history"
    )
    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            json_data = response.json()
            data = json_data["chart"]["result"][0]
            meta = data["meta"]
            GMToffset = meta["gmtoffset"] #GMToffset
            timestamps = data["timestamp"]
            quote = data["indicators"]["quote"][0]
            
            from datetime import datetime, timezone, timedelta

            df = pd.DataFrame({
                "<TICKER>": ticker, #meta["symbol"]
                "<NAME>": name, #meta["longName"]
                "<DTYYYYMMDD>": [datetime.fromtimestamp(ts, timezone(timedelta(seconds=GMToffset))).strftime("%Y%m%d") for ts in timestamps],
                "<OPEN>": quote["open"],
                "<HIGH>": quote["high"],
                "<LOW>": quote["low"],
                "<CLOSE>": quote["close"],
                "<VOL>": quote["volume"]
            })

            # Clean invalid data
            df['high_flag'] = (
                (df['<OPEN>'] > df['<HIGH>']) | 
                (df['<LOW>'] > df['<HIGH>']) | 
                (df['<CLOSE>'] > df['<HIGH>'])
            )
            df['low_flag'] = (
                (df['<OPEN>'] < df['<LOW>']) | 
                (df['<HIGH>'] < df['<LOW>']) | 
                (df['<CLOSE>'] < df['<LOW>'])
            )

            df = df[(~df['high_flag']) & (~df['low_flag'])].drop(columns=['high_flag', 'low_flag'])
            df = df.set_index("<TICKER>")

            cleaned_ticker = ticker.replace("^", "")
            df.to_csv(output_path + cleaned_ticker + ".csv")
            return f"Saved {cleaned_ticker}.csv"
        except Exception as e:
            attempt += 1
            if attempt == max_retries:
                return f"Failed {ticker} after {max_retries} attempts: {str(e)}"
            time.sleep(2)  # wait 2 seconds before retrying

# --- Parallel Execution with progress bar ---
if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(fetch_and_save_ticker_data, tickers[i], names[i])
            for i in range(len(tickers))
        ]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="Downloading"):
            pass  # progress bar update

    print("All downloads completed.")
