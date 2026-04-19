import requests
import pandas as pd
import csv
import io
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

#tickers = ["AA", "AAPL"]
#names = ["Alcoa Corporation", "APPLE"]

stocks_list = pd.read_csv("SP500.csv")
stocks = list(stocks_list["<TICKER>"])
tickers = stocks

name_list = pd.read_csv("SP500.csv")
names = list(name_list["<NAME>"])

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

def fetch_ticker_data(ticker, name, max_retries=1, backoff_factor=2):
    for attempt in range(max_retries):
        try:
            ticker_full = ticker + ".US"
            url = f"https://stooq.com/q/l/?s={ticker_full}&f=sd2t2ohlcv&h&e=csv"
            response = requests.get(url, headers=headers, timeout=10)
            reader = csv.reader(io.StringIO(response.text))
            df = pd.DataFrame(reader)
            row1 = df.iloc[1]
            date_clean = row1[1].replace("-", "")
            return {
                "<TICKER>": ticker,
                "<NAME>": name,
                "<DTYYYYMMDD>": date_clean,
                "<OPEN>": row1[3],
                "<HIGH>": row1[4],
                "<LOW>": row1[5],
                "<CLOSE>": row1[6],
                "<VOL>": row1[7]
            }
        except Exception as e:
            wait_time = backoff_factor ** attempt
            print(f"Retry {attempt+1} for {ticker} after {wait_time}s due to error: {e}")
            time.sleep(wait_time)
    return None
    
def batch_fetch(ticker_list, name_list, max_retries=1):
    results = []
    failed = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(fetch_ticker_data, ticker_list[i], name_list[i], max_retries): (ticker_list[i], name_list[i])
            for i in range(len(ticker_list))
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching tickers"):
            result = future.result()
            if result:
                results.append(result)
            else:
                failed.append(futures[future])
    return results, failed    

# 🔹 First batch
data, failed_batch = batch_fetch(tickers, names, max_retries=1)

still_failed = []

# 🔁 Retry failed tickers
if failed_batch:
    print(f"\nRetrying {len(failed_batch)} failed tickers...\n")
    retry_tickers, retry_names = zip(*failed_batch)
    retry_data, still_failed = batch_fetch(retry_tickers, retry_names, max_retries=2)
    data.extend(retry_data)

# Create DataFrame
df = pd.DataFrame(data)

# Convert to float type
for col in ['<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Create flag columns
df['high_flag'] = ((df['<OPEN>'] > df['<HIGH>']) |
                   (df['<LOW>'] > df['<HIGH>']) |
                   (df['<CLOSE>'] > df['<HIGH>']))

df['low_flag'] = ((df['<OPEN>'] < df['<LOW>']) |
                  (df['<HIGH>'] < df['<LOW>']) |
                  (df['<CLOSE>'] < df['<LOW>']))

# Filter invalid rows
df = df[df['high_flag'] == False].reset_index(drop=True)
df = df[df['low_flag'] == False].reset_index(drop=True)

# Drop flag columns
df = df.drop(columns=['high_flag', 'low_flag'])

# Set index and save
df = df.set_index("<TICKER>")
getdate = df.iloc[0]["<DTYYYYMMDD>"]
path = "/files/Stooq/SNP500/"
df.to_csv(path + f"US STOOQ SNP500 quotes {getdate}.csv")

print("Done.")

# Optional: print any tickers that failed both attempts
if still_failed:
    print("\nTickers that failed both attempts:")
    for ticker, _ in still_failed:
        print(f" - {ticker}")

# Optional: log tickers that failed both attempts        
if still_failed:
    log_path = "/files/Stooq/SNP500/SNP500_failed_tickers.txt"
    with open(log_path, "w") as log_file:
        log_file.write("Tickers that failed both attempts:\n")
        for ticker, name in still_failed:
            log_file.write(f"{ticker} - {name}\n")
    print(f"\nLogged {len(still_failed)} failed tickers to {log_path}")
