import yfinance as yf
import pandas as pd
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # progress bar
import os
import time
import random

# ---------------------------
# 1. Download ticker lists
# ---------------------------
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}
    response = requests.get(url, headers=headers)
    #tables = pd.read_html(response.text)
    tables = pd.read_html(io.StringIO(response.text))
    df = tables[0]
    return df["Symbol"].tolist()

def get_nasdaq100_tickers():
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}
    response = requests.get(url, headers=headers)
    #tables = pd.read_html(response.text)
    tables = pd.read_html(io.StringIO(response.text))
    df = tables[5]  # 6th table is current constituents
    return df["Ticker"].tolist()

def get_dow30_tickers():
    url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}    
    response = requests.get(url, headers=headers)
    #tables = pd.read_html(response.text)
    tables = pd.read_html(io.StringIO(response.text))
    df = tables[1]  # 2nd table is current components
    return df["Symbol"].tolist()    
	
#tickers = get_dow30_tickers()
tickers = tickers = list(set(get_dow30_tickers() + get_nasdaq100_tickers() + get_sp500_tickers()))

# --------------------------------
# 2. Function to fetch one ticker (with retry logic)
# --------------------------------

#for ticker in tickers:
#for ticker in tqdm(tickers, desc="Processing tickers", unit="stock"):
#def process_ticker(ticker):
def process_ticker(ticker, max_retries=3, retry_delay=2):
    for attempt in range(max_retries):
        try:
            stock = yf.Ticker(ticker)
            fin = stock.financials
            bs = stock.balance_sheet
            info = stock.info

            # Exclude Financials & Utilities
            sector = info.get("sector", "")
            if any(x in sector for x in ["Financial Services", "Financial", "Finance", "Bank", "Insurance", "Utility", "Utilities"]):
                return None

            # EBIT (Operating Income from financials)
            if "Operating Income" in fin.index:
                EBIT = fin.loc["Operating Income"].iloc[0]
            else:
                return None  # skip if no EBIT

            # Enterprise Value (MarketCap + TotalDebt - Cash)
            market_cap = info.get("marketCap", None)
            if market_cap is None:
                return None

            total_debt = bs.loc["Total Debt"].iloc[0] if "Total Debt" in bs.index else 0
            cash = bs.loc["Cash"].iloc[0] if "Cash" in bs.index else 0
            EV = market_cap + total_debt - cash
            if EV <= 0:
                return None

            # Net Working Capital = Current Assets - Current Liabilities
            current_assets = bs.loc["Total Current Assets"].iloc[0] if "Total Current Assets" in bs.index else 0
            current_liabilities = bs.loc["Total Current Liabilities"].iloc[0] if "Total Current Liabilities" in bs.index else 0
            NWC = current_assets - current_liabilities

            # Net Fixed Assets = Total Assets - Current Assets - Intangibles
            total_assets = bs.loc["Total Assets"].iloc[0] if "Total Assets" in bs.index else 0
            intangibles = bs.loc["Intangible Assets"].iloc[0] if "Intangible Assets" in bs.index else 0
            net_fixed = total_assets - current_assets - intangibles

            capital = NWC + net_fixed
            if capital <= 0:
                return None

            # Metrics
            earnings_yield = EBIT / EV
            ROC = EBIT / capital

            return{
                "Ticker": ticker,
                "Market Cap": market_cap,
                "Sector": sector,
                "EBIT": EBIT,
                "EV": EV,
                "EY": earnings_yield,
                "ROC": ROC
            }
        except Exception as e:
            # Wait and retry
            if attempt < max_retries - 1:
                time.sleep(retry_delay + random.uniform(0, 1))  # random jitter
            else:
                return None

# ---------------------------
# 3. Determine dynamic thread count
# ---------------------------
cpu_count = os.cpu_count() or 2  # fallback to 4 if undetected
max_threads = min(cpu_count * 1, 30)  # safe cap at 30
print(f"\nDetected {cpu_count} CPU cores → using {max_threads} threads.\n")

# --------------------------------
# 4. Multithreaded download with progress bar and retries
# --------------------------------
data = []
failed_tickers = []
print(f"Fetching data for {len(tickers)} tickers...\n")

with ThreadPoolExecutor(max_workers=2) as executor:
    futures = {executor.submit(process_ticker, t): t for t in tickers}

    for future in tqdm(as_completed(futures), total=len(futures), desc="Processing tickers", unit="stock"):
        ticker = futures[future]
        result = future.result()
        if result:
            data.append(result)
        else:
            failed_tickers.append(ticker)

# --------------------------------
# 5. Retry failed tickers (second pass)
# --------------------------------
if failed_tickers:
    print(f"\nRetrying {len(failed_tickers)} failed tickers...\n")
    with ThreadPoolExecutor(max_workers=min(10, max_threads)) as executor:
        futures = {executor.submit(process_ticker, t): t for t in failed_tickers}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Retrying", unit="stock"):
            result = future.result()
            if result:
                data.append(result)

# ---------------------------
# 5. Rank and output
# ---------------------------
df = pd.DataFrame(data)

df["EY_rank"] = df["EY"].rank(ascending=False)
df["ROC_rank"] = df["ROC"].rank(ascending=False)
df["Score"] = df["EY_rank"] + df["ROC_rank"]

df = df.sort_values("Score").reset_index(drop=True)

# Print top 20
print("\n=== Top 20 Magic Formula Stocks (Dow30) ===")
print(df.head(20)[["Ticker", "Market Cap", "Sector", "EY", "ROC", "Score"]])

# ---------------------------
# 6. Save to file
# ---------------------------
today = datetime.today().strftime("%Y-%m-%d")
csv_file = f"Dow30_Nasdaq100_SNP500_magic_formula_ranking_{today}.csv"
excel_file = f"Dow30_Nasdaq100_SNP500_magic_formula_ranking_{today}.xlsx"

#df.to_csv("Dow30_magic_formula_ranking.csv", index=False)
#df.to_excel("Dow30_magic_formula_ranking.xlsx", index=False)

df.to_csv(csv_file, index=False)
df.to_excel(excel_file, index=False)

#print("\nFull ranking saved to:")
#print(" - Dow30_magic_formula_ranking.csv")
#print(" - Dow30_magic_formula_ranking.xlsx")

print(f"\nFull ranking saved as:\n - {csv_file}\n - {excel_file}")				