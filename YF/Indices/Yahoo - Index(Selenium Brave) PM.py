import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import sys
from tqdm import tqdm  # Add this import

# Path to Brave browser executable (adjust to your OS)
#brave_path = "/usr/bin/brave"
brave_path = "C:/Program Files/BraveSoftware/Brave-Browser/Application/brave.exe"

tickers = ["^HSI", "^STI", "^N225"]
names = ["Hang Seng Index", "Straits Times Index", "Nikkei 225"]

# Override for testing
#tickers = ["DX-Y.NYB"]
#names = ["US Dollar Index"]

# Add progress bar wrapper
for i in tqdm(range(len(tickers)), desc="Processing tickers", unit="ticker"):
    
    # Update progress bar description with current ticker
    tqdm.write(f"Fetching data for {names[i]} ({tickers[i]})...")
    
    options = webdriver.ChromeOptions()
    options.binary_location = brave_path
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    url = "https://sg.finance.yahoo.com/quote/" + tickers[i] + "/history/"
    driver.get(url)

    time.sleep(5)
    html = driver.page_source
    driver.quit()

    #tickers[i] = tickers[i].replace("^", "")
    cleaned_ticker = tickers[i].replace("^", "")
    cleaned_ticker = cleaned_ticker.replace("-", "")
    
    tables = pd.read_html(html)
    df = tables[0]
    
    df['Date'] = df['Date'].astype('datetime64[ns]')
    df['Date'] = df['Date'].dt.strftime('%Y%m%d').astype(int)

    df['high_flag'] = ((df['Open'] > df['High']) | 
                      (df['Low'] > df['High']) | 
                      (df['Close Closing price adjusted for splits.'] > df['High']))

    df['low_flag'] = ((df['Open'] < df['Low']) | 
                      (df['High'] < df['Low']) | 
                      (df['Close Closing price adjusted for splits.'] < df['Low']))

    df = df[df['high_flag'] == False].reset_index(drop=True)
    df = df[df['low_flag'] == False].reset_index(drop=True)

    fifth_column_name = df.columns[5]
    seventh_column_name = df.columns[7]
    eighth_column_name = df.columns[8]

    df_modified = df.drop(columns=[fifth_column_name, seventh_column_name, eighth_column_name])
    df = df_modified
    
    df.insert(0, "<TICKER>", tickers[i], True)
    df.insert(1, "<NAME>", names[i], True)

    mapping = {'Date': '<DTYYYYMMDD>', 'Open': '<OPEN>', 'High': '<HIGH>', 'Low': '<LOW>', 
               'Close Closing price adjusted for splits.': '<CLOSE>', 'Volume': '<VOL>', 
               'Open Interest': '<OPENINT>'}
    
    df = df.rename(columns=mapping)
    df = df.set_index("<TICKER>")

    #path = "/home/kangjl/download/temp/Yahoo/Index/"
    path = "C:\\temp\\Yahoo\\Index\\"
    
    df.to_csv(path + cleaned_ticker + ".csv")
    
    tqdm.write(f"✓ Saved {tickers[i]}.csv")

print("\n✓ All downloads complete!")
