import requests
import pandas as pd
import time
import sys
import datetime
from datetime import date, timedelta

import json
from io import StringIO
import csv
import io

# Get the current date
enddate = date.today()

# Calculate the date 28 days earlier
startdate = enddate - timedelta(days=28)

#Convert to string
s_startdate = str(startdate)
s_enddate = str(enddate)

#tickers = ["VIX", "DJI", "NDX", "IXIC", "GSPC", "DJT"]
#names = ["CBOE Volatility Index", "Dow Jones Industrial Average", "Nasdaq 100", "NASDAQ Composite", "S&P 500", "DOW JONES TRANS"]

tickers = ["N225", "HSI"]
names = ["Nikkei 225 Index", "Hang Seng Index (Hong Kong)"]

#tickers = ["N225"]
#names = ["Nikkei 225 Index"]

# API_key = Array("5cdd088c4fe1f4.01504348", "5e3d2219df58e7.93097668", "69e0abbea75476.27381605")

#for ticker in tickers:
for i in range(len(tickers)):   

    url = "https://eodhistoricaldata.com/api/eod/" + tickers[i] + ".INDX" + "?from=" + s_startdate + "&to=" \
        + s_enddate + "&api_token=" + "5e3d2219df58e7.93097668"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
        }

    response = requests.get(url, headers=headers)
    
    # Turn from YYYY-MM-DD to YYYYMMDD
    csv_data = response.text.replace('-', '')
    
    # Read the CSV text into a DataFrame
    df = pd.read_csv(StringIO(csv_data))
    
    #Convert to float type
    df['Open'] = df['Open'].astype(float)
    df['High'] = df['High'].astype(float)
    df['Low'] = df['Low'].astype(float)
    df['Close'] = df['Close'].astype(float)

    # Create flag column based on condition
    df['high_flag'] = ((df['Open'] > df['High']) | 
                      (df['Low'] > df['High']) | 
                      (df['Close'] > df['High']))

    df['low_flag'] = ((df['Open'] < df['Low']) | 
                      (df['High'] < df['Low']) | 
                      (df['Close'] < df['Low']))

    # Delete invalid high and low
    df = df[df['high_flag'] == False].reset_index(drop=True)
    df = df[df['low_flag'] == False].reset_index(drop=True)  

    # Get the name of the 7th,8th column
    fifth_column_name = df.columns[5]
    seventh_column_name = df.columns[7]
    eighth_column_name = df.columns[8]

    # Drop the 7th,8th column by name
    df_modified = df.drop(columns=[fifth_column_name, seventh_column_name, eighth_column_name])    

    df = df_modified

    # Using insert() to add a column at position 0 (1st column)
    df.insert(0, "<TICKER>", tickers[i]+ ".I", True)

    # Using insert() to add a column at position 1 (2nd column)
    df.insert(1, "<NAME>", names[i], True)    

    #Map too Metastock format
    mapping = {'Date':'<DTYYYYMMDD>','Open':'<OPEN>','High':'<HIGH>','Low':'<LOW>','Close':'<CLOSE>',\
           'Volume':'<VOL>'}
    
    df = df.rename(columns=mapping)    

    df = df.set_index("<TICKER>")    
    
    path = "/files/EOD/Indices/"
    
    #Save ticker price 
    df.to_csv(path + tickers[i] + ".INDX" +".csv") 

print("All downloads completed.")
