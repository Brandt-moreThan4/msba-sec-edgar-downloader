from importlib.resources import path
import pandas as pd
from pathlib import Path
import sys
import datetime
import pandas as pd


# This is sloppy, but I don't know how to fix it yet.
# This just needs to be the path to the folder that all of this code is stored in. 
sys.path.append(r'C:\Users\User\OneDrive\Desktop\Code\msba_edgar')
from sec_edgar_downloader import Downloader


filing_types = ['10-K','10-Q']
tickers = ['EIX', 'NRG','WMB', 'SWN','DPL']
tickers = ['JPM', 'DIS','V']
# tickers = ['TREC']

start_date = "2000-01-01"
start_date = "2018-01-01"
end_date = "2021-01-01"

downloader = Downloader("scraper")

# Also add in file size?
log_dict: dict = {'ticker':[],'cik':[],'filing_type':[],'period_end':[],'file_name':[],'url':[],'success':[]}

failed_lookups = []

for ticker in tickers:
    print(f'Getting ticker: {ticker}')
    for filing_type in filing_types:
        # Add a try/except here
        try:
            downloader.get2(filing_type, ticker, after=start_date, before=end_date,log_dict=log_dict)
        except:
            print(f'Failed somewhere for: {ticker}-{filing_type}')
            failed_lookups.append([ticker,filing_type])


# Save the log to a csv

df_log = pd.DataFrame(log_dict)
log_path = Path() / 'scraper' / 'logs' / f'log_{str(datetime.datetime.now()).replace(" ","-").replace(":","_")}.csv'

df_log.to_csv(log_path)

df_failures = pd.DataFrame(data=failed_lookups,columns=['ticker','filing_type'])
if len(df_failures) > 0:
    df_failures.to_csv('failures.csv')




