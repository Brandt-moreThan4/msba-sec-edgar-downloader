from importlib.resources import path
import pandas as pd
from pathlib import Path
import sys
import datetime
import pandas as pd


# This is sloppy, but I don't know how to fix it yet.
sys.path.append(r'C:\Users\User\OneDrive\Desktop\Code\msba_edgar')
from sec_edgar_downloader import Downloader



filing_types = ['10-K','10-Q']
tickers = ['EIX', 'NRG','WMB', 'SWN','DPL']

tickers = ['TREC']


start_date = "2000-01-01"
start_date = "2015-01-01"
end_date = "2021-01-01"


downloader = Downloader("scraper")

# log_path = Path() / 'scraper'  f'log_{str(datetime.datetime.now()).replace(" ","")}.csv'

log_dict: dict = {'ticker':[],'cik':[],'filing_type':[],'period_end':[],'file_name':[],'url':[],'success':[]}


for ticker in tickers:
    print(f'Getting ticker: {ticker}')
    for filing_type in filing_types:
        
        downloader.get2(filing_type, ticker, after=start_date, before=end_date,log_dict=log_dict)


        
df_log = pd.DataFrame(log_dict)
log_path = Path() / 'scraper' / f'log_{str(datetime.datetime.now()).replace(" ","-").replace(":","_")}.csv'
df_log.to_csv(log_path)



print('hahah')
print('hoooooooo')

