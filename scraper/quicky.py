import pandas as pd
from pathlib import Path
import sys

sys.path.append(r'C:\Users\User\OneDrive\Desktop\Code\msba_edgar')
from sec_edgar_downloader import Downloader


# company_df = pd.read_csv('scrape/stocklist_24022022.csv')
# tickers = company_df.sort_values('Volume',ascending=False).Symbol.iloc[:5].to_list()
filing_types = ['10-K','10-Q']
tickers = ['AMZN', 'MSFT']
tickers = ['EIX', 'NRG','WMB', 'SWN','DPL']
# tickers = ['AMZN','MSFT']
tickers = ['AMZN']



start_date = "2000-01-01"
start_date = "2018-01-31"
end_date = "2021-01-01"

downloader = Downloader("scraper")


for ticker in tickers:
    print(f'Getting ticker: {ticker}')
    for filing_type in filing_types:
        
        downloader.get(filing_type,ticker,after=start_date, before=end_date)
        # downloader.get(filing_type,ticker,amount=1)
        



print('hahah')

print('hoooooooo')

