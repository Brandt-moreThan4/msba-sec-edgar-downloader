import pandas as pd
from pathlib import Path
import sys
import datetime
import pandas as pd
import time

startTime = time.time()

# This is sloppy, but I don't know how to fix it yet.
# This just needs to be the path to the folder that all of this code is stored in. 
sys.path.append(r'C:\Users\User\OneDrive\Desktop\Code\msba_edgar')
from ut_msba_edgar_scraper import Downloader


filing_types = ['10-K','10-Q']
tickers = ['EIX', 'NRG','WMB', 'SWN','DPL']
# tickers = ['TREC']


stock_mapping_df = pd.read_csv('scraper/company_data.csv')
stock_mapping_df['datadate'] = pd.to_datetime(stock_mapping_df['datadate'])
stock_mapping_df = stock_mapping_df[stock_mapping_df.datadate >='2000-01-01']
stock_mapping_df = stock_mapping_df.dropna()
stock_mapping_df['cik'] = stock_mapping_df['cik'].astype(int).astype(str)
energy_df = stock_mapping_df[stock_mapping_df['gsector'] == 10]
tickers = list(energy_df['tic'].unique())

# Grab first 5 ciks
ciks = list(energy_df['cik'].unique())[:200]


# ciks = ['19617']
# ciks = ['0000019617']
# ciks = [cik.rjust(10,'0') for cik in ciks]


start_date = "2000-01-01"
start_date = "2015-01-01"
end_date = "2021-01-01"


downloader = Downloader("scraper")


log_dict: dict = {'ticker':[],'cik':[],'filing_type':[],'period_end':[],'file_name':[],'url':[],'success':[]}
failed_lookups = []

for ticker in ciks:
# for ticker in tickers:
    print(f'Getting ticker: {ticker}')
    for filing_type in filing_types:
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


executionTime = (time.time() - startTime)
print('Execution time in seconds: ' + str(executionTime))


