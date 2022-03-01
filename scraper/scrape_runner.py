import pandas as pd
from pathlib import Path
import sys
import datetime
import pandas as pd
import time



# Start the timer to see how long the program runs
startTime = time.time()


# This is sloppy, but I don't know how to fix it yet.
# This just needs to be the path to the folder where the scraping code package is stored in. 
sys.path.append(r'C:\Users\User\OneDrive\Desktop\Code\msba_edgar')
from ut_msba_edgar_scraper import Downloader


filing_types = ['10-K','10-Q']


start_date = "2000-01-01"
start_date = "2018-01-01"
end_date = "2021-01-01"

# This block is mainly used to get the cik-gvkey mapping
stock_mapping_df = pd.read_csv('scraper/company_data.csv')
stock_mapping_df['datadate'] = pd.to_datetime(stock_mapping_df['datadate'])
stock_mapping_df = stock_mapping_df[stock_mapping_df.datadate >='2000-01-01']
stock_mapping_df = stock_mapping_df.dropna()
stock_mapping_df['gvkey'] = stock_mapping_df['gvkey'].astype(str) 
stock_mapping_df['cik'] = stock_mapping_df['cik'].astype(int).astype(str)

# Only look at energy sector companies
energy_df = stock_mapping_df[stock_mapping_df['gsector'] == 10]
energy_df = energy_df[energy_df.datadate >= start_date]

# Grab first 200 ciks
ciks = list(energy_df['cik'].unique())
# print(len(ciks))


def get_cik(gvkey:str, date:str) -> str:
    """Send in a gvkey string, and this will return the cik associated with that Date in formate YYYY-MM-DD"""
    filtered_df = stock_mapping_df[(stock_mapping_df.gvkey == gvkey) & (stock_mapping_df.datadate <= date) ]
    ciks = list(filtered_df.cik.unique())
    return ciks[-1] # Returns the last cik. Just in case there are multiple, which there shouldn't be



ciks = ['19617']
# ciks = ['0000019617']
# ciks = [cik.rjust(10,'0') for cik in ciks]



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
print('Execution time in minutes: ' + str(executionTime/60))


