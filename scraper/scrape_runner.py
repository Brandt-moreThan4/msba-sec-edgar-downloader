# from cv2 import CHAIN_APPROX_TC89_KCOS
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
from ut_msba_edgar_scraper.msba_utils import get_ticker_from_gvkey, get_cik_from_gvkey 


# print(get_cik('8264','2004-01-01'))
# print(get_cik('8264','2004-01-01'))
# print(get_cik('8264','2004-01-01'))

filing_types = ['10-K','10-Q']



start_date = "2000-01-01"
start_date = "2019-01-01"
end_date = "2021-01-01"

# This block is here to get info on the universe of stocks we are going to scrape
stock_df = pd.read_csv('scraper/scraping_universe.csv') # pd.read_csv('scraper/company_data.csv')
stock_df['datadate'] = pd.to_datetime(stock_df['datadate'])
stock_df = stock_df[stock_df.datadate >='2000-01-01']
stock_df = stock_df.dropna()
stock_df['gvkey'] = stock_df['gvkey'].astype(str) 
stock_df['cik'] = stock_df['cik'].astype(int).astype(str)



# Only look at energy sector companies
# energy_df = stock_df[stock_df['gsector'] == 10]
# energy_df = energy_df[energy_df.datadate >= start_date]
# ciks = list(energy_df['cik'].unique())


ciks = list(stock_df['cik'].unique())
ciks = ciks[:20]
print(len(ciks))


consumer_df = pd.read_csv('scraper/consumer_gvkey.csv')
gvkeys = consumer_df['gvkey'].unique()




downloader = Downloader("scraper")

log_dict: dict = {'ticker':[],'cik':[],'filing_type':[],'period_end':[],'file_name':[],'url':[],'success':[]}
failed_lookups = []

for ticker in ciks:
# for ticker in tickers:
    print(f'Getting cik: "{ticker}"')
    for filing_type in filing_types:
        downloader.get2(filing_type, ticker, after=start_date, before=end_date,log_dict=log_dict)
        # try:
        #     downloader.get2(filing_type, ticker, after=start_date, before=end_date,log_dict=log_dict)
        # except:
        #     print(f'Failed somewhere for: {ticker}-{filing_type}')
        #     failed_lookups.append([ticker,filing_type])



# Save the log to a csv
df_log = pd.DataFrame(log_dict)
log_path = Path() / 'scraper' / 'logs' / f'log_{str(datetime.datetime.now()).replace(" ","-").replace(":","_")}.csv'

df_log.to_csv(log_path)

df_failures = pd.DataFrame(data=failed_lookups,columns=['ticker','filing_type'])
if len(df_failures) > 0:
    df_failures.to_csv('failures.csv')


executionTime = (time.time() - startTime)
print('Execution time in minutes: ' + str(executionTime/60))


