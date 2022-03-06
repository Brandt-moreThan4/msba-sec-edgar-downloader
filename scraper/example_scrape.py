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
# from ut_msba_edgar_scraper.msba_utils import get_ticker_from_gvkey, get_cik_from_gvkey, gvkey_exists 


# This block is here to get info on the universe of stocks we are going to scrape
stock_df = pd.read_csv('scraper/scraping_universe.csv') # pd.read_csv('scraper/company_data.csv')
stock_df['datadate'] = pd.to_datetime(stock_df['datadate'])
stock_df = stock_df[stock_df.datadate >='2000-01-01']
stock_df = stock_df.dropna()
stock_df['gvkey'] = stock_df['gvkey'].astype(str) 
stock_df['cik'] = stock_df['cik'].astype(int).astype(str)

gvkeys = list(stock_df.gvkey.unique())[:20]


downloader = Downloader("scraper/consumer_discretionary")


gvkey = gvkeys[0]



reports = downloader.get_filings('10-K','51616', before='2020-07-15')

# reports = downloader.get_filings('10-Q',gvkey, before='2020-07-15', is_gvkey=True)

# Grab the most recent report
most_recent_report = reports[-1]
print(most_recent_report.data_date)
# Raw is the html stuff
# raw = most_recent_report.get_report()

# raw = most_recent_report.get_report(type='text')


print(reports)


print('haha')




