# from cv2 import CHAIN_APPROX_TC89_KCOS
import pandas as pd
from pathlib import Path
import sys
import datetime
import pandas as pd
import time

sys.path.append(r'C:\Users\User\OneDrive\Desktop\Code\msba_edgar')
from ut_msba_edgar_scraper import Downloader
from ut_msba_edgar_scraper.msba_utils import get_ratings_df, get_ticker_from_gvkey, get_cik_from_gvkey 



# Start the timer to see how long the program runs
startTime = time.time()


filing_types = ['10-K','10-Q']
ratings_df = get_ratings_df()
gvkeys = ratings_df['gvkey'].unique()



downloader = Downloader("scraper")

log_dict: dict = {'ticker':[],'cik':[],'filing_type':[],'period_end':[],'file_name':[],'url':[],'success':[]}
failed_lookups = []

for gvkey in gvkeys:
    print(f'Getting gvkey: "{gvkey}"')
    for filing_type in filing_types:
        downloader.get3(filing_type, gvkey, log_dict=log_dict)
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


