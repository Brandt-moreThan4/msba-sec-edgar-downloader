import pandas as pd
from pathlib import Path
import sys

import pandas as pd
import time

sys.path.append(r'C:\Users\User\OneDrive\Desktop\Code\msba_edgar')
from ut_msba_edgar_scraper import Downloader
from ut_msba_edgar_scraper.msba_utils import get_ratings_df

from utils import save_logs

# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive

# # Authorization to make sure we can 
# gauth = GoogleAuth()           
# drive = GoogleDrive(gauth)  


# Start the timer to see how long the program runs
startTime = time.time()


filing_types = ['10-K','10-Q']
filing_types = ['10-Q']
ratings_df = get_ratings_df() # this is our scraping universe
# ratings_df = ratings_df.iloc[:2]
gvkeys = ratings_df['gvkey'].unique()[:3]
print(f'unique gvkeys = {len(gvkeys)}')

# gvkeys = [str(x) for x in [1004,1238, 1848, 2697]]
# gvkeys = [str(x) for x in [1238, 1848, 2697]]
gvkeys = ['1224']

downloader = Downloader("scraper")

log_list: list = []
failed_lookups = []


for gvkey in gvkeys:
    print(f'Getting gvkey: "{gvkey}"')
    for filing_type in filing_types:
        try:
            downloader.get(filing_type, gvkey, log_list=log_list)
            if len(log_list['cik']) % 30 == 0:
                save_logs(log_list,failed_lookups)
        except:
            print(f'Failed somewhere for: {gvkey}-{filing_type}')
            failed_lookups.append([gvkey,filing_type])




save_logs(log_list, failed_lookups)


executionTime = (time.time() - startTime)
print('Execution time in minutes: ' + str(executionTime/60))

print('ha')


