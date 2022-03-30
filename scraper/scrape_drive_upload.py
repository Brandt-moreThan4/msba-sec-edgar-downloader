import pandas as pd
from pathlib import Path
import sys

import pandas as pd
import time

sys.path.append(r'C:\Users\User\OneDrive\Desktop\Code\msba_edgar')
from ut_msba_edgar_scraper import Downloader
from ut_msba_edgar_scraper.msba_utils import get_ratings_df

from utils import save_logs2, read_master_log

df_master_log = read_master_log()
df_master_log = df_master_log.reset_index().drop(columns='index') # We just want to make sure indexing is proper so that the log appending with loc works later
# df_master_log = df_master_log[df_master_log.success == True].copy() # We only want to carry around the successes on this scrapinging journey
# log_df[log_df.duplicated(subset=['accession_number','gvkey','file_name','success'], keep=False) == True] # examine duplicates

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Authorization to make sure we can 
gauth = GoogleAuth()           
drive = GoogleDrive(gauth)  

gfile = drive.CreateFile({'parents': [{'id': '1bCHSa7AO0jfQRoKYFisA_pgzhsz2uKbs'}],'title':'silly_test uplaod for authentification'})
gfile.SetContentString('lololololo') 
gfile.Upload() 

# Start the timer to see how long the program runs
startTime = time.time()

filing_types = ['10-K','10-Q']
# filing_types = ['10-K']

ratings_df = get_ratings_df() # this is our scraping universe

# gvkeys = ratings_df['gvkey'].unique()
# last_gvkey = df_master_log.iloc[-1].gvkey
# gvkeys = gvkeys[np.where(gvkeys==last_gvkey)[0][0]+1:]

gvkeys = pd.read_csv('scraper/data/fallen.csv').gvkey.unique() # fallen angels


# df_failures = pd.read_csv(Path() / 'scraper/logs/master_failures.csv')

print(f'unique gvkeys = {len(gvkeys)}')
downloader = Downloader("scraper",drive=drive)

log_df = df_master_log
failed_lookups = []

log_length = len(df_master_log)

for gvkey in gvkeys:
    print(f'Getting gvkey: "{gvkey}"')
    for filing_type in filing_types:
        # try:
        downloader.download_reports(filing_type, gvkey, log_df=log_df,location='drive')
            # downloader.download_reports(filing_type, gvkey, log_df=log_df,location='local')
        if len(log_df) >= (log_length + 25): # save if the log size has icnreased by at least 25 rows
            save_logs2(log_df,failed_lookups)
            executionTime = (time.time() - startTime)
            print('Execution time in minutes: ' + str(executionTime/60))
            log_length = len(log_df)
        # except Exception as e:
        #     print(f'Failed somewhere for: {gvkey}-{filing_type}')
        #     failed_lookups.append([gvkey,filing_type,str(e)])
            # save_logs2(log_df,failed_lookups)



save_logs2(log_df, failed_lookups,startTime=startTime)


executionTime = (time.time() - startTime)
print('Execution time in minutes: ' + str(executionTime/60))

print('haha done')


