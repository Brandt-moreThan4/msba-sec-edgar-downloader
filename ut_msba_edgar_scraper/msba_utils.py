import pandas as pd
from pathlib import Path
from datetime import datetime
# from bs4 import BeautifulSoup

# folder_path = Path(__file__).parent
edgar_filing_path = Path().cwd() / 'scraper' / 'sec-edgar-filings'
# company_data_path = folder_path / 'company_data.csv'

todays_date = datetime.today().strftime('%Y-%m-%d') # We need this for several lookups later


# This block is  used to get the cik-gvkey mapping. 
stock_mapping_df = pd.read_csv('https://github.com/Brandt-moreThan4/Data/blob/main/company_data.csv?raw=true') 
stock_mapping_df['datadate'] = pd.to_datetime(stock_mapping_df['datadate'])
stock_mapping_df = stock_mapping_df[stock_mapping_df.datadate >= '2000-01-01']
stock_mapping_df = stock_mapping_df.dropna()
stock_mapping_df['gvkey'] = stock_mapping_df['gvkey'].astype(str) 
stock_mapping_df['cik'] = stock_mapping_df['cik'].astype(int).astype(str)
stock_mapping_df['cik'] = stock_mapping_df['cik'].str.zfill(10)


# We could speed up the below functions by making it an index lookup instead of filters

def get_ticker_from_gvkey(gvkey:str, date:str) -> str:
    """Send in a gvkey string, and this will return the cik associated with that Date in formate YYYY-MM-DD"""
    filtered_df = stock_mapping_df[(stock_mapping_df.gvkey == gvkey) & (stock_mapping_df.datadate <= date) ]
    tickers = list(filtered_df.tic.unique())

    return str(tickers[-1]) # Returns the last cik. Just in case there are multiple, which there shouldn't be


def gvkey_exists(gvkey:str) -> bool:

    if str(gvkey) in stock_mapping_df['gvkey'].values:
        return True
    else:
        return False

def cik_exists(cik:str) -> bool:
    cik = str(cik).zfill(10)
    if cik in stock_mapping_df['cik'].values:
        return True
    else:
        return False

def get_cik_from_gvkey(gvkey:str, date:str=todays_date) -> str:
    """Send in a gvkey string, and this will return the cik associated with that Date in formate YYYY-MM-DD
    returns None if there are no matches. This should only happen if their is no mapping before the provided date
    """

    gvkey = str(gvkey) # Needs to be a string to be conistent with other dataframes

    if not gvkey_exists(gvkey):
        raise Exception(f'Sorry, gvkey "{gvkey}" is invalid. Or we do not have this gvkey in our CIK-gvkey mapping table dataframe.')

    filtered_df = stock_mapping_df[(stock_mapping_df.gvkey == gvkey) & (stock_mapping_df.datadate <= todays_date)]

    if len(filtered_df) == 0:
        return None

    ciks = list(filtered_df.cik.unique())
    # Returns the last cik. Just in case there are multiple, which there shouldn't be
    # Also, make sure it is in the 10 digit format. So fill in leading zeros if nexessary
    cik = str(ciks[-1]).zfill(10) 

    return cik 


def get_gvkey_from_cik(cik:str, date:str=todays_date):
    cik = str(cik).zfill(10) # Needs to be a string to be conistent with other dataframes

    if not cik_exists(cik):
        raise Exception(f'Sorry, cik "{cik}" is invalid. Or we do not have this gvkey in our CIK-gvkey mapping table dataframe.')

    filtered_df = stock_mapping_df[(stock_mapping_df.cik == cik) & (stock_mapping_df.datadate <= date)]

    if len(filtered_df) == 0:
        return None

    gvkeys = list(filtered_df.gvkey.unique())
    gvkey = str(gvkeys[-1])

    return gvkey 


def get_ratings_df() -> pd.DataFrame:
    # This block is here to get info on the universe of stocks we are going to scrape
    ratings_df = pd.read_csv('https://github.com/Brandt-moreThan4/Data/blob/main/bond_ratings.csv?raw=true') 

    ratings_df['datadate'] = pd.to_datetime(ratings_df['datadate'])
    ratings_df = ratings_df[ratings_df.datadate >='2000-01-01']
    ratings_df = ratings_df.dropna()
    ratings_df['gvkey'] = ratings_df['gvkey'].astype(str) 
    ratings_df['cik'] = ratings_df['cik'].astype(int).astype(str)

    return ratings_df



def filing_in_drive(filing, df_log:pd.DataFrame) -> bool:
    """Returns True if the filing has already been uploaded to the drive."""

    log_successes = df_log[df_log.success == True]

    # We consider the file uploaded if we have a record with matching accession number and gvkey
    # gvkey needs to be converted to same format as log gvkeys. Which is string, without zero padding
    if filing.accession_number in log_successes['accession_number'].values and str(int(filing.gvkey)) in log_successes['gvkey'].values:
        return True
    else:
        return False