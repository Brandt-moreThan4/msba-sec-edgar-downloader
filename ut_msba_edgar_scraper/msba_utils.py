import pandas as pd
from pathlib import Path
import numpy as np
from bs4 import BeautifulSoup

folder_path = Path(__file__).parent
edgar_filing_path = Path().cwd() / 'scraper' / 'sec-edgar-filings'
data_path = folder_path / 'company_data.csv'


# This block is  used to get the cik-gvkey mapping
stock_mapping_df = pd.read_csv(data_path)
stock_mapping_df['datadate'] = pd.to_datetime(stock_mapping_df['datadate'])
stock_mapping_df = stock_mapping_df[stock_mapping_df.datadate >= '2000-01-01']
stock_mapping_df = stock_mapping_df.dropna()
stock_mapping_df['gvkey'] = stock_mapping_df['gvkey'].astype(str) 
stock_mapping_df['cik'] = stock_mapping_df['cik'].astype(int).astype(str)


# We could speed up the below functions by making it an index lookup instead of filters

def get_ticker_from_gvkey(gvkey:str, date:str) -> str:
    """Send in a gvkey string, and this will return the cik associated with that Date in formate YYYY-MM-DD"""
    filtered_df = stock_mapping_df[(stock_mapping_df.gvkey == gvkey) & (stock_mapping_df.datadate <= date) ]
    tickers = list(filtered_df.tic.unique())

    return str(tickers[-1]) # Returns the last cik. Just in case there are multiple, which there shouldn't be

# get_ticker('11506','2004-01-01')


def gvkey_exists(gvkey:str) -> bool:
    if str(gvkey) in stock_mapping_df['gvkey']:
        return True
    else:
        return False

def get_cik_from_gvkey(gvkey:str, date:str) -> str:
    """Send in a gvkey string, and this will return the cik associated with that Date in formate YYYY-MM-DD
    returns None if there are no matches. This should only happen if their is no mapping before the provided date
    """

    gvkey = str(gvkey) # Needs to be a string to be conistent with other dataframes

    if not gvkey_exists(gvkey):
        Exception(f'Sorry, gvkey "{gvkey}" is invalid. Or we do not have this gvkey in our CIK-gvkey mapping table dataframe.')

    filtered_df = stock_mapping_df[(stock_mapping_df.gvkey == gvkey) & (stock_mapping_df.datadate <= date)]

    if len(filtered_df) == 0:
        return None

    ciks = list(filtered_df.cik.unique())
    # Returns the last cik. Just in case there are multiple, which there shouldn't be
    # Also, make sure it is in the 10 digit format. So fill in leading zeros if nexessary
    cik = str(ciks[-1]).zfill(10) 

    return cik 



# print(get_cik('8264','2004-01-01'))


filing_info_path = folder_path / 'filings_info.csv'

df_file_mapping = pd.read_csv(filing_info_path,dtype='string')
df_file_mapping['date'] = pd.to_datetime(df_file_mapping['date'])
df_file_mapping = df_file_mapping.sort_values(['cik','date'])




def get_file_name(gvkey:str,date:str,report_type:str) -> str: # This should be changes so report type is a list of options
    """Add descripton please"""
    cik_num = get_cik_from_gvkey(gvkey,date) # Lookup cik
    # Now filter the dataframe for that cik
    df_filtered = df_file_mapping[(df_file_mapping['cik'] == cik_num) & (df_file_mapping['report_type'] == report_type) & (df_file_mapping['date'] <= date)]
    
    if len(df_filtered) == 0:
        return None
    else:
        last_row = df_filtered.iloc[-1] # This should give the latest report for the company as long as the previous filterings are good
        return last_row['file_name']


def get_report(file_name:str, type='raw') -> str:
    """type options are 'soup, raw, and text' """
    file_path = edgar_filing_path / file_name
    with file_path.open("r", encoding='utf-8') as f:
        text = f.read()
    
    if type == 'raw':
        return text
    elif type == 'soup':
        return BeautifulSoup(text)
    elif type == 'text':
        soup = BeautifulSoup(text)
        return soup.get_text()
    else:
        Exception(f'Sorry, invalid type. Must be one of: "soup, raw, or text" ')


# print(get_file_name('1004','2019-09-01','10-Q'))
# print(get_file_name('1004','2019-07-01','10-Q'))

# soup = get_report(get_file_name('1004','2019-07-01','10-Q'))


# print(soup.get_text())
# print('ha')




def print_filing_stats(filing_path):

    all_filings = list(filing_path.iterdir())
    all_sizes = [file.stat().st_size/1_000_000 for file in all_filings]
    all_names = [file.name for file in all_filings]
    all_ciks = list(set([name[:10] for name in all_names]))

    print(f"""
    Number of filings = {len(all_filings)}
    Number of unique ciks = {len(all_ciks)} 
    Avergae filing size = {np.mean(all_sizes)}
    """)

if __name__ == '__main__':
    print_filing_stats(edgar_filing_path)