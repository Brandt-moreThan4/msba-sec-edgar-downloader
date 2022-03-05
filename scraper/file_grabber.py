from pathlib import Path
import os
import pandas as pd
from bs4 import BeautifulSoup
import sys

sys.path.append(r'C:\Users\User\OneDrive\Desktop\Code\msba_edgar')

from ut_msba_edgar_scraper.msba_utils import get_cik_from_gvkey, get_ticker_from_gvkey

filing_path_root = Path().cwd() / 'scraper' / 'sec-edgar-filings'
filing_info_path = filing_path_root.parent / 'filings_info.csv'



def build_file_info_csv():
    all_names = [file.name for file in filing_path_root.iterdir()]

    df = pd.DataFrame(all_names,columns=['file_name'])
    df['cik'] = df['file_name'].str.split('_').str.get(0)
    df['date'] = pd.to_datetime(df['file_name'].str.split('_').str.get(1))
    df['report_type'] = df['file_name'].str.split('_').str.get(2)
    df['accession_num'] = df['file_name'].str.split('_').str.get(3).str.split('.').str.get(0)
    df['file_suffix'] = df['file_name'].str.split('_').str.get(3).str.split('.').str.get(1)


    df.to_csv(filing_info_path,index=False)



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

def get_report(file_name:str, type='soup') -> str:
    """type options are 'soup, raw, and text' """
    file_path = filing_path_root / file_name
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


print(get_file_name('1004','2019-09-01','10-Q'))
print(get_file_name('1004','2019-07-01','10-Q'))

soup = get_report(get_file_name('1004','2019-07-01','10-Q'))

print(soup.get_text())
print('ha')