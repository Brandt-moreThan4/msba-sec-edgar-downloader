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


# print('ha')