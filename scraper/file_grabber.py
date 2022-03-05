from pathlib import Path
import os
import pandas as pd
from bs4 import BeautifulSoup


filing_path = Path().cwd() / 'scraper' / 'sec-edgar-filings'
filing_info_path = filing_path.parent / 'filings_info.csv'


def get_filing(cik:str, date:str, report_type='10-K'):
    pass

# file_path = Path(filing_path /'0000001750_2020-08-31_10-Q_0001104659-20-108360.html')

# with file_path.open("r", encoding='utf-8') as f:
#     text = f.read()

# soup = BeautifulSoup(text)
# get_filing('0000001750','2020-11-29','10-Q')



all_names = [file.name for file in filing_path.iterdir()]

df = pd.DataFrame(all_names,columns=['file_name'])
# df = df.astype('string')
df['cik'] = df['file_name'].str.split('_').str.get(0)
df['date'] = pd.to_datetime(df['file_name'].str.split('_').str.get(1))
df['report_type'] = df['file_name'].str.split('_').str.get(2)
df['accession_num'] = df['file_name'].str.split('_').str.get(3).str.split('.').str.get(0)
df['file_suffix'] = df['file_name'].str.split('_').str.get(3).str.split('.').str.get(1)

print(df)

df.to_csv(filing_info_path,index=False)

print('ha')