from pathlib import Path
import pandas as pd
import sys
import datetime

# sys.path.append(r'C:\Users\User\OneDrive\Desktop\Code\msba_edgar')

def save_logs(log_dict:dict,failed_lookups,col:list=None):
    df_log = pd.DataFrame(log_dict)
    log_path = Path() / 'scraper' / 'logs' / f'log_{str(datetime.datetime.now()).replace(" ","-").replace(":","_")}.csv'
    df_log.to_csv(log_path)

    df_failures = pd.DataFrame(data=failed_lookups,columns=['ticker','filing_type'])
    if len(df_failures) > 0:
        df_failures.to_csv(Path() / 'scraper' / 'logs' / f'failures_{str(datetime.datetime.now()).replace(" ","-").replace(":","_")}.csv')


def build_file_info_csv():
    """Extract information about all of the reports that have been downloaded."""
    filing_path_root = Path().cwd() / 'scraper' / 'sec-edgar-filings'
    filing_info_path = filing_path_root.parent / 'filings_info.csv'
    all_names = [file.name for file in filing_path_root.iterdir()]

    df = pd.DataFrame(all_names,columns=['file_name'])
    df['cik'] = df['file_name'].str.split('_').str.get(0)
    df['date'] = pd.to_datetime(df['file_name'].str.split('_').str.get(1))
    df['report_type'] = df['file_name'].str.split('_').str.get(2)
    df['accession_num'] = df['file_name'].str.split('_').str.get(3).str.split('.').str.get(0)
    df['file_suffix'] = df['file_name'].str.split('_').str.get(3).str.split('.').str.get(1)


    df.to_csv(filing_info_path,index=False)
