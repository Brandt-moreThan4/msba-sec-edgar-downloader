
import pandas as pd
from pathlib import Path

# Import the lookup data set. This should probably be moved to the INIT somehow?

folder_path = Path(__file__).parent
data_path = folder_path / 'company_data.csv'

# This block is mainly used to get the cik-gvkey mapping
stock_mapping_df = pd.read_csv(data_path)
stock_mapping_df['datadate'] = pd.to_datetime(stock_mapping_df['datadate'])
stock_mapping_df = stock_mapping_df[stock_mapping_df.datadate >= '2000-01-01']
stock_mapping_df = stock_mapping_df.dropna()
stock_mapping_df['gvkey'] = stock_mapping_df['gvkey'].astype(str) 
stock_mapping_df['cik'] = stock_mapping_df['cik'].astype(int).astype(str)


def get_ticker(gvkey:str, date:str) -> str:
    """Send in a gvkey string, and this will return the cik associated with that Date in formate YYYY-MM-DD"""
    filtered_df = stock_mapping_df[(stock_mapping_df.gvkey == gvkey) & (stock_mapping_df.datadate <= date) ]
    tickers = list(filtered_df.tic.unique())

    return tickers[-1] # Returns the last cik. Just in case there are multiple, which there shouldn't be

# get_ticker('11506','2004-01-01')

def get_cik(gvkey:str, date:str) -> str:
    """Send in a gvkey string, and this will return the cik associated with that Date in formate YYYY-MM-DD"""
    filtered_df = stock_mapping_df[(stock_mapping_df.gvkey == gvkey) & (stock_mapping_df.datadate <= date) ]
    ciks = list(filtered_df.cik.unique())

    return ciks[-1] # Returns the last cik. Just in case there are multiple, which there shouldn't be

# print(get_cik('8264','2004-01-01'))