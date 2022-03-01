from pathlib import Path
import os



filing_path = Path().cwd() / 'scraper' / 'sec-edgar-filings'

print(filing_path.exists())

all_filings = list(filing_path.iterdir())
all_sizes = [file.stat().st_size/1_000_000 for file in all_filings]
all_names = [file.name for file in all_filings]
all_ciks = list(set([name[:10] for name in all_names]))
print(all_ciks)

print('ha')