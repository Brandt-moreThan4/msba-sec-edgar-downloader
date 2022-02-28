
from ut_msba_edgar_scraper import Downloader


downloader = Downloader()
downloader.get_most_recent_report(report_types = ['10-K'], gvkey=1075, date='2011-12-31')


# Returns the most recent report. 
# If you send in a 
