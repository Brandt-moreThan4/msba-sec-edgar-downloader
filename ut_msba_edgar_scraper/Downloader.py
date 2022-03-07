"""Provides a :class:`Downloader` class for downloading SEC EDGAR filings."""

import sys
from pathlib import Path
from typing import ClassVar, List, Optional, Union
import pandas as pd

import requests
from requests.adapters import HTTPAdapter

from ._constants import DATE_FORMAT_TOKENS, DEFAULT_AFTER_DATE, DEFAULT_BEFORE_DATE, ROOT_SAVE_FOLDER_NAME
from ._constants import SUPPORTED_FILINGS as _SUPPORTED_FILINGS
from ._utils import (download_filings, build_filings, get_number_of_unique_filings, is_cik, validate_date_format,retries)

from datetime import datetime 

from .msba_utils import get_cik_from_gvkey, get_gvkey_from_cik



TODAYS_DATE = DEFAULT_BEFORE_DATE.strftime(DATE_FORMAT_TOKENS)


class Downloader:
    """A :class:`Downloader` object.

    :param download_folder: relative or absolute path to download location.
        Defaults to the current working directory.


    """

    supported_filings: ClassVar[List[str]] = sorted(_SUPPORTED_FILINGS)

    def __init__(self, download_folder: Union[str, Path, None] = None, drive=None) -> None:
        """Constructor for the :class:`Downloader` class."""
        if download_folder is None:
            self.download_folder = Path.cwd()
        elif isinstance(download_folder, Path):
            self.download_folder = download_folder
        else:
            self.download_folder = Path(download_folder).expanduser().resolve()

    def get_download_folder(self):
        return self.download_folder


    def get_filings(
        self,
        filing_type: str,
        identifier: str,
        amount: Optional[int] = None,
        after: Optional[str] = None,
        before: Optional[str] = TODAYS_DATE, 
        include_amends: bool = False,
        query: str = "",
        is_gvkey=True,
    ) -> int:
        """ ADD DESCRIPTION HERE"""

        # TODAYS_DATE = DEFAULT_BEFORE_DATE.strftime(DATE_FORMAT_TOKENS)
        gvkey = None

        if is_gvkey: # If the identifier is a gvkey, then we first want to convert it to  a CIK
            gvkey = identifier

            identifier = get_cik_from_gvkey(identifier,date=TODAYS_DATE)
            

        identifier = str(identifier).strip().upper()

        # Detect CIKs and ensure that they are properly zero-padded
        if is_cik(identifier):
            if len(identifier) > 10:
                raise ValueError("Invalid CIK. CIKs must be at most 10 digits long.")
            # Pad CIK with 0s to ensure that it is exactly 10 digits long
            # The SEC Edgar Search API requires zero-padded CIKs to ensure
            # that search results are accurate. Relates to issue #84.
            identifier = identifier.zfill(10)

        if amount is None:
            # If amount is not specified, obtain all available filings.
            # We simply need a large number to denote this and the loop
            # responsible for fetching the URLs will break appropriately.
            amount = sys.maxsize
        else:
            amount = int(amount)
            if amount < 1:
                raise ValueError("Invalid amount. Please enter a number greater than 1.")

        # SEC allows for filing searches from 2000 onwards
        if after is None:
            after = DEFAULT_AFTER_DATE.strftime(DATE_FORMAT_TOKENS)
        else:
            validate_date_format(after)

            if after < DEFAULT_AFTER_DATE.strftime(DATE_FORMAT_TOKENS):
                raise ValueError(
                    f"Filings cannot be downloaded prior to {DEFAULT_AFTER_DATE.year}. "
                    f"Please enter a date on or after {DEFAULT_AFTER_DATE}."
                )

        if before is None:
            before = TODAYS_DATE
        else:
            validate_date_format(before)

        if after > before:
            raise ValueError(
                "Invalid after and before date combination. "
                "Please enter an after date that is less than the before date."
            )

        if filing_type not in _SUPPORTED_FILINGS:
            filing_options = ", ".join(self.supported_filings)
            raise ValueError(
                f"'{filing}' filings are not supported. "
                f"Please choose from the following: {filing_options}."
            )

        if not isinstance(query, str):
            raise TypeError("Query must be of type string.")

        pd_before = pd.to_datetime(before)

        filings_to_fetch = build_filings(
            filing_type,
            identifier,
            amount,
            after,
            TODAYS_DATE, 
            include_amends,
            query,
        )


        for filing in filings_to_fetch:
            filing.report_type = filing_type
            filing.file_name = (f'{filing.cik}_{filing.period_end_date}_{filing.report_type}_{filing.accession_number}.html')
            filing.save_path = self.download_folder / ROOT_SAVE_FOLDER_NAME / filing.file_name
            filing.gvkey = gvkey

            if filing.gvkey is None:
                try:
                    filing.gvkey = get_gvkey_from_cik(filing.cik, filing.period_end_date)
                except:
                    pass


        filings_to_fetch = list(filter(lambda x: x.data_date <= pd_before, filings_to_fetch))
        filings_to_fetch.sort(key=lambda x: x.data_date) # Make sure it is sorted in ascending order by period end date
            
        return filings_to_fetch

    def get(
        self,
        filing_type: str,
        identifier: str,
        log_dict: dict,
        amount: Optional[int] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        include_amends: bool = False,
        query: str = "",
        is_gvkey=True,
    ) -> int:
        """ ADD DESCRIPTION HERE

        """

        filings_to_fetch = self.get_filings(filing_type,identifier,amount,after,before,include_amends,query,is_gvkey)
            
        download_filings(filings_to_fetch, log_dict)


    def download_to_drive(
        self,
        filing_type: str,
        identifier: str,
        log_dict: dict,
        drive:None,
        amount: Optional[int] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        include_amends: bool = False,
        query: str = "",
        is_gvkey=True,
    ) -> int:
        """ ADD DESCRIPTION HERE
        """

        filings_to_fetch = self.get_filings(filing_type,identifier,amount,after,before,include_amends,query,is_gvkey)
            
        download_filings(filings_to_fetch, log_dict,'drive', drive)



    def test_scraping(
        self,
        filing_type: str,
        identifier: str,
        log_dict: dict,
        amount: Optional[int] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        include_amends: bool = False,
        query: str = "",
        is_gvkey=True,
    ) -> int:
        """ ADD DESCRIPTION HERE

        """
        

        filings_to_fetch = self.get_filings(filing_type,identifier,amount,after,before,include_amends,query,is_gvkey)

        for filing in filings_to_fetch:

            # Record the attempt in the log
            log_dict['ticker'].append(filing.edgar_name)
            log_dict['cik'].append(filing.cik) # This will capture the last cik from the query. May not match the orginally input ticker
            log_dict['period_end'].append(filing.period_end_date)
            log_dict['filing_type'].append(filing.report_type)
            log_dict['url'].append(filing.filing_details_url)
            log_dict['file_name'].append(filing.save_path.absolute()) # This actually only would make sense if it is a success
            log_dict['gvkey'].append(filing.gvkey)
            log_dict['success'].append(False) # The default is false

            report = filing.get_report(resolve_urls=False,type='raw')

            if report is not None:
                log_dict['success'][-1] = True
                    

