"""Utility functions for the downloader class."""
import time
from datetime import datetime
from pathlib import Path
from tkinter.messagebox import NO
from typing import List
from urllib.parse import urljoin
from importlib_metadata import metadata

import requests
from bs4 import BeautifulSoup
from faker import Faker
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

from .msba_utils import filing_in_drive

from ._constants import (
    DATE_FORMAT_TOKENS,
    FILING_DETAILS_FILENAME_STEM,
    FILING_FULL_SUBMISSION_FILENAME,
    MAX_RETRIES,
    ROOT_SAVE_FOLDER_NAME,
    SEC_EDGAR_ARCHIVES_BASE_URL,
    SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL,
    SEC_EDGAR_SEARCH_API_ENDPOINT,
)


class EdgarSearchApiError(Exception):
    """Error raised when Edgar Search API encounters a problem."""


class Filing:

    def __init__(self) -> None:
        self.accession_number = None
        self.full_submission_url = None
        self.filing_details_url = None
        self.period_end_date = None
        self.cik = None
        self.cik_lookup = None
        self.edgar_name = None
        self.file_name = None
        self.file_name_txt = None
        self.save_path = None
        self.report_type = None
        self.gvkey = None
        self.ticker = None
        self.file_date = None
        self.hit = None
    
    def __repr__(self) -> str:
        return f'{self.gvkey}_{self.period_end_date}_{self.report_type}'

    def __str__(self) -> str:
        return str(self.edgar_name)

    @property
    def data_date(self):
        """So that we can sort the string date"""
        return pd.to_datetime(self.period_end_date)

    def as_list(self):
        return list(self.__dict__.values())

    def download_and_save_filing_html(
        self,
        client: requests.Session,
        resolve_urls: bool = True,
        overwrite:bool= True, # Change this default to False later on
    ) -> None:

        """This"""

        # Only grab the new report and save it if we don't already have it, or if we want to overwrite what's already there.
        if not self.save_path.exists() or overwrite:

            # header is needed to declare who you are to Edgar's server
            headers = {
                "User-Agent": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate",
                "Host": "www.sec.gov",
            }

            # This is what actually downloads the html from Edgar
            resp = client.get(self.filing_details_url, headers=headers,timeout=1)
            resp.raise_for_status()
            filing_text = resp.content

            # Only resolve URLs in HTML files
            if resolve_urls and self.save_path.suffix == ".html":
                filing_text = resolve_relative_urls_in_filing(filing_text, self.filing_details_url)

            # If the directory doesn't exists, create it.
            self.save_path.parent.mkdir(parents=True, exist_ok=True)
            # Save the document

            # If path does not exists
            self.save_path.write_bytes(filing_text)

            # Prevent rate limiting and don't be a dick to edgar
            time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)

    def get_report(self,resolve_urls=True,type='raw'):
        """ type options are 'soup, raw, and text'. This returns 'none' if the scraping fails. """

        client = requests.Session()
        adapter = HTTPAdapter(max_retries=retries)
        adapter.max_retries.respect_retry_after_header = False
        client.mount("http://", adapter)

        filing_text = None # Return none if the scraping fails.

        headers = {
                "User-Agent": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate",
                "Host": "www.sec.gov",
            }

        try:
             # This is what actually downloads the html from Edgar
            resp = client.get(self.filing_details_url, headers=headers,timeout=1)
            resp.raise_for_status()
            filing_text = resp.content

            # Only resolve URLs in HTML files
            if resolve_urls and self.save_path.suffix == ".html":
                filing_text = resolve_relative_urls_in_filing(filing_text, self.filing_details_url)

            time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)
            
        except requests.exceptions.HTTPError as e:  # pragma: no cover
            pass


        finally:
            client.close()

        if filing_text is not None:
            if type == 'raw':
                return filing_text
            elif type == 'soup':
                return BeautifulSoup(filing_text,"lxml")
            elif type == 'text':
                soup = BeautifulSoup(filing_text,"lxml")
                return soup.get_text()
            else:
                raise Exception(f'Sorry, invalid type. Must be one of: "soup, raw, or text" ') 

        return None


    def download_and_save_filing_to_drive(
        self,
        client: requests.Session,
        drive,
        resolve_urls: bool = True,
        overwrite:bool= True, # Change this default to False later on
    ) -> None:

        """This"""
        # Exception handling is being done in the function that calls this

        # Check to see if the file is already in google drive or if overwrite is ok
        if True or overwrite: # or file does not exists in drive already <-- add that in later
            # If so, then do this good stuff
            # header is needed to declare who you are to Edgar's server

            headers = {
                "User-Agent": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate",
                "Host": "www.sec.gov",
            }

            # This is what actually downloads the html from Edgar
            resp = client.get(self.filing_details_url, headers=headers,timeout=1)
            resp.raise_for_status()
            filing_html = resp.content

             # Only resolve URLs in HTML files
            if resolve_urls and self.save_path.suffix == ".html":
                filing_html = resolve_relative_urls_in_filing(filing_html, self.filing_details_url)

            filing_text = BeautifulSoup(filing_html,"lxml").get_text()

            # gfile = drive.CreateFile({'parents': [{'id': '1PJJApvb0yby5zCJ2XRwI_OjYhQMRqCOZ'}],'title':f'{self.file_name_txt}'}) # this is the actual edgar_filings folder
            gfile = drive.CreateFile({'parents': [{'id': '1bCHSa7AO0jfQRoKYFisA_pgzhsz2uKbs'}],'title':f'{self.file_name_txt}'}) # this is the test folder
            # Read file and set it as the content of this instance.
            # gfile.SetContentFile(upload_file)
            gfile.SetContentString(filing_text) # Is converting to string the best option?
            gfile.Upload() # Upload the file.

            # Prevent rate limiting and don't be a dick to edgar
            time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)

    def download_and_save_filing_text_to_drive(
        self,
        client: requests.Session,
        drive
    ) -> None:

        """Only save the filing text to drive. not html"""
        # Exception handling is being done in the function that calls this
        
        # header is needed to declare who you are to Edgar's server
        headers = {
            "User-Agent": generate_random_user_agent(),
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov",
        }

        # This is what actually downloads the html from Edgar
        resp = client.get(self.filing_details_url, headers=headers,timeout=1) # If you don't specifiy a timeout value, you could get stuck
        resp.raise_for_status()
        filing_html = resp.content

        filing_text = BeautifulSoup(filing_html,"lxml").get_text()

        # gfile = drive.CreateFile({'parents': [{'id': '1PJJApvb0yby5zCJ2XRwI_OjYhQMRqCOZ'}],'title':f'{self.file_name}','mimeType':'html'})
        # gfile = drive.CreateFile({'parents': [{'id': '1bCHSa7AO0jfQRoKYFisA_pgzhsz2uKbs'}],'title':f'{self.file_name_txt}'})
        
        gfile = drive.CreateFile({'parents': [{'id': '1PJJApvb0yby5zCJ2XRwI_OjYhQMRqCOZ'}],'title':f'{self.file_name_txt}'})
        gfile.SetContentString(filing_text) 
        gfile.Upload() # Upload the file.

        # Prevent rate limiting and don't be a dick to edgar
        time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)


# Object for generating fake user-agent strings
fake = Faker()

# Specify max number of request retries
# https://stackoverflow.com/a/35504626/3820660

retries = Retry(
    total=MAX_RETRIES,
    backoff_factor=SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL,
    status_forcelist=[403, 500, 502, 503, 504],
)


def validate_date_format(date_format: str) -> None:
    error_msg_base = "Please enter a date string of the form YYYY-MM-DD."

    if not isinstance(date_format, str):
        raise TypeError(error_msg_base)

    try:
        datetime.strptime(date_format, DATE_FORMAT_TOKENS)
    except ValueError as exc:
        # Re-raise with custom error message
        raise ValueError(f"Incorrect date format. {error_msg_base}") from exc


def form_request_payload(
    ticker_or_cik: str,
    filing_types: List[str],
    start_date: str,
    end_date: str,
    start_index: int,
    query: str,
) -> dict:
    payload = {
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
        "entityName": ticker_or_cik,
        "forms": filing_types,
        "from": start_index,
        "q": query,
    }

    return payload


def build_filing_metadata_from_hit(hit: dict) -> Filing:

    filing = Filing()
    filing.hit = hit
    # The "_id" items contains the important detail we need to figure out the url
    filing.accession_number, filing_details_filename = hit["_id"].split(":", 1)

    # Company CIK should be last in the CIK list. This list may also include
    # the CIKs of executives carrying out insider transactions like in form 4.
    filing.cik = hit["_source"]["ciks"][-1]
    accession_number_no_dashes = filing.accession_number.replace("-", "", 2) # We need the accession number without dashes to build url below
    filing.period_end_date = hit['_source']['period_ending']
    filing.edgar_name = hit["_source"]['display_names'][-1] # This contains a list of display names
    filing.file_date = hit["_source"]['file_date']


    submission_base_url = (f"{SEC_EDGAR_ARCHIVES_BASE_URL}/{filing.cik}/{accession_number_no_dashes}")

    # This is the url for the file that contains data the difficult to parse, complete file
    filing.full_submission_url = f"{submission_base_url}/{filing.accession_number}.txt" 

    # Url for the html file that we want
    filing.filing_details_url = f"{submission_base_url}/{filing_details_filename}"

    return filing



def build_filings(
    filing_type: str,
    ticker_or_cik: str,
    num_filings_to_download: int,
    after_date: str,
    before_date: str,
    include_amends: bool,
    query: str = "",
) -> List[Filing]:

    filings_to_fetch: List[Filing] = []
    start_index = 0

    client = requests.Session()
    client.mount("http://", HTTPAdapter(max_retries=retries))
    
    try:
        while len(filings_to_fetch) < num_filings_to_download:
            payload = form_request_payload(
                ticker_or_cik,
                [filing_type],
                after_date,
                before_date,
                start_index,
                query,
            )
            headers = {
                "User-Agent": generate_random_user_agent(),
                "Accept-Encoding": "gzip, deflate",
                "Host": "efts.sec.gov",
            }
            resp = client.post(
                SEC_EDGAR_SEARCH_API_ENDPOINT, json=payload, headers=headers
            )
            resp.raise_for_status()
            search_query_results = resp.json()

            if "error" in search_query_results:
                try:
                    root_cause = search_query_results["error"]["root_cause"]
                    if not root_cause:  # pragma: no cover
                        raise ValueError

                    error_reason = root_cause[0]["reason"]
                    raise EdgarSearchApiError(
                        f"Edgar Search API encountered an error: {error_reason}. "
                        f"Request payload:\n{payload}"
                    )
                except (ValueError, KeyError):  # pragma: no cover
                    raise EdgarSearchApiError(
                        "Edgar Search API encountered an unknown error. "
                        f"Request payload:\n{payload}"
                    ) from None

            query_hits = search_query_results["hits"]["hits"]

            # No more results to process
            if not query_hits:
                break

            for hit in query_hits:
                hit_filing_type = hit["_source"]["file_type"]

                is_amend = hit_filing_type[-2:] == "/A"
                if not include_amends and is_amend:
                    continue

                # Work around bug where incorrect filings are sometimes included.
                # For example, AAPL 8-K searches include N-Q entries.
                if not is_amend and hit_filing_type != filing_type:
                    continue

                metadata = build_filing_metadata_from_hit(hit)
                filings_to_fetch.append(metadata)

                if len(filings_to_fetch) == num_filings_to_download:
                    return filings_to_fetch

            # Edgar queries 100 entries at a time, but it is best to set this
            # from the response payload in case it changes in the future
            query_size = search_query_results["query"]["size"]
            start_index += query_size

            # Prevent rate limiting & don't be a dick to Edgar's website
            time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)
    finally:
        client.close()

    return filings_to_fetch


def resolve_relative_urls_in_filing(filing_text: str, download_url: str) -> str:
    """Makes sure the urls embedded in the html work, even when you're not viewing the document on edgar's site"""
    soup = BeautifulSoup(filing_text, "lxml")
    base_url = f"{download_url.rsplit('/', 1)[0]}/"

    for url in soup.find_all("a", href=True):
        # Do not resolve a URL if it is a fragment or it already contains a full URL
        if url["href"].startswith("#") or url["href"].startswith("http"):
            continue
        url["href"] = urljoin(base_url, url["href"])

    for image in soup.find_all("img", src=True):
        image["src"] = urljoin(base_url, image["src"])

    if soup.original_encoding is None:  # pragma: no cover
        return soup

    return soup.encode(soup.original_encoding)





def download_filings(
    filings_to_fetch: List[Filing],
    log_df:pd.DataFrame,
    location:str='local',
    drive=None,
) -> None:

    client = requests.Session()
    adapter = HTTPAdapter(max_retries=retries)
    adapter.max_retries.respect_retry_after_header = False
    client.mount("http://", adapter)
    
    try:
        for filing in filings_to_fetch:

            if filing_in_drive(filing,log_df):
                continue # Don't download the report again if we already have it
                # This is bad code and needs to be refactored. It doesn't distinguish between txt reports and html
                # It also might be worth filtering out the former failures so that we try them again.

            this_filing_log:list = filing.as_list()

            try:
                if location.lower() == 'local':
                    filing.download_and_save_filing_html(client)
                    this_filing_log.append(True)
                elif location.lower() == 'drive':
                    if drive is None:
                        raise Exception('Yo, you need to provide a drive object to this function if you want to save the filings to the google drive.')
                    filing.download_and_save_filing_text_to_drive(client,drive)
                    this_filing_log.append(True)
                else:
                    this_filing_log.append(False)
                    raise Exception('Sorry bro, not a valid destination. It should be either "drive" or "local"')
                
            except Exception as e:  
                this_filing_log.append(False)
                print(f'Error in the attemmpt to download company:{filing.edgar_name} for {filing.file_name}. Exception message = {str(e)}')

            finally:
                # Make sure to record the attempt
                new_log_row = pd.Series(data=this_filing_log,index=log_df.columns)
                log_df.loc[len(log_df)] = new_log_row
                # log_df = log_df.append(new_log_row,ignore_index=True)

    finally:
        client.close()




def get_number_of_unique_filings(filings: List[Filing]) -> int:
    return len({metadata.accession_number for metadata in filings})


def generate_random_user_agent() -> str:
    return f"{fake.first_name()} {fake.last_name()} {fake.email()}"


def is_cik(ticker_or_cik: str) -> bool:
    try:
        int(ticker_or_cik)
        return True
    except ValueError:
        return False
