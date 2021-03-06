from zipfile import ZipFile
from zipfile import BadZipFile
from io import BytesIO
from requests import session
from bs4 import BeautifulSoup
import logging

TOP_URL="https://publication.epo.org/raw-data"
AUTH_URL=f"{TOP_URL}/authentication"
RAW_DATA_URL=f"{TOP_URL}/product?productId=86"

def login(username, pwd):
    """Log into your PATSTAT account and setup a session"""
    s = session()    
    r = s.post(AUTH_URL, data=dict(action=1, submit="Log in",
                                   login=username, pwd=pwd))
    if username not in r.text:
        raise ValueError(f"Invalid login credentials for {AUTH_URL}")
    r.raise_for_status()
    return s


def _zipfiles_on_pages(download_suffix='', **credentials):
    """Retrieve a list of all zipfiles"""
    s = login(**credentials)
    r = s.get(RAW_DATA_URL, stream=True)
    soup = BeautifulSoup(r.text, "lxml")
    for anchor in soup.find_all("a", href=True):
        url = anchor["href"]
        if not (url.endswith(".zip") and url.startswith("download")):
            continue        
        if not url.endswith(download_suffix):
            logging.info(f'Skipping {url}')
            continue
        s = login(**credentials)
        yield (url, _zipfile_from_url(s, url))


def zipfiles_on_pages(s):
    """Retrieve a list of all zipfiles"""
    r = s.get(RAW_DATA_URL)
    soup = BeautifulSoup(r.text, "lxml")
    for anchor in soup.find_all("a", href=True):
        url = anchor["href"]
        if not (url.endswith(".zip") and url.startswith("download")):
            continue
        yield (url, _zipfile_from_url(s, url))


def _zipfile_from_url(s, url, chunk_size=2**25):  # Around 30MB
    """Retrieve a zipfile"""
    r = s.get(f"{TOP_URL}/{url}", stream=True)
    file_handle = BytesIO()
    for chunk in r.iter_content(chunk_size):
        file_handle.write(chunk)
    return file_handle

        
def files_in_zipfile(bio, skip_table_prefixes=[], yield_zipfile_too=False):
    """Yield individual files from the zipfile"""
    try:
        zf = ZipFile(bio)
    except BadZipFile:
        bio.close()
        return
    
    for zipinfo in zf.infolist():
        if any(zipinfo.filename.startswith(fn) for fn in skip_table_prefixes):
            logging.info(f"\t\tSkipping {zipinfo.filename}")
            continue
        with zf.open(zipinfo) as f:
            if yield_zipfile_too:
                yield (zipinfo.filename, f, zf)
            else:
                yield (zipinfo.filename, f)
    zf.close()
    bio.close()
