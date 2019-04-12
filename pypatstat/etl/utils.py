from zipfile import ZipFile
from io import BytesIO
from requests import session
from bs4 import BeautifulSoup

TOP_URL="https://publication.epo.org/raw-data"
AUTH_URL=f"{TOP_URL}/authentication"
RAW_DATA_URL=f"{TOP_URL}/product?productId=86"

def login(username, pwd):
    s = session()    
    r = s.post(AUTH_URL, data=dict(action=1, submit="Log in",
                                   login=username, pwd=pwd))
    if username not in r.text:
        raise ValueError(f"Invalid login credentials for {AUTH_URL}")
    r.raise_for_status()
    return s

def zipfiles_on_pages(s):
    r = s.get(RAW_DATA_URL)
    soup = BeautifulSoup(r.text, "lxml")
    for anchor in soup.find_all("a", href=True):
        url = anchor["href"]
        if not (url.endswith(".zip") and url.startswith("download")):
            continue
        yield (url, _zipfile_from_url(s, url))

def _zipfile_from_url(s, url, chunk_size=2**25):  # Around 30MB
    r = s.get(f"{TOP_URL}/{url}", stream=True)
    file_handle = BytesIO()
    for chunk in r.iter_content(chunk_size):
        file_handle.write(chunk)
    return file_handle
        
def files_in_zipfile(bio):
    zf = ZipFile(bio)
    for zipinfo in zf.infolist():
        with zf.open(zipinfo) as f:
            yield (zipinfo.filename, f)
    zf.close()
    bio.close()
