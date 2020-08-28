import pytest
from unittest import mock

from utils import login
from utils import zipfiles_on_pages
from utils import files_in_zipfile

from requests import Session

USERNAME = "username@something.com"

@mock.patch.object(Session, "post")
def test_good_login(mocked_post):
    mocked_post.return_value.text = f"blah blah {USERNAME} blah blah"
    s = login(USERNAME, "mypassword")
    assert type(s) == Session

@mock.patch.object(Session, "post")
def test_bad_login(mocked_post):
    mocked_post.return_value.text = f"blah blah blah blah"
    with pytest.raises(ValueError):
        s = login(USERNAME, "mypassword")

@mock.patch("utils._zipfile_from_url")
def test_zipfiles_on_pages(mocked_zf):
    # Only want URLs in the form of `good_zip_url` to be counted
    good_zip_url = "<a href='download/something.zip'></a>"
    bad_zip_url_1 = "<a href='download/something.text'></a>"
    bad_zip_url_2 = "<a href='something/something.zip'></a>"

    # How many of each type of URL. Deliberately awkward numbers.
    i_good = 53
    i_bad = 1428

    # Mock up the session
    mocked_get = mock.MagicMock()
    mocked_get.return_value.text = good_zip_url*i_good + (bad_zip_url_1 + bad_zip_url_2)*i_bad
    mocked_session = mock.MagicMock(get=mocked_get)

    # Execute the test: expect to find only the good URLs
    for i, (url, zf) in enumerate(zipfiles_on_pages(mocked_session)):
        pass
    assert i == i_good-1


@mock.patch("utils.ZipFile")
def test_files_in_zipfile(mocked_zf):
    # Mock up the zipfiles
    n_zips = 2431
    infolist = [mock.MagicMock(filename=f'dummy{i}.txt') for i in range(n_zips)]
    mocked_infolist = mock.MagicMock(return_value=infolist)
    mocked_zf.return_value = mock.MagicMock(infolist=mocked_infolist)
    
    # Execute the test: expect to find `n_zips`
    for i, (filename, f) in enumerate(files_in_zipfile(mocked_zf)):
        assert filename == f'dummy{i}.txt'
    assert i == n_zips-1

