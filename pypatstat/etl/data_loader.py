from utils import login
from utils import _zipfiles_on_pages
from utils import files_in_zipfile
from schema_maker import generate_schema
from schema_maker import INDEX_DOC_STR
from pydoc import locate
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists
from sqlalchemy_utils import create_database
import logging
from io import BytesIO
import pandas as pd


def iterchunks(zipped_csv, chunksize=1000):
    """Iterate through a zipped CSV file in chunks

    Args:
        zipped_csv (ZipFile): A zipped CSV file object.
        chunksize (int): Size parameter to pass to :obj:`pd.read_csv`.
    Yields:
        rows (list): Rows of the CSV.
    """
    with BytesIO(zipped_csv.read()) as zio:
        for _, f in files_in_zipfile(zio):
            for chunk in pd.read_csv(f, chunksize=chunksize):
                rows = []
                for idx, row in chunk.iterrows():
                    row = {k:(v if not pd.isnull(v) else None)
                           for k, v in row.items()}
                    rows.append(row)
                yield rows


def get_class_by_tablename(Base, tablename):
    """Return class reference mapped to table.
    Args:
        Base: SQLalchemy ORM Base object.
        tablename (str): Prefix of table name.
    Returns:
        reference to table model.
    """
    for c in Base._decl_class_registry.values():
        try:
            if c.__tablename__.split("_")[0] == tablename:
                return c
        except AttributeError:
            pass
    raise NameError(tablename)


def try_until_allowed(f, *args, **kwargs):
    '''Keep trying a function if a OperationalError is raised.
    Specifically meant for handling too many
    connections to a database.
    Args:
        f (:obj:`function`): A function to keep trying.
    '''
    while True:
        try:
            value = f(*args, **kwargs)
        except OperationalError:
            logging.warning("Waiting on OperationalError")
            time.sleep(5)
            continue
        else:
            return value


def write_to_db(db_url, Base, _class, rows, create_db=True, core_insert=True):
    """Bulk write rows of data to the database.

    Args:
        db_url (str): Database connection string.
        Base: SQLalchemy ORM Base object.
        _class: SQLalchemy ORM object.
        rows (list): Rows of data (:obj:`dict` format) to write.
        create_db (bool): Create the database if it doesn't exist?
    """
    # Create the DB if required
    engine = create_engine(db_url)
    if not database_exists(engine.url):
        create_database(engine.url)

    # Create the tables
    try_until_allowed(Base.metadata.create_all, engine)

    # Insert the data
    if core_insert:
        conn = engine.connect()
        conn.execute(_class.__table__.insert(), rows)
        conn.close()
        del conn
    else:
        entries = [_class(**row) for row in rows]
        Session = try_until_allowed(sessionmaker, engine)
        session = try_until_allowed(Session)
        session.bulk_save_objects(entries)
        session.commit()
        session.close()
        del session
    del engine


def zipfile_to_db(zipfile, db_url, Base, chunksize=1000, 
                  skip_fnames=[]):
    """Write a zipfile contents (assumed zipped CSV) to a database.

    Args:
        zipfile (ZipFile): A zipfile, assumed to contained zipped CSVs.
        db_url (str): Database connection string.
        Base: SQLalchemy ORM Base object.
        chunksize (int): Size parameter to pass to :obj:`pd.read_csv`.
    """
    for fname, f, zf in files_in_zipfile(zipfile, skip_fnames=skip_fnames,
                                         yield_zipfile_too=True):
        logging.info(f"\tProcessing nested file {fname}...")
        tablename = fname.split("_")[0]
        _class = get_class_by_tablename(Base, tablename)
        logging.info(f"\t\tRetrieved class from table name {tablename}.")
        i = 0
        with zf.open(fname) as z:
            for rows in iterchunks(z, chunksize=chunksize):
                i+=len(rows)
                write_to_db(db_url, Base, _class, rows)
        logging.info(f"\t\tWritten {i} entries for {tablename}.")


def _download_patstat_to_db(db_url, Base, chunksize=1000,
                            skip_fnames=[], **session_credentials):
    """Download all patstat global data and write to a database.

    Args:
        session (:obj:`requests.Session`): A requests session logged into the Patstat website.
        db_url (str): Database connection string.
        Base: SQLalchemy ORM Base object.
        chunksize (int): Size parameter to pass to :obj:`pd.read_csv`.
    """
    for url, zipfile in _zipfiles_on_pages(**session_credentials):
        if INDEX_DOC_STR in url:
            continue
        logging.info(f"Processing file {url}...")
        zipfile_to_db(zipfile, db_url, Base, chunksize=chunksize, 
                      skip_fnames=skip_fnames)


def download_patstat_to_db(patstat_usr, patstat_pwd, db_url, chunksize=1000,
                           skip_fnames=[]):
    """Automatically generate PATSTAT database and tables and populate 
    all tables in memory.

    Args:
        patstat_{usr, pwd} (str): PATSTAT username and password.
        db_url (str): Database connection string.
        chunksize (int): Size parameter to pass to :obj:`pd.read_csv`.
    """
    # Log into the PATSTAT website
    session = login(username=patstat_usr, pwd=patstat_pwd)
    logging.info("Downloading and generating the schema...")
    # Generate the PATSTAT Global schema
    db_suffix = generate_schema(session)
    db_url=f"{db_url}/patstat_{db_suffix}"
    logging.info(f"Generated the schema for {db_suffix}. "
                 f"A database will be created at {db_url}")
    # Download the data and populate the database
    _download_patstat_to_db(db_url=db_url, chunksize=chunksize,
                            Base=locate(f'orms.patstat_{db_suffix}.Base'),
                            skip_fnames=skip_fnames, username=patstat_usr, pwd=patstat_pwd)


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    download_patstat_to_db("MY_EMAIL", "MY_PATSTAT_PASSWORD",
                           ("mysql+pymysql://USERNAME:PASSWORD@"
                            "DB_PATH"
                            ".eu-west-2.rds.amazonaws.com"), chunksize=10000,
                           skip_fnames=['tls2','tls8','tls901','tls902','tls904'])
                           #skip_fnames=['tls201', 'tls202', 'tls203',
                           #             'tls204', 'tls205'])
