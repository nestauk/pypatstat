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


def make_pk(row, _class):
    pkey_cols = _class.__table__.primary_key.columns
    pk = tuple([row[pkey.name]                       # Cast to str since
                if pkey.type.python_type is not str  # pd can wrongly guess
                else str(row[pkey.name])             # the type as int
                for pkey in pkey_cols])
    return pk


def pk_chunks(session, _class, chunksize=100000):
    pkey_cols = _class.__table__.primary_key.columns
    fields = [getattr(_class, pkey.name)
              for pkey in pkey_cols]
    q = session.query(*fields)
    offset = 0
    while offset == 0 or len(pks) == chunksize:
        pks = q.limit(chunksize).offset(offset).all()
        yield set(pks)
        offset += chunksize


def write_to_db(db_url, Base, _class, rows, create_db=True, 
                filter_pks=True):
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
    engine.execution_options(stream_results=True)

    # Create the tables
    try_until_allowed(Base.metadata.create_all, engine)

    # Filter results if already in the db 
    if filter_pks:
        logging.info('Will filter PKs before inserting data')
        Session = try_until_allowed(sessionmaker, engine)
        session = try_until_allowed(Session)
        # Filter results if already in the db
        # (this might look like a complicated setup,
        # but its super fast and super memory efficient)
        pks = [make_pk(row, _class) for row in rows]
        logging.info('Generated new PKs')
        new_pks = set(pks)
        for old_pks in pk_chunks(session, _class):
            new_pks = new_pks - old_pks # remove done pks
        rows = [rows for pk, row in zip(pks, rows)
                if pk in new_pks]
        logging.info(f'Removing {len(rows) - len(pks)} '
                     'rows before insert.')
        session.close()
        del session

    # Insert the data
    conn = engine.connect()
    conn.execute(_class.__table__.insert(), rows)
    conn.close()
    del conn
    del engine


def zipfile_to_db(zipfile, db_url, Base, chunksize=1000, 
                  skip_fnames=[], restart_filename=None):
    """Write a zipfile contents (assumed zipped CSV) to a database.

    Args:
        zipfile (ZipFile): A zipfile, assumed to contained zipped CSVs.
        db_url (str): Database connection string.
        Base: SQLalchemy ORM Base object.
        chunksize (int): Size parameter to pass to :obj:`pd.read_csv`.
    """
    start = bool(restart_filename is None)    
    for fname, f, zf in files_in_zipfile(zipfile, skip_fnames=skip_fnames,
                                         yield_zipfile_too=True):
        restarting = (not start) and restart_filename in fname
        if (not start) and restarting:
            start = True
        if not start:
            logging.info(f"\tSkipping file {fname}...")
            continue

        logging.info(f"\tProcessing nested file {fname}...")
        tablename = fname.split("_")[0]
        _class = get_class_by_tablename(Base, tablename)
        logging.info(f"\t\tRetrieved class from table name {tablename}.")
        i = 0
        with zf.open(fname) as z:
            for rows in iterchunks(z, chunksize=chunksize):
                i+=len(rows)
                write_to_db(db_url, Base, _class, rows,
                            filter_pks=restarting)
        logging.info(f"\t\tWritten {i} entries for {tablename}.")


def _download_patstat_to_db(db_url, Base, chunksize=1000,
                            skip_fnames=[], restart_filename=None,
                            n_jobs=1, start_from='',
                            **session_credentials):
    """Download all patstat global data and write to a database.

    Args:
        session (:obj:`requests.Session`): A requests session logged into the Patstat website.
        db_url (str): Database connection string.
        Base: SQLalchemy ORM Base object.
        chunksize (int): Size parameter to pass to :obj:`pd.read_csv`.
    """
    # rf = restart_filename
    # if n_jobs > 1:
    #     Parallel(n_jobs=n_jobs)(delayed(zipfile_to_db)(zipfile, db_url, 
    #                                                    Base, chunksize=chunksize,
    #                                                    skip_fnames=skip_fnames,
    #                                                    restart_filename=rf)
    #                             for url, zipfile 
    #                             in _zipfiles_on_pages(**session_credentials)
    #                             if INDEX_DOC_STR not in url)
    #     return

    # TODO: ugly repetition
    for url, zipfile in _zipfiles_on_pages(start_from=start_from, 
                                           **session_credentials):
        if INDEX_DOC_STR in url:
            continue
        logging.info(f"Processing file {url}...")
        zipfile_to_db(zipfile, db_url, Base, chunksize=chunksize, 
                      skip_fnames=skip_fnames, 
                      restart_filename=restart_filename)


def download_patstat_to_db(patstat_usr, patstat_pwd, db_url, 
                           chunksize=1000, skip_fnames=[],
                           n_jobs=1, start_from='',
                           restart_filename=None):
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
                            skip_fnames=skip_fnames, 
                            restart_filename=restart_filename,
                            n_jobs=n_jobs,
                            start_from=start_from,
                            username=patstat_usr, 
                            pwd=patstat_pwd)


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    start_from = '_06.zip'
    download_patstat_to_db("@nesta.org.uk", "",
                           ("mysql+pymysql://@"
                            ".ci9272ypsbyf"
                            ".eu-west-2.rds.amazonaws.com"), chunksize=10000,
                           skip_fnames=['tls20', 'tls21'],
                           start_from=start_from,
                           n_jobs=1)
