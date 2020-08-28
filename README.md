# pypatstat

Tools for loading and retrieving PATSTAT's Global data into *any* SQL database, without having to click anything ever.

## Usage:

```python
from pypatstat import download_patstat_to_db

email = "MY_PATSTAT_EMAIL@SOMETHING.com"  # The email address you have which is registered with PATSTAT
password = "MY_PATSTAT_PASSWORD"  # The password PATSTAT gave you for your paid access to PATSTAT Global
db_url = "mysql+pymysql://USERNAME:PASSWORD@MY_DB_ADDRESS" # DB connection URL

download_patstat_to_db(email, password, db_url)  # <--- will take a couple of days. I suggest running in tmux or similar.
```


## Advanced usage:

In addition to the above setup, you may consider using the arguments:

* `chunksize (int)`: Size of the chunks you write to the database. Increase with caution.
* `skip_table_prefixes (list of str)`: Skip table names with these prefixes.
* `download_suffix (str)`: Only download a file with this suffix.

For example:

```python
download_suffix = '_09.zip'  # If specified, only download file names with this suffix                                        
skip_table_prefixes=['tls20', 'tls21']  # By table name prefixes to skip                                                 
download_patstat_to_db("MY_PATSTAT_EMAIL@SOMETHING.com", "MY_PATSTAT_PASSWORD",
                       "mysql+pymysql://USERNAME:PASSWORD@MY_DB_ADDRESS",
                       chunksize=10000,  # Don't make this too big                                                            
                       skip_tables=skip_tables,
                       download_suffix=download_suffix)
```
