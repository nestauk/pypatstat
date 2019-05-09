from utils import zipfiles_on_pages
from utils import files_in_zipfile

from collections import defaultdict
import re

START_PATTERN = "CREATE TABLE"
END_PATTERN = "PRIMARY KEY CLUSTERED"
SQL_FIELD_REGEX_1 = "\[(\w+)\] \[(\w+)\]\((\w+)\)"
SQL_FIELD_REGEX_2 = "\[(\w+)\] \[(\w+)\]"
SQL_FIELD_DEFAULT = "('.*?')"
SQL_FIELD_PKEY = "\[(\w+)\]"
SQL_TABLE_NAME = "Table \[dbo\]\.\[(.*)\]"

INDEX_DOC_STR = 'index_documentation_scripts'


def extract_datestamp(url):
    """Extract datestamp from the PATSTAT file URL.
    
    Args:
        url (str): PATSTAT file URL
    Returns:
        datastamp (str): Date formatted as "%Y_%m_%d"
    """
    numbers = re.findall("(\d+)", url)
    return "_".join(numbers[0:3])


def _get_field_data(sql_line):
    """Extract field information from a line of SQL CREATE TABLE code.
    
    Args:
        sql_line (str): A line of SQL CREATE TABLE code.
    Returns:
        field_info (tuple): Field name, field type, field length and default value.
    """
    # Try to extract the field name, type and length
    results = re.findall(SQL_FIELD_REGEX_1, sql_line)
    if len(results) == 0:
        field_name, field_type = re.findall(SQL_FIELD_REGEX_2, sql_line)[0]
        field_length = None
    else:        
        field_name, field_type, field_length = results[0]
        # Convert numeric lengths
        if field_length.isdigit():
            field_length = int(field_length)
        # MSSS 'max' value is ok, but anything else isn't anticipated
        if not (type(field_length) is int or field_length.lower() == "max"):
            raise ValueError(f"Unexpected field length for {field_type.upper()}: "
                             f"'{field_length}'")
    # Find the default value, if any
    default_value = None
    result = re.findall(SQL_FIELD_DEFAULT, sql_line)
    if len(result) > 0:
        default_value = result[0]
        # Strip out quotes if this is a number
        if default_value.replace("'","").isdigit():
            default_value = int(default_value.replace("'",""))
    return (field_name.lower(), field_type, field_length, default_value)

def _get_pkey_field(sql_line):
    """Extract the field name if this is a primary key.
    
    Args:
        sql_line (str): A line of SQL CREATE TABLE code.
    Returns:
        results (list): A list of found primary key fields.
    """
    results = re.findall(SQL_FIELD_PKEY, sql_line)[0]
    return results


def get_index_doc(s):
    """Scan forwards for the PATSTAT index document, which contains the schema.
    
    Args:
        s (:obj:`requests.session`): A requests session, logged into the PATSTAT website.
    Returns:
        info (tuple): URL and ZipFile corresponding to the PATSTAT index document.
    """
    for url, zipfile in zipfiles_on_pages(s):
        if INDEX_DOC_STR in url:
            break
    return url, zipfile


def get_sql_data(zipfile):
    """Extract all SQL creation scripts from the PATSTAT index zipfile.
    
    Args:
        zipfile (ZipFile): The PATSTAT index zipfile.
    Returns:
        sql_data (dict): The SQL creation scripts organised by table type.
    """
    sql_data = defaultdict(dict)
    for fname, f in files_in_zipfile(zipfile):
        if not (fname.startswith("CreateScripts") and fname.endswith(".sql")):
            continue
        if "tls" not in fname:
            continue
        _, table_type, table_name = fname.split("/")
        sql_data[table_type][table_name] = f.read().decode()
    return sql_data

def parse_sql_table_fields(sql_table_text):
    """ """
    field_data = {}
    pkeys = []
    start, end = False, False
    for line in sql_table_text.split("\n"):    
        if START_PATTERN in line:
            start = True
            continue
        if END_PATTERN in line:
            start = False
            end = True
            continue

        if not line.startswith("\t"):
            continue
        if not (start or end):
            continue

        if start:
            field_name, field_type, field_length, default_value = _get_field_data(line)
            field_data[field_name] = (field_type, field_length, default_value)
        if end:
            field_name = _get_pkey_field(line)
            pkeys.append(field_name.lower())

    if len(pkeys) == 0:
        raise ValueError(f"No primary keys found in {sql_table_text}")

    return field_data, pkeys

def generate_model_text(table_name, field_data, pkeys, 
                        default_field_length=100000):  ## Allows MySQL to default to MEDIUMTEXT
    types = []
    model_text = (f"class {table_name.title().replace('_','')}(Base):\n"
                  f"\t__tablename__ = '{table_name}'\n")
    for field_name, (field_type, field_length, default_value) in field_data.items():
        if field_type.upper() == "TINYINT":
            field_type = "SMALLINT"

        text = f"\t{field_name} = Column({field_type.upper()}"
        if field_length is not None:
            if type(field_length) is str and field_length.lower() == "max":
                field_length = default_field_length
            text += f"({field_length})"
        if field_name in pkeys:
            text += ", primary_key=True"
        if default_value is not None:
            text += f", default={default_value}"
        text += ")\n"
        model_text += text
        types.append(field_type.upper())
    return model_text, types

def generate_orm_head(types):
    text = "'''Automatically generated by pypatstat "
    text += "(https://github.com/nestauk/pypatstat)'''\n\n"
    text += "from sqlalchemy.ext.declarative import declarative_base\n"
    text += "from sqlalchemy import Column\n"
    text += f"from sqlalchemy.types import {','.join(set(types))}\n\n"
    text += "Base = declarative_base()\n\n"
    return text


def get_sql_table_name(sql_table_text):
    return re.findall(SQL_TABLE_NAME, sql_table_text)[0]

def generate_schema(session):
    url, zipfile = get_index_doc(session)  
    db_suffix = extract_datestamp(url)
    sql_data = get_sql_data(zipfile)
    
    all_model_texts = []
    types = []
    for sql_table_text in sql_data['CreateTableScripts'].values():
        field_data, pkeys = parse_sql_table_fields(sql_table_text)
        table_name = get_sql_table_name(sql_table_text)
        model_text, _types = generate_model_text(table_name, field_data, pkeys)
        types += _types
        all_model_texts.append(model_text)
        
    head = generate_orm_head(types)
    orm_text = head + "\n\n".join(all_model_texts)
    
    with open(f"orms/patstat_{db_suffix}.py", "w") as f:
        f.write(orm_text)
    return db_suffix
    

if __name__ == "__main__":
    from utils import login
    session = login(username="soraya.rusmaully@nesta.org.uk", pwd="6-Ttybw0LgNC")
    db_suffix = generate_schema(session)

#session = login(username="soraya.rusmaully@nesta.org.uk", pwd="6-Ttybw0LgNC")
#url, zipfile = get_index_doc(session)
#db_suffix = extract_datestamp(url)
#sql_data = get_sql_data(zipfile)
