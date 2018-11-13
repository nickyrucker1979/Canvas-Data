# Script that reads the files in the imported folder and creates a sql import sql

import os
import pandas as pd
import exasol as e
import config_Exasol as ec
import time
import datetime

exaconnect = e.connect(
            dsn=ec.dsn,
            DRIVER=ec.DRIVER,
            EXAHOST=ec.EXAHOST,
            EXAUID=ec.EXAUID,
            EXAPWD=ec.EXAPWD,
            autocommit=True
            )

now = datetime.datetime.now()
exasol_import_schema = 'CANVAS_DATA_STG.'
nightly_dump_directory = './nightly_data'
path = '/Users/ruckern/CU_Online_Marketing_Scripts/CanvasData_Production/nightly_data/'
sql_script_file = "canvas_import.sql"

def truncate_sql_file():
    open(sql_script_file, 'w').close()

def truncate_stg_tables(file_name):
    table_name = file_name.split(".", 1)[0]
    exasol_import_db = exasol_import_schema + table_name
    if table_name == 'requests':
        # Requests file is not a full dump, incremental changes only - do not truncate this table
        pass;
    else:
        truncate_script = 'TRUNCATE TABLE ' + exasol_import_db + ';\n'
        with open(sql_script_file, "a") as myfile:
            myfile.write(truncate_script)

def import_file_to_canvas(file_name):
    # split file name to identify table
    table_name = file_name.split(".", 1)[0]
    exasol_import_db = exasol_import_schema + table_name

    # file import statement
    filepath = path + file_name
    import_args = "COLUMN SEPARATOR = 'TAB' COLUMN DELIMITER = '\\t' NULL = '\\N' "
    error_message = 'ERRORS INTO ' + exasol_import_schema + 'ERROR_TABLE (' + "'" +  file_name + ' - ' + str(now) + "'" + ') REJECT LIMIT UNLIMITED;'
    import_statement = 'IMPORT INTO ' + exasol_import_db + '\nFROM LOCAL CSV FILE ' + "'" + filepath + "'" + '\n' + import_args + '\n' + error_message + '\n\n'

    with open(sql_script_file, "a") as myfile:
        myfile.write(import_statement)

if __name__ == '__main__':
    # clear out sql import file
    truncate_sql_file()

    filenames = [f for f in os.listdir(nightly_dump_directory) if os.path.isfile(os.path.join(nightly_dump_directory, f))]
    print(filenames)
    print('')

    # For each file in directory...
    for file_to_read in filenames:
        print("writing import script for: " + file_to_read)
        truncate_stg_tables(file_to_read)
        import_file_to_canvas(file_to_read)
