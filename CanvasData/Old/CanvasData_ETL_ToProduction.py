# Canvas schema is destroyed and rebuilt based on files imported from Canvas Data into Staging db

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

canvas_tables_to_import = """
    select
        distinct TABLE_NAME
    from sys.EXA_ALL_TABLES
    where TABLE_SCHEMA = 'CANVAS_DATA_STG'
        and TABLE_NAME <> 'ERROR_TABLE'
        and TABLE_ROW_COUNT > 0
    order by TABLE_NAME

"""
sql_script_file = "CanvasData_ETL_ToProduction_Statements.sql"

def truncate_sql_file():
    open(sql_script_file, 'w').close()

def write_etl_script():
    tables_read = exaconnect.readData(canvas_tables_to_import)
    table_names_df = pd.DataFrame(tables_read)

    for index, row in table_names_df.iterrows():
        table = str(row["TABLE_NAME"])
        truncate_script = "TRUNCATE TABLE CANVAS_DATA." + table + ";\n"
        import_script = "INSERT INTO CANVAS_DATA." + table +" (SELECT * FROM CANVAS_DATA_STG." + table +");\n\n"

        with open(sql_script_file, "a") as myfile:
            myfile.write(str(truncate_script))
            myfile.write(str(import_script))


if __name__ == '__main__':

    truncate_sql_file()
    write_etl_script()
