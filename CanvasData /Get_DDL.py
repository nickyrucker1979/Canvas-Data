import subprocess
import time

file_name = 'canvas_ddl.txt'
schema = 'CANVAS_DATA_STG.'

def get_canvas_ddl(filename):
    getDDL = "canvas-data -c CanvasConfig.yml get-ddl"
    canvas_ddl = open(filename, 'w')
    canvas_ddl.flush()  # <-- here's something not to forget!
    c = subprocess.Popen(getDDL, stdout=canvas_ddl)

def convert_to_exasol_datatypes_fields(filename, db_schema):
    # Read in the file
    with open(filename, 'r') as file :
      filedata = file.read()

    # Create Tables ddl
    # filedata = filedata.replace('DROP TABLE', '# DROP TABLE')
    filedata = filedata.replace('CREATE TABLE', 'CREATE OR REPLACE TABLE')
    filedata = filedata.replace('TABLE ', 'TABLE ' + db_schema)

    # Update datatypes to Exasol datatypes
    filedata = filedata.replace(' TEXT', ' VARCHAR(50000)')
    filedata = filedata.replace(' BIGINT', ' DECIMAL(18,0)')
    filedata = filedata.replace(' FLOAT', ' DECIMAL(18,3)')
    filedata = filedata.replace(' INTEGER', ' DECIMAL(18,0)')

    # Update fieldnames
    filedata = filedata.replace('position ', 'field_position ')
    filedata = filedata.replace('generated ', 'field_generated ')
    filedata = filedata.replace('date TIMESTAMP', 'field_date TIMESTAMP')
    filedata = filedata.replace('timestamp TIMESTAMP', 'field_timestamp TIMESTAMP')

    # Write the file out again
    with open(filename, 'w') as file:
      file.write(filedata)

if __name__ == '__main__':

    get_canvas_ddl(file_name)
    time.sleep(5)
    convert_to_exasol_datatypes_fields(file_name, schema)
