# This script downloads the files from the Canvas Data portal
# python scripts errors out reading some of the larger files


import os
from canvas_data.api import CanvasDataAPI
import config_CanvasData as CanvasDataConfig
import pandas as pd

API_KEY = CanvasDataConfig.api_key
API_SECRET = CanvasDataConfig.api_secret
cd = CanvasDataAPI(api_key=API_KEY, api_secret=API_SECRET, download_chunk_size=1024*1024)
nightly_dump_directory = './nightly_downloads'
nightly_data_directory = './nightly_data'

def purge_subdirectory(subdirectory):
    for the_file in os.listdir(subdirectory):
        file_path = os.path.join(subdirectory, the_file)
        print(file_path)
        if os.path.isfile(file_path):
            os.unlink(file_path)
        # # uncomment if you want to remove subdirectories too...
        # elif os.path.isdir(file_path): shutil.rmtree(file_path)

if __name__ == '__main__':

    # # Delete all the files in the subdirectory before migrating the data
    purge_subdirectory(nightly_dump_directory)
    purge_subdirectory(nightly_data_directory)

    # # Nightly dump method
    download_nightly_data = cd.get_data_for_dump(dump_id='latest', account_id='self', data_directory=nightly_data_directory,
                          download_directory=nightly_dump_directory, include_requests=True)

    # # Read names of each file in directory
    filenames = [f for f in os.listdir(nightly_dump_directory) if os.path.isfile(os.path.join(nightly_dump_directory, f))]
    print(filenames)
