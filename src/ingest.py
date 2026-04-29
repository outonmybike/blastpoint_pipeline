from pathlib import Path
from datetime import datetime, timedelta
import shutil
import gzip
import os
import config

FILE_VERSION = "V1"

def ingest_data(data_obj:dict):
    client_id = data_obj["client_id"]
    file_name = data_obj["file_name"]
    source_url = data_obj["source_url"]
    file_type = data_obj["file_type"]
    
    if "compression" in data_obj:
        compression = data_obj["compression"] 
        comp_syntax = "." + compression
    else:
        comp_syntax = ""
    source_path_name = f"{source_url}/{file_name}.{file_type}{comp_syntax}"

    target_path = f"outputs/raw/{client_id}/{file_name}"
    path = Path(target_path)
    path.mkdir(parents=True, exist_ok=True)

    new_name = f"{file_name}__{FILE_VERSION}.{file_type}"
    target_path_name = f"{target_path}/{new_name}"

    if not Path(target_path_name).exists() and not Path(f"{target_path_name}{comp_syntax}").exists() or config.FORCE_REFRESH:
        to_write_name = target_path_name+comp_syntax
        shutil.copy(source_path_name, to_write_name)
    else:
        print('no new files detected')

    # checking for compressed files
    files = [f for f in Path(target_path).iterdir() if f.is_file()]
    for x in files:
        suffix = str(x).split(".")[-1]
        if  suffix == "gz":
            write_name = str(x).replace(f".{suffix}","")
            with gzip.open(x,'rb') as zipped:
                with open(write_name,'wb') as unzipped:
                    shutil.copyfileobj(zipped,unzipped)
            os.remove(x)

def ingest_to_raw():
    for file_obj in config.FILE_LIST:
        ingest_data(file_obj)

if __name__ == "__main__":
    ingest_to_raw()