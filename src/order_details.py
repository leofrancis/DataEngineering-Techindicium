import pandas as pd
from pathlib import Path
from os.path import exists as exist_path, splitext

def load_order_details(file_path: Path):
    df = pd.DataFrame()
    df_list = []
    if not exist_path(file_path):
        print(f"ORDER_DETAILS: Path of the file does not exists {file_path}.")
        df_list.append(["order_details", df])
        return df_list

    file_name, file_extension = splitext(file_path)

    print(f"Exctracting data from {file_extension} file.")

    try:
        if file_extension.upper() == ".CSV":
            df = pd.read_csv(file_path)
        elif file_extension.upper() == ".PARQUET":
            df = pd.read_parquet(file_path)
        else:
            print(f" ORDER_DETAILS: extension of the file {file_extension} is no available for extracting.")
    finally:
        print(f" ORDER_DETAILS: finished process")
        df_list.append(["order_details", df])
        return  df_list