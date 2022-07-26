from order_details import load_order_details
from order import load_order
from exporting import export, export_data

import sys
import pandas as pd
import warnings
import os
import os.path as Path
from datetime import date, datetime

warnings.filterwarnings('ignore')

def exists_dir(path, create_dir=True):
    if not Path.isdir(path):
        if create_dir: 
            os.mkdir(path)
            # print(f"created {path}...")
        return False
    else: 
        return True

def exists_file(file_path):
    return Path.exists(file_path)

def main(INPUT_DATE):
    if not INPUT_DATE is None:
        INPUT_DATE =  datetime.strptime(INPUT_DATE, '%Y-%m-%d').date()

    export_process = True
    ORDER_DETAILS_FILE = "order_details.csv"

    #BASE PATHS
    PATH_BASE       = f"{os.getcwd()}"
    PATH_DATA       = f"{PATH_BASE}/data"
    PATH_DETAILS    = f"{PATH_DATA}/{ORDER_DETAILS_FILE}"
    PATH_POSTGRES   = f"{PATH_DATA}/postgres"
    PATH_CSV        = f"{PATH_DATA}/csv"
    PATH_RESULT     = f"{PATH_DATA}/result"

    if INPUT_DATE is None:
        date_base = date.today()
    else:
        date_base = INPUT_DATE

    print(f"Pipeline is running on {date_base}")

    df_tables  = load_order()
    df_details = load_order_details(PATH_DETAILS)

    date_today    = str(date_base)
    file_datename = date_base.strftime('%s')

    #POSTGRESS
    exists_dir(PATH_POSTGRES)

    for table_name, df_table in df_tables:
        PATH_TABLE = f"{PATH_POSTGRES}/{table_name}"
        exists_dir(PATH_TABLE)
        
        PATH_TABLE_DATE = f"{PATH_TABLE}/{date_today}"
        exists_dir(PATH_TABLE_DATE)

        #name the exporting file
        POSTGRES_FILE_NAME = f"{PATH_TABLE_DATE}/{file_datename}.parquet"
        
        #save on path 
        df_table.to_parquet(POSTGRES_FILE_NAME)
        
        #valid process for POSTGRES
        created_postgres_file = exists_file(POSTGRES_FILE_NAME)
        
        if not created_postgres_file:
            export_process = False
            print(f"{table_name}: Could not export file to {POSTGRES_FILE_NAME} path.")

    #CSV
    exists_dir(PATH_CSV)

    for table_name, df_detail in df_details:
        PATH_CSV_DATE = f"{PATH_CSV}/{date_today}"
        exists_dir(PATH_CSV_DATE)

        CSV_FILE_NAME = f"{PATH_CSV_DATE}/{file_datename}.parquet"
                
        #save on path 
        df_detail.to_parquet(CSV_FILE_NAME)

        #valid process for CSV
        created_csv_file =  exists_file(CSV_FILE_NAME)

        if not created_csv_file:
            export_process = False
            print(f"details: Could not export file to {CSV_FILE_NAME} path.")
    
    if len(df_tables) > 0 and len(df_details) > 0:
        #exists CSV path and FILE of the date
        if not exists_dir(PATH_CSV_DATE, False):
            export_process = False
            print(f"Path of {PATH_CSV_DATE} have not been created, it means that process of exctracting have not been completed successfully.")
            
        if not exists_file(CSV_FILE_NAME):
            export_process = False
            print(f"File on path {CSV_FILE_NAME} does not exists, it means that process of exctracting have not been completed successfully.")

        if export_process:
            export(date_base)
    else:
        print("Exporting process of orders and orders details have not been completed successfully")
    
    if export_process:
        df = export_data()

        FILE_RESULT = f"{PATH_RESULT}-{date_today}.csv"
        df.to_csv(FILE_RESULT, index=False)

        if exists_file(FILE_RESULT):
            print(f"Result exported on {FILE_RESULT}")
        else:
            print(f"It was not possible to generate the file on {FILE_RESULT}")

if __name__ == "__main__":
    try: 
        data_input = sys.argv[1]
    except:
        data_input = None

    main(data_input)