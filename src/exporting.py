import os
import pathlib
import psycopg2
import pandas as pd
from configparser import ConfigParser
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine

PATH_BASE       = pathlib.Path(f"{os.getcwd()}")

if str(PATH_BASE)[len(str(PATH_BASE))-3:] == 'src':
    PATH_BASE = PATH_BASE.parent

PATH_DATA       = f"{PATH_BASE}/data"
PATH_POSTGRES   = f"{PATH_DATA}/postgres"
PATH_CSV        = f"{PATH_DATA}/csv"

def config(filename=f'{PATH_BASE}/src/database_exp.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def export_data():
    print('Exporting local data to the PostgreSQL database...')
    conn = None
    df = pd.DataFrame()
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print(' Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        		
        # create a cursor
        cur = conn.cursor()
        
	    # execute a statement
        query_execute = " SELECT * FROM orders_details, orders WHERE orders_details.order_id = orders.order_id; "
        df = pd.read_sql_query(f"{query_execute}", conn)
        print(f" {len(df)} results of the query...")

	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print(' Database connection closed.')

    return df

def exists_dir(path, create_dir=True):
    if not os.path.isdir(path):
        if create_dir: 
            os.mkdir(path)
            # print(f"created {path}...")
        return False
    else: 
        return True

def exists_file(file_path):
    return os.path.exists(file_path)

def export (date_base):
    print("Loading data...")
    try: 
        rows_imported = 0
        
        date_today = str(date_base)
        file_datename = date_base.strftime('%s')

        params = config()

        engine = create_engine(f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}")

        #POSTGRES
        tables = os.listdir(PATH_POSTGRES)

        for tab in tables:
            PATH_DATE_POSTGRES = f"{PATH_POSTGRES}/{tab}/{date_today}/{file_datename}.parquet"

            if exists_file(PATH_DATE_POSTGRES):
                df = pd.read_parquet(PATH_DATE_POSTGRES)

                size_df = len(df)

                if size_df == 0:
                    print(f" No rows to be imported for table {tab}...")
                else:
                    print(f" Importing rows {rows_imported} to {rows_imported+size_df} for table {tab}...")

                df.to_sql(f"{tab}", engine, if_exists="replace", index=False)
                rows_imported += size_df

                if size_df == 0:
                    print(f"  Table imported successfull on {tab}.")
                else:
                    print(f"  Data imported successfull for table {tab}.")
        
        #CSV
        tab = "orders_details"
        PATH_DATE_CSV = f"{PATH_CSV}/{date_today}/{file_datename}.parquet"

        df = pd.read_parquet(PATH_DATE_CSV)

        size_df = len(df)
        print(f" Importing rows {rows_imported} to {rows_imported+size_df} for table {tab}...")

        df.to_sql(f"{tab}", engine, if_exists="replace", index=False)
        rows_imported += size_df

        print(f"  Data imported successfull for table {tab}.")

    except Exception as e:
        print("Data load error: " + str(e))
