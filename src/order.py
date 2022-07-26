import os
import pathlib
import psycopg2
import pandas as pd
from configparser import ConfigParser

PATH_BASE = pathlib.Path(f"{os.getcwd()}")

if str(PATH_BASE)[len(str(PATH_BASE))-3:] == 'src':
    PATH_BASE = PATH_BASE.parent

def config(filename=f'{PATH_BASE}/src/database.ini', section='postgresql'):
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

def connect():
    print('Extracting data from the PostgreSQL database...')
    conn = None
    df_list = []
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
	    # execute a statement
        print('PostgreSQL: selecting all tables ...')
        query_execute = " SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' "
        cur.execute(query_execute)

        src_tables = cur.fetchall()
        cont = 0
        size = len(src_tables)
        print('PostgreSQL: extracting data from tables...')
        for tbl in src_tables:
            cont+=1
            print(f" {cont}/{size} {tbl[0]}")
            df = pd.read_sql_query(f"SELECT * FROM {tbl[0]};", conn)
            # df["table"] = tbl[0]
            df_list.append([tbl[0], df])
       
	    # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

    return df_list

def load_order():
    return connect()
