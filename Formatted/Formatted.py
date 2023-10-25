import duckdb
import glob
import time
import os
import pandas as pd

def Formatted(filepath):
    conn = duckdb.connect()
    # Display our tables in duckdb
    worldometer_v1 = conn.execute("""SELECT * FROM read_csv_auto(filepath + 'worldometer_data_v1.csv')""").df()
    worldometer_v2 = conn.execute("""SELECT * FROM read_csv_auto(filepath + 'worldometer_data_v2.csv')""").df()
    # Joining them with DuckDB
    wordldometer_complete = conn.execute("""SELECT * FROM read_csv_auto([filepath + 'worldometer_data_v1.csv',filepath + 'worldometer_data_v2.csv'], union_by_name=true)""").df()

    owid_v1 = conn.execute("""SELECT * FROM read_csv_auto(filepath + 'owid-covid-data_v1.csv')""").df()
    owid_v2 = conn.execute("""SELECT * FROM read_csv_auto(filepath + 'owid-covid-data_v2.csv')""").df()
    owid_complete = conn.execute("""SELECT * FROM read_csv_auto([filepath + 'owid-covid-data_v1.csv',filepath + 'owid-covid-data_v2.csv], union_by_name=true)""").df()

