import duckdb
import glob
import time
import os
import pandas as pd

def Formatted(filepath):
    conn = duckdb.connect()
    # Display our tables in duckdb
    file_list = os.listdir(filepath)
    # Initialize dataframes to store the data
    owid_dataframes = []
    worldometer_dataframes = []
    for file in file_list:
        if file.endswith(".csv"):
            # Check if the file name contains "owid" or "worldometer"
            if "owid" in file:
                owid_dataframes.append(file)
            elif "worldometer" in file:
                worldometer_dataframes.append(file)
    
    # Joining the datasets: Worldometer
    w1 = worldometer_dataframes[0]
    w2 = worldometer_dataframes[1]
    worldometer_complete = conn.execute(f"SELECT * FROM read_csv_auto(['{filepath}{w1}','{filepath}{w2}'], union_by_name=true)").df()
    output_file = os.path.join(filepath, 'Worldometer_joined.csv')
    worldometer_complete.to_csv(output_file)  # or True?


    # Joining the datasets: Owid
    o1 = owid_dataframes[0]
    o2 = owid_dataframes[1]
    owid_complete = conn.execute(f"SELECT * FROM read_csv_auto(['{filepath}{o1}','{filepath}{o2}'], union_by_name=true)").df()
    output_file = os.path.join(filepath, 'Owid_joined.csv')
    owid_complete.to_csv(output_file)  # or True?

