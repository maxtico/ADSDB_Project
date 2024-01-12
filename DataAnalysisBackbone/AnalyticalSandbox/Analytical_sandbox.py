import duckdb
import glob
import time
import os
import pandas as pd
import numpy as np

def Sandbox(exp_dir,sand_dir):
    con = duckdb.connect(database= os.path.join(exp_dir, 'exploitation2.duckdb'), read_only=False)

    #load dataframes, keep only relavant information
    pandemic = con.execute("SELECT * FROM Pandemic").df()
    pandemic = pandemic.loc[pandemic["Date"] == "2022-10-27"]
    pandemic = pandemic.drop('Date', axis=1)

    country_info  = con.execute("SELECT * FROM Country_info").df()
    country_info = country_info.drop('Date', axis=1)
    country_info = country_info.drop_duplicates()
    
    population_health  = con.execute("SELECT * FROM Population_Health").df()
    population_health = population_health.loc[population_health["Date"] == "2022-10-27"]
    population_health = population_health.drop('Date', axis=1)

    print(population_health)

    con.close()

    # Saving into Sandbox
    con = duckdb.connect(database= os.path.join(sand_dir, 'covid_score_indicators.duckdb'), read_only=False)
    tables= ['Population_Health', 'Country_info','Pandemic']
    dataframes = ['population_health', 'country_info','pandemic']
    for i in range(3):
        con.execute(f'DROP TABLE IF EXISTS {tables[i]}')
        con.execute(f'CREATE TABLE IF NOT EXISTS {tables[i]} AS SELECT * FROM {dataframes[i]};')

'''
rel_path = os.path.dirname(os.path.abspath(__name__))
sand_dir = rel_path+"/DataAnalysisBackbone/AnalyticalSandbox/"
exp_dir = rel_path+"/DataManagementBackbone/Exploitation/"

Sandbox(exp_dir,sand_dir)
'''