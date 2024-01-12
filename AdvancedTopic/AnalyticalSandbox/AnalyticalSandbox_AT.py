import duckdb
import glob
import time
import os
import pandas as pd
import numpy as np

def Sandbox_AT(exp_dir,sand_dir):
    con = duckdb.connect(database= os.path.join(exp_dir, 'exploitation_AT.duckdb'), read_only=False)
    development  = con.execute("SELECT * FROM Development").df()

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

    development  = con.execute("SELECT * FROM Development").df()
    development = development.loc[development["Date"] == "2022-10-27"]
    development = development.drop('Date', axis=1)

    con.close()

    con = duckdb.connect(database= os.path.join(sand_dir, 'covid_score_indicators_AT.duckdb'), read_only=False)
    tables= ['Population_Health', 'Country_info','Pandemic','Development' ] #here i deleted All_data
    dataframes = ['population_health', 'country_info','pandemic','development' ]
    for i in range(len(dataframes)):
        con.execute(f'DROP TABLE IF EXISTS {tables[i]}')
        con.execute(f'CREATE TABLE IF NOT EXISTS {tables[i]} AS SELECT * FROM {dataframes[i]};')
    
    con.close()

'''
rel_path = os.path.dirname(os.path.abspath(__name__))
sand_dir=rel_path+'/AdvancedTopic/AnalyticalSandbox'
exp_dir=rel_path+'/AdvancedTopic/Exploitation'
Sandbox_AT(exp_dir,sand_dir)
'''