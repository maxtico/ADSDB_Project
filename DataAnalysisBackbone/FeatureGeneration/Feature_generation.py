import duckdb
import glob
import time
import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

def Feature_generation(filepath, filedest):
    con = duckdb.connect(database= os.path.join(filepath, 'covid_score_indicators.duckdb'), read_only=False)
    # Reading tables
    country = con.execute("SELECT * FROM Country_info").df()
    # Defining two healths
    health1 = con.execute("SELECT * FROM Population_health").df()
    health2 = con.execute("SELECT * FROM Population_health").df()

    ### STEP 0: Scaling the data

    # Saving stats for each column HEALTH 1
    stats = {}
    features = ['life_expectancy','male_smokers','female_smokers','median_age','human_development_index']
    for column in features:
        mean_value = health1[column].mean()
        std_dev_value = health1[column].std()
        stats[column] = [mean_value, std_dev_value]

    ## Scaling the data for the training
    for cols in stats:
        health1[cols+'_scaled'] = ( health1[cols] - stats[cols][0] ) / stats[cols][1]

    #########################################

    # Saving stats for each column HEALTH 2
    stats = {}
    features = ['life_expectancy','male_smokers','female_smokers','median_age','human_development_index']
    for column in features:
        mean_value = health2[column].mean()
        std_dev_value = health2[column].std()
        stats[column] = [mean_value, std_dev_value]

    ## Scaling the data for the health2
    for cols in stats:
        health2[cols+'_scaled'] = ( health2[cols] - stats[cols][0] ) / stats[cols][1]

    ### STEP 1: Feature Generation

    ####### HEALTH 1

    ## Creating the feature with linear combinations of the actual features
    health1['health_score1'] = (0.4 * health1['human_development_index_scaled'])
    + (0.3 * health1['life_expectancy_scaled']) + (0.12 * health1['male_smokers_scaled'])
    + (0.12 *health1['female_smokers_scaled']) + (0.06 * health1['median_age_scaled'])

    ####### HEALTH 2

    ## Creating the feature with linear combinations of the actual features
    health2['health_score2'] = (0.2 * health2['human_development_index_scaled'])
    + (0.4 * health2['life_expectancy_scaled']) + (0.17 * health2['male_smokers_scaled'])
    + (0.17 *health2['female_smokers_scaled']) + (0.06 * health2['median_age_scaled'])

    ### STEP 2: Creating Label for Pandemic table

    pandemic = con.execute("SELECT * FROM Pandemic").df()

    # Creating the label HEALTH 1
    health1['covid_score'] = (0.4 * (pandemic['TotalCases']) / pandemic['Population']) + (0.6 * (pandemic['TotalDeaths'] / pandemic['Population']))
    country = country[country['GDP'] != '..']
    country['GDP'] = country['GDP'].astype(float)
    mean_value = country['GDP'].mean()
    std_dev_value = country['GDP'].std()
    health1['GDP'] =   ( country['GDP'] - mean_value ) / std_dev_value

    # Creating the label HEALTH 2
    health2['covid_score'] = (0.4 * (pandemic['TotalCases']) / pandemic['Population']) + (0.6 * (pandemic['TotalDeaths'] / pandemic['Population']))
    health2['GDP'] = ( country['GDP'] - mean_value ) / std_dev_value

    # Dropping useless columns
    health1= health1[['health_score1','Population','GDP','covid_score']]
    health2= health2[['health_score2','Population','GDP','covid_score']]


    con.close()

    ### STEP 3: Saving into duckdb file

    # Feature generation 1
    con = duckdb.connect(database= os.path.join(filedest, 'feature_generation1.duckdb'), read_only=False)
    tables= ['Generated_features1']
    dataframes = ['health1']
    for i in range(1):
        con.execute(f'DROP TABLE IF EXISTS {tables[i]}')
        con.execute(f'CREATE TABLE IF NOT EXISTS {tables[i]} AS SELECT * FROM {dataframes[i]};')
    con.close()

    # Feature generation 2
    con = duckdb.connect(database= os.path.join(filedest, 'feature_generation2.duckdb'), read_only=False)
    tables= ['Generated_features2']
    dataframes = ['health2']
    for i in range(1):
        con.execute(f'DROP TABLE IF EXISTS {tables[i]}')
        con.execute(f'CREATE TABLE IF NOT EXISTS {tables[i]} AS SELECT * FROM {dataframes[i]};')
    con.close()

'''
rel_path = os.path.dirname(os.path.abspath(__name__))
sand_dir = rel_path+"/DataAnalysisBackbone/AnalyticalSandbox/"
feat_dir = rel_path+"/DataAnalysisBackbone/FeatureGeneration/"

Feature_generation(sand_dir,feat_dir)
'''