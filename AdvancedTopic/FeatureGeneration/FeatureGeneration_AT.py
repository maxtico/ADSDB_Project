import duckdb
import glob
import time
import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

def Feature_generation_AT(sand_dir,feat_dir):
    con = duckdb.connect(database= os.path.join(sand_dir, 'covid_score_indicators_AT.duckdb'), read_only=False)
    country = con.execute("SELECT * FROM Country_info").df()
    # Only health score 2
    health = con.execute("SELECT * FROM Population_health").df()
    development1 = con.execute("SELECT * FROM Development").df()
    development2 = con.execute("SELECT * FROM Development").df()

    # Saving stats for each column HEALTH 2
    stats = {}
    features = ['life_expectancy','male_smokers','female_smokers','median_age','human_development_index']
    for column in features:
        mean_value = health[column].mean()
        std_dev_value = health[column].std()
        stats[column] = [mean_value, std_dev_value]
    ## Scaling the data for the training
    for cols in stats:
        health[cols+'_scaled'] = ( health[cols] - stats[cols][0] ) / stats[cols][1]
    
    # Saving stats for each column Development 1
    stats = {}
    features = ['GDP_growth', 'Birth_certificates',
        'C02_production_per_capita', 'Corruption_estimation',
        'Access_to_electricity', 'Population']
    for column in features:
        mean_value = development1[column].mean()
        std_dev_value = development1[column].std()
        stats[column] = [mean_value, std_dev_value]
    ## Scaling the data for the training
    for cols in stats:
        development1[cols+'_scaled'] = ( development1[cols] - stats[cols][0] ) / stats[cols][1]

    ###############################

    # Saving stats for each column Development 2
    stats = {}
    features = ['GDP_growth', 'Birth_certificates',
        'C02_production_per_capita', 'Corruption_estimation',
        'Access_to_electricity', 'Population']
    for column in features:
        mean_value = development2[column].mean()
        std_dev_value = development2[column].std()
        stats[column] = [mean_value, std_dev_value]
    ## Scaling the data for the training
    for cols in stats:
        development2[cols+'_scaled'] = ( development2[cols] - stats[cols][0] ) / stats[cols][1]
    
    # We will create a feature which is a linear combination of hdi, life_exp, m_smk, f_smk & median_age

    ####### HEALTH 2

    ## Creating the feature with linear combinations of the actual features
    health['health_score'] = (0.2 * health['human_development_index_scaled'])
    + (0.4 * health['life_expectancy_scaled']) + (0.17 * health['male_smokers_scaled'])
    + (0.17 *health['female_smokers_scaled']) + (0.06 * health['median_age_scaled'])

    # Approach 1: Weights based on perceived importance
    weights_approach1 = {
        'GDP_growth': 0.2,
        'Birth_certificates': 0.15,
        'C02_production_per_capita': -0.1,
        'Corruption_estimation': -0.2,
        'Access_to_electricity': 0.25,
        'Population': -0.1
    }
    development1['SocioEconomic_Score'] = development1[list(weights_approach1.keys())].mul(pd.Series(weights_approach1)).sum(axis=1)


    # Approach 2: Weights based on different assumptions
    weights_approach2 = {
        'GDP_growth': 0.15,
        'Birth_certificates': 0.1,
        'C02_production_per_capita': -0.15,  # Emphasizing lower CO2 production more
        'Corruption_estimation': -0.1,  # Lowering the impact of corruption
        'Access_to_electricity': 0.2,
        'Population': -0.2  # Emphasizing lower population more
    }
    development2['SocioEconomic_Score'] = development2[list(weights_approach2.keys())].mul(pd.Series(weights_approach2)).sum(axis=1)

    pandemic = con.execute("SELECT * FROM Pandemic").df()

    # Creating the label HEALTH 1
    development1['covid_score'] = (0.4 * (pandemic['TotalCases']) / health['Population']) + (0.6 * (pandemic['TotalDeaths'] / health['Population']))
    development1['health_score'] = health['health_score']
    development1['Population'] = health['Population']


    ##############################
    # Creating the label HEALTH 2
    development2['covid_score'] = (0.4 * (pandemic['TotalCases']) / health['Population']) + (0.6 * (pandemic['TotalDeaths'] / health['Population']))
    development2['health_score'] = health['health_score']
    development2['Population'] = health['Population']

    # Dropping useless columns
    development1= development1[['SocioEconomic_Score','Population','health_score','covid_score']]
    development2= development2[['SocioEconomic_Score','Population','health_score','covid_score']]
    con.close()

    #FEATURE GEN 1
    con = duckdb.connect(database= os.path.join(feat_dir, 'feature_generation1_AT.duckdb'), read_only=False)
    tables= ['Generated_features1']
    dataframes = ['development1']
    for i in range(1):
        con.execute(f'DROP TABLE IF EXISTS {tables[i]}')
        con.execute(f'CREATE TABLE IF NOT EXISTS {tables[i]} AS SELECT * FROM {dataframes[i]};')
    print(con.execute("SHOW TABLES").fetchall())
    con.close()

    #FEATURE GEN 2
    con = duckdb.connect(database= os.path.join(feat_dir, 'feature_generation2_AT.duckdb'), read_only=False)
    tables= ['Generated_features2']
    dataframes = ['development2']
    for i in range(1):
        con.execute(f'DROP TABLE IF EXISTS {tables[i]}')
        con.execute(f'CREATE TABLE IF NOT EXISTS {tables[i]} AS SELECT * FROM {dataframes[i]};')
    print(con.execute("SHOW TABLES").fetchall())
    con.close()

'''
rel_path = os.path.dirname(os.path.abspath(__name__))
sand_dir=rel_path+'/AdvancedTopic/AnalyticalSandbox'
feat_dir=rel_path+'/AdvancedTopic/FeatureGeneration'
Feature_generation_AT(sand_dir,feat_dir)
'''