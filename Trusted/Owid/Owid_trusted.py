import duckdb
import pandas as pd
import glob
import time
import os
import matplotlib
from sklearn.impute._iterative import IterativeImputer

def percentage_missing_values(group):
    return group.isnull().sum() * 100 / len(group)

def Owid_trusted():
    #Creating a connection to the duck db database:
    conn = duckdb.connect()
    owid_complete = conn.execute("""SELECT * FROM read_csv_auto(['/Users/maxtico/Documents/Master Data Science/ADSDB/ADSDB_Project/owid-covid-data_v1.csv'], union_by_name=true)""").df()
    # Reducing the dataset to less columns
    reduced_owid = owid_complete[['continent','date','location','total_cases','total_deaths','median_age',
                             'female_smokers','male_smokers','life_expectancy','total_vaccinations','new_vaccinations','people_vaccinated','human_development_index',
                             'population']]
    # Counting the number of present values
    continent_vaccination_present = reduced_owid.groupby(['continent'])['new_vaccinations'].count()
    # Subselecting date
    adjusted_owid = reduced_owid[reduced_owid['date']>'2022-03-07']
    grouped = adjusted_owid.groupby(['location'])
    # Initialize an empty list to store the imputed DataFrames
    imputed_dataframes = []
    # Iterate through each group and impute missing values for all columns except 'Date'
    for name, group in grouped:
        group.loc[:, group.columns != 'date'] = group.loc[:, group.columns != 'date'].interpolate()
        imputed_dataframes.append(group)
    # Concatenate the imputed DataFrames back into one final DataFrame
    imputed_df = pd.concat(imputed_dataframes, ignore_index=True)

    grouped_df = imputed_df.groupby('location')
    result = grouped.apply(percentage_missing_values)

    new = result.index[result['new_vaccinations']>70]
    peop = result.index[result['people_vaccinated']>70]
    total = result.index[result['total_vaccinations']>70]

    common_values = set(total).intersection(set(peop), set(new))
    common_values_list = list(common_values)

    out_countries = imputed_df[~(imputed_df['location'].isin(common_values_list))]
    # Lets remove random countries
    list_rand_count = ['Africa','Asia','Europe','High income']
    new_dt = out_countries[~(out_countries['location'].isin(list_rand_count))]

    # We still have some missing values to impute

    # Imputing the remaining missing values
    imp_mean = IterativeImputer(max_iter=10, random_state=0)
    num_imp = imp_mean.fit_transform(new_dt[['total_deaths','median_age', 'female_smokers', 'male_smokers', 'life_expectancy',
       'total_vaccinations', 'new_vaccinations', 'people_vaccinated',
       'human_development_index']])

    # Saving them in the df
    new_dt[['total_deaths','median_age', 'female_smokers', 'male_smokers', 'life_expectancy',
       'total_vaccinations', 'new_vaccinations', 'people_vaccinated',
       'human_development_index']] = num_imp

    new_dt.to_csv('/Users/maxtico/Documents/Master Data Science/ADSDB/ADSDB_Project/Trusted/Owid/owid_preprocessed.csv')  # or True?

Owid_trusted()