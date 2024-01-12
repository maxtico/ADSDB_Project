import duckdb
import pandas as pd
import glob
import time
import os
import matplotlib
from sklearn.impute._iterative import IterativeImputer
from sklearn.ensemble import IsolationForest

# Define a function to calculate the percentage of missing values
def percentage_missing_values(group):
    return group.isnull().sum() * 100 / len(group)

def Owid_trusted(filepath):
    con = duckdb.connect()
    file_list = os.listdir(filepath)
    # Initialize dataframes to store the data
    owid_dataframes = ''
    for file in file_list:
        if file.endswith(".csv"):
            if "Owid_joined" in file:
                owid_dataframes=file

    owid_complete = con.execute(f"SELECT * FROM read_csv_auto(['{filepath}{owid_dataframes}'])").df()

    reduced_owid = owid_complete[['continent','date','location','total_cases','total_deaths','median_age',
                             'female_smokers','male_smokers','life_expectancy','total_vaccinations','new_vaccinations','people_vaccinated','human_development_index',
                             'population']]
    # Number of present values
    continent_vaccination_present = reduced_owid.groupby(['continent'])['new_vaccinations'].count()
    # Number of missing values
    continent_vaccination_mis = reduced_owid.groupby(['continent'])['new_vaccinations'].apply(lambda x: x.isna().sum())
    # Adjusting data
    adjusted_owid = reduced_owid[reduced_owid['date']>'2022-03-07']

    # Percentage of missing per country
    df = adjusted_owid
    # Group the DataFrame by the 'Country' column
    grouped = df.groupby(['location'])
    result = grouped.apply(percentage_missing_values)

    # Imputation
    # Initialize an empty list to store the imputed DataFrames
    imputed_dataframes = []
    # Iterate through each group and impute missing values for all columns except 'Date'
    for name, group in grouped:
        group.loc[:, group.columns != 'date'] = group.loc[:, group.columns != 'date'].interpolate()
        imputed_dataframes.append(group)
    # Concatenate the imputed DataFrames back into one final DataFrame
    imputed_df = pd.concat(imputed_dataframes, ignore_index=True)

    # Checking missing values
    grouped_df = imputed_df.groupby('location')
    result = grouped_df.apply(percentage_missing_values)

    new = result.index[result['new_vaccinations']>70]
    peop = result.index[result['people_vaccinated']>70]
    total = result.index[result['total_vaccinations']>70]

    common_values = set(total).intersection(set(peop), set(new))
    common_values_list = list(common_values)

    out_countries = imputed_df[~(imputed_df['location'].isin(common_values_list))]
    # Lets remove random countries
    list_rand_count = ['Africa','Asia','Europe','High income','World','Upper middle income','Lower middle income','European Union', 'South America',
                    'Low income','Oceania','North America']
    new_dt = out_countries[~(out_countries['location'].isin(list_rand_count))]

    # Imputing the remaining missing values
    imp_mean = IterativeImputer(max_iter=10, random_state=0)
    num_imp = imp_mean.fit_transform(new_dt[['total_cases','total_deaths','median_age', 'female_smokers', 'male_smokers', 'life_expectancy',
           'total_vaccinations', 'new_vaccinations', 'people_vaccinated',
           'human_development_index']])
    # Saving them in the df
    new_dt[['total_cases','total_deaths','median_age', 'female_smokers', 'male_smokers', 'life_expectancy',
           'total_vaccinations', 'new_vaccinations', 'people_vaccinated',
           'human_development_index']] = num_imp
    
    # Outlier detection
    model = IsolationForest(contamination=0.004, random_state=42)  # You can set a random seed for reproducibility.
    grouped = new_dt.groupby('location')
    dataframes = []
    columns = ['total_cases', 'total_deaths', 'new_vaccinations', 'total_vaccinations', 'people_vaccinated']
    # Iterate through each group (country)
    for column in columns:
      for name, group in grouped:
        model.fit(group[[column]])
        group[f'outliers_{column}'] = model.predict(group[[column]])

        # Convert -1 to 'yes' and 1 to 'no'
        group[f'outliers_{column}'] = group[f'outliers_{column}'].apply(lambda x: 'yes' if x == -1 else 'no')
        dataframes.append(group)
    # Concatenate the imputed DataFrames back into one final DataFrame
    data_outliers = pd.concat(dataframes, ignore_index=True)

    #output_file = os.path.join(filepath, 'Owid_preprocessed.csv')
    #new_dt.to_csv(output_file)  # or True?

'''
rel_path = os.path.dirname(os.path.abspath(__name__))
destination_folder = rel_path+"/DataManagementBackbone/Landing/Persistent/"
Owid_trusted(destination_folder)
'''