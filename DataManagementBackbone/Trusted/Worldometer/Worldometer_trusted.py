import duckdb
import pandas as pd
import glob
import time
import os
import matplotlib
import numpy as np
from sklearn.ensemble import IsolationForest

# Function to check if a group has more than a specified number of missing values for a given column
def has_more_than_N_missing(group, column, N):
        return group[column].isna().sum() > N

def Worldometer_trusted(filepath):
    conn = duckdb.connect()
    file_list = os.listdir(filepath)
    # Initialize dataframes to store the data
    world_dataframes = ''
    for file in file_list:
        if file.endswith(".csv"):
            if "Worldometer_joined" in file:
                world_dataframes=file

    worldometer_complete = conn.execute(f"SELECT * FROM read_csv_auto(['{filepath}{world_dataframes}'])").df()
    
    data = worldometer_complete
    #Furthermore, we delete columns which can be easily constructed in the Feature engineering step:  'Tot Cases/1M pop', 'Deaths/1M pop', 'Tests/1M pop'
    data = data.drop(["NewCases", 'NewDeaths', 'NewRecovered','Serious,Critical', 'Tot Cases/1M pop', 'Deaths/1M pop', 'Tests/1M pop' ], axis = 1)#, ])
    USA_pop_2022 = 338289857 / 1000000
    #expert imputing (for USA). The only missings are for a part of data for USA
    data['Population'] = data['Population'].fillna(USA_pop_2022)
    # The total number of ActiveCases in Brunei on 2022-05-03 was negative: -98. We replace by None and impute later.
    data.loc[12373][4] = None

    # Interpolation
    df = data
    # Make sure 'Date' column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'])

    # Group by 'Country,Other'
    grouped = df.groupby('Country,Other')
    # Create a new DataFrame with a date range covering the desired date range
    date_range = pd.date_range(start=df['Date'].min(), end=df['Date'].max(), freq='D')
    # Create an empty DataFrame to store the results
    result_df = pd.DataFrame()

    # Loop through each group (country) and fill missing dates with NaN
    for name, group in grouped:
        # Merge the group with the date range DataFrame, filling in NaN for missing dates
        merged_group = pd.merge(date_range.to_frame(name='Date'), group, on='Date', how='left')

        # Fill in 'Country,Other' and 'Population' values
        merged_group['Country,Other'] = name
        merged_group['Population'] = group['Population'].iloc[0]  # Assuming 'Population' is constant for the country

        # Append the merged group to the result DataFrame
        result_df = result_df._append(merged_group)

    # Reset the index of the result DataFrame
    result_df.reset_index(drop=True, inplace=True)
    # Sort the DataFrame by 'Country,Other' and 'Date' if needed
    result_df.sort_values(by=['Country,Other', 'Date'], inplace=True)
    # Fill in NaN values in other columns if necessary
    columns_to_fill = ['TotalCases', 'TotalDeaths', 'TotalRecovered', 'ActiveCases', 'TotalTests']
    result_df[columns_to_fill] = result_df[columns_to_fill].fillna(np.nan)
    # Now, result_df contains the missing rows with NaN values filled in.
    grouped = result_df.groupby('Country,Other')
    #for name, group in grouped: #This shows all countries now have 234 dates
    #print(len(group), name)

    # ---------------------------------------------- Step 2 --------------------------------------------------
    # Then we filter the countries with more than 80 % missings. 234 * 0.8 = 187.2
    N = 187
    grouped = result_df.groupby('Country,Other')
    #names = ['DPRK', 'Saint Helena', 'Tuvalu', 'Vatican City', 'Western Sahara', 'Marshall Islands', 'Niue','Tajikistan']
    names = []
    columns = ['TotalCases', 'TotalDeaths', 'TotalRecovered', 'ActiveCases', 'TotalTests']
    for i in range(5):
        for name, group in grouped:
           if has_more_than_N_missing(group, columns[i], N) and name not in names:
            names.append(name)

    new_data = result_df
    for i in names:
        new_data = new_data.loc[new_data['Country,Other'] != i ]
    #  19 countries are filtered out.

    # ---------------------------------------------- Step 3 --------------------------------------------------
    # Now we imputate. First, backfill and forward fill, then interpolation, then MICE.
    df = new_data
    grouped = df.groupby('Country,Other')
    # Initialize an empty list to store the imputed DataFrames
    imputed_dataframes = []
    # Iterate through each group and impute missing values for all columns except 'Date'
    for name, group in grouped:
        group.loc[:, group.columns != 'Date'] = group.loc[:, group.columns != 'Date'].interpolate().fillna(method='bfill')
        #group.loc[:, group.columns != 'Date'] = group.loc[:, group.columns != 'Date'].interpolate()
        imputed_dataframes.append(group)

    # Concatenate the imputed DataFrames back into one final DataFrame
    imputed_df = pd.concat(imputed_dataframes, ignore_index=True)
    
    model = IsolationForest(contamination=0.004, random_state=42)  # You can set a random seed for reproducibility.

    grouped = imputed_df.groupby('Country,Other')
    dataframes = []
    columns = ['TotalCases', 'TotalDeaths', 'TotalRecovered', 'ActiveCases', 'TotalTests']
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

    output_file = os.path.join(filepath, 'Worldometer_preprocessed.csv')
    imputed_df.to_csv(output_file)

