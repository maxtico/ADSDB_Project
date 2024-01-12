import pandas as pd
import numpy
import os
import duckdb
import glob
import time
import os

def Formatted_AT(filepath,form_dir):
    l = os.listdir(filepath)
    if '.DS_Store' in l:
       l.remove('.DS_Store')
    custom_order = ['WBG_GDP growth_1705049736.csv', 'WBG_Birth certificates_1705049740.csv', 
    'WBG_C02 production per capita_1705049761.csv', 'WBG_Corruption estimation_1705049782.csv', 
    'WBG_Access to electricity_1705049804.csv', 'OEBC_GDP2020_1705050301.csv', 
    'IBAN_CountryCodes_1705050301.csv']

    custom_order_positions = {filename: position for position, filename in enumerate(custom_order)}

    sorted_list = sorted(l, key=lambda x: custom_order_positions[x])

    WBG_data = []

    for i in range(5):
        data = pd.read_csv(f'{filepath}/{sorted_list[i]}',sep = ",")
        data = pd.DataFrame(data)
        WBG_data.append(data)

    gdp = pd.read_csv(f'{filepath}/{sorted_list[5]}',sep = ",")
    country_codes = pd.read_csv(f'{filepath}/{sorted_list[6]}',sep = ",")

    chosen_tables=["NYGDPMKTPKDZ","ID.OWN.BRTH.ZS","EN.ATM.CO2E.PC", "CC.EST", "EG.ELC.ACCS.ZS" ]
    chosen_table_rename=["GDP growth", "Birth certificates", "C02 production per capita", "Corruption estimation", "Access to electricity"]
    data_renamed = []
    for i in range(len(chosen_tables)):
        df = WBG_data[i]
        df = df.rename(columns={chosen_tables[i]: chosen_table_rename[i]})
        df = df.drop(['Unnamed: 0'], axis=1)
        #  df = df.rename(columns={'economy'  :'Country'})
        df = pd.merge(df, country_codes,left_on = ['economy'], right_on =['Alpha-3 code'])
        #df = df.rename(columns={'economy'  :'Country'})
        df = df.drop(['Numeric', 'Alpha-2 code', 'Alpha-3 code','economy','Unnamed: 0'], axis=1)

        df['time'] = df['time'].astype(str)  # Ensure 'time' column is of string type
        df['time'] = df['time'].str.replace('YR', '')
        data_renamed.append(df)

    gdp = gdp.drop(['Unnamed: 0','Gross domestic product in 2020.1'], axis=1)
    gdp = gdp.rename(columns={'County'  :'Country', 'Gross domestic product in 2020': "GDP2020"})
    data_renamed.append(gdp)

    a = data_renamed[0]
    b = data_renamed[1]
    c = data_renamed[2]
    d = data_renamed[3]
    e = data_renamed[4]
    f = data_renamed[5]

    # Saving the tables
    con = duckdb.connect(database= os.path.join(form_dir, 'formatted_AT.duckdb'), read_only=False)
    tables= ["GDP_growth", "Birth_certificates", "C02_production_per_capita", "Corruption_estimation", "Access_to_electricity", "GDP_2020"]
    dataframes = ['a', 'b','c','d','e','f' ]
    for i in range(len(data_renamed)):
        con.execute(f'DROP TABLE IF EXISTS {tables[i]}')
        con.execute(f'CREATE TABLE IF NOT EXISTS {tables[i]} AS SELECT * FROM {dataframes[i]};')

    con.execute("SHOW TABLES").fetchall()
    con.close()

'''
rel_path = os.path.dirname(os.path.abspath(__name__))
filepath=rel_path+'/AdvancedTopic/Landing/Persistent/'
form_dir=rel_path+'/AdvancedTopic/Formatted'
Formatted_AT(filepath, form_dir)
'''
