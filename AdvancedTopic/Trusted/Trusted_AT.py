import pandas as pd
import numpy
import os
import duckdb
import glob
import time
import os

def Trusted_AT(form_dir,trust_dir):
    con = duckdb.connect(database= os.path.join(form_dir, 'formatted_AT.duckdb'), read_only=False)
    tables= ["GDP_growth", "Birth_certificates", "C02_production_per_capita", "Corruption_estimation", "Access_to_electricity"]
    imp_result = []
    for i in tables:
        df =  con.execute(f'SELECT * FROM {i}').df()
        grouped = df.groupby("Country")
        imp_df = []
        #print(df.tail())
        for name, group in grouped:
            #mean = group.iloc[:,2].mean()
            group.iloc[:,1] = group.iloc[:,1].fillna(method="ffill")
            group.iloc[:,1] = group.iloc[:,1].fillna(0)
            imp_df.append(group.iloc[:, :].reset_index(drop=True))
        result_df = pd.concat(imp_df, ignore_index=True)
        imp_result.append(result_df)

    df =  con.execute(f'SELECT * FROM GDP_2020').df()
    #no missings after imputation
    imp_result.append(df)
    con.close()

    a = imp_result[0]
    b = imp_result[1]
    c = imp_result[2]
    d = imp_result[3]
    e = imp_result[4]
    f = imp_result[5]

    #load imp data in new duckdb file
    con = duckdb.connect(database= os.path.join(trust_dir, 'trusted_AT.duckdb'), read_only=False)

    tables= ["GDP_growth", "Birth_certificates", "C02_production_per_capita", "Corruption_estimation", "Access_to_electricity", "GDP_2020"]
    dataframes = ['a', 'b','c','d','e','f' ]
    for i in range(len(imp_result)):
        con.execute(f'DROP TABLE IF EXISTS {tables[i]}')
        con.execute(f'CREATE TABLE IF NOT EXISTS {tables[i]} AS SELECT * FROM {dataframes[i]};')
    con.close()

'''
rel_path = os.path.dirname(os.path.abspath(__name__))
trust_dir=rel_path+'/AdvancedTopic/Trusted'
form_dir=rel_path+'/AdvancedTopic/Formatted'
Trusted_AT(form_dir,trust_dir)
'''