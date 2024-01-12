import numpy as np
import pandas as pd
from ydata_profiling import ProfileReport
import duckdb
import os

def Feature_generation_profiling(feat_dir):

    con = duckdb.connect(database= os.path.join(feat_dir, 'feature_generation1.duckdb'), read_only=False)
    df1 = con.execute(f'SELECT * FROM Generated_features1').df()
    con.close()

    con = duckdb.connect(database= os.path.join(feat_dir, 'feature_generation2.duckdb'), read_only=False)
    df2 = con.execute(f'SELECT * FROM Generated_features2').df()
    con.close()

    dfs = [df1, df2]
    titles = ["Feature generation set 1", "Feature generation set 2"]

    i=0
    for df in dfs:
        profile = ProfileReport(df, title=titles[i], html={'style' : {'full_width':True}})
        report_file_path = os.path.join(feat_dir, f'{i}_profile_report.html')
        profile.to_file(report_file_path)
        i+=1

'''
rel_path = os.path.dirname(os.path.abspath(__name__))
feat_dir = rel_path+"/DataAnalysisBackbone/FeatureGeneration/"
Feature_generation_profiling(feat_dir)
'''