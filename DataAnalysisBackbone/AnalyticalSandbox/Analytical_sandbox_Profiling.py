import numpy as np
import pandas as pd
from ydata_profiling import ProfileReport
import duckdb
import os

def Sandbox_profiling(sand_dir):
    con = duckdb.connect(database= os.path.join(sand_dir, 'covid_score_indicators.duckdb'), read_only=False)
    tables = con.execute("SHOW TABLES").fetchall()
    tables = list([t for (t,) in tables])

    for ds in tables:
        df = con.execute(f'SELECT * FROM {ds}').df()
        profile = ProfileReport(df, title=ds, html={'style' : {'full_width':True}})
        report_file_path = os.path.join(sand_dir, f'{ds}_profile_report.html')
        profile.to_file(report_file_path)
    
    con.close()

'''
rel_path = os.path.dirname(os.path.abspath(__name__))
sand_dir = rel_path+"/DataAnalysisBackbone/AnalyticalSandbox/"
Sandbox_profiling(sand_dir)
'''