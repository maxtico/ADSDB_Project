import duckdb
import glob
import time
import os
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import cross_val_score,ShuffleSplit, cross_validate, GridSearchCV
from sklearn.neural_network import MLPRegressor
from sklearn import svm
import joblib


def Model(filepath,model_path):
    # Feature gen 1
    con = duckdb.connect(database= os.path.join(filepath, 'feature_generation1.duckdb'), read_only=False)
    data1 = con.execute("SELECT * FROM Generated_features1").df()
    print(data1)
    con.close()

    # Feature gen 2
    con = duckdb.connect(database= os.path.join(filepath, 'feature_generation2.duckdb'), read_only=False)
    data2 = con.execute("SELECT * FROM Generated_features2").df()
    print(data2)
    con.close()

    # RANDOM FOREST MODELLING

    # Feat gen 1
    X1 = data1.drop(['covid_score'], axis=1)
    y1 = np.array(data1.drop(['health_score1',	'Population',	'GDP'], axis=1)).reshape(-1,)

    #Grid search for hyperparameter tuning and cross validation using 5 folds
    param_grid = {
        'n_estimators': [10, 50, 100, 200],
        'max_depth': [None, 10, 20, 30],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4]
    }
    rf_model = RandomForestRegressor()
    grid_search = GridSearchCV(rf_model, param_grid, cv=5, scoring='neg_mean_squared_error', n_jobs=-1)
    # Fitting into the training data
    grid_search.fit(X1, y1)

    # Getting the best parameters
    best_params = grid_search.best_params_
    best_estimator = grid_search.best_estimator_

    # Evaluating the performance of the best model
    y_pred = best_estimator.predict(X1)
    mse = mean_squared_error(y1, y_pred)
    print(f'Best Model MSE with Random Forest 1: {mse}')
    model_config = best_estimator.get_params()
    joblib.dump(model_config, model_path+'Random_forest_fg1.joblib')

    # Feat gen 2
    X2 = data2.drop(['covid_score'], axis=1)
    y2 = np.array(data2.drop(['health_score2',	'Population',	'GDP'], axis=1)).reshape(-1,)
    #Grid search for hyperparameter tuning and cross validation using 5 folds
    param_grid = {
        'n_estimators': [10, 50, 100, 200],
        'max_depth': [None, 10, 20, 30],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4]
    }
    rf_model = RandomForestRegressor()
    grid_search = GridSearchCV(rf_model, param_grid, cv=5, scoring='neg_mean_squared_error', n_jobs=-1)
    # Fitting into the training data
    grid_search.fit(X2, y2)
    # Getting the best parameters
    best_params = grid_search.best_params_
    best_estimator = grid_search.best_estimator_

    # Evaluating the performance of the best model
    y_pred_2 = best_estimator.predict(X2)
    mse = mean_squared_error(y2, y_pred_2)
    print(f'Best Model MSE with Random Forest 2: {mse}')
    model_config = best_estimator.get_params()
    joblib.dump(model_config, model_path+'Random_forest_fg2.joblib')


    # NEURAL NETWORK MODELLING

    # Feat gen 1

    param_grid = {
    'hidden_layer_sizes': [(50,), (100,), (50, 50), (100, 50)],
    'activation': ['relu', 'tanh'],
    'alpha': [0.0001, 0.001, 0.01],
    'learning_rate': ['constant', 'invscaling', 'adaptive']
    }
    # Creating a MLPRegressor
    mlp_model = MLPRegressor(max_iter=1000)

    # Grid search
    grid_search = GridSearchCV(mlp_model, param_grid, cv=5, scoring='neg_mean_squared_error', n_jobs=-1)

    # Grid search to the training data
    grid_search.fit(X1, y1)

    # Best parameters & estimators
    best_params = grid_search.best_params_
    best_estimator = grid_search.best_estimator_

    # Getting MSE
    y_pred = best_estimator.predict(X1)
    mse = mean_squared_error(y1, y_pred)
    print(f'Best Model Mean Squared Error Neural Network 1: {mse}')
    model_config = best_estimator.get_params()
    joblib.dump(model_config, model_path+'Neural_network_fg1.joblib')

    # Feat gen 2

    param_grid = {
        'hidden_layer_sizes': [(50,), (100,), (50, 50), (100, 50)],
        'activation': ['relu', 'tanh'],
        'alpha': [0.0001, 0.001, 0.01],
        'learning_rate': ['constant', 'invscaling', 'adaptive']
    }
    # Creating a MLPRegressor
    mlp_model = MLPRegressor(max_iter=1000)
    # Grid search
    grid_search = GridSearchCV(mlp_model, param_grid, cv=5, scoring='neg_mean_squared_error', n_jobs=-1)
    # Grid search to the training data
    grid_search.fit(X2, y2)
    # Best parameters & estimators
    best_params = grid_search.best_params_
    best_estimator = grid_search.best_estimator_
    # Getting MSE
    y_pred = best_estimator.predict(X2)
    mse = mean_squared_error(y2, y_pred)
    print(f'Best Model Mean Squared Error Neural Network 2: {mse}')
    model_config = best_estimator.get_params()
    joblib.dump(model_config, model_path+'Neural_network_fg2.joblib')

    # SVM MODELLING

    # Feat gen 1

    #Grid search for hyperparameter tuning and cross validation using 5 folds
    tuned_parameters = [
        {"kernel": ["rbf"], "epsilon": [1e-3, 1e-2, 1e-1,1], "C": [1, 10, 100]},
        {"kernel": ["poly"], "epsilon": [1e-3, 1e-2, 1e-1,1], "C": [1, 10, 100]},
        {"kernel": ["sigmoid"], "epsilon": [1e-3, 1e-2, 1e-1,1], "C": [1, 10, 100]},
        ]
    #SVR on data1
    svr1_cv = GridSearchCV(estimator=svm.SVR(), param_grid=tuned_parameters, cv= 5, scoring = 'neg_mean_squared_error')
    svr1_cv.fit(X1, y1)

    best_estimator = svr1_cv.best_estimator_

    #best model
    svr1 = svm.SVR(C = 1, epsilon =  0.1, kernel = 'poly')
    svr1.fit(X1, y1)

    # Getting MSE
    y_pred1 = svr1.predict(X1)
    mse1 = mean_squared_error(y1, y_pred1)
    print(f'Best Model Mean Squared Error SVM 1: {mse1}')
    model_config = best_estimator.get_params()
    joblib.dump(model_config, model_path+'SVM_fg1.joblib')

    # Feat gen 2
    #SVR on data2
    svr2_cv = GridSearchCV(estimator=svm.SVR(), param_grid=tuned_parameters, cv= 5, scoring = 'neg_mean_squared_error')
    svr2_cv.fit(X2, y2)
    best_estimator = svr2_cv.best_estimator_

    #best model
    svr2 = svm.SVR(C = 1, epsilon =  0.1, kernel = 'poly')
    svr2.fit(X2, y2)

    # Getting MSE
    y_pred2 = svr2.predict(X2)
    mse = mean_squared_error(y2, y_pred2)
    print(f'Best Model Mean Squared Error SVM2: {mse}')
    model_config = best_estimator.get_params()
    joblib.dump(model_config, model_path+'SVM_fg2.joblib')

'''
rel_path = os.path.dirname(os.path.abspath(__name__))
feat_dir = rel_path+"/DataAnalysisBackbone/FeatureGeneration/"
Model(feat_dir)
'''