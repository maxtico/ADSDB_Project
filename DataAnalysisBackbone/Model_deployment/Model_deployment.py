import duckdb
import glob
import time
import os
import pandas as pd
import numpy as np
import joblib

def Deployment(filepath):
    model_files = os.listdir(filepath)
    # Print the available models to the user
    print("Available Models:")
    for i, filename in enumerate(model_files, start=1):
        print(f"{i}. {filename}")


    model_number = input("Enter the number (1 to 6) of the model you want to use: ")

    try:
        model_number = int(model_number)
    except ValueError:
        print("Invalid input. Please enter a number between 1 and 6.")
        exit()

    if 1 <= model_number <= len(model_files):
        # Construct the full path based on the selected model number
        selected_model_path = os.path.join(filepath, model_files[model_number - 1])

        # Load the selected model
        selected_model = joblib.load(selected_model_path)

        print(f"Selected model '{model_files[model_number - 1]}' loaded successfully.")

    else:
        print("Invalid input. Please enter a number between 1 and 6.")