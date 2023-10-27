# ADSDB_Project
## Table of contents
* [General info](#general-info)
* [Prerequisites](#prerequisites)
* [Usage](#usage)
* [Authors](#authors)

## General info
This serves as the Data Management Backbone for our ADSDB project. Each zone is represented by a distinct folder, 
with each folder housing the relevant Python script. When Operations.py is executed, it seamlessly runs all the 
distinct zones and stores their output files in the /Landing/Persistent directory. 

## Prerequisites
These are the requisites for running this code:
* Duckdb version: 0.9.1
* Pandas version: 2.0.3
* Numpy version: 1.26.0
* Sklearn version: 0.0.post10

## Usage
First we need to install the main packages that this repository uses:
```
pip install -r requirements.txt
```

To execute Operations.py:
```
python Operations.py
```

## Authors
The authors of this project are: Julian Fransen & Max Ticó
