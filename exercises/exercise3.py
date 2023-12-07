import pandas as pd
import sqlite3
import re

def download_source(url):
    cars=pd.read_csv(url,encoding='latin1',delimiter=";",skiprows=range(0,7),skipfooter=4,header=None)
    return cars

def data_cleaning(data):
    column_names={0:'date',1:'CIN',2:'name',12:'petrol',22:'diesel',32:'gas',42:'electro',52:'hybrid',62:'plugInHybrid',72:'others'}
    data=data.rename(columns=column_names)
    data=data[['date','CIN','name','petrol','diesel','gas','electro','hybrid','plugInHybrid','others']]
    str_types = {
    'date': str,
    'CIN': str,
    'name': str
                }
    data=data.astype(str_types)
    cinValidation=data['CIN'].str.match(r'^0?\d{4}$')
    data=data[cinValidation]
    int_columns=['petrol','diesel','gas','electro','hybrid','plugInHybrid','others']
    data[int_columns]=data[int_columns].apply(pd.to_numeric, errors='coerce')
    data=data.dropna()
    int_types = {
    'petrol': int,
    'diesel': int,
    'gas': int,
    'electro': int,
    'hybrid': int,
    'plugInHybrid': int,
    'others': int
                }
    data[int_columns]=data[int_columns].astype(int_types)
    return data


def load_data(data):
    connection=sqlite3.connect('../data/cars.sqlite')
    data.to_sql("cars",connection,if_exists="replace")
    connection.close()


print("Extracting data..")
extractedData=download_source("https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv")

print("Cleaning data..")
cleanedData=data_cleaning(extractedData)

print("Loading data in database..")
load_data(cleanedData)
