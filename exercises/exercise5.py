import pandas as pd
from urllib.request import urlretrieve
from zipfile import ZipFile
import os
import sqlite3

url='https://gtfs.rhoenenergie-bus.de/GTFS.zip'
filename='zipfile.zip'
current_directory = os.getcwd()
urlretrieve(url,filename)

with ZipFile('zipfile.zip','r') as object:
    object.extractall(current_directory)

txtfile=pd.read_csv('stops.txt')

dataframe=txtfile[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']]
dataframe=dataframe.loc[dataframe['zone_id']==2001]
dataframe=dataframe[(dataframe['stop_lat'] >= -90) & (dataframe['stop_lat'] <= 90)]
dataframe=dataframe[(dataframe['stop_lon'] >= -90) & (dataframe['stop_lon'] <= 90)]


def load_data(data):
    connection=sqlite3.connect('gtfs.sqlite')
    data.to_sql("stops",connection,if_exists="replace",index=False)
    connection.close()

load_data(dataframe)