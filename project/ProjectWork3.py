import requests
import pandas as pd
import gdown
import sqlite3
import subprocess


#Downloading data from the url, I am using gdown to get data from google drive
#because one of the dataset is dynamically created using JS, to automatically download from the website needs
#selenium and web automation which is a long process
def download_bayernData(url):
    subprocess.run(["gdown","--fuzzy",url,"-O",r'../data/BayernData.csv'])
    bayern_data=pd.read_csv("../data/BayernData.csv")
    return bayern_data

def download_schleswigData(url):
    response=requests.get(url)
    with open("../data/schleswigData.xlsx","wb") as file:
        file.write(response.content)
    schleswigData=pd.read_excel("../data/SchleswigData.xlsx")
    return schleswigData


'''   
    Transforming and Cleaning data includes:
    --selecting columns relevant for data analysis
    --renaming/translating column names
    --extract just the first word from company name and make them uppercase to show it in map visualization
    --Dropping null records
    --Changing datatypes
    --dropping duplicate locations and selecting only the first occurence of the each location
    
    Note: Reason for using is that the columns we need from the datasets are on different indexes in both datasets.
    
    '''
def transform_bayernData(extractedData):
    selectedColumns=extractedData[["Name","Leistung (kW)","Gesamthöhe (m)","Rotordurchmesser (m)","Hersteller"]]
    renamedData=selectedColumns.rename(columns={"Name": "Location", "Leistung (kW)": "Power Generation(kW)","Gesamthöhe (m)":"Turbine Height (m)","Rotordurchmesser (m)":"Rotor Diameter (m)","Hersteller":"Company"})
    renamedData['Company'] = renamedData['Company'].str.split().str[0].str.upper()
    removedNull=renamedData[~renamedData['Company'].str.contains('UNBEKANNT')].copy()
    removedNull=removedNull[~removedNull['Turbine Height (m)'].str.contains('unbekannt')].copy()
    removedNull=removedNull.drop_duplicates(subset='Location', keep='first')
    removedNull['Turbine Height (m)'] = removedNull['Turbine Height (m)'].astype(float)
    removedNull['Rotor Diameter (m)'] = removedNull['Rotor Diameter (m)'].astype(float)
    cleanedData=removedNull
    return cleanedData


def transform_schleswigData(extractedData):
    selectedColumns=extractedData[["GEMEINDE","LEISTUNG","NABENHOEHE","ROTORDURCHMESSER","HERSTELLER"]]
    renamedData=selectedColumns.rename(columns={"GEMEINDE": "Location", "LEISTUNG": "Power Generation(kW)","NABENHOEHE":"Turbine Height (m)","ROTORDURCHMESSER":"Rotor Diameter (m)","HERSTELLER":"Company"})
    renamedData['Company'] = renamedData['Company'].str.split().str[0].str.upper()
    removedNull=renamedData[~renamedData['Company'].str.contains('HERSTELLER')].copy()
    removedNull=removedNull.dropna(subset=['Turbine Height (m)'])
    removedNull=removedNull.drop_duplicates(subset='Location', keep='first')
    removedNull['Turbine Height (m)'] = removedNull['Turbine Height (m)'].apply(lambda x: float(x.replace(',', '.')))
    removedNull['Rotor Diameter (m)'] = removedNull['Rotor Diameter (m)'].apply(lambda x: float(x.replace(',', '.')))
    cleanedData=removedNull
    return cleanedData


def load_bayernData(selectedbayernData,connection):
    selectedbayernData.to_sql("BayernData",connection,if_exists="replace")

def load_schleswigData(selectedschleswigData,connection):
    selectedschleswigData.to_sql("SchleswigData",connection,if_exists="replace")


def connect(databaseurl):
    connection=sqlite3.connect(databaseurl)
    return connection

if __name__ == "__main__":
    #Running the pipeline
    print("Extracting data...")
    bayernData=download_bayernData("https://drive.google.com/file/d/1_ldLqz8O1XrEj8Px7nf8AHhsq00VHDCW/view?usp=sharing")
    schleswigData=download_schleswigData("https://opendata.zitsh.de/data/llur72/opendata_WKA_ib_gv_vb_SH_200201019.xlsx")

    print("Transforming data...")
    selectedbayernData=transform_bayernData(bayernData)
    selectedschleswigData=transform_schleswigData(schleswigData)

    print("Loading data in OLTP")
    connection=connect('../data/Turbines.sqlite')
    load_bayernData(selectedbayernData,connection)
    load_schleswigData(selectedschleswigData,connection)
    connection.close()
    print("Data has been loaded!")



