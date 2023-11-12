#Importing necessay libraries
from sqlalchemy import create_engine,Column,String,Integer,CHAR,FLOAT,TEXT,BIGINT,VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

#function to get data as csv
def get_data(filepath):
    data=pd.read_csv(filepath,sep=";")
    return data

#Creating base class object
Base=declarative_base()

#Creating Airport class this will be the query
class Airport(Base):
    __tablename__="airports"
#Added a primary key as sql alchemy do not run without primary key
    id=Column("Id",BIGINT,primary_key=True)
    column1=Column("column_1",BIGINT)
    column2=Column("column_2",TEXT)
    column3=Column("column_3",TEXT)
    column4=Column("column_4",TEXT)
    column5=Column("column_5",TEXT)
    column6=Column("column_6",TEXT)
    column7=Column("column_7",BIGINT)
    column8=Column("column_8",BIGINT)
    column9=Column("column_9",BIGINT)
    column10=Column("column_10",FLOAT)
    column11=Column("column_11",TEXT)
    column12=Column("column_12",TEXT)
    geopunkt=Column("geo_punkt",TEXT)

    def __init__(self,id,column1,column2,column3,column4,column5,column6,column7,column8,column9,column10,column11,column12,geopunkt):
        self.id=id
        self.column1=column1
        self.column2=column2
        self.column3=column3
        self.column4=column4
        self.column5=column5
        self.column6=column6
        self.column7=column7
        self.column8=column8
        self.column9=column9
        self.column10=column10
        self.column11=column11
        self.column12=column12
        self.geopunkt=geopunkt


    def __repr__(self):
        return f"({self.id},{self.column1},{self.column2},{self.column3},{self.column4},{self.column5},{self.column6},{self.column7},{self.column8},{self.column9},{self.column10},{self.column11},{self.column12},{self.geopunkt})"

#Creating sql alchemy engine object to connect to databases
engine=create_engine("sqlite:///airports.sqlite",echo=True)
Base.metadata.create_all(bind=engine)
Session=sessionmaker(bind=engine)
session=Session()


#Creating the function to create object for Airport class and inserting data to airports table from the csv
def loading(ingestedData):
    for i,row in ingestedData.iterrows():
        airport=Airport(i,row['column_1'],row['column_2'],row['column_3'],row['column_4'],
                        row['column_5'],row['column_6'],row['column_7'],row['column_8'],
                        row['column_9'],row['column_10'],row['column_11'],row['column_12'],row['geo_punkt'])
        session.add(airport)

#Calling the get_data-Extracting function 
imported_Dataframe=get_data("https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv")
#Loading in the database
loading(imported_Dataframe)


#Finally, commiting the changes
session.commit()









