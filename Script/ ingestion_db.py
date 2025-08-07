#jai saraswati mata
import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
#here i will create the log_funtion for see the aye error is occuring in the code.
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)
#Here we will use sqlite for sql quries.
engine=create_engine('sqlite:///inventory.db')
#If our data is conming from server and we have to store the sata continuasely in data base.so we will have to type the script.
def ingest_db(df,table_name,engine):
    '''thise function will ingest the dataframe into database table'''
    df.to_sql(table_name, con = engine,if_exists='replace',index=False)
#Remve the .ipynd file
def load_raw_data():
    '''thise function will load the CSVS as DataFrame and ingest into db'''
    start = time.time()
    for file in os.listdir('vendor_data'):
        if ".csv" in file:
            df=pd.read_csv('vendor_data/'+file)
            logging.info(f'Ingesting {file} in db')#if you want to print any normal message
            ingest_db(df,file[:-4],engine)
    end = time.time()
    total_time = (end-start)/60
    logging.info('--------Ingestion Complete---------')
    logging.info(f'Total time taken: {total_time} minuts')

#from here our script will start working .
if __name__ == '__main__':
    load_raw_data()
