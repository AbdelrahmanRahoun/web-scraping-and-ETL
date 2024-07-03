# Importing the required libraries
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
      code execution to a log file. Function returns nothing'''
    time=datetime.now().strftime('%Y-%M-%D %H:%M:%S')
    log_message=f"{time}:{message}\n"
    with open('code_log.txt','a') as log_file:
        log_file.write(log_message)

def extract(url, table_columns):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_columns)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[1].find_all('a') is not None and '_' not in col[1]:
                data_dict = {"Bank": col[1].text.strip(),
                             "Market_Cap": col[2].text.strip()}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    return df

def transform(top_10_banks):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    change_rate = pd.read_csv('exchange_rate.csv')
    dct = change_rate.set_index('Currency').to_dict()['Rate']
    top_10_banks['Market_Cap'] = top_10_banks['Market_Cap'].astype(float)
    top_10_banks['MC_GBP_Billion'] = [np.round(x * dct['GBP'], 2) for x in top_10_banks['Market_Cap']]
    top_10_banks['MC_EUR_Billion'] = [np.round(x * dct['EUR'], 2) for x in top_10_banks['Market_Cap']]
    top_10_banks['MC_INR_Billion'] = [np.round(x * dct['INR'], 2) for x in top_10_banks['Market_Cap']]

    return top_10_banks

def load_to_csv(transformed_data):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    transformed_data.to_csv('top_10_banks.csv')

def load_to_db(top_10_banks , sql_connection , table_name ):
    ''' This function saves the final data frame to a database
      table with the provided name. Function returns nothing.'''
    conn=sqlite3.connect(sql_connection)
    transformed_data.to_sql(table_name,conn)

def run_queries(query_statement, sqlite_connection):
    ''' This function runs the query on the database table and
      prints the output on the terminal. Function returns nothing. '''
    df = pd.read_sql_query(query_statement, sqlite_connection)

    return df



log_progress('Preliminaries complete. Initiating ETL process')
top_10_banks=extract("https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks",['Bank','Market_Cap'])
log_progress('Data extraction complete. Initiating Transformation process')
transformed_data=transform(top_10_banks)
log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(transformed_data)
log_progress('Data saved to CSV file')
load_to_db(top_10_banks  , 'Banks.db' , 'Largest_banks')
log_progress('Data loaded to Database as a table, Executing queries')
#Print the contents of the entire table
run_queries('SELECT * FROM Largest_banks',sqlite3.connect('Banks.db'))
#Print the average market capitalization of all the banks in Billion USD.
run_queries('SELECT AVG(MC_GBP_Billion) FROM Largest_banks' , sqlite3.connect('Banks.db'))
#Print only the names of the top 5 banks
run_queries('SELECT Bank from Largest_banks LIMIT 5',sqlite3.connect('Banks.db'))
log_progress('Process Complete')