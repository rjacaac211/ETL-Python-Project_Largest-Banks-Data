import sqlite3
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


def log_progress(message):
    '''
    This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing
    '''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # Timestamp now
    timestamp = now.strftime(timestamp_format)
    with open(log_file,'a') as f:
        f.write(f'{timestamp} : {message}\n')


def extract(url, table_attribs):
    '''
    This function aims to extract the required
    information from the website and save it to a data frame.
    The function returns the data frame for further processing.
    '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                        'Name': col[1].text.strip(),
                        'MC_USD_Billion': float(col[2].text.strip())
                        }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, csv_path):
    '''
    This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies
    '''
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate_dict = exchange_rate_df.set_index('Currency')['Rate'].to_dict()
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict.get('GBP', 1), 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict.get('EUR', 1), 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict.get('INR', 1), 2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    '''
    This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.
    '''
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    '''
    This function saves the final data frame to a database
    table with the provided name. Function returns nothing.
    '''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    '''
    This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing.
    '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name','MC_USD_Billion']
csv_path = 'exchange_rate.csv'
output_path = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'


# Execute ETL Process

# EXTRACT

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

# TRANSFORM

df = transform(df, csv_path)

log_progress('Data transformation complete. Initiating loading process')

# LOAD

load_to_csv(df, output_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the queries')

# QUERY

query_statement = f'SELECT * FROM {table_name}'
run_query(query_statement, sql_connection)

query_statement = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
run_query(query_statement, sql_connection)

query_statement = f'SELECT Name FROM {table_name} LIMIT 5'
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()