import os
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import sqlite3
import requests
import numpy as np


def log_progress(message):
    """
    Logs a progress message with a timestamp to a log file.

    Args:
        message (str): The message to be logged.
    """
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    log_dir = '../logs'

    logfile_name = os.path.join(log_dir, 'code_log.txt')

    # If path doesn't exist, creates it
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    with open(logfile_name, 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attrib_start):
    """
    Extracts bank data from a specified URL.

    This function scrapes the webpage at the given URL, parses the HTML to find the table body,
    and extracts the bank names and their market capitalization in USD.

    Args:
        url (str): The URL of the webpage to scrape.
        table_attrib_start (list): A list of column names for the initial DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted bank names and their market capitalization in USD.
                      Returns an empty DataFrame if an error occurs.
    """
    try:
        log_progress("Data extraction started")
        html_page = requests.get(url).text
        data = BeautifulSoup(html_page, 'html.parser')

        table = data.find('tbody')
        raws = table.find_all('tr')

        list_data = []
        for raw in raws:
            col = raw.find_all('td')
            if len(col) != 0:
                dict_data = {
                    "Name": col[1].get_text(strip=True),
                    "MC_USD_Billion": col[2].get_text(strip=True)
                }
                list_data.append(dict_data)

        dataframe = pd.DataFrame(list_data, columns=table_attrib_start)

        log_progress("Data extraction finished")
        return dataframe
    except Exception as e:
        log_progress(f"Error occurred with data extraction : {e}")
        return pd.DataFrame()


def transform(data, table_attrib_final):
    """
    Transforms the extracted data by adding market capitalization in GBP, EUR, and INR.

    This function reads exchange rates from a CSV file and calculates the market capitalization
    in GBP, EUR, and INR based on the USD values. It then adds these as new columns to the DataFrame.

    Args:
        data (pd.DataFrame): The DataFrame containing the extracted data.
        table_attrib_final (list): A list of column names for the transformed DataFrame.

    Returns:
        pd.DataFrame: The transformed DataFrame with additional columns for GBP, EUR, and INR market capitalization.
                      Returns an empty DataFrame if an error occurs.
    """
    try:
        log_progress("Data transformation started")

        # Add the new columns: "MC_GBP_Billion", "MC_EUR_Billion", and "MC_INR_Billion"
        data = data.reindex(columns=table_attrib_final)

        exchange_rate = pd.read_csv('../assets/exchange_rate.csv')

        # Convert exchange_rate.csv into dictionary
        dict_exchange_rate = exchange_rate.set_index('Currency')['Rate'].to_dict()

        data['MC_GBP_Billion'] = [
            np.round(float(x) * dict_exchange_rate['GBP'], 2) for x in data['MC_USD_Billion']
        ]
        data['MC_EUR_Billion'] = [
            np.round(float(x) * dict_exchange_rate['EUR'], 2) for x in data['MC_USD_Billion']
        ]
        data['MC_INR_Billion'] = [
            np.round(float(x) * dict_exchange_rate['INR'], 2) for x in data['MC_USD_Billion']
        ]

        log_progress("Data transformation finished")
        return data
    except FileNotFoundError as e:
        log_progress(f"File error  : {e}")
        return pd.DataFrame()
    except Exception as e:
        log_progress(f"Error occurred with data transformation : {e}")
        return pd.DataFrame()


def load_to_csv(transformed_data, csv_path):
    """
    Loads the transformed data into a CSV file.

    This function saves the provided DataFrame to a specified CSV path. If the output directory
    does not exist, it creates the directory before saving the file.

    Args:
        transformed_data (pd.DataFrame): The DataFrame containing the transformed data.
        csv_path (str): The file path where the CSV will be saved.
    """
    log_progress("Load data to csv started")

    if not os.path.exists('../output'):
        os.makedirs('../output')
    transformed_data.to_csv(csv_path, index=False)

    log_progress("Load data to csv finished")


def load_to_db(transformed_data, conn, table_name):
    """
    Loads the transformed data into an SQLite database.

    This function writes the provided DataFrame to a specified table within the SQLite database.
    If the table already exists, it replaces it.

    Args:
        transformed_data (pd.DataFrame): The DataFrame containing the transformed data.
        conn (sqlite3.Connection): The SQLite database connection object.
        table_name (str): The name of the table where data will be inserted.
    """
    log_progress("Load data to db started")
    transformed_data.to_sql(table_name, conn, if_exists='replace', index=False)
    log_progress("Load data to db finished")


def run_queries(query, conn):
    """
    Executes a SQL query on the provided database connection and prints the results.

    This function logs the start and completion of the query execution. It also prints the query
    and it's output to the console.

    Args:
        query (str): The SQL query to be executed.
        conn (sqlite3.Connection): The SQLite database connection object.
    """
    print(query)
    log_progress(f"Run query '{query}' started")

    query_output = pd.read_sql(query, conn)

    print(query_output)
    log_progress(f"Run query '{query}' executed")


def main():
    """
    Main function to execute the data extraction, transformation, and loading process.

    This function orchestrates the entire workflow:
    1. Extracts data from a specified URL.
    2. Transforms the extracted data by adding market capitalization in different currencies.
    3. Loads the transformed data into a CSV file and an SQLite database.
    4. Runs predefined SQL queries to analyze the data.
    """
    page_url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'

    table_attrib_start = ["Name", "MC_USD_Billion"]
    table_attrib_final = [
        "Name",
        "MC_USD_Billion",
        "MC_GBP_Billion",
        "MC_EUR_Billion",
        "MC_INR_Billion"
    ]
    output_csv_path = '../output/Largest_banks_data.csv'

    db_name = '../Banks.db'
    db_table_name = 'Largest_banks'

    extracted_data = extract(page_url, table_attrib_start)

    transformed_data = transform(extracted_data, table_attrib_final)

    load_to_csv(transformed_data, output_csv_path)

    conn = sqlite3.connect(db_name)
    load_to_db(transformed_data, conn, db_table_name)

    query1 = 'SELECT * FROM Largest_banks'
    query2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
    query3 = 'SELECT Name FROM Largest_banks LIMIT 5'

    # Print the contents of the entire table
    run_queries(query1, conn)
    # Print the average market capitalization of all the banks in Billion GBP.
    run_queries(query2, conn)
    # Print only the names of the top 5 banks
    run_queries(query3, conn)

    conn.close()


if __name__ == "__main__":
    main()
