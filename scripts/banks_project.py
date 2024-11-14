import os
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import sqlite3
import requests
import numpy as np


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    log_dir = '../logs'

    logfile_name = os.path.join(log_dir, 'code_log.txt')

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    with open(logfile_name, 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attrib_start):
    log_progress("dwd")
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    table = data.find('tbody')
    raws = table.find_all('tr')

    list_data = []
    for raw in raws:
        col = raw.find_all('td')

        if len(col) != 0:
            dict_data = {"Name": col[1].get_text(strip=True),
                         "MC_USD_Billion": col[2].get_text(strip=True)}

            list_data.append(dict_data)

    dataframe = pd.DataFrame(list_data, columns=table_attrib_start)
    return dataframe


def transform(data, table_attrib_final):
    # Add the new columns: "MC_GBP_Billion", "MC_EUR_Billion", and "MC_INR_Billion"
    data = data.reindex(columns=table_attrib_final)

    exchange_rate = pd.read_csv('../assets/exchange_rate.csv')

    # Convert exchange_rate.csv into dictionary
    dict_exchange_rate = exchange_rate.set_index('Currency')['Rate'].to_dict()

    data['MC_GBP_Billion'] = [np.round(float(x) * dict_exchange_rate['GBP'], 2) for x in data['MC_USD_Billion']]
    data['MC_EUR_Billion'] = [np.round(float(x) * dict_exchange_rate['EUR'], 2) for x in data['MC_USD_Billion']]
    data['MC_INR_Billion'] = [np.round(float(x) * dict_exchange_rate['INR'], 2) for x in data['MC_USD_Billion']]

    return data


def load_to_csv(transformed_data, csv_path):
    if not os.path.exists('../output'):
        os.makedirs('../output')
    transformed_data.to_csv(csv_path, index=False)


def load_to_db():
    pass


def main():
    page_url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
    table_attrib_start = ["Name", "MC_USD_Billion"]
    table_attrib_final = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
    output_csv_path = '../output/Largest_banks_data.csv'

    db_name = 'Banks.db'
    db_table_name = 'Largest_banks'

    extracted_data = extract(page_url, table_attrib_start)

    transformed_data = transform(extracted_data, table_attrib_final)

    load_to_csv(transformed_data, output_csv_path)


if __name__ == main():
    main()