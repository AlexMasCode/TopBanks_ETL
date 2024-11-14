# Top 10 Largest Banks Data Automation Project

## Project Overview

This project is designed to automate the extraction, transformation, and storage of data related to the top 10 largest banks in the world, ranked by market capitalization in USD. The data engineer team has been tasked with building a system to automate this process, which will run quarterly to generate updated reports for financial analysis. The extracted data is further transformed to include market capitalizations in GBP, EUR, and INR based on current exchange rates, and is saved both locally as a CSV file and in a database for efficient querying.

## Project Structure

- **extract(url, table_attrib_start)**: Extracts bank data from a webpage containing the list of the top 10 banks by market capitalization.
- **transform(data, table_attrib_final)**: Transforms the extracted data to include market capitalizations in GBP, EUR, and INR based on exchange rates from a CSV file.
- **load_to_csv(transformed_data, csv_path)**: Saves the transformed data to a CSV file.
- **load_to_db(transformed_data, conn, table_name)**: Stores the transformed data in an SQLite database.
- **run_queries(query, conn)**: Executes SQL queries for analysis and reporting.
- **log_progress(message)**: Logs the progress of each process in a log file for tracking purposes.

## Prerequisites

- Python 3.x
- Required libraries: `requests`, `pandas`, `numpy`, `sqlite3`, `BeautifulSoup` (part of `bs4`)
- An exchange rate CSV file (`exchange_rate.csv`) containing the columns `Currency` and `Rate`.

## Project Files

- **`main.py`**: Main script containing all functions and execution flow.
- **`exchange_rate.csv`**: CSV file with exchange rates for GBP, EUR, and INR.
- **`Banks.db`**: SQLite database for storing the bank data.
- **`output/Largest_banks_data.csv`**: Output CSV file containing the transformed data.
- **`logs/code_log.txt`**: Log file for tracking the process.

## Execution Flow

1. **Data Extraction**: Scrapes bank data from a Wikipedia page and compiles a list with the `Name` and `Market Capitalization in USD`.
2. **Data Transformation**: Reads exchange rates from `exchange_rate.csv` and adds columns for `Market Capitalization in GBP`, `EUR`, and `INR`.
3. **Data Load**: Saves the transformed data locally as a CSV and loads it into an SQLite database.
4. **Queries**: Runs queries on the database to analyze the data.

## Functions

### `log_progress(message)`
Logs the current status and timestamp into `code_log.txt`.

### `extract(url, table_attrib_start)`
Scrapes bank information from the specified URL and returns it as a pandas DataFrame.

### `transform(data, table_attrib_final)`
Adds columns for market capitalization in GBP, EUR, and INR using exchange rates from `exchange_rate.csv`.

### `load_to_csv(transformed_data, csv_path)`
Saves the transformed data to a CSV file.

### `load_to_db(transformed_data, conn, table_name)`
Stores the data in an SQLite database table.

### `run_queries(query, conn)`
Executes SQL queries on the database for analysis and reporting.

## Usage

1. Clone this repository.
2. Place the `exchange_rate.csv` file in the `assets` directory with exchange rates for `GBP`, `EUR`, and `INR`.
3. Run `main.py` to start the automated data extraction, transformation, and loading process.
4. Output data will be saved in `output/Largest_banks_data.csv` and the SQLite database `Banks.db`.
