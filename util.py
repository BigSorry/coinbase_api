import pickle
import csv
import os
import pandas as pd
import gzip
import json

def readZIP(file_path):
    " Reads a gzipped JSON file and returns a list of dictionaries."
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        data = []
        for line in f:
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Skipping malformed line: {e}")
        return data
def savePickle(path, python_object):
    with open(path, "wb") as fp:
        pickle.dump(python_object, fp)


def readPickle(path):
    with open(path, "rb") as fp:
        data = pickle.load(fp)
        return data


def printDict(dict, print_keys=None):
    for id_key, values in dict.items():
        if print_keys is None or id_key in print_keys:
            print(id_key)
            print(values)
            print()


def saveCSV(data, fieldnames, filename="data.csv"):
    """
    Save collected data to a CSV file, ensuring unique trade IDs are kept.

    :param data: List of dictionaries to save
                 For historical data list of data for one coin
    :param filename: The CSV filename to save to
    """
    try:
        with open(filename, mode="a", newline="") as file:
            writer = csv.DictWriter(file, fieldnames)
            # Write the header if the file is empty
            file.seek(0, 2)  # Move to the end of the file
            if file.tell() == 0:  # Check if the file is empty
                writer.writeheader()
            for row_dict in data:
                writer.writerow(row_dict)
    except Exception as e:
        print(f"Error saving to CSV: {e}")


def readCSV(file_path):
    try:
        # Check if the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file at path '{file_path}' does not exist.")
        # Read the CSV file
        data = pd.read_csv(file_path)
        return data

    except FileNotFoundError as e:
        print(e)
        return pd.DataFrame()  # Return an empty DataFrame if the file is not found

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()


def saveHistoricalDataCSV(data_dict, fieldnames, filename="data.csv"):
    """
    Save collected data to a CSV file, ensuring unique trade IDs are kept.

    :param data: dictionary with candle data based on a time-intervals
    :param filename: The CSV filename to save to
    """
    try:
        with open(filename, mode="a", newline="") as file:
            writer = csv.DictWriter(file, fieldnames)
            # Write the header if the file is empty
            file.seek(0, 2)  # Move to the end of the file
            if file.tell() == 0:  # Check if the file is empty
                writer.writeheader()
            for timestamp_begin, timestamp_data in data_dict.items():
                for row_values in timestamp_data:
                    row_dict = dict(zip(fieldnames, row_values))
                    writer.writerow(row_dict)
    except Exception as e:
        print(f"Error saving to CSV: {e}")
