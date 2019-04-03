"""This module will process the csv files"""

import os
import sys
import pandas as pd


def get_files(path):
    """returns all file names in the given directory"""
    all_files = []
    try:
        for root, dirs, files in os.walk(path):
            for name in files:
                all_files.append(os.path.join(root, name))
    except OSError:
        print(f"The path: {path} is not valid")
        sys.exit(1)
    return all_files


def import_files(file_names):
    """Returns all file objects"""
    data = []
    for name in file_names:
        temp_df = pd.read_csv(name, header=2)
        if temp_df.shape[0] > 600:
            data.append(temp_df)
    return data


def run():
    if len(sys.argv) < 2:
        print("Usage: python src/process.py DATAPATH")
        sys.exit(1)
    else:
        path = sys.argv[1]
        data = import_files(get_files(path))



if __name__ == '__main__':
    run()
