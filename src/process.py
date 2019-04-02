"""This module will process the csv files"""
import pandas
import os
import sys










def get_files(path):
    all_files = []
    try:
        for root, dirs, files in os.walk(path):
            all_files += files
    except OSError:
        print(f"The path: {path} is not valid")
        sys.exit(1)
    return all_files


def run():
    if len(sys.argv) < 2:
        print("Usage: python src/process.py DATAPATH")
        sys.exit(1)
    else:
        path = sys.argv[1]




