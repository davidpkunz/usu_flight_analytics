"""This module will process the csv files"""

import os
import sys
import pandas as pd
import numpy as np
import dateutil
import matplotlib.pyplot as pyplot

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


def create_date_index(df):
    """takes the seperated data, time, utc offset columns and combines them into one and sets as the index"""
    date_time_list = []
    for row in df[['Lcl Date', 'Lcl Time', 'UTCOfst']].iterrows():
        date_time = row[1]['Lcl Date'] + 'T' + row[1]['Lcl Time'].strip() + row[1]['UTCOfst'].strip()
        # ISO 8601 "%Y-%m-%dT%H:%M:%S%z"
        date_time_list.append(dateutil.parser.parse(date_time))

    dti = pd.to_datetime(date_time_list)
    del df['Lcl Date'], df['Lcl Time'], df['UTCOfst']
    df.index = dti
    df.index.name = 'datetime'


def aggregate_data(df):
    """aggregates columns"""
    df['CHT'] = df[['E1 CHT1', 'E1 CHT2', 'E1 CHT3', 'E1 CHT4']].mean(axis=1)
    del df['E1 CHT1'], df['E1 CHT2'], df['E1 CHT3'], df['E1 CHT4']

    df['EGT'] = df[['E1 EGT1', 'E1 EGT2', 'E1 EGT3', 'E1 EGT4']].mean(axis=1)
    del df['E1 EGT1'], df['E1 EGT2'], df['E1 EGT3'], df['E1 EGT4']

    # This one doesn't work, but the others do
    df['Alt Avg'] = df[['AltB', 'AltMSL', 'AltGPS']].astype('float').mean(axis=1)
    del df['AltGPS'], df['AltB'], df['AltMSL']


def delete_data(df):
    """deletes irrelevant data columns and data from startup until AHRS initialized"""
    del df['BaroA'], df['NAV1'], df['NAV2'], df['COM1'], df['COM2'], df['VCDI'], df['HCDI'], df['WptDst'], \
        df['WptBrg'], df['MagVar'], df['AfcsOn'], df['RollM'], df['PitchM'], df['RollC'], df['PichC'], df['GPSfix'], \
        df['HAL'], df['VAL'], df['HPLwas'], df['HPLfd'], df['VPLwas'], df['AtvWpt']

    return df[df.Pitch.str.strip() != '']


def resample_data(df, minutes):
    """Resamples the data to the given minute interval"""
    df.resample(str(minutes) + 'Min')


def import_files(file_names):
    """Returns all file objects after minor processing"""
    data = []
    for name in file_names:
        temp_df = pd.read_csv(name, header=2)

        # only accept data greater than 10 minutes
        if temp_df.shape[0] > 600:
            # remove whitespace from data and headers
            data_frame_trimmed = temp_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

            new_headers = []
            for col in data_frame_trimmed.columns:
                new_headers.append(col.strip())

            data_frame_trimmed.columns = new_headers
            data.append(data_frame_trimmed)

    return data


def run():
    if len(sys.argv) < 2:
        print("Usage: python src/process.py DATAPATH")
        sys.exit(1)
    else:
        path = sys.argv[1]
        data = import_files(get_files(path))
        for each in data:
            create_date_index(each)
            each = delete_data(each)
            aggregate_data(each)

            resample_data(each, 5)
            each.to_csv('data/test.csv')



if __name__ == '__main__':
    run()
