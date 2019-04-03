"""This module will process the csv files"""

import os
import sys
import pandas as pd
import dateutil



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
    flag = True
    for row in df[['Lcl Date', 'Lcl Time', 'UTCOfst']].iterrows():

        print(row[1])
        flag = False
        if pd.notna(row[1]['Lcl Date']):
            date_time = row[1]['Lcl Date'] + 'T' + row[1]['Lcl Time'] + row[1]['UTCOfst']
            # ISO 8601 "%Y-%m-%dT%H:%M:%S%z"
            date_time_list.append(dateutil.parser.parse(date_time))
        else:
            df = df[pd.notna(df['Lcl Date'])]

    dti = pd.to_datetime(date_time_list)
    del df['Lcl Date'], df['Lcl Time'], df['UTCOfst']
    df.index = dti
    df.index.name = 'datetime'

    return df


def aggregate_data(df):
    """aggregates columns"""
    df['CHT'] = df[['E1 CHT1', 'E1 CHT2', 'E1 CHT3', 'E1 CHT4']].mean(axis=1)
    del df['E1 CHT1'], df['E1 CHT2'], df['E1 CHT3'], df['E1 CHT4']

    df['EGT'] = df[['E1 EGT1', 'E1 EGT2', 'E1 EGT3', 'E1 EGT4']].mean(axis=1)
    del df['E1 EGT1'], df['E1 EGT2'], df['E1 EGT3'], df['E1 EGT4']

    df['Altitude'] = df[['AltB', 'AltMSL', 'AltGPS']].mean(axis=1)
    del df['AltGPS'], df['AltB'], df['AltMSL']


def delete_data(df):
    """deletes irrelevant data columns and data from startup until AHRS initialized"""
    # del df['BaroA'], df['NAV1'], df['NAV2'], df['COM1'], df['COM2'], df['VCDI'], df['HCDI'], df['WptDst'], \
    #    df['WptBrg'], df['MagVar'], df['AfcsOn'], df['RollM'], df['PitchM'], df['RollC'], df['PichC'], df['GPSfix'], \
    #    df['HAL'], df['VAL'], df['HPLwas'], df['HPLfd'], df['VPLwas'], df['AtvWpt'], df['HSIS']

    return df[pd.notna(df.Pitch)]


def resample_data(df, minutes):
    """Resamples the data to the given minute interval"""
    return df.resample(str(minutes) + 'Min').mean()


def import_files(file_names):
    """Returns all file objects after minor processing"""
    data = []
    for name in file_names:
        print(name)
        temp_df = pd.read_csv(name, header=2, skipinitialspace=True, index_col=False, engine='python', skipfooter=1, usecols=[
              0,1,2,4,5,6,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,37,44,45
        ])
        # only accept data greater than 10 minutes
        if temp_df.shape[0] > 600:
            data.append(temp_df)

    return data


def run():
    if len(sys.argv) < 2:
        print("Usage: python src/process.py DATAPATH")
        sys.exit(1)
    else:
        path = sys.argv[1]
        raw_data = import_files(get_files(path))
        final_data = []

        for each in raw_data:
            each = create_date_index(each)
            each = delete_data(each)
            each.fillna(0, inplace=True)
            aggregate_data(each)
            # each = resample_data(each, 1)
            final_data.append(each)
            each.to_csv('data/test/output.csv')

        print(len(final_data))


if __name__ == '__main__':
    run()
