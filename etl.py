import re
import os
import glob
import json
import csv
import numpy as np
import pandas as pd
from cassandra.cluster import Cluster

#%% Global variables

data_folder = '/event_data'
generated_file = 'event_datafile_new.csv'

#%% Insert data into tables

def process_data():
    # Creates list of csv filepaths
    filepath = os.getcwd() + data_folder
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root, '*'))

    # Combine the csv into one file
    full_data_rows_list = []
    for f in file_path_list:
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for line in csvreader:
                full_data_rows_list.append(line)

    csv.register_dialect('myDialect', 
                         quoting=csv.QUOTE_ALL,
                         skipinitialspace=True)

    with open(generated_file, 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 
                         'lastName', 'length', 'level', 'location', 
                         'sessionId', 'song', 'userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4],
                             row[5], row[6], row[7], row[8],
                             row[12], row[13], row[16]))

def insert_data():
    return 0

# def insert_data_userdb():

# def insert_data_sessiondb():

# def insert_data_songdb():


#%% Main execution

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute('''
        CREATE KEYSPACE IF NOT EXISTS music_library
        WITH REPLICATION =
        {'class': 'SimpleStrategy', 'replication_factor': 1}
    ''')
    session.set_keyspace('music_library')

    process_data()
    

if __name__ == '__main__':
    main()
