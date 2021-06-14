import os
import csv
import glob
import traceback
from cassandra.cluster import Cluster
from cql_queries import *

#%% Insert data into tables


def process_data(data_folder, generated_file):
    """Process and combine all the CSV files in the directory passed,
    and save it with the filename passed (generated_file).

    Parameters
    ----------
    data_folder : str
        Filepath of the directory containing the all the csv files
        to be processed.
    generated_file : str
        Filename of the CSV file to be generated.
    """
    print('Processing data and saving it in a the csv file...')
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


def insert_data_sessiondb(session, generated_file):
    """Reads the generated csv file and insert its rows into the 
    session_library table.

    Parameters
    ----------
    session : cassandra.Cluster.Session
        A collection of connection pools for each host in the cluster. It will
        be used to execute queries and statements using execute() method.
    generated_file : str
        Filename of the CSV file generated using `process_data()`.
    """
    print('Inserting data into session_library...')
    file = generated_file
    query = session_table_insert

    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)
        for line in csvreader:
            session.execute(query, (int(line[8]), int(line[3]),
                                    line[0], line[9], float(line[5])))


def insert_data_userdb(session, generated_file):
    """Reads the generated csv file and insert its rows into the user_library
    table.

    Parameters
    ----------
    session : cassandra.Cluster.Session
        A collection of connection pools for each host in the cluster. It will
        be used to execute queries and statements using execute() method.
    generated_file : str
        Filename of the CSV file generated using `process_data()`.
    """
    print('Inserting data into user_library...')
    file = generated_file
    query = user_table_insert

    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)
        for line in csvreader:
            session.execute(query, (int(line[10]), int(line[8]), int(line[3]),
                                    line[0], line[9], line[1], line[4]))


def insert_data_songdb(session, generated_file):
    """Reads the generated csv file and insert its rows into the song_library
    table.

    Parameters
    ----------
    session : cassandra.Cluster.Session
        A collection of connection pools for each host in the cluster. It will
        be used to execute queries and statements using execute() method.
    generated_file : str
        Filename of the CSV file generated using `process_data()`.
    """
    print('Inserting data into song_library...')
    file = generated_file
    query = song_table_insert

    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)
        for line in csvreader:
            session.execute(query, (line[9], int(line[10]), int(line[8]),
                                    int(line[3]), line[1], line[4]))


#%% Main execution

def main():
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
        session.set_keyspace('music_library')
    except Exception as e:
        print('Error connecting to Cassandra cluster.')
        print(e)

    data_folder = '/event_data'
    generated_file = 'event_datafile_new.csv'

    try:
        process_data(data_folder, generated_file)
    except Exception as e:
        print('Error processing data.')
        print(e)

    try:
        insert_data_sessiondb(session, generated_file)
        insert_data_userdb(session, generated_file)
        insert_data_songdb(session, generated_file)
    except Exception as e:
        print('Error inserting data to databases.')
        print(e)
        print(traceback.format_exc())

    session.shutdown()
    cluster.shutdown()


if __name__ == '__main__':
    main()
