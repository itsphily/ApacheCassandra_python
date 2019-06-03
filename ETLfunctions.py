'''
This script contains all the python functions necessary to create an ETL pipeline.
The functions included in this script allow us to connect to a database, create a keyspace,
extract the data from the files in the chosen directory, create tables modeled to optimize our queries,
insert the values in the new tables, and execute the queries.

Author: Philippe Habra
'''


# Imports and libraries
import pandas as pd
import cassandra
from cassandra.cluster import Cluster
import re
import os
import glob
import json
import csv
from CQLqueries import *
from ETLfunctions import*


# Connects to a given database
def connect_database(local_host_address = '127.0.0.1'):
    # Initializes a cluster
    cluster = Cluster([local_host_address])
    # Connects to Apache Cassandra
    session = cluster.connect()
    return session, cluster

# Creates a keyspace
def create_keyspace(session, name = 'sparkify'):
    # Resets and creates a database
    session.execute(drop_cassandra_keyspace)
    session.execute(create_cassandra_keyspace)
    # Connect to the Sparkify keyspace
    session.set_keyspace(name)

# Extracts the data from a given folder
def extract_data(session, folder_extension = '/event_data', datafile_new_csv = 'event_datafile_new.csv'):

    # Retrieves the  list of of filepaths
    filepath = os.getcwd() + folder_extension
    # Collect each filepath, note: walk() generates the file names in a directory tree
    for root, vdirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root, '*'))

    # Iterate through the file_path_list to extract the data from each log and store it in full_data_row_list
    full_data_rows_list = []
    for f in file_path_list:
        # Open the data logs as csv files
        with open(f, 'r', encoding='utf8', newline = '') as csvfile:
            # Creating a csv reader object which will iterate over lines in the csvfile
            csvreader = csv.reader(csvfile)
            # Point the csvreader on the next filepath (next csv file containing the event data)
            next(csvreader)
            for line in csvreader:
                full_data_rows_list.append(line)

    # Selecting a subset of the data and loading it in a csv file
    # Register the dialect that was used to read the file
    csv.register_dialect('myDialect', quoting = csv.QUOTE_ALL, skipinitialspace = True)
    # Open a  new file
    with open(datafile_new_csv, 'w', encoding = 'utf8', newline='') as f:
        # Select the dialect we registered from the reads to write in the same format
        writer = csv.writer(f, dialect = 'myDialect')
        # Write column names
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        # Enter the values from the full_data_rows_list in the corresponding columns
        for row in full_data_rows_list:
            # Check if the row we are writing is empty
            if (row[0] == ''):
                continue
            # Enter the values from each row into the corresponding columns
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    # check the number of rows in your csv file (remove # to enable check)
    # with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    #    print(sum(1 for line in f))


# Creates the tables that we need to execute query 1, 2, 3


''' 
Modeling to execute Query #1
Query#1: Get artist, song title and song 's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
This table was modeled to allow queries based on sessionid and iteminsession.
Recall that Cassandra does not support entries with similar keys
Since a user could listen to several songs within a session, I chose to use sessionid and iteminsession as primary keys
'''
def table_song_in_session_query(session):
    ## Resets(drop and create) song_in_session
    session.execute(drop_song_in_session)
    session.execute(create_song_in_session)

    ## Create a new table to fit query#1
    # Opens the new datafile we just created
    file = 'event_datafile_new.csv'
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        # skip header line
        next(csvreader)
        for line in csvreader:
        # Creates a table inserting the values we selected
        # sessionid, iteminsession, artist, song, length, PRIMARY KEY(sessionid, iteminsession)
            session.execute(insert_song_in_session, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

    # REMOVE BRACKETS TO VERIFY IF THE DATA WAS ENTERED PROPERLY IN THE TABLE
    """
    rows = session.execute(select_all_song_in_session)
    count = 0
    for entries in rows:
        count += 1
    print('There are {} entries in song_in_session'.format(count))
    # Verify the column names
    print('The column names are:', rows.column_names)
    #(remove comments if you want to see every entry in the song_in_session)
    #for row in rows:
    #    print(row.sessionid, row.iteminsession, row.artist, row.song, row.length)"""

''' 
Modeling to execute Query #2
Query #2: Give me only the following: name of artist, song(sorted by itemInSession) and user(first and last name) for userid = 10, sessionid = 182
This table was modeled to allow queries based on userid and sessionid, allowing us to understand what users typically listen to.
Recall that Cassandra does not support entries with similar keys
Since a user could start several sessions, I chose to use userid and sessionid as primary keys.
'''
def table_song_playlist_session_query(session):
    ## Resets(drop and create) song_playlist_session
    session.execute(drop_song_playlist_session)
    session.execute(create_song_playlist_session)

    ## Create a new table to fit query#2
    # Opens the new datafile we just created
    file = 'event_datafile_new.csv'
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            #userid , sessionid, itemInSession, artist, song, firstname, lastname
            session.execute(insert_song_playlist_session, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))

    # REMOVE BRACKETS TO VERIFY IF THE DATA WAS ENTERED PROPERLY IN THE TABLE
    """
    rows = session.execute(select_all_song_playlist_session)
    count = 0
    for entries in rows:
        count += 1
    print('There are {} entries in song_playlist_session'.format(count))
    # Checking the column names
    print('The column names are:', rows.column_names)
    #(remove comments if you want to see every entry in the song_playlist_session)
    #for row in rows:
    #    print(row.sessionid, row.iteminsession, row.artist, row.song, row.length)"""

''' 
Modeling to execute Query #3
Query #3: Give me every user name(first and last) in my music app history who listened to the song 'All Hands Against His Own'
This table was modeled to allow queries based on song allowing us to see which users listened to a specific song.
Recall that Cassandra does not support entries with similar keys
Since several users could listen to the same song, and that users could have the same name, I chose to use userid and song as primary keys
'''
def table_songs_listened_query(session):
    # Resets (drop and create) songs_listened
    session.execute(drop_songs_listened)
    session.execute(create_songs_listened)
    ## Create a new songs_listened to fit query#3
    # Opens the new datafile we just created
    file = 'event_datafile_new.csv'
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            # song, userid, firstname, lastname
            session.execute(insert_songs_listened, (line[9], int(line[10]), line[1], line[4]))

    # REMOVE BRACKETS TO VERIFY IF THE DATA WAS ENTERED PROPERLY IN THE TABLE
    """"
    rows = session.execute(select_all_songs_listened)
    count = 0
    for entries in rows:
        count += 1
    print('There are {} entries songs_listened'.format(count))
    # Checking the column names
    print('The column names are:', rows.column_names)
    #(remove comments if you want to see every entry in the songs_listened)
    #for row in rows:
    #    print(row.sessionid, row.iteminsession, row.artist, row.song, row.length)"""



# Query #1: Get artist, song title and song 's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
def query1(session):
    print('*'*100)
    print('Query#1 returns the following rows:')
    rows = session.execute(query1_command)
    for row in rows:
        print('Artist:', row.artist, '\t\t\t Song:', row.song, '\t\t\t length:', row.length)
    print('*' * 100, '\n')
# Query #2: Give me only the following: name of artist, song(sorted by itemInSession) and user(first and last name) for userid = 10, sessionid = 182
def query2(session):
    print('*' * 100)
    print('Query#2 returns the following rows:')
    rows = session.execute(query2_command)
    print('First name:', rows[0].firstname, '\t Last name:', rows[0].lastname)
    for row in rows:
        print('Artist:', row.artist, '\t\t\t Song:', row.song, '\t\t\t Item in session:', row.iteminsession)
    print('*' * 100, '\n')

# Query #3: Give me every user name(first and last) in my music app history who listened to the song 'All Hands Against His Own'
def query3(session):
    print('*' * 100)
    print('Query#3 returns the following rows:')
    rows = session.execute(query3_command)
    for row in rows:
        print('First name:', row.firstname, '\t\t\t Last name:', row.lastname)
    print('*' * 100, '\n')

# Drops the tables and closes the connection
def close(session, cluster):
    # Drop song_in_session, table2, and table 3
    session.execute(drop_song_in_session)
    session.execute(drop_song_playlist_session)
    session.execute(drop_songs_listened)
    # Close the session and the connection
    session.shutdown()
    cluster.shutdown()
