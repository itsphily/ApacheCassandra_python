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

# Connect to a database
session, cluster = connect_database()
# Create a keyspace
create_keyspace(session)


## This function expects a folder with the logs from which we retrieve the data
# The fonction will fetch all the paths to the logs inside the folder use the filepath to open the file
# Load the content of the file in a list, and into a new csv file (which we will use to create our tables for the analysis)
extract_data(session)


# Use the extracted data to create new tables to fit query#1,2 and 3
table_song_in_session_query(session)
table_song_playlist_session_query(session)
table_songs_listened_query(session)

## Execute the queries
query1(session)
query2(session)
query3(session)

# Close the cluster and the connection to the database
close(session, cluster)