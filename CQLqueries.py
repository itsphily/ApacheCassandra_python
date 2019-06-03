
'''
This script contains all the CQL queries that are needed to:
Create/drop a keyspace, create/drop tables, insert values in those tables and execute the queries.

Author: Philippe Habra
'''

# Drop and create the keyspace
drop_cassandra_keyspace = """DROP KEYSPACE IF EXISTS Sparkify """
create_cassandra_keyspace = """CREATE KEYSPACE IF NOT EXISTS Sparkify 
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""

## Drop and create song_in_session, song_playlist_session, songs_listened  CQL command
## song_in_session was modeled to to execute Query #1
# Query#1: Get artist, song title and song 's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# This table was modeled to allow queries based on sessionid and iteminsession.
# Recall that Cassandra does not support entries with similar keys
# Since a user could listen to several songs within a session, I chose to use sessionid and iteminsession as primary keys
drop_song_in_session = """ DROP TABLE IF EXISTS song_in_session"""
create_song_in_session = """CREATE TABLE IF NOT EXISTS song_in_session
(sessionid int, iteminsession int, artist text, song text, length float, PRIMARY KEY(sessionid, iteminsession))
"""

## Modeling to execute Query #2
# Query #2: Give me only the following: name of artist, song(sorted by itemInSession) and user(first and last name) for userid = 10, sessionid = 182
# This table was modeled to allow queries based on userid and sessionid, allowing us to understand what users typically listen to.
# Recall that Cassandra does not support entries with similar keys
# Since a user could start several sessions, I chose to use userid and sessionid as primary keys
# I added iteminsession as a partionning key to be able to retrieve the songs in order they were listened to within the session (ordered by iteminsession)
drop_song_playlist_session = """ DROP TABLE IF EXISTS song_playlist_session"""
create_song_playlist_session = """CREATE TABLE IF NOT EXISTS song_playlist_session 
(userid int, sessionid int, itemInSession int, artist text, song text,  firstname text, lastname text, PRIMARY KEY(userid, sessionid,iteminsession))
"""

## Modeling to execute Query #3
# Query #3: Give me every user name(first and last) in my music app history who listened to the song 'All Hands Against His Own'
# This table was modeled to allow queries based on song allowing us to see which users listened to a specific song.
# Recall that Cassandra does not support entries with similar keys
# Since several users could listen to the same song, and that users could have the same name, I chose to use userid and song as primary keys
drop_songs_listened = """ DROP TABLE IF EXISTS songs_listened"""
create_songs_listened = """CREATE TABLE IF NOT EXISTS songs_listened 
(song text, userid int, firstname text, lastname text, PRIMARY KEY((song), userid))
"""

# Insert the data in the tables
insert_song_in_session = """INSERT into song_in_session (sessionid, iteminsession, artist, song, length) 
VALUES(%s, %s, %s, %s, %s)"""
insert_song_playlist_session = """INSERT into song_playlist_session (userid, sessionid, itemInSession, artist, song, firstname, lastname)
VALUES(%s, %s, %s, %s, %s, %s, %s)"""
insert_songs_listened = """INSERT into songs_listened (song, userid, firstname, lastname) 
VALUES(%s, %s, %s, %s)"""

# Query the data
# Query #1: Get artist, song title and song 's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
query1_command = """ SELECT artist, song, length FROM song_in_session
WHERE sessionId = 338 AND itemInSession = 4 """
# Query #2: Give me only the following: name of artist, song(sorted by itemInSession) and user(first and last name) for userid = 10, sessionid = 182
query2_command = """SELECT artist, song, firstname, lastname, iteminsession FROM song_playlist_session 
WHERE userid = 10 AND sessionid = 182 """
# Query #3: Give me every user name(first and last) in my music app history who listened to the song 'All Hands Against His Own'
query3_command = """SELECT firstname, lastname FROM songs_listened 
WHERE song =  'All Hands Against His Own' """


# Select all the data in table1,2,3
select_all_song_in_session = "SELECT * FROM song_in_session;"
select_all_song_playlist_session = "SELECT * FROM song_playlist_session ;"
select_all_songs_listened = "SELECT * FROM songs_listened;"