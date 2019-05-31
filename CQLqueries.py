
# Drop and create the keyspace
drop_cassandra_keyspace = """DROP KEYSPACE Sparkify """
create_cassandra_keyspace = """CREATE KEYSPACE Sparkify 
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""


# Drop and create table1, table2, table 3 CQL command
drop_table1 = """ DROP TABLE IF EXISTS table1"""
create_table1 = """CREATE TABLE IF NOT EXISTS table1 
(sessionId text, itemInSession text, artist text, song text, length text, PRIMARY KEY(sessionId, itemInSession))
"""
drop_table2 = """ DROP TABLE IF EXISTS table2"""
create_table2 = """CREATE TABLE IF NOT EXISTS table2 
(userId text, sessionid text,itemInSession text, artist text, song text,  firstname text, lastname text, PRIMARY KEY((userid, sessionid),itemInSession))
"""
drop_table3 = """ DROP TABLE IF EXISTS table3"""
create_table3 = """CREATE TABLE IF NOT EXISTS table3 
(song text, firstname text, lastname text, PRIMARY KEY(song, firstname, lastname))
"""


# Insert the data in the tables
insert_table1 = """INSERT into table1 (sessionId, itemInSession, artist, song, length) 
VALUES(%s, %s, %s, %s, %s)"""
insert_table2 = """INSERT into table2 (userId, sessionid, artist, song, itemInSession, firstname, lastname)
VALUES(%s, %s, %s, %s, %s, %s, %s)"""
insert_table3 = """INSERT into table3 (song, firstname, lastname) 
VALUES(%s, %s, %s)"""


# Query the data
# Query #1: Get artist, song title and song 's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
query1_command = """ SELECT * FROM table1 
WHERE sessionId = '338' AND itemInSession = '4' """
# Query #2: Give me only the following: name of artist, song(sorted by itemInSession) and user(first and last name) for userid = 10, sessionid = 182
query2_command = """SELECT artist, song, firstname, lastname FROM table2 
WHERE userid = '10' AND sessionid = '182' 
ORDER BY itemInSession """
# Query #3: Give me every user name(first and last) in my music app history who listened to the song 'All Hands Against His Own'
query3_command = """SELECT firstname, lastname FROM table3 
WHERE song =  'All Hands Against His Own' """


# Select all the data in table1,2,3
select_all_table1 = "SELECT * FROM table1;"
select_all_table2 = "SELECT * FROM table2;"
select_all_table3 = "SELECT * FROM table3;"