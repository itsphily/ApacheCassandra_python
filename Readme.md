# Description
Built an Apache Cassandra database to analyze data on songs and user activity, and find what songs are users listening to.

# Included
>- ELT.py: Main,run to execute the 3 queries.
>- ETLfunctions.py
>- CQLqueries.py 
>- The data (included in) resides in a event_data directory of CSV files on user activity on the app.

# How to install 

### Lastest version of Java
#### Java download
https://www.java.com/en/download/
#### Java SE Development Kit 8 Downloads
https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

### Pandas
Installing from PyPI: __pip install pandas__
https://pandas.pydata.org/pandas-docs/stable/install.html

### Apache Cassandra
Download the latest version of cassandra: http://cassandra.apache.org/download/
#### install Cassandra  
__Linux installation__: https://www.vultr.com/docs/how-to-install-apache-cassandra-3-11-x-on-ubuntu-16-04-lts <br>
__Windows installation__: https://www.guru99.com/download-install-cassandra.html
#### Cassandra-driver
Installing from PyPI: __pip install cassandra-driver__
https://datastax.github.io/python-driver/installation.html


# How to run
Go to the Apache Cassandra directory and start a Cassandra server in the cmd line using cassandra -f <br>
Once server is running, run script ETL.py. 


# ETL.py description
Build, fill and analyze the database by creating an ELT pipeline using a python driver.<br>
   > __Steps__:
- Extract the data from the logs contained in the folder for which we specify the path   
- Load the data in a new csv file: event_datafile_new.csv  
- Create a denormalized dataset to be able to query our data   
- Load the data in the tables modeled to execute the queries
- Run the queries

> event_datafile_new.csv contains the following columns: 
- artist 
- firstName of user
- gender of user
- item number in session
- last name of user
- length of the song
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

# Queries and tables

### song_in_session
__Query#1: Get artist, song title and song 's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4__ <br>
This song_in_session table was modeled to to execute Query #1, queries based on sessionid and iteminsession. Recall that Cassandra does not support entries with similar keys. Since a user could listen to several songs within a session, I chose to use sessionid and iteminsession as primary keys.
#### __song_in_session table__  
    > __sessionId\*__  
    > __itemInSession\*__  
    > artist  
    > song  
    > length  
Sample query(query1): Get artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4.

### song_playlist_session
__Query #2: Give me only the following: name of artist, song(sorted by itemInSession) and user(first and last name) for userid = 10, sessionid = 182__ <br>
This song_playlist_session table was modeled to allow queries based on userid and sessionid, allowing us to understand what users typically listen to.Recall that Cassandra does not support entries with similar keys. 
Since a user could start several sessions, I chose to use userid and sessionid as primary keys, I added iteminsession as a partionning key to be able to retrieve the songs in order they were listened to within the session (ordered by iteminsession).  
#### __song_playlist_session table__   
    > __userid\*__  
    > __sessionid\*__  
    > artist  
    > song  
    > firstname
    > lastname
Sample query(query2): Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182.

### songs_listened
__Query #3: Give me every user name(first and last) in my music app history who listened to the song 'All Hands Against His Own'__
This songs_listened table was modeled to allow queries based on song allowing us to see which users listened to a specific song.
Recall that Cassandra does not support entries with similar keys.Since several users could listen to the same song, and that users could have the same name, I chose to use userid and song as primary keys.
#### __songs_listened table__ 
    > __song\*__  
    > __userid\*__    
    > __firstname__  
    > __lastname__  
Sample query(query3): Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'.