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
Go to the Apache Cassandra directory and start a Cassandra server in the cmd line using cassandra -f
Once server is running, run script ETL.py. 


# ETL.py description
Build, fill and analyze the database by creating an ELT pipeline using a python driver.<br>
   > __Steps__:
- Extract the data from the logs contained in the folder for which we specify the path   
- Load the data in a new csv file: event_datafile_new.csv  
- Create a denormalized dataset to be able to query our data   
- Load the data  
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

### song_play_librairy
1. Get artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
> __sessionId\*__  
> __itemInSession\*__  
> artist  
> song  
> length  

### user_listened_librairy
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
> __userid\*__  
> __sessionid\*__  
> artist  
> song  
> firstname
> lastname

### songs
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
> __song\*__  
> __userid\*__    
> __firstname__  
> __lastname__  
 
