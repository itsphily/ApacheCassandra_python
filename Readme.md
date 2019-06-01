# Description
Built an Apache Cassandra database to analyze data on songs and user activity, and find what songs are users listening to.

# Included
>- ELT.py: Main,run to execute the 3 queries.
>- ETLfunctions.py
>- CQLqueries.py 
>- The data (included in) resides in a event_data directory of CSV files on user activity on the app.

# Required
Have apache cassandra installed and running
Install the following libraries: pandas, cassandra, Cluster (from cassandra.cluster).

# How
Build, fill and analyze the database by creating an ELT pipeline using a python driver.<br>
   > __Steps__:
- Process event_datafile_new.csv  
- Create a denormalized dataset
- Model the data tables  
- Load the data  
- Run your queries

# Build a new table event_datafile_new.csv 
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
1. Get artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
> __sessionId\*__  
> __itemInSession\*__  
> artist  
> song  
> length  
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
> __userid\*__  
> __sessionid\*__  
> artist  
> song  
> firstname
> lastname
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
> __song\*__  
> __firstname\*__  
> __lastname\*__  
 
