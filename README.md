# 30-Databases
Java, MySQL, JDBC. CRUD, JOIN, Transaction. Connection, Statement, PreparedStatement, ResultSet.  

This is a learning project that connects a Java application with a MySQL database.  

Packages:
-  test_connection package: test if it is a successful connection with the database. Do some basic CRUD operation: create a schema, create a table, insert, update, delete data.  
-  music package has classes that work with a music database with three tables: songs, albums, artists.    

Classes in the music package:    
o	A class that will hold methods for each query, or other operations.(ex: Datasource).  
o	Classes for every table in  (ex: Song).  
o	A class for view or join result if we want to retrieve data from many tables (ex: SongArtist).  
o	Main class will apply methods from the Datasource class. Main class doesn’t know about the database.  

We use: 
-  Conneciton class to connect to database.    
-  Statement to execute the SQL.    
-  PreparedStatement to execut SQL and avoid SQL injection.  
-  ResultSet to get the return data.  
-  ResultSetMetaData to get metadata (ex: column name).  
-  commit() to execute a transaction with three inserts.  

More to add:  
•	Different JOIN Types  
•	Normalize Form  examples  

Notes   
When running the project, in the console you will be asked two times for a song name.   
-  First time we use Statement . Statement use SQL that Concatenate Strings. An Injection Attack can be done.  
-  Second time we use PrepairedStatement. PrepairedStatement use SQL that contains a placeholder,  (" ?"),  that will be replaced with a literal value. Injection can not be done.  
Use:  
Go Your Own Way  - for a valid song name  
Go Your Own Way" or 1=1 or " – to Inject SQL   

Database Diagram:  
![Database Diagram:](/music diagram.png)  


 [BACK TO START PAGE](https://github.com/FlorescuAndrei/Start.git)

