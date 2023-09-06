# Database Implementation Using FoundationDB
Database implementation using the FoundationDB API. There are four tests made to ensure functionality. To run type:
```
make test<number>
```
Note: FoundationDB will need to be installed 


## Functionality

Stores data as a key-value pair in the following manner: <br>
* Creates a directory for each data table
* A subdirectory is created for each unique attribute 
* Data is stored as a record and users can either specify whether they want their data to be stored in a non clustered hash index or a non clustered B+ tree index
* Users can create a cursor object that can scan through all data entries that meet the search criteria
* Implemention supports relational algebraic operators such as join, project, and select
