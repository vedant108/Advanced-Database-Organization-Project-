Assignment 03
ADVANCED DATABASE ORGANIZATION
Team Members -:
Vedant Suram
Siddharth Mishra
Kanyakumari Kashyap

RECORD MANAGER
The main aim of this assignment is to implement the record manager. The purpose of the record manager is to handle the tables present in the database. It handles the database tables using fixed schemas. The record manager scans the records present in the database table along with performing other functions such as – inserting records, deleting records, updating records in the table. The record manager scans the records with the help of a search condition. The search condition checks the records from the table and returns the respective records that matches with the provided condition.

FUNCTIONS
The purpose of these functionsmethods is to initialize, shutdown the record manager, tables related functions such as – creating, opening, closing and deleting tables and record related functions – inserting a record, deleting a record, updating records, get the records, free the records. Along with these, there are other few functions – start scanning, close scanning, create schema, free schema.
1. initRecordManager This function helps to initialize the record manager. It returns RC_OK once the records get initialized.
2. shutdownRecordManager The shutdownRecordManager function helps to shutdown the record manager and returns RC_OK once the record manager shuts down.

TABLE FUNCTIONS
3. createTable This function helps to create a table based on the ‘name’ and ‘schema’ and it also creates a page file.
4. openTable The openTable function helps to open a table based on ‘name’ once the buffer pool is initialized.
5. closeTable This function helps to close the table once the buffer pool is shut down.
This is done by using the shutdownBufferPool function. Once the shutdownBufferPool function is called, the buffer pool is made free.
6. deleteTable The deleteTable deletes the table in the database and destroys the page file.
7. getNumTuples The getNumTuples function when is called in the record manager program, it returns the number of records which is tuple in the database table.
RECORD FUNCTIONS
The record functions helps to retrieve the record, insert a new record, delete or update already existing record.
8. insertRecord The insertRecord function helps to insert a new record in the database table.
9. deleteRecord This function deletes a record from the table. It has a record ID and it saves the data of the page back to the disk .
10. updateRecord This function helps to update the record in the database table with a new record. The data is placed to a new record once it is copied.
11. getRecord This function consists of a record ID and record pointer. The record pointer helps to find the record size from the schema.

SCAN FUNCTIONS
12. startScan This method helps to scan the data in the memory.
13. next This helps to scan the next record. The records will be scanned till it finds a matching tuple in the data and if it doesn’t find any matching tuples then it fails in execution thus returning an error message in the code.
14. closeScan Once all the scanning operation is completed, it closes through this function.

SCHEMA FUNCTIONS
The schema functions helps to execute the schemas by executing a newly created schema and to get the size of the record. It also free up the space that is consumed by the schema.
15. createSchema This function helps to create a new schema in the memory.
16. getRecordSize The getRecordSize function helps to find the size of a record in the schema.
17. freeSchema The space in the memory occupied by a schema has to be made free with the help of freeSchema function.

ATTRIBUTE FUNCTIONS
There are two types of attribute functions we will use in our record manager. The get attribute and the set attribute.
18. getAttr This function helps to get the attribute from the record in the given schema.
19. setAttr This function helps to set the attribute in the given schema.
20. createRecord This function helps to create a record in the schema. Once the record is created, we obtain the record size and returns RC_OK once it performs the operations successfully.
21. freeRecord This function helps to free the space allocated by the record in the memory.

## How to run the Script ##
For compilation $ make
For Running $ make run
To revert
$ make clean