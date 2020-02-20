"# DataConversion" 

1) All the classes and methods have been kept in one file for readability. 
2) The connection strings and users have been replaced with xxx. This is to be changed before running the code.
3) It is highly customized for the existing AccessDB tables and data provided to us. 

4) The logic has been optimized to reduce the time required for porting. 
    a) Try to write to mysql in batches of 5000 records. 
    b) If the batch insert fails, then try to insert these 5000 records one at a time and log the records with exceptions. 
    c) Do this until all the records of a given table have been inserted in mysql. 
