Part 1:

To compile the java file: javac SalaryStdDev.java
To run the java class: java -cp <path>:. SalaryStdDev sample employee <username> <password>



Part 2: 

Drop existing procedure if needed
To create procedure: db2 -td@ -f stddev.sql
To call the procedure: db2 call "find_stddev(?)"

Notes: 
- The name of my procedure is "find_stddev", it returns a variable named "stddev" of typed double. 
- The output of the procedure is in scientific notation form. 
SELECT * FROM syscat.procedures                  