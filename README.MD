# Logs Analysis Project
Project of the Udacity Fullstack Developer Nano Degree Program.
To build an internal reporting tool that will use information from the database to discover what kind of articles the site's readers like
on a newspaper site.

## Assumptions
User knows and can execute Python 2, SQL, Postgresql and database software (provided by a Linux virtual machine) well.


### Design of this Project
The database contains newspaper articles, as well as the web server log for the site. The log has a database row for each time a reader loaded a web page.
The reporting tool is a Python program using the psycopg2 module to connect to the database and prints out reports (in plain text) based on the data in the database. 

The program written in this project runs from the command line. It won't take any input from the user. 
Instead, it will connect to that database, use SQL queries to analyze the log data, and print out the answers to the following questions:

1. What are the most popular three articles of all time?

2. Who are the most popular article authors of all time?

3. On which days did more than 1% of requests lead to errors?

###Project files are:
1. newsdata.sql
	* link to database for the Project in SQL format:
	https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip
2. dbCodeLogsAnalysisProject.py
	* the code written in Python to connect to the database and execute SQL queries and print out the results.
3. README.MD
	* The README file includes instructions for how to run the program, as well as a description of the program's design..

### How to run the Project
1. Download the project file.
2. Unzip the project file to folder and put it in the directory which is shared with your virtual machine.
3. From the terminal, to load the data, cd into the virtual machine directory and use the command *psql -d news -f newsdata.sql*.
4. After database is connected, execute command:
	* python dbCodeLogsAnalysisProject.py

#####Acknowledgement
The newsdata.sql is provided by Udacity

#####License
Free for distribution

#####Disclaimer
No liability will be accepted by the author.
