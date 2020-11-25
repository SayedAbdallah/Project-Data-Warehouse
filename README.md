### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, 
as well as a directory with JSON metadata on the songs in their app.


### Project Description
In this project, we will do the following
1- create DW Star Schema in AWS Redshift in addition to two staging tables.
2- build an ETL pipeline to copy data from S3 buckets to AWS Redshift staging tables
3- write SQL Queries to populate star schema from staging tables

### Steps to run the project
1- run RUN_Project.ipynb file 
this file do the following
   1- run create_tables.py file to drop all tables if they exists then create them again
   2- run etl.py file to start ETL process to copy data from S3 to staging tables then insert data into final tables 
       from staging tables

