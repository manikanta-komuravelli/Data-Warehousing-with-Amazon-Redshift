Project Datawarehousing with Amazon Redshift

Context:

A Music streaming start up Sparkify has a growing user base and song database; their user activity and songs metadata resides in json files in an S3 bucket. My goal for this project is to build an ETL pipeline that extracts the data from S3 bucket and stage them in Redshift and transform them into Fact and dimensional tables for the analytics team at Sparkify to perform their analysis in the pursuit of finding the insights of various songs their users are listening to.

Steps Followed:
    Created an AWS Redshift Cluster and copied the host/end point of the Cluster.
    Created a corresponding IAM role to read the S3 bucket and copied the ARN.
    Updated the config file (dwh.config) with the necessary details as follows:
    Created the necessary tables, read the necessary files, extracted necessary files, transformed necessary data, and loaded them into the created tables.



Project Structure:
    There are two dumps of data in S3 bucket, Log Data and the Song Data:
        Log Data, Song Data.
    Created necessary staging tables to extract the data from the bucket and also necessary analytical tables to transform the extracted data. Below are the tables.
    Staging Tables: 
        Staging_events, staging_songs
    Fact Tables:
		Songplays: Below are the columns used in this table
		songplay_id, start_time, user_id , level, song_id , artist_id, session_id, location , user_agent 
    Dimension Tables:
		Users, songs, artists, time

The code required to create the above mentioned tables was written in sql_queries.py and was executed in create_tables.py; etl.py is used to stage the data into staging tables and load them into the Fact and Analytical tables.  

Steps Followed:
Create Table Schemas
    Created a Redshift cluster and accessed it using the IAM role and added them into the configuration files dwh.cfg
    Designed tables for staging, fact and dimension tables.
        Utilised various distribution techniques/strategies learnt in the Chapter by assigning sort keys and dist keys for better optimization of the results.
    Written SQL CREATE statements for each of these tables in sql_queries.py, and used create_tables.py to connect to the database and create the tables.
    Tested the creation of these tables in the Redshift cluster by opening the query editor and writing a query to get the empty tables.
Build ETL Pipeline
    Implemented the logic in etl.py to load data from S3 buckets to staging tables on Redshift and also to load the data from these staging tables to the analytical tables.
    Tested by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
    Deleted the redshift cluster when finished.

Documented Process in the README.md file.
