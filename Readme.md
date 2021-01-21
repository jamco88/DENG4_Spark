# Data Engineering Nanodegree Project 4: Building a Data Lake on AWS with Apache Spark

## Project Summary

From Udacity's introduction to the project:

Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


In this project I;

* Created a Spark Session on AWS
* Ingested log and song data files
* Processed these files:
    * Adding unique ID's
    * Parsing Timestamps
    * Removing duplicate values
    * Converted the two log files into 4 dimension and 1 fact table
    * Wrote these tables to S3
    
By using the Star Schema (as defined below) this will allow the team at Sparkify to easily drill down by song, artist and user. They will be able to understand how user behaviour changes over time fairly easily - user, song, artists and time (our fact tables) can be investigated with reference to the songplays dimensional table. A big improvement on messy json!
    
## How to use

Simply run etl.py from the python console or terminal

## Files

The project includes two files:

* etl.py reads data from S3, processes that data using Spark, and writes them back to S3
* README.md is the document you are reading now.
* dl.cfg - config file with env vars (aws keys removed for opsec)

## Fact Table

* songplays - records in log data associated with song plays i.e. records with page NextSong (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

## Dimension Tables

* users - users in the app (user_id, first_name, last_name, gender, level)
* songs - songs in music database (song_id, title, artist_id, year, duration)
* artists - artists in music database (artist_id, name, location, lattitude, longitude)
* time - timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)
    
## Discussion

I found the forums invaluable while working on this project. Understanding how to parse out the various time units from the unix epoch timestamp was probably the trickiest part. Some complexity was added by some redundant boilerplate code but I took the mentor's advice and removed it.

