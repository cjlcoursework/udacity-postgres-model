Do the following steps in your README.md file.

## The purpose of this database 
To join `event` data (from log_data/) with additional `dimensional` data from songs_data/`
The basic schema comes from the project instructions, and they looked reasonable,and I stayed close to those requirements to avoid confusion.
Some possible variations are:
- I generated my own songplay_id key because there was nothing good in the data that made sense to me 
  - I could have auto-created that key as an integer, but all of the other keys were text
- I created all primary keys as unique
- I made the dim_time foreign key as a UTC bigint for performance 
- I changed the column select code to a common approach: `my_df = df[[col1, col2, col3]]`
- I only found one case where the log data matched the song data - so only one songplay record has good foreign keys
- I stayed with text columns except where the data was already numeric - no big transformations

## How to run the Python scripts
- [x] update the environment variables to point to the correct Postgres installation
- [x] `python3 create_tables.py` - to create the tables  
- [x] `python3 etl.py` - to load the tables from the data/ directory
- [x] `python3 sanity_checks.py` - to validate the tables

## An explanation of the files in the repository
- `README.md`  -- This file 
- `RUBRIC`  -- just a quick checklist 
---
- `data/`  -- data provided by udacity
- `notebooks/`  -- contains completed jupyter notebooks originally provided by udacity
- `outputs/`  -- the results of the sanity (unit) tests
---
- `sql_queries.py`  -- completed create, select and insert statements
- `sanity_tests.py`  -- unittest sanity checks. check `outputs/` for an example
- `dbase.py`  -- a separate file for connections to Postgres
- `dbase.env`  -- I've placed database connection info in environment variables
- `create_tables.py`  -- database creation skeleton code provided
- `etl.py`  -- completed to do the actual etl

## State and justify your database schema design and ETL pipeline.
- The schema is the same one required by the Udacity instructions.  
  - I decided that it would be confusing to stray too far from that design 
  - So I stuck with the schema, code and approach from the project instructions
- The basic fact is that a user listened to a song 
- Information specific to the song, the artist, the listener (user) and the time are obvious dimensions

#### fact
- A `song_plays` represents basic fact that a user listened to a song
- information about the song, artist, user, and time are redundant at this level so we only keep the foreign keys
- 
#### dimensions
- The `song` is an object dimension containing information about each unique song
- The `artist` is an object dimension containing information about each unique artist
- The `time` allows us to easily query by day of week, year, month over month, etc.
- The `user` is a separate object dimension containing information for each unique user

## [Optional]
- [x] Provide example queries and results for song play analysis.(I just provided the 'kitchensink` multi-join with no filters, variations of which could be used for many typical analysis)
- [x] Provide DOCSTRING statement in each function implementation to describe what each function does.