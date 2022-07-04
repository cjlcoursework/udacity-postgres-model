# DROP TABLES
from dbase import SONG_PLAY_TABLE_NAME, USER_TABLE_NAME, SONG_TABLE_NAME, ARTIST_TABLE_NAME, TIME_TABLE_NAME

songplay_table_drop = f"drop table if exists {SONG_PLAY_TABLE_NAME}"
user_table_drop = f"drop table if exists {USER_TABLE_NAME}"
song_table_drop = f"drop table if exists {SONG_TABLE_NAME}"
artist_table_drop = f"drop table if exists {ARTIST_TABLE_NAME}"
time_table_drop = f"drop table if exists {TIME_TABLE_NAME}"

# CREATE TABLES
songplay_table_create = (f"""
create table {SONG_PLAY_TABLE_NAME} (
    songplay_id varchar(60) PRIMARY KEY,
    start_time bigint ,
    user_id text,
    level text,
    song_id text,
    artist_id text,
    session_id int,
    location text,
    user_agent text )
""")

user_table_create = (f"""
create table {USER_TABLE_NAME} (
    user_id varchar(60)  PRIMARY KEY,
    first_name text,
    last_name text,
    gender varchar(2),
    level  text )
""")

song_table_create = (f"""
create table {SONG_TABLE_NAME} (
    song_id  varchar(60)  PRIMARY KEY,
    title text,
    artist_id text,
    year int,
    duration numeric )
;
""")

artist_table_create = (f"""
create table {ARTIST_TABLE_NAME} (
    artist_id  varchar(60)  PRIMARY KEY,
    name text,
    location text,
    latitude numeric,
    longitude numeric)
;
""")

time_table_create = (f"""
create table {TIME_TABLE_NAME} (
    start_time  bigint  PRIMARY KEY, 
    start_date  timestamp, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
)
""")

# INSERT RECORDS

songplay_table_insert = (f"""
insert into {SONG_PLAY_TABLE_NAME} 
(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
values(%s,%s,%s,%s,%s,%s,%s,%s,%s)   on conflict do nothing 

""")


user_table_insert = (f"""
insert into {USER_TABLE_NAME} (user_id, first_name, last_name, gender, level)  
values(%s,%s,%s,%s,%s) on conflict do nothing 
""")

song_table_insert = (f"""
insert into {SONG_TABLE_NAME} (song_id,title,artist_id,year,duration)  
values(%s,%s,%s,%s,%s)   on conflict do nothing 
""")

artist_table_insert = (f"""
insert into {ARTIST_TABLE_NAME} (artist_id,name,location,latitude,longitude)  
values(%s,%s,%s,%s,%s)  on conflict do nothing 
""")


time_table_insert = (f"""
insert into {TIME_TABLE_NAME} (start_time, start_date, hour, day, week, month, year, weekday)  
values(%s,%s,%s,%s,%s,%s,%s,%s) on conflict do nothing 
""")

# FIND SONGS  -- almost no hits on join
song_select = (f"""
select da.artist_id, ds.song_id from {SONG_TABLE_NAME} ds
join {ARTIST_TABLE_NAME} da on ds.artist_id = da.artist_id
where title=%s and da.name=%s and ds.duration=%s
limit 1""")


# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]