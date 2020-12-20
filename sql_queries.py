import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
USER_LOC = config.get('LOC', 'REGION')

#user_loc= 'us-west-2'
# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events(
    artist text, 
    auth text, 
    firstName text, 
    gender text, 
    ItemInSession int, 
    lastName text, 
    length numeric, 
    level text, 
    location text, 
    method text, 
    page text, 
    registration text, 
    sessionId int, 
    song text, 
    status int, 
    ts timestamp, 
    userAgent text, 
    userId int)
""")

staging_songs_table_create = ("""
create table if not exists staging_songs(
    song_id text PRIMARY KEY, 
    artist_id text, 
    artist_latitude numeric, 
    artist_location text, 
    artist_longitude numeric, 
    artist_name text, 
    duration numeric, 
    num_songs int, 
    title text, 
    year int)

""")

songplay_table_create = ("""
create table if not exists songplays(
songplay_id int identity primary key,
start_time timestamp references time(start_time) sortkey,
user_id int not null references users(user_id),
level text not null,
song_id text references songs(song_id),
artist_id text references artists(artist_id) distkey,
session_id int not null,
location text not null,
user_agent text not null
)
""")

user_table_create = ("""
create table if not exists users(
user_id int primary key sortkey,
first_name varchar not null,
last_name varchar not null,
gender text not null,
level text not null
)
""")

song_table_create = ("""
create table if not exists songs(
song_id text primary key sortkey,
title text not null,
artist_id text not null references artists(artist_id),
year int,
duration numeric not null
)
""")

artist_table_create = ("""
create table if not exists artists(
artist_id text primary key distkey,
name text not null,
location text,
latitude numeric,
longitude numeric
)
""")

time_table_create = ("""
create table if not exists time(
start_time timestamp primary key sortkey,
hour int not null,
day int not null,
week int not null,
month int not null,
year int not null,
weekday int not null
)
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {} iam_role {} region {} FORMAT AS JSON {} timeformat 'epochmillisecs';
""").format(LOG_DATA, ARN, USER_LOC, LOG_JSONPATH)

staging_songs_copy = (""" copy staging_songs from {} iam_role {} region {} format as json 'auto';
""").format(SONG_DATA, ARN, USER_LOC)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select distinct se.ts, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
from staging_events se
inner join staging_songs ss
on se.song = ss.title
where se.page = 'NextSong' 
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
select distinct se.userId, se.firstName, se.lastName, se.gender, se.level
from staging_events se
where se.page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
select distinct ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
FROM staging_songs ss
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
select distinct ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
from staging_songs ss
where ss.artist_id IS NOT NULL
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select distinct se.ts, DATE_PART('hour', se.ts) :: int, DATE_PART('day', se.ts) :: int, DATE_PART('week', se.ts) :: int, DATE_PART('month', se.ts) :: int, DATE_PART('year', se.ts) :: int, DATE_PART('dow', se.ts) :: int
from staging_events se
where se.page = 'NextSong'
""")

# QUERY LISTS

#create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
#create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
#drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]