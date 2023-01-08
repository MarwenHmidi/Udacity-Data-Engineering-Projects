# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
songplay_table_create = ("""
CREATE TABLE songplays (
songplay_id serial PRIMARY KEY,
start_time timestamp NOT NULL,
user_id int NOT NULL,
level varchar NOT NULL,
song_id varchar,
artist_id varchar,
session_id int NOT NULL,
location varchar NOT NULL,
user_agent varchar NOT NULL,
FOREIGN KEY (start_time) REFERENCES time(start_time),
FOREIGN KEY (user_id) REFERENCES users(user_id),
FOREIGN KEY (song_id) REFERENCES songs(song_id),
FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
unique(start_time,user_id,song_id,artist_id));
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour int NOT NULL,
    day int NOT NULL,
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday varchar NOT NULL);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int,
    duration DOUBLE PRECISION NOT NULL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name varchar NOT NULL,
    longitude double precision,
    latitude double precision,
    location text);
""")


# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (start_time, user_id, song_id, artist_id) DO UPDATE
SET start_time = excluded.start_time, 
    user_id = excluded.user_id,
    level = excluded.level,
    song_id = excluded.song_id, 
    artist_id = excluded.artist_id, 
    session_id = excluded.session_id, 
    location = excluded.location, 
    user_agent = excluded.user_agent;
""")

user_table_insert = ("""
INSERT INTO users (
user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE 
SET user_id = excluded.user_id,
    first_name = excluded.first_name,
    last_name = excluded.last_name,
    gender = excluded.gender,
    level = excluded.level;
""")

song_table_insert = ("""
INSERT INTO songs (
song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)  DO UPDATE
SET song_id = excluded.song_id,
    title = excluded.title,
    artist_id = excluded.artist_id,
    year = excluded.year,
    duration = excluded.duration;
""")

artist_table_insert = ("""
INSERT INTO artists (
artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE
SET artist_id = excluded.artist_id,
    name = excluded.name,
    location = excluded.location, 
    latitude = excluded.latitude, 
    longitude = excluded.longitude;
""")


time_table_insert = ("""
INSERT INTO time (
    start_time, hour, day, week, month, year, weekday
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO UPDATE
SET start_time = excluded.start_time,
    hour = excluded.hour,
    day = excluded.day, 
    week = excluded.week, 
    month = excluded.month,
    year = excluded.year,
    weekday = excluded.year;
""")

# FIND SONGS

# Retrieve song_id and artist_id using a song title, artist_name, and song duration from song and artist tables
song_select = ("""
SELECT song_id, sg.artist_id
FROM songs as sg JOIN artists ats ON sg.artist_id=ats.artist_id
WHERE sg.title=%s AND ats.name=%s AND sg.duration=%s;
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]