import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events
                                (
                                    artist varchar,
                                    auth varchar,
                                    firstName varchar,
                                    gender char,
                                    itemInSession int,
                                    lastName varchar,
                                    length float,
                                    level varchar,
                                    location varchar, 
                                    method varchar,
                                    page varchar,
                                    registration bigint,
                                    sessionId int,
                                    song varchar,
                                    status int,
                                    ts bigint,
                                    userAgent varchar,
                                    userId int
                                );
""")
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                (   song_id varchar PRIMARY KEY,
                                    num_songs int,
                                    artist_id varchar,
                                    artist_latitude float,
                                    artist_longitude float,
                                    artist_location varchar,
                                    artist_name varchar,
                                    title varchar,
                                    duration float,
                                    year int
                                );
""")

songplay_table_create = ("""CREATE TABLE songplays 
                            (songplay_id integer IDENTITY(0,1) PRIMARY KEY,
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

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                        (
                            user_id varchar PRIMARY KEY,
                            first_name varchar, 
                            last_name varchar, 
                            gender char, 
                            level varchar
                        );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
                        (
                            song_id varchar PRIMARY KEY, 
                            title varchar,
                            artist_id varchar, 
                            year int, 
                            duration float
                        );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                        (
                            artist_id varchar PRIMARY KEY,
                            name varchar, 
                            location varchar, 
                            latitude float, 
                            longitude float
                        );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                        (
                            start_time timestamp PRIMARY KEY,
                            hour int, 
                            day int, 
                            week int, 
                            month int, 
                            year int,
                            weekday varchar
                        );
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}' 
FORMAT AS JSON {} REGION 'us-west-2';
""").format(config['S3']['log_data'], config['IAM_ROLE']['arn'], config['S3']['log_jsonpath'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}' 
FORMAT AS JSON 'auto' REGION 'us-west-2';
""").format(config['S3']['song_data'], config['IAM_ROLE']['arn'])
# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, 
                            user_id, level, song_id, artist_id, session_id,
                            location, user_agent)
                            SELECT distinct
                            timestamp 'epoch' + (se.ts / 1000) * INTERVAL '1 second',
                            se.userId::INTEGER AS user_id,
                            se.level,
                            s.song_id,
                            s.artist_id,
                            se.sessionId AS session_id,
                            se.location,
                            se.userAgent AS user_agent
                            FROM staging_events se
                            JOIN staging_songs s ON (se.song = s.title and se.length = s.duration 
                            and se.artist = s.artist_name)
                            where se.page='NextSong';
""")

user_table_insert = ("""
                        INSERT INTO users (user_id, first_name, last_name,
                        gender, level)
                        SELECT DISTINCT userId AS user_id,
                        firstName AS first_name,
                        lastName AS last_name,
                        gender,
                        level
                        FROM staging_events
                        WHERE userId IS NOT NULL;""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, 
                        year, duration)
                        SELECT DISTINCT song_id,
                        title,
                        artist_id,
                        year,
                        duration
                        FROM staging_songs;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, 
                        latitude, longitude)
                        SELECT DISTINCT artist_id,
                        artist_name AS name,
                        artist_location AS location,
                        artist_latitude AS latitude,
                        artist_longitude AS longitude
                        FROM staging_songs;""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
SELECT DISTINCT
    TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
    EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(WEEKS FROM start_time) AS week,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    to_char(start_time, 'Day') AS weekday
    FROM staging_events;""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]