import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN= config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay;"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS song;"
artist_table_drop         = "DROP TABLE IF EXISTS artist;"
time_table_drop           = "DROP TABLE IF EXISTS time;"


# CREATE TABLES
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
    num_songs           INTEGER,
    artist_id           VARCHAR NOT NULL,
    artist_latitude     NUMERIC NULL,
    artist_longitude    NUMERIC NULL,
    artist_location     VARCHAR NULL,
    artist_name         VARCHAR NOT NULL,
    song_id             VARCHAR NOT NULL,
    title               VARCHAR NOT NULL,
    duration            NUMERIC NOT NULL,
    year                INTEGER NOT NULL
);
""")

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist          VARCHAR NULL,
    auth            VARCHAR NOT NULL,
    firstName       VARCHAR NULL,
    gender          VARCHAR NULL,
    itemInSession   INTEGER NOT NULL,
    lastName        VARCHAR NULL,
    length          NUMERIC NULL,
    level           VARCHAR NOT NULL,
    location        VARCHAR NULL,
    method          VARCHAR NOT NULL,
    page            VARCHAR NOT NULL,
    registration    NUMERIC NULL,
    sessionId       INTEGER NOT NULL,
    song            VARCHAR NULL,
    status          INTEGER NOT NULL,
    ts              BIGINT NOT NULL,
    userAgent       VARCHAR NULL,
    userId          VARCHAR NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist
(
    artist_id  VARCHAR,
    name       VARCHAR    NOT NULL sortkey,
    location   VARCHAR    NULL, 
    latitude   NUMERIC    NULL, 
    longitude  NUMERIC    NULL,
    CONSTRAINT ARTIST_PK PRIMARY KEY (artist_id)
)diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song
(
    song_id    VARCHAR             distkey,
    title      VARCHAR   NOT NULL  sortkey,
    artist_id  VARCHAR   NOT NULL,
    year       INT       NOT NULL,
    duration   NUMERIC   NOT NULL,
    CONSTRAINT SONG_PK PRIMARY KEY (song_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id VARCHAR, 
    first_name VARCHAR NOT NULL sortkey, 
    last_name VARCHAR  NOT NULL, 
    gender VARCHAR     NOT NULL, 
    level VARCHAR      NOT NULL,
    CONSTRAINT USER_PK PRIMARY KEY (user_id)
)diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time NUMERIC               sortkey,
    hour       INT         NOT NULL,
    day        INT         NOT NULL,
    week       INT         NOT NULL,
    month      INT         NOT NULL,
    year       INT         NOT NULL,
    weekday    VARCHAR     NOT NULL,
    CONSTRAINT TIME_PK PRIMARY KEY (start_time)
)diststyle all;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
(
    songplay_id  BIGINT IDENTITY(0,1),
    start_time NUMERIC  NOT NULL    sortkey,
    user_id VARCHAR     NOT NULL,
    level VARCHAR       NOT NULL,
    song_id VARCHAR     NULL        distkey,
    artist_id VARCHAR   NULL,
    session_id INT      NOT NULL,
    location VARCHAR    NOT NULL,
    user_agent VARCHAR  NOT NULL,
    CONSTRAINT songplays_PK PRIMARY KEY(songplay_id)
);
""")

# STAGING TABLES

staging_songs_copy = ("""
copy staging_songs from {} 
iam_role {}
format as json 'auto';
""").format(SONG_DATA, ARN)

staging_events_copy = ("""
copy staging_events from {} 
iam_role {}
json {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)



# FINAL TABLES

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, latitude, longitude) 
select artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from
(
  select artist_id, artist_name, artist_location, artist_latitude, artist_longitude, 
  row_number() over(partition by  artist_id order by year desc ) rn 
  from staging_songs
)
where rn =1;
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, duration, year, artist_id) 
select DISTINCT song_id, title, duration, year, artist_id from staging_songs;
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
select userid, firstName, lastName, gender, level
from
    (
        select userid, firstName, lastName, gender, level, row_number() over(partition by  userid order by ts desc ) rn
        from staging_events 
        where page = 'NextSong'
    )
where rn =1;
""")

time_table_insert = ("""
INSERT INTO time(start_time, day, month, year, week,  weekday, hour) 
select DISTINCT
    ts, 
    extract(day from real_datetime) as d, extract(month from real_datetime) as m, extract(year from real_datetime) as y,
    extract(week from real_datetime) as week,
    extract(weekday from real_datetime) as weekday,
    extract(hour from real_datetime) as h
from
    (
        select ts, TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' as real_datetime
        from staging_events 
        where page = 'NextSong'
    );
""")



songplay_table_insert = ("""
insert into songplay
(
    SESSION_ID ,
    ARTIST_ID ,
    SONG_ID ,
    USER_ID ,
    LEVEL ,
    LOCATION ,
    USER_AGENT ,
    start_time 
)
SELECT distinct
    a.sessionId,
    b.artist_id ,
    b.song_id,
    a.userId,
    a.level,
    a.location,
    a.userAgent,
    a.ts
FROM  
     staging_events a INNER JOIN staging_songs b on a.song = b.title and a.length = b.duration and a.artist = b.artist_name
     where page = 'NextSong'; 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
