# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS  songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id serial PRIMARY KEY, start_time timestamp, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int NOT NULL, location varchar, user_agent varchar);
""")

# The user must have a first name and a last name, hence, I included the NOT NULL constrain
user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar NOT NULL, last_name varchar NOT NULL, gender varchar, level varchar)
""")

# A song must have a title, must be created by an artist and must have a duration, this values cannot be null
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar NOT NULL, artist_id varchar NOT NULL, year int, duration int NOT NULL)
""")

# An artist must have a name
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar NOT NULL, location varchar, lattitude varchar, longitude varchar)
""")

# All the values in the time table must exist
time_table_create = ("""CREATE TABLE IF NOT EXISTS time (id serial PRIMARY KEY, start_time time without time zone NOT NULL, hour int NOT NULL,  day int NOT NULL, week int NOT NULL, month int NOT NULL, year int NOT NULL, weekday int NOT NULL)
""")



# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location , user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (songplay_id) 
DO NOTHING;""")

# It is possible to update de user level i.e. form free to paid in case of conflict
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
""")

# The information related to a song which has already been created cannot change the same applies to artist and time
song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)  ON CONFLICT (song_id) 
DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
VALUES (%s, %s, %s, %s, %s)  ON CONFLICT (artist_id) 
DO NOTHING;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)  ON CONFLICT (id) 
DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id FROM songs s JOIN artists a
ON a.artist_id = s.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]