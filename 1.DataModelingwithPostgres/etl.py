import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Extracts information related to a song from a json file, transforms it into a dataframe
    and loads the resulting information into a relational database using sql queries
    
    args:
        - cur : Cursor object that permits modify a database through sql queries.
        - filepath : Path pointing to the json file to be used.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = artist_data = list(df[["artist_id", 
                                         "artist_name", 
                                         "artist_location", 
                                         "artist_latitude", 
                                         "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Extracts information related to user logs from a json file, transforms it into a dataframe
    and loads the resulting information into a relational database using sql queries
    
    args:
        - cur : Cursor object that permits to connect and modify a database
        - filepath : Path pointing to the json file to be used.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    is_next = df['page']=='NextSong'
    df = df[is_next]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime( df['ts'] , unit = 'ms')
    t = df['ts']
    
    # insert time data records
    time_data = list((t.dt.time, 
                      t.dt.hour, t.dt.day, 
                      t.dt.week, t.dt.month, t.dt.year, t.dt.dayofweek))

    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year','weekday')
    time_df = pd.DataFrame(time_data, column_labels).T

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']] 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Helper function that permits to navigate through a folder, extract paths pointing
    to raw json files and finally executes the either process_log_file or process_song_file
    to populate a database
    
    args:
        - cur : Cursor object that permits modify a database through sql queries.
        - conn : Conn object that permits to connect to a database.
        - filepath : Path pointing to the json file to be used.
        - func : Function used to populate the database, can be either process_log_file or process_song_file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """This method executed the whole pipeline to populate the sparkifydb database from raw json files"""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()