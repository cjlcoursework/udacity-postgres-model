import glob
import os
import uuid

import pandas as pd

from dbase import get_connection, DATABASE_NAME
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process one song_data file by populating postgres dim tables

    Each song_data file contains a single flat json record that populates both dim_song, and dim_artist tables

    Parameters
    ----------
    cur : psycopg2 cursor
    filepath : the full path of a json file containing one and only one json line
    """
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record - todo try the alternate method in the jupyter file
    song_df = df[["song_id", "title", "artist_id", "year", "duration"]]
    song_data = song_df.to_numpy().tolist()

    # song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]])
    v = cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_df = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    artist_data = artist_df.to_numpy().tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process one song events logfile by populating postgres dim and fact tables

    Each log_data file contains multiple json lines that populate the song_play fact table and several dim tables
    Only page=NextSong records are included - all others are filtered

    Parameters
    ----------
    cur : psycopg2 cursor
    filepath : the full path of a json file containing one and only one json line
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    # df.info()
    # v1 = len(df)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']
    # v2 = len(df)
    # df.info()

    # load time data records
    df['start_time'] = df['ts']
    df['start_date'] = pd.to_datetime(df['ts'])
    df['year'] = df['start_date'].dt.year
    df['month'] = df['start_date'].dt.month
    df['week'] = df['start_date'].dt.isocalendar().week
    df['day'] = df['start_date'].dt.day
    df['hour'] = df['start_date'].dt.hour
    df['weekday'] = df['start_date'].dt.dayofweek
    # print(df.head())
    # df.info()
    time_df = df[["start_time", "start_date", "hour", "day", "week", "month", "year", "weekday"]]
    for i, row in time_df.iterrows():
        l = list(row)
        v = cur.execute(time_table_insert, l)

    # load user table - todo get ri of the list() here
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    for i, row in user_df.iterrows():
        l = list(row)
        cur.execute(user_table_insert, l)

    # load song_play fact records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        songid, artistid = None, None
        if results:
            songid, artistid = results

        # insert songplay record
        songplay_data = [str(uuid.uuid4()), row.start_time, row.userId, row.level, songid, artistid, row.sessionId,
                         row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    recursively read all json files from a directory

    Call the provided function (func) to proces each json line

    Parameters
    ----------
    cur : psycopg2 cursor
    conn : psycopg2 connection
    filepath : the full path of a json file containing one and only one json line
    func : the actual logic for loading the data to Postgres
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    """
    entry point for processing of data/lod_data and data/song_data files.
    The filepaths contain json lines files

    """
    conn = get_connection(DATABASE_NAME)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
