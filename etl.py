import os
import glob
import pandas as pd 
from create_tables import *
from sql_queries import *

def process_info(conn, cur, filepath, process_func):
	"""
		This function returns all the json files with their absolute path
	"""
	file_list = []
	for root, dir, files in os.walk(filepath):
		files = glob.glob(os.path.join(root, '*.json'))
		for file in files:
			file_list.append(os.path.abspath(file))
	print('{} files found in {}'.format(len(file_list), filepath))

	for index, filename in enumerate(file_list, 1):
		process_func(filename, cur)
		conn.commit()
		print('{} file record are inserted in database and file number is {}'.format(filename, index))


def process_song_data(filename, cur):
	df = pd.read_json(filename, lines=True)
	for value in df.values:
		artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year = value
		artist_data = [artist_id, artist_name, artist_location, artist_longitude, artist_latitude]
		cur.execute(artist_table_insert, artist_data)
		song_data = [song_id, title, artist_id, year, duration]
		cur.execute(song_table_insert, song_data)

def process_log_data(filename, cur):
    """Reads user activity log file row by row, filters by NexSong, selects needed fields, transforms them and inserts
    them into time, user and songplay tables.

            Parameters:
                cur (psycopg2.cursor()): Cursor of the sparkifydb database
                filename (str): filename of the file to be analyzed
    """
    df = pd.read_json(filename, lines=True)

    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms') 
    
    time_data = []
    for line in t:
        time_data.append([line, line.hour, line.day, line.week, line.month, line.year, line.day_name()])
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, pd.to_datetime(row.ts, unit='ms'), int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)



if __name__ == "__main__":
	connection = DatabaseConnection()
	conn, cur = connection._connect()
	process_info(conn, cur, filepath='data/song_data', process_func=process_song_data)
	process_info(conn, cur, filepath='data/log_data', process_func=process_log_data)
	conn.close()
