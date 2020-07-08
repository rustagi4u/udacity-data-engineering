import os
import glob
import json
import csv

def file_path(filepath):
	file_list = []
	for root, dir, files in os.walk(filepath):
		files = glob.glob(os.path.join(root, '*.csv'))
		for file in files:
			file_list.append(os.path.abspath(file))
	print('{} files found in {}'.format(len(file_list), filepath))
	return file_list

def readWriteCsv(file_list):
	full_data_csv = []
	for file in file_list:
		with open(file, 'r', encoding = 'utf8', newline='') as csvfile:
			csvreader = csv.reader(csvfile) 
			next(csvreader)
			for line in csvreader:
				full_data_csv.append(line)
	csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)			
	with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
		writer = csv.writer(f, dialect='myDialect')
		writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
		for row in full_data_csv:
			if (row[0] == ''):
				continue
			writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

	with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
		print(sum(1 for line in f))

def csvToCasandra():
	from cassandra.cluster import Cluster
	try:
		cluster = Cluster(['127.0.0.1'])
		session = cluster.connect()
		
	except Exception as e:
		print(e)

	session.execute("""CREATE KEYSPACE IF NOT EXISTS sparkify
	WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")
	session.set_keyspace('sparkify')
	session.execute("""DROP TABLE app_history""")
	session.execute("""DROP TABLE user_songs""")
	session.execute("""DROP TABLE session_songs""")
	session.execute("""CREATE TABLE IF NOT EXISTS session_songs(sessionId int, itemInSession int, artist text, song_title text, song_length float,PRIMARY KEY(sessionId, itemInSession))""")

	file = 'event_datafile_new.csv'

	with open(file, encoding = 'utf8') as f:
		csvreader = csv.reader(f)
		next(csvreader) # skip header
		for line in csvreader:
			query = "INSERT INTO session_songs (sessionId, itemInSession, artist, song_title, song_length)"
			query = query + " VALUES (%s, %s, %s, %s, %s)"
			artist_name, user_name, gender, itemInSession, user_last_name, length, level, location, sessionId, song, userId = line
			session.execute(query, (int(sessionId), int(itemInSession), artist_name, song, float(length)))

	rows = session.execute("""SELECT artist, song_title, song_length FROM session_songs WHERE sessionId = 338 AND itemInSession = 4""")
	for row in rows:
		print(row.artist, row.song_title, row.song_length)

	session.execute("""CREATE TABLE IF NOT EXISTS user_songs (userId int, sessionId int, artist text, song text, firstName text, lastName text, itemInSession int, PRIMARY KEY((userId, sessionId), itemInSession))""")
	with open(file, encoding = 'utf8') as f:
		csvreader = csv.reader(f)
		next(csvreader) # skip header
		for line in csvreader:
			query = "INSERT INTO user_songs (userId, sessionId, artist, song, firstName, lastName, itemInSession)"
			query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
			artist, firstName, gender, itemInSession, lastName, length, level, location, sessionId, song, userId = line
			session.execute(query, (int(userId), int(sessionId), artist, song, firstName, lastName, int(itemInSession)))

	rows = session.execute("""SELECT itemInSession, artist, song, firstName, lastName FROM user_songs WHERE userId = 10 AND sessionId = 182""")
	for row in rows:
		print(row.iteminsession, row.artist, row.song, row.firstname, row.lastname)

	session.execute("""CREATE TABLE IF NOT EXISTS app_history (song text, firstName text, lastName text, userId int, PRIMARY KEY(song, userId))""")
	with open(file, encoding = 'utf8') as f:
		csvreader = csv.reader(f)
		next(csvreader) # skip header
		for line in csvreader:
			query = "INSERT INTO app_history (song, firstName, lastName, userId)"
			query = query + " VALUES (%s, %s, %s, %s)"
			artist, firstName, gender, itemInSession, lastName, length, level, location, sessionId, song, userId = line
			session.execute(query, (song, firstName, lastName, int(userId)))

	rows = session.execute("""SELECT firstName, lastName FROM app_history WHERE song = 'All Hands Against His Own'""")
	for row in rows:
		print(row.firstname, row.lastname)

	session.execute("""DROP TABLE app_history""")
	session.execute("""DROP TABLE user_songs""")
	session.execute("""DROP TABLE session_songs""")
	session.shutdown()
	cluster.shutdown()

if __name__=="__main__":
	file_path_list = file_path(filepath='event_data')
	readWriteCsv(file_path_list)
	csvToCasandra()