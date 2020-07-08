import psycopg2
from sql_queries import create_table_queries, drop_table_queries

class DatabaseConnection:
	"""
		This class is used to connect with postgress sparkifydb, create, drop and modify table
	"""
	def __init__(self, host='127.0.0.1', dbname='sparkifydb',user='postgres',password='postgres'):
		self.host = host
		self.dbname = dbname
		self.user = user
		self.password = password

	def _connect(self):
		try:
			conn = psycopg2.connect("host={host} dbname={dbname} user={user} password={password}".format(host=self.host, dbname='student', user=self.user, password=self.password))
			conn.set_session(autocommit=True)
			cur = conn.cursor()
			cur.execute("DROP DATABASE IF EXISTS sparkifydb")
			cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
			conn.close()
		except psycopg2.Error as e:
			print("""Error: could not make connection to the {} postgres database""".format('student'))
			print(e)

		try:
			conn = psycopg2.connect("host={host} dbname={dbname} user={user} password={password}".format(host=self.host, dbname=self.dbname, user=self.user, password=self.password))
			cur = conn.cursor()
		except psycopg2.Error as e:
			print("""Error: could not make connection to the {dbname} postgres database""".format(dbname=self.dbname))

		return conn, cur

	def _drop_tables(self,conn,cur):
		for query in drop_table_queries:
			cur.execute(query)
			conn.commit()

	def _create_tables(self,conn, cur):
		for query in create_table_queries:
			cur.execute(query)
			conn.commit()

if __name__ == "__main__":
	connection = DatabaseConnection()
	conn, cur = connection._connect()
	connection._drop_tables(conn, cur)
	connection._create_tables(conn, cur)
	conn.close()

