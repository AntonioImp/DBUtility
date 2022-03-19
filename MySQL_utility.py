import configparser
import pymysql
import datetime
import logging

def retValueIfNotNone(value):
	if value == None:
		return 'null'
	elif value == True:
		value = 1
	elif value == False:
		value = 0
	return "'" + str(value) + "'"

class DBHelper:

	def __init__(self, host, user, password, database, port=3306, logTable=None, logger=None):
		self.host=host
		self.user=user
		self.password=password
		self.database=database
		self.port=int(port)
		self.dataLog={
			"query":None,
			"row_id":None,
			"table_name":None,
			"action":None
		}
		self.logTable=logTable
		self.logger=logger

	def __connect__(self):
		self.con = pymysql.connect( 
			host=self.host,
			user=self.user,
			password=self.password,
			database=self.database,
			port=self.port,
			cursorclass=pymysql.cursors.DictCursor
		)
	
		self.cur = self.con.cursor()

	def __disconnect__(self):
		self.cur.close()
		self.con.close()

	def fetch(self, sql):
		try:
			self.__connect__()
			self.cur.execute(sql)
			result = self.cur.fetchall()
			self.__disconnect__()
			return result
		except Exception as arg:
			print('Fetch exception: ')
			print(arg)
			if self.logger:
				self.logger.error('Fetch exception', exc_info=True)
			return False

	def execute(self, sql):
		try:
			self.__connect__()
			self.cur.execute(sql)
			self.con.commit()

			self.dataLog["query"] = sql
			if self.dataLog["action"] == "INSERT":
				self.dataLog["row_id"] = self.cur.lastrowid
			elif self.dataLog["action"] == "UPDATE":
				self.cur.execute("SELECT @LastUpdateID")
				self.dataLog["row_id"] = self.cur.fetchall()[0]["@LastUpdateID"]
			elif self.dataLog["action"] == "DELETE":
				self.cur.execute("SELECT @LastDeleteID")
				self.dataLog["row_id"] = self.cur.fetchall()[0]["@LastDeleteID"]
			
			self.__disconnect__()
			return True
		except Exception as arg:
			print('Execute exception: ')
			print(arg)
			self.con.rollback()
			if self.logger:
				self.logger.error('Execute exception', exc_info=True)
			return False
			
	def lastInsertId(self):
		return self.cur.lastrowid

	def startTransaction(self):
		try:
			self.__connect__()
			return True
		except Exception as arg:
			print('StartTransaction exception: ')
			print(arg)
			if self.logger:
				self.logger.error('StartTransaction exception', exc_info=True)
			return False
	
	def transactionQuery(self, sql):
		try:
			self.cur.execute(sql)
			return True
		except Exception as arg:
			print('TransactionQuery exception: ')
			print(arg)
			self.con.rollback()
			if self.logger:
				self.logger.error('TransactionQuery exception', exc_info=True)
			return False
	
	def stopTransaction(self):
		try:
			self.con.commit()
			self.__disconnect__()
			return True
		except Exception as arg:
			print('StopTransaction exception: ')
			print(arg)
			self.con.rollback()
			if self.logger:
				self.logger.error('StopTransaction exception', exc_info=True)
			return False
	
	def abortTransaction(self):
		try:
			self.con.rollback()
			self.__disconnect__()
			return True
		except Exception as arg:
			print('AbortTransaction exception: ')
			print(arg)
			self.con.rollback()
			if self.logger:
				self.logger.error('AbortTransaction exception', exc_info=True)
			return False

	#func = select(["name", "ID_M"], "booking", [("CF", "==", CF), ("name", "!=", name)], ["AND", ""])
	def select(self, param, table, filt = [], logic = [""]):
		def queryGenerator():
			query = "SELECT "
			if len(param) == 0:
				query += "*"
			else:
				query += str(param.pop(0))
				for p in param:
					query += ", " + str(p)
			query += " FROM " + table
			if len(filt) != 0:
				query += " WHERE "
				for i, f in enumerate(filt):
					query += str(f[0]) + " " + str(f[1]) + " " + retValueIfNotNone(f[2])
					if logic[i] != "":
						query += " " + str(logic[i]) + " "
			
			print(query)
			if self.logger:
				self.logger.debug(query)

			self.dataLog["action"] = "SELECT"
			self.dataLog["table_name"] = table
			return self.fetch(query)
		return queryGenerator()

	#insert({"name": "CF", "ID_M": "name"}, "booking", False)
	def insert(self, data, table, trans = False):
		key = list(data.keys())
		val = list(data.values())
		def queryGenerator():
			query = "INSERT INTO " + table + "(" + str(key.pop(0))
			for k in key:
				query += ", " + str(k)
			query += ") VALUES (" + retValueIfNotNone(val.pop(0))
			for v in val:
				query += ", " + retValueIfNotNone(v)
			query += ")"
			
			print(query)
			if self.logger:
				self.logger.debug(query)

			self.dataLog["action"] = "INSERT"
			self.dataLog["table_name"] = table
			if trans:
				return self.transactionQuery(query)
			else:
				return self.execute(query)
		return queryGenerator()

	#update({"name": "CF", "ID_M": "name"}, "booking", [("id", "=", "A")], [""], False)
	def update(self, data, table, filt = [], logic = [""], trans = False):
		item = list(data.items())
		def queryGenerator():
			query = "UPDATE " + table + " SET " + str(item[0][0]) + "=" + retValueIfNotNone(item[0][1])
			del item[0]
			for i in item:
				query += ", " + str(i[0]) + "=" + retValueIfNotNone(i[1])
			query += " WHERE "
			for i, f in enumerate(filt):
				query += str(f[0]) + " " + str(f[1]) + " " + retValueIfNotNone(f[2])
				if logic[i] != "":
					query += " " + str(logic[i]) + " "
			query += " AND (SELECT @LastUpdateID := id)"
			
			print(query)
			if self.logger:
				self.logger.debug(query)
			
			self.dataLog["action"] = "UPDATE"
			self.dataLog["table_name"] = table
			
			if trans:
				return self.transactionQuery(query)
			else:
				return self.execute(query)
		return queryGenerator()
	
	#delete("booking", [("id", "=", "A")], [""], False)
	def delete(self, table, filt = [], logic = [""], trans = False):
		def queryGenerator():
			query = "DELETE FROM " + table + " WHERE "
			for i, f in enumerate(filt):
				query += str(f[0]) + " " + str(f[1]) + " '" + str(f[2]) + "'"
				if logic[i] != "":
					query += " " + str(logic[i]) + " "
			query += " AND (SELECT @LastDeleteID := id)"
			
			print(query)
			if self.logger:
				self.logger.debug(query)
			
			self.dataLog["action"] = "DELETE"
			self.dataLog["table_name"] = table
			
			if trans:
				return self.transactionQuery(query)
			else:
				return self.execute(query)
		return queryGenerator()

	def countRow(self, table):
		rows = self.fetch("SELECT * FROM {0}".format(table))
		return rows.__len__()

	def checksumTable(self, table):
		return self.fetch("CHECKSUM TABLE {0}".format(table))[0]

	def logQuery(self):
		if self.logTable == None or self.dataLog["action"] == "SELECT":
			return False
		
		# print("dataLog: ", self.dataLog)
		
		res = self.execute("INSERT INTO sync_log(query, row_id, table_name, action) VALUES ({0}, {1}, {2}, {3})".format(
			repr(self.dataLog["query"]),
			retValueIfNotNone(self.dataLog["row_id"]),
			retValueIfNotNone(self.dataLog["table_name"]),
			retValueIfNotNone(self.dataLog["action"])
		))

		self.dataLog = {
			"query":None,
			"row_id":None,
			"table_name":None,
			"action":None
		}

		if self.logger:
			if res:
				self.logger.debug("Query logged")
			else:
				self.logger.debug("Query not logged")

		return res


if __name__ == "__main__":

	logging.basicConfig(
		filename='sync.log',
		filemode='a',
		format="%(asctime)s - %(name)s : [%(levelname)s] %(message)s",
		level=logging.DEBUG
	)
	logger = logging.getLogger("syncLogger")

	start = datetime.datetime.now()
	print("Start:", start)
	logger.debug("Start: {0}".format(start))

	tableToCheck = ["table1", "table2"]

	check = []
	tableToUpdated = {}

	with open('./file.conf') as f:
		file_content = '[db]\n' + f.read()

	config_parser = configparser.RawConfigParser()
	config_parser.read_string(file_content)

	localDB = DBHelper( 
		host=config_parser['db']['LOCAL_DB_HOST'],
		user=config_parser['db']['LOCAL_DB_USR'],
		password=config_parser['db']['LOCAL_DB_PSW'],
		database=config_parser['db']['LOCAL_DB_NAME'],
		port=config_parser['db']['LOCAL_DB_PORT'],
		logTable="sync_log",
		logger=logger
	)

	remoteDB = DBHelper(
		host=config_parser['db']['REMOTE_DB_HOST'],
		user=config_parser['db']['REMOTE_DB_USR'],
		password=config_parser['db']['REMOTE_DB_PSW'],
		database=config_parser['db']['REMOTE_DB_NAME'],
		port=config_parser['db']['REMOTE_DB_PORT'],
		logTable="sync_log",
		logger=logger
	)

	masterDBForAdd = config_parser['db']['MASTER_DB'] #This key -> 'remote' or 'local'

	for table in tableToCheck:
		localRow = localDB.countRow(table)
		remoteRow = remoteDB.countRow(table)
		localChecksum = localDB.checksumTable(table)["Checksum"]
		remoteChecksum = remoteDB.checksumTable(table)["Checksum"]

		if localRow == remoteRow and localChecksum == remoteChecksum:
			check.append(True)
		else:
			tableToUpdated[table] = { "localRow": localRow, "remoteRow": remoteRow, "localChecksum": localChecksum, "remoteChecksum": remoteChecksum }
	
	if len(check) == len(tableToCheck):
		print("All tables are already updated.")
		logger.debug("All tables are already updated")
	else:
		print("Some tables are not updated.")
		logger.debug("Some tables are not updated")
		
	for table in tableToUpdated:
		if tableToUpdated[table]["localRow"] != tableToUpdated[table]["remoteRow"]:
			remoteRows = remoteDB.select([], table)
			localRows = localDB.select([], table)

			if masterDBForAdd == "local":
				slaveDB = remoteDB
				slaveRows = remoteRows
				masterRows = localRows
			else:
				slaveDB = localDB
				slaveRows = localRows
				masterRows = remoteRows
			
			for i in range(masterRows.__len__()):
				if not masterRows[i] in slaveRows:
					if slaveDB.insert(masterRows[i], table):
						print(slaveDB.logQuery())

			if localDB.countRow(table) == remoteDB.countRow(table):
				print("Rows added in localDB.{0}".format(table))
				logger.debug("Rows added in localDB.{0}".format(table))
			else:
				print("Error in rows alignment (add in localDB.{0})".format(table))
				logger.warning("Error in rows alignment (add in localDB.{0})".format(table))

			tableToUpdated[table]["localChecksum"] = localDB.checksumTable(table)["Checksum"]

		if tableToUpdated[table]["localChecksum"] != tableToUpdated[table]["remoteChecksum"]:
			remoteRows = remoteDB.select([], table)
			localRows = localDB.select([], table)

			for i in range(localRows.__len__()):
				if localRows[i] != remoteRows[i]:
					if localRows[i]["last_update"] > remoteRows[i]["last_update"]:
						slaveDB = remoteDB
						masterDB = localDB
						masterRow = localRows[i]
					else:
						slaveDB = localDB
						masterDB = remoteDB
						masterRow = remoteRows[i]

					if slaveDB.update(masterRow, table, [("id", "=", masterRow["id"])]):
						slaveDB.logQuery()
			
			if localDB.checksumTable(table)["Checksum"] == remoteDB.checksumTable(table)["Checksum"]:
				print("Rows updated in {0}".format(table))
				logger.debug("Rows updated in {0}".format(table))
			else:
				print("Error in rows alignment (update in {0})".format(table))
				logger.warning("Error in rows alignment (update in {0})".format(table))

	
	finish = datetime.datetime.now()
	print("End:", finish)
	print("Time taken:", finish - start)
	logger.debug("End: {0}".format(finish))
	logger.debug("Time taken: {0}".format(finish - start))
