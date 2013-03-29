# Warehouse_Store.py

import MySQLdb
from lxml import etree

class Data:
	def __init__(self, xml, schema):
		self.xml = xml
		self.schema = schema
		self.values = []

	def validate(self):
		"""
		Insert validation code here, once we get a schema from the data sources team.
		For now, validate by checking that the table name given by the data sources 
		matches a table that exists in our database.
		"""
		if (table_exists(self.xml_name)):
			return true
		else:
			return false 

	def parse(self):
		# for field in xml:
		#     Create new DBdata for (field, value) pair.
		doc = etree.parse(self.xml)


		self.values.append(DBdata("datetime", "'2013-03-28 23:59:59'"))
		self.values.append(DBdata("upload", 2.2))
		self.values.append(DBdata("download", 2.5))
		
	def store(self, cursor):
		# initialize an insert command
		num_fields = 0
		date_time_list = []
		upload_list = []
		download_list = []

		# Sort the data that was parsed into fields and rows to put into the table.
		for data in self.values :
			num_fields += 1
			if data.field == "datetime" :
				date_time_list.append(data.value)
			elif data.field == "upload" :
				upload_list.append(data.value)
			elif data.field == "download" :
				download_list.append(data.value)
			else :
				num_fields -= 1
				print "Invalid field %s" % (data.field)

		# Each row has 3 fields
		num_rows = num_fields / 3
		sql_statements = []
		for i in range(0, num_rows) :
			sql_statements.append("INSERT INTO `cis3760`.`Internet_Throughput` (`datetime`, `upload`, `download`) VALUES (%s, %f, %f)" % (date_time_list[i], upload_list[i], download_list[i]))
		for statement in sql_statements :
			print statement
			try:
				cursor.execute(statement)
				results = cursor.fetchall()
			except (MySQLdb.OperationalError, MySQLdb.IntegrityError) as error:
				print error
		print "still running"


class DBdata:
	def __init__(self, data_field, data_value):
		self.field = data_field
		self.value = data_value

class DataObserver:
	"""
	Class called by the data sources team.
	They will call the "store_data" function
	which stores the xml string in the warehouse.
	"""
	def __init__(self):
		# Initialize a connection with the Database here.
		# Should have a handle that can be called.

		self.db = MySQLdb.connect("www.farm-to-fork.ca","manager","waterfall","cis3760")

		self.cursor = self.db.cursor()

		# sql_command = "SELECT * FROM `Internet_Throughput`"
		# self.cursor.execute(sql_command)
		# results = self.cursor.fetchall()
		# print results
	def lookup_schema(self, name):
		# SQL query here for the schema related to the table name.
		schema = []
		return schema

	def store_data(self, data_name, xml):
		schema = self.lookup_schema(data_name)
		data_to_add = Data(xml, schema)

		table = "Internet_Throughput"

		if (table == None):
			# This table doesn't exist in the warehouse, return an error
			return False

		#data_to_add.validate()
		data_to_add.parse()
		data_to_add.store(self.cursor)

		self.db.commit()
		self.db.close()

if __name__ == "__main__": 

	observer = DataObserver()
	observer.store_data("Internet_Throughput", "xml")