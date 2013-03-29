# Warehouse_Store.py

#
# This Module Performs the functions necessary for storing data in the database.
# It parses an XML file, verifies it against a schema, and stores the data.
# 
# Currently, no validation is done of the xml against a schema.

import MySQLdb
from lxml import etree

class Data:
	def __init__(self, xml, schema):
		self.xml = xml
		self.schema = schema
		self.values = []

	def validate(self):
		#
		# Insert validation code here, once we get a schema from the data sources team.
		# For now, validate by checking that the table name given by the data sources 
		# matches a table that exists in our database.
		#
		if (table_exists(self.xml_name)):
			return true
		else:
			return false 

	def parse(self):
		print self.xml
		try:
			doc = etree.fromstring(self.xml)
		except:
			print "Error reading XML file"
			raise

		for element in doc.getiterator():
			# Create a new DBdata object for the piece of data, and store the (field, value) pair.
			if (element.text != None):
				self.values.append(DBdata(element.tag, element.text))
		
	def store(self, cursor):
		# initialize an insert command
		num_fields = 0
		date_time_list = []
		upload_list = []
		download_list = []

		# Sort the data that was parsed into fields and rows to put into the table.
		for data in self.values :
			num_fields += 1
			if data.field == "time" :
				date_time_list.append("'%s'" % (data.value))
			elif data.field == "upload" :
				upload_list.append(float(data.value))
			elif data.field == "download" :
				download_list.append(float(data.value))
			else :
				num_fields -= 1
				print "Invalid field %s" % (data.field)

		#
		# Format the statements to store the data in the database
		#

		# Each row has 3 fields
		num_rows = num_fields / 3
		sql_statements = []
		for i in range(0, num_rows) :
			sql_statements.append("INSERT INTO `cis3760`.`Internet_Throughput` (`datetime`, `upload`, `download`) VALUES (%s, %f, %f)" % (date_time_list[i], upload_list[i], download_list[i]))
		

		#
		# Store the data
		#
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
	#
	# Class called by the data sources team.
	# They will call the "store_data" function
	# which stores the xml string in the warehouse.
	#
	def __init__(self):
		# Initialize a connection with the Database.
		self.db = MySQLdb.connect("www.farm-to-fork.ca","manager","waterfall","cis3760")
		self.cursor = self.db.cursor()

		# PRINT the contents of the table
		# sql_command = "SELECT * FROM `Internet_Throughput`"
		# self.cursor.execute(sql_command)
		# results = self.cursor.fetchall()
		# print results

	# This function doesnt do anything right now
	# If we implement a table of schemas in the warehouse
	# This function will look up the schema for the specified table.
	def lookup_schema(self, name):
		# SQL query here for the schema related to the table name.
		schema = []
		return schema

	def store_data(self, data_name, xml):
		schema = self.lookup_schema(data_name)
		data_to_add = Data(xml, schema)

		# NOT Validating right now. We need a schema from the data sources before we can do that.
		#data_to_add.validate()
		data_to_add.parse()
		data_to_add.store(self.cursor)

		self.db.commit()
		self.db.close()

if __name__ == "__main__": 

	#
	# This function is used to test our store functionality. The data sources
	# module will eventually perform these functions.
	#
	test_xml = "<xml><record><upload>3.5</upload><download>3.4</download><time>2010-01-23 23:52:59</time></record><record><upload>2.4</upload><download>2.5</download><time>2011-02-28 23:00:04</time></record></xml>"

	observer = DataObserver()
	observer.store_data("Internet_Throughput", test_xml)