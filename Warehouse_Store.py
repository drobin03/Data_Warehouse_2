# Warehouse_Store.py

#
# This Module Performs the functions necessary for storing data in the database.
# It parses an XML file, verifies it against a schema, and stores the data.
# 
# Currently, no validation is done of the xml against a schema.

import MySQLdb
from lxml import etree

class Data:
	def __init__(self, name, xml, schema, cursor):
		self.xml = xml
		self.schema = schema
		self.values = []
		self.rows = []
		self.table_name = name
		self.cursor = cursor
		self.fields = []

		#
		# Get the table fields from the database
		#
		query = "SELECT column_name FROM information_schema.columns WHERE table_name='%s'" % (self.table_name)

		try:
			self.cursor.execute(query)
			results = self.cursor.fetchall()
		except (MySQLdb.OperationalError, MySQLdb.IntegrityError) as error:
			print error
			raise
		for source in results:
			self.fields.append("%s" % (source))

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
		parser = etree.XMLParser(remove_blank_text=True)
		try:
			doc = etree.fromstring(self.xml, parser)
		except:
			print "Error reading XML file"
			raise

		current_row = 0
		for element in doc.getiterator():

			if (element.tag == "record"):
				current_row += 1
				self.rows.append([])

			# Create a new DBdata object for the piece of data, and store the (field, value) pair.
			# Only accept the data if the tag matches the field in the database.
			if (element.tag in self.fields):
				self.rows[current_row - 1].append(DBdata(element.tag, element.text))
		
	def store(self, cursor):
		# initialize an insert command for each row
		sql_statements = []
		for row in self.rows :
			statement = "INSERT INTO `%s`.`%s` (" % ("cis3760", self.table_name)
			for field in self.fields :
				statement += "`%s`," % (field)
			statement = statement.rstrip(",")
			statement += ") VALUES ("

			for data in row:
				if (isinstance(data.value, str) ):
					val = "'%s'" % (data.value)
				else :
					val = data.value

				statement += "%s," % (str(val))
			statement = statement.rstrip(",")
			statement += ")"

			sql_statements.append(statement)

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


class DBdata:
	def __init__(self, data_field, data_value):
		self.field = data_field
		self.value = data_value

class Table:
	def __init__(self):
		self.name = ""
		self.fields = {}

	def add_field(self, name, value):
		self.fields[name] = value;


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
		data_to_add = Data(data_name, xml, schema, self.cursor)

		# NOT Validating right now. We need a schema from the data sources before we can do that.
		#data_to_add.validate()
		data_to_add.parse()
		data_to_add.store(self.cursor)

		self.db.commit()
		self.db.close()

	# This function creates a new table in the database.
	# The table name will be set to 'name' and the structure of
	# the table will follow the definitions in the XML schema 'schema'
	def add_data_source(self, name, schema):
		new_table = Table()

		try:
			doc = etree.fromstring(schema)
		except:
			print "Error reading SCHEMA"
			raise

		# Get the Field names and types from the xsd.
		for element in doc.getiterator():
			if (element.get("type") != None):
				# Trim the 'xs:' from the beginning of the types.
				trimmed_type = element.get("type")[3:]
				new_table.add_field(element.get("name"), trimmed_type)

		# Create a new table that follows the data read from the schema.
		sql_statement = "CREATE TABLE `%s` (" % (name) 
		# Add each of the column names, and data types.
		for field in new_table.fields.keys():
			sql_statement += " %s %s," % (field, new_table.fields[field])
		sql_statement = sql_statement.rstrip(",")
		sql_statement += " )"
		
		print sql_statement

		try:
			self.cursor.execute(sql_statement)
			results = self.cursor.fetchall()
		except (MySQLdb.OperationalError, MySQLdb.IntegrityError) as error:
			print error

if __name__ == "__main__": 

	#
	# This function is used to test our store functionality. The data sources
	# module will eventually perform these functions.
	#
	observer = DataObserver()
	
	test_xml_file = open("./test_big_brother_data.xml", "r")
	test_xml = test_xml_file.read()

	schema_file = open("./big_brother.xsd", "r")

	#
	# Test adding a new data source
	#
	#observer.add_data_source("Big Brother", schema_file.read())

	#
	# Test the storage function
	#
	observer.store_data("Internet_Throughput", test_xml)