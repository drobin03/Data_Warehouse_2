from Warehouse_Store import *

if __name__ == "__main__":

	new_source_name = "Big Brother"
	new_schema = "big_brother.xsd"

	observer = DataObserver()
	
	schema_file = open(new_schema, "r")
	#
	# Test adding a new data source
	#
	observer.add_data_source(new_source_name, schema_file.read())
