from Warehouse_Store import *

if __name__ == "__main__":

	xml_name = "Internet_Throughput"
	xml_to_add = "test_big_brother_data.xml"
	#
	# This function is used to test our store functionality. The data sources
	# module will eventually perform these functions.
	#
	observer = DataObserver()
	
	test_xml_file = open(xml_to_add, "r")
	test_xml = test_xml_file.read()

	#
	# Test the storage function
	#
	observer.store_data(xml_name, test_xml)
