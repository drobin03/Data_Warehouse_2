#!/usr/local/bin/python

# Warehouse_Consolidate.py
# Chris Alderson 

import datetime
import MySQLdb
import time

class Consolidator():
    startDate = datetime.datetime(2013,03,28,10,05,00)
    endDate = datetime.datetime(2013,03,28,10,05,00)

    def main():
        summary = 0.0
        table = "Internet_Throughput"
        db = MySQLdb.connect("www.farm-to-fork.ca","manager","waterfall","cis3760") #IP, USERNAME, PASS, DB NAME
        
        cursor = db.cursor()
        # execute SQL query using execute() method.
        cursor.execute("SELECT VERSION()")

        # Fetch a single row using fetchone() method.
        data = cursor.fetchone()

        print "Database version : %s " % data

        # disconnect from server
        
        sq1 = "SELECT * FROM `Internet_Throughput` WHERE `datetime` BETWEEN '2009-03-28 00:00:00' AND '2013-03-28 23:59:59'"
        try:
            cursor.execute(sq1)
            results = cursor.fetchall()
            for row in results:
                datetime = row[0]
                upload = row[1]
                download = row[2]
                summary = summary + row[2]
                #sex = row[3]
                #income = row[4]
          # Now print fetched result
                print "datetime=%s,upload=%s,download=%s" % \
                 (datetime, upload, download )
            print summary
        except:
            print "Error: unable to fetch data"
        db.close()
    if __name__ == "__main__":
        main()
