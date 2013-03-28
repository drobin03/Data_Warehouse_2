#!/usr/local/bin/python

# Warehouse_Consolidate.py
# Chris Alderson 


import MySQLdb
import datetime

def main():
    db = MySQLdb.connect("www.farm-to-fork.ca","manager","waterfall","cis3760") #IP, USERNAME, PASS, DB NAME

    cursor = db.cursor()
    # execute SQL query using execute() method.
    cursor.execute("SELECT VERSION()")

    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()

    print "Database version : %s " % data

    # disconnect from server
    
    sq1 = "SELECT * FROM Internet_Throughput "
    
    try:
        cursor.execute(sq1)
        results = cursor.fetchall()
        for row in results:
            datetime = row[0]
            upload = row[1]
            download = row[2]
            #sex = row[3]
            #income = row[4]
      # Now print fetched result
            print "datetime=%s,upload=%d,download=%d" % \
             (datetime, upload, download )
    except:
        print "Error: unable to fetch data"
    db.close()
if __name__ == "__main__":
    main()
