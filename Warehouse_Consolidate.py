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
    db.close()
    #sq1 = "SELECT * FROM cis3760"
    
    #try:
#        cursor.execute(sq1)
#        results = cursor.fetchall()
#        for row in results:
#            fname = row[0]
#            lname = row[1]
 #           #age = row[2]
            #sex = row[3]
            #income = row[4]
      # Now print fetched result
 #       print "fname=%s,lname=%s,age=%d,sex=%s,income=%d" % \
 #            (fname, lname )
 #   except:
 #       print "Error: unable to fetch data"
if __name__ == "__main__":
    main()
