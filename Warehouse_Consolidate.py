#!/usr/local/bin/python

# Warehouse_Consolidate.py
# Chris Alderson 

import datetime
import MySQLdb
import time
import string
class Consolidator:

    def main():
        data = open('config.txt', 'r') 
        buff = data.readline().rstrip()
        startDate = data.readline().rstrip()
        buf = data.readline().rstrip()
        endDate = data.readline().rstrip()
        print startDate
        print endDate
        summaryU = 0.0
        summary = 0.0
        table = "Internet_Throughput"
        db = MySQLdb.connect("www.farm-to-fork.ca","manager","waterfall","cis3760") #IP, USERNAME, PASS, DB NAME
        db.autocommit(1)#enable auto commit
        cursor = db.cursor()
        # execute SQL query using execute() method.
        cursor.execute("SELECT VERSION()")
        # Fetch a single row using fetchone() method.
        data = cursor.fetchone()
        print "Database version : %s " % data
        
        sq1 = "SELECT * FROM `Internet_Throughput` WHERE `datetime` BETWEEN '"
        sq1 += startDate
        sq1 +="' AND '"
        sq1 += endDate
        sq1 += "';"
        print sq1
        sq2 = "INSERT INTO `cis3760`.`Consolidation` (`start_date`, `end_date`, `upload`, `download`) VALUES ('"
        sq2 += startDate
        sq2 += "', '"
        sq2 += endDate
        sq2 += "', '"
        try:
            cursor.execute(sq1)
            results = cursor.fetchall()
            for row in results:
                datetime = row[0]
                upload = row[1]
                summaryU = summaryU + row[1]
                download = row[2]
                summary = summary + row[2]
                #sex = row[3]
                #income = row[4]
          # Now print fetched result
                print "datetime=%s,upload=%s,download=%s" % \
                 (datetime, upload, download )
            print summary
            print summaryU
            sq2 += str(summaryU)
            sq2 += "', '"
            sq2 += str(summary)
            sq2 += "');"
                    
            print sq2  
            cursor.execute(sq2)  
        except (MySQLdb.OperationalError, MySQLdb.IntegrityError) as error:
                print error
        # disconnect from server
        db.close()
    if __name__ == "__main__":
        main()
