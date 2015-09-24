#!/usr/bin/env python


# Author                :       Abhishek S. Okheda <abhi.emailto@gmail.com>
# Date                  :       Thu Sep 24 22:57:39 NPT 2015
# Purpose               :       Mass Remove Measurements using Regex Match

import re
from influxdb import InfluxDBClient
import sys
import getopt



# Set Default Values
host='localhost'
port=8086
user='admin'
password='admin'
database='empty'
regexAbsent = True

def usage():
        # Print Usage Help
	print ""
        print "Mass Remove Measurements using Regex Match"
        print "Usage: %s [-h|--help] [-H|--host] [-P|--port] [-u|--user] [-d|--database] -r|--regex" % sys.argv[0]

# Parse Arguments
try:
        opts, args = getopt.getopt(sys.argv[1:], "hH:P:u:p:d:+r:", ["help", "host", "port", "user", "password", "database", "regex"])
except getopt.GetoptError as err:
        print str(err)
        sys.exit(1)


for option, value in opts:
        if option in ("-h", "--help"):
                usage()
                sys.exit(0)
        elif option in ("-H", "--host"):
                host=value
        elif option in ("-P", "--port"):
                port=value
        elif option in ("-u", "--user"):
                user=value
        elif option in ("-p", "--password"):
                password=value
        elif option in ("-d", "--database"):
                database=value
        elif option in ("-r", "--regex"):
                if value != "":
                        regexAbsent = False
                regex=value

if regexAbsent:
        usage()
        sys.exit(1)



def dropMeasurements(dropList):
        # Drop the measurements
        for item in dropList:
                cmd='DROP MEASUREMENT "%s"' % item
                print cmd
                print client.query(cmd)
                print "---"


# Connect influx
client = InfluxDBClient(host, port, user, password, database)
if client:
        # Get All measurements list
        result = []
        try:
                result = client.query('SHOW MEASUREMENTS')
        except Exception as e:
                print str(e)
                print ""
                print "Connecting to InfluxDB FAILED. Exiting."
                sys.exit(10)

        measurements = result.raw['series'][0]['values']
        measurementsList = [item for sublist in measurements for item in sublist]


        # Prepare regex for deleting
        dropRegex = re.compile(regex)
        dropList = (filter (lambda measurement: dropRegex.match(measurement), measurementsList))

        # Confirmation
        print ""
        if len(dropList) == 0:
                print "NO measurements found matching the given regex"
                sys.exit(0)
        else:
                print "%d MEASUREMENTS found" % len(dropList)
                print ""
                confirmation = "n"

                print "Do you want to: "
                print "\tLIST (l)"
                print "\tDROP (D)"
                print "\tCANCEL (c)"
                command = raw_input("[c] ? ")
                if command == 'D':
                        confirmation = raw_input("Are you sure? (Y/n)? [n] ")
                        if confirmation == 'Y':
                                print ""
                                print "Dropping MEASUREMENTS"
                                #dropMeasurements(dropList)
                elif command == 'l':
                                print ""
                                print dropList
                                print ""
                                confirmation = raw_input("Are you sure to DROP these MEASUREMENTS? (Y/n) [n]? ")
                                if confirmation == 'Y':
                                        print ""
                                        print "Dropping MEASUREMENTS"
                                        #dropMeasurements(dropList)
sys.exit(1)

