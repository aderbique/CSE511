#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()
    if os.path.exists('RangeQueryOut.txt'): os.remove('RangeQueryOut.txt')
    with open('RangeQueryOut.txt', 'w') as f: f.write('PartitionName, UserID, MovieID, Rating\n')
    cur.execute("SELECT MAX(partitionnum) + 1 FROM rangeratingsmetadata;")
    range_len = int(cur.fetchone()[0])
    

def PointQuery(ratingsTableName, ratingValue, openconnection):
    pass


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
