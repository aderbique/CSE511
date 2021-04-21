#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    # Query Range Tables
    cur = openconnection.cursor()
    if os.path.exists('RangeQueryOut.txt'): os.remove('RangeQueryOut.txt')
    with open('RangeQueryOut.txt', 'w') as f:
        f.write('PartitionName, UserID, MovieID, Rating\n')
    cur.execute("SELECT MAX(partitionnum) + 1 FROM rangeratingsmetadata;")
    range_parts = int(cur.fetchone()[0])

    for part in range(range_parts):
        rq =  "SELECT 'RangeRatingsPart" + str(part) + "' AS PartitionName, UserID, MovieID, Rating " \
              "FROM rangeratingspart" + str(part) + " " +\
              "WHERE rating <= " + str(ratingMaxValue) + " AND rating >= " + str(ratingMinValue)

        cur.execute(rq)
        results = cur.fetchall()
        writeToFile('RangeQueryOut.txt', results) 

    # Query Round Robin
    cur.execute("SELECT partitionnum FROM roundrobinratingsmetadata;")
    rr = int(cur.fetchone()[0])
    for part in range(rr):
        rq = "SELECT 'RoundRobinRatingsPart" + str(part) + "' AS PartitionName, UserID, MovieID, Rating " \
           "FROM roundrobinratingspart" + str(part) + " " + \
           "WHERE rating <= " + str(ratingMaxValue) + " AND rating >= " + str(ratingMinValue)
        cur.execute(rq)
        results = cur.fetchall()
        writeToFile('RangeQueryOut.txt', results)

    openconnection.commit()

def PointQuery(ratingsTableName, ratingValue, openconnection):
    # Query Range Tables
    cur = openconnection.cursor()
    if os.path.exists('PointQueryOut.txt'): os.remove('PointQueryOut.txt')
    with open('PointQueryOut.txt', 'w') as f:
        f.write('PartitionName, UserID, MovieID, Rating\n')
    cur.execute("SELECT MAX(partitionnum) + 1 FROM rangeratingsmetadata;")
    point_parts = int(cur.fetchone()[0])

    for part in range(point_parts):
        pq = "SELECT 'RangeRatingsPart" + str(part) + "' AS PartitionName, UserID, MovieID, Rating " \
             "FROM rangeratingspart" + str(part) + " " + \
             "WHERE rating = " + str(ratingValue)    
        cur.execute(pq)
        results = cur.fetchall()
        writeToFile('PointQueryOut.txt', results) 

    # Query Round Robin
    cur.execute("SELECT partitionnum FROM roundrobinratingsmetadata;")
    rr = int(cur.fetchone()[0])
    for part in range(rr):
        pq = "SELECT 'RoundRobinRatingsPart" + str(part) + "' AS PartitionName, UserID, MovieID, Rating " \
             "FROM roundrobinratingspart" + str(part) + " " + \
             "WHERE rating = " + str(ratingValue)
        cur.execute(pq)
        results = cur.fetchall()
        writeToFile('PointQueryOut.txt', results)
    openconnection.commit()

def writeToFile(filename, rows):
    f = open(filename, 'a')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
