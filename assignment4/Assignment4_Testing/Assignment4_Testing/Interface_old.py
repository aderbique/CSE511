#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    fw = open("./RangeQueryOut.txt", 'w')
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
        for result in cur:
            fw.write("{0},{1},{2},{3}\n".format(part, result[0], result[1], result[2]))
        #results = cur.fetchall()
        #writeToFile('RangeQueryOut.txt', results) 

    # Query Round Robin
    cur.execute("SELECT partitionnum FROM roundrobinratingsmetadata;")
    rr = int(cur.fetchone()[0])
    for part in range(rr):
        rq = "SELECT 'RoundRobinRatingsPart" + str(part) + "' AS PartitionName, UserID, MovieID, Rating " \
           "FROM roundrobinratingspart" + str(part) + " " + \
           "WHERE rating <= " + str(ratingMaxValue) + " AND rating >= " + str(ratingMinValue)
        cur.execute(rq)
        for result in cur:
            fw.write("{0},{1},{2},{3}\n".format(part, result[0], result[1], result[2]))
        #results = cur.fetchall()
        #writeToFile('RangeQueryOut.txt', results)
        
    openconnection.commit()
    fw.close()
    cur.close()     

def PointQuery(ratingsTableName, ratingValue, openconnection):
    fw = open("./PointQueryOut.txt", 'w')
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
        for result in cur:
            fw.write("{0},{1},{2},{3}\n".format(part, result[0], result[1], result[2]))
        #results = cur.fetchall()
        #writeToFile('PointQueryOut.txt', results) 

    # Query Round Robin
    cur.execute("SELECT partitionnum FROM roundrobinratingsmetadata;")
    rr = int(cur.fetchone()[0])
    for part in range(rr):
        pq = "SELECT 'RoundRobinRatingsPart" + str(part) + "' AS PartitionName, UserID, MovieID, Rating " \
             "FROM roundrobinratingspart" + str(part) + " " + \
             "WHERE rating = " + str(ratingValue)
        cur.execute(pq)
        for result in cur:
            fw.write("{0},{1},{2},{3}\n".format(part, result[0], result[1], result[2]))
        #results = cur.fetchall()
        #writeToFile('PointQueryOut.txt', results)
    openconnection.commit()
    fw.close()
    cur.close()    

def writeToFile(filename, rows):
    f = open(filename, 'a')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
