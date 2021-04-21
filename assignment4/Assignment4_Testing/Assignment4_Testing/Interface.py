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
    cur = openconnection.cursor()

    cur.execute("select table_name from information_schema.tables where table_name like 'roundrobinratingspart%'")
    l = []
    for row in cur:
        l.append(row[0])

    for tablename in l:
        cur.execute("select * from {0} where rating >= {1} and rating <= {2}".format(tablename, ratingMinValue, ratingMaxValue))
        for result in cur:
            fw.write("{0},{1},{2},{3}\n".format(tablename, result[0], result[1], result[2]))

    cur.execute("select * from rangeratingsmetadata")
    l = []
    for row in cur:
        l.append(row)

    for row in l:
        if float(row[1]) > ratingMaxValue or float(row[2]) < ratingMinValue:
            continue
        elif float(row[1]) >= ratingMinValue and float(row[2]) <= ratingMinValue:
            cur.execute("select * from rangeratingspart{0}".format(row[0]))
            for result in cur:
                fw.write("rangeratingspart{0},{1},{2},{3}\n".format(row[0], result[0], result[1], result[2]))
        else:
            cur.execute(
                "select * from rangeratingspart{0} where rating >= {1} and rating <= {2}".format(row[0], ratingMinValue,
                                                                                                 ratingMaxValue))
            for result in cur:
                fw.write("rangeratingspart{0},{1},{2},{3}\n".format(row[0], result[0], result[1], result[2]))

    fw.close()
    cur.close()

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