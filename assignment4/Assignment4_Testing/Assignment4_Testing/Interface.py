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
    #if os.path.exists('RangeQueryOut.txt'): os.remove('RangeQueryOut.txt')
    #with open('RangeQueryOut.txt', 'w') as f:
    #    f.write('PartitionName, UserID, MovieID, Rating\n')
    cur = openconnection.cursor()
    # Query Range Tables
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'roundrobinratingspart%'")
    range_partition = []
    for item in cur:
        range_partition.append(item[0])

    for tablename in range_partition:
        cur.execute("SELECT * FROM {} WHERE rating >= {} and rating <= {}".format(tablename, ratingMinValue, ratingMaxValue))
        #results = cur.fetchall()
        #writeToFile('RangeQueryOut.txt', results)
        for calc in cur:
            fw.write("{},{},{},{}\n".format(tablename, calc[0], calc[1], calc[2]))

    cur.execute("SELECT * FROM rangeratingsmetadata")
    range_partition = []
    for item in cur:
        range_partition.append(item)
    # Query Round Robin
    for item in range_partition:
        if float(item[1]) > ratingMaxValue or float(item[2]) < ratingMinValue:
            continue
        elif float(item[1]) >= ratingMinValue and float(item[2]) <= ratingMinValue:
            cur.execute("SELECT * FROM rangeratingspart{}".format(item[0]))
            #results = cur.fetchall()
            #writeToFile('RangeQueryOut.txt', results)
            for calc in cur:
                fw.write("rangeratingspart{},{},{},{}\n".format(item[0], calc[0], calc[1], calc[2]))
        else:
            cur.execute("SELECT * FROM rangeratingspart{} WHERE rating >= {} and rating <= {}".format(item[0], ratingMinValue,ratingMaxValue))
            #results = cur.fetchall()
            #writeToFile('RangeQueryOut.txt', results)
            for calc in cur:
                fw.write("rangeratingspart{},{},{},{}\n".format(item[0], calc[0], calc[1], calc[2]))

    fw.close()
    cur.close()

def PointQuery(ratingsTableName, ratingValue, openconnection):
    # Query Range Tables
    fw = open("./PointQueryOut.txt", 'w')
    #if os.path.exists('PointQueryOut.txt'): os.remove('PointQueryOut.txt')
    #with open('PointQueryOut.txt', 'w') as f:
    #    f.write('PartitionName, UserID, MovieID, Rating\n')
    cur = openconnection.cursor()

    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'roundrobinratingspart%'")
    range_partition = []
    for item in cur:
        range_partition.append(item[0])

    for tablename in range_partition:
        cur.execute("SELECT * FROM {} WHERE rating = {}".format(tablename, ratingValue))
        #results = cur.fetchall()
        #writeToFile('PointQueryOut.txt', results)
        for calc in cur:
            fw.write("{},{},{},{}\n".format(tablename, calc[0], calc[1], calc[2]))

    cur.execute("SELECT * FROM rangeratingsmetadata")
    range_partition = []
    for item in cur:
        range_partition.append(item)

    # Query Round Robin
    for item in range_partition:
        if float(item[1]) > ratingValue or float(item[2]) < ratingValue:
            continue
        else:
            cur.execute("SELECT * FROM rangeratingspart{} WHERE rating = {}".format(item[0], ratingValue))

            #results = cur.fetchall()
            #writeToFile('PointQueryOut.txt', results)
            for calc in cur:
                fw.write("rangeratingspart{},{},{},{}\n".format(item[0], calc[0], calc[1], calc[2]))

    fw.close()
    cur.close()

def writeToFile(filename, items):
    f = open(filename, 'a')
    for line in items:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
