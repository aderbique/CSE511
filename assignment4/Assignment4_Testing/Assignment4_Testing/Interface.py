#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    if os.path.exists('RangeQueryOut.txt'): os.remove('RangeQueryOut.txt')
    with open('RangeQueryOut.txt', 'w') as f:
        f.write('PartitionName, UserID, MovieID, Rating\n')
    cur = openconnection.cursor()

    cur.execute("select table_name from information_schema.tables where table_name like 'roundrobinratingspart%'")
    range_partition = []
    for item in cur:
        range_partition.append(item[0])

    for tablename in range_partition:
        cur.execute("select * from {0} where rating >= {1} and rating <= {2}".format(tablename, ratingMinValue, ratingMaxValue))
        results = cur.fetchall()
        writeToFile('RangeQueryOut.txt', results)
        #for calc in cur:
        #    fw.write("{0},{1},{2},{3}\n".format(tablename, calc[0], calc[1], calc[2]))

    cur.execute("select * from rangeratingsmetadata")
    range_partition = []
    for item in cur:
        range_partition.append(item)

    for item in range_partition:
        if float(item[1]) > ratingMaxValue or float(item[2]) < ratingMinValue:
            continue
        elif float(item[1]) >= ratingMinValue and float(item[2]) <= ratingMinValue:
            cur.execute("select * from rangeratingspart{0}".format(item[0]))
            results = cur.fetchall()
            writeToFile('RangeQueryOut.txt', results)
            #for calc in cur:
            #    fw.write("rangeratingspart{0},{1},{2},{3}\n".format(item[0], calc[0], calc[1], calc[2]))
        else:
            cur.execute("select * from rangeratingspart{0} where rating >= {1} and rating <= {2}".format(item[0], ratingMinValue,ratingMaxValue))
            results = cur.fetchall()
            writeToFile('RangeQueryOut.txt', results)
            #for calc in cur:
            #    fw.write("rangeratingspart{0},{1},{2},{3}\n".format(item[0], calc[0], calc[1], calc[2]))

    #fw.close()
    cur.close()

def PointQuery(ratingsTableName, ratingValue, openconnection):
    if os.path.exists('PointQueryOut.txt'): os.remove('PointQueryOut.txt')
    with open('PointQueryOut.txt', 'w') as f:
        f.write('PartitionName, UserID, MovieID, Rating\n')
    cur = openconnection.cursor()

    cur.execute("select table_name from information_schema.tables where table_name like 'roundrobinratingspart%'")
    range_partition = []
    for item in cur:
        range_partition.append(item[0])

    for tablename in range_partition:
        cur.execute("select * from {0} where rating = {1}".format(tablename, ratingValue))
        results = cur.fetchall()
        writeToFile('PointQueryOut.txt', results)
        #for calc in cur:
        #    fw.write("{0},{1},{2},{3}\n".format(tablename, calc[0], calc[1], calc[2]))

    cur.execute("select * from rangeratingsmetadata")
    range_partition = []
    for item in cur:
        range_partition.append(item)

    for item in range_partition:
        if float(item[1]) > ratingValue or float(item[2]) < ratingValue:
            continue
        else:
            cur.execute("select * from rangeratingspart{0} where rating = {1}".format(item[0], ratingValue))

            results = cur.fetchall()
            writeToFile('PointQueryOut.txt', results)
            #for calc in cur:
            #    fw.write("rangeratingspart{0},{1},{2},{3}\n".format(item[0], calc[0], calc[1], calc[2]))

    #fw.close()
    cur.close()

def writeToFile(filename, items):
    f = open(filename, 'a')
    for line in items:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
