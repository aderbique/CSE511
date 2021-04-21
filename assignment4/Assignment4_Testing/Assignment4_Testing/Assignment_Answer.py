#!/usr/bin/python2.7
#
# Assignment2 Interface
# This file is the correct answer
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
    fw = open("./PointQueryOut.txt", 'w')
    cur = openconnection.cursor()

    cur.execute("select table_name from information_schema.tables where table_name like 'roundrobinratingspart%'")
    l = []
    for row in cur:
        l.append(row[0])

    for tablename in l:
        cur.execute(
            "select * from {0} where rating = {1}".format(tablename, ratingValue))
        for result in cur:
            fw.write("{0},{1},{2},{3}\n".format(tablename, result[0], result[1], result[2]))

    cur.execute("select * from rangeratingsmetadata")
    l = []
    for row in cur:
        l.append(row)

    for row in l:
        if float(row[1]) > ratingValue or float(row[2]) < ratingValue:
            continue
        else:
            cur.execute(
                "select * from rangeratingspart{0} where rating = {1}".format(row[0], ratingValue))
            for result in cur:
                fw.write("rangeratingspart{0},{1},{2},{3}\n".format(row[0], result[0], result[1], result[2]))

    fw.close()
    cur.close()
