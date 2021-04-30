#!/usr/bin/python2.7
#
# Assignment3 Interface
#
import psycopg2
import os
import sys
import threading


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    thread_num = 5
    cur = openconnection.cursor()
    
    cur.execute("SELECT MAX({}) FROM {}".format(SortingColumnName, InputTable))
    sort_max = cur.fetchone()[0]
    
    cur.execute("SELECT MIN({}) FROM {}".format(SortingColumnName, InputTable))
    sort_min = cur.fetchone()[0]
    
    step = (sort_max - sort_min)/thread_num
    threads_list = []
    for thread_num in range(thread_num):

        cur.execute("DROP TABLE IF EXISTS SortPart{}".format(thread_num))
        cur.execute("CREATE TABLE SortPart{} (LIKE {})".format(thread_num, InputTable))

        lower, upper = sort_min+thread_num*step, sort_min+(thread_num+1)*step
        cur_thread = threading.Thread(target=SortHelper,args=(openconnection, thread_num, InputTable, SortingColumnName, lower, upper))
        cur_thread.start()
        threads_list.append(cur_thread)
    [thread.join() for thread in threads_list]
    
    cur.execute("DROP TABLE IF EXISTS {}".format(OutputTable))
    cur.execute("CREATE TABLE {} (LIKE {} )".format(OutputTable, InputTable))
    for thread_num in range(thread_num):
        cur.execute("INSERT INTO {} SELECT * FROM SortPart{}".format(OutputTable, thread_num))
        cur.execute("DROP TABLE IF EXISTS SortPart{}".format(thread_num))
    cur.close()
    openconnection.commit()

    
def Helper(openconnection, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, lower, upper, JoinPart):
    cur = openconnection.cursor()
    cur.execute("INSERT INTO {0} SELECT * FROM {1} INNER JOIN {2} ON {1}.{3}={2}.{4} WHERE {1}.{3}<={5} AND {1}.{3}>={6}".format(JoinPart, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, upper, lower))
    cur.close()
    openconnection.commit()

def SortHelper(openconnection, thread_num, InputTable, SortingColumnName, lower, upper):
    cur = openconnection.cursor()
    if thread_num == 0: cur.execute("INSERT INTO SortPart{0} SELECT * FROM {1} WHERE {2}>={3} AND {2}<={4} ORDER BY {2} ASC".format(thread_num, InputTable, SortingColumnName, lower, upper))
    else: cur.execute("INSERT INTO SortPart{0} SELECT * FROM {1} WHERE {2}>{3} AND {2}<={4} ORDER BY {2} ASC".format(thread_num, InputTable, SortingColumnName, lower, upper))
    cur.close()
    openconnection.commit()

def sortworker(start, end, first, inputTable, SortingColumn, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS %s%s" % (inputTable, first))
    if (first == 1):
        cur.execute("create table %s%s AS (select * from %s where %s >= %s AND %s <= %s order by %s)" % (
        inputTable, first, inputTable, SortingColumn, start, SortingColumn, end, SortingColumn))
    else:
        cur.execute("create table %s%s AS (select * from %s where %s > %s AND %s <= %s order by %s)" % (
        inputTable, first, inputTable, SortingColumn, start, SortingColumn, end, SortingColumn))
    cur.close()
    return    


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    # Implement ParallelJoin Here.
    cur = openconnection.cursor();
    cur.execute("Select min(%s) , max(%s) from %s" % (Table1JoinColumn, Table1JoinColumn, InputTable1))
    row = cur.fetchone()
    max_num = float(row[1])
    min_num = float(row[0])
    increment = (max_num - min_num) / 5.0
    seed = min_num
    threads2 = []
    for i in range(1, 6):
        t = threading.Thread(target=joinworker, args=(
        seed, seed + increment, i, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection))
        threads2.append(t)
        seed += increment
        t.start()
    cur.execute("DROP TABLE IF EXISTS %s" % (OutputTable,))
    cur.execute("CREATE TABLE %s AS SELECT * FROM %s,%s WHERE 1=2" % (OutputTable, InputTable1, InputTable2))
    for i in range(0, 5):
        threads2[i].join()
        cur.execute("INSERT INTO %s SELECT * FROM %s%s" % (OutputTable, InputTable1, i + 1))
    cur.close()
    openconnection.commit()


def joinworker(start, end, first, inputTable1, inputTable2, Table1JoinColumn, Table2JoinColumn, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS %s%s" % (inputTable1, first))
    if (first == 1):
        cur.execute(
            "create table %s%s AS (select * from %s inner join %s on %s.%s=%s.%s where %s.%s >= %s AND %s.%s <= %s )" % (
            inputTable1, first, inputTable1, inputTable2, inputTable1, Table1JoinColumn, inputTable2, Table2JoinColumn,
            inputTable1, Table1JoinColumn, start, inputTable1, Table1JoinColumn, end))
    else:
        cur.execute(
            "create table %s%s AS (select * from %s inner join %s on %s.%s=%s.%s where %s.%s > %s AND %s.%s <= %s )" % (
            inputTable1, first, inputTable1, inputTable2, inputTable1, Table1JoinColumn, inputTable2, Table2JoinColumn,
            inputTable1, Table1JoinColumn, start, inputTable1, Table1JoinColumn, end))

    cur.close()
    return