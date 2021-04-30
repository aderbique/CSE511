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


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    thread_num = 5
    cur = openconnection.cursor()
    
    cur.execute("SELECT MAX({}) FROM {}".format(Table1JoinColumn, InputTable1))
    max_sort1 = cur.fetchone()[0]
    
    cur.execute("SELECT MIN({}) FROM {}".format(Table1JoinColumn, InputTable1))
    min_sort1 = cur.fetchone()[0]
    
    cur.execute("SELECT MAX({}) FROM {}".format(Table2JoinColumn, InputTable2))
    max_sort2 = cur.fetchone()[0]
    
    cur.execute("SELECT MIN({}) FROM {}".format(Table2JoinColumn, InputTable2))
    min_sort2 = cur.fetchone()[0]
    
    minimum, maximum = min(min_sort1, min_sort2), max(max_sort1, max_sort2) 
    step = (maximum - minimum) / float(thread_num)
    
    cur.execute("DROP TABLE IF EXISTS {}".format(OutputTable))
    cur.execute("CREATE TABLE {} AS SELECT * FROM {}, {} WHERE 1=2".format(OutputTable, InputTable1, InputTable2))
    
    threads_list = []
    for thread_num in range(thread_num):

        cur.execute("DROP TABLE IF EXISTS JoinPart{}".format(thread_num))
        cur.execute("CREATE TABLE JoinPart{} AS SELECT * FROM {},{} WHERE 1 = 2".format(thread_num, InputTable1, InputTable2))

        lower, upper = minimum + thread_num * step, minimum + (thread_num + 1) * step
        cur_thread = threading.Thread(target=Helper,args=(openconnection, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,lower, upper, 'JoinPart'+str(thread_num)))
        cur_thread.start()
        threads_list.append(cur_thread)
    [thread.join() for thread in threads_list]
    for thread_num in range(thread_num):
        cur.execute("INSERT INTO {} SELECT * FROM JoinPart{}".format(OutputTable, thread_num))
        cur.execute("DROP TABLE IF EXISTS JoinPart{}".format(thread_num))
    cur.close()
    openconnection.commit()