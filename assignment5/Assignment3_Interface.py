#!/usr/bin/python2.7
#
# Assignment3 Interface
#
import psycopg2
import os
import sys
import threading


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    # Implement ParallelSort Here.
    cur = openconnection.cursor();
    cur.execute("Select min(%s) , max(%s) from %s" % (SortingColumnName, SortingColumnName, InputTable))
    row = cur.fetchone()
    max_num = float(row[1])
    min_num = float(row[0])
    increment = (max_num - min_num) / 5.0
    seed = min_num
    threads = []
    for i in range(1, 6):
        t = threading.Thread(target=sortworker,
                             args=(seed, seed + increment, i, InputTable, SortingColumnName, openconnection))
        threads.append(t)
        seed += increment
        t.start()
    cur.execute("DROP TABLE IF EXISTS %s" % (OutputTable,))
    cur.execute("CREATE TABLE %s AS SELECT * FROM %s WHERE 1=2" % (OutputTable, InputTable))
    for i in range(0, 5):
        threads[i].join()
        cur.execute("INSERT INTO %s SELECT * FROM %s%s" % (OutputTable, InputTable, i + 1))
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