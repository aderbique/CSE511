#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'


##########################################################################################################


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

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()


# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" % (ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d` + ",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


if __name__ == '__main__':
    try:
        # Creating Database ddsassignment2
        print "Creating Database named as ddsassignment2"
        createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment2 database"
        con = getOpenConnection();

        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE,
                     'parallelJoinOutputTable', con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail