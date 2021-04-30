import os
import os.path
import imp
import psycopg2
import sys
import csv
import traceback


# modify this line, give the Assignment5_Testing folder path
parentDir = "/home/user/CSE511/assignment5/Assignment5_Testing"

DATABASE_NAME = 'ddsassignment3'
SORT_TABLE_NAME = 'ratings'
SORT_OUTPUT_TABLE_NAME = 'parallelSortOutputTable'
SORT_COLUMN_NAME = 'Rating'

JOIN_LEFTTABLE_NAME = 'ratings'
JOIN_RIGHTTABLE_NAME = 'movies'
JOIN_LEFT_COLUMN = 'MovieId'
JOIN_RIGHT_COLUMN = 'MovieId1'
JOIN_OUTPUT_TABLE_NAME = 'parallelJoinOutputTable'



homeDirectory = parentDir

submissionDirectory =  parentDir

downloadfilename = "2019-10-23T1808_Grades-2019Fall-T-CSE512-79228.csv"
uploadfilename = "upload.csv"
commentsFile="Comments.csv"

def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

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

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS "+ratingstablename)

    cur.execute("CREATE TABLE "+ratingstablename+" (UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating REAL, temp5 VARCHAR(10), Timestamp INT)")

    loadout = open(ratingsfilepath,'r')

    cur.copy_from(loadout,ratingstablename,sep = ':',columns=('UserID','temp1','MovieID','temp3','Rating','temp5','Timestamp'))
    cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")

    cur.close()
    openconnection.commit()

def loadMovies(movietablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS " + movietablename)

    cur.execute("CREATE TABLE " + movietablename + " (MovieId1 INT,  Title VARCHAR(100),  Genre VARCHAR(100))")

    loadout = open(ratingsfilepath,'r')

    cur.copy_from(loadout, movietablename, sep ='_', columns=('MovieId1', 'Title', 'Genre'))
    #cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")

    cur.close()
    openconnection.commit()

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

def compareFileList(fileList1, fileList2):
    for element1 in fileList1:
        # print "element in studentList: " + element1
        found = False
        for element2 in fileList2:
            # print "element in baselineList: " + element2
            if (str(element1) in str(element2)):
                found = True
                break;
        if found == False:
            return -1, element1

    for element1 in fileList2:
        found = False
        for element2 in fileList1:
            if (str(element2) in str(element1)):
                found = True
                break;
        if found == False:
            return 1, element1

    return 0, None

def generateGradeFile(filename):
    try:
        #directory = os.listdir(submissionDirectory)
        directory = submissionDirectory
        print directory

        gradeDic = {}
        exceptionDic = {}

        print "Creating Database named as ddsassignment3"
        createDB();

        baselineSortList = []
        con = getOpenConnection()
        deleteTables('ALL', con)
        loadRatings(SORT_TABLE_NAME, 'ratings.dat', con)

        cur = con.cursor()
        cur.execute('select * from {0} order by {1}'.format(SORT_TABLE_NAME, SORT_COLUMN_NAME));
        for line in cur:
            baselineSortList.append(line)
        cur.close()
        deleteTables('ALL', con)

        baselineJoinList = []
        loadRatings(JOIN_LEFTTABLE_NAME, 'ratings.dat', con);
        loadMovies(JOIN_RIGHTTABLE_NAME, 'movies.dat', con);
        cur = con.cursor()
        cur.execute('select * from {0},{1} where {0}.{2} = {1}.{3}'.format(JOIN_LEFTTABLE_NAME, JOIN_RIGHTTABLE_NAME,
                                                                           JOIN_LEFT_COLUMN, JOIN_RIGHT_COLUMN));
        for line in cur:
            baselineJoinList.append(line)
        cur.close()
        deleteTables('ALL', con)
        con.close()

        # filename = "palak.py"
        uniqueId = filename

        # print filename
        if ".py" in filename and (".pyc" in filename ) == False:
            print "asuid:"+uniqueId+":"+filename

            print "Creating Database named as ddsassignment3"
            createDB();

            # Getting connection to the database
            print "Getting connection from the ddsassignment3 database"
            con = getOpenConnection();
            con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            deleteTables('All', con);

            # Loading two tables ratings and movies
            loadRatings(SORT_TABLE_NAME, 'ratings.dat', con);

            filename = filename[0:filename.__len__() - 3]
            fp, pathname, description = imp.find_module(filename, [submissionDirectory]);
            Assignment3 = imp.load_module(filename, fp, pathname, description)
            passCount = 0;
            exceptionList = [];
            # Calling ParallelSort
            try:
                print "Performing Parallel Sort"
                Assignment3.ParallelSort(SORT_TABLE_NAME, SORT_COLUMN_NAME, SORT_OUTPUT_TABLE_NAME, con);

                studentList = []
                cur = con.cursor()
                cur.execute('select * from {0}'.format(SORT_OUTPUT_TABLE_NAME))
                for line in cur:
                    studentList.append(line)
                cur.close()

                if ( len(baselineSortList) != len(studentList)):
                    raise Exception('{0} has {1} rows while {2} has {3}'.format(
                        SORT_OUTPUT_TABLE_NAME, len(studentList), SORT_TABLE_NAME, len(baselineSortList)
                    ))

                flag, element = compareFileList(baselineSortList, studentList)
                print "flag is " + str(flag)
                if ( flag == 0):
                    passCount+=1
                elif flag == -1:
                    raise Exception(str(element) + " does not exist in result table")
                else:
                    raise Exception(str(element) + " should not exist because it is not in the original table")
            except Exception as e:
                exceptionList.append("Error in ParallelSort: " + e.message)

            deleteTables('ALL', con);

            # Loading two tables ratings and movies
            loadRatings(JOIN_LEFTTABLE_NAME, 'ratings.dat', con);
            loadMovies(JOIN_RIGHTTABLE_NAME, 'movies.dat', con);
            # Calling ParallelJoin
            print "Performing Parallel Join"
            try:
                Assignment3.ParallelJoin(JOIN_LEFTTABLE_NAME, JOIN_RIGHTTABLE_NAME,
                                         JOIN_LEFT_COLUMN, JOIN_RIGHT_COLUMN, JOIN_OUTPUT_TABLE_NAME,
                                         con);
                studentList = []
                cur = con.cursor()
                cur.execute('select * from {0}'.format(JOIN_OUTPUT_TABLE_NAME))
                for line in cur:
                    studentList.append(line)
                cur.close()

                flag, element = compareFileList(baselineJoinList, studentList)
                print "flag is " + str(flag)
                if (flag == 0):
                    passCount += 1
                elif flag == -1:
                    raise Exception(str(element) + " does not exist in result table")
                else:
                    raise Exception(str(element) + " should not exist because it is not in the original table")
            except Exception as e:
                exceptionList.append("Error in ParallelJoin: " + str(e.message))

            deleteTables('ALL', con);

            gradeDic[uniqueId] = float(passCount * 10.0)
            if exceptionList.__len__()>0:
                exceptionDic[uniqueId] = exceptionList

            if con:
                con.close()
        print gradeDic
        print exceptionDic

        gradeOutput = open("grade.txt", "w")
        for key in gradeDic.keys():
            gradeOutput.write("{0}\t{1}\n".format(key, gradeDic[key]))
        gradeOutput.close()

        exceptionOutput = open("exception.txt", "w")
        for key in exceptionDic.keys():
            exceptionOutput.write("{0}\t{1}\n".format(key, exceptionDic[key]))
        exceptionOutput.close()


    except Exception as detail:
        traceback.print_exc()
        con = getOpenConnection();
        deleteTables('all', con);

if __name__ == '__main__':
    try:
        
        
      
        # modify the file name here, give your filename
        # your_file_name = "Assignment_Answer.py"
        your_file_name = 'Assignment3_Interface.py'
        generateGradeFile(your_file_name)

    except Exception as e:
        traceback.print_exc()
