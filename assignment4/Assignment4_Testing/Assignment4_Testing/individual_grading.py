import os
import os.path
import Assignment1
import imp
import psycopg2
import traceback
import csv
import sys
# modify this line, give the Assignment4_Testing folder path
parentDir = "/Users/CSE-511/Assignment4/Assignment4_Testing"


homeDirectory = parentDir
submissionDirectory = parentDir




# modify this line
downloadcsvfile = "2019-10-09t1523_grades-2019fall-t-cse512-79228.csv"
uploadcsvfile = "upload.csv"
ratingsTableName = 'ratings'
ratingsPath = homeDirectory + "/test_data.dat"
rangeQueryOutputPath = homeDirectory + "/RangeQueryOut.txt"
pointQueryOutputPath = homeDirectory + "/PointQueryOut.txt"
commentsFile="Comments2.csv"

def compareLine(line1, line2):
    lineList1 = line1.split(",")
    lineList2 = line2.split(",")
    if ( lineList1.__len__() != lineList2.__len__()):
        return False

    length = lineList1.__len__()
    for i in range(0, length-1):
        if (lineList1[i].__eq__(lineList2[i])):
            continue
        else:
            return False

    if ( lineList1[length-1].__eq__(lineList2[length-1])):
        return True
    else:
        rate1 = float(lineList1[length-1])
        rate2 = float(lineList2[length-1])
        if (rate1 == rate2):
            return True
        else:
            return False

def compareFileRangeQuery(ratingMinValue, ratingMaxValue):
    baselinePath = "RangeQueryOut_" + str(ratingMinValue) + "_" + str(ratingMaxValue) + ".txt"
    file = open(baselinePath, "r")
    baselineList = []
    for line in file:
        baselineList.append(line.replace("\n","").replace("\r",""))
    file.close()

    studentList = []
    file = open(rangeQueryOutputPath, "r")
    for line in file:
        studentList.append(line.replace("\n","").lower())
    file.close()

    for element1 in studentList:
        print "element in studentList: " + element1
        found = False
        for element2 in baselineList:
            print "element in baselineList: " + element2
            if (compareLine(element1, element2)):
                found = True
                break;
            # else:

        if found == False:
            raise Exception("[{2}] should not exist for range query [{0},{1}]!".format(
                                ratingMinValue, ratingMaxValue, element1
                            ))

    for element1 in baselineList:
        found = False
        for element2 in studentList:
            if (compareLine(element1, element2)):
                found = True
                break;
        if found == False:
            raise Exception("[{2}] not found in your result for range query [{0},{1}]!".format(
                                ratingMinValue, ratingMaxValue, element1
                            ))

def compareFilePointQuery(ratingValue):
    baselinePath = "PointQueryOut_" + str(ratingValue) + ".txt"
    file = open(baselinePath, "r")
    baselineList = []
    for line in file:
        baselineList.append(line.replace("\n","").lower())
    file.close()

    studentList = []
    file = open(pointQueryOutputPath, "r")
    for line in file:
        studentList.append(line.replace("\n","").lower())
    file.close()

    # test code
    # print "student list size: " + str(studentList.__len__())

    for element1 in studentList:
        # print "element in studentList: " + element1
        found = False
        for element2 in baselineList:
            # print "element in baselineList: " + element2
            if (compareLine(element1, element2)):
                found = True
                break;
        if found == False:
            raise Exception("[{1}] should not exist for point query {0}!".format(
                                ratingValue, element1
                            ))

    for element1 in baselineList:
        found = False
        for element2 in studentList:
            if (compareLine(element1, element2)):
                found = True
                break;
        if found == False:
            raise Exception("[{1}] not found in your result for point query {0}!".format(
                                ratingValue, element1
                            ))


def generateGradeFile(filename):
    try:

        directory = os.listdir(submissionDirectory)
        # print directory

        gradeDic = {}
        exceptionDic = {}


        # print filename
        if ".py" in filename and (".pyc" in filename ) == False:
            print ":"+filename
            # Use this function to do any set up before creating the DB, if any

            # print "Creating Database named as ddsassignment2"
            # Creating Database ddsassignment2
            Assignment1.createDB();

            # Getting connection to the database
            # print "Getting connection from the ddsassignment2 database"
            con = Assignment1.getOpenConnection();
            con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            # clear tables
            Assignment1.deleteTables('all', con);

            # Loading Ratings table
            # print "Creating and Loading the ratings table"
            Assignment1.loadRatings(ratingsTableName, ratingsPath, con);

            # Doing Range Partition
            print "Doing the Range Partitions"
            Assignment1.rangePartition(ratingsTableName, 5, con);

            # Doing Round Robin Partition
            print "Doing the Round Robin Partitions"
            Assignment1.roundRobinPartition(ratingsTableName, 5, con);

            # Deleting Ratings Table because Point Query and Range Query should not use ratings table instead they should use partitions.
            Assignment1.deleteTables(ratingsTableName, con);

            filename = filename.split(".")[0]
            fp, pathname, description = imp.find_module(filename, [submissionDirectory]);
            Assignment2 = imp.load_module(filename, fp, pathname, description)

            passCount = 0;
            exceptionList = []

            ratingMinValueList = [1.5, 1, 1, 3.5, 3]
            ratingMaxValueList = [3.5, 2, 4, 4.5, 5]

            for i in range(0, 5):
                ratingMinValue = ratingMinValueList[i]
                ratingMaxValue = ratingMaxValueList[i]
                print "Performing Range Query [{0},{1}]".format(ratingMinValue, ratingMaxValue)
                try:
                    file = open(rangeQueryOutputPath, "w")
                    file.write("")
                    file.close()
                    Assignment2.RangeQuery("ratings",ratingMinValue, ratingMaxValue, con);
                    compareFileRangeQuery(ratingMinValue, ratingMaxValue)
                    passCount += 1
                    file = open(rangeQueryOutputPath, "w")
                    file.write("")
                    file.close()
                except Exception as e:
                    exceptionList.append(e)
                    traceback.print_exc()
            print "Range Query passCount: "+str(passCount)

            ratingValueList = [2, 4.5, 5, 3, 0.5]

            for ratingValue in ratingValueList:
                print "Performing Point Query {0}".format(ratingValue)
                try:
                    file = open(pointQueryOutputPath, "w")
                    file.write("")
                    file.close()
                    Assignment2.PointQuery("ratings",ratingValue, con);
                    compareFilePointQuery(ratingValue)
                    passCount += 1
                    file = open(pointQueryOutputPath, "w")
                    file.write("")
                    file.close()
                except Exception as e:
                    exceptionList.append(e)
                    print e
            print "Total query passcount: "+str(passCount)

            gradeDic[filename] = float(passCount * 2.0)
            if exceptionList.__len__()>0:
                exceptionDic[filename] = exceptionList

            # Deleting All Tables
            Assignment1.deleteTables('all', con);

            if con:
                con.close()
        print gradeDic
        print exceptionDic

    except Exception as detail:
        con = Assignment1.getOpenConnection();
        Assignment1.deleteTables('all', con);
        print detail

if __name__ == '__main__':
    try:
        # modify the file name here, give your filename
        your_file_name = "Assignment_Answer.py"
        generateGradeFile(your_file_name)
        
        
    except Exception as e:
        print e
