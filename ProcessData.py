#0 -*- coding: utf-8 -*-
"""
Created on Sun Oct 11 12:05:41 2015

@author: Askery Canabarro

THIS CODE IS TO COMPUTE MINUTE DATA FROM STOCK MARKET (BRAZILIAN STOCK LAYOUT ATTACHED)
"""

# THIS IS TO DETERMINE THE COMPUTATIONAL TIME DURATION
from datetime import datetime
start=datetime.now()


from pyspark import SparkContext
#1 ALL DAILY FILES ARE IN THE SAME FOLDER - JUST ADDRESSING THE FOLDER PATH IS ENOUGHT TO GATHER EVERYTHING
logFile = "/media/data/rowData_201304_201512/NEG_20130920.TXT"  # FILE FROM SYSTEM
#
sc = SparkContext("local", "Minute Data")

#2 CREATES THE RDD 
logData = sc.textFile(logFile)


#2 SELECTION OF STOCKS 
stocks = ['PETR3', 'PETR4' ,'ABEV3', 'BBAS3', 'BBDC4', 'BVMF3', 'GGBR4', 'ITSA4', 'ITUB4','VALE3', 'VALE5']
def filterStock(line):
    return any(keyword in line for keyword in stocks)

#3 IF WANT ALL STOCKS, COMMENT EVERYTHING BETWEEN #2 AND #3 AND UNCOMMENT EVERYTHING BETWEEN #3 AND #4
# CODE TO DEAL WITH ALL STOCKS COMING SOON

#4 SELECTION OF SUITABLE COLUMNS AND KEY
def makeColumns(line):
    pieces = line.split(';');   # SEPARATORS ARE ";"
    date   = pieces[0]          # FIRST COLUMN  - DATE
    symb   = pieces[1].strip()  # SECOND COLUMN - STOCK SYMBOL
    hour   = pieces[5][:2]      # EXTRACT HOUR FROM TRANSACTION TIME FROM (HH:MM)
    minu   = pieces[5][3:5]     # EXTRACT MINUTE FROM (HH:MM)
    key    = symb + date + hour + minu # KEY DEFINITION AS CONCATENATION OF TIME COLUMNS
    output = str(hour) + str(minu) + "   " + symb + "   " + str("%.2f" % float(pieces[3])) + "   " + str(int(pieces[4])) # OUTOUT FORMAT WITHOUT 'U', ',' OR '()'
    return (key, output)

#5 SELECT FIRST MINUTE OCCURENCY. TO PICK LAST USE 'return b;'
def minData(a, b):
    return a;

#6 THIS IS TO DEAL WITH DIFFERENT STOCK IN DISTINCT OUTPUT FILES
def separate(a):
    if stock in a[0]:
        return (a[0],a[1])    

#7 THIS IS THE APPLICATION OF SOME TRANSFORMATIONS AND ACTIONS NEEDED ON THE 'logData' RDD
#   FIRST A FILTER TO DEAL ONLY WITH THE DESIRED STOCKS - filter(filterStock)
#   THEN WE SELECT THE COLUMNS OF INTEREST IN 'map(makeColumns)'
#   NOW WE EXTRACT THE MINUTE DATA USING 'reduceByKey(minData)'
#   FINALLY WE SORT BY KEY
result= logData.filter(filterStock).map(makeColumns).reduceByKey(minData).sortByKey()


#8 OUTPUT
#   FIRST WE FILTER IF stock IS IN THE KEYS - filter(separate)   
#   ".values()" IS TO TAKE ONLY THE output OF THE (KEY, VALUE) PAIRS
#   ".coalesce(1)" PUT EVERYTHING IN JUST ONE OUTPUT FILE, NAMED AS THE ELEMENTS IN THE stocks LIST
for stock in stocks:
    result.filter(separate).values().coalesce(1).saveAsTextFile(str(stock))

# PRINT THE DURATION OF THE EXECUTION OF THE PROGRAM
print datetime.now() - start