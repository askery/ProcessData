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
logFile = "/media/data/rowData_201304_201601/NEG_20130920.TXT"  # FILE FROM SYSTEM
#
sc = SparkContext("local", "Minute Data")

#2 Creates the RDD 
logData = sc.textFile(logFile)


#2 Selection of stocks - here we are slecting the 61 IBOV (main BM&FBovespa Index) stocks 
stocks = ['ABEV3','BBAS3']

#stocks = ['ABEV3','BBAS3','BBDC3','BBDC4','BBSE3','BRAP4','BRFS3','BRKM5','BRML3','BVMF3',
#'CCRO3','CESP6','CIEL3','CMIG4','CPFE3','CPLE6','CSAN3','CSNA3','CTIP3','CYRE3',
#'ECOR3','EMBR3','ENBR3','EQTL3','ESTC3','FIBR3','GGBR4','GOAU4','HGTX3','HYPE3',
#'ITSA4','ITUB4','JBSS3','KLBN11','KROT3','LAME4','LREN3','MRFG3','MRVE3','MULT3',
#'NATU3','OIBR3','PCAR4','PETR3','PETR4','QUAL3','RADL3','RENT3','RUMO3','SANB11',
#'SBSP3','SMLE3','SUZB5','TBLE3','TIMP3','UGPA3','USIM5','VALE3','VALE5','VIVT4','WEGE3']

#3 selection filter
def filterStock(line):
    return any(keyword in line for keyword in stocks)


#4 Selection of columns 
def makeColumns(line):
    pieces  = line.split(';');          # delimiters are ";"
    date    = pieces[0].strip()         # first column  - date
    symb    = pieces[1].strip()         # second column - stock symbol
    time    = pieces[5].split(':')      # Extract hours, minutes, seconds and miliseconds from trade time (HH:MM:SS.NNN)
    hour    = time[0]                   # extract hour - fisrt element in 'time'   
    minu    = time[1]                   # extract minutes - second element in 'time'
    sec     = time[2].split('.')
    key     = symb + date + hour + minu # KEY DEFINITION AS CONCATENATION OF TIME COLUMNS
    output  = str(hour) + str(minu) + "   " + symb + "   " + str("%.2f" % float(pieces[3])) + "   " + str(int(pieces[4])) # OUTOUT FORMAT WITHOUT 'U', ',' OR '()'
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