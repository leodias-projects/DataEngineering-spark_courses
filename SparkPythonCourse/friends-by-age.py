# -*- coding: utf-8 -*-
"""
Created on Fri Dec 24 12:15:12 2021

@author: LeonardoDiasdaRosa
"""

from pyspark import SparkConf, SparkContext

## Data
DATA_PATH = '../data/'
FILE = "fakefriends-noheader.csv"

data = DATA_PATH + FILE

# Function to get data from lines
def parseLine(lines):
    line = lines.split(",")
    age = int(line[2])
    friends = int(line[3])
    return (age, friends)


## Create Spark context
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

## Friends by age
friendsByAge = sc.textFile(data).map(parseLine).mapValues(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(lambda x: x[0] / x[1])

for i in (sorted(friendsByAge.collect())):
    print(i[0], round(i[1], 2))

## Read file
#lines = sc.textFile(data)

## Data
#dataValues = lines.map(parseLine)

## Adjust dict
#newDict = dataValues.mapValues(lambda x: (x, 1))

## Sum the occurences
#sumDict = newDict.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#averagesByAge = sumDict.mapValues(lambda x: x[0] / x[1])

## Print
#for i in (sorted(averagesByAge.collect())):
#    print(i[0], round(i[1], 2))
