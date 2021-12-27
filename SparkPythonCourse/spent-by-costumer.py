# -*- coding: utf-8 -*-
"""
Created on Sun Dec 26 19:51:48 2021

@author: LeonardoDiasdaRosa
"""

from pyspark import SparkConf, SparkContext

## Data
DATA_PATH = '../data/'
FILE = "customer-orders.csv"

data = DATA_PATH + FILE

## Function to parse the data
def parseData(csvData):
    clientData = csvData.split(",")
    clientID = clientData[0]
    spent = float(clientData[2])
    return (clientID, spent)
    

## Create Spark context
conf = SparkConf().setMaster("local").setAppName("SpentByCostumer")
sc = SparkContext(conf = conf)


## The function
spentByCostumerOrderedByTotal = sc.textFile(data).map(parseData).reduceByKey(lambda x, y: (x + y))\
    .map(lambda x: (x[1], x[0])).sortByKey()

## Read file
# spentData = sc.textFile(data)

## Relevant tuple
# spentPerID_tuple = spentData.map(parseData)

## reduce by key
# spentByClient = spentPerID_tuple.reduceByKey(lambda x, y: (x + y))

## reorder tuple to sort by total spent
# reOrder = spentByClient.map(lambda x: (x[1], x[0]))

## Sort
# spentByCostumerOrderedByTotal = reOrder.sortByKey()

for i in spentByCostumerOrderedByTotal.collect():
    print(i[1], round(i[0], 2))

