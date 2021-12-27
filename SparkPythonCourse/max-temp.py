# -*- coding: utf-8 -*-
"""
Created on Fri Dec 24 13:09:58 2021

@author: LeonardoDiasdaRosa
"""

from pyspark import SparkConf, SparkContext


## Data
DATA_PATH = '../data/'
FILE = "1800.csv"

data = DATA_PATH + FILE

# Function to get data from lines
def parseLine(lines):
    line = lines.split(",")
    stationID = line[0]
    typeMeasure = line[2]
    temperature = float(line[3])
    return (stationID, typeMeasure, temperature)


## Create Spark context
conf = SparkConf().setMaster("local").setAppName("MaximumTemperature")
sc = SparkContext(conf = conf)

## Maximum temperature
maximumTemperature = sc.textFile(data).map(parseLine).filter(lambda x: "TMAX" in x[1])\
    .map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: max(x, y))

for i in (maximumTemperature.collect()):
    print(i[0], round(i[1], 2))
    
## Read file
#lines = sc.textFile(data)

## Relevant Data
#relevantData = lines.map(parseLine)

## Filter Data
#dataTMAX = relevantData.filter(lambda x: "TMAX" in x[1])

## Get the data
#stationTemp = dataTMAX.map(lambda x: (x[0], x[2]))

## Group by station
#maxByStation = stationTemp.reduceByKey(lambda x, y: max(x, y))

## Print results

#for i in (maxByStation.collect()):
    #print(i[0], round(i[1], 2))
