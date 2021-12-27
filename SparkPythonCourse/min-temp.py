# -*- coding: utf-8 -*-
"""
Created on Fri Dec 24 12:49:16 2021

@author: LeonardoDiasdaRosa
"""

from pyspark import SparkConf, SparkContext

def parseLine(lines):
    line = lines.split(",")
    stationID = line[0]
    typeMeasure = line[2]
    temperature = float(line[3])
    return (stationID, typeMeasure, temperature)


## Create Spark context
conf = SparkConf().setMaster("local").setAppName("MinimumTemperature")
sc = SparkContext(conf = conf)

## Data
DATA_PATH = "C:/SparkPythonCourse/SparkPythonCourse/data/"
data_file = "1800.csv"

data = DATA_PATH + data_file

## Minimum temperature
minimumTemperature = sc.textFile(data).map(parseLine).filter(lambda x: "TMIN" in x[1])\
    .map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: min(x, y))

for i in (minimumTemperature.collect()):
    print(i[0], round(i[1], 2))
    
## Read file
#lines = sc.textFile(data)

## Relevant Data
#relevantData = lines.map(parseLine)

## Filter Data
#dataTMIN = relevantData.filter(lambda x: "TMIN" in x[1])

## Get the data
#stationTemp = dataTMIN.map(lambda x: (x[0], x[2]))

## Group by station
#minByStation = stationTemp.reduceByKey(lambda x, y: min(x, y))

## Print results

#for i in (minByStation.collect()):
    #print(i[0], round(i[1], 2))
