# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 10:36:19 2021

@author: LeonardoDiasdaRosa
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

## Data
DATA_PATH = '../data/'
FILE = "1800.csv"

data = DATA_PATH + FILE

# Create a SparkSession
spark = SparkSession.builder.appName("MinimumTemperature").getOrCreate()

## Create the schema
schema = StructType([\
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True), ])

## Open the file with the schema created
data = spark.read.schema(schema).csv(data)

## Filter the data to get only minimum measures
tminData = data.filter(data.measure_type == "TMIN")

## Get only stationID and temperature
relevantData = tminData.select("stationID", "temperature")

## Group by stationID with minimum temperature
minTemperatureByStation = relevantData.groupBy("stationID").agg(func.round(func.min("temperature"), 2).alias("minimum temperature"))

minTemperatureByStation.show()
spark.stop()
