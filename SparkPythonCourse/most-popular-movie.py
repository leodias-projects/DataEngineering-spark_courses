# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 14:26:25 2021

@author: LeonardoDiasdaRosa
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

## Data
DATA_PATH = '../data/'
FILE = "ml-100k/u.data"

data = DATA_PATH + FILE

# Create a SparkSession
spark = SparkSession.builder.appName("MostPopularMovie").getOrCreate()

## Create the schema
schema = StructType([\
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True), ])

## Open the file with the schema created
data = spark.read.option("sep","\t").schema(schema).csv(data)

## Get relevant columns
relevantData = data.select("movieID", "rating")

## Group by movie ID, count the number of ratings, rename, and sort
mostPopularMovies = relevantData.groupBy("movieID").agg(func.count("rating").alias("n_ratings")).sort("n_ratings")

## Function to get movies name from file
def loadMovieNames():
    movieNames = {}
    with codecs.open(DATA_PATH + "ml-100k/u.item", 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


IDNamesDict = spark.sparkContext.broadcast(loadMovieNames())

## Look up the names based on ID
def getName(movieID):
    return IDNamesDict.value[movieID]


## Create a new function
movieNamesUDF = func.udf(getName)

## Create new column with defined function
movieWithNames = mostPopularMovies.withColumn("MovieTitle", movieNamesUDF(func.col("movieID")))

newTable = movieWithNames.select("MovieTitle", "n_ratings")
newTable.show(newTable.count())

spark.stop()
