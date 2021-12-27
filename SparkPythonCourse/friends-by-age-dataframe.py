# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 09:57:22 2021

@author: LeonardoDiasdaRosa
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

## Data
DATA_PATH = '../data/'
FILE = "fakefriends.csv"

data = DATA_PATH + FILE

## Create a SparkSession
spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

## Get data 
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv(data)

## Select age and number of friends only
relevantData = people.select(people.age, people.friends)

## Group by age using average number of friends - round, rename, and sort
friendsByAge = relevantData.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age")
friendsByAge.show()

spark.stop()
