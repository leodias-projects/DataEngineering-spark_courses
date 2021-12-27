# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 09:57:22 2021

@author: LeonardoDiasdaRosa
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

# Create a SparkSession
spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

DATA_PATH = "C:/SparkPythonCourse/SparkPythonCourse/data/"
data_file = "fakefriends.csv"

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv(DATA_PATH + data_file)


relevantData = people.select(people.age, people.friends)

friendsByAge = relevantData.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age")
friendsByAge.show()

spark.stop()
