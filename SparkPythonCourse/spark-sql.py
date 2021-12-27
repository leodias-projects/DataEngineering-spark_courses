# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 08:52:10 2021

@author: LeonardoDiasdaRosa
"""

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


DATA_PATH = "C:/SparkPythonCourse/SparkPythonCourse/data/"
data_file = "fakefriends.csv"

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv(DATA_PATH + data_file)

people.printSchema()

people.select("name").show()

people.filter(people.age < 21).show()

people.groupBy("age").count().show

people.select(people.name, people.age + 10).show()
spark.stop()
