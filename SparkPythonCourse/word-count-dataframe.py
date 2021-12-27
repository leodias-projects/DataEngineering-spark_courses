# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 10:13:22 2021

@author: LeonardoDiasdaRosa
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

# Create a SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()

DATA_PATH = "C:/SparkPythonCourse/SparkPythonCourse/data/"
data_file = "book.txt"

inputDF = spark.read.text(DATA_PATH + data_file)

words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lowerCaseWords = words.select(func.lower(words.word).alias("word"))
wordCount = lowerCaseWords.groupBy("word").count()
wordCountSorted = wordCount.sort("count")

wordCountSorted.show(wordCountSorted.count())
spark.stop()