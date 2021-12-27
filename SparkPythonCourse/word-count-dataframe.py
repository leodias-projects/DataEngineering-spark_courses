# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 10:13:22 2021

@author: LeonardoDiasdaRosa
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

## Data
DATA_PATH = '../data/'
FILE = "book.txt"

data = DATA_PATH + FILE

# Create a SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()

## Function
inputDF = spark.read.text(data)

words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lowerCaseWords = words.select(func.lower(words.word).alias("word"))
wordCount = lowerCaseWords.groupBy("word").count()
wordCountSorted = wordCount.sort("count")

wordCountSorted.show(wordCountSorted.count())
spark.stop()