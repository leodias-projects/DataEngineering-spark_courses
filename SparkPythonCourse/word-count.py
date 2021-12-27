# -*- coding: utf-8 -*-
"""
Created on Fri Dec 24 13:21:33 2021

@author: LeonardoDiasdaRosa
"""

from pyspark import SparkConf, SparkContext
import re

## Data
DATA_PATH = '../data/'
FILE = "book.txt"

data = DATA_PATH + FILE

## Function to process words
def normalizeWords(text):
    '''
    Adjust everything to ignore special characteres, 
    get only words (regular expression)
    and put it all to lower case
    '''
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


## Create Spark context
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

## Read file
lines = sc.textFile(data)

## The function
sortedUniqueWords = lines.flatMap(normalizeWords).map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x + y).map(lambda x: (x[1],x[0])).sortByKey(ascending=True)

## Get the words
# words = lines.flatMap(normalizeWords)

## create pairs in tuple
# createPairs = words.map(lambda x: (x, 1))

## Reduce by key
# reduceByKey_Pairs = createPairs.reduceByKey(lambda x, y: x + y)
## Order

## Reorder the tuple for in the future order by key
# organizedTuple = reduceByKey_Pairs.map(lambda x: (x[1],x[0]))

## Sort by key
# sortedUniqueWords = organizedTuple.sortByKey(ascending=True)

## Print
for i in sortedUniqueWords.collect():
    print(i[1], "------ numbers of time: ", i[0])
    