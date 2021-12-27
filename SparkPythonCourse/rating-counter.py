# -*- coding: utf-8 -*-
"""
Created on Fri Dec 24 10:28:57 2021

@author: LeonardoDiasdaRosa
"""

from pyspark import SparkConf, SparkContext
import collections

## Data
DATA_PATH = '../data/'
FILE = "ml-100k/u.data"

data = DATA_PATH + FILE

## Create Spark context
conf = SparkConf().setMaster("local").setAppName("RatingCounter")
sc = SparkContext(conf = conf)

## Read file
lines = sc.textFile(data)
ratings = lines.map(lambda x: x.split()[2])
results = ratings.countByValue()

## Print
sortedResults = collections.OrderedDict(sorted(results.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))



