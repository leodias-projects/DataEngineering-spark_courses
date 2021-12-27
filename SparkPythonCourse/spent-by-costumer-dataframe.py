# -*- coding: utf-8 -*-
"""
Created on Mon Dec 27 12:12:40 2021

@author: LeonardoDiasdaRosa
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

## Data
DATA_PATH = '../data/'
FILE = "customer-orders.csv"

data = DATA_PATH + FILE

# Create a SparkSession
spark = SparkSession.builder.appName("SpentByCostumer").getOrCreate()

## Create the schema
schema = StructType([\
                     StructField("customerID", IntegerType(), True), \
                     StructField("productID", IntegerType(), True), \
                     StructField("spent", FloatType(), True)])

## Open the file with the schema created
data = spark.read.schema(schema).csv(data)

## Get only customer ID and spent
relevantData = data.select("customerID", "spent")

## Group by ID, gets average or total spent by customer, change name and sort ascending
spentByCostumer = relevantData.groupBy("customerID").agg(func.round(func.sum("spent"), 2).alias("total_spent"))\
    .sort("total_spent")
    
spentByCostumer.show(spentByCostumer.count())
spark.stop()
