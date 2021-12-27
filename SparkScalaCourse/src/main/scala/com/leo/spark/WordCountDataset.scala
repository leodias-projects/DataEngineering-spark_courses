package com.leo.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountDataset {

  case class Book(value: String)

  def main(args: Array[String]): Unit = {

    val DATA_PATH = "../data/"

    // Remove messages that are not error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a spark session with sql library
    val spark = SparkSession
      .builder
      .appName("WordCountDataset")
      .master("local[*]")
      .getOrCreate()

    // Create a dataset schema
    import spark.implicits._
    val peopleDataset = spark.read.text(DATA_PATH + "book.txt").as[Book]

    // Create rows with words
    val wordsAsRows = peopleDataset.select(explode(split($"value", "\\W+"))
      .alias("word"))
      .filter($"word" =!= "")

    // Lower case words, count, and sort
    val lowerCaseWords = wordsAsRows.select(lower($"word").alias("word"))
      .groupBy("word")
      .count()
      .sort("count")

    lowerCaseWords.show(lowerCaseWords.count.toInt)


    spark.stop()
  }
}
