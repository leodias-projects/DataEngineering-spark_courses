package com.leo.scalaspark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FriendsByAgeDataset {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]): Unit = {

    val DATA_PATH = "../data/"

    // Remove messages that are not error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a spark session with sql library
    val spark = SparkSession
      .builder
      .appName("FriendsByAgeDataset")
      .master("local[*]")
      .getOrCreate()

    // Create a dataset schema
    import spark.implicits._
    val peopleDataset = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(DATA_PATH + "fakefriends.csv")
      .as[Person]

    // The function
    val friendsByAge = peopleDataset.select("age", "friends")
      .groupBy("age").agg(round(avg("friends"), 2)
      .alias("Average friends"))
      .sort("age")
      .show()

    spark.stop()
  }
}
