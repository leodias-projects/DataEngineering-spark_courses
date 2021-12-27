package com.leo.scalaspark

import org.apache.log4j._
import org.apache.spark.sql._

object DataframeDataset {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]): Unit = {

    val DATA_PATH = "C:/SparkScalaCourse/SparkScalaCourse/data/"

    // Remove messages that are not error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a spark session with sql library
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Create a dataset schema
    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(DATA_PATH + "fakefriends.csv")
      .as[Person]

    // Select name columns
    people.select("name").show()

    // Getting people under 21
    people.filter(people("age") < 21).show()

    // Group by age
    people.groupBy(people("age")).count().show()

    // Make everyone 18 years older
    people.select(people("name"), people("age") + 10).show()

    spark.stop()
   }
}
