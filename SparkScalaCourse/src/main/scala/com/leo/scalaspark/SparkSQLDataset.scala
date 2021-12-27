package com.leo.scalaspark

import org.apache.log4j._
import org.apache.spark.sql._

object SparkSQLDataset {

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
     val schemaPeople = spark.read
       .option("header", "true")
       .option("inferSchema", "true")
       .csv(DATA_PATH + "fakefriends.csv")
       .as[Person]

      // Create "a table"
      schemaPeople.createOrReplaceTempView("People")
      // Read as SQL
      val teenagers = spark.sql("SELECT * FROM PEOPLE WHERE age >= 13 and age <= 19")

      teenagers.collect.foreach(println)

      spark.stop()
   }
}
