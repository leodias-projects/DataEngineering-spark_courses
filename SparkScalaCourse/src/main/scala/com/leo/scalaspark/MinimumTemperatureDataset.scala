package com.leo.scalaspark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object MinimumTemperatureDataset {

  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)

  def main(args: Array[String]): Unit = {

    val DATA_PATH = "C:/SparkScalaCourse/SparkScalaCourse/data/"

    // Remove messages that are not error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a spark session with sql library
    val spark = SparkSession
      .builder
      .appName("MinTemperatures")
      .master("local[*]")
      .getOrCreate()

    // Create a dataset schema
    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", IntegerType, nullable = true)

    import spark.implicits._
    val temperatureDataset = spark.read
      .schema(temperatureSchema)
      .csv(DATA_PATH + "1800.csv")
      .as[Temperature]

    val minTemperatures = temperatureDataset.filter($"measure_type" === "TMIN")
      .select("stationID", "temperature")
      .groupBy("stationID").min("temperature")

    minTemperatures.collect().foreach(println)
    // Take only data where measure type is TMIN
    //val minTemperatures = temperatureDataset.filter($"measure_type" === "TMIN")

    // Select stationID and temperature
    //val stationTemps = minTemperatures.select("stationID", "temperature")

    // Group by stationIDs by minimum temperature
    //val minTempByStation = stationTemps.groupBy("stationID").min("temperature")

    //minTempByStation.collect().foreach(println)
    spark.stop()
  }
}
