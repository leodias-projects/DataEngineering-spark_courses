package com.leo.spark

import org.apache.log4j._
import org.apache.spark._
import scala.math.max

object MaximumTemperature {

  /** Split and return a tuple with age and number of friends for every line */
  def parseLine(line: String): (String, String, Float) ={
    // split every line by the comma
    val linesColumns = line.split(",")
    // get the first field of the line (stationID)
    val stationID = linesColumns(0)
    // get the third field of the line (TMAX or TMIN)
    val typeOfInformation = linesColumns(2)
    // get the temperature in Celsius
    val temperature = linesColumns(3).toFloat
    // return a tuple (stationID, type, temperature)
    (stationID, typeOfInformation, temperature)
  }

   def main(args: Array[String]): Unit = {

     val DATA_PATH = "../data/"

     // Remove messages that are not error
     Logger.getLogger("org").setLevel(Level.ERROR)
     // The function
     val maxTemperature = new SparkContext("local[*]", "MaximumTemperature")
       .textFile(DATA_PATH + "1800.csv")
       .map(parseLine)
       .filter(x=>x._2 == "TMAX")
       .map(x=>(x._1, x._3))
       .reduceByKey((x,y)=>max(x,y))

     maxTemperature.collect().foreach(f=>{
       val station = f._1
       val temperature = f._2
       println(s"The station $station maximum temperature was $temperature degrees Celsius")})

     //for (result <- maxTemperature.collect()){
     //  val station = result._1
     //  val temperature = result._2
     //  println(s"The station $station maximum temperature was $temperature degrees Celsius")
     // }

     // Creates new spark context FriendsByAge locally
     //val sc = new SparkContext("local[*]", "MaximumTemperature")

     // Read data
     //val readFile = sc.textFile(DATA_PATH + "1800.csv")

     // readFile.collect().foreach(f=>{println(f)})
     //val parsedFile = readFile.map(parseLine)

     // Get data with TMAX information only
     //val max TemperatureData = parsedFile.filter(x=>x._2 == "TMAX")

     // Discard TMAX "row"
     //val stationsTemp = maxTemperatureData.map(x=>(x._1, x._3))

     // Get the maximum value from the resulting data
     //val maxTemperature = stationsTemp.reduceByKey((x,y)=>max(x,y))
     //maxTemperature.collect().foreach(f=>{
     //  val station = f._1
     //  val temperature = f._2
     //  println(s"The station $station maximum temperature was $temperature degrees Celsius")})

   }
}
