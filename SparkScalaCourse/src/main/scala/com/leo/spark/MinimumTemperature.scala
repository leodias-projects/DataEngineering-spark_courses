package com.leo.spark

import org.apache.log4j._
import org.apache.spark._
import scala.math.min

object MinimumTemperature {

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
     val minTemperature = new SparkContext("local[*]", "MinimumTemperature")
       .textFile(DATA_PATH + "1800.csv")
       .map(parseLine)
       .filter(x=>x._2 == "TMIN")
       .map(x=>(x._1, x._3))
       .reduceByKey((x,y)=>min(x,y))

     minTemperature.collect().foreach(f=>{
       val station = f._1
       val temperature = f._2
       println(s"The station $station minimum temperature was $temperature degrees Celsius")})

     //for (result <- minTemperature.collect()){
     //  val station = result._1
     //  val temperature = result._2
     //  println(s"The station $station minimum temperature was $temperature degrees Celsius")
     // }

     // Creates new spark context FriendsByAge locally
     //val sc = new SparkContext("local[*]", "MinimumTemperature")

     // Read data
     //val readFile = sc.textFile(DATA_PATH + "1800.csv")

     // readFile.collect().foreach(f=>{println(f)})
     //val parsedFile = readFile.map(parseLine)

     // Get data with TMIN information only
     //val minTemperatureData = parsedFile.filter(x=>x._2 == "TMIN")

     // Discard TMIN "row"
     //val stationsTemp = minTemperatureData.map(x=>(x._1, x._3))

     // Get the minimum value from the resulting data
     //val minTemperature = stationsTemp.reduceByKey((x,y)=>min(x,y))
     //minTemperature.collect().foreach(f=>{
     //  val station = f._1
     //  val temperature = f._2
     //  println(s"The station $station minimum temperature was $temperature degrees Celsius")})

   }
}
