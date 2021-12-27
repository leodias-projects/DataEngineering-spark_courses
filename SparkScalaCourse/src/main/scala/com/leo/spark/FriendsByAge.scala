package com.leo.scalaspark

import org.apache.log4j._
import org.apache.spark._

object FriendsByAge {

  /** Split and return a tuple with age and number of friends for every line */
  def parseLine(line: String): (Int, Int) ={
    // split every line by the comma
    val linesColumns = line.split(",")
    // get the third field of the line (age) and transform to int
    val age = linesColumns(2).toInt
    // get the fourth field of the line (number of friends) and transform to int
    val numFriends = linesColumns(3).toInt
    // return a tuple (age, number of friends)
    (age, numFriends)
  }

  def main(args: Array[String]): Unit ={

    val DATA_PATH = "../data/"

    // Remove messages that are not error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // The function
    val friendsByAgeFinal = new SparkContext("local[*]", "FriendsByAge")
      .textFile(DATA_PATH + "fakefriends-noheader.csv")
      .map(parseLine)
      .mapValues(x=>(x,1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x=>(x._1 / x._2))
      .sortByKey()

    friendsByAgeFinal.collect().foreach(println)

    // Creates new spark context FriendsByAge locally
    //val sc = new SparkContext("local[*]", "FriendsByAge")

    // Read the data file
    // val dataFile = sc.textFile(DATA_PATH + "fakefriends-noheader.csv")

    //xx.collect().foreach(f=>{
    //  println(f)})

    // Get every line of the file as a input (with map) to parseLine
    //val relevantData = dataFile.map(parseLine)

    // Create a tuple (age, (number of friends, 1)) for every sample
    //val tupleData = relevantData.mapValues(x=>(x,1))

    // For a tuple (key, value) group the same keys and sum its values
    //val groupedByAgeData = tupleData.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // Divide the total amount to number of occurrences

    //val averageByAge = groupedByAgeData.mapValues(x=>(x._1 / x._2))

    // Sort by key

    //val results = averageByAge.collect()
    //results.sorted.foreach(println)
  }
}
