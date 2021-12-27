package com.leo.spark

import org.apache.log4j._
import org.apache.spark._

object SpentByCostumer {

  /** Function to separate lines to a tuple (customerID, value spent) */
  def parseLines(lines: String): (Int, Float) ={
    val lineData = lines.split(",")
    val customer = lineData(0).toInt
    val value = lineData(2).toFloat
    (customer, value)
  }

  def main(args: Array[String]): Unit = {

    val DATA_PATH = "../data/"

     // Remove messages that are not error
    Logger.getLogger("org").setLevel(Level.ERROR)

     // The function
    val totalCostumer = new SparkContext("local[*]", "SpentByCostumer")
      .textFile(DATA_PATH + "customer-orders.csv")
      .map(parseLines)
      .reduceByKey((x, y) => (x + y))
      .map(x => (x._2, x._1))
      .sortByKey()
    totalCostumer.collect().foreach(println)
     //countWordsOccurrence.collect().foreach(println)

     // Creates new spark context FriendsByAge locally
    //val sc = new SparkContext("local[*]", "TotalCostumer")

     // Read data
    //val readFile = sc.textFile(DATA_PATH + "customer-orders.csv")

     // Get information as tuple (customerID, spent)
    //val tupleID_Value = readFile.map(parseLines)

    // Sum the amount in a key
    //val totalByCustomer = tupleID_Value.reduceByKey((x, y) => (x + y))

    // Swap ID and value
    //val swappedData = totalByCustomer.map(x => (x._2, x._1))

    // Sort by key
    //val orderedTotal = swappedData.sortByKey()

   }
}
