package com.leo.spark

import org.apache.log4j._
import org.apache.spark._

object RatingCounter {

  def main(args: Array[String]): Unit ={

    val DATA_PATH = "../data/"

    // Remove messages that are not error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // The function
    val finalCountingByValue = new SparkContext("local[*]", "RatingCounter")
      .textFile(DATA_PATH + "ml-100k/u.data")
      .map(x=> x.toString().split("\t")(2))
      .countByValue()
      .toSeq.sortBy(_._1)
      .foreach(println)

    // Create spark context locally
    //val sc = new SparkContext("local[*]", "RatingCounter")

    // Read the file
    //val ratingCounterFile = sc.textFile(DATA_PATH + "ml-100k/u.data")

    // For printing every line
    //readFile.collect().foreach(f=>{
    //  println(f)
    //})

    // Gets the rating value based on data columns (third column)
    //al ratingValue = readFile.map(x=> x.toString().split("\t")(2))

    // Group together same values and count'em!

    //val countValues = ratingValue.countByValue()

    // Sort the result
    // val sortedResult = countValues.toSeq.sortBy(_._1)

    // Print the result

    // sortedResult.foreach(println)
  }
}
