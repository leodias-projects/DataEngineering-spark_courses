package com.leo.spark

import org.apache.log4j._
import org.apache.spark._

object CountingWordsOccurrence {

  def main(args: Array[String]): Unit = {

     val DATA_PATH = "../data/"

     // Remove messages that are not error
     Logger.getLogger("org").setLevel(Level.ERROR)

     // The function
     val countWordsOccurrence = new SparkContext("local[*]", "CountingWordsOccurrence")
       .textFile(DATA_PATH + "book.txt")
       .flatMap(x=> x.split("\\W+"))
       .map(x => x.toLowerCase())
     //  .countByValue()
       .map(x => (x,1))
       .reduceByKey((x,y) => x + y)
       .map(x => (x._2, x._1))
       .sortByKey()
     countWordsOccurrence.collect().foreach(println)

     // Creates new spark context FriendsByAge locally
     //val sc = new SparkContext("local[*]", "CountingWordsOccurrence")

     // Read data
     //val readFile = sc.textFile(DATA_PATH + "book.txt")

     // Separate by space
     //val words = readFile.flatMap(x => x.split("\\W+"))

     // to lower case
     //val lowerCaseWords = words.map(x => x.toLowerCase())

     // Tuple the words to count occurrences
     //val tupleWords = lowerCaseWords.map(x => (x,1))

     // Sum the same words (counting occurrence)
     //val sumWordOccurrence = tupleWords.reduceByKey((x,y) => x + y)

     // Flip the tuple
     //val wordOccurrence = sumWordOccurrence.map(x => (x._2, x._1))

     // order by value
     //val sortedOccurrence = wordOccurrence.sortByKey()
     //sortedOccurrence.collect().foreach(println)
   }
}
