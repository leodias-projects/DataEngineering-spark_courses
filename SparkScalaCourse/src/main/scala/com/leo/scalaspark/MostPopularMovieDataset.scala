package com.leo.scalaspark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}

object MostPopularMovieDataset {

  final case class movie(movieID: Int)

  def main(args: Array[String]): Unit = {

    val DATA_PATH = "C:/SparkScalaCourse/SparkScalaCourse/data/"

    // Remove messages that are not error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a spark session with sql library
    val spark = SparkSession
      .builder
      .appName("MostPopularMovieDataset")
      .master("local[*]")
      .getOrCreate()

    // Create a dataset schema
    val movieReview = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", IntegerType, nullable = true)

    import spark.implicits._
    val movieReviewDataset = spark.read
      .option("sep", "\t")
      .schema(movieReview)
      .csv(DATA_PATH + "ml-100k/u.data")
      .as[movie]

    // Select Movie ID
    val movie = movieReviewDataset.groupBy("movieID" ).count().orderBy(asc("count"))

    movie.show(movie.count.toInt)

    spark.stop()
  }
}
