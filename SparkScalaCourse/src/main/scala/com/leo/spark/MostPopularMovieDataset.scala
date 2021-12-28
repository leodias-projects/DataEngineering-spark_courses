package com.leo.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}

import scala.io.{Codec, Source}

object MostPopularMovieDataset {

  final case class movie(movieID: Int)

  def loadMoviesNames(): Map[Int,String] = {

    implicit val codec: Codec = Codec("ISO-8859-1")
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("../data/ml-100k/u.item")
    for (line <- lines.getLines()){
      val fields = line.split('|')
      if (fields.length > 1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    movieNames
  }

  def main(args: Array[String]): Unit = {

    val DATA_PATH = "../data/"

    // Remove messages that are not error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a spark session with sql library
    val spark = SparkSession
      .builder
      .appName("MostPopularMovieDataset")
      .master("local[*]")
      .getOrCreate()

    // Load movie names

    val names = spark.sparkContext.broadcast(loadMoviesNames())

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

    val lookupName : Int => String = (movieID:Int) => {
      names.value(movieID)
    }

    val nameUDF = udf(lookupName)

    val moviesWithNames = movie.withColumn("movieTitle", nameUDF(col("movieID")))

    moviesWithNames.collect().foreach(println)

    spark.stop()
  }
}
