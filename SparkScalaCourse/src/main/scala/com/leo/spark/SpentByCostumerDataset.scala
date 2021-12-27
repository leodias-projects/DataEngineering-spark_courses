package com.leo.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._

object SpentByCostumerDataset {
  case class Spent(customerID: Int, productID: Int, value: Float)

  def main(args: Array[String]): Unit = {

    val DATA_PATH = "../data/"

    // Remove messages that are not error
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Creates a spark session with sql library
    val spark = SparkSession
      .builder
      .appName("SpentByCostumerDataset")
      .master("local[*]")
      .getOrCreate()

    // Create a dataset schema
    val spentByCostumer = new StructType()
      .add("customerID", IntegerType, nullable = true)
      .add("productID", IntegerType, nullable = true)
      .add("value", FloatType, nullable = true)

    import spark.implicits._
    val customerOrderDataset = spark.read
      .schema(spentByCostumer)
      .csv(DATA_PATH + "customer-orders.csv")
      .as[Spent]

    val avgSpentByCustomerSorted = customerOrderDataset.select("customerID", "value" )
      .groupBy("customerID")
      .agg(round(sum("value"), 2).alias("average Spent"))
      .sort("average Spent")

    avgSpentByCustomerSorted.show(avgSpentByCustomerSorted.count.toInt)

    // Select customer ID and value spent
    //val customerID_value = customerOrderDataset.select("customerID", "value" )

    // Group by customerID and average rounded
    //val customerAvgSpent = customerID_value.groupBy("customerID")
    //  .agg(round(avg("value"), 2).alias("average Spent"))

    // Sort by amount spent
    // val sortedCustomerAvgSpent = customerAvgSpent.sort("average Spent")

    //sortedCustomerAvgSpent.collect().foreach(println)

    spark.stop()
  }
}
