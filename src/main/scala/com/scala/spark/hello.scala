package com.scala.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object hello {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("hello").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._
    import spark.sql
    val data   = "C://BigData\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("delimiter",",").load(data)
    df.show()
    spark.stop()
  }
}