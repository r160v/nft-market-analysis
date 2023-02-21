package com.raphtory.nft

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object WindowProcessCDFs extends App {
  val spark = SparkSession
    .builder()
    .appName("CDFs")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def write(df: DataFrame, path: String, name: String): Unit = {
    df
      .coalesce(1)
      .write
      .option("header", value = true)
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save(s"$path$name")
  }

  /**
   * This function reads the output of EdgeWeight algorithm and finds the weight CDF
   * @param inputPath
   * @param outputPath
   * @param name
   */
  def weightCDF(inputPath: String, outputPath: String, name: String, writeDF: Boolean): Unit = {

    val schema = StructType(Array(
      StructField("timestamp", StringType, nullable = false),
      StructField("window", StringType, nullable = false),
      StructField("buyer", StringType, nullable = false),
      StructField("seller", StringType, nullable = false),
      StructField("weight", StringType, nullable = false),
    ))

    val weights = spark.read.schema(schema).csv(inputPath)
      .drop("window")
      .withColumn("timestamp", from_unixtime(col("timestamp") / 1000))
      .orderBy("timestamp")

    val window = Window.partitionBy("timestamp").orderBy(col("weight").desc)

    val window2 = Window.partitionBy("timestamp").orderBy(col("weight").desc)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val resultDF = weights
      .withColumn("cumulative_probability", round(cume_dist().over(window), 2))
      .withColumn("total_transactions", sum(col("weight")).over(window2))
      .withColumn("percentage_of_total_transaction",
        round(col("weight") / col("total_transactions") * 100, 2))
      .drop("total_transactions", "buyer", "seller")

    if (writeDF)
      write(resultDF, outputPath, name)
    else resultDF.show(false)
  }

  def weightCDFOneWindow(inputPath: String, outputPath: String, name: String, writeDF: Boolean): Unit = {

    val schema = StructType(Array(
      StructField("timestamp", StringType, nullable = false),
      StructField("window", StringType, nullable = false),
      StructField("buyer", StringType, nullable = false),
      StructField("seller", StringType, nullable = false),
      StructField("weight", IntegerType, nullable = false),
    ))

    val weights = spark.read.schema(schema).csv(inputPath)
      .drop("timestamp", "window", "buyer", "seller")

    weights.show()

    val weightWindow = Window.orderBy(col("weight").desc)
    val weightWindow2 = Window.orderBy(col("weight").desc)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val resultDF = weights
      .select("weight")
      .filter(col("weight").isNotNull)
      .withColumn("cumulative_probability", round(cume_dist().over(weightWindow), 5))
      .withColumn("total_transactions", sum(col("weight")).over(weightWindow2))
      .withColumn("percentage_of_total_transaction",
        round(col("weight") / col("total_transactions") * 100, 2))
//      .drop("total_transactions", "buyer", "seller")

    if (writeDF)
      write(resultDF, outputPath, name)
    else resultDF.orderBy(col("weight").desc).show(false)
  }

  /**
   * This function reads the output of TraderStrength algorithm and finds the strength CDF
   * @param inputPath: indicates where to read the dataframe from
   * @param outputPath: indicates where to store the processed dataframe
   * @param name: indicates the name of the folder where the processed dataframe is stored
   */
  def NFTStrengthCDF(inputPath: String, outputPath: String, name: String, writeDF: Boolean): Unit = {
    val schema = StructType(Array(
      StructField("timestamp", StringType, nullable = false),
      StructField("window", StringType, nullable = false),
      StructField("trader", StringType, nullable = false),
      StructField("spent", StringType, nullable = false),
      StructField("gained", StringType, nullable = false),
      StructField("purchased", StringType, nullable = false),
      StructField("sold", StringType, nullable = false),
      StructField("strength", IntegerType, nullable = false),
      StructField("activity", IntegerType, nullable = false),
      StructField("purchasedInfo", StringType, nullable = false),
      StructField("soldInfo", StringType, nullable = false),
    ))

    val strength = spark.read.schema(schema).csv(inputPath)
      .select("timestamp", "strength", "activity")

    val (strengthWindow, strengthWindow2) =
      (
        Window.partitionBy("timestamp").orderBy(col("strength").desc),
        Window.partitionBy("timestamp").orderBy(col("strength").desc)
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      )

    val (activityWindow, activityWindow2) =
      (
        Window.partitionBy("timestamp").orderBy(col("activity").desc),
        Window.partitionBy("timestamp").orderBy(col("activity").desc)
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      )

    val resultDF = strength
      .withColumn("strength_cumulative_probability", round(cume_dist().over(strengthWindow), 11))
      .withColumn("total_transactions", sum(col("strength")).over(strengthWindow2))
      .withColumn("activity_cumulative_probability", round(cume_dist().over(activityWindow), 11))
      .withColumn("total_activity_days", sum(col("activity")).over(activityWindow2))

    if (writeDF)
      write(resultDF, outputPath, name)
    else resultDF.orderBy(col("timestamp").asc, col("strength").desc).show(200, false)
  }

  def traderStrengthCDF(inputPath: String, outputPath: String, name: String, writeDF: Boolean): Unit = {
    val schema = StructType(Array(
      StructField("timestamp", StringType, nullable = false),
      StructField("window", StringType, nullable = false),
      StructField("trader", StringType, nullable = false),
      StructField("inDegree", IntegerType, nullable = false),
      StructField("outDegree", IntegerType, nullable = false),
      StructField("totalDegree", IntegerType, nullable = false)
    ))

    val strength = spark.read.schema(schema).csv(inputPath)
      .select("timestamp", "totalDegree")

    val (strengthWindow, strengthWindow2) =
      (
        Window.partitionBy("timestamp").orderBy(col("totalDegree").desc),
        Window.partitionBy("timestamp").orderBy(col("totalDegree").desc)
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      )

    val resultDF = strength
      .withColumn("strength_cumulative_probability", round(cume_dist().over(strengthWindow), 11))
      .withColumn("total_connections", sum(col("totalDegree")).over(strengthWindow2))

    if (writeDF)
      write(resultDF, outputPath, name)
    else resultDF.orderBy(col("timestamp").asc, col("totalDegree").desc)
      .show(200, truncate = false)
  }

  def windowAverageWeight(inputPath: String, outputPath: String, name: String, writeDF: Boolean): Unit = {

    val schema = StructType(Array(
      StructField("timestamp", StringType, nullable = false),
      StructField("window", StringType, nullable = false),
      StructField("buyer", StringType, nullable = false),
      StructField("seller", StringType, nullable = false),
      StructField("weight", IntegerType, nullable = false),
    ))

    val weights = spark.read.schema(schema).csv(inputPath)
      .drop("window", "buyer", "seller")

    weights.orderBy("timestamp").show()

    val resultDF = weights
      .groupBy("timestamp")
      .agg(
        avg(col("weight")).as("average"),
        expr("approx_percentile(weight, array(0.5))").alias("median")
      )

    if (writeDF)
      write(resultDF, outputPath, name)
    else resultDF.show(false)
  }

  def dailyVolume(inputPath: String, outputPath: String, name: String, writeDF: Boolean): Unit = {

    val schema = StructType(Array(
      StructField("date", StringType, nullable = false),
      StructField("category", StringType, nullable = false),
      StructField("dailyVolume", StringType, nullable = false)
    ))

    val dailyVolume = spark.read.option("header", value = true).schema(schema).csv(inputPath)
      .withColumn("date", to_date(col("date")))
      .withColumn("dailyVolume", col("dailyVolume").cast(DecimalType(38, 18)))

    dailyVolume.orderBy("date").show()

    val resultDF = dailyVolume
      .orderBy("date")
      .groupBy(window(dailyVolume.col("date"),"30 days", slideDuration="1 days"))
      .agg(avg("dailyVolume").as("monthly_average"))
      .withColumn("date", col("window.start"))
      .drop("window")
      .orderBy("date")

    if (writeDF)
      write(resultDF, outputPath, name)
    else resultDF.show(false)
  }

  def activityVSStrength(inputPath: String, outputPath: String, name: String, writeDF: Boolean): Unit = {
    val schema = StructType(Array(
      StructField("timestamp", StringType, nullable = false),
      StructField("strength", IntegerType, nullable = false),
      StructField("activity", IntegerType, nullable = false),
      StructField("strength_cumulative_probability", StringType, nullable = false),
      StructField("total_transactions", StringType, nullable = false),
      StructField("activity_cumulative_probability", StringType, nullable = false),
      StructField("total_activity_days", StringType, nullable = false)
    ))

    val activityStrength = spark.read.option("header", value = true).schema(schema).csv(inputPath)
      .drop("strength_cumulative_probability", "total_transactions", "activity_cumulative_probability",
            "total_activity_days")
      .withColumn("timestamp", from_unixtime(col("timestamp") / 1000))

    val wndw = Window.orderBy(col("activity"))

    activityStrength.withColumn("lag", lag(col("strength"), 1).over(wndw))
      .withColumn("difference", col("strength") - col("lag")).show()
  }

  def test(inputPath: String): Unit = {
    val schema = StructType(Array(
      StructField("strength", IntegerType, nullable = false),
      StructField("activity", IntegerType, nullable = false)
    ))

    val activityStrength = spark.read.option("header", value = true).schema(schema).csv(inputPath)

    val wndw = Window.orderBy(col("activity"))

    activityStrength.withColumn("wind", lag(col("strength"), 1).over(wndw)).show()
  }

  /**
   * strengthCDF is OK: Whole data and different window size calculated correctly
   * -- TODO: Update price and weightCDF in the same way
   */

  //weightCDF("src/main/scala/com/raphtory/nft/output/raphtory/EdgeWeight/EdgeWeight_1668550344994", "", "", writeDF = false)
  //weightCDFOneWindow("src/main/scala/com/raphtory/nft/output/raphtory/total_window", "src/main/scala/com/raphtory/nft/output/raphtory/EdgeWeight_", "weightCDF_total_window", writeDF = true)
  //windowAverageWeight("src/main/scala/com/raphtory/nft/output/raphtory/EdgeWeight/EdgeWeight_1668550344994", "src/main/scala/com/raphtory/nft/output/raphtory/EdgeWeight_", "weightCDF_total_window", writeDF = false)
//  NFTStrengthCDF("src/main/scala/com/raphtory/nft/output/raphtory/TraderStrength/TraderStrength_One_Day_Window", "", "", writeDF = false)
//  NFTStrengthCDF("src/main/scala/com/raphtory/nft/output/raphtory/TraderStrength/TraderStrength_Thirty_Day_Window", "", "", writeDF = false)
//  dailyVolume("src/main/scala/com/raphtory/nft/data/daily_volume_per_cat.csv", "src/main/scala/com/raphtory/nft/output/raphtory/", "dailyVolume.csv", writeDF = true)
  //traderStrengthCDF("src/main/scala/com/raphtory/nft/output/raphtory/TraderNFTDegree/Trader/Trader", "", "", writeDF = false)
  activityVSStrength("src/main/scala/com/raphtory/nft/data/strengthCDF_total_window.csv", "", "", false)

  spark.stop()
}
