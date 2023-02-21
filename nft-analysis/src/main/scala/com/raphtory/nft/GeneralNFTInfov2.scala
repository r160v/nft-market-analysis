package com.raphtory.nft

import org.apache.log4j._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This version is the last one. It's prepared for the dit machine (just have to change the package name)
 */
object GeneralNFTInfov2 extends App {

  case class Transaction(timestamp: String, nft: String, seller: String, sellerUsername: String, buyer: String,
                         buyerUsername: String, category: String, collection: String, priceUSD: String,
                         priceCrypto: String, crypto: String)

  def mapper(line: String): Transaction = {
    val fields = line.split(",").map(_.trim).toList

    val nft = fields(1)
    val seller = fields(3)
    val sellerUsername = fields(4)
    val buyer = fields(5)
    val buyerUsername = fields(6)
    val priceCrypto = fields(11)
    val crypto = fields(12)
    val priceUSD = fields(13)
    val category = fields.last
    val collection = fields(fields.length - 2)
    val timestamp  = fields(fields.length - 5)

    Transaction(timestamp, nft, seller, sellerUsername, buyer, buyerUsername, category, collection, priceUSD,
      priceCrypto, crypto)
  }

  //Logger.getLogger("app").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("NFT General Info")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val lines = spark.sparkContext
    .textFile("/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/data/datasetOKNoHeader.csv")
  val transactions: DataFrame = lines.map(mapper).toDS()
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:m:ss"))
    .withColumn("date", to_date(col("timestamp")))
    .withColumn("priceUSD", col("priceUSD").cast(DecimalType(38, 2)))
    .withColumn("priceCrypto", col("priceCrypto").cast(DecimalType(38, 2)))

  transactions.show(false)
  transactions.printSchema()

  def write(df: DataFrame, path: String, name: String): Unit = {
    df
      .coalesce(1)
      .write
      .option("header", value = true)
      .format("csv")
      .save(s"$path$name")
  }

  def numberOfNFT(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                  name: String = "total_number_of_nft"): Unit = {
    spark.sparkContext.parallelize(Seq(df
      .select("nft").distinct().count()))
      .toDF("total_number_of_nft")
      .coalesce(1)
      .write
      .option("header", value = true)
      .format("csv")
      .save(s"$path$name")
  }

  def numberOfTransactions(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                           name: String = "total_number_of_transactions"): Unit = {
    spark.sparkContext.parallelize(Seq(df
      .count()))
      .toDF("total_number_of_transactions")
      .coalesce(1)
      .write
      .option("header", value = true)
      .format("csv")
      .save(s"$path$name")
  }

  def numberOfNFTPerCrypto(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                           name: String = "number_of_nft_per_crypto"): Unit = {
    write(
    df
      .select("nft", "crypto")
      .groupBy("crypto")
      .agg(countDistinct("nft").alias("distinct_nft"))
      .orderBy("distinct_nft"),
      path,
      name
    )
  }

  def numberOfTransactionsPerCrypto(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                                    name: String = "number_of_transactions_per_crypto"): Unit = {
    write(
      df
        .select("crypto", "nft")
        .groupBy("crypto").count()
        .orderBy("count"),
      path,
      name
    )
  }

  def numberOfNFTPerCategory(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                             name: String = "number_of_nft_per_category"): Unit = {
    write(
      df
        .select("nft", "category")
        .groupBy("category")
        .agg(countDistinct("nft").alias("distinct_nft"))
        .orderBy("distinct_nft"),
      path,
      name
    )
  }

  def numberOfNFTPerCollection(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                               name: String = "number_of_nft_per_collection"): Unit = {
    write(
      df
        .select("nft", "collection", "category")
        .groupBy("category", "collection")
        .agg(countDistinct("nft").alias("distinct_nft"))
        .orderBy("category", "collection", "distinct_nft"),
      path,
      name
    )
  }

  def numberOfTransactionsPerCategory(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                                      name: String = "number_of_transactions_per_category"): Unit = {
    write(
      df
        .select("nft", "category")
        .groupBy("category").count()
        .orderBy("count"),
      path,
      name
    )
  }

  def dailyVolumeUSD(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                     name: String = "daily_volume"): Unit = {
    write(
    df
      .select("date", "category", "priceUSD")
      .groupBy("date", "category")
        .agg(sum("priceUSD").as("dailyVolume"))
      .orderBy("date", "category"),
      path,
      name
    )
  }

  def shareOfVolume(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                    name: String = "share_of_volume"): Unit = {
    val w: WindowSpec = Window.partitionBy("date")
      .orderBy("category")
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    write(
      df
        .select("date", "category", "priceUSD")
        .groupBy("date", "category")
        .agg(sum("priceUSD").as("dailyVolume"))
        .withColumn("totalDailyVolume", sum(col("dailyVolume")).over(w))
        .withColumn("shareOfVolume", col("dailyVolume")/col("totalDailyVolume"))
        .select("date", "category", "shareOfVolume")
        .orderBy("date", "category"),
      path,
      name
    )
  }

  def shareOfTransactions(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                          name: String = "share_of_transactions"): Unit = {
    val w: WindowSpec = Window.partitionBy("date")
      .orderBy("category")
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    write(
      df
        .select("date", "category")
        .groupBy("date", "category")
        .count()
        .withColumn("totalDailyTransactions", sum(col("count")).over(w))
        .withColumn("shareOfTransactions", col("count")/col("totalDailyTransactions"))
        .select("date", "category", "shareOfTransactions")
        .orderBy("date", "category"),
      path,
      name
    )
  }

  def numberOfTransactionsPerDay(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                                 name: String = "number_of_transactions_per_day"): Unit = {
    write(
      df
        .groupBy("date")
        .count(),
      path,
      name
    )
  }

  def numberOfTransactionsPerDayAndCategory(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                                 name: String = "number_of_transactions_per_day_and_category"): Unit = {
    write(
      df
        .groupBy("date", "category")
        .count(),
      path,
      name
    )
  }

  def saveCleanDataset(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                       name: String = "clean_dataset"): Unit = {
    write(
      df
        .orderBy("timestamp", "category", "collection"),
      path, name
    )
  }

  def priceCDF(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
               name: String = "priceCDF", wndw: Option[String]): Unit = {

    val resultDF = if (wndw.isDefined) {
      val priceWindow = Window.partitionBy("start", "end").orderBy(col("priceUSD").desc)
      val priceWindow2 = Window.partitionBy("start", "end").orderBy(col("priceUSD").desc)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

      df
        .filter(col("priceUSD").isNotNull)
        .select(window(col("timestamp"), wndw.get), col("priceUSD"))
        .withColumn("start", col("window.start"))
        .withColumn("end", col("window.end"))
        .withColumn("cumulative_probability", round(cume_dist().over(priceWindow), 2))
        .withColumn("total_price_sum", sum(col("priceUSD")).over(priceWindow2))
        .withColumn("percentage_of_total_price_sum",
          round(col("priceUSD")/col("total_price_sum") * 100, 2))
        .drop("window")
    }
    else {
      val priceWindow = Window.orderBy(col("priceUSD").desc)
      val priceWindow2 = Window.orderBy(col("priceUSD").desc)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

      df
        .select("priceUSD")
        .filter(col("priceUSD").isNotNull)
        .withColumn("cumulative_probability", round(cume_dist().over(priceWindow), 2))
        .withColumn("total_price_sum", sum(col("priceUSD")).over(priceWindow2))
        .withColumn("percentage_of_total_price_sum",
          round(col("priceUSD")/col("total_price_sum") * 100, 2))
        .drop("total_price_sum")
    }

    write(resultDF, path, name)
  }

  def numberOfUniqueTraders(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                            name: String = "unique_traders"): Unit = {

    val buyers = df
      .select("buyer")
      .withColumnRenamed("buyer", "traders")

    val sellers = df
      .select("seller")
      .withColumnRenamed("seller", "traders")

    val uniqueTraders = buyers.join(sellers, Seq("traders"), "outer")
      .distinct()

    write(
      spark.sparkContext.parallelize(Seq(uniqueTraders.count())).toDF("unique_traders"),
      path,
      name
    )
  }

  def numberOfUniqueCollections(df: DataFrame, path: String = "/home/rvlopez/tfm/raphtory4/src/main/scala/com/raphtory/examples/lotr/output/spark/",
                                name: String = "unique_collections"): Unit = {

    write(
      spark.sparkContext.parallelize(Seq(df.select("collection").distinct().count()))
        .toDF("unique_collections"),
      path,
      name
    )
  }

  def priceCDFPerCategory(): Unit = {
    for(category <- transactions.select("category").distinct.as[String].collect.toList) {
      priceCDF(
        transactions.filter(col("category") === category),
        name = s"priceCDF_$category",
        wndw = None
      )
    }
  }


  numberOfNFT(transactions)
  numberOfTransactions(transactions)
  numberOfNFTPerCrypto(transactions)
  numberOfTransactionsPerCrypto(transactions)
  numberOfNFTPerCategory(transactions)
  numberOfNFTPerCollection(transactions)
  numberOfTransactionsPerCategory(transactions)
  dailyVolumeUSD(transactions)
  shareOfVolume(transactions)
  shareOfTransactions(transactions)
  saveCleanDataset(transactions)
  priceCDF(transactions, wndw=None)
  priceCDFPerCategory()
  numberOfUniqueTraders(transactions)
  numberOfUniqueCollections(transactions)

  spark.stop()

  // package com.raphtory.examples.lotr.graphbuilders

}
