package com.supergloo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*
CSV file source looks like
AMZN,"2017/08/28","946.0200","2576456.0000","946.5400","953.0000","942.2500"
AMZN,"2017/08/25","945.2600","3316602.0000","956.0000","957.6210","944.1000"
AMZN,"2017/08/24","952.4500","5186993.0000","957.4200","959.0000","941.1400"
AMZN,"2017/08/23","958.0000","2642936.0000","959.3800","962.0000","954.2000"

no header line
 */
object StockExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Stock Example")
    conf.setIfMissing("spark.master", "local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val source = spark.read
                  .csv("file:///tmp/csv/*.csv")
                  .withColumnRenamed("_c0", "symbol")
                  .withColumnRenamed("_c1", "date")
                  .withColumnRenamed("_c2", "close")
                  .withColumnRenamed("_c3", "volume")
                  .withColumnRenamed("_c4", "open")
                  .withColumnRenamed("_c5", "high")
                  .withColumnRenamed("_c6", "low")
                  .withColumn("date", unix_timestamp($"date", "yyyy/MM/dd"))
                  .withColumn("close", $"close".cast("decimal"))
                  .withColumn("volume", $"volume".cast("decimal"))
                  .withColumn("open", $"open".cast("decimal"))
                  .withColumn("high", $"high".cast("decimal"))
                  .withColumn("low", $"low".cast("decimal"))
                  .as[Stock]

    println(s"YO: ${source.collectAsList.parallelStream().count()}")

    source.foreachPartition(i => println(i))

    spark.stop()
    sys.exit(0)
  }
}
case class Stock(symbol: String, date: java.sql.Timestamp, close: BigDecimal, volume: BigDecimal,
                 open: BigDecimal, high: BigDecimal, low: BigDecimal)