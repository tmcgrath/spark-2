package com.supergloo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * An example of running
  * spark-submit --class com.supergloo.PoolExample
  * --conf spark.scheduler.mode=FAIR
  * --conf spark.scheduler.allocation.file=./src/main/resources/fair-example.xml
  * --master spark://tmcgrath-rmbp15.local:7077
  * ./target/scala-2.11/spark-2-assembly-1.0.jar
  */

object PoolExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Pool Example Before")
    conf.setIfMissing("spark.master", "local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // before FAIR pool
    Range(0, 15).foreach { i =>
      val csv = spark.read
        .csv(s"file:////Users/toddmcgrath/Development/tmp/csv/stock${i}.csv")
      println(csv.count)
      csv.foreachPartition(i => println(i))
    }

    // After FAIR POOL
//    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair_pool")
//
//    Range(0, 15).par.foreach { i =>
//      val csv = spark.read
//        .csv(s"file:////Users/toddmcgrath/Development/tmp/csv/stock${i}.csv")
//      println(csv.count)
//
//      spark.sparkContext.setLocalProperty("spark.scheduler.pool", "a_different_pool")
//      csv.foreachPartition(i => println(i))
//    }

    spark.stop()
    sys.exit(0)
  }
}
