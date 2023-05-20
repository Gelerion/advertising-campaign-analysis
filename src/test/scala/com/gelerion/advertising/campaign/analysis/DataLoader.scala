package com.gelerion.advertising.campaign.analysis

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, from_unixtime, to_date}

import java.time.LocalDateTime
import java.time.ZoneOffset.UTC

object DataLoader {

  def main(args: Array[String]): Unit = {
    val spark = buildSpark
    import spark.implicits._

    val startDate = LocalDateTime.now(UTC).minusDays(3)
    val endDate = LocalDateTime.now(UTC)

    //generate 100,000 events
    AdEventsDataGenerator.generate(100000, BetweenDates(startDate, endDate))
      .toDS()
      .withColumn("date", to_date(from_unixtime(col("timestamp"))))
      .repartition(1, col("date"))
      .write
      .partitionBy("date")
      .mode(SaveMode.Overwrite)
      .json("data/ad_events")
  }

  private def buildSpark: SparkSession = {
    SparkSession.builder()
      .master("local[3]")
      .appName("data_loader")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()
  }
}
