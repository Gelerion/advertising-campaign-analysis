package com.gelerion.advertising.campaign.analysis

import org.apache.spark.sql.functions.{col, countDistinct, from_unixtime, to_date}

object CampaignAnalysisMain extends SparkJob {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    // Read the streaming event data for the previous day
    val adEvents = spark.read
      .option("basePath", "data/streaming_event_data")
      .json("data/streaming_event_data/date=*")
    adEvents.show(4)
    println("=======================================================================")

    adEvents.createOrReplaceTempView("ad_events")

    // Compute the daily aggregates
    // 1. Audience reach
    val audienceReach = spark.sql(
      """
        |SELECT date, campaign_id, count(DISTINCT user_id) AS unique_users
        |FROM ad_events
        |GROUP BY date, campaign_id
        |""".stripMargin)

    // 2. User engagement
    val userEngagement = spark.sql(
      """
        |SELECT date, campaign_id, ad_id, count(DISTINCT user_id) AS unique_users
        |FROM ad_events
        |WHERE click_id IS NOT NULL
        |GROUP BY date, campaign_id, ad_id
        |""".stripMargin)

    println("Audience reach report")
    audienceReach.show(4)
    println("=======================================================================")
    println("User engagement report")
    userEngagement.show(4)

    // Write the results to the data lake
    audienceReach
      .repartition(1, col("date"))
      .write
      .mode("overwrite")
      .partitionBy("date")
      .json("data/historical_data/audience_reach")

    userEngagement
      .repartition(1, col("date"))
        .write
        .mode("overwrite")
        .partitionBy("date")
        .json("data/historical_data/user_engagement")
  }

  //Scala code example˚
  //SQL example
  //    adEvents
  //      .groupBy($"date", $"ad_id", $"campaign_id")
  //      .agg(
  //        countDistinct($"user_id").as("unique_users")
  //      )
  //      .show(4)

  // Compute the daily aggregates: audience reach
  //    adEvents.groupBy($"date", $"campaign_id").agg(countDistinct($"user_id"))

  // Compute the daily aggregates: user engagement
  //    adEvents.where($"click_id".isNotNull).groupBy($"date", $"ad_id", $"campaign_id").agg(countDistinct($"user_id"))

}
