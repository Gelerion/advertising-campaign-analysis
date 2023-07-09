package com.gelerion.advertising.campaign.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object CampaignAnalysis extends SparkJob {

  def readAdEvents(datesGlob: String): DataFrame = {
    val adEvents = spark.read
      .option("basePath", "data/streaming_event_data")
      .json(s"data/streaming_event_data/date=$datesGlob")

    adEvents.createOrReplaceTempView("ad_events")
    adEvents
  }

  def loadToDataLake(tableName: String, report: DataFrame): Unit = {
    report
      .repartition(1, col("date"))
      .write
      .mode("overwrite")
      .partitionBy("date")
      .parquet(s"data/historical_data/$tableName")
  }

  def prepareHistoricalAggregatedData(): Unit = {
    val adEvents = spark.read
      .option("basePath", "data/streaming_event_data")
      .json("data/streaming_event_data/date=2023-05-1[7-9]")
    adEvents.createOrReplaceTempView("ad_events")

    val audienceReach = spark.sql(
      """
        |SELECT date, campaign_id, theta_sketch_build(user_id) AS unique_users
        |FROM ad_events
        |GROUP BY date, campaign_id
        |""".stripMargin)

    val userEngagement = spark.sql(
      """
        |SELECT date, campaign_id, ad_id, theta_sketch_build(user_id) AS unique_users
        |FROM ad_events
        |WHERE click_id IS NOT NULL
        |GROUP BY date, campaign_id, ad_id
        |""".stripMargin)
  }

}
