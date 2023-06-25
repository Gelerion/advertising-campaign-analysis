package com.gelerion.advertising.campaign.analysis

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.registrar.SketchFunctionsRegistrar

object CampaignAnalysisSketchesStrategy extends SparkJob {

  def main(args: Array[String]): Unit = {
    //register sketch functions to be available in SQL queries
    SketchFunctionsRegistrar.registerFunctions(spark)

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
        |SELECT date, campaign_id, theta_sketch_build(user_id) AS unique_users
        |FROM ad_events
        |GROUP BY date, campaign_id
        |""".stripMargin)
    audienceReach.createOrReplaceTempView("audience_reach")

    // 2. User engagement
    val userEngagement = spark.sql(
      """
        |SELECT date, campaign_id, ad_id, theta_sketch_build(user_id) AS unique_users
        |FROM ad_events
        |WHERE click_id IS NOT NULL
        |GROUP BY date, campaign_id, ad_id
        |""".stripMargin)
    userEngagement.createOrReplaceTempView("user_engagement")

    // [Optional] Show output
    showAggregatedReport()

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

  private def showAggregatedReport(): Unit = {
    // [Optional] Show output
    println("Audience reach report")
    spark.sql(
      """
        |SELECT date, campaign_id, theta_sketch_get_estimate(unique_users) as unique_users
        |FROM audience_reach
        |ORDER BY unique_users DESC
        |""".stripMargin)
      .show(10)
    println("=======================================================================")
    println("User engagement report")
    spark.sql(
      """
        |SELECT date, campaign_id, ad_id, theta_sketch_get_estimate(unique_users) as unique_users
        |FROM user_engagement
        |ORDER BY unique_users DESC
        |""".stripMargin)
      .show(10)
  }
}
