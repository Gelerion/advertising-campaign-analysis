package com.gelerion.advertising.campaign.analysis.reports

import com.gelerion.advertising.campaign.analysis.SparkJob
import org.apache.spark.sql.registrar.SketchFunctionsRegistrar

/**
 * Evaluating user engagement: By calculating the count of distinct users who clicked on the ads, the agency can
 * analyze the effectiveness of the campaign in terms of user engagement.
 */
object UserEngagementReport extends SparkJob {
  SketchFunctionsRegistrar.registerFunctions(spark)

  def main(args: Array[String]): Unit = {
    val userEngagement = spark.read
      .option("basePath", "data/historical_data/user_engagement")
      .parquet("data/historical_data/user_engagement/date=*")
    userEngagement.createOrReplaceTempView("user_engagement")

    val campaignIdToAnalyze = 1
    println("User engagement report: The user engagement for each ad per day")
    spark.sql(
      s"""
         |SELECT date, campaign_id, ad_id, theta_sketch_get_estimate(unique_users) as unique_users
         |FROM user_engagement
         |WHERE campaign_id = $campaignIdToAnalyze
         |ORDER BY date, ad_id, unique_users DESC
         |""".stripMargin)
      .show(10)

    println("User engagement report: Top two ads with the highest user engagement per day")
    spark.sql(
      s"""
         |WITH ordered_user_engagement as (
         |  SELECT date, campaign_id, ad_id, theta_sketch_get_estimate(unique_users) as unique_users,
         |         RANK() OVER (PARTITION BY date ORDER BY theta_sketch_get_estimate(unique_users) DESC) as topN
         |  FROM user_engagement
         |  WHERE campaign_id = $campaignIdToAnalyze
         |)
         |SELECT date, campaign_id, ad_id, unique_users
         |FROM ordered_user_engagement
         |WHERE topN <= 2
         |ORDER BY date, ad_id, unique_users DESC
         |""".stripMargin)
      .show(10)

    println("User engagement report: The daily trend for user engagement in the campaign.")
    spark.sql(
      s"""
         |SELECT date, campaign_id, theta_sketch_get_estimate(theta_sketch_merge(unique_users)) as unique_users
         |FROM user_engagement
         |WHERE campaign_id = $campaignIdToAnalyze
         |GROUP BY date, campaign_id
         |ORDER BY date, unique_users DESC
         |""".stripMargin)
      .show(10)
  }
}
