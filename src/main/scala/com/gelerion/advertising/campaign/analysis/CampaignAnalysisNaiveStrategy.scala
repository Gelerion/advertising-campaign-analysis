package com.gelerion.advertising.campaign.analysis

object CampaignAnalysisNaiveStrategy extends SparkJob {

  def main(args: Array[String]): Unit = {
    // Read the streaming event data for the previous day
    val adEvents = CampaignAnalysis.readAdEvents("*")
    adEvents.show(4)
    println("=======================================================================")

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

    // Load to the data lake
    CampaignAnalysis.loadToDataLake("audience_reach", audienceReach)
    CampaignAnalysis.loadToDataLake("user_engagement", userEngagement)
  }
}
