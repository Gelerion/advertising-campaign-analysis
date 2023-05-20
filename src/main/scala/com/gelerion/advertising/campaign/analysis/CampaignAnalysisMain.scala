package com.gelerion.advertising.campaign.analysis

import org.apache.spark.sql.functions.{col, countDistinct, from_unixtime, to_date}

object CampaignAnalysisMain extends SparkJob {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    // Read the streaming event data for the previous day
    val adEvents = spark.read.json("data/ad_events/date=*")
    adEvents.show(4)

    // Compute the daily aggregates: user engagement
    adEvents
      .groupBy($"ad_id", $"campaign_id", to_date(from_unixtime(col("timestamp"))).as("date"))
      .agg(
        countDistinct($"user_id").as("unique_users")
      )
      .show(4)

    // Compute the daily aggregates: audience reach
//    adEvents.groupBy($"date", $"campaign_id").agg(countDistinct($"user_id"))

    // Compute the daily aggregates: user engagement
//    adEvents.where($"click_id".isNotNull).groupBy($"date", $"ad_id", $"campaign_id").agg(countDistinct($"user_id"))

//    adEvents.select($"user_id").distinct().count()

    // 2. Evaluating user engagement


//    val dailyAggregates = adEvents
//      .groupBy($"campaign_id", $"ad_id", $"event_type")
//      .count()




  }

}
