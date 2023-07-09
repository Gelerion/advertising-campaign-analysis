package com.gelerion.advertising.campaign.analysis

import org.apache.spark.sql.registrar.SketchFunctionsRegistrar

object AnalyseHistoricalData extends SparkJob {

  def main(args: Array[String]): Unit = {
    //register sketch functions to be available in SQL queries
    SketchFunctionsRegistrar.registerFunctions(spark)

    val userEngagementHistorical = spark.read
      .option("basePath", "data/historical_data/user_engagement")
      .parquet("data/historical_data/user_engagement/date=*")

    userEngagementHistorical.createOrReplaceTempView("user_engagement_historical")

    spark.sql("desc user_engagement_historical").show(100, false)

//    userEngagementHistorical.printSchema()
  }
}
