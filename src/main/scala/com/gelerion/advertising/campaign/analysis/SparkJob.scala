package com.gelerion.advertising.campaign.analysis

import java.lang.Math.{max, min}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkJob {
  val nCores: Int = Runtime.getRuntime.availableProcessors

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("usage statistics test")
    .master(s"local[${min(max(nCores - 2, 1), 4)}]") //use up to 4 cores
    .config("spark.network.timeout", "10000001") //for debugging, don't use in prod setup
    .config("spark.executor.heartbeatInterval", "10000000") //for debugging, don't use in prod setup
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()

}
