package com.gelerion.advertising.campaign.analysis

import org.scalacheck.Gen

import java.time.LocalDateTime
import java.time.ZoneOffset.UTC

object AdEventsDataGenerator {

  def generate(howMany: Int, betweenDates: BetweenDates): Seq[AdEvent] = {
    val adIdGen: Gen[Int] = Gen.chooseNum(1, 40)
    val userIdGen: Gen[Int] = Gen.chooseNum(1, 10000)
    val campaignIdGen: Gen[Int] = Gen.chooseNum(1, 10)
    val clickIdGen: Gen[Option[Int]] = Gen.option(Gen.chooseNum(1, 100000))

    val adEventGen: Gen[AdEvent] = for {
      adId <- adIdGen
      userId <- userIdGen
      campaignId <- campaignIdGen
      clickId <- clickIdGen
      timestamp <- timestampGen(betweenDates)
    } yield {
      AdEvent(adId, userId, campaignId, clickId.map(_.toString), timestamp)
    }

    Gen.listOfN(howMany, adEventGen).sample.get
  }

  private def timestampGen(between: BetweenDates): Gen[Long] = {
    Gen.choose(between.start, between.end).map(_.toEpochSecond(UTC))
  }
}

case class BetweenDates(start: LocalDateTime, end: LocalDateTime)
case class AdEvent(ad_id: Int, user_id: Int, campaign_id: Int, click_id: Option[String], timestamp: Long)