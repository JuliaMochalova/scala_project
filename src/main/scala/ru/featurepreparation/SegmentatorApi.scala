package ru.featurepreparation

import com.typesafe.scalalogging.Logger
import org.apache.commons.lang.time.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, regexp_replace}
import org.joda.time.DateTime
import ru.configs.exceptions.{BadRequestException, ParsingException}
import ru.model.{LalResult, LalSegment, SegmentatorSegment}
import scalaj.http.{Http, HttpResponse}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.json4s.{DefaultFormats, Extraction}
import ru.configs.DefaultConfigRepository
import ru.configs.clickhouse.CHConfig
import ru.utils.ClickHouseHelper

import java.util.Date
import scala.math.Ordered.orderingToOrdered

class SegmentatorApi(segmentatorUrl: String) extends ClickHouseHelper{
  override val clickHouseConfig: CHConfig = DefaultConfigRepository.clickHouseConfigs
  val logger: Logger = Logger("Segmentator Api Logger")
  implicit val formats = DefaultFormats

  private def getRequest(parameters: String):JValue = {
    val httpResponse: HttpResponse[String] = Http(segmentatorUrl + "v2/" + parameters).asString
    val response = if (httpResponse.code == 200) httpResponse.body
    else throw new BadRequestException("Bad HTTP response: code = " + httpResponse.code)
    try {
      parse(response).camelizeKeys
    } catch {
      case e: Exception =>
        throw new ParsingException(e.getMessage)
    }
  }

  def getNewSegments: List[LalSegment] = {
    logger.info(" Send Http Get Request (Start) ")
    val newSegments = getRequest("lal?status=NEW").extract[List[LalSegment]]

    val failedSegments = {
      val yesterday = DateUtils.addDays(new Date(), -1)
      getRequest("lal?status=FAILED").extract[List[LalSegment]]
        .filter { segment =>
        val segmentDate = new Date(segment.updatedAt.toLong * 1000)
        segmentDate > yesterday
      }
    }
    failedSegments++newSegments
  }

  def changeSegmentStatus(segment: LalSegment, lalResult: LalResult): Unit = {
    val putData = write(Extraction.decompose(lalResult).snakizeKeys)
    val httpResponse = Http(segmentatorUrl + "v2/" + s"lal/${segment.taskId}").put(f"$putData").execute()
    if (httpResponse.code == 200) {
      logger.info("Segment status changed")
    }
    else {
      throw new BadRequestException("Bad HTTP response: code = "+httpResponse.code)
    }
  }

  def checkStatus(segment: LalSegment, status: String): Boolean = {
    getRequest(f"lal/${segment.taskId}").extract[LalSegment].status == status
  }

  def writeSegment(segmentDf: DataFrame, segmentName: String): Unit = {
    val currentDate =  DateTime.now()

    val segment = segmentDf
      .select ("mac")
      .distinct()
      .withColumn("identifier_value", regexp_replace(col("mac"),":","-"))
      .withColumn("identifier_type", lit("mac"))
      .select("identifier_type", "identifier_value")
      .withColumn("timestamp", lit(currentDate.getMillis))
      .withColumn("segment_name", lit(segmentName))

    writeDf(segment, clickHouseConfig.tables.dmpSegmentsTable)

  }
  def loadSegment(segmentName: String): Int={
    val putData = s"""{
      "ch_segment_name":"$segmentName"
    }"""
    try {
      parse(response).camelizeKeys.extract[SegmentatorSegment].dmpId
    } catch {
      case e: Exception =>
        throw new ParsingException(e.getMessage)
    }
  }
}
