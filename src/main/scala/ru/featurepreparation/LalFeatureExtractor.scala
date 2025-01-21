package ru.featurepreparation

import breeze.linalg.min
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{concat, explode, lit, regexp_replace, sha2, sum, udf}
import org.joda.time.DateTime
import ru.configs.DefaultConfigRepository
import ru.configs.clickhouse.CHConfig
import ru.model.{DMPAvroCJProfile, PreparedData}
import ru.utils.ClickHouseHelper

class LalFeatureExtractor(spark: SparkSession) extends ClickHouseHelper{
  override val clickHouseConfig: CHConfig = DefaultConfigRepository.clickHouseConfigs
  val mac2gidTable: String = clickHouseConfig.tables.mac2gidTable
  val interestsTable: String = clickHouseConfig.tables.interestsTable
  val logger: Logger = Logger("LAL feature extractor Logger")

  val fullProfilesTable: String = clickHouseConfig.tables.oldProfileTable
  val fullProfilesDf = read(spark, s"select * from $fullProfilesTable", Seq("mac"))
  val preparedProfileDf = prepareProfile(fullProfilesDf)
  preparedProfileDf.cache()
  import spark.implicits._

  def getLalDf(segmentId: String): (PreparedData, Long) ={
    val dmpSegment = readDmpSegment(segmentId)
    val dmpSegmentCount = dmpSegment.count()
    dmpSegment.cache()
    logger.info(f"dmpSegmentLength $dmpSegmentCount")
    val featuresDf = getFeaturesDf(dmpSegment, preparedProfileDf)
    featuresDf.cache()
    val positiveSamplesDf = featuresDf
      .filter($"label"===1)

    positiveSamplesDf
    .cache()
    val posLength = positiveSamplesDf.count()
    logger.info(f"positive $posLength")
    val magnificFactor = 6
    val testDf = featuresDf
      .filter($"label"===0)
      .cache()
    val testDfSize = testDf.count().toDouble
    val negativeSamplesDf = testDf
      .sample(min(Seq(magnificFactor * posLength / testDfSize, 0.5)))

    negativeSamplesDf.persist()
    logger.info(f"negative ${negativeSamplesDf.count()}")
    (PreparedData(positiveSamplesDf, negativeSamplesDf
      .union(positiveSamplesDf), testDf), dmpSegmentCount)
  }

  private def readDmpSegment(segmentId: String): DataFrame ={
    val dmpHdfsUrl: String = DefaultConfigRepository.hadoopConfigs.oneDmp.hdfs.url
    val dmpPath: String = ConfigFactory.load().getString("dmp-path")
    val dmpSegmentPath = s"$dmpHdfsUrl/$dmpPath/segment=$segmentId"

    import spark.implicits._

    spark.read.format("com.databricks.spark.avro")
        .load(dmpSegmentPath)
        .as[DMPAvroCJProfile]
        .map(record => {
        record.gid
          }).filter(_ != null)
      .toDF("gid")
      .distinct()
      .withColumn("label", lit(1))
  }

  private def getGid2MachashDf(gidSegments: DataFrame): DataFrame = {
    read(spark, s"select * from $mac2gidTable", Seq("gid"))
      .join(gidSegments, Seq("gid"), "left")
      .select($"gid", $"label", $"machash")
      .distinct()
  }

  private def readInterests = {
    val currentDate = DateTime.now().minusDays(30).toString().split("T")(0)

    read(spark, s"""
    |SELECT mac, int_id, weight, partition_date
    |FROM $interestsTable where partition_date > '$currentDate'
    |""".stripMargin, Seq("partition_date"))
      .groupBy("mac")
      .pivot("int_id")
      .agg(sum("weight"))
  }

  private def getFeaturesDf(dmpSegment: DataFrame, preparedProfiles: DataFrame): DataFrame ={
    val macSalt: String = ConfigFactory.load().getString("mac-salt")
    val gid2macDf = getGid2MachashDf(dmpSegment)
    preparedProfiles
      .withColumn("machash", sha2(concat(regexp_replace($"mac", ":","-"), lit(macSalt)),256))
      .join(gid2macDf, "machash")
      .join(readInterests, Seq("mac"), "left")
      .na.fill(0)
  }

  def prepareProfile(fullProfiles: DataFrame): DataFrame = {

    val groupDf = fullProfiles
      .withColumn("value", lit(1))
      .groupBy("mac")

    groupDf.cache()
    val newColName = "mac" +: (1 until groupDf.columns.length).map(e => "c_" + e.toString)
    groupDf
      .toDF(newColName:_*)
      .na.fill(-1)
  }
}
