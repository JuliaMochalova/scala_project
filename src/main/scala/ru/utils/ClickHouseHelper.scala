package ru.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.clickhouse.model.HashTypes.{cityHash64, sipHash64}
import ru.clickhouse.{ClickhouseReader, ClickhouseWriter}
import ru.clickhouse.model.Query
import ru.clickhouse.util.ClickhouseUtils
import ru.configs.clickhouse.CHConfig

import java.util.Properties

trait ClickHouseHelper {

  val clickHouseConfig: CHConfig
  val clickHouseProperties = new Properties()

  def read(spark: SparkSession, query: String, predicateColumns: Seq[String]): DataFrame= {
    val clickhouseReader = ClickhouseReader(spark, clickHouseConfig.config)
    val parallelism = clickHouseConfig.config.numPartitions
    val fetchSize = clickHouseConfig.config.batchSize.get

    val predicates = ClickhouseUtils
      .getPredicatesForParallelReading(
        parallelism,
        predicateColumns,
        cityHash64)

    val readOptions = Map(
      "fetchSize" -> fetchSize.toString,
      "numPartitions" -> parallelism.toString
    )
    clickhouseReader
      .readWithPredicates(Query(query).prepare, predicates, readOptions = readOptions)
  }

  def writeDf(df: DataFrame, table: String) {
    val clickhouseWriter = ClickhouseWriter(clickHouseConfig.config)
    val saveMode = SaveMode.Append

    clickhouseWriter.write(
      table,
      df,
      saveMode
    )
  }
}
