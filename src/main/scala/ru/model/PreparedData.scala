package ru.model

import org.apache.spark.sql.DataFrame

case class PreparedData(oldSegment: DataFrame, trainDf: DataFrame, predictionDf:DataFrame)
