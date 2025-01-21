package ru.utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ru.configs.{SparkConfig, HadoopConfigs}


trait SparkJob {
  val sparkConfig: SparkConfig

  val hadoopConfigs: HadoopConfigs

  val jobName: String

  def initSparkSession: SparkSession = {

    val builder = SparkSession
      .builder()
      .appName(jobName)
//      .enableHiveSupport()

    val sparkSession = builder.configureEnvironment()
    .getOrCreate()

  //  hadoopConfigs.oneDmp.addTo(sparkSession.sparkContext.hadoopConfiguration)
    sparkSession

  }
  private implicit class SparkBuilderExtension(builder: SparkSession.Builder) {
    def configureEnvironment(): SparkSession.Builder = {
      if (sparkConfig.debugMode)
        builder
          .master("local")
      else
        builder
    }
  }

}
