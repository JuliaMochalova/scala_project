package ru.configs

import pureconfig._
import pureconfig.generic.auto._
import ru.configs.clickhouse.CHConfig
import ru.configs.exceptions.ConfigParseException


/**
 * Трейт для разбора полей источника конфигурации
 *
 */
sealed trait ConfigRepository {

  val configSource: ConfigObjectSource

  lazy val hadoopConfigs: HadoopConfigs = loadConfig[HadoopConfigs]("hadoop")
  lazy val mlflowConfig: MlflowConfig = loadConfig[MlflowConfig]("mlflow-config")
  lazy val sparkConfig: SparkConfig = loadConfig[SparkConfig]("spark")
  lazy val segmentatorConfig: SegmentatorConfig = loadConfig[SegmentatorConfig]("segmentator-config")
  lazy val clickHouseConfigs: CHConfig = loadConfig[CHConfig]("clickhouse")

  def loadConfig[ConfigClass](namespace: String)(implicit reader: ConfigReader[ConfigClass]): ConfigClass = {
    configSource.at(namespace).load[ConfigClass] match {
      case Left(failures) => throw new ConfigParseException(failures.prettyPrint())
      case Right(config) => config
    }
  }

}


/**
 * Объект для чтения конфигурации по умолчанию
 * @see [[ConfigSource.default]]
 */
object DefaultConfigRepository extends ConfigRepository {
  override val configSource: ConfigObjectSource = ConfigSource.default
}
