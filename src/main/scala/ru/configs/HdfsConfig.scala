package ru.configs

/**
 * Конфигурация HDFS
 */
sealed trait HdfsConfig {
  def url: String
}

/**
 * Конфигурация HDFS для HA режима
 *
 * @param nameService Название nameService
 * @param nameNodes   Map с адресами нод
 */
case class HighAvailabilityHdfsConfig(nameService: String,
                                      nameNodes: Map[String, String]) extends HdfsConfig {
  override def url: String = s"hdfs://$nameService"
}

/**
 * Конфигурация HDFS для Single Node режима
 *
 * @param url hdfs url
 */

case class SimpleHdfsConfig(url: String) extends HdfsConfig