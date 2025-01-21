package ru.configs

import org.apache.hadoop.conf.Configuration

/**
 * Конфигурация для работы с двумя кластерами Hadoop
 *
 * @param oneDmp   конфигурация OneDmp
 */
case class HadoopConfigs(oneDmp: HadoopClusterConfig)

/**
 * Конфигурация для hadoop cluster
 *
 * @param hdfs конфигурация hdfs
 */
case class HadoopClusterConfig(hdfs: HdfsConfig) {

  def asHadoopConfig: Configuration = {
    val hadoopConf = new Configuration()
    addTo(hadoopConf)
    hadoopConf
  }

  def addTo(initialConf: => Configuration): Unit = {
    hdfs match {
      case HighAvailabilityHdfsConfig(nameservice, namenodes) =>
        val nameService = initialConf.get("dfs.nameservices")
        if (nameService == null)
          initialConf.set("dfs.nameservices", nameservice)
        else if (!nameService.contains(nameservice))
          initialConf.set("dfs.nameservices", nameService + "," + nameservice)
        initialConf.set(s"dfs.client.failover.proxy.provider.$nameservice",
          "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
        initialConf.set(s"dfs.ha.namenodes.$nameservice", namenodes.keys.mkString(","))
        namenodes.foreach { case (nnName: String, nnAddress: String) =>
          initialConf.set(
            s"dfs.namenode.rpc-address.$nameservice.$nnName",
            nnAddress
          )
        }
      case SimpleHdfsConfig(_) => ()
    }
  }
}
