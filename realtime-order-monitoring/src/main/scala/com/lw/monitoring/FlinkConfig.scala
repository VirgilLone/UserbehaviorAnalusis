package com.lw.monitoring

import java.io.{File, FileInputStream}
import java.util.Properties

class FlinkConfig() {

  private val properties = new Properties

  def this(fileName: String) {
    this()

    val file = new File(fileName)
    try {
      val fis = new FileInputStream(file)
      this.properties.load(fis)
      fis.close()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

  def getKafkaBootStrapServers: String = properties.getProperty("kafka.bootstrap.servers")

  def getKafkaGroupid: String =properties.getProperty("kafka.topic.groupid")

  def getKafkaTopicMetric: String = properties.getProperty("kafka.topic.metric")

  def getKafkaTopicUser: String = properties.getProperty("kafka.topic.user")

  def getKafkaTopicTrade: String = properties.getProperty("kafka.topic.trade")

  def getSinkName: String = properties.getProperty("sink.name")

  def getHBaseZKQuorum: String = properties.getProperty("hbase.zookeeper.quorum")

  def getHBaeZKPort: String = properties.getProperty("hbase.zookeeper.property.clientPort")

  def getHBaseZKParent: String = properties.getProperty("hbase.zookeeper.znode.parent")

  def getHBaseMetricTableName: String = properties.getProperty("hbase.table.metric")


}
