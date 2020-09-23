package com.lw.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

object KafkaProducerUtil {

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
  def writeToKafka(topic: String): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)

    // 从文件读取数据，逐行写入kafka
    val bufferedSource: BufferedSource = io.Source.fromFile("/Users/xyj/developer/idea_prj/UserbehaviorAnalusis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")

    for( line <- bufferedSource.getLines() ){
      producer.send(new ProducerRecord[String, String](topic, line))
    }

    producer.close()
  }

}
