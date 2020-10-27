package com.lw.monitoring

import java.net.URL
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.lw.monitoring.model.{KUser, Scala_KUser}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object FlinkMetricJob {

  // 解析配置文件
  val url: URL = getClass.getResource("/job.properties")
  val config = new FlinkConfig(url.getPath)

  val kafkaProperties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", config.getKafkaBootStrapServers)
  kafkaProperties.setProperty("group.id", config.getKafkaGroupid)
  //    kafkaProperties.setProperty("key.deserializer",
  //      "org.apache.kafka.common.serialization.StringDeserializer")
  //    kafkaProperties.setProperty("value.deserializer",
  //      "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.setProperty("auto.offset.reset", "earliest")

  def main(args: Array[String]): Unit = {
    println("============>"+kafkaProperties)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.setParallelism(1)


    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](config.getKafkaTopicUser, new SimpleStringSchema(), kafkaProperties))

    val city_userid: DataStream[(String, Long)] = kafkaStream.map(data => {
      //      new KUser(data) // java写法
      Scala_KUser(data) //scala写法
    })
      .filter(_.dmlType == "UPDATE")
      .map(e => (e.city, 1L))
      .keyBy(_._1)
      .map(new AccumulateFunc())

//    kafkaStream.print("origin")
//    city_userid.print("city_userid")

//    city_userid.map(new UnifyOutputMetrics("MAccRegisterUserCntPerCity"))
    val register_user_metricsData: DataStream[String] =
      city_userid.map(e=>("MAccRegisterUserCntPerCity",e._1.toString,e._2.toString).toString())

//    val producer:FlinkKafkaProducer[String] = new FlinkKafkaProducer(config.getKafkaTopicMetric, new ProducerStringSerializationSchema(config.getKafkaTopicMetric), kafkaProperties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
//    register_user_metricsData.addSink(producer)

    addSink(register_user_metricsData)

    register_user_metricsData.print("metrics:")

    env.execute("realtime-monitor")

  }

  def addSink(out:DataStream[String]): Unit ={
    config.getSinkName match {
      case "kafka" =>
        val producer:FlinkKafkaProducer[String] = new FlinkKafkaProducer(config.getKafkaTopicMetric, new ProducerStringSerializationSchema(config.getKafkaTopicMetric), kafkaProperties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
        out.addSink(producer)
      case "hbase" => println("hbase not support at present！")
      case _=> println("sink destination error!!!")
    }

  }

}

/**
 * 输入依次：(shanghai,1)、(beijing,1)、(beijing,1)、(suzhou,1)、(beijing,1)...
 * 输出依次：(shanghai,1)、(beijing,1)、(beijing,2)、(suzhou,1)、(beijing,3)
 */
class AccumulateFunc extends RichMapFunction[ (String,Long),(String,Long)]{
  var accSumState: ValueState[Long] = _
  override def open(parameters: Configuration): Unit = {
    accSumState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastTemp", Types.of[Long]))
  }

  override def map(value: (String, Long)): (String, Long) = {
    val afterAcc: Long = accSumState.value() + value._2
    accSumState.update(afterAcc)
    (value._1,afterAcc)
  }
}

/*class UnifyOutputMetrics(metricsName: String) extends RichMapFunction[(String,Long),(String,String,String)] {
  override def map(value: (String, Long)): (String, String, String) = {
    (metricsName,value._1.toString,value._2.toString)
  }
}*/

