package com.lw.monitoring

import java.net.URL
import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.lw.monitoring.model.{KUser, Scala_KTrade, Scala_KUser}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import scala.util.Random

object FlinkMetricJob {

  // 解析配置文件
  val url: URL = getClass.getResource("/job.properties")
  val config = new FlinkConfig(url.getPath)

  val kafkaProperties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", config.getKafkaBootStrapServers)
  kafkaProperties.setProperty("group.id", config.getKafkaGroupid)
  kafkaProperties.setProperty("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.setProperty("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  kafkaProperties.setProperty("auto.offset.reset", "earliest")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)

    /*//jobManager给source任务触发checkpoint的时间间隔
    env.enableCheckpointing(1000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //前一个checkpoint的尾和后一个checkpoint的头最小间隔时间（会使MaxConcurrentCheckpoints配置失效）
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    //更偏向使用checkpoint做故障恢复（即使savepoint更近）
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    //设置可容忍3次checkpoint
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)*/


    val kafkaStream_user : DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](config.getKafkaTopicUser, new SimpleStringSchema(), kafkaProperties))
    val kafkaStream_trade: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](config.getKafkaTopicTrade, new SimpleStringSchema(), kafkaProperties))

    analyze(env,kafkaStream_user,kafkaStream_trade)

    env.execute("realtime-monitor")

  }

  def analyze(env: StreamExecutionEnvironment,kafkaStream_user : DataStream[String],kafkaStream_trade: DataStream[String]):Unit ={
    // 处理注册用户指标数据，并写回kafka
    // 数据形式：(MAccRegisterUserCntPerCity，上海，234)、(MAccRegisterUserCntPerCity，北京，133)。。。
    val city_userid: DataStream[(String, Double)] = kafkaStream_user.map(data => {
      //      new KUser(data) // java写法
      Scala_KUser(data) //scala写法
    })
      .filter(_.dmlType == "INSERT")
      .map(e => (e.city, 1d))
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
    register_user_metricsData.print("register_user-->")

    // --------------------------------------
    // 处理订单GMV数据，计算累计成交额，并写回kafka
    val tradeStream: DataStream[Scala_KTrade] = kafkaStream_trade.map(data => {
      Scala_KTrade(data)
    })
      .filter(_.dmlType == "INSERT")

    val GMV_metricsData: DataStream[String] = tradeStream
      .map(e => ("全站", e.totalPrice))
//      .map(e=>(Random.nextString(10), e.totalPrice)) // key打散导致无法聚合累加
      .keyBy(_._1)
      .map(new AccumulateFunc())
      .map(e => ("MAccTradePrice", "全站成交额", e._2.toString).toString())
    addSink(GMV_metricsData)
    GMV_metricsData.print("GMV-->")
    tradeStream.map(e => ("全站", e.totalPrice))



    val lastTenSec_GMV_metricsData: DataStream[String] = tradeStream
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Scala_KTrade](Time.seconds(1)) {
        override def extractTimestamp(element: Scala_KTrade): Long = {
          val milli: Long = LocalDateTime.parse(element.orderTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            .toInstant(ZoneOffset.of("+8")).toEpochMilli
//          println(milli)
          milli
          //          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(element.orderTime).getTime
        }
      })
      .map(e => ("过去10s", e.totalPrice))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce((a, b) => (a._1, a._2 + b._2))
      .map(e=>("lastTenSec_GMV",e._1,e._2).toString())
    addSink(lastTenSec_GMV_metricsData)
    lastTenSec_GMV_metricsData.print("10s内的GMV-->")




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
class AccumulateFunc extends RichMapFunction[ (String,Double),(String,Double)]{
  var accSumState: ValueState[Double] = _
  override def open(parameters: Configuration): Unit = {
    accSumState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))
  }

  override def map(value: (String, Double)): (String, Double) = {
    val afterAcc: Double = accSumState.value() + value._2
    accSumState.update(afterAcc)
    (value._1,afterAcc)
  }
}

/*class UnifyOutputMetrics(metricsName: String) extends RichMapFunction[(String,Long),(String,String,String)] {
  override def map(value: (String, Long)): (String, String, String) = {
    (metricsName,value._1.toString,value._2.toString)
  }
}*/

