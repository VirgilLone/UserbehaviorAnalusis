package com.lw.hotitems_analysis

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object HotItemsWithSql {
  def main(args: Array[String]): Unit = {
    //初始化flink streaming环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局的并行度
    env.setParallelism(1)
    //设置时间语义为event时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //source
//    val inputStream: DataStream[String] = env.readTextFile("/Users/xyj/developer/idea_prj/UserbehaviorAnalusis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "flink-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest")
    val inputStream: DataStream[String] =env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))


    val dataStream: DataStream[UserBehavior] = inputStream.map(e => {
      val arr: Array[String] = e.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 定义表执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 基于DataStream创建Table
    val dataTable = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    // 1.1 Table API进行开窗聚合统计
    val aggTable = dataTable
      .filter('behavior === "pv")
      .window( Slide over 1.hours every 5.minutes on 'ts as 'sw )
      .groupBy( 'itemId, 'sw)
      .select( 'itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt )
    // 1.2 用SQL去实现TopN的选取
    tableEnv.createTemporaryView("aggtable", aggTable, 'itemId, 'windowEnd, 'cnt)
    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select
        |    *,
        |    row_number() over (partition by windowEnd order by cnt desc) as row_num
        |  from aggtable )
        |where row_num <= 5
      """.stripMargin)


    // 2. 纯SQL实现
    tableEnv.createTemporaryView("datatable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select
        |    *,
        |    row_number() over (partition by windowEnd order by cnt desc) as row_num
        |  from
        |       (select
        |                itemId,
        |                hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd,
        |                count(itemId) as cnt
        |              from datatable
        |              where behavior = 'pv'
        |              group by
        |                itemId,
        |                hop(ts, interval '5' minute, interval '1' hour))
        |     )
        |where row_num <= 5
        |
        |""".stripMargin)

    dataStream.print()
    resultSqlTable.toRetractStream[Row].print()

    env.execute("hot_items with sql")

  }

}
