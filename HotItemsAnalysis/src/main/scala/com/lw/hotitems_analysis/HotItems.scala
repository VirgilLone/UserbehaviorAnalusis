package com.lw.hotitems_analysis

import java.sql.Timestamp
import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1, Tuple2}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int,
                        behavior: String, timestamp: Long)

//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItems {

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


    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") //过滤pv数据
      .keyBy("itemId") //按照商品id分组
      .timeWindow(Time.hours(1), Time.minutes(5)) //设置滑动窗口进行统计
      //在窗口内先预聚合，等到该窗口关闭的时候再处理聚合结果
      .aggregate(new CountAgg(), new ItemViewCountWindowResult())

    val result: DataStream[String] = aggStream
      .keyBy("windowEnd")
      .process(new TopNHotItems(5))

//    dataStream.print("ori")
//    aggStream.print("agg")
    result.print()

    env.execute("hot_items")

  }

}

//自定义增量聚合函数，作预聚合，
//AggregateFunction聚合状态就是当前商品的count值为Long类型，OUT作为传给后面全窗口函数的IN，此时也为count值为Long
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  //每来一条数据调用一次add，则count值+1
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数WindowFunction
class ItemViewCountWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()

    out.collect(ItemViewCount(itemId, windowEnd, count))

  }
}

class TopNHotItems(N: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  lazy val itemViewCountListState: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", Types.of[ItemViewCount]))

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每来一条数据直接加入ListState
    itemViewCountListState.add(value)

    //注册一个到windowEnd+1触发的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //当定时器触发时做的处理，这里可以认为所有窗口的统计结果已经到齐，做排序输出就完事了
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    var iter: util.Iterator[ItemViewCount] = itemViewCountListState.get().iterator()
    while (iter.hasNext) {
      allItemViewCounts += iter.next()
    }

    itemViewCountListState.clear()

    //按照count大小排序
    val sortedItemViewCounts: ListBuffer[ItemViewCount] = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(N)

    // 将排名信息格式化成String，便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")

    // 遍历结果列表中的每个ItemViewCount，输出到一行
    for (i <- sortedItemViewCounts.indices) {
      val currentItemViewCount = sortedItemViewCounts(i)
      result.append("NO.").append(i + 1).append(":\t")
        .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
        .append("热度 = ").append(currentItemViewCount.count).append("\n")
    }

    result.append("\n==================================\n\n")

    Thread.sleep(1000)
    out.collect(result.toString())


  }
}

