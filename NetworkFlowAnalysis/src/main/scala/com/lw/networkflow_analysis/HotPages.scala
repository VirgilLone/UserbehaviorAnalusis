package com.lw.networkflow_analysis

import java.net.URL
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//输入数据的样例类
case class ApacheLogEvent(id: String, userId: String, timestamp: Long,
                          method: String, url: String)

case class PageViewCount(url: String, windowEnd: Long, count: Long)

/**
 * 通过tomcat或nginx日志统计实时热门页面
 */
object HotPages {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/apache.log")
    val inputStream = env.readTextFile(resource.getPath)
//    val inputStream = env.socketTextStream("localhost", 7777)

    val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")

    val dataStream: DataStream[ApacheLogEvent] = inputStream
      .map(d => {
        val arr = d.split(" ")
        val ts = simpleDateFormat.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent) = element.timestamp
      })

    //根据访问的url进行分组，进行开窗聚合
    val aggStream: DataStream[PageViewCount] = dataStream
      .filter(_.method == "GET")
      .filter(data => {
        val pattern = "^((?!\\.(css|js|ico)$).)*$".r()
        (pattern findFirstIn data.url).nonEmpty
      })
      //      .keyBy("url")
      .keyBy(_.url)
      .timeWindow(Time.minutes(5), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late_data"))
      .aggregate(new PageCountAgg(), new PageViewCountWindowResult())

    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPages(3))

//    dataStream.print("data")
//    aggStream.print("agg")
    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late_data")).print("late_data")
    resultStream.print()
    env.execute("hot pages job")

  }

}

class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
  }
}


class TopNHotPages(N: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

  //  private var pageViewCountListState: ListState[PageViewCount] = _
  private var pageViewCountMapState: MapState[String, Long] = _

  override def open(parameters: Configuration): Unit = {
    //    pageViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))
    pageViewCountMapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String],classOf[Long]))
  }

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    //    pageViewCountListState.add(value)
    pageViewCountMapState.put(value.url, value.count)

    ctx.timerService().registerEventTimeTimer(value.windowEnd)

    // 再注册一个定时器，1分钟之后触发，这时窗口已经彻底关闭，不会再有聚合结果输出，此时可以清空状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    /*val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
    val iter = pageViewCountListState.get().iterator()
    while (iter.hasNext)
      allPageViewCounts += iter.next()*/

    // 判断定时器触发时间，如果已经是窗口结束时间1分钟之后，那么直接清空状态
    // KeyedProcessFunction就是根据windowEnd分组的，当前key(ctx.getCurrentKey)就是windowEnd
    if (timestamp == ctx.getCurrentKey + 60000L) {
      pageViewCountMapState.clear()
      return
    }

    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
    val iter = pageViewCountMapState.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))
    }

    //    // 提前清空状态
    //    pageViewCountListState.clear()

    // 按照访问量排序并输出top N
    val sortedPageViewCounts: ListBuffer[(String, Long)] = allPageViewCounts.sortWith(_._2 > _._2).take(N)

    // 将排名信息格式化成String，便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp)).append("\n")

    // 遍历结果列表中的每个ItemViewCount，输出到一行
    for (i <- sortedPageViewCounts.indices) {
      val currentItemViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("页面URL = ").append(currentItemViewCount._1).append("\t")
        .append("热门度 = ").append(currentItemViewCount._2).append("\n")
    }

    result.append("\n==================================\n\n")

    Thread.sleep(1000)
    out.collect(result.toString())
  }

}
