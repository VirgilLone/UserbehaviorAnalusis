import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
 * LoginFail版本中的一些逻辑错误：
 * 1。必须等到2秒后定时器触发才能捕捉到异常登录。如果前0.5秒内都已经连续登录失败很多次了，此时应该立即报警而不是还要等到2秒后再触发报警
 * 2。如果前1秒登录失败了很多次，在1.5秒的时候却成功了，定时器就会被清除，就会被认为在2秒内是正常的，而这肯定是不合理的
 *
 * 此版本就是数据来一条就处理一条，不用定时器了，增加了实效性
 */
object LoginFailAdvance {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val resource = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    // 转换成样例类类型，并提起时间戳和watermark
    val loginEventStream = inputStream
      .map( data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      })

    // 进行判断和检测，如果2秒之内连续登录失败，输出报警信息
    val loginFailWarningStream = loginEventStream
      .keyBy(_.userId)
      .process( new LoginFailWaringAdvanceResult() )

    loginFailWarningStream.print()
    env.execute("login fail detect job")
  }
}

class LoginFailWaringAdvanceResult() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{
  // 定义状态，保存当前所有的登录失败事件
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 首先判断事件类型
    if( value.eventType == "fail" ){
      // 1. 如果是失败，进一步做判断
      val iter = loginFailListState.get().iterator()
      // 判断之前是否有登录失败事件
      if(iter.hasNext){
        // 1.1 如果有，那么判断两次失败的时间差
        val firstFailEvent = iter.next()
        if( value.timestamp-firstFailEvent.timestamp <  2 ){
          // 如果在2秒之内，输出报警
          out.collect(LoginFailWarning(value.userId, firstFailEvent.timestamp, value.timestamp, "login fail 2 times in 2s"))
        }
        // 不管报不报警，当前都已处理完毕，将状态更新为最近依次登录失败的事件
        loginFailListState.clear()
        loginFailListState.add(value)
      } else {
        // 1.2 如果没有，直接把当前事件添加到ListState中
        loginFailListState.add(value)
      }
    } else {
      // 2. 如果是成功，直接清空状态
      loginFailListState.clear()
    }
  }
}