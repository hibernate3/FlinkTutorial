package com.api

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取数据
    val inputPath = "/Users/hdb-dsj-003/Documents/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    //简单转换操作
    val dataStream: DataStream[SensorReading] = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    val resultStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
//      .window(TumblingEventTimeWindows.of(Time.seconds(15))) //滚动时间窗口
//      .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3))) //滑动时间窗口
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10))) //会话时间窗口
//      .countWindow(10) //滚动计数窗口
//      .countWindow(10, 30) //滑动计数窗口

    env.execute()
  }
}
