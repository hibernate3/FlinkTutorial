package com.sink

import com.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

object FileSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //读取数据
    val inputPath = "/Users/hdb-dsj-003/Documents/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    //简单转换操作
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("/Users/hdb-dsj-003/Documents/IdeaProjects/FlinkTutorial/src/main/resources/out.txt"),
        new SimpleStringEncoder[SensorReading]()).build()).setParallelism(1)

    env.execute("flink sink test")
  }
}
