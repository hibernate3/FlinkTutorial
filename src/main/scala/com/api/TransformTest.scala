package com.api

import org.apache.flink.api.common.functions.{FilterFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

object TransformTest {
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

    //分组聚合，输出每个传感器当前最小值
    val aggStream = dataStream
      .keyBy("id")
      .minBy("temperature")

    //需要输出当前最小的温度值以及最近的时间戳
    val resultStream = dataStream
      .keyBy("id")
      .reduce((curState, newData) => SensorReading(curState.id, newData.timestamp, curState.temperature.min(newData.temperature)))
    //    resultStream.print()

    //多流转换操作
    //分流，将传感器温度数据分成低温、高温两条流
    val highTag = OutputTag[SensorReading]("high")
    val lowTag = OutputTag[SensorReading]("low")
    val allTag = OutputTag[SensorReading]("all")

    val splitStream = dataStream.process(new ProcessFunction[SensorReading, SensorReading]() {
      override def processElement(value: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
        if (value.temperature > 30.0) {
          context.output(highTag, value);
        } else if (value.temperature <= 30.0) {
          context.output(lowTag, value);
        }

        context.output(allTag, value)
      }
    })

//    splitStream.getSideOutput(highTag).print("high")
//    splitStream.getSideOutput(lowTag).print("low")
//    splitStream.getSideOutput(allTag).print("all")

    //合流操作
    val warningStream = splitStream.getSideOutput(highTag).map(data => (data.id, data.temperature))
    val connectedStreams = warningStream.connect(splitStream.getSideOutput(lowTag))

    //用coMap对数据进行分别处理
    val coMapResultStream = connectedStreams
      .map(warningData => (warningData._1, warningData._2, "warning"),
        lowTempData => (lowTempData.id, "healthy"))

//    coMapResultStream.print("coMap")

    //union合流
    val unionStream = splitStream.getSideOutput(highTag).union(splitStream.getSideOutput(lowTag))

    //自定义函数类
    val myFilterStream: DataStream[SensorReading] = dataStream.filter(new MyFilter)
//    myFilterStream.print()

    env.execute("transform test")
  }
}

class MyReduceFunction extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id, value2.timestamp, value1.temperature.min(value2.temperature))
  }
}

class MyFilter extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}
