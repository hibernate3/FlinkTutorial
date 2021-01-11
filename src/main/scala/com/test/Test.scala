package com.test

import com.api.SensorReading
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import java.util.Properties

object Test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = env.readTextFile("")

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "localhost:9092")

    val myProducer = new FlinkKafkaProducer[String](
      "my-topic",               // 目标 topic
      new SimpleStringSchema(), // 序列化 schema
      properties) // 容错

    stream.addSink(myProducer)

    stream.addSink(new FlinkKafkaProducer[String]("", new SimpleStringSchema(), properties))
  }

}
