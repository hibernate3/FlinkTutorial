package com.api

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.util.Random

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    )

    val stream1 = env.fromCollection(dataList)
    //    stream1.print()

    val inputPath = "/Users/hdb-dsj-003/Documents/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt"
    val stream2 = env.readTextFile(inputPath)
    //    stream2.print()

    //kafka
    System.setProperty("java.security.krb5.conf", "/Users/hdb-dsj-003/Documents/IdeaProjects/FlinkTutorial/kafka_kb_conf/krb5.conf")
    System.setProperty("java.security.auth.login.config", "/Users/hdb-dsj-003/Documents/IdeaProjects/FlinkTutorial/kafka_kb_conf/jaas.conf")
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")

    val properties = new Properties()
    properties.put("bootstrap.servers", "bigdata-dev-kafka-01:9092,bigdata-dev-kafka-02:9092,bigdata-dev-kafka-03:9092");
    properties.put("group.id", "kb_test");
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("session.timeout.ms", "30000");
    properties.put("auto.offset.reset", "earliest");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("security.protocol", "SASL_PLAINTEXT");
    properties.put("sasl.kerberos.service.name", "kafka");

    val stream3 = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))
    //    stream3.print()

    //自定义source
    val stream4 = env.addSource(new MySensorSource())
    stream4.print()

    env.execute("source test")
  }
}

class MySensorSource extends SourceFunction[SensorReading] {
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    var curTemp = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))

    while (running) {
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )

      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, curTime, data._2))
      )

      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
