package com.sink

import com.api.SensorReading
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, FlinkKafkaProducer011}

import java.util.Properties
import scala.sys.env

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //读取数据
    val inputPath = "/Users/hdb-dsj-003/Documents/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    System.setProperty("java.security.krb5.conf", "/Users/hdb-dsj-003/Documents/IdeaProjects/FlinkTutorial/kafka_kb_conf/krb5.conf")
    System.setProperty("java.security.auth.login.config", "/Users/hdb-dsj-003/Documents/IdeaProjects/FlinkTutorial/kafka_kb_conf/jaas.conf")
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")

    val properties = new Properties
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

    inputStream.addSink(new FlinkKafkaProducer[String]("test", new SimpleStringSchema(), properties))

    env.execute("kafka sink test")
  }
}
