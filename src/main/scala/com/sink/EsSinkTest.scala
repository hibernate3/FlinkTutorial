package com.sink

import com.api.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

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

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("", 9200))

    val myEsSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("id", t.id)
        dataSource.put("temperature", t.temperature.toString)
        dataSource.put("timestamp", t.timestamp.toString)

        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .source(dataSource)

        requestIndexer.add(indexRequest)
      }
    }

    dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts, myEsSinkFunc).build())

    env.execute("es sink test")
  }
}
