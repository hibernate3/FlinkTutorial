package com.sink

import com.api.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, DriverManager, PreparedStatement}

object JdbcSinkTest {
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

    dataStream.addSink(new MyJdbcSinkFunc())

    env.execute("jdbc sink test")
  }
}

class MyJdbcSinkFunc extends RichSinkFunction[SensorReading] {
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://10.101.40.216:3306/buried", "hdb_user", "hdb123456")

    insertStmt = conn.prepareStatement("insert into real_time_24hours values(?, ?)")
    updateStmt = conn.prepareStatement("update real_time_24hours set HOUR = ? where id = ?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context): Unit = {
    updateStmt.setString(1, "24")
    updateStmt.setInt(2, 1)
    updateStmt.execute()

    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setInt(1, 1)
      insertStmt.setString(2, "12")
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    if (updateStmt != null)
      updateStmt.close()

    if (insertStmt != null)
      insertStmt.close()

    if (conn != null)
      conn.close()
  }
}
