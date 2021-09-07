package com.maomao.apitest.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}
import com.maomao.apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @Author maohongqi
 * @Date 2019/12/14 14:32
 * @Version 1.0
 **/
object JDBCSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //source
    val inputStream = env.readTextFile("src/main/resources/test")
    //1 、基本转换算子与简单聚合算子  transfrom
    val dataStream = inputStream.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    })

    //sink
    dataStream.addSink(new MyJDBCSink)
    env.execute("JDBCSinkTest")
  }
}

class MyJDBCSink() extends RichSinkFunction[SensorReading] {
  //定义sql连接  预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //初始化，创建连接与预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("insert into temperatures (sansor,temp) values (?,?)")
    updateStmt = conn.prepareStatement("update temperatures set temp = ? where sensor = ?")

  }

  //调用连接，执行sql
  override def invoke(value: SensorReading, context: Context): Unit = {
    //执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    //如果update没有更新，就插入新数据
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }

  }

  //关闭资源
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
