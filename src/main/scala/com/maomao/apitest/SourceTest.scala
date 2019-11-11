package com.maomao.apitest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

//温度传感器读数样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //1、自定义集合
    val stream = env.fromCollection(List(
      SensorReading("123123", 1577718250, 38.123456)
    ))

    //2、从文件中读取
    val stream2 = env.readTextFile("F:\\flinkdemo\\src\\main\\resources\\test")

    val stream3 = env.fromElements(1, 2, 3, 5, 8, "sadda")

    stream3.print("stream3")
    env.execute()
  }
}
