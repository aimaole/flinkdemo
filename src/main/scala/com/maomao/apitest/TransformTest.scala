package com.maomao.apitest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val value = env.readTextFile("src/main/resources/test")

    //1 、基本转换算子与简单聚合算子
    val dataStream = value.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    })

    val aggStream = dataStream.keyBy(0)
      .reduce((x, y) => SensorReading(y.id, x.timestamp * 100, y.temperature + 1))
    //      .sum(2)
    aggStream.print()


    //2、多流转换算子
    val splitStream = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })
    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("low", "high")

    high.print("high")
    low.print("low")
    all.print("all")

    //合并两条流
    val warning = high.map(data => (data.id, data.temperature))
    val connString = warning.connect(low)
    val redataStream = connString.map(
      warningData => (warningData._1, warningData._2, "warning"), data => (data.id, "healthy")
    )
    redataStream.print("redata")



    val unionStream = high.union(low )

    unionStream.print("union")


    env.execute("transfromTest")
  }

}
