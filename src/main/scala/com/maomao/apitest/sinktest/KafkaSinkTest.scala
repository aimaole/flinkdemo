package com.maomao.apitest.sinktest

import com.maomao.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, FlinkKafkaProducer011}

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.readTextFile("/home/mao/study/flinkdemo/src/main/resources/test")

    //1 、基本转换算子与简单聚合算子
    val dataStream = inputStream.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble).toString
    })
    dataStream.addSink(new FlinkKafkaProducer010[String]("localhost:9092","test",new SimpleStringSchema()))

    env.execute("KafkaSinkTest")
  }

}
