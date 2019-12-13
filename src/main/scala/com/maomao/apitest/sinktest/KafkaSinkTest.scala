package com.maomao.apitest.sinktest

import java.util.Properties

import com.maomao.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010, FlinkKafkaProducer011}

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    val inputStream = env.readTextFile("/home/mao/study/flinkdemo/src/main/resources/test")
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "group1")
    //    props.put("enable.auto.commit", "true")
    //    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    val stream4 = env.addSource(new FlinkKafkaConsumer010[String]("input", new SimpleStringSchema(), props))


    //1 、基本转换算子与简单聚合算子
    val dataStream = stream4.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble).toString
    })


    dataStream.addSink(new FlinkKafkaProducer010[String]("localhost:9092", "output", new SimpleStringSchema()))

    env.execute("KafkaSinkTest")
  }

}
