package com.maomao.apitest

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase}

import scala.util.Random

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
    val stream2 = env.readTextFile("src/main/resources/test")

    val stream3 = env.fromElements(1, 2, 3, 5, 8, "sadda")


    //3、从kafka读数据

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "group1")
    //    props.put("enable.auto.commit", "true")
    //    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    val stream4 = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), props))
    /**
     * bin/kafka-topics.sh --list --zookeeper localhost:2181
     *
     * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
     *
     * bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic test
     *
     * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
     *
     * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
     *
     *
     */
    //4、自定义
    val stream5 = env.addSource(new TestSourceMM())
    stream5.print("stream5")

    env.execute()
  }
}

class TestSourceMM() extends SourceFunction[SensorReading] {
  //定义一个flag，表示数据原是否正常运行
  var running: Boolean = true

  //正常生成
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数发生器
    val random = new Random()

    //初始化定义一组传感器数据
    var curTemp = 1.to(10).map(
      i => ("sensor_" + i, 60 + random.nextGaussian() * 20)
    )
    //产生数据流
    while (running) {
      //更新当前温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + random.nextGaussian())
      )
      //时间戳
      val time = System.currentTimeMillis()
      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1, time, t._2))
      )
      //设置时间间隔
      Thread.sleep(500)
    }
  }

  //取消数据源的生成
  override def cancel(): Unit = {
    running = false
  }
}
