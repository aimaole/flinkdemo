package com.maomao.apitest.sinktest

import java.util

import com.maomao.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //source
    val inputStream = env.readTextFile("F:\\flinkdemo\\src\\main\\resources\\test")
    //1 、基本转换算子与简单聚合算子  transfrom
    val dataStream = inputStream.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    })

    //sink
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))
    //创建一个esSinkBuilder
    val esSink = new ElasticsearchSink.Builder[SensorReading](httpHosts, new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        println("saving data" + t)

        //包装成一个json或者Map
        val json = new util.HashMap[String, String]()
        json.put("id", t.id)
        json.put("temperature", t.temperature.toString)
        json.put("timestamp", t.timestamp.toString)
        //创建index request 准备发送数据
        val indexRequest = Requests.indexRequest().index("sensor").`type`("readingdata").source(json)
        //利用indexr发送请求  写入数据
        requestIndexer.add(indexRequest)
        println("data save success")
      }
    }).build()
    dataStream.addSink(esSink)

    env.execute("EsSinkTest")
  }

}
