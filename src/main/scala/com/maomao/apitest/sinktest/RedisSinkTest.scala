package com.maomao.apitest.sinktest

import com.maomao.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.readTextFile("src/main/resources/test")
    //1 、基本转换算子与简单聚合算子
    val dataStream = inputStream.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    })

    val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()


    dataStream.addSink(new RedisSink(conf,new MyRedisMapper))

    env.execute("RedisSinkTest")
  }

}

class MyRedisMapper() extends RedisMapper[SensorReading] {

  //定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    //把传感器id和温度保存成哈希表 HSET key field value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperatyre")
  }

  //定义保存到redis的value
  override def getKeyFromData(t: SensorReading): String = t.temperature.toString

  //定义保存到redis的key
  override def getValueFromData(t: SensorReading): String = t.id
}
