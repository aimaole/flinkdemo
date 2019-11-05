package com.maomao.wc

import org.apache.flink.streaming.api.scala._

/**
 * 流处理wordcount
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //接受一个socket文本流
    val dataStream = env.socketTextStream("localhost", 12345)

    //对数据进行处理与转换
    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print()

    //启动executer
    env.execute("StreamWordCount")
  }
}
