package com.maomao.wc

import org.apache.flink.api.scala._

/**
 * 批处理wordcount
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //从文件读数据
    val inputPath = "D:\\study\\flinkdemo\\src\\main\\resources\\test";
    val inputDataSet = env.readTextFile(inputPath)
    //wordcount
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    wordCountDataSet.print()

  }
}
