package com.yankee.spark.streaming.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Yankee
 * @date 2021/4/4 18:19
 */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("HAOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    // StreamingContext创建时，需要传递两个参数
    //  第一个参数表示环境配置
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    //  第二个参数表示批量处理的周期（采集周期）
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // TODO 逻辑处理
    // 获取端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("master", 9999)
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()

    // TODO 关闭环境
    // 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
    // 如果main方法执行完毕，应用程序也会自动结束，所以不能让main执行完毕
    // ssc.stop()
    // 1.启动采集器
    ssc.start()
    // 2.等待采集器的关闭
    ssc.awaitTermination()
  }
}
