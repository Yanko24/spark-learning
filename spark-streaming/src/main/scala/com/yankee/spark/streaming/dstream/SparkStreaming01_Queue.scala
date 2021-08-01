package com.yankee.spark.streaming.dstream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author Yankee
 * @date 2021/4/5 11:46
 */
object SparkStreaming01_Queue {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // TODO 业务逻辑
    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()
    // 创建QueueInputDStream
    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)
    // 处理队列中的RDD数据
    val mappedStream: DStream[(Int, Int)] = inputStream.map((_, 1))
    val reducedStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)
    // 打印结果
    reducedStream.print()

    // TODO 启动任务
    ssc.start()

    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}
