package com.yankee.spark.streaming.dstream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * @author Yankee
 * @date 2021/4/5 11:46
 */
object SparkStreaming02_DIY {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // TODO 业务逻辑
    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)
    messageDS.print()

    // TODO 启动任务
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 自定义数据采集器
   *  1.继承Receiver，定义泛型
   *  2.重写两个方法：onStart、onStop
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag: Boolean = true

    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flag) {
            val message: String = "采集的数据为：" + new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }
}
