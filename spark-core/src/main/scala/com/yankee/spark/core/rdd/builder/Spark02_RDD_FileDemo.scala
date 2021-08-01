package com.yankee.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/1 19:20
 */
object Spark02_RDD_FileDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 创建RDD
    // textFile：以行为单位读取数据，读取的数据都是字符串
    // wholeTextFiles：以文件为单位读取数据
    // 读取的结果表示为元组，第一个元素表示文件路径，第二个元素表示文件内容
    val data: RDD[(String, String)] = sc.wholeTextFiles("data")
    data.collect.foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
