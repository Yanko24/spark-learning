package com.yankee.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/4 20:10
 */
object Spark03_RDD_File_ParallelismDemo2 {
  def main(args: Array[String]): Unit = {
    // TODO 设置访问HDFS的用户
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 创建RDD
    // 14个字节 2个分区 14 / 2 = 7
    // 14 / 7 = 2个分区
    // 数据分布：[0, 7] [7, 14]的偏移量

    // 如果数据源是多个文件，那么计算分区时以文件为单位进行分区
    val data: RDD[String] = sc.textFile("data/word.txt", 2)
    data.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }
}
