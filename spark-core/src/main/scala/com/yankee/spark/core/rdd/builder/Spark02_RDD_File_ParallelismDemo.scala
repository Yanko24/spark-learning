package com.yankee.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/4 19:58
 */
object Spark02_RDD_File_ParallelismDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 创建RDD
    // TODO 分区数据的分配
    // 1.数据以行为单位进行读取
    //   Spark读取文件，采用的是hadoop的方式读取，所以一行一行的读取数据，和字节数没有任何关系
    // 2.数据读取时是以偏移量为单位的
    // 3.数据分区的偏移量范围的计算，偏移量不会被重复读取
    val data: RDD[String] = sc.textFile("data/num.txt", 2)
    data.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }
}
