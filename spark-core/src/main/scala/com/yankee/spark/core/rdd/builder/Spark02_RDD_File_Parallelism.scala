package com.yankee.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/1 22:28
 */
object Spark02_RDD_File_Parallelism {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 创建RDD
    // textFile可以将文件作为数据处理的数据源，默认也可以设定分区
    // minPartitions：最小分区数量
    // math.min(defaultParallelism, 2)
    // 如果不想使用默认的分区数量，可以通过第二个参数指定分区数
    //val data: RDD[String] = sc.textFile("data")
    // Spark读取文件，底层其实使用的就是Hadoop的读取方式
    // 分区数量的计算
    //  totalSize = 字节数
    //  goalSize = 字节数 / 分区数（计算每个分区多少个字节）
    //  字节数 / 每个分区多少个字节 = 分区个数....余数（余数如果占比大于每个分区字节的1.1，则新建一个分区）
    val data: RDD[String] = sc.textFile("data", 3)

    data.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }
}
