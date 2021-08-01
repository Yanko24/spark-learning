package com.yankee.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/1 21:49
 */
object Spark01_RDD_Memory_ParallelismDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 创建RDD
    val data: RDD[Int] = sc.makeRDD(Seq(1, 2, 3, 4), 3)
    // 将数据保存成分区文件
    data.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }
}
