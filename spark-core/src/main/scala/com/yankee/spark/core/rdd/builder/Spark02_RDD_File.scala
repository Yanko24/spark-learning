package com.yankee.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/2/28 21:59
 */
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    // TODO 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 创建RDD
    // path可以是文件的具体路径，也可以是目录名称
    // path路径还可以使用通配符：1*.txt
    // path还可以是分布式存储系统路径：HDFS
    //val value: RDD[(String, Int)] = sc.textFile("datas").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //val value: RDD[(String, Int)] = sc.textFile("datas/1*.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    val value: RDD[(String, Int)] = sc.textFile("data").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    value.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
