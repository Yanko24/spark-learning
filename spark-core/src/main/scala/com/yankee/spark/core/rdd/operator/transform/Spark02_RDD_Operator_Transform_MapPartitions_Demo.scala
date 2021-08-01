package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/5 21:40
 */
object Spark02_RDD_Operator_Transform_MapPartitions_Demo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4), 2)
    val mapPartitionsRDD: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })
    mapPartitionsRDD.collect().foreach(println)


    // TODO 关闭环境
    sc.stop()
  }
}
