package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/5 22:00
 */
object Spark03_RDD_Operator_Transform_MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——mapPartitionsWithIndex
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapPartitionsWithIndexRDD: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) {
        iter
      } else {
        Nil.iterator
      }
    })
    mapPartitionsWithIndexRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
