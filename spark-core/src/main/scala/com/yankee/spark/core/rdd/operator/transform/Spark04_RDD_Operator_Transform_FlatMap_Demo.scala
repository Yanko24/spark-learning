package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/5 22:10
 */
object Spark04_RDD_Operator_Transform_FlatMap_Demo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——flatMap
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val flatMapRDD: RDD[String] = rdd.flatMap(line => {
      line.split(" ")
    })
    flatMapRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
