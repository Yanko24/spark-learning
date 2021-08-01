package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/5 22:10
 */
object Spark04_RDD_Operator_Transform_FlatMap_Demo2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——flatMap
    val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))
    val flatMapRDD: RDD[Any] = rdd.flatMap(data => {
      data match {
        case list: List[_] => list
        case dat => List(dat)
      }
    })
    flatMapRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
