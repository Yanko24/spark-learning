package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:28
 */
object Spark07_RDD_Operator_Transform_Filter_Demo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——filter
    val rdd: RDD[String] = sc.textFile("data/apache.log")
    rdd.filter(line => {
      val datas: Array[String] = line.split(" ")
      val time: String = datas(3)
      time.startsWith("17/05/2015")
    }).collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
