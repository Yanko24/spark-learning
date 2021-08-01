package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/5 19:46
 */
object Spark01_RDD_Operator_Transform_Map_Demo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val rdd: RDD[String] = sc.textFile("data/apache.log")
    val mapRDD: RDD[String] = rdd.map(line => {
      val datas: Array[String] = line.split(" ")
      datas(6)
    })
    mapRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
