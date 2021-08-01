package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/4 22:16
 */
object Spark01_RDD_Operator_Transform_Map {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——map
    val rdd: RDD[Int] = sc.makeRDD(Seq(1, 2, 3, 4))

    def mapFunction(num: Int): Int = {
      num * 2
    }

    //val mapRDD: RDD[Int] = rdd.map(mapFunction)
    //val mapRDD: RDD[Int] = rdd.map((num: Int) => {
    //  num * 2
    //})
    val mapRDD: RDD[Int] = rdd.map(_ * 2)

    mapRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
