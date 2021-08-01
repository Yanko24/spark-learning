package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark09_RDD_Operator_Transform_Distinct {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——distinct
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 2, 4, 4, 7, 7, 7, 9, 10))
    // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    println(rdd.distinct().collect().mkString(","))

    // TODO 关闭环境
    sc.stop()
  }
}
