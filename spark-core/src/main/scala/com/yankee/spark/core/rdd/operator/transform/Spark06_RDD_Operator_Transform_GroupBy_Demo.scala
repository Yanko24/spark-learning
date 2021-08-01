package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 13:38
 */
object Spark06_RDD_Operator_Transform_GroupBy_Demo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——groupBy
    // 分组和分区没有必然的关系
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Spark", "Scala", "Hadoop"), 2)
    val groupByRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))
    groupByRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
