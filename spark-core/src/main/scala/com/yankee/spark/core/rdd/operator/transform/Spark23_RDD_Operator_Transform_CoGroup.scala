package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark23_RDD_Operator_Transform_CoGroup {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——cogroup
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)), 2)
    val rdd3: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("d", 6)), 2)
    val rdd4: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("a", 6)), 2)

    // cogroup = connect + group
    println(rdd.cogroup(rdd2).collect().mkString(","))
    println(rdd.cogroup(rdd3).collect().mkString(","))
    println(rdd.cogroup(rdd4).collect().mkString(","))

    // TODO 关闭环境
    sc.stop()
  }
}
