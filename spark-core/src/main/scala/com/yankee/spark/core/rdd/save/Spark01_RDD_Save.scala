package com.yankee.spark.core.rdd.save

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/18 21:08
 */
object Spark01_RDD_Save {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3),
      ("d", 4)
    ), 2)

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    // 只有Key-Value类型才可以使用
    rdd.saveAsSequenceFile("output3")

    // TODO 关闭环境
    sc.stop()
  }
}
