package com.yankee.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/7 11:48
 */
object Spark10_RDD_Operator_Action_Save {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD行动算子——save
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 2), ("b", 4), ("a", 4)), 2)
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    // saveAsSequenceFile要求数据类型必须是K-V类型
    rdd.saveAsSequenceFile("output2")

    // TODO 关闭环境
    sc.stop()
  }
}
