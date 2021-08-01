package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark18_RDD_Operator_Transform_FoldByKey {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——foldByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)
    // foldByKey：存在函数的柯里化，有两个参数列表
    // 第一个参数列表，需要传递一个参数，表示为初始值
    //              主要用于当碰见第一个Key的时候，和Value进行分区内计算
    // 第二个参数列表需要传递1个参数：表示分区内和分区间的计算规则
    rdd.foldByKey(0)((x, y) => x + y).collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
