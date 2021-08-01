package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark19_RDD_Operator_Transform_CombineByKey {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——combineByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)
    // combineByKey：方法需要三个参数
    // 第一个参数表示：将相同Key的第一个数据进行结构的转换，实现操作
    // 第二个参数表示：分区内的计算规则
    // 第三个参数表示：分区间的计算规则
    val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(v => (1, v), (x: (Int, Int), y) => (x
      ._1 + 1, x._2 + y), (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2))
    combineRDD.mapValues {
      case (cnt, sum) => sum / cnt
    }.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
