package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark20_RDD_Operator_Transform_FourByKey {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)

    /*
    三个参数：
      1.相同Key的第一条数据进行的处理函数
      2.表示分区内的处理函数
      3.表示分区间的处理函数

    reduceByKey:
      combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
    foldByKey:
      combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v), cleanedFunc, cleanedFunc, partitioner)
    aggregateByKey:
      combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v), cleanedSeqOp, combOp, partitioner)
    combineByKey:
      combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine, serializer)(null)
     */

    println("reduceByKey >>>>> " + rdd.reduceByKey(_ + _).collect().mkString(","))
    println("foldByKey >>>>> " + rdd.foldByKey(0)(_ + _).collect().mkString(","))
    println("aggregateByKey >>>>> " + rdd.aggregateByKey(0)(_ + _, _ + _).collect().mkString(","))
    println("combineByKey >>>>> " + rdd.combineByKey(v => v, (x: Int, y) => x + y, (x: Int, y:Int) => x + y).collect().mkString(","))

    // TODO 关闭环境
    sc.stop()
  }
}
