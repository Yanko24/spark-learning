package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark13_RDD_Operator_Transform_DoubleValue {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——interSection、union、subtract、zip
    // 交集、并集和差集要求两个数据源的数据类型保持一致
    // 拉链操作的两个数据源的数据类型可以不一致

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))

    // 交集
    println("interSection >>>>> " + rdd.intersection(rdd2).collect().mkString(","))

    // 并集
    println("union >>>>> " + rdd.union(rdd2).collect().mkString(","))

    // 差集
    println("subtract >>>>> " + rdd.subtract(rdd2).collect().mkString(","))

    // 拉链
    println("zip >>>>> " + rdd.zip(rdd2).collect().mkString(","))

    // 拉链分区
    val rdd3: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd4: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 4)
    // 两个做拉链的数据源的分区数量要保持一致
    // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    //println("zip-partition >>>>> " + rdd3.zip(rdd4).collect().mkString(","))
    val rdd5: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd6: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)
    // 两个做拉链的数据源的分区中的数量要保持一致
    // Can only zip RDDs with same number of elements in each partition
    println("zip-partition >>>>> " + rdd5.zip(rdd6).collect().mkString(","))


    // TODO 关闭环境
    sc.stop()
  }
}
