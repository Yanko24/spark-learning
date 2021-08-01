package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/5 21:26
 */
object Spark02_RDD_Operator_Transform_MapPartitions {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——mapPartitions
    // mapPartitions：可以以分区为单位进行数据转换操作，但是会将这个分区的数据加载到内存中进行引用
    //     如果处理完的数据是不会被释放掉的，存在对象的引用。
    // 在内存较小，数据量较大的场合下，容易出现内存溢出
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapPartitionsRDD: RDD[Int] = rdd.mapPartitions(iter => {
      println(">>>>>>>>>>")
      iter.map(_ * 2)
    })
    mapPartitionsRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
