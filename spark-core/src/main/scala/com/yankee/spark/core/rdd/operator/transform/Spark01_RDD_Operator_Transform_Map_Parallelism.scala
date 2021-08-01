package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/5 20:19
 */
object Spark01_RDD_Operator_Transform_Map_Parallelism {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    // 1.rdd的计算一个分区内的数据是一个一个执行逻辑
    //   只有前面一个数据全部的逻辑执行完毕之后，才会执行下一个数据，分区内数据的执行是有序的
    // 2.不同分区之间的数据计算是无序的
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD: RDD[Int] = rdd.map(num => {
      println(s">>>>> num = $num")
      num
    })

    val value: RDD[Int] = mapRDD.map(num => {
      println(s"##### num = $num")
      num
    })

    value.collect()

    // TODO 关闭环境
    sc.stop()
  }
}
