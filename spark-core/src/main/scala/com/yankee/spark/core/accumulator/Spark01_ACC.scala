package com.yankee.spark.core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/18 21:34
 */
object Spark01_ACC {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // reduce：分区内计算，分区间计算
    //val i: Int = rdd.reduce(_ + _)
    //println(i)

    // 错误示例：
    var sum: Int = 0
    rdd.foreach(num => {
      sum += num
    })
    println(s"sum = $sum")


    // TODO 关闭环境
    sc.stop()
  }
}
