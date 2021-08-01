package com.yankee.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/6 14:45
 */
object Spark10_RDD_Operator_Transform_Coalesce {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD转换算子——coalesce
    // coalesce：算子可以扩大分区，但是如果不进行shuffle操作，是没有意义的，不起任何作用。
    // 如果想要实现扩大分区的效果，必须使用shuffle
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    // coalesce：默认不会将分区内的数据重新打乱组合，缩减分区只是会将分区个数减少
    // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    // 如果想要数据均衡，可以进行shuffle处理
    //val coalesceRDD: RDD[Int] = rdd.coalesce(2)
    // 默认不会进行shuffle
    val coalesceRDD: RDD[Int] = rdd.coalesce(2, true)
    coalesceRDD.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }
}
