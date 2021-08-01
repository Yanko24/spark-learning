package com.yankee.spark.core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/18 21:34
 */
object Spark02_ACC {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // Spark默认就提供了简单数据聚合的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    //sc.doubleAccumulator()
    //sc.collectionAccumulator()
    rdd.foreach(
      num => {
        // 使用累加器
        sumAcc.add(num)
      }
    )
    // 获取累加器的值
    println(sumAcc.value)

    // TODO 关闭环境
    sc.stop()
  }
}
