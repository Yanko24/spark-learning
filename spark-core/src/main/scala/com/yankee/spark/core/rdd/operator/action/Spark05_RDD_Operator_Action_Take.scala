package com.yankee.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/7 11:48
 */
object Spark05_RDD_Operator_Action_Take {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD行动算子——take
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 所谓的行动算子，其实就是触发作业的执行
    // 行动算子底层调用的是sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    // 底层会创建ActiveJob，并提交执行
    println(rdd.take(3).mkString(","))

    // TODO 关闭环境
    sc.stop()
  }
}
