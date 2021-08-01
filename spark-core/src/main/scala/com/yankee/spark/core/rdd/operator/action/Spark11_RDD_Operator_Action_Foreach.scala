package com.yankee.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/7 11:48
 */
object Spark11_RDD_Operator_Action_Foreach {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO RDD行动算子——foreach
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // foreach是Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("=====================")
    // foreach是Executor端内存数据的循环遍历
    rdd.foreach(println)

    // 算子：Operator（操作）
    // RDD的方法和Scala集合对象的方法不一样
    // 集合对象的方法都是在同一个节点的内存中完成的。
    // RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
    // 为了区分不同的处理效果，所以将RDD的方法称之为算子。
    // RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行。

    // TODO 关闭环境
    sc.stop()
  }
}
