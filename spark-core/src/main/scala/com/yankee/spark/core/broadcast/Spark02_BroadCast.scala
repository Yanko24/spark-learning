package com.yankee.spark.core.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Yankee
 * @date 2021/3/20 13:22
 */
object Spark02_BroadCast {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)), 2)
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // 封装广播变量
    val broadCast: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map {
      case (word, count) => {
        val i: Int = broadCast.value.getOrElse(word, 0)
        (word, (count, i))
      }
    }.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
