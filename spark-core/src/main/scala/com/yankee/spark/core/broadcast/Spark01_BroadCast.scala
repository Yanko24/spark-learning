package com.yankee.spark.core.broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Yankee
 * @date 2021/3/20 13:22
 */
object Spark01_BroadCast {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)), 2)
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // join会导致数据量几何增长，并且会影响shuffle的性能，不推荐使用
    //val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    //joinRDD.collect().foreach(println)
    //val zipRDD: RDD[((String, Int), (String, Int))] = rdd1.zip(rdd2)
    //zipRDD.collect().foreach(println)
    // 闭包数据都是以Task为单位发送的，每个任务中包含闭包数据，可能会导致一个Executor中包含多个闭包数据，占用内存空间。
    rdd1.map {
      case (word, count) => {
        val i: Int = map.getOrElse(word, 0)
        (word, (count, i))
      }
    }.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
