package com.yankee.spark.core.rdd.depend

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/16 21:21
 */
object Spark01_RDD_Depend {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val lines: RDD[String] = sc.textFile("data/words.txt")
    println(lines.toDebugString)
    println("=====================================")
    val flatMapRDD: RDD[String] = lines.flatMap(_.split(" "))
    println(flatMapRDD.toDebugString)
    println("=====================================")
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))
    println(mapRDD.toDebugString)
    println("=====================================")
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println(reduceRDD.toDebugString)
    println("=====================================")
    reduceRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
