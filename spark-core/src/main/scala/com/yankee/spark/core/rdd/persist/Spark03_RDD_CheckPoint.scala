package com.yankee.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/17 21:03
 */
object Spark03_RDD_CheckPoint {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")

    // TODO 业务逻辑
    val list: List[String] = List("hello scala", "hello spark")
    val rdd: RDD[String] = sc.makeRDD(list, 2)
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map(word => {
      println("@@@@@@@@@@@@@")
      (word, 1)
    })

    // checkpoint需要落盘，需要指定检查点路径
    // 检查点路径中保存的文件，执行完成后不会被删除
    // 一般我们的保存路径都是在分布式存储系统中
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    reduceRDD.collect().foreach(println)
    println("******************************")
    groupRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
