package com.yankee.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/17 21:03
 */
object Spark02_RDD_CacheAndPersist {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val list: List[String] = List("hello scala", "hello spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map(word => {
      println("@@@@@@@@@@@@@")
      (word, 1)
    })
    // 数据持久化操作
    // cache默认持久化的操作，只能将数据保存到内存中，如果想要保存到磁盘文件，需要更改存储级别
    // mapRDD.cache()
    // 持久化操作必须在行动算子执行时执行完成
    println(mapRDD.toDebugString)
    mapRDD.persist(StorageLevel.DISK_ONLY)
    println(mapRDD.toDebugString)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    reduceRDD.collect().foreach(println)
    println("******************************")
    groupRDD.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
