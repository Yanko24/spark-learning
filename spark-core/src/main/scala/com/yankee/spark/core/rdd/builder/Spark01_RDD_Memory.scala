package com.yankee.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/2/28 20:11
 */
object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 创建RDD
    // parallelize：并行
    //val datas: RDD[String] = sc.parallelize(List("Hello World", "Hello Spark", "Hello Scala"))
    // makeRDD底层调用的就是parallelize方法
    val datas: RDD[String] = sc.makeRDD(Seq("Hello World", "Hello Spark", "Hello Scala"))
    val value: RDD[(String, Int)] = datas.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    value.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
