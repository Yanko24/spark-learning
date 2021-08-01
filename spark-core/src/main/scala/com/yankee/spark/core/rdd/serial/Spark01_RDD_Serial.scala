package com.yankee.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/15 21:35
 */
object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hello hive", "zookeeper"))

    val search: Search = new Search("h")

    // 需要闭包检测
    //search.getMatch1(rdd).collect().foreach(println)
    // 需要闭包检测
    //search.getMatch2(rdd).collect().foreach(println)
    // 不需要闭包检测
    search.getMatch3(rdd).collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }

  // 类的构造参数其实是类的属性，构造参数需要进行闭包检测，其实就等同于类进行闭包检测
  class Search(query: String) /*extends Serializable*/ {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => x.contains(query))
    }

    // 这样可以不需要闭包检测
    def getMatch3(rdd: RDD[String]): RDD[String] = {
      val s: String = query
      rdd.filter(x => x.contains(s))
    }
  }
}
