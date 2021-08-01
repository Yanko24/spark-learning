package com.yankee.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/15 22:06
 */
object Spark02_RDD_Kryo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Search]))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    val rdd: RDD[String] = sc.makeRDD(Array("hello spark", "hello scala", "zookeeper"))
    val search: Search = Search("hello")
    search.getMatch(rdd).collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }

  case class Search(query: String) {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    def getMatch(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => x.contains(query))
    }
  }
}
