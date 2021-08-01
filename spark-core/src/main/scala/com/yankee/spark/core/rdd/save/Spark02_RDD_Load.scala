package com.yankee.spark.core.rdd.save

import org.apache.spark.api.java.JavaSparkContext.fromSparkContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/3/18 21:08
 */
object Spark02_RDD_Load {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    // TODO 创建环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 业务逻辑
    println(sc.textFile("output1").collect().mkString(","))
    println(sc.objectFile[(String, Int)]("output2").collect().mkString(","))
    println(sc.sequenceFile[String, Int]("output3").collect().mkString(","))

    // TODO 关闭环境
    sc.stop()
  }
}
