package com.yankee.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/2/24 22:20
 */
object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("datas")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    //wordToOne.reduceByKey((x, y) => {x + y})
    //wordToOne.reduceByKey((x, y) => x + y)
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    wordToCount.collect().foreach(println)

    // TODO 关闭连接
    sc.stop()
  }
}
