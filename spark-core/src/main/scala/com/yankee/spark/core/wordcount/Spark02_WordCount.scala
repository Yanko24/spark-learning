package com.yankee.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yankee
 * @date 2021/2/24 22:06
 */
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    // TODO 建立和Spark框架的连接
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName.filter(!_.equals('$')))
    val sc: SparkContext = new SparkContext(conf)

    // TODO 执行业务操作
    // 1.读取文件，获取一行一行的数据
    val lines: RDD[String] = sc.textFile("datas")
    // 2.将一行数据拆分成一个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

    //wordToOne.reduceByKey(_+_).sortBy(_._1,false).collect().foreach(println)
    // 3.将数据根据单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)
    // 4.对分组后的数据进行聚合
    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        list.reduce((t1, t2) => {
          (t1._1, t1._2 + t2._2)
        })
      }
    }
    // 5.将转换结果采集到控制台打印出来
    wordToCount.collect().foreach(println)

    // TODO 关闭连接
    sc.stop()
  }
}
